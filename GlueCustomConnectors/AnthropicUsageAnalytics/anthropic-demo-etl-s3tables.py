# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""anthropic-demo-etl: live Anthropic reads via REST-ANTHROPIC-DEMO -> raw + curated tables (Glue 5.0).

Stage 1 (extract): read Admin API entities (admin connection) and workspace entities (one connection per
  workspace) through the connector and land them as raw Parquet tables.
Stage 2 (curate):  explode usage/cost `results` JSON into fact tables and parse JSON columns into dimensions.
Stage 4 (marts, S3 Tables target): business-ready Iceberg tables in the `mart_tables` namespace for Athena and Amazon Quick
  (Athena can't create views over S3 Tables).
Stage 3 (skill content, DIRECT API, not the connector): download each skill version's ZIP from
  GET /v1/skills/{skill_id}/versions/{version_id}/content (the connector only reads JSON), store file text,
  and diff consecutive versions file by file.
--target s3tables: Apache Iceberg tables in the S3 table bucket (namespaces raw, curated, reference), with
  incremental MERGE of usage/cost over a short trailing window. --target gdc: Parquet tables in `anthropic_demo`.
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T
from connectivity.adapter import CatalogConnectionHelper

args = getResolvedOptions(sys.argv, ["JOB_NAME", "connection_name", "ws_connections", "s3_root", "starting_at", "content_source_types", "target"])
sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)

CONN = args["connection_name"]
# workspace-scoped entities need a workspace API key: one Glue connection per workspace
WS_CONNS = __import__("json").loads(args["ws_connections"])  # {"<connection name>": "<workspace name>"}
ROOT = args["s3_root"].rstrip("/")
START = args["starting_at"]
DB = "anthropic_demo"
CONNECTOR = "rest-anthropic-demo"
TARGET = args["target"]          # "s3tables" or "gdc"
CAT = "s3tables"                 # Iceberg catalog registered via --conf (s3tablescatalog/<table bucket>)
REFRESH_DAYS = 3                 # re-read this many trailing days so late cost corrections are merged


helper = CatalogConnectionHelper(spark)


def read(entity, filter_predicate=None, conn=None, **read_options):
    """Read an Anthropic entity straight into a Spark DataFrame through the Glue connection."""
    opts = {"ENTITY_NAME": entity, "SELECTED_FIELDS": "*", **read_options}
    if filter_predicate:
        opts["FILTER_PREDICATE"] = filter_predicate
    df = helper.read(CONNECTOR, conn or CONN, opts)
    print(f"READ {entity} filter=[{filter_predicate}] options={read_options} rows={df.count()}")
    return df


def table_exists(full_name):
    try:
        spark.table(full_name).limit(0).collect()
        return True
    except Exception:  # noqa: BLE001 - AnalysisException when the table doesn't exist yet
        return False


def save(df, table, layer, merge_keys=None):
    """gdc: overwrite Parquet tables. s3tables: Iceberg createOrReplace, or MERGE on merge_keys once the table exists."""
    df = df.withColumn("ingested_at", F.current_timestamp())
    if TARGET == "gdc":
        (df.write.mode("overwrite").format("parquet")
           .option("path", f"{ROOT}/{layer}/{table}/")
           .saveAsTable(f"{DB}.{table}"))
        print(f"WROTE {DB}.{table} rows={df.count()}")
        return
    full = f"{CAT}.{layer}.{table}"
    # S3 Tables storage doesn't allow clients to delete objects. Iceberg deletes the data file of any write task
    # that receives zero rows, so write small tables from a single partition and skip empty loads.
    df = df.coalesce(1)
    existed = table_exists(full)
    if existed and layer == "mart_tables" and set(spark.table(full).columns) != set(df.columns):
        # marts are rebuilt every run: when a mart's columns change, drop it and create it again
        spark.sql(f"DROP TABLE {full} PURGE")  # S3 Tables requires PURGE and deletes the table's data itself
        print(f"DROPPED {full} (mart columns changed)")
        existed = False
    if not existed:
        # Create through the S3 Tables catalog with DDL (the catalog assigns the table location), then load.
        cols = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {full} ({cols}) USING iceberg")
    if existed:  # schema evolution: add columns that are new in this run (Iceberg ALTER TABLE ... ADD COLUMNS)
        current = set(spark.table(full).columns)
        added = [f for f in df.schema if f.name not in current]
        if added:
            spark.sql(f"ALTER TABLE {full} ADD COLUMNS (" +
                      ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in added) + ")")
            print(f"EVOLVED {full} added={[f.name for f in added]}")
    if df.limit(1).count() == 0:
        print(f"SKIPPED {full} (no rows to load)")
        return
    if merge_keys and existed:
        df.createOrReplaceTempView("src")
        on = " AND ".join(f"t.{k} <=> s.{k}" for k in merge_keys)
        spark.sql(f"MERGE INTO {full} t USING src s ON {on} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
        print(f"MERGED {full} source_rows={df.count()} table_rows={spark.table(full).count()}")
    else:
        df.writeTo(full).overwrite(F.lit(True))  # full refresh (dimensions) or first load (facts)
        print(f"WROTE {full} rows={df.count()}")


# incremental window: backfill from --starting_at on the first S3 Tables run, then only the trailing days
READ_START = START
if TARGET == "s3tables" and table_exists(f"{CAT}.curated.fact_cost_daily"):
    import datetime as _dt
    READ_START = (_dt.datetime.now(_dt.timezone.utc).date() - _dt.timedelta(days=REFRESH_DAYS)).isoformat() + "T00:00:00Z"
print(f"READ_START {READ_START} (target={TARGET})")


# ---------------- Stage 1: extract (live connector reads) ----------------
raw = {
    "raw_users": read("users"),
    "raw_invites": read("invites"),
    "raw_workspaces": read("workspaces"),
    "raw_api_keys": read("api_keys"),
    # one grouped read per report: Anthropic breaks each bucket down by every group_by dimension
    "raw_usage_report": read(
        "usage_report",
        f"starting_at = '{READ_START}' AND group_by = 'workspace_id' AND group_by = 'api_key_id' AND group_by = 'model'",
    ).select("starting_at", "ending_at", "results"),
    "raw_cost_report": read(
        "cost_report",
        f"starting_at = '{READ_START}' AND group_by = 'workspace_id' AND group_by = 'description'",
    ).select("starting_at", "ending_at", "results"),
}
# workspace connections: skills, batches and files from every workspace
def union_all(dfs):
    out = dfs[0]
    for d in dfs[1:]:
        out = out.unionByName(d, allowMissingColumns=True)
    return out


def from_each_workspace(entity):
    return union_all([read(entity, conn=c).withColumn("workspace_name", F.lit(w)).withColumn("connection_name", F.lit(c))
                      for c, w in WS_CONNS.items()])


# Anthropic-provided skills are listed in every workspace; keep one row per skill (custom skills win)
skills_all = from_each_workspace("skills").withColumn("source_type", F.get_json_object("source", "$.type"))
from pyspark.sql.window import Window
pick = Window.partitionBy("id").orderBy(F.when(F.col("source_type") == "custom", 0).otherwise(1), "connection_name")
raw["raw_skills"] = (skills_all.withColumn("_rn", F.row_number().over(pick)).filter("_rn = 1")
                     .withColumn("workspace_name", F.when(F.col("source_type") == "custom", F.col("workspace_name")))
                     .drop("_rn", "source_type"))
SKILL_CONN = {r["id"]: r["connection_name"] for r in raw["raw_skills"].select("id", "connection_name").collect()}
# versions are nested: one read per skill, through the connection of the workspace that owns it
raw["raw_skill_versions"] = union_all([read(f"skill_versions/{sid}", conn=c) for sid, c in SKILL_CONN.items()])
raw["raw_message_batches"] = from_each_workspace("message_batches")
raw["raw_files"] = from_each_workspace("files")

for name, df in raw.items():
    save(df, name, "raw", merge_keys=["starting_at"] if name in ("raw_usage_report", "raw_cost_report") else None)

# business context (customer-owned reference data, CSV in S3) -> reference tables (S3 Tables target only)
if TARGET == "s3tables":
    # support_outcomes_daily is SAMPLE data (stands in for the support system's daily export)
    for ref in ("workspace_business_unit", "support_outcomes_daily", "hr_directory", "skill_business_unit"):
        ref_df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{ROOT}/customer/{ref}/")
        save(ref_df, ref, "reference")

# ---------------- Stage 2: curate ----------------
default_ws = (raw["raw_workspaces"].filter(F.col("name") == "Default").select("id").first() or [None])[0]
print(f"Default workspace id: {default_ws} (Anthropic reports Default-workspace activity with workspace_id = null)")

usage_line = T.ArrayType(T.StructType([
    T.StructField("workspace_id", T.StringType()), T.StructField("api_key_id", T.StringType()),
    T.StructField("model", T.StringType()), T.StructField("service_tier", T.StringType()),
    T.StructField("uncached_input_tokens", T.LongType()), T.StructField("cache_read_input_tokens", T.LongType()),
    T.StructField("cache_creation", T.StructType([
        T.StructField("ephemeral_1h_input_tokens", T.LongType()),
        T.StructField("ephemeral_5m_input_tokens", T.LongType())])),
    T.StructField("output_tokens", T.LongType()),
]))
cost_line = T.ArrayType(T.StructType([
    T.StructField("workspace_id", T.StringType()), T.StructField("description", T.StringType()),
    T.StructField("model", T.StringType()), T.StructField("cost_type", T.StringType()),
    T.StructField("token_type", T.StringType()), T.StructField("currency", T.StringType()),
    T.StructField("amount", T.StringType()),
]))

fact_usage = (raw["raw_usage_report"]
    .withColumn("line", F.explode(F.from_json("results", usage_line)))
    .select(F.to_date("starting_at").alias("usage_date"),
            F.coalesce("line.workspace_id", F.lit(default_ws)).alias("workspace_id"),
            "line.api_key_id", "line.model", "line.service_tier",
            F.col("line.uncached_input_tokens").alias("uncached_input_tokens"),
            F.col("line.cache_read_input_tokens").alias("cache_read_input_tokens"),
            (F.coalesce("line.cache_creation.ephemeral_1h_input_tokens", F.lit(0))
             + F.coalesce("line.cache_creation.ephemeral_5m_input_tokens", F.lit(0))).alias("cache_creation_input_tokens"),
            F.col("line.output_tokens").alias("output_tokens"))
    .groupBy("usage_date", "workspace_id", "api_key_id", "model", "service_tier")
    .agg(*[F.sum(c).alias(c) for c in ("uncached_input_tokens", "cache_read_input_tokens",
                                       "cache_creation_input_tokens", "output_tokens")]))
USAGE_KEYS = ["usage_date", "workspace_id", "api_key_id", "model", "service_tier"]

fact_cost = (raw["raw_cost_report"]
    .withColumn("line", F.explode(F.from_json("results", cost_line)))
    .select(F.to_date("starting_at").alias("cost_date"),
            F.coalesce("line.workspace_id", F.lit(default_ws)).alias("workspace_id"),
            "line.description", "line.model", "line.cost_type", "line.token_type", "line.currency",
            (F.col("line.amount").cast(T.DecimalType(20, 8)) / 100).alias("amount_usd"))  # Anthropic reports cents
    .groupBy("cost_date", "workspace_id", "description", "model", "cost_type", "token_type", "currency")
    .agg(F.sum("amount_usd").cast(T.DecimalType(20, 8)).alias("amount_usd")))
COST_KEYS = ["cost_date", "workspace_id", "description", "model", "cost_type", "token_type", "currency"]

id_type = T.StructType([T.StructField("id", T.StringType()), T.StructField("type", T.StringType())])
scope_type = T.StructType([T.StructField("type", T.StringType()), T.StructField("workspace_id", T.StringType())])
dim_api_key = (raw["raw_api_keys"]
    .withColumn("cb", F.from_json("created_by", id_type)).withColumn("sc", F.from_json("scope", scope_type))
    .select(F.col("id").alias("api_key_id"), "name", "status", "partial_key_hint", "created_at", "expires_at",
            F.col("cb.id").alias("created_by_user_id"), F.col("sc.type").alias("scope_type"),
            F.coalesce("sc.workspace_id", "workspace_id").alias("workspace_id"),
            F.coalesce(F.get_json_object("principal", "$.type"), F.lit("legacy")).alias("principal_type"),
            F.when(F.get_json_object("principal", "$.type") == "user_actor", "linked")
             .when(F.get_json_object("principal", "$.type") == "service_account_actor", "service account")
             .otherwise("legacy").alias("key_type")))
dim_user = raw["raw_users"].select(F.col("id").alias("user_id"), "email", "name", "role", "added_at")
dim_workspace = raw["raw_workspaces"].select(F.col("id").alias("workspace_id"), "name", "created_at", "archived_at")

src_type = T.StructType([T.StructField("type", T.StringType())])
dim_skill = (raw["raw_skills"].withColumn("src", F.from_json("source", src_type))
    .select(F.col("id").alias("skill_id"), "display_name", F.col("src.type").alias("source_type"),
            "workspace_name", "latest_version_id", "created_at", "updated_at"))
w = Window.partitionBy("skill_id").orderBy("created_at")
fact_skill_version = (raw["raw_skill_versions"]
    .select(F.col("id").alias("version_id"), "skill_id", "name", "description", "created_at")
    .withColumn("version_number", F.row_number().over(w))
    .withColumn("previous_description", F.lag("description").over(w))
    .withColumn("description_changed",
                F.col("previous_description").isNotNull() & (F.col("description") != F.col("previous_description")))
    .withColumn("version_date", F.to_date("created_at")))

counts = "request_counts"
fact_message_batch = raw["raw_message_batches"].select(
    F.col("id").alias("batch_id"), "workspace_name", "processing_status", "created_at", "ended_at", "expires_at",
    *[F.get_json_object(counts, f"$.{k}").cast("long").alias(f"{k}_requests")
      for k in ("processing", "succeeded", "errored", "canceled", "expired")])
dim_file = raw["raw_files"].select(F.col("id").alias("file_id"), "workspace_name", "filename", "mime_type",
                                   F.col("size_bytes").cast("long").alias("size_bytes"), "created_at", "downloadable")

for name, df in {"fact_message_batch": fact_message_batch, "dim_file": dim_file, "dim_skill": dim_skill, "fact_skill_version": fact_skill_version,
                 "fact_usage_daily": fact_usage, "fact_cost_daily": fact_cost, "dim_api_key": dim_api_key,
                 "dim_user": dim_user, "dim_workspace": dim_workspace}.items():
    save(df, name, "curated", merge_keys={"fact_usage_daily": USAGE_KEYS, "fact_cost_daily": COST_KEYS}.get(name))

# ---------------- Stage 3: skill content + diff (direct API call, NOT via the connector) ----------------
import boto3, difflib, hashlib, io, json as _json, urllib.request, zipfile
_glue, _sm = boto3.client("glue"), boto3.client("secretsmanager")
_keys = {}


def api_key_for(conn):
    """The workspace key stored in the Glue connection's Secrets Manager secret."""
    if conn not in _keys:
        arn = _glue.get_connection(Name=conn)["Connection"]["AuthenticationConfiguration"]["SecretArn"]
        _keys[conn] = _json.loads(_sm.get_secret_value(SecretId=arn)["SecretString"])["apiKey"]
    return _keys[conn]



content_types = [t.strip() for t in args["content_source_types"].split(",") if t.strip()]
targets = (fact_skill_version.join(dim_skill.select("skill_id", "source_type"), "skill_id")
           .filter(F.col("source_type").isin(content_types))
           .select("skill_id", "version_id", "version_number", "created_at").orderBy("skill_id", "version_number").collect())
print(f"CONTENT fetching {len(targets)} versions for source types {content_types}")

files = []
for t in targets:
    req = urllib.request.Request(
        f"https://api.anthropic.com/v1/skills/{t.skill_id}/versions/{t.version_id}/content",
        headers={"x-api-key": api_key_for(SKILL_CONN[t.skill_id]), "anthropic-version": "2023-06-01"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        archive = zipfile.ZipFile(io.BytesIO(resp.read()))
    for name in archive.namelist():
        if name.endswith("/"):
            continue
        raw_bytes = archive.read(name)
        text = raw_bytes.decode("utf-8", errors="replace")
        files.append((t.skill_id, t.version_id, t.version_number, t.created_at, name.split("/", 1)[-1],
                      text, len(raw_bytes), hashlib.sha256(raw_bytes).hexdigest()))
file_schema = T.StructType([
    T.StructField("skill_id", T.StringType()), T.StructField("version_id", T.StringType()),
    T.StructField("version_number", T.IntegerType()), T.StructField("version_created_at", T.TimestampType()),
    T.StructField("file_path", T.StringType()), T.StructField("content", T.StringType()),
    T.StructField("size_bytes", T.LongType()), T.StructField("content_sha256", T.StringType())])
fact_file = spark.createDataFrame(files, file_schema)

# diff each version against the previous version of the same skill, file by file
by_version = {}
for f in files:
    by_version.setdefault((f[0], f[2]), {"version_id": f[1], "created_at": f[3], "files": {}})["files"][f[4]] = f[5]
diffs = []
for (skill_id, vnum), cur in sorted(by_version.items()):
    prev = by_version.get((skill_id, vnum - 1))
    prev_files = prev["files"] if prev else {}
    for path in sorted(set(cur["files"]) | set(prev_files)):
        old, new = prev_files.get(path), cur["files"].get(path)
        if prev is None:
            change = "initial"
        elif old is None:
            change = "added"
        elif new is None:
            change = "removed"
        elif old == new:
            change = "unchanged"
        else:
            change = "modified"
        lines = list(difflib.unified_diff((old or "").splitlines(), (new or "").splitlines(),
                                          fromfile=f"v{vnum - 1}/{path}", tofile=f"v{vnum}/{path}", lineterm=""))
        added = sum(1 for l in lines if l.startswith("+") and not l.startswith("+++"))
        removed = sum(1 for l in lines if l.startswith("-") and not l.startswith("---"))
        diffs.append((skill_id, cur["version_id"], vnum, cur["created_at"], prev["version_id"] if prev else None,
                      path, change, added, removed, "\n".join(lines) if prev else None))
diff_schema = T.StructType([
    T.StructField("skill_id", T.StringType()), T.StructField("version_id", T.StringType()),
    T.StructField("version_number", T.IntegerType()), T.StructField("version_created_at", T.TimestampType()),
    T.StructField("previous_version_id", T.StringType()), T.StructField("file_path", T.StringType()),
    T.StructField("change_type", T.StringType()), T.StructField("lines_added", T.IntegerType()),
    T.StructField("lines_removed", T.IntegerType()), T.StructField("unified_diff", T.StringType())])
fact_diff = spark.createDataFrame(diffs, diff_schema)
for name, df in {"fact_skill_version_file": fact_file, "fact_skill_version_diff": fact_diff}.items():
    save(df, name, "curated")

# ---------------- Stage 4: marts (S3 Tables target) ----------------
# Athena can't create views over S3 Tables, so business-ready results are materialized as Iceberg tables in `mart_tables`.
if TARGET == "s3tables":
    def t(ns, name):
        return f"{CAT}.{ns}.{name}"

    MARTS = {
        "daily_spend_by_business_unit": f"""
            SELECT c.cost_date, coalesce(bu.business_unit, w.name, c.workspace_id) AS business_unit,
                   sum(c.amount_usd) AS spend_usd
            FROM {t('curated','fact_cost_daily')} c
            LEFT JOIN {t('curated','dim_workspace')} w ON w.workspace_id = c.workspace_id
            LEFT JOIN {t('reference','workspace_business_unit')} bu ON bu.workspace_name = w.name
            GROUP BY 1, 2""",
        "weekly_spend_by_business_unit": f"""
            WITH weekly AS (
              SELECT CAST(date_trunc('WEEK', c.cost_date) AS DATE) AS week_start,
                     coalesce(bu.business_unit, w.name, c.workspace_id) AS business_unit,
                     sum(c.amount_usd) AS spend_usd
              FROM {t('curated','fact_cost_daily')} c
              LEFT JOIN {t('curated','dim_workspace')} w ON w.workspace_id = c.workspace_id
              LEFT JOIN {t('reference','workspace_business_unit')} bu ON bu.workspace_name = w.name
              GROUP BY 1, 2)
            SELECT week_start, business_unit, spend_usd,
                   lag(spend_usd) OVER (PARTITION BY business_unit ORDER BY week_start) AS prev_week_spend_usd,
                   spend_usd - coalesce(lag(spend_usd) OVER (PARTITION BY business_unit ORDER BY week_start), 0) AS change_usd
            FROM weekly""",
        "spend_by_cost_line": f"""
            SELECT CAST(date_trunc('WEEK', cost_date) AS DATE) AS week_start, description, model, token_type,
                   sum(amount_usd) AS spend_usd
            FROM {t('curated','fact_cost_daily')} GROUP BY 1, 2, 3, 4""",
        "unit_cost": f"""
            SELECT o.outcome_date, o.business_unit, s.spend_usd, o.conversations, o.completed_resolutions,
                   o.qa_score_pct, s.spend_usd / nullif(o.completed_resolutions, 0) AS cost_per_resolution_usd,
                   o.is_sample
            FROM (SELECT to_date(outcome_date) AS outcome_date, business_unit, conversations,
                         completed_resolutions, qa_score_pct, is_sample
                  FROM {t('reference','support_outcomes_daily')}) o
            JOIN (SELECT c.cost_date, coalesce(bu.business_unit, w.name) AS business_unit, sum(c.amount_usd) AS spend_usd
                  FROM {t('curated','fact_cost_daily')} c
                  LEFT JOIN {t('curated','dim_workspace')} w ON w.workspace_id = c.workspace_id
                  LEFT JOIN {t('reference','workspace_business_unit')} bu ON bu.workspace_name = w.name
                  GROUP BY 1, 2) s
              ON s.business_unit = o.business_unit AND s.cost_date = o.outcome_date""",
        "access_review": f"""
            WITH key_usage AS (
              SELECT api_key_id, max(usage_date) AS last_used,
                     sum(uncached_input_tokens) AS input_tokens, sum(output_tokens) AS output_tokens
              FROM {t('curated','fact_usage_daily')} GROUP BY 1)
            SELECT k.api_key_id, k.name AS key_name, k.partial_key_hint, k.status, k.key_type,
                   w.name AS workspace, u.email AS created_by, hr.employment_status AS creator_status,
                   ku.last_used, ku.input_tokens, ku.output_tokens,
                   (k.status = 'active' AND hr.employment_status = 'departed' AND ku.last_used IS NOT NULL) AS review_candidate
            FROM {t('curated','dim_api_key')} k
            LEFT JOIN {t('curated','dim_user')} u ON u.user_id = k.created_by_user_id
            LEFT JOIN {t('reference','hr_directory')} hr ON hr.email = u.email
            LEFT JOIN {t('curated','dim_workspace')} w ON w.workspace_id = k.workspace_id
            LEFT JOIN key_usage ku ON ku.api_key_id = k.api_key_id""",
        "skill_inventory": f"""
            SELECT s.skill_id, s.display_name, s.source_type, s.workspace_name,
                   bu.business_unit AS owner_business_unit,
                   count(v.version_id) AS version_count, min(v.created_at) AS first_version_at,
                   max(v.created_at) AS latest_version_at, s.updated_at,
                   datediff(current_date(), to_date(s.updated_at)) AS days_since_update
            FROM {t('curated','dim_skill')} s
            LEFT JOIN {t('curated','fact_skill_version')} v ON v.skill_id = s.skill_id
            LEFT JOIN {t('reference','skill_business_unit')} bu ON bu.skill_display_name = s.display_name
            GROUP BY s.skill_id, s.display_name, s.source_type, s.workspace_name, bu.business_unit, s.updated_at""",
        "skill_change_log": f"""
            SELECT s.display_name, s.source_type, v.version_number, v.version_id, v.created_at,
                   datediff(v.created_at, lag(v.created_at) OVER (PARTITION BY v.skill_id ORDER BY v.created_at))
                     AS days_since_previous_version,
                   (v.version_id = s.latest_version_id) AS is_latest, v.description_changed,
                   v.previous_description, v.description
            FROM {t('curated','fact_skill_version')} v
            JOIN {t('curated','dim_skill')} s ON s.skill_id = v.skill_id""",
        "skill_content_changelog": f"""
            SELECT s.display_name, s.source_type, d.version_number, d.version_id, d.version_created_at,
                   d.previous_version_id, d.file_path, d.change_type, d.lines_added, d.lines_removed,
                   d.unified_diff, f.size_bytes, f.content_sha256
            FROM {t('curated','fact_skill_version_diff')} d
            JOIN {t('curated','dim_skill')} s ON s.skill_id = d.skill_id
            LEFT JOIN {t('curated','fact_skill_version_file')} f
              ON f.version_id = d.version_id AND f.file_path = d.file_path""",
        "anthropic_skill_releases": f"""
            SELECT CAST(date_trunc('WEEK', v.created_at) AS DATE) AS week_start, s.display_name,
                   count(*) AS new_versions, max(v.created_at) AS latest_release_at
            FROM {t('curated','fact_skill_version')} v
            JOIN {t('curated','dim_skill')} s ON s.skill_id = v.skill_id
            WHERE s.source_type = 'anthropic' GROUP BY 1, 2""",
    }
    for name, sql in MARTS.items():
        save(spark.sql(sql), name, "mart_tables")

    # skill versions created each day next to the owning business unit's daily unit cost (sample outcomes)
    save(spark.sql(f"""
        WITH changes AS (
          SELECT bu.business_unit, to_date(v.created_at) AS change_date,
                 count(*) AS skill_versions_created, concat_ws(', ', collect_set(s.display_name)) AS skills_changed
          FROM {t('curated','fact_skill_version')} v
          JOIN {t('curated','dim_skill')} s ON s.skill_id = v.skill_id
          JOIN {t('reference','skill_business_unit')} bu ON bu.skill_display_name = s.display_name
          GROUP BY 1, 2)
        SELECT u.outcome_date, u.business_unit, u.spend_usd, u.completed_resolutions, u.qa_score_pct,
               u.cost_per_resolution_usd, coalesce(c.skill_versions_created, 0) AS skill_versions_created,
               c.skills_changed, u.is_sample
        FROM {t('mart_tables','unit_cost')} u
        LEFT JOIN changes c ON c.business_unit = u.business_unit AND c.change_date = u.outcome_date"""),
        "skill_change_vs_outcome", "mart_tables")

job.commit()
