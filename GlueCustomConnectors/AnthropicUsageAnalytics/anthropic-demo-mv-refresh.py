# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Anthropic usage analytics: Apache Iceberg materialized views (Glue Data Catalog) over the S3 Tables curated layer.

Alternative to the table-based marts written by anthropic-demo-etl-s3tables (namespace `mart_tables`). This job creates the
same marts as materialized views in namespace `mart`, then refreshes them. It runs after the ETL job succeeds.
Requires AWS Glue 5.1+ and iam:PassRole on the job role (the role is stored with each view for managed refreshes).
"""
import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
spark = SparkSession.builder.getOrCreate()
spark.sql("SET spark.sql.optimizer.incrementalMVRefresh.enabled=true")

CAT, NS = "s3tables", "mart"


def t(ns, name):
    return f"{CAT}.{ns}.{name}"


def spend_by_bu(grain):
    """Spend per business unit at a day (`c.cost_date`) or week grain."""
    period = "c.cost_date" if grain == "day" else "CAST(date_trunc('WEEK', c.cost_date) AS DATE)"
    return f"""
        SELECT {period} AS period_start, coalesce(bu.business_unit, w.name, c.workspace_id) AS business_unit,
               sum(c.amount_usd) AS spend_usd
        FROM {t('curated', 'fact_cost_daily')} c
        LEFT JOIN {t('curated', 'dim_workspace')} w ON w.workspace_id = c.workspace_id
        LEFT JOIN {t('reference', 'workspace_business_unit')} bu ON bu.workspace_name = w.name
        GROUP BY 1, 2"""


# daily unit cost, only on days that have both spend and (sample) support outcomes
UNIT_COST = f"""
    SELECT o.outcome_date, o.business_unit, s.period_start AS cost_date, s.spend_usd, o.conversations,
           o.completed_resolutions, o.qa_score_pct,
           s.spend_usd / nullif(o.completed_resolutions, 0) AS cost_per_resolution_usd, o.is_sample
    FROM (SELECT to_date(outcome_date) AS outcome_date, business_unit, conversations, completed_resolutions,
                 qa_score_pct, is_sample
          FROM {t('reference', 'support_outcomes_daily')}) o
    JOIN ({spend_by_bu('day')}) s ON s.business_unit = o.business_unit AND s.period_start = o.outcome_date"""

SKILL_INVENTORY_BASE = f"""
    SELECT s.skill_id, s.display_name, s.source_type, s.workspace_name, bu.business_unit AS owner_business_unit,
           count(v.version_id) AS version_count, min(v.created_at) AS first_version_at,
           max(v.created_at) AS latest_version_at, s.updated_at{{extra}}
    FROM {t('curated', 'dim_skill')} s
    LEFT JOIN {t('curated', 'fact_skill_version')} v ON v.skill_id = s.skill_id
    LEFT JOIN {t('reference', 'skill_business_unit')} bu ON bu.skill_display_name = s.display_name
    GROUP BY s.skill_id, s.display_name, s.source_type, s.workspace_name, bu.business_unit, s.updated_at"""

MVS = {
    "daily_spend_by_business_unit": f"""
        SELECT period_start AS cost_date, business_unit, spend_usd FROM ({spend_by_bu('day')})""",
    "weekly_spend_by_business_unit": f"""
        SELECT period_start AS week_start, business_unit, spend_usd,
               lag(spend_usd) OVER (PARTITION BY business_unit ORDER BY period_start) AS prev_week_spend_usd,
               spend_usd - coalesce(lag(spend_usd) OVER (PARTITION BY business_unit ORDER BY period_start), 0)
                 AS change_usd
        FROM ({spend_by_bu('week')})""",
    "spend_by_cost_line": f"""
        SELECT CAST(date_trunc('WEEK', cost_date) AS DATE) AS week_start, description, model, token_type,
               sum(amount_usd) AS spend_usd
        FROM {t('curated', 'fact_cost_daily')} GROUP BY 1, 2, 3, 4""",
    "unit_cost": UNIT_COST,
    "access_review": f"""
        SELECT k.api_key_id, k.name AS key_name, k.partial_key_hint, k.status, k.key_type,
               w.name AS workspace, u.email AS created_by, hr.employment_status AS creator_status,
               ku.last_used, ku.input_tokens, ku.output_tokens,
               (k.status = 'active' AND hr.employment_status = 'departed' AND ku.last_used IS NOT NULL)
                 AS review_candidate
        FROM {t('curated', 'dim_api_key')} k
        LEFT JOIN {t('curated', 'dim_user')} u ON u.user_id = k.created_by_user_id
        LEFT JOIN {t('reference', 'hr_directory')} hr ON hr.email = u.email
        LEFT JOIN {t('curated', 'dim_workspace')} w ON w.workspace_id = k.workspace_id
        LEFT JOIN (SELECT api_key_id, max(usage_date) AS last_used,
                          sum(uncached_input_tokens) AS input_tokens, sum(output_tokens) AS output_tokens
                   FROM {t('curated', 'fact_usage_daily')} GROUP BY 1) ku ON ku.api_key_id = k.api_key_id""",
    "skill_inventory": SKILL_INVENTORY_BASE.format(
        extra=",\n           datediff(current_date(), to_date(s.updated_at)) AS days_since_update"),
    "skill_change_log": f"""
        SELECT s.display_name, s.source_type, v.version_number, v.version_id, v.created_at,
               datediff(v.created_at, lag(v.created_at) OVER (PARTITION BY v.skill_id ORDER BY v.created_at))
                 AS days_since_previous_version,
               (v.version_id = s.latest_version_id) AS is_latest, v.description_changed,
               v.previous_description, v.description
        FROM {t('curated', 'fact_skill_version')} v
        JOIN {t('curated', 'dim_skill')} s ON s.skill_id = v.skill_id""",
    "skill_content_changelog": f"""
        SELECT s.display_name, s.source_type, d.version_number, d.version_id, d.version_created_at,
               d.previous_version_id, d.file_path, d.change_type, d.lines_added, d.lines_removed,
               d.unified_diff, f.size_bytes, f.content_sha256
        FROM {t('curated', 'fact_skill_version_diff')} d
        JOIN {t('curated', 'dim_skill')} s ON s.skill_id = d.skill_id
        LEFT JOIN {t('curated', 'fact_skill_version_file')} f
          ON f.version_id = d.version_id AND f.file_path = d.file_path""",
    "anthropic_skill_releases": f"""
        SELECT CAST(date_trunc('WEEK', v.created_at) AS DATE) AS week_start, s.display_name,
               count(*) AS new_versions, max(v.created_at) AS latest_release_at
        FROM {t('curated', 'fact_skill_version')} v
        JOIN {t('curated', 'dim_skill')} s ON s.skill_id = v.skill_id
        WHERE s.source_type = 'anthropic' GROUP BY 1, 2""",
    "skill_change_vs_outcome": f"""
        SELECT u.outcome_date, u.business_unit, u.spend_usd, u.completed_resolutions, u.qa_score_pct,
               u.cost_per_resolution_usd, coalesce(c.skill_versions_created, 0) AS skill_versions_created,
               c.skills_changed, u.is_sample
        FROM ({UNIT_COST}) u
        LEFT JOIN (
          SELECT bu.business_unit, to_date(v.created_at) AS change_date,
                 count(*) AS skill_versions_created, concat_ws(', ', collect_set(s.display_name)) AS skills_changed
          FROM {t('curated', 'fact_skill_version')} v
          JOIN {t('curated', 'dim_skill')} s ON s.skill_id = v.skill_id
          JOIN {t('reference', 'skill_business_unit')} bu ON bu.skill_display_name = s.display_name
          GROUP BY 1, 2) c ON c.business_unit = u.business_unit AND c.change_date = u.outcome_date""",
}

# Refresh rejects CTE (WITH) references as non-versioned relations, so definitions use inline subqueries.
# Views must be deterministic; if current_date() is rejected, drop the derived column (compute it at query time).
FALLBACKS = {"skill_inventory": SKILL_INVENTORY_BASE.format(extra="")}

opts = getResolvedOptions(sys.argv, ["recreate"]) if "--recreate" in sys.argv else {"recreate": ""}
for name in filter(None, opts["recreate"].split(",")):
    spark.sql(f"DROP MATERIALIZED VIEW IF EXISTS {t(NS, name)}")
    print(f"MV {name}: dropped for recreate")

existing = {r.tableName for r in spark.sql(f"SHOW TABLES IN {CAT}.{NS}").collect()}

# To change a view's definition, run the job with --recreate <view>[,<view>] (drop + create).
failures = []
for name, sql in MVS.items():
    full = t(NS, name)
    try:
        if name not in existing:
            try:
                spark.sql(f"CREATE MATERIALIZED VIEW {full} AS {sql}")
            except Exception as e:  # noqa: BLE001
                if name not in FALLBACKS:
                    raise
                print(f"MV {name}: primary definition rejected ({str(e)[:300]}); using fallback")
                spark.sql(f"CREATE MATERIALIZED VIEW {full} AS {FALLBACKS[name]}")
            print(f"MV {name}: created")
        else:
            spark.sql(f"REFRESH MATERIALIZED VIEW {full}")
            print(f"MV {name}: refreshed")
        props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {full}").collect()}
        rows = spark.sql(f"SELECT count(*) AS n FROM {full}").first().n
        print(f"MV {name}: rows={rows} lastRefreshType={props.get('lastRefreshType')}")
    except Exception as e:  # noqa: BLE001
        failures.append(name)
        print(f"MV {name}: FAILED {type(e).__name__}: {str(e).splitlines()[0][:800]}")

if failures:
    raise RuntimeError(f"materialized views failed: {failures}")
