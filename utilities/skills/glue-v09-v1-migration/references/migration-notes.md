# Reference: Glue 4.0 Migration — Failure Categories, Fixes, and Version Notes

Used by: `SKILL: Upgrade AWS Glue Job to Glue 4.0`

> **Fact-check status:** Verified against official sources. See inline `[VERIFIED]`, `[INFERRED]`, `[SEE EXTERNAL]` annotations.

## External Guides

- **AWS Glue Migration Guide (to Glue 4.0):** https://docs.aws.amazon.com/glue/latest/dg/migrating-version-40.html
- **Apache Spark SQL Migration Guide:** https://spark.apache.org/docs/latest/sql-migration-guide.html
- **Log4j 2 Migration Guide:** https://logging.apache.org/log4j/2.x/manual/migration.html

The agent MUST consult these guides when diagnosing unfamiliar failures.

---

## Failure Categories and Fixes

Classify failures by matching `raw_log_text` and `exception_line` against the detection patterns below. Use the FIRST matching category.

### PYTHON2_SYNTAX
**Detection:** `IllegalArgumentException` with message containing `"pythonVersion"` or `"Python 2"` at job start (JVM-level configuration rejection — Python 2 is rejected before the interpreter runs), OR `SyntaxError` in Python traceback if the job config was patched to Python 3 but script still has Python 2 syntax.
**Cause:** Python 2 is not supported with Spark 3.3.0. `"Any job requesting Python 2 in the job configuration will fail with an IllegalArgumentException."` [VERIFIED – Glue migration guide]
**Fix:**
- Update job config: `Command.PythonVersion = "3"` (most likely caught by preflight Step 2.1b; this is a fallback).
- Update script for Python 3 compatibility (general Python 2→3 migration — not Glue-specific documented behavior; refer to [Python 3 What's New](https://docs.python.org/3/whatsnew/3.0.html)):
  - `print x` → `print(x)`
  - `unicode(x)` → `str(x)`, `basestring` → `str`, `xrange` → `range`
  - `dict.iteritems()` → `dict.items()`, `dict.itervalues()` → `dict.values()`, `dict.has_key(k)` → `k in dict`
  - `except Type, e:` → `except Type as e:`
  - `exec statement` → `exec(statement)`
- If extensive Python 2 usage: `"WARNING: Extensive Python 2→3 porting required. Automated fixes applied common patterns; manual review strongly recommended."`

---

### PYTHON3_IMPORT
**Detection:** `ImportError` or `ModuleNotFoundError` referencing a Python 2-only or renamed module (e.g., `ConfigParser`, `cPickle`, `urllib2`, `urlparse`, `httplib`, `Queue`, `__builtin__`).
**Cause:** Python 2-only modules not available in Python 3.10. [General Python 3 migration — not Glue-specific documentation]
**Fix:**

| Python 2 import | Python 3 replacement |
|---|---|
| `import ConfigParser` | `import configparser` |
| `import cPickle` | `import pickle` |
| `import urllib2` | `import urllib.request, urllib.error` |
| `import urlparse` | `from urllib.parse import urlparse` |
| `from urlparse import urlparse` | `from urllib.parse import urlparse` |
| `import httplib` | `import http.client` |
| `import Queue` | `import queue` |
| `import __builtin__` | `import builtins` |

---

### SCALA_UNTYPED_UDF
**Detection:** `AnalysisException` or `IllegalArgumentException` referencing the two-argument form `udf(AnyRef, DataType)`.
**Cause:** `org.apache.spark.sql.functions.udf(AnyRef, DataType)` — the two-argument form — was disallowed by default starting in **Spark 3.0** (not 3.3.0). [VERIFIED – Spark SQL migration guide]
**Fix:**
```scala
// Before: two-argument form (removed in Spark 3.0+)
val myUdf = udf((x: AnyRef) => x.toString, StringType)

// After: typed UDF form
val myUdf = udf((x: String) => x)
```
**Temporary escape hatch — use ONLY if the UDF input types cannot be determined from script context and the typed rewrite fails:** Append ` --conf spark.sql.legacy.allowUntypedScalaUDF=true` to `updated_args["--conf"]` (if `"--conf"` key exists with value `<existing>`, use `<existing> --conf spark.sql.legacy.allowUntypedScalaUDF=true`; if key does not exist, set it to `spark.sql.legacy.allowUntypedScalaUDF=true`). Always emit this warning to the user: `"WARNING: spark.sql.legacy.allowUntypedScalaUDF=true is a temporary workaround — remove it before production use."`

---

### SCALA_BINARY_INCOMPATIBLE
**Detection:** `ClassNotFoundException`, `NoClassDefFoundError`, or `IncompatibleClassChangeError` referencing a class in a third-party package that is NOT an AWS SDK (`com.amazonaws.`), Spark built-in (`org.apache.spark.`), or bundled library (`com.fasterxml.jackson.`, `io.netty.`, `org.slf4j.`). If the failing class is in one of those bundled namespaces, use JAR_CLASSPATH_CONFLICT instead.
**Cause:** Glue 4.0 uses Scala 2.12. The upgrade from Scala 2.11 to 2.12 occurred at **Glue 3.0** (not 4.0) — users migrating from Glue 0.9/1.0/2.0 need to recompile. Users already on Glue 3.0 are unaffected by a Scala version change. `"Scala 2.12 is NOT backward compatible with Scala 2.11."` [VERIFIED – Glue migration guide]
**Fix:**
- Add `"--user-jars-first": "true"` to `updated_args` if not already set.
- Log: `"ACTION REQUIRED: Any third-party JARs compiled against Scala 2.11 must be replaced with Scala 2.12 builds. Automated fix is limited to classpath ordering only."`

---

### LOG4J_CONFIG
**Detection:** Stack trace from `org.apache.log4j` (NOT `org.apache.logging.log4j`), OR `ClassNotFoundException: org.apache.log4j.Logger`, OR script contains `import org.apache.log4j.`.
**Cause:** `"Log4j has been upgraded to Log4j2."` [VERIFIED – Glue migration guide]. Config file must be renamed; import paths change (see Log4j migration guide). [SEE EXTERNAL – not documented in Glue migration guide directly]
**Fix (config file):** `"ACTION REQUIRED: Rename any custom log4j.properties → log4j2.properties and reformat using Log4j2 properties syntax. See https://logging.apache.org/log4j/2.x/manual/migration.html"`
**Fix (script — general Log4j migration, not Glue-specific):**
- `import org.apache.log4j.Logger` → `import org.apache.logging.log4j.Logger`
- `import org.apache.log4j.LogManager` → `import org.apache.logging.log4j.LogManager`
- `import org.apache.log4j._` → `import org.apache.logging.log4j._`

---

### PARQUET_TIMESTAMP
**Detection:** Unexpected timestamp values in output OR error containing `"int96"`, `"DateTimeException"`, `"RebaseException"`, or `"spark.sql.legacy.parquet"`.
**Cause:** `"Starting with Spark 3.1, there was a change in the behavior of loading/saving timestamps from/to parquet files."` [VERIFIED – Glue migration guide, section on Parquet timestamp rebase]. The Proleptic Gregorian calendar replaced the hybrid Julian/Gregorian calendar as default.
**Fix:** Append to `updated_args["--conf"]`. Each config after the first must be prefixed with ` --conf `:
- If `"--conf"` key does not exist: set `updated_args["--conf"] = "spark.sql.legacy.parquet.int96RebaseModeInRead=LEGACY --conf spark.sql.legacy.parquet.int96RebaseModeInWrite=LEGACY --conf spark.sql.legacy.parquet.datetimeRebaseModeInRead=LEGACY --conf spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY"`
- If `"--conf"` key already exists with value `<existing>`: set `updated_args["--conf"] = "<existing> --conf spark.sql.legacy.parquet.int96RebaseModeInRead=LEGACY --conf spark.sql.legacy.parquet.int96RebaseModeInWrite=LEGACY --conf spark.sql.legacy.parquet.datetimeRebaseModeInRead=LEGACY --conf spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY"`
> Note: These `legacy`-prefixed config keys are Spark 3.x-specific. In Spark 4.0 they were renamed to non-legacy equivalents. [VERIFIED – Spark SQL migration guide]

---

### HDFS_PATH
**Detection:** Error message contains `"hdfs://"` OR `"No FileSystem for scheme: hdfs"`.
**Cause:** `"AWS Glue 4.0 does not have a Hadoop Distributed File System (HDFS)."` [VERIFIED – Glue migration guide]
**Fix:** Replace HDFS paths with an alternative supported storage or data source. Workloads using HDFS for intermediate or persistent storage should migrate to S3 or another supported source. The mapping MUST be provided by the user if not deterministic from context.
- If mapping cannot be determined: `"BLOCKED: Script references HDFS paths. Provide the target storage equivalent for each HDFS path and rerun."`

---

### JAR_CLASSPATH_CONFLICT
**Detection:** `NoSuchMethodError`, `NoClassDefFoundError`, or `IncompatibleClassChangeError` in a bundled Glue/Spark library (`org.apache.spark.`, `com.fasterxml.jackson.`, `io.netty.`, `org.slf4j.`) where the class is NOT from a user-supplied JAR and NOT in `com.amazonaws.` (see AWS_SDK_VERSION for those).
**Cause:** Extra JARs supplied with the job depend on an older version of a library upgraded in the Glue 4.0 runtime.
**Fix:** Set `updated_args["--user-jars-first"] = "true"` in `UpdateJob`.

---

### AWS_SDK_VERSION
**Detection:** `NoSuchMethodError` or `ClassNotFoundException` referencing a class or method specifically in `com.amazonaws.` (distinct from JAR_CLASSPATH_CONFLICT which covers other bundled libraries).
**Cause:** `"The AWS SDK provided in ETL jobs is now upgraded from 1.11 to 1.12."` [VERIFIED – Glue migration guide]. Specific API-level breaking changes are NOT documented in the Glue migration guide.
**Fix:**
- Log: `"ACTION REQUIRED: AWS SDK upgraded from 1.11 to 1.12. Verify specific API calls against the AWS SDK for Java changelog at https://github.com/aws/aws-sdk-java/blob/master/CHANGELOG.md. The Glue migration guide does not enumerate specific removed APIs."`
- Known common fixes (from AWS SDK changelog — not in Glue guide) [INFERRED]:
  - `new AmazonS3Client(credentials)` → `AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).build()`

---

### MONGODB_CONNECTOR
**Detection:** `MongoCommandException` OR `IllegalArgumentException` referencing `"uri"` as an unrecognized option.
**Cause:** `"Due to the connector upgrade, a few property names changed. For example, the URI property name changed to connection.uri."` [VERIFIED – Glue migration guide]
**Fix (script):** Replace `.option("uri", ...)` with `.option("connection.uri", ...)` everywhere.

---

### HUDI_PRIMARY_KEY
**Detection:** `HoodieException` containing `"primaryKey"` or `"preCombineField"` required, OR `IllegalArgumentException` in Hudi write path.
**Cause:** From Glue migration guide: `"Any Hudi table created before version 0.10.0 without a primaryKey needs to be recreated with a primaryKey field since version 0.10.0. Spark SQL with Hudi requires a primaryKey specified by tblproperties or options."` [VERIFIED]
**Fix (script):** For Spark SQL usage, add to Hudi table properties or write options:
```python
# Spark SQL DDL:
TBLPROPERTIES ('primaryKey' = '<primary_key_column>', 'preCombineField' = '<ts_column>')

# DataFrame write options (config keys from Hudi docs — not in Glue migration guide):
.option("hoodie.datasource.write.recordkey.field", "<primary_key_column>")  # [INFERRED from Hudi docs]
.option("hoodie.datasource.write.precombine.field", "<timestamp_or_sequence_column>")  # [INFERRED from Hudi docs]
```
**Note:** This requirement applies to (a) tables created before Hudi 0.10.0 without a primaryKey, and (b) Spark SQL usage. It is not a universal requirement for all Hudi tables. [VERIFIED]

---

### ICEBERG_DROP_TABLE
**Detection:** `DROP TABLE` in script AND job involves Iceberg tables, AND job succeeds but data is not deleted.
**Cause:** `"In Iceberg 1.0.0, DROP TABLE only removes the table from the catalog. To delete the table contents use DROP TABLE PURGE."` [VERIFIED – Glue migration guide]
**Fix (script):** Append `PURGE` to the end of any `DROP TABLE` statement that does not already include it. Example: `DROP TABLE my_table;` → `DROP TABLE my_table PURGE;`, `DROP TABLE IF EXISTS my_table;` → `DROP TABLE IF EXISTS my_table PURGE;`

---

### OOM_RESOURCE
**Detection:** `"OutOfMemoryError"`, `"GC overhead limit exceeded"`, `"ExecutorLostFailure"` with `"OOM"` — WITH a user-code traceback present.
**Cause:** Job requires more memory than current `WorkerType` provides. Distinct from `TRANSIENT_INFRA` (no user-code traceback). [INFERRED — not specifically documented as a Glue 4.0 migration issue]
**Fix:** Scale up worker type one tier: `Standard` → `G.1X` → `G.2X` → `G.4X` → `G.8X`. `Z.2X` (memory-optimized) is an alternative at the G.4X/G.8X tier for memory-pressure failures but requires explicit user confirmation due to higher cost. If already at `G.8X`, increase `NumberOfWorkers` using `ceil(current_NumberOfWorkers * 1.5)` (capping the total at 50 workers — a skill-imposed budget cap, not an AWS service limit). Increment `oom_retries` — does NOT consume `fix_attempt` budget.

---

### TRANSIENT_INFRA
**Detection:** `"InternalFailure"`, `"ResourceNotAvailable"`, `"ServiceUnavailableException"`, `"ThrottlingException"`, `"RequestExpired"` — AND no user-code traceback present.
**Cause:** Transient infrastructure issue. (`OutOfMemoryError` WITH user-code traceback → `OOM_RESOURCE`, not this.)
**Fix:** Retry without modification. Increment `transient_retries`. If `transient_retries > MAX_TRANSIENT_RETRIES` (2), go to Step 7.

---

### UNKNOWN_FAILURE
**Detection:** None of the above categories matched.
**Investigation pass before giving up:**
1. Re-scan `raw_log_text` for the LAST `Caused by:` block — initial match may have been a secondary exception.
2. Search for Glue error codes (`GlueException`, `InvalidInputException`, `EntityNotFoundException`) and extract the specific resource.
3. If the exception mentions a class or method, check the Dependency Version Reference below for the library name.
4. Check if `AnalysisException` could be related to a type coercion change per the Spark SQL migration guide.
5. If no classification found: record all findings in the assessment report and go to Step 7.

---

## Migration Path Notes (Glue 0.9/1.0 → 4.0)

[VERIFIED unless noted]

- **Spark:** 2.2.1 (0.9) / 2.4.3 (1.0) → 3.3.0 — all cumulative breaking changes from Spark 2.3 through 3.3 apply.
- **Python:** 2.7 (Glue 0.9) / 2.7 or 3.6 (Glue 1.0) → 3.10 — Python 2 jobs fail immediately with `IllegalArgumentException`.
- **Scala:** 2.11 → 2.12 — binary-incompatible. Note: Glue 3.0 already used Scala 2.12; this is a new breaking change only for users on Glue 0.9/1.0/2.0.
- **No HDFS:** Glue 4.0 does not have HDFS. Migrate HDFS-dependent workloads to supported alternatives.
- **No YARN:** YARN-related settings should be removed (silently ignored in Glue 4.0).
- **AWS Encryption SDK 1.x → 2.x:** Cannot migrate directly from Glue 0.9/1.0 to 4.0 when encryption is used. Required path: migrate to Glue 2.0/3.0 first, **run the job at least once** at that version (the bridge version), then migrate to 4.0. Running once is required — simply redeploying is not sufficient.
- **`maxCapacity` removed:** Specify `numberOfWorkers` + `workerType` instead. (Relevant for Glue 0.9/1.0 → 4.0 migration paths only.)
- **Log4j 1.x → 2.x:** Rename `log4j.properties` → `log4j2.properties` and reformat.
- **AWS SDK 1.11 → 1.12:** Most APIs unchanged; see AWS SDK changelog for specific removed APIs.

---

## Dependency Version Reference

[VERIFIED from official Glue dependency table except where noted]

| Dependency | Glue 0.9 | Glue 1.0 | Glue 4.0 |
|---|---|---|---|
| Spark | 2.2.1 ✓ | 2.4.3 ✓ | 3.3.0-amzn-1 ✓ |
| Hadoop | 2.8.3-amzn-1 [INFERRED] | 2.8.5-amzn-1 ✓ | 3.3.3-amzn-0 ✓ |
| Scala | 2.11 ✓ | 2.11 ✓ | 2.12 ✓ |
| Python | 2.7 ✓ | 2.7 / 3.6 ✓ | 3.10 ✓ |
| AWS SDK | 1.11 [INFERRED] | 1.11 [INFERRED] | 1.12 ✓ |
| Log4j | 1.x [INFERRED] | 1.x [INFERRED] | 2.x ✓ |
| AWS Encryption SDK | 1.x [INFERRED] | 1.x [INFERRED] | 2.x ✓ |
| Arrow | — | 0.10.0 | 7.0.0 ✓ |
| Jackson | 2.6.x [INFERRED] | 2.7.x [INFERRED] | 2.13.3 ✓ |
| Hudi | N/A | N/A | 0.12.1 ✓ |
| Delta Lake | N/A | N/A | 2.1.0 ✓ |
| Iceberg | N/A | N/A | 1.0.0 ✓ |
| MongoDB JDBC | 2.0.0 [INFERRED] | 2.0.0 [INFERRED] | 4.7.2 ✓ |
| MongoDB connector | — | — | 10.0.4 ✓ |

> ✓ = confirmed from official Glue migration guide. [INFERRED] = not explicitly stated in official docs; derived from migration prose context.
