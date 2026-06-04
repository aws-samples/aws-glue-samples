---
name: glue-09-10-migration
description: "Upgrade an AWS Glue ETL job from Glue version 0.9 or 1.0 to Glue 4.0. Runs the job against Glue 4.0, diagnoses failures against a known breaking-change catalogue, patches the job script and configuration, and iterates until the job succeeds or the fix attempt limit is exhausted. Jobs using the AWS Encryption SDK automatically run a two-step upgrade via Glue 2.0 first. Triggers on: upgrade glue job, migrate glue to 4.0, glue 0.9 to 4.0, glue 1.0 to 4.0, glue version upgrade, glue 4 migration, update glue version. Do NOT use for: creating new Glue jobs, jobs already running on Glue 4.0, or migrating from Glue 2.0/3.0 (different breaking-change set)."
argument-hint: "[job-name] [aws-region] [aws-cli-profile-name]"
owner_team: AWS Analytics
owner_cti: AWS/Analytics/Agent Skills
stages: [preprod]
version: 1
metadata:
  service: [glue]
  task: [upgrade, migrate]
  persona: [developer, data-engineer]
  workload: [data-analytics]
---

Upgrade an AWS Glue ETL job from Glue version 0.9 or 1.0 to Glue 4.0. Uses the run-fail-fix-rerun loop: update the job to Glue 4.0, run it, diagnose failures against a known breaking-change catalogue, patch the script or configuration, and repeat until the job succeeds or the fix attempt limit (5 iterations) is exhausted. If not able to upgrade, reverts the job and script to their original state.

## Reference Documentation

- [references/migration-notes.md](references/migration-notes.md) -- Failure categories, detection patterns, code-level fixes, dependency version reference, and migration path notes (fact-checked against official Glue and Spark migration guides)

## Workflow

### Prerequisites

Before starting, confirm:
1. **Job name** — the exact Glue job name to upgrade.
2. **AWS credentials** — `glue:GetJob/GetJobRun/GetJobRuns/StartJobRun/UpdateJob/BatchStopJobRun`, `s3:GetObject/s3:PutObject` on the script bucket, `logs:DescribeLogStreams/GetLogEvents/FilterLogEvents` on `/aws-glue/jobs/*`, `iam:GetRole` and `iam:PassRole` on the job execution role.
3. **Source Glue version** — call `glue:GetJob` and verify `GlueVersion` is `"0.9"` or `"1.0"`. Halt for any other version.
4. **S3 write permission** — verify script bucket is writable by attempting a `s3:PutObject` of a 0-byte probe to `script_location + ".write-check"`, then delete it. If the write fails, ask the user for the correct S3 location to use for storing the updated script.
5. **No active runs** — call `glue:GetJobRuns`; confirm no `RUNNING` or `STARTING` state.

MUST NOT proceed if any prerequisite cannot be confirmed.

### Constants

```
MAX_FIX_ATTEMPTS      = 5
MAX_TRANSIENT_RETRIES = 2
MAX_OOM_RETRIES       = 2
MAX_POLL_DURATION_SEC = 7200  # fallback if no successful run history available
POLL_TIMEOUT_FACTOR   = 1.5   # if history available: timeout = 1.5 × last successful run duration
POLL_INTERVAL_SEC     = 30
BACKUP_S3_SUFFIX      = ".glue40-upgrade-backup"
```

### Step 1: Gather Job Information

**Complexity routing:** All jobs follow the same pipeline. ML transforms are a hard blocker (HALT in step 1.5). Jobs using the Encryption SDK take a two-step path (Step 1A then main loop). All other jobs proceed directly through the run-fail-fix loop.

1.1. Call `glue:GetJob(JobName=<job_name>)`. Store as `original_job_definition`. Upload as JSON to `<script_s3_directory>/.glue40-upgrade-backup.original_job_definition.json` via `s3:PutObject` (where `<script_s3_directory>` is the S3 prefix up to the last `/` of `script_location`).
1.2. Extract: `script_location`, `original_glue_version`, `original_default_args`, `original_worker_type`, `original_num_workers`, `original_max_capacity`, `execution_role`, `python_version`.
1.2b. Call `glue:GetJobRuns` (max 5 runs). Find the most recent `SUCCEEDED` run and record its `ExecutionTime` (seconds) as `last_success_duration`. If no successful run exists, set `last_success_duration = null`.
1.3. Download job script from S3. Store as `original_script_text`.
1.4. **Backup script** — copy script to `script_location + BACKUP_S3_SUFFIX` using `s3:GetObject` (download) + `s3:PutObject` (upload to backup path). MUST NOT skip.
1.5. **Hard blocker checks (evaluate in this order):**
   - ML transforms — check if job script references `MLTransform` or `create_dynamic_frame` with `transform_type` set, OR if `Job.Command.Name` is `gluestreaming` with ML features. If detected → HALT immediately: `"BLOCKED: ML transforms not supported in Glue 4.0."` (takes precedence over all other checks)
   - **Encryption SDK** in args/script/SecurityConfiguration → do NOT halt. Run two-step upgrade: execute Step 1A below, then continue to Step 2.
   - Python 2 in config → record as preflight fix, do NOT halt.

#### Step 1A: Intermediate Upgrade to Glue 2.0 (Encryption SDK path only)

1A.1. Call `glue:UpdateJob`: set `GlueVersion="2.0"`, `Command.PythonVersion="3"`, `WorkerType="G.1X"`, `NumberOfWorkers` = the greater of 2 or the integer (truncated) value of the job's current `MaxCapacity`. Add `"--user-jars-first": "true"` to `DefaultArguments`. MUST NOT pass `MaxCapacity`.
1A.2. Initialize: `intermediate_fix_attempt=0`, `intermediate_fingerprints=set()`.
1A.3. Call `glue:StartJobRun`. Poll to completion (same 30s interval and 1.5× history timeout rules as Step 3).
1A.4. `SUCCEEDED` → continue to Step 2 (now upgrade to 4.0). `FAILED` → classify failure and apply fix using the same catalogue in `references/migration-notes.md`.
1A.5. Increment `intermediate_fix_attempt`. If `>= MAX_FIX_ATTEMPTS` → HALT: `"BLOCKED: Intermediate Glue 2.0 upgrade failed after 5 attempts. Revert and inspect manually."` → go to Step 7.
1A.6. Cycle detection: if fingerprint already seen → HALT → Step 7.
1A.7. Repeat from 1A.3 until SUCCEEDED or budget exhausted.

### Step 2: Prepare Job for Glue 4.0

2.1. Build `updated_args` from current job `DefaultArguments` (use post-Step-1A args if the Encryption SDK path was taken, otherwise use `original_default_args`). Apply pre-flight fixes:
   - Remove `maxCapacity` → set `WorkerType=G.1X`, `NumberOfWorkers` = the greater of 2 or the integer (truncated) value of the job's current `MaxCapacity`
   - Python 2 → set `Command.PythonVersion="3"`
   - Glue 0.9 → add `"--user-jars-first": "true"`
   - Remove all `--conf spark.yarn.*` and `--yarn-*` keys

2.2. Call `glue:UpdateJob` with `GlueVersion="4.0"`, updated worker type/count, `DefaultArguments=updated_args`, `Command.PythonVersion="3"`. MUST NOT pass `MaxCapacity`.

2.3. Initialize: `fix_attempt=0`, `transient_retries=0`, `oom_retries=0`, `applied_fixes=[]`, `failure_fingerprints=set()`.

### Step 3: Run the Job

3.1. Call `glue:StartJobRun`. Record `job_run_id` and `run_start_time`.
3.2. Poll `glue:GetJobRun` every 30s. Timeout threshold = `1.5 × last_success_duration` if known, else 7200s (2h). If elapsed > threshold: `glue:BatchStopJobRun` → treat as `TRANSIENT_INFRA`.
3.3. `SUCCEEDED` → Step 6. `TIMEOUT` → Step 4 (TRANSIENT_INFRA). `FAILED/ERROR` → Step 4.

### Step 4: Analyze Failure

4.1. Read `error_message` from `GetJobRun`.
4.2. Fetch last 500 log lines from `/aws-glue/jobs/error/<job_name>/<job_run_id>` via CloudWatch.
4.3. Extract primary exception from `Traceback`, `Caused by:`, `java.lang.`, etc.
4.4. Compute `failure_fingerprint = hash(exception_type + ":" + failing_symbol)`.
4.5. Cycle detection: if fingerprint already seen → HALT → Step 7.
4.6. Classify into failure category per `references/migration-notes.md`. Use FIRST match.

### Step 5: Apply Fix

5.1. Apply fix per `references/migration-notes.md`:
   - **Script fix**: re-fetch from S3, patch, upload via `s3:PutObject`.
   - **Config fix**: update `updated_args`, call `glue:UpdateJob`.
   - **Blocked**: HDFS_PATH (unmappable), HUDI_PRIMARY_KEY (unknown key), UNKNOWN_FAILURE → Step 7.
5.2. Increment `fix_attempt`. If `>= MAX_FIX_ATTEMPTS` → Step 7.
5.3. Go to Step 3.

### Step 6: Declare Success

Output:
```
UPGRADE RESULT: SUCCESS
Job: <job_name>  |  Original: <glue_version>  |  Target: 4.0
Fix Iterations: <n>
Script backup: <script_location>.glue40-upgrade-backup
Fixes Applied: [list]
Final Run ID: <id> | SUCCEEDED
```

### Step 7: Revert and Report

7.1. Call `glue:UpdateJob` with original `GlueVersion`, `DefaultArguments`, `WorkerType`, `NumberOfWorkers`, `MaxCapacity`.
7.2. Restore script: `s3:GetObject` from backup path (`script_location + BACKUP_S3_SUFFIX`) → `s3:PutObject` to original `script_location`.
7.3. Output:
```
UPGRADE RESULT: FAILED — REVERTED
Job: <job_name>
Halt Reason: <BUDGET_EXHAUSTED|CYCLE_DETECTED|TRANSIENT_INFRA|OOM_UNRESOLVABLE|HARD_BLOCKER|UNKNOWN_FAILURE>
Last Failure: <category> | <exception>
Fixes Attempted: [list]
Manual Actions Required: [list]
Revert Status: job definition REVERTED | script REVERTED from backup
```

## Convergence Criteria

| Condition | Action |
|---|---|
| `SUCCEEDED` | Declare success |
| `fix_attempt >= 5` | Budget exhausted. Revert. |
| Cycle detected | Same failure after fix. Revert. |
| `transient_retries > 2` | Persistent infra issue. Revert. |
| `oom_retries > 2` | Cannot scale up. Revert. |
| Poll > timeout | Job hung (1.5× last run duration, or 2h if no history). Stop run. |
| BLOCKED category | Hard blocker. Revert. |

## Tools Used

`glue:GetJob`, `glue:GetJobRuns`, `glue:UpdateJob`, `glue:StartJobRun`, `glue:GetJobRun`, `glue:BatchStopJobRun`, `s3:GetObject`, `s3:PutObject`, `logs:DescribeLogStreams`, `logs:GetLogEvents`, `logs:FilterLogEvents`, `iam:GetRole`, `iam:PassRole`
