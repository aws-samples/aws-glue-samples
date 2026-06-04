# Glue 0.9 / 1.0 → 4.0 Migration Skill

> **Skill file:** [`SKILL.md`](./SKILL.md) — the agent reads this file when invoked. The README is for humans; `SKILL.md` is the contract the AI agent follows.

An AI-agent skill that upgrades an AWS Glue ETL job from **GlueVersion 0.9 or 1.0** to **GlueVersion 4.0** using a `run → fail → fix → rerun` loop. The skill reads the existing job, runs it on 4.0, classifies failures against a 14-category breaking-change catalogue, patches the script or configuration, and iterates up to 5 attempts. If it cannot converge, it reverts the job and the script to their original state.

---

## At a glance

| | |
|---|---|
| **Source versions** | Glue **0.9** and **1.0** only (2.0 / 3.0 / 4.0 → halt) |
| **Target version** | Glue **4.0** (Spark 3.3.0, Python 3.10, Scala 2.12, Hadoop 3.3.3) |
| **Strategy** | Run → fail → fix → rerun, up to **5 fix iterations** |
| **On failure** | Reverts the job and the script to their original state |
| **Skill file** | [`SKILL.md`](./SKILL.md) |
| **Failure catalogue** | [`references/migration-notes.md`](./references/migration-notes.md) |

---

## Important constraints — read before running

These are intentional choices and limits in the current skill. Review each before pointing at a real workload.

1. **Workers reset to `G.1X`.** `MaxCapacity` is removed; `NumberOfWorkers = max(2, floor(MaxCapacity))`. Re-tune after upgrade if your job was on a specific DPU count or memory-optimized worker.

2. **No data validation.** `SUCCEEDED` means the Spark job didn't throw — not that your output is correct. Run your own row-count, schema, and business-rule checks before promoting. Critical across the Spark 2.x → 3.3 jump (timestamp rebase, decimal precision, division-by-zero, type coercion).

3. **Capacity translation is mechanical.** 1:1 `MaxCapacity` → `NumberOfWorkers` (floor 2) under `G.1X`. Glue 4.0 reserves one worker as driver, so effective executor count may be lower than your original — resize after.

4. **Pre-prod first. Always.** The skill mutates a live Glue job and triggers real (billed) runs, and overwrites your script in S3 (backup at `<script>.glue40-upgrade-backup`). Do not point at prod until verified end-to-end.

5. **No auth or governance checks.** Trusts whatever local credentials you give it. Does not honor change-management policy, deployment windows, or active-traffic detection.

6. **Auto-scaling not enabled.** Worker count is fixed at preflight. Opt in manually after upgrade if needed.

---

## How it works

In order:

1. **Backup** — copies the original job script to `<script>.glue40-upgrade-backup` in S3, and writes a JSON snapshot of the original `GetJob` response to `.glue40-upgrade-backup.original_job_definition.json` alongside it.
2. **Preflight fixes** —
   - Strips `MaxCapacity`; sets `WorkerType=G.1X`, `NumberOfWorkers=max(2, floor(MaxCapacity))`.
   - Sets `PythonVersion=3`.
   - For Glue 0.9 sources, adds `--user-jars-first: true`.
   - Removes `--conf spark.yarn.*` and `--yarn-*` keys (silently ignored on 4.0).
3. **`UpdateJob`** to `GlueVersion=4.0` with the corrected configuration.
4. **`StartJobRun`** and poll until terminal.
5. **Diagnose** any failure against the [14-category catalogue](./references/migration-notes.md) (Python 2 syntax, Log4j 1.x, Parquet timestamp rebase, Scala 2.11 binary incompat, MongoDB connector, Hudi primary key, Iceberg drop-table, …).
6. **Patch and rerun.** Up to 5 fix iterations.
7. **Declare success or revert** — restore the script and job definition from backup if no convergence.

> **Encryption SDK path.** If the job uses the AWS Encryption SDK, the skill upgrades to Glue 2.0 first, requires at least one successful run there, then upgrades to 4.0. Direct 0.9/1.0 → 4.0 is not possible when AES SDK 1.x → 2.x is in play.

---

## Prerequisites

### Install

The skill ships in this repo:

```bash
git clone https://github.com/aws-samples/aws-glue-samples.git
```

Then point your AI agent (Claude Code, Kiro CLI, Codex CLI) at the skill file:

```text
use the skill at <local-path>/aws-glue-samples/utilities/skills/glue-09-10-migration/SKILL.md
to upgrade my Glue 0.9 job <job-name> in <region>.
use aws profile <profile-name> for any aws call
```

Substitute `Glue 1.0` and the matching job name for 1.0 upgrades.

### IAM permissions on the local credential

| Service | Actions |
|---|---|
| Glue | `GetJob`, `GetJobRun`, `GetJobRuns`, `StartJobRun`, `UpdateJob`, `BatchStopJobRun` |
| S3 (script bucket) | `GetObject`, `PutObject`, `CopyObject` |
| CloudWatch Logs | `DescribeLogStreams`, `GetLogEvents`, `FilterLogEvents` on `/aws-glue/jobs/*` |
| IAM | `GetRole`, `PassRole` on the job's execution role |

### Pre-flight checklist (you, before running)

- [ ] Running in a **pre-prod / non-production** account.
- [ ] Recent backup or snapshot of any output the job has been writing.
- [ ] You have a way to **validate** the upgraded job's output (row counts, schema diff, business-rule checks).
- [ ] Source `GlueVersion` is exactly **0.9** or **1.0**.
- [ ] You have read the **Important constraints** section above.

---

## Example: Python 2 syntax error in the job script

**Job:** `ember-e2e-glue09-two-issues` (Glue 0.9, Python 2)
**Breaking change:** Python 2 `print "..."` statements raise `SyntaxError` on 4.0.

#### Step 1 — Preflight

```text
Hard blocker checks:
  Encryption SDK: Not present
  ML transforms: None
  Python 2: Yes -> will be fixed in preflight

Backing up script -> ember-e2e-two-issues.py.glue40-upgrade-backup OK

Pre-flight fixes applied:
  MaxCapacity=2          -> WorkerType=G.1X, NumberOfWorkers=2
  PythonVersion 2        -> 3
  --user-jars-first      -> true
```

#### Iteration 1 — FAILED

```text
Error:    SyntaxError: Missing parentheses in call to 'print'.
Category: PYTHON2_SYNTAX
Fix:      print "..."  ->  print(...)   on lines 21 and 51
```

#### Iteration 2 — SUCCEEDED

```text
UPGRADE RESULT: SUCCESS
Job:             ember-e2e-glue09-two-issues
Original:        0.9      Target: 4.0
Fix Iterations:  1
Fixes Applied:
  PRE-FLIGHT       PythonVersion 2->3, MaxCapacity->G.1X/2 workers, --user-jars-first
  PYTHON2_SYNTAX   print statements -> print() calls (lines 21, 51)
Final Run ID:    jr_78a98813...   SUCCEEDED
```

> **Note:** The Log4j 1.x access via `sc._jvm.org.apache.log4j` did not fail — Glue 4.0 retains a JVM-level compatibility shim for that pattern via Py4J. PYTHON2_SYNTAX was the only blocking issue.

Full transcript: <https://paste.amazon.com/show/yxiaoru/1780443222>

---

## Example: custom JAR with Scala 2.11 binary incompatibility

**Job:** `ember-e2e-glue09-jar-error` (Glue 0.9, Python 2)
**Extra JAR:** `ember-sales-aggregator-glue09_2.11.jar` — a custom Scala library compiled against Scala 2.11 + Spark 2.4. The job calls `com.example.glue.SalesAggregator.aggregateByRegion()` from this JAR. The PySpark script itself is clean — the breaking change lives entirely inside the JAR.

#### Iteration 1 — FAILED

```text
Error:    scala.Predef$.refArrayOps([Ljava/lang/Object;)Lscala/collection/mutable/ArrayOps;
Category: SCALA_BINARY_INCOMPATIBLE
Cause:    JAR compiled for Scala 2.11. Glue 4.0 uses Spark 3.3 / Scala 2.12 -> ABI mismatch.
          PySpark script has no fix available.
```

The agent recognised the failure was inside the JAR, not the script. Instead of reverting:

```text
1. Downloaded ember-sales-aggregator-glue09_2.11.jar from S3
2. Decompiled bytecode (javap) to reconstruct the class structure
3. Rewrote SalesAggregator.scala for Scala 2.12 / Spark 3.3:
     SQLContext (removed in Spark 3.0)  ->  SparkSession.builder().getOrCreate()
     JavaConversions (deprecated)       ->  JavaConverters
     Removed Scala 2.11-era implicits   (CanBuildFrom, refArrayOps)
4. Downloaded Scala 2.12.15 compiler + Spark 3.3.0 jars
5. Compiled  ->  ember-sales-aggregator-glue40_2.12.jar
6. Uploaded new JAR to S3
7. UpdateJob: replaced --extra-jars reference with new JAR
```

#### Iteration 2 — SUCCEEDED

```text
UPGRADE RESULT: SUCCESS
Job:             ember-e2e-glue09-jar-error
Original:        0.9      Target: 4.0
Fix Iterations:  1
Fixes Applied:
  PRE-FLIGHT                    PythonVersion 2->3, MaxCapacity->G.1X/2 workers, --user-jars-first
  SCALA_BINARY_INCOMPATIBLE     Decompiled Scala 2.11 JAR, rewrote source for
                                Scala 2.12/Spark 3.3.0, recompiled as
                                ember-sales-aggregator-glue40_2.12.jar
Final Run ID:    jr_cfbc8054...   SUCCEEDED
```

> **Note:** The agent went beyond script patching — it reconstructed and recompiled a custom binary dependency the customer had no Scala 2.12 source for. The customer got a working Glue 4.0 job without locating the original Scala source or setting up a build environment.

Full transcript: <https://paste.amazon.com/show/yxiaoru/1780442096>

---

## Batch / headless upgrade at scale

The skill is per-job and stateless — read the job, upgrade it, exit — so it parallelizes naturally.

### Option 1 — Claude Code workflow orchestration

Claude Code supports multi-agent [workflows](https://code.claude.com/docs/en/workflows) where you describe the fan-out pattern and it manages concurrency. Example prompt:

> *"Use a workflow to upgrade all Glue jobs in account 123456789012 that are on version 0.9 or 1.0. Use the glue-09-10-migration skill for each. Run up to 10 in parallel."*

Claude lists matching jobs via `glue:ListJobs`, filters by `GlueVersion`, spawns parallel sub-agents, and collects a per-job summary. No wrapper script needed.

### Option 2 — Headless per-job sessions

```bash
# Claude Code (headless)
claude --print "Upgrade Glue job 'my-etl-job' in us-east-1 to Glue 4.0 using the glue-09-10-migration skill"

# Kiro CLI (headless)
kiro-cli chat "Upgrade Glue job 'my-etl-job' in us-east-1 to Glue 4.0 using the glue-09-10-migration skill" \
  --no-interactive --trust-all-tools

# Codex CLI (headless)
codex "Upgrade Glue job 'my-etl-job' in us-east-1 to Glue 4.0 using the glue-09-10-migration skill"
```

Sessions run independently — one job's failure does not block others. Each session emits a structured result block that can be collected and reviewed.

---

## Out of scope

- **Output data validation.** Run your own checks.
- **Auto-scaling.** Worker count is fixed at preflight.
- **Glue 2.0 / 3.0 source jobs.** Different breaking-change set; the skill halts.
- **`pythonshell` and streaming jobs.** The catalogue is built for `glueetl`.
- **ML transforms.** Hard blocker; the skill halts.
- **Full revert fidelity.** Connections, SecurityConfiguration, MaxRetries, Timeout, etc. should be reviewed after revert.
- **Change management.** No deployment-window or stakeholder coordination.

---

## Trigger phrases

The skill can be invoked by an AI agent on any of:

- `upgrade glue job to 4.0`
- `migrate my glue 0.9 job to glue 4.0`
- `glue 1.0 to 4.0 migration`
- `update glue version`
- `glue 4 migration`

---

## Reference

| | |
|---|---|
| Skill source | [`SKILL.md`](./SKILL.md) |
| Failure catalogue | [`references/migration-notes.md`](./references/migration-notes.md) |
| AWS Glue 4.0 migration guide | <https://docs.aws.amazon.com/glue/latest/dg/migrating-version-40.html> |
| Apache Spark SQL migration guide | <https://spark.apache.org/docs/latest/sql-migration-guide.html> |
| Log4j 2 migration guide | <https://logging.apache.org/log4j/2.x/manual/migration.html> |

---

## Final reminder

The skill mutates real Glue jobs and runs them. **Use a pre-prod environment first.** Keep the backup files (`<script>.glue40-upgrade-backup` and `.glue40-upgrade-backup.original_job_definition.json`) until you have validated the upgraded job's output against your data-quality criteria. Promote to production only after that validation passes.
