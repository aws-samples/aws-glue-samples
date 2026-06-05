# Glue Dev Endpoint → Interactive Session Migration Skill

> **Skill file:** [`SKILL.md`](./SKILL.md) — the agent reads this file when invoked. The README is for humans; `SKILL.md` is the contract the AI agent follows.

An AI-agent skill that moves a developer off a legacy AWS Glue **development endpoint** (Glue 0.9/1.0, no console since March 2023, billed continuously, 10–15 min startup) onto a Glue **interactive session** (Glue 2.0+, sub-minute startup, configurable idle timeout, console support). It follows the [official AWS migration checklist](https://docs.aws.amazon.com/glue/latest/dg/development-migration-checklist.html).

---

## At a glance

| | |
|---|---|
| **Source** | Glue **dev endpoint** (Glue 0.9/1.0) |
| **Target** | Glue **interactive session** (Glue 2.0+, default 4.0) |
| **Strategy** | Inventory → route → provision → validate → confirm → delete |
| **Reversibility** | Additive and reversible until the final step (dev endpoint kept until user confirms deletion) |
| **Skill file** | [`SKILL.md`](./SKILL.md) |
| **Config mapping & access-method routing** | [`references/migration-mapping.md`](./references/migration-mapping.md) |

The skill:
- Inventories the dev endpoint config via `glue:GetDevEndpoint` (role, version, workers, VPC, args, extra libs/jars).
- Routes on **access method** (notebook / IDE / REPL / SSH) to pick the right session interface, per the official checklist.
- Routes on **code compatibility** — interactive sessions cannot run Glue 0.9/1.0, so if the code uses 0.9/1.0-specific behavior (HDFS, YARN, Spark 2.x, Python 2) it delegates the code upgrade to the [`glue-v09-v1-migration`](../glue-v09-v1-migration/) skill first.
- Sets up the **two-principal IAM** model (client principal + runtime role).
- Provisions an equivalent interactive session and **validates** by running the developer's own Spark/PySpark code, comparing output to the dev endpoint.
- Deletes the dev endpoint **only after** validation passes and the user explicitly confirms — and backs the dev endpoint definition up to S3 first.

The migration is **additive and reversible until the final step**: the session is created and validated while the dev endpoint is still running. The dev endpoint — the rollback path — is deleted last, on explicit confirmation.

No console hunting for a feature that was removed in 2023. No 24/7 endpoint billing carried over. No silent code breakage on the version jump.

---

## Important constraints — read before running

1. **Interactive sessions cannot run Glue 0.9/1.0.** If the dev endpoint's code uses HDFS paths, YARN configs, Python 2 syntax, or Spark 2.x semantics, the code MUST be upgraded via the [`glue-v09-v1-migration`](../glue-v09-v1-migration/) skill before provisioning the session.

2. **Two IAM principals.** The runtime role (passed to `CreateSession`) is not enough. The client principal (the user/role calling the API or running the notebook) ALSO needs session API permissions plus `iam:PassRole` on the runtime role. Missing `iam:PassRole` on the client is the #1 setup failure.

3. **Always set an idle timeout.** Dev endpoints never timed out and billed 24/7. A session with no idle timeout reintroduces that cost. The skill defaults to 30 minutes.

4. **VPC access changes shape.** Raw `SubnetId` + `SecurityGroupIds` become a named Glue **connection** referenced by `%connections` / `--connections`. There is no raw-subnet session arg.

5. **No SSH successor.** If the developer relied on SSH into the dev endpoint, there is no session equivalent — the skill routes them to the Glue Docker image for local development.

6. **Decommission is destructive and last.** The skill never deletes the dev endpoint before the session is validated AND the user confirms. The dev endpoint is the only rollback path.

7. **The dev endpoint definition is backed up to S3 before deletion.** A deleted dev endpoint cannot be recovered, so the full `GetDevEndpoint` JSON is persisted to S3 first. The skill MUST NOT skip this step, even when the user says "just delete it."

---

## How it works

In order:

1. **Inventory** — `glue:GetDevEndpoint`; record `RoleArn`, `GlueVersion`, `WorkerType`, `NumberOfWorkers`, `SubnetId`, `SecurityGroupIds`, `Arguments`, `ExtraPythonLibsS3Path`, `ExtraJarsS3Path`.
2. **Route on access method** — notebook → Glue Studio notebook; IDE → IDE integration; REPL → local `aws-glue-sessions`; SSH → Glue Docker image (no direct session equivalent).
3. **Route on code compatibility** — if the code is 0.9/1.0-specific, delegate to `glue-v09-v1-migration` before provisioning.
4. **IAM check** — verify the runtime role's trust policy and the client principal's `iam:PassRole` on it.
5. **Provision the session** — map dev endpoint fields to session magics or `CreateSession` flags. Default `--glue-version 4.0`, same `WorkerType`/`NumberOfWorkers` as the dev endpoint, `--idle-timeout 30`, Python 3.
6. **Validate equivalence** — submit the developer's own Spark/PySpark code via `RunStatement`, poll `GetStatement`, compare output to the dev endpoint. Spark and Python version differences are expected.
7. **Classify and fix on failure** — `CODE_COMPATIBILITY` (delegate to `glue-v09-v1-migration`, validate the patch in this session), `MIGRATION_CONFIG_GAP` (add the missing connection, lib, jar, or catalog arg, recreate the session), or `TRANSIENT` (retry once). `MAX_FIX_ATTEMPTS = 5` with cycle detection.
8. **Confirm and decommission** — present the validation result, ask the user a direct yes/no question, back up the dev endpoint definition to S3, then `glue:DeleteDevEndpoint`.

If validation cannot be made to pass, the skill stops the session, leaves the dev endpoint running, and reports the failure class with manual next steps. The rollback stays intact.

---

## Prerequisites

### Install

The skill ships in this repo:

```bash
git clone https://github.com/aws-samples/aws-glue-samples.git
```

Then point your AI agent (Claude Code, Kiro CLI, Codex CLI) at the skill file:

```text
use the skill at <local-path>/aws-glue-samples/utilities/skills/migrate-glue-devendpoint-to-interactive-sessions/SKILL.md
to migrate my Glue dev endpoint <name> in <region> to an interactive session.
use aws profile <profile-name> for any aws call.
```

### IAM permissions on the local credential (the client principal)

| Service | Actions |
|---|---|
| Glue (dev endpoint) | `GetDevEndpoint`, `DeleteDevEndpoint` |
| Glue (sessions) | `CreateSession`, `GetSession`, `RunStatement`, `GetStatement`, `ListStatements`, `StopSession`, `DeleteSession`, `ListSessions` |
| Glue (connections, optional) | `CreateConnection`, `GetConnection` (only if the dev endpoint had a `SubnetId`/`SecurityGroupIds` to translate) |
| IAM | `PassRole` on the **runtime role** |
| S3 (backup bucket) | `PutObject` for the dev endpoint definition backup |

`AWSGlueConsoleFullAccess` covers the session APIs; the explicit `iam:PassRole` statement on the runtime role is still required.

### Pre-flight checklist (you, before running)

- [ ] Running in a **pre-prod / non-production** account.
- [ ] You have the dev endpoint's name, region, and an AWS CLI profile with the permissions above.
- [ ] A bucket the runtime role can write to is available for the dev endpoint definition backup (or you're ready to provide one when asked).
- [ ] You have a reproducible piece of Spark/PySpark code from the dev endpoint to validate equivalence against.
- [ ] You have read the **Important constraints** section above.

---

## Example: SSH/REPL dev endpoint with version-agnostic PySpark

This is a **real, verbatim transcript** of Claude running the skill headlessly (`claude -p ... --output-format stream-json`) against a live Glue 1.0 dev endpoint and a live Glue 4.0 interactive session in account `783661989716` / `us-east-1`.

**Source:** `de2is-poc-endpoint` (Glue 1.0, G.1X × 2, public/no-VPC, Python 3, Data Catalog enabled)
**Code:** version-agnostic PySpark — `SparkContext` + `GlueContext`, a DataFrame, a Spark SQL aggregation, a DynamicFrame round-trip, an S3 parquet write+read. No HDFS, no YARN configs, no Python 2 syntax.

#### Step 1 — Read the skill, inventory the dev endpoint, route

The agent read `SKILL.md`, the user's script, and `references/migration-mapping.md`, called `glue:GetDevEndpoint`, then stated its routing decisions:

```text
Dev endpoint inventory:
  Glue version:   1.0 (Spark 2.4, Python 3.6)
  Role:           arn:aws:iam::783661989716:role/GlueTestRole
  Workers:        G.1X x 2
  VPC:            None (empty SecurityGroupIds, no SubnetId)
  Arguments:      --enable-glue-datacatalog, Python 3
  Access:         SSH/REPL
  Extra libs/jars: None

Routing decisions:
  Access method:    SSH — no direct session equivalent; validate headlessly via
                    CreateSession/RunStatement (per user request).
  Code compatibility: Standard PySpark + GlueContext (DataFrames, Spark SQL,
                    DynamicFrame, S3 I/O) — version-agnostic. No upgrade needed.
```

#### Step 2 — Verify IAM, capture the baseline on the dev endpoint

```text
ssh -i /tmp/de2is_key glue@ec2-54-211-193-20.compute-1.amazonaws.com \
  'cat /tmp/de2is_baseline.py | gluepyspark'

SPARK_VERSION: 2.4.3
PYTHON_VERSION: 3.6.12
ROW_COUNT: 5
AGG_ROW: eng 3 300
AGG_ROW: sales 2 150
DYF_COUNT: 5
DYF_ROUNDTRIP_COUNT: 5
S3_REREAD_COUNT: 2
```

#### Step 3 — Provision the interactive session

The agent built the `CreateSession` call by mapping the dev endpoint fields — including carrying `--enable-glue-datacatalog` into `--default-arguments` and adding the idle timeout the dev endpoint never had:

```bash
aws glue create-session \
  --id de2is-poc-session \
  --role 'arn:aws:iam::783661989716:role/GlueTestRole' \
  --command '{"Name":"glueetl","PythonVersion":"3"}' \
  --glue-version 4.0 \
  --worker-type G.1X \
  --number-of-workers 2 \
  --idle-timeout 30 \
  --default-arguments '{"--enable-glue-datacatalog":"true"}' \
  --region us-east-1 --profile 783661989716-Admin
```

Session was `READY` in ~30 seconds.

#### Step 4 — Validate equivalence

The agent submitted the same script via `RunStatement` and polled `GetStatement`. Raw session output (`Output.Data.TextPlain`):

```text
=== MARKER:START ===
SPARK_VERSION: 3.3.0-amzn-1
PYTHON_VERSION: 3.10.2
ROW_COUNT: 5
=== MARKER:AGG ===
AGG_ROW: eng 3 300
AGG_ROW: sales 2 150
DYF_COUNT: 5
DYF_ROUNDTRIP_COUNT: 5
S3_REREAD_COUNT: 2
=== MARKER:DONE ===
/opt/amazon/spark/python/lib/pyspark.zip/pyspark/sql/dataframe.py:127:
  UserWarning: DataFrame constructor is internal. Do not directly use it.
```

Side-by-side comparison:

| Marker | Dev Endpoint (Glue 1.0) | Interactive Session (Glue 4.0) | Match? |
|---|---|---|---|
| `SPARK_VERSION` | `2.4.3` | `3.3.0-amzn-1` | expected diff |
| `PYTHON_VERSION` | `3.6.12` | `3.10.2` | expected diff |
| `ROW_COUNT` | 5 | 5 | yes |
| `AGG_ROW eng` | `3, 300` | `3, 300` | yes |
| `AGG_ROW sales` | `2, 150` | `2, 150` | yes |
| `DYF_ROUNDTRIP_COUNT` | 5 | 5 | yes |
| `S3_REREAD_COUNT` | 2 | 2 | yes |

The benign `UserWarning: DataFrame constructor is internal` on the session output is the expected `DynamicFrame.fromDF` warning on Spark 3.x (noted in the skill's Gotchas section) — not an error.

#### Step 5 — Stop before decommission (per user instruction)

```text
MIGRATION RESULT: VALIDATED (pending your decommission)
Dev endpoint:        de2is-poc-endpoint  (Glue 1.0, Spark 2.4.3)  -> KEPT (per your request)
Interactive session: de2is-poc-session   (Glue 4.0, Spark 3.3.0)  -> STOPPED
Access method:       SSH/REPL -> headless CLI (CreateSession/RunStatement)
Code upgrade:        none (script was version-agnostic)
Validation:          PASSED (all functional output matched)
Idle timeout:        30 min
Runtime role:        arn:aws:iam::783661989716:role/GlueTestRole (reused)
```

The dev endpoint was left running. The session was stopped to avoid billing. Deletion of the dev endpoint is the user's call (`aws glue delete-dev-endpoint ...`).

---

## Scenario the skill routes away: 0.9/1.0-specific code

If the dev endpoint's code depends on Glue 0.9/1.0 behavior (`hdfs://` paths, Python 2 `print` statements, YARN configs, Spark 2.x semantics) — interactive sessions, which run Glue 2.0+ only, cannot run it as-is. The skill's Step 1.4 treats the code upgrade as a non-optional prerequisite and delegates it to the [`glue-v09-v1-migration`](../glue-v09-v1-migration/) skill (HDFS → S3, Python 2 → 3, and the rest of its 14-category Spark 2.x → 3.x catalogue) before provisioning the session.

The two skills compose: `glue-v09-v1-migration` makes the *code* Glue 3.0+-ready; this skill moves the *dev environment* off the endpoint onto a session.

---

## Config mapping (what the skill carries over)

| Dev endpoint | Interactive session |
|---|---|
| `RoleArn` | `--role` / `%iam_role` (runtime role — reuse it) |
| `GlueVersion` 0.9/1.0 | `--glue-version 4.0` (never 0.9/1.0) |
| `WorkerType` / `NumberOfWorkers` | `--worker-type` / `--number-of-workers` |
| `SubnetId` + `SecurityGroupIds` | a Glue **connection** via `%connections` |
| `ExtraPythonLibsS3Path` | `%additional_python_modules` (C-extension libs now OK) |
| `ExtraJarsS3Path` | `%extra_jars` |
| `--enable-glue-datacatalog` | session `--default-arguments` / `%%configure` |
| SSH access | **no equivalent** — Glue Docker image for local dev |
| (never times out) | `--idle-timeout` — **always set** |

Full mapping, magics quick reference, IAM two-principal policies, and code-compatibility notes: [`references/migration-mapping.md`](./references/migration-mapping.md).

---

## Batch / headless migration at scale

The skill is per-dev-endpoint and stateless — it inventories one endpoint, migrates it, validates, and exits. This makes it naturally parallelizable.

### Option 1 — Claude Code workflow orchestration

Claude Code supports multi-agent [workflows](https://code.claude.com/docs/en/workflows) where you describe the fan-out pattern and it manages concurrency. Example prompt:

> *"Use a workflow to migrate all Glue dev endpoints in account 123456789012 to interactive sessions. Use the migrate-glue-devendpoint-to-interactive-sessions skill for each. Run up to 5 in parallel."*

Claude lists endpoints via `glue:ListDevEndpoints`, spawns parallel sub-agents each running the skill on one endpoint, and collects a summary (migrated / needs code upgrade / blocked). Because decommission requires explicit confirmation, the fan-out can be run in a validate-only mode that stops before deletion for batch review.

### Option 2 — Headless per-endpoint sessions

```bash
# Claude Code (headless)
claude --print "Migrate Glue dev endpoint 'analytics-dev' in us-east-1 to an interactive session using the migrate-glue-devendpoint-to-interactive-sessions skill. Validate but do not delete the dev endpoint."

# Kiro CLI (headless)
kiro-cli chat "Migrate Glue dev endpoint 'analytics-dev' in us-east-1 to an interactive session using the migrate-glue-devendpoint-to-interactive-sessions skill" \
  --no-interactive --trust-all-tools

# Codex CLI (headless)
codex "Migrate Glue dev endpoint 'analytics-dev' in us-east-1 to an interactive session using the migrate-glue-devendpoint-to-interactive-sessions skill"
```

Sessions run independently — one endpoint's code-upgrade blocker does not hold up the others. Each session emits a structured result block that can be collected and reviewed.

---

## Out of scope

- **Migrating dev endpoints to Amazon EMR.** This skill targets Glue interactive sessions only.
- **Creating a brand-new interactive session with no source dev endpoint.** Use the standard Glue console / CLI.
- **Upgrading a Glue ETL *job* from 0.9/1.0 to 4.0.** Use [`glue-v09-v1-migration`](../glue-v09-v1-migration/).
- **Output data validation beyond functional equivalence.** The skill checks that the same code produces the same functional output on the session as on the dev endpoint; it does not run business-rule data checks.
- **Change management.** No deployment-window or stakeholder coordination.

---

## Trigger phrases

The skill can be invoked by an AI agent on any of:

- `migrate my glue dev endpoint to an interactive session`
- `move off glue dev endpoints`
- `our glue dev endpoint is deprecated, what do we replace it with`
- `replace glue dev endpoint with interactive sessions`
- `modernize our glue dev environment off dev endpoints`

The skill will **not** trigger for:

- `upgrade my glue 1.0 ETL job to glue 4.0` (routes to `glue-v09-v1-migration`)
- `create a new glue interactive session` (no source dev endpoint)
- `migrate my glue dev endpoint to an EMR cluster` (different target)

---

## Reference

| | |
|---|---|
| Skill source | [`SKILL.md`](./SKILL.md) |
| Config mapping & decision tables | [`references/migration-mapping.md`](./references/migration-mapping.md) |
| Official AWS migration checklist | <https://docs.aws.amazon.com/glue/latest/dg/development-migration-checklist.html> |
| Interactive sessions overview | <https://docs.aws.amazon.com/glue/latest/dg/interactive-sessions.html> |
| Interactive sessions magics | <https://docs.aws.amazon.com/glue/latest/dg/interactive-sessions-magics.html> |
| Companion skill (code upgrade) | [`../glue-v09-v1-migration/`](../glue-v09-v1-migration/) |

---

## Final reminder

The skill provisions a real Glue interactive session (billed) and, on confirmation, deletes a real dev endpoint. **Use a pre-prod environment first.** The dev endpoint's full definition is backed up to S3 before deletion — keep that JSON file until you have validated the new session against your own data-quality criteria. The interactive session is the only forward path; the S3 backup is the only way to recreate the dev endpoint via `glue:CreateDevEndpoint` if you need to roll back.
