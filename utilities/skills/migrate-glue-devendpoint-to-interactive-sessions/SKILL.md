---
name: migrate-glue-devendpoint-to-interactive-sessions
description: >
  Migrate a legacy AWS Glue development endpoint to a Glue interactive session,
  following the official AWS migration checklist. Inventories the dev endpoint
  config, maps it to an equivalent interactive session (runtime role, Glue
  version, workers, VPC connection, idle timeout), provisions the session,
  validates by running the developer's Spark/PySpark code, and deletes the dev
  endpoint after the user confirms. Use when: migrate glue dev endpoint, move
  off dev endpoints, dev endpoint to interactive sessions, replace glue dev
  endpoint, glue dev endpoint deprecated, modernize glue dev environment.
  Do NOT use for: upgrading a Glue ETL job version 0.9/1.0 to 4.0 (use
  glue-09-10-migration), creating a brand-new interactive session with no
  source dev endpoint, or migrating dev endpoints to Amazon EMR.
argument-hint: "[dev-endpoint-name] [aws-region] [aws-cli-profile-name]"
owner_team: AWS Analytics
owner_cti: AWS/Analytics/Agent Skills
stages: [preprod]
version: 1
metadata:
  service: [glue]
  task: [migrate, upgrade]
  persona: [developer, data-engineer]
  workload: [data-analytics]
---

# Migrate Glue Dev Endpoint to Interactive Session

Move a developer off a legacy Glue development endpoint (Glue 0.9/1.0, no console
since March 2023, billed continuously) onto a Glue interactive session (Glue 2.0+,
sub-minute startup, idle timeout, console support). Follows the official checklist:
https://docs.aws.amazon.com/glue/latest/dg/development-migration-checklist.html

The migration is **additive and reversible until the final step**: the session is
created and validated alongside the still-running dev endpoint. The dev endpoint is
deleted only after the user confirms the session works.

## Reference Documentation

- [references/migration-mapping.md](references/migration-mapping.md) -- DevEndpoint
  field → session magic/API mapping, the access-method decision table, IAM
  two-principal setup, and code-compatibility notes for the 0.9/1.0 → 3.0+ jump.

## Workflow

### 1. Inventory the Dev Endpoint and Route

1.1. Confirm inputs: dev endpoint name, region, AWS CLI profile. MUST NOT proceed
without a dev endpoint name.

1.2. Call `glue:GetDevEndpoint(EndpointName=<name>)`. Store as `source`. Record:
`RoleArn`, `GlueVersion`, `WorkerType`, `NumberOfWorkers` (or `NumberOfNodes`),
`SubnetId`, `SecurityGroupIds`, `Arguments`, `ExtraPythonLibsS3Path`,
`ExtraJarsS3Path`, `PublicAddress`/`PrivateAddress`, `Status`.

1.3. **Route on access method** — ask the user how they use the dev endpoint today
(or infer from `source`). This determines the session interface, per the official
checklist. See the decision table in `references/migration-mapping.md`:
   - SageMaker / Jupyter / JupyterLab notebook → Glue Studio notebook (upload `.ipynb`)
   - Zeppelin notebook → convert to Jupyter, then Glue Studio notebook
   - IDE (PyCharm / VS Code) → IDE integration for interactive sessions
   - REPL → local `aws-glue-sessions` package
   - SSH → **no direct equivalent**; use the Glue Docker image for local dev

1.4. **Route on code compatibility (this is the hard gate):** interactive sessions
run **Glue 2.0+ only** — they cannot run 0.9/1.0. If the dev endpoint's scripts
depend on 0.9/1.0-specific behavior (HDFS, YARN configs, Spark 2.x semantics,
Python 2 syntax), the code MUST be upgraded to Glue 3.0+ first.
   - If the user has 0.9/1.0-specific code → migrate the code using the
     `glue-09-10-migration` skill (its breaking-change catalogue covers Spark 2.x→3.x,
     Python 2→3, Scala, Log4j, Parquet timestamps) BEFORE provisioning the session.
   - If the code is already version-agnostic PySpark/GlueContext → proceed directly.

### 2. Set Up IAM (two-principal model)

Interactive sessions need two principals (the #1 setup failure). See
`references/migration-mapping.md` for exact policies.
- **Runtime role**: pass to `CreateSession`. Reuse the dev endpoint's `RoleArn` —
  it already has the right Glue job permissions and trusts `glue.amazonaws.com`.
- **Client principal**: the user/role running the notebook or CLI. MUST have
  permission to call session APIs (e.g. `AWSGlueConsoleFullAccess`) AND
  `iam:PassRole` on the runtime role. Verify before creating the session.

### 3. Provision the Interactive Session

Build session config by mapping `source` fields (full table in the reference):

| Dev endpoint | Session setting |
|---|---|
| `RoleArn` | `--role` (runtime role) |
| `GlueVersion` 0.9/1.0 | `--glue-version 3.0` (or 4.0/5.0); never 0.9/1.0 |
| `WorkerType` / `NumberOfWorkers` | `--worker-type` / `--number-of-workers` (default 5) |
| `SubnetId` + `SecurityGroupIds` | a Glue **connection** referenced via `%connections` / `--connections` |
| `ExtraPythonLibsS3Path` | `%additional_python_modules` |
| `ExtraJarsS3Path` | `%extra_jars` |
| `--enable-glue-datacatalog` arg | session default arg / `%%configure` |
| (dev endpoints never time out) | `--idle-timeout <min>` — **always set this** |

3.1. **Default**: `--glue-version 4.0`, same `WorkerType`/`NumberOfWorkers` as the
dev endpoint, `--idle-timeout 30`, Python 3. Override on request ("use Glue 3.0",
"use 10 workers", "idle timeout 60").

3.2. Create via the user's chosen interface:
   - **Notebook (Glue Studio / Jupyter)**: set magics in the first cell —
     `%iam_role`, `%glue_version`, `%worker_type`, `%number_of_workers`,
     `%idle_timeout`, `%connections` — then run a cell to start the session.
   - **Headless / CLI** (use for automated validation): `glue:CreateSession` with
     `--id`, `--role`, `--command Name=glueetl,PythonVersion=3`, `--glue-version`,
     `--worker-type`, `--number-of-workers`, `--idle-timeout`. Poll `glue:GetSession`
     until `Status=READY` (typically under 1 minute).

### 4. Validate Equivalence

4.1. Run the developer's actual Spark/PySpark code in the new session and compare
output to the dev endpoint.
- **Notebook path**: run the migrated `.ipynb` cells; confirm no errors and matching
  results.
- **Headless path**: submit the code with `glue:RunStatement(SessionId, Code)`, poll
  `glue:GetStatement` until `Statement.State=AVAILABLE`, then read the result from
  `Statement.Output.Data.TextPlain` (the exact JSON paths — do not guess). A
  `Statement.Output.Status=error` means the code failed — capture
  `Statement.Output.ErrorName`/`ErrorValue`.

4.2. **PASS criterion**: the session produces functionally equivalent output. Spark
version (2.4.x → 3.3.x+) and Python version (3.6 → 3.10) WILL differ; that is expected
and not a failure. On PASS → Step 5.

4.3. **On failure, classify the error before doing anything else:**

   - **CODE_COMPATIBILITY** — a Glue 0.9/1.0 breaking change surfacing on Spark 3.x /
     Python 3.10. Signals: `SyntaxError`, `ImportError`/`ModuleNotFoundError`,
     `No FileSystem for scheme: hdfs`, untyped Scala UDF, Log4j 1.x classes, Parquet
     timestamp/rebase errors, Scala 2.11 binary incompatibility, AWS SDK 1.11 method
     not found. **Fix path:** use the `glue-09-10-migration` skill's breaking-change
     catalogue to diagnose and patch the code. **Validate the patched code by
     re-running it in THIS interactive session via `glue:RunStatement` — do NOT create
     or run a Glue job for validation** (that is `glue-09-10-migration`'s native
     mechanism; here the session is the validation surface). Then re-run 4.1.

   - **MIGRATION_CONFIG_GAP** — the code is fine but the session is missing something
     the dev endpoint provided. Signals: cannot reach a VPC data store (no Glue
     connection), `ModuleNotFoundError` for a lib that was in `ExtraPythonLibsS3Path`,
     `ClassNotFoundException` for a jar that was in `ExtraJarsS3Path`, catalog tables
     not found (`--enable-glue-datacatalog` not carried over). **Fix path:** patch the
     session config (add `%connections`/`--connections`, `%additional_python_modules`,
     `%extra_jars`, or the catalog arg) per the mapping in Step 3, recreate the session,
     and re-run 4.1.

   - **TRANSIENT** — provisioning/throttling/internal errors with no user-code
     traceback. **Fix path:** retry 4.1 once.

4.4. **Attempt budget** (mirrors `glue-09-10-migration`): `MAX_FIX_ATTEMPTS = 5`.
Each fix-and-rerun consumes one attempt. After each failure, compute a fingerprint
(`error_name + ":" + failing_symbol`); if a fingerprint repeats (the same failure
recurred after its fix), go to the **Exit path** immediately — do not burn the rest of
the budget on a fix that is not converging. If the error matches no class in 4.3, go
to the **Exit path**. On reaching 5 attempts without a PASS, go to the **Exit path**.

4.5. **Exit path (validation cannot be made to pass):** MUST NOT proceed to Step 5 and
MUST NOT delete the dev endpoint — it is the rollback. Keep the dev endpoint running.
Stop the session (`glue:StopSession`) to avoid cost, but leave it for debugging.
Report classification, the error, what was tried, and manual next steps:
```
MIGRATION RESULT: VALIDATION FAILED — dev endpoint RETAINED
Dev endpoint: <name>  (Glue <ver>)  -> KEPT (rollback intact)
Interactive session: <id>  -> STOPPED (kept for debugging)
Halt reason: <BUDGET_EXHAUSTED|CYCLE_DETECTED|UNCLASSIFIED>
Failure class: <CODE_COMPATIBILITY|MIGRATION_CONFIG_GAP|TRANSIENT|UNCLASSIFIED>
Error: <ErrorName>: <ErrorValue>
Fixes attempted: [list]
Manual next steps: [list]
```

### 5. Decommission the Dev Endpoint (confirm first)

5.1. Present the validation result, then **explicitly ask the user to confirm deletion
of the dev endpoint** — it is the rollback path. Ask a direct yes/no question naming
the endpoint, for example: `"Validation passed. Delete dev endpoint '<name>' in
<region> now? Its definition will be backed up to S3 first. (yes/no)"` MUST wait for an
affirmative reply. Treat anything other than a clear yes (no answer, "later", "keep
it", silence) as NO: skip deletion, leave the dev endpoint running, and tell the user
it was kept and how to delete it themselves later. MUST NOT delete on assumption or to
"save cost" without this explicit yes.

5.2. **Back up the dev endpoint definition before deleting (MUST NOT skip).** A
deleted dev endpoint cannot be recovered, so persist its full config first. Write the
`source` definition captured in Step 1.2 (the raw `glue:GetDevEndpoint` response) as
JSON to S3 — default `s3://<a bucket the runtime role can write>/glue-devendpoint-backups/<name>-<region>.json`;
ask the user for the bucket if none is evident. This is the rebuild record: it captures
`RoleArn`, `GlueVersion`, `WorkerType`/`NumberOfWorkers`, `SubnetId`,
`SecurityGroupIds`, `Arguments`, `ExtraPythonLibsS3Path`, `ExtraJarsS3Path`, and
`PublicKeys`, so the endpoint can be recreated with `glue:CreateDevEndpoint` if the
migration must be rolled back. Confirm the `s3:PutObject` succeeded before proceeding.

5.3. On confirmation and a confirmed backup: `glue:DeleteDevEndpoint(EndpointName=<name>)`.
Remove any endpoint-specific scaffolding the migration added (registered SSH public
keys, temporary Elastic IPs).

5.4. Output the result:
```
MIGRATION RESULT: SUCCESS
Dev endpoint: <name>  (Glue <ver>)  -> DELETED
Definition backup: s3://<bucket>/glue-devendpoint-backups/<name>-<region>.json
Interactive session: <id>  (Glue 4.0)
Access method: <notebook|IDE|REPL|docker>
Code upgrade: <none | via glue-09-10-migration>
Validation: PASSED (output matched)
Idle timeout: <n> min
```

## Gotchas

- **Interactive sessions cannot run Glue 0.9/1.0.** If the code is version-specific,
  upgrading it to Glue 3.0+ is a prerequisite, not optional. Delegate to
  `glue-09-10-migration`.
- **Two IAM principals.** Missing `iam:PassRole` on the client principal is the most
  common `CreateSession`/`AccessDenied` failure. The runtime role alone is not enough.
- **Always set `--idle-timeout`.** Dev endpoints never timed out and billed 24/7. A
  session with no idle timeout reintroduces that cost. Default 30 min.
- **VPC access changes shape.** Raw `SubnetId` + `SecurityGroupIds` become a named
  Glue **connection** referenced by `%connections`. There is no raw-subnet session arg.
- **No SSH successor.** If the developer relied on SSH into the endpoint, there is no
  session equivalent — route them to the Glue Docker image for local development.
- **The C-extension limitation is lifted.** Dev endpoints rejected C-extension Python
  libs (e.g. pandas); `%additional_python_modules` supports PyPI and S3 wheels.
- **Decommission is destructive and last.** Never delete the dev endpoint before the
  session is validated and the user confirms. It is the only rollback.
- **Back up the dev endpoint definition to S3 before deleting.** A deleted dev endpoint
  is unrecoverable; the saved JSON config is the only way to recreate it via
  `glue:CreateDevEndpoint`. MUST NOT skip, even when the user says "just delete it."
- **`DynamicFrame.fromDF` emits a benign `UserWarning` on Spark 3.x** ("DataFrame
  constructor is internal"). It is not an error; output is unaffected.

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `AccessDeniedException` on `CreateSession` | Client principal lacks session API perms or `iam:PassRole` | Attach `AWSGlueConsoleFullAccess` (or session API perms) AND `iam:PassRole` on the runtime role to the client principal |
| `CreateSession` fails on runtime role | Runtime role missing `glue.amazonaws.com` trust or Glue permissions | Reuse the dev endpoint's `RoleArn`, or attach `AWSGlueServiceRole` with the Glue trust policy |
| Statement `State=ERROR` with `IllegalArgumentException`/`SyntaxError` at start | Code has Glue 0.9/1.0 (Python 2 / Spark 2.x) constructs | Upgrade the code to Glue 3.0+ first via `glue-09-10-migration`, then re-run |
| `No FileSystem for scheme: hdfs` | Script uses HDFS (not present in Glue 3.0+) | Repoint to S3; covered by `glue-09-10-migration` HDFS_PATH handling |
| Session works but cannot reach a VPC data store | No Glue connection attached | Create a Glue connection for the subnet/SGs and pass it via `%connections` / `--connections` |
| `Session already exists` on `CreateSession` | An earlier session with the same `--id` is still alive | Pick a new `--id`, or `glue:DeleteSession` the stale one first |

## References

- `references/migration-mapping.md` — full config mapping, access-method decision
  table, IAM two-principal setup, code-compatibility notes.