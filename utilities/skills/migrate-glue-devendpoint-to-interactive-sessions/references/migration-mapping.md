# Reference: Dev Endpoint → Interactive Session mapping

Used by: `SKILL: Migrate Glue Dev Endpoint to Interactive Session`

Contents:
- [Config mapping (DevEndpoint → session)](#config-mapping)
- [Access-method decision table](#access-method-decision-table)
- [IAM two-principal setup](#iam-two-principal-setup)
- [Code compatibility (0.9/1.0 → 3.0+)](#code-compatibility)
- [Notebook magics quick reference](#notebook-magics-quick-reference)

Official checklist: https://docs.aws.amazon.com/glue/latest/dg/development-migration-checklist.html

## Config mapping

`glue:GetDevEndpoint` field → interactive session setting (magic for notebooks,
flag for `glue:CreateSession`).

| DevEndpoint field | Session magic | CreateSession flag | Notes |
|---|---|---|---|
| `RoleArn` | `%iam_role <arn>` | `--role <arn>` | runtime role; reuse the endpoint's role |
| `GlueVersion` (0.9/1.0) | `%glue_version 4.0` | `--glue-version 4.0` | MUST be 2.0+. Default 4.0. Never 0.9/1.0 |
| (Python via `Arguments`) | — | `--command Name=glueetl,PythonVersion=3` | Python 3 only |
| `WorkerType` | `%worker_type G.1X` | `--worker-type G.1X` | carry over as-is |
| `NumberOfWorkers` | `%number_of_workers N` | `--number-of-workers N` | default 5 if unset |
| `NumberOfNodes` (legacy DPUs) | `%number_of_workers N` | `--number-of-workers N` | translate DPUs → workers (G.1X = 1 DPU) |
| `SubnetId` + `SecurityGroupIds` | `%connections <conn>` | `--connections <conn>` | create a Glue connection; no raw-subnet arg |
| `ExtraPythonLibsS3Path` | `%additional_python_modules ...` | default arg | now also supports C-extension libs + PyPI |
| `ExtraJarsS3Path` | `%extra_jars ...` | default arg | |
| `Arguments["--enable-glue-datacatalog"]` | `%%configure {"--enable-glue-datacatalog":"true"}` | `--default-arguments` | |
| `SecurityConfiguration` | `%%configure` security fields | `--default-arguments` | |
| `Tags` | `%%tags {...}` | `--tags` | |
| (never times out) | `%idle_timeout 30` | `--idle-timeout 30` | NEW. Always set to control cost |
| `PublicKey(s)` / SSH | — | — | no equivalent; use Glue Docker image |

Session lifecycle (headless): `CreateSession` → poll `GetSession` until
`Status=READY` → `RunStatement` → poll `GetStatement` until `State=AVAILABLE` →
`StopSession` / `DeleteSession`. `%stop_session`, `%status`, `%list_sessions` are
the notebook equivalents.

## Access-method decision table

From the official checklist — pick the session interface by how the dev endpoint
is accessed today.

| Old access method | Migration target | Action |
|---|---|---|
| SageMaker / Jupyter / JupyterLab notebook | Glue Studio notebook | Download `.ipynb`, create a Glue Studio notebook job, upload it. Or SageMaker Studio with the Glue kernel |
| Zeppelin notebook | Glue Studio notebook | Convert to Jupyter `.ipynb` (manual copy/paste or a converter such as `ze2nb`), then upload |
| IDE (PyCharm / VS Code) | IDE integration | Use the interactive-sessions IDE guides (PyCharm blog; VS Code docs) |
| REPL | local `aws-glue-sessions` | `pip3 install --upgrade jupyter boto3 aws-glue-sessions && install-glue-kernels` |
| SSH | Glue Docker image | No session equivalent; develop locally with the Glue Docker image |

## IAM two-principal setup

Interactive sessions use two IAM principals. Confusing them is the most common
setup failure.

**Runtime role** — passed to `CreateSession`; Glue assumes it to run statements.
Same permissions as a Glue job role. Reuse the dev endpoint's `RoleArn`. Trust:
```json
{ "Version": "2012-10-17", "Statement": [ {
  "Effect": "Allow",
  "Principal": { "Service": ["glue.amazonaws.com"] },
  "Action": ["sts:AssumeRole"] } ] }
```

**Client principal** — the user/role that runs the notebook or CLI. Must be allowed
to call the session APIs and to pass the runtime role:
```json
{ "Version": "2012-10-17", "Statement": [
  { "Effect": "Allow",
    "Action": ["glue:CreateSession","glue:GetSession","glue:RunStatement",
               "glue:GetStatement","glue:ListStatements","glue:StopSession",
               "glue:DeleteSession","glue:ListSessions"],
    "Resource": "*" },
  { "Effect": "Allow", "Action": "iam:PassRole",
    "Resource": "<runtime-role-arn>" } ] }
```
`AWSGlueConsoleFullAccess` covers the session APIs; the `iam:PassRole` statement is
still required. For private sessions, use TagOnCreate with an `owner=${aws:userId}`
RequestTag condition (managed policy `AwsGlueSessionUserRestrictedServiceRole`).

## Code compatibility

Interactive sessions run **Glue 2.0+ only**. Code that ran on a 0.9/1.0 dev endpoint
may rely on behavior removed in Glue 3.0+. Upgrade the code first when any apply:

- **HDFS** paths (`hdfs://`) — not present in Glue 3.0+; repoint to S3.
- **YARN** configs (`--conf spark.yarn.*`) — silently ignored; remove.
- **Python 2** syntax — Glue 2.0+ is Python 3 only.
- **Spark 2.x semantics** — Parquet timestamp rebase, untyped Scala UDFs, Log4j 1.x,
  Scala 2.11 jars, AWS SDK 1.11 — all change at Glue 3.0+.

Delegate the code upgrade to the `glue-09-10-migration` skill (its breaking-change
catalogue covers all of the above). Run it before provisioning the session. Code that
is plain version-agnostic PySpark + GlueContext (DataFrames, Spark SQL, DynamicFrames,
S3 I/O) needs no changes.

## Notebook magics quick reference

First-cell session config (Glue Studio / Jupyter):
```
%iam_role arn:aws:iam::<acct>:role/<GlueRuntimeRole>
%region us-east-1
%glue_version 4.0
%worker_type G.1X
%number_of_workers 2
%idle_timeout 30
%connections my-vpc-connection            # only if VPC data access is needed
%additional_python_modules pandas,pyarrow # optional
%%configure
{ "--enable-glue-datacatalog": "true" }
```
Lifecycle: `%status`, `%list_sessions`, `%stop_session`. Full magic list:
https://docs.aws.amazon.com/glue/latest/dg/interactive-sessions-magics.html