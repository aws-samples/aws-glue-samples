# Managing Glue Jobs: Auto Version Skill

> **Skill file:** [`SKILL.md`](./SKILL.md) — the agent reads this file when invoked. The README is for humans; `SKILL.md` is the contract the AI agent follows.

> **Customize for your environment — shared responsibility.** `SKILL.md` and the files next to it are a starting template, provided as-is as sample code under the repository license — not a managed service. You are encouraged to **customize them to your own requirements**: which jobs and accounts are in scope, IAM boundaries, change-management and approval rules, notification recipients, and any organization-specific constraints. Any edits you make are yours to own and maintain.

An AI-agent skill for AWS Glue auto version (`GlueVersion = "auto"`) on existing Spark ETL jobs. It covers two workflows:

- **Setup:** assess which jobs are eligible for auto version, enable it for the jobs you confirm ("proceed" for the eligible table, or a request that names the jobs), and optionally set up encrypted email notifications.
- **Consent:** find jobs whose upgrade is waiting for your consent, review the upgrade eligibility report with you, and record `APPROVE` or `DECLINE` for one job at a time, only when you name the job and the decision. When an upgrade has finished, it reads the job's evaluation summary and explains the result: for a failed upgrade, why AWS Glue rolled the job back (for example a data validation check, a failed run, or a blocking finding) and what the runs on the new version wrote.

---

## Quick start

**1. Install the skill** — copy this directory into your AI agent's skills folder, for example for Claude Code:

```bash
git clone https://github.com/aws-samples/aws-glue-samples.git
mkdir -p ~/.claude/skills
cp -R aws-glue-samples/utilities/skills/managing-glue-jobs ~/.claude/skills/
```

For other agents, use their skills folder, or point the agent at `SKILL.md` as in the prompt below.

**2. Invoke the skill** — ask your AI coding agent, substituting your Region and AWS profile:

```text
use the managing-glue-jobs skill to assess my Glue jobs in <region> for auto version.
use aws profile <profile-name> for any aws call.
```

or, for upgrades that are waiting for consent:

```text
use the managing-glue-jobs skill: which of my Glue jobs in <region> are waiting for upgrade consent?
```

The agent asks before every change, and records a consent decision only after you name the job and the decision in a later message.

---

## Sample session

The prompts and responses below are condensed from a real session. Job names, the account, the analysis ID and the email address are examples. The agent also runs read-only AWS CLI calls between the turns; your agent client may ask you to allow each command.

### Setup: assess, enable and set up notifications

**You:**

```text
Can you help me set up my Glue 3.0, 4.0 or 5.0 jobs in us-east-2 of account 111122223333? My jobs are the ones prefixed with etl-.
```

**Agent** (after a read-only scan and an eligibility assessment per job):

```text
Three of your four jobs are eligible for auto version, with Glue 6.0 as the target. Nothing in the account has been changed so far.

Immediately eligible for auto version (Glue 4.0/5.0 → 6.0): 3 of 4 jobs

Every listed job: script and job args analyzed; passed Spark config, job args, logging, connectors, Scala/SDK, platform, dependencies.

| Job                   | Current version |
|-----------------------|-----------------|
| etl-inventory-nightly | 5.0             |
| etl-orders-hourly     | 4.0             |
| etl-sales-daily       | 4.0             |

Not listed: 1 needs code or config changes.
Say "details" for the reasons per job.
Target: Glue 6.0 (default). For the latest AWS Glue versions, see https://docs.aws.amazon.com/glue/latest/dg/release-notes.html; I can re-run the check for a different target.

Reply "proceed" to turn on auto version for the 3 jobs above. This changes their job definitions: each one's GlueVersion is set to auto, and all its other settings are kept. Turning it on doesn't approve any upgrade; each upgrade still waits for your consent per job.
```

**You:**

```text
Proceed, and help me set up the email notifications.
```

**Agent** (after a full-definition `update-job` per job, each verified with a fresh `get-job`):

```text
Auto version is now on for your three eligible jobs. The email notifications aren't set up yet: I need a recipient address from you first.

✓ etl-inventory-nightly: GlueVersion 5.0 → auto (enrollment started)
✓ etl-orders-hourly: GlueVersion 4.0 → auto (enrollment started)
✓ etl-sales-daily: GlueVersion 4.0 → auto (enrollment started)

What happens next:
- Nothing runs now. Each job keeps running on its current Glue version (5.0 or 4.0) on its normal schedule.
- After 3 successful runs, the upgrade to Glue 6.0 waits for your approval. You'll get an email if notifications are set up; you can also check in the AWS Glue console or ask me.
- Approve within 7 days (the email shows when the 7 days started, in UTC) to upgrade. If you don't approve, or you decline, the job stays on Glue 5.0 or 4.0; after an expiry, a new cycle starts on a later run.

Email notifications. If you confirm, I will create:
- A dedicated customer managed KMS key, alias/glue-auto-version-notifications (a monthly charge plus request charges; see AWS KMS pricing), with a key policy that lets EventBridge use it only for the four rules below.
- The SNS topic glue-auto-version-notifications, encrypted with that key.
- Four EventBridge rules with plain-text emails: glue-auto-version-consent-required, glue-auto-version-upgrade-completed, glue-auto-version-upgrade-failed and glue-auto-version-upgrade-stopped.
- A topic policy that lets only those four rules publish, and an email subscription for your address.

The rules cover every auto version job in this account and Region.

To go ahead, please send:
1. The recipient email address.
2. Confirmation that it belongs to people allowed to receive job names, Glue versions, failure reasons and AWS Glue console links for all auto version jobs in 111122223333 / us-east-2.
```

**You:**

```text
The email address is data-team@example.com, and it's our team's own list.
```

**Agent:**

```text
Email notifications are set up for us-east-2, but no emails will arrive until you click the confirmation link that AWS Notifications sent to data-team@example.com.

Created: KMS key alias/glue-auto-version-notifications, SNS topic glue-auto-version-notifications, 4 EventBridge rules (every auto version job in us-east-2), email subscription for data-team@example.com (pending confirmation).

Emails are best effort and one can be missed. When an approval is due, you can check in the AWS Glue console or ask me to review the upgrades waiting for your consent.
```

### Consent: review and approve

After three successful runs, you get the approval email for `etl-sales-daily`.

**You:**

```text
I got a notification that my job etl-sales-daily in us-east-2 of account 111122223333 is ready for upgrade. Can you help review it?
```

**Agent** (after a status read and a read of the whole upgrade eligibility report; the presigned report link is never shown):

```text
etl-sales-daily (analysis ja-0a1b2c3d-1111-4222-8333-444455556666, Glue 4.0 -> 6.0) is waiting for consent.

Nothing in the report blocks the upgrade.

Recommendation: approve.

Full report: in the AWS Glue console (the job's View Auto upgrade analysis); I can fetch it again on request. I can show the matched code and suggested fixes, compatibility checks, dependency changes or write paths.

Before you decide on etl-sales-daily: approving lets up to 3 production runs of etl-sales-daily execute on the target Glue version and write to its real sinks; that data is not rolled back. Declining stops this analysis, sets etl-sales-daily back to Glue 4.0 with auto version off, and cannot be undone. If no decision is made within 7 days, the request expires and the job stays on its current version.

A job that passes the check can still fail or behave differently on the target version.

To approve, reply `approve etl-sales-daily`. To decline, reply `decline etl-sales-daily`, or let the deadline pass.
```

**You:**

```text
approve etl-sales-daily
```

**Agent** (after a fresh status check that the same analysis is still waiting for consent):

```text
I approved the upgrade for etl-sales-daily. The analysis now shows Upgrading: runs on Glue 6.0 in progress.

The upgrade happens over the job's next runs:
- Up to 3 of its normal scheduled or triggered runs use Glue 6.0 and write to the job's real outputs. Anything those runs write is not rolled back or deleted.
- If one of those runs fails, the job is retried on Glue 4.0 using the job's retry budget (MaxRetries).
- After the upgrade graduates, GlueVersion stays auto; future Glue versions follow the same consent flow.
```

After the three upgrade runs on Glue 6.0 finish, you get the "upgrade completed" email.

**You:**

```text
Can you check the status of etl-sales-daily?
```

**Agent** (after a status read and a read of the job's evaluation summary; the presigned summary link is never shown):

```text
etl-sales-daily (analysis ja-0a1b2c3d-1111-4222-8333-444455556666, Glue 4.0 -> 6.0): Upgraded to Glue 6.0.

The 3 upgraded runs on Glue 6.0 passed every check against the 3 baseline runs on Glue 4.0: operation structure, schema, output file format, completeness, output row count and output file count. Execution time is marked WARN (advisory): 455 s on Glue 6.0 against 410 s on the baseline.

The job now runs on Glue 6.0 and stays on auto version; a later Glue version goes through the same consent flow.
```

---


## At a glance

| | |
|---|---|
| **Jobs** | Existing AWS Glue Spark ETL jobs (`glueetl`). Streaming, Python shell and Ray jobs are reported as out of scope |
| **Assessment** | Optional: through the SageMaker Unified Studio Spark Upgrade MCP server. Without it, you can enable jobs one at a time that you confirm as idempotent |
| **Assessment result** | One table of the immediately eligible jobs, with what they all share stated once above it, and a count of the others; the reason per job on request |
| **Enable** | A full-definition `update-job` to `GlueVersion = "auto"` for the jobs you confirm ("proceed" for the eligible table, or a request that names them), verified with a fresh `get-job` afterwards |
| **Notifications** | Optional: an SNS topic encrypted with a customer managed KMS key, an email subscription, and four EventBridge rules for all auto version jobs in the account and Region (account-level; an existing setup is detected and reused, and an older one is reported as outdated). Plain-text emails for approval needed, upgrade completed, upgrade failed (one email per failure, with the reason) and upgrade stopped (an expired request or auto version turned off share one email); no raw state names, and the approval email shows when it was sent (UTC) and that the request expires 7 days later. Each email line appears in double quotes, which EventBridge requires for text templates |
| **Consent** | A review reply per job (the blocking check, a recommendation to approve or to review the analysis in the console, the report location, the consent risks), then `approve <job>` or `decline <job>` from you. Statuses are shown as plain labels (for example "Not upgraded: AWS Glue rolled the job back to Glue 4.0"), and a deadline as a UTC date and time |
| **AWS access** | The AWS CLI only, with your own credentials |
| **Skill file** | [`SKILL.md`](./SKILL.md) |
| **References** | [`references/`](./references/): the two workflows, the enable procedure, notifications, report format, CLI checks, MCP configuration, errors |

---

## Important constraints — read before running

1. **Enabling auto version changes the job.** The skill updates the whole job definition (Glue `UpdateJob` replaces it), copying every other setting unchanged from a fresh `get-job`, and verifies the result with another `get-job`. The update payload holds `DefaultArguments` values, which can contain secrets; it is written to a temporary directory that is deleted in the same command.
2. **Approval runs production jobs on the new version.** After you approve, up to 3 of the job's normal runs execute on the new Glue version against its real sources and sinks, and the data they write is not rolled back. A decline sets the job back to its previous Glue version. The skill never calls an upgrade safe.
3. **Jobs enabled without the assessment.** Without the SageMaker Unified Studio Spark Upgrade MCP server, the skill can still enable a job you name, only after you confirm that the job is idempotent (safe to re-run against its real sources and sinks). AWS Glue runs its pre-upgrade eligibility check on the update and rolls an ineligible job back; the skill reports that.
4. **One job, one decision.** The skill never approves or declines in bulk, on a schedule, or on a conditional instruction, and never stops an upgrade analysis.
5. **Report links are credentials.** A report or evaluation summary link can be a presigned URL. The skill reads each one inside a single command that keeps the link out of the conversation and deletes the local copy in the same call.
6. **Encrypted reports.** For a job whose reports and evaluation summaries are encrypted with your KMS key, the skill decrypts with your credentials, allowing only that key. If no AWS Encryption SDK tool is installed, it asks once before installing `aws-encryption-sdk-cli==4.3.0` into a temporary Python environment that is deleted in the same command.
7. **Attribution.** Every `update-job` the skill runs is prefixed with `AWS_SDK_UA_APP_ID=managing-glue-jobs-skill`, which adds `app/managing-glue-jobs-skill` to the call's user agent, so the change can be attributed in AWS CloudTrail. It sends no other data.

---

## Prerequisites

- **AWS CLI v2, latest version,** with the Glue auto version operations. Check with `aws glue get-job-upgrade-analysis --generate-cli-skeleton > /dev/null`; an `Invalid choice` error means the CLI must be updated. The skill never installs or updates the AWS CLI.
- **AWS credentials** for the account and Region in scope. Read-only credentials are enough for assessment and review.
- **Optional, for the assessment:** the SageMaker Unified Studio Spark Upgrade MCP server, reached through [`mcp-proxy-for-aws`](https://pypi.org/project/mcp-proxy-for-aws/) (needs `uv`). See below.
- **Optional, for encrypted reports or evaluation summaries only:** Python 3.9.2 or later with `venv`, if neither `aws-encryption-cli` nor the `aws_encryption_sdk` Python package is installed.
- `curl`, to read reports and evaluation summaries served as presigned links.

### Optional: the Spark Upgrade MCP server

The eligibility assessment uses the `spark-upgrade` server's `generate_assessment` tool, which has no AWS API or CLI equivalent. To use it, add the server to your agent's MCP configuration and restart the agent session:

- Claude Code (`.mcp.json`) and other clients that use the same format: [`mcp.json.example`](./mcp.json.example)
- Kiro (`.kiro/settings/mcp.json`): [`kiro-mcp.json.example`](./kiro-mcp.json.example)

Replace `<region>` in both the endpoint URL and `--region` with a supported Region; see [`references/mcp-configuration.md`](./references/mcp-configuration.md) and the [SageMaker Unified Studio documentation](https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/) for the current list. The configuration uses SigV4 with your standard AWS credential chain; never put access keys in it.

### IAM permissions on the agent's credential

| Workflow | Permissions |
|---|---|
| Assess, review | `glue:GetJob`, `glue:GetJobs`, `glue:ListJobs`, `glue:BatchGetJobs`, `glue:GetJobUpgradeAnalysis`, `sts:GetCallerIdentity`; `s3:GetObject` on the eligibility report and evaluation summary locations when they are `s3://` URIs (none for presigned links), and on the job scripts for the assessment; `kms:Decrypt` on the job's content KMS key, only for encrypted reports and evaluation summaries; the SageMaker Unified Studio MCP service permissions for the assessment |
| Enable (Phase 2) | `glue:GetJob`, `glue:UpdateJob`, and `iam:PassRole` on the job's existing role |
| Notifications (Phase 3) | `kms:CreateKey`, `kms:CreateAlias`, `kms:TagResource`, `kms:DescribeKey` (dedicated key), `sns:CreateTopic`, `sns:Subscribe`, `sns:GetTopicAttributes`, `sns:SetTopicAttributes`, `sns:ListSubscriptionsByTopic`, `sns:ListTopics`, `events:PutRule`, `events:PutTargets`, `events:DescribeRule`, `events:ListTargetsByRule`, `events:ListRules`; `events:RemoveTargets` and `events:DeleteRule` only if you ask to delete a named older rule |
| Consent decision | `glue:StartJobUpgradeAnalysis` |

The skill does not create or modify IAM roles or policies; grant these separately, scoped to the jobs, topic and rules involved.

---

## What the skill changes, and what it never does

**Changes (each only after your confirmation: "proceed" for the listed jobs, a request that names the jobs, or a reply that names the job for consent):**

- `aws glue update-job` to set `GlueVersion` to `auto` (and, on request, to opt a job out by setting a specific Glue version, with the same full-update rules).
- On request: an SNS topic, a KMS key (or your own key), an email subscription and four EventBridge rules for auto version notifications, reported as one `Created:` / `Updated:` / `Unchanged:` line each; an outdated rule is updated only when you name it.
- `aws glue start-job-upgrade-analysis --upgrade-consent APPROVE` or `DECLINE` for one job.
- A temporary update payload, deleted in the same command.

**Never:**

- creates or modifies IAM roles or policies;
- calls `stop-job-upgrade-analysis`, or batches, schedules or automates approvals and declines;
- sends a partial job update, or changes a job's `Role` or `MaxRetries`;
- edits job scripts or schedules, or creates jobs;
- installs or updates software, except the consented temporary decrypt environment above;
- shows or stores a presigned report or evaluation summary link;
- keeps a copy of the job definition (the update payload is temporary and deleted in the same command).

---

## Trigger phrases

- `assess my glue jobs for auto version`
- `enable glue auto version for <job>`
- `set up glue auto version emails`
- `I got a consent required email for <job>`
- `which glue jobs are waiting for upgrade consent?`
- `why did the glue upgrade of <job> fail?`
- `approve <job>` / `decline <job>` (after the review)

---

## Reference

| | |
|---|---|
| Skill source | [`SKILL.md`](./SKILL.md) |
| Setup workflow | [`references/auto-version-setup.md`](./references/auto-version-setup.md) |
| Consent workflow | [`references/auto-version-consent.md`](./references/auto-version-consent.md) |
| Enable procedure | [`references/enabling-jobs.md`](./references/enabling-jobs.md) |
| Notifications | [`references/notifications.md`](./references/notifications.md) |
| AWS Glue auto version | <https://docs.aws.amazon.com/glue/latest/dg/> |

---

## Final reminder

Enabling auto version and approving an upgrade change how your production jobs run. **Try the skill in a non-production account or on non-production jobs first**, and review each table and each report before you confirm.

Maintained by the AWS Glue team.
