# Managing Glue Jobs: AWS Glue auto upgrade skill

> **Skill file:** [`SKILL.md`](./SKILL.md). The agent reads this file when you invoke the skill. This README is for people.

> **Customize for your environment: shared responsibility.** `SKILL.md` and the files next to it are a starting template, provided as-is as sample code under the repository license. They aren't a managed service. Review them, and adapt the permissions, names and wording to your organization before you use them.

An AI-agent skill for AWS Glue automatic version upgrades (auto upgrade, `GlueVersion` = `auto`) on Spark ETL jobs. It follows [Automatic version upgrades for AWS Glue jobs](https://docs.aws.amazon.com/glue/latest/dg/auto-version-upgrades.html) in the AWS Glue User Guide, step by step:

1. Assess which jobs can use auto upgrade.
2. Confirm that each job is safe to retry.
3. Set up email notifications.
4. Turn on auto upgrade.
5. Review the eligibility summary.
6. Approve or decline one named job.
7. Monitor the result.

---

## Quick start

**1. Install the skill.** Copy this directory into your AI agent's skills folder. For example, for Claude Code:

```bash
git clone https://github.com/aws-samples/aws-glue-samples.git
mkdir -p ~/.claude/skills
cp -R aws-glue-samples/utilities/skills/managing-glue-jobs ~/.claude/skills/
```

**2. Connect the assessment server (optional).** The assessment uses the `generate_assessment` tool of the SageMaker Unified Studio `spark-upgrade` MCP server. Add it to your agent's MCP configuration, then restart the session:

- Claude Code (`.mcp.json`) and clients that use the same format: [`mcp.json.example`](./mcp.json.example)
- Kiro (`.kiro/settings/mcp.json`): [`kiro-mcp.json.example`](./kiro-mcp.json.example)

Replace `<region>` in both the endpoint URL and `--region`. Without the server, the skill marks jobs "not analyzed", and `proceed` doesn't cover them.

**3. Ask your agent.** These are the sample prompts from the user guide:

```text
Assess my Glue jobs in us-west-2 for auto upgrade. Use AWS profile my-profile.
Proceed with the eligible jobs, and set up auto upgrade email notifications to data-team@example.com.
Which of my Glue jobs in us-west-2 are waiting for upgrade approval?
Approve the upgrade for my-etl-job.
```

---

## Sample session

The replies below are condensed from eval runs of this skill. Job names, the account and the email address are examples. Where a turn wasn't run live, the reply shape comes from the skill's own template; those turns are marked.

### Assess

**You:**

```text
Assess the Glue job etl-sales-daily in account 111122223333 region us-east-1 for auto upgrade.
```

**Agent** (after a read-only `get-jobs` / `get-job` and `generate_assessment`):

```text
AWS Glue can auto upgrade this job.

The job meets the requirements and passes the compatibility check.

| Job             | Current version | Target version      |
|-----------------|-----------------|---------------------|
| etl-sales-daily | 4.0             | 6.0 (latest stable) |

Before you turn on auto upgrade, confirm that the job is safe to retry. If an upgraded run fails, AWS Glue retries the run on the previous version, and the retried run writes to the same targets again.

AWS Glue can email you when an upgrade needs approval, expires, fails, finishes, or stops. The emails include job names, AWS Glue versions, and failure reasons.

To turn on auto upgrade for this job, enter `proceed`. To also get emails, enter `proceed, email <address>`.
```

### Turn on, with email notifications

**You:**

```text
proceed, email data-team@example.com
```

**Agent** (template shape, `references/assess-and-turn-on.md`): the agent sets up the emails first, then makes a full-definition `update-job` per job, verified with a fresh `get-job`:

```text
Auto upgrade is now on for 1 job. The job stays on AWS Glue 4.0 until you approve an upgrade.

| Job             | GlueVersion | Runs on |
|-----------------|-------------|---------|
| etl-sales-daily | `auto`      | 4.0     |

What happens next
1. You keep running the job as usual.
2. AWS Glue analyzes the job and doesn't change the job.
3. If the job needs no changes, AWS Glue emails you and requests your approval. You have 7 days to approve or decline.

Email notifications
AWS Glue now emails data-team@example.com about auto upgrades in us-east-1. To start receiving email, open the email titled "AWS Notification - Subscription Confirmation" and choose Confirm subscription.

If you miss an email, enter `Which of my Glue jobs in us-east-1 are waiting for upgrade approval?`
```

If the email setup can't complete (for example, a missing permission), the agent stops before turning on any job and tells you what to grant. From an eval run:

```text
The current role lacks EventBridge permissions (`events:DescribeRule`), so I can't check or create the notification rules. … Per the skill, the emails must be set up before turning on auto upgrade, so no approval email is missed. Since I can't complete the notification setup, I'll stop and tell you what to grant, rather than turning on auto upgrade without the emails you asked for.
```

### Review the eligibility summary

Later, for another job that has finished its observation runs (this one started on AWS Glue 3.0):

**You:**

```text
Which of my Glue jobs in us-east-1 are waiting for upgrade approval?
```

**Agent** (after a status read and one command that fetches the summary inside the command; the presigned link is never shown):

```text
1 job is waiting for your approval to upgrade from AWS Glue 3.0 to 6.0. AWS Glue found no blocking issues.

| Job                   | Write mode   | Write paths |
|-----------------------|--------------|-------------|
| etl-inventory-nightly | Not recorded | None        |

AWS Glue recorded no write operations during the observation run, so no write paths are listed.

If an upgraded run fails, AWS Glue retries the run on 3.0, and the retried run writes to the same paths again. Before you approve, confirm the following:
- The job's write paths are captured. The summary shows only paths from observed runs, and this job recorded none.
- A retried run doesn't leave duplicate records or uncleaned data from the failed run for downstream jobs and dashboards.

After you approve, check the output of the first upgraded run.

To approve, enter `Approve the upgrade for etl-inventory-nightly`. To decline, enter `Decline the upgrade for etl-inventory-nightly`.
```

### Approve

**You:**

```text
Approve the upgrade for etl-inventory-nightly
```

**Agent** (template shape, `references/approval.md`; after a fresh status check that the same analysis is still waiting):

```text
You approved the upgrade for etl-inventory-nightly. Subsequent job runs use AWS Glue 6.0.

After the first upgraded run, check its output.
```

---

## Important constraints: read before running

1. **Turning on auto upgrade changes the job.** `update-job` replaces the whole job definition. The skill builds the update from a fresh `get-job`, sets only `GlueVersion` to `auto`, and reads the job back. It stops for a job whose `ProfileName` or `SourceControlDetails` an update can't keep.
2. **Approval runs your job on the new version.** After you approve, subsequent runs use the target version and write to the job's real outputs. AWS Glue doesn't remove data that upgraded runs wrote. A failed upgraded run is retried on the previous version, which writes to the same targets again. That's why each job must be safe to retry.
3. **One named decision per job.** The skill records an approval or a decline only after it shows that job's review, and only when you name the job and the decision. It doesn't approve in bulk, on a condition ("approve it if there are no blocking issues"), or on a schedule. A recorded decision can't be changed.
4. **Summary links are credentials.** The eligibility summary and the auto upgrade evaluation summary can be presigned links. The skill reads them inside one command and never prints or stores them. An encrypted summary is read in the AWS Glue console.
5. **Attribution.** Every `update-job` and `create-job` is prefixed with `AWS_SDK_UA_APP_ID=managing-glue-jobs-skill`, so the change can be attributed in AWS CloudTrail.
6. **If you set up emails with an earlier version of this skill:** your account already has the EventBridge rules `glue-auto-version-consent-required`, `glue-auto-version-upgrade-completed`, `glue-auto-version-upgrade-failed` and `glue-auto-version-upgrade-stopped`, and the SNS topic `glue-auto-version-notifications`. This version creates `glue-auto-upgrade-*` rules and the topic `glue-auto-upgrade-notifications`. The skill doesn't detect or change the earlier resources, so each auto upgrade event sends two emails until you delete the earlier rules and topic yourself.

---

## Prerequisites

- **AWS CLI v2, latest version,** with the auto upgrade commands. The skill checks with `aws glue get-job-upgrade-analysis --generate-cli-skeleton > /dev/null` and stops if the CLI is too old. It never installs or updates software.
- **AWS credentials** for the account and Region. Read-only credentials are enough for assessment and review.
- **Optional:** the `spark-upgrade` MCP server for the assessment (see Quick start). The AWS MCP server is recommended for working with AWS, but this skill doesn't require it.
- `curl`, to read summaries served as presigned links.

### IAM permissions

[`references/permissions.md`](./references/permissions.md) has one policy per step: assess and review (read-only); turn on, turn off and decide; and email notifications. The skill doesn't create or change IAM roles or policies.

---

## What the skill changes, and what it never does

**Changes** (each only after your confirmation in the conversation):

- `aws glue update-job` to set `GlueVersion` to `auto`, or back to a specific version to turn auto upgrade off;
- `aws glue create-job` for a new job with auto upgrade, on request;
- one SNS topic, an email subscription, five EventBridge rules and their targets, and one topic policy statement. These are created only when missing; existing resources are never overwritten;
- `aws glue start-job-upgrade-analysis --upgrade-consent APPROVE` or `DECLINE` for one named job.

**Never:**

- creates or modifies IAM roles or policies, or KMS keys;
- approves or declines in bulk, on a condition or on a schedule;
- sends a partial job update, or changes a job's `MaxRetries` or job run queuing;
- edits job scripts;
- prints or stores a presigned summary link;
- deletes resources (on a failed email setup, it gives you the clean-up commands to run yourself).

---

## Reference

| | |
|---|---|
| Skill | [`SKILL.md`](./SKILL.md) |
| Assess, safe to retry, turn on | [`references/assess-and-turn-on.md`](./references/assess-and-turn-on.md) |
| Review, approve, monitor, results | [`references/approval.md`](./references/approval.md) |
| Email notifications | [`references/notifications.md`](./references/notifications.md) |
| IAM permissions | [`references/permissions.md`](./references/permissions.md) |
| AWS Glue user guide | [Automatic version upgrades for AWS Glue jobs](https://docs.aws.amazon.com/glue/latest/dg/auto-version-upgrades.html) |

---

## Final reminder

Turning on auto upgrade and approving an upgrade change how your production jobs run. **Try the skill in a non-production account or on non-production jobs first**, and review each table and each summary before you confirm.

Maintained by the AWS Glue team.
