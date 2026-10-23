# Glue for Ray → Amazon EKS Migration Skill

> **Skill file:** [`SKILL.md`](./SKILL.md) — the agent reads this file when invoked. The README is for humans; `SKILL.md` is the contract the AI agent follows.

> **Customize for your environment — shared responsibility.** `SKILL.md` and the files next to it are a starting template, provided as-is as sample code under the repository license — not a managed service. You are encouraged to **customize them to your own requirements**: cluster and network choices, IAM boundaries, change-management rules, instance sizing, validation criteria, and any organization-specific constraints. Any edits you make are yours to own and maintain.

An AI-agent skill that migrates an AWS Glue for Ray job to a Ray job on **Amazon EKS** with **KubeRay**. The job keeps the same Ray (2.4.0) and Python (3.9) versions, the same package versions, the same IAM permissions and the same capacity, so it behaves as it did on Glue. The skill adds no code of its own to the job or its image. It reads the job, analyzes it, builds an image, renders the KubeRay manifests and the pod role's IAM policies, pauses for your review, and runs one test run that writes to locations you choose.

---

## Quick start

**1. Install the skill** — clone this repository so your AI agent can read the skill files from disk:

```bash
git clone https://github.com/aws-samples/aws-glue-samples.git
```

**2. Invoke the skill** — paste the prompt below into your AI coding agent (for example Claude Code), substituting your job name, region and AWS profile:

```text
use the skill at <local-path>/aws-glue-samples/utilities/skills/migrating-glue-for-ray-jobs-to-amazon-eks/SKILL.md
to migrate my Glue for Ray job <job-name> in <region> to Amazon EKS.
use aws profile <profile-name> for any aws call.
```

The agent asks before anything billable, at the review gate, and for the decisions only you can make.

---

## At a glance

| | |
|---|---|
| **Source** | AWS Glue for Ray jobs (`Command.Name` `glueray`, runtime Ray2.4) |
| **Target** | A KubeRay `RayJob` on Amazon EKS: one ephemeral Ray cluster per run, deleted after the run |
| **Versions** | Unchanged: Ray 2.4.0, Python 3.9, the Glue runtime's package versions and the job's own |
| **Capacity** | One `r7g.2xlarge` (Graviton, like Glue's `Z.2X`) per Glue worker; the same CPU count as on Glue |
| **Permissions** | A new pod role (IRSA) with the Glue job role's policies, permissions boundary and tags |
| **Cluster** | A new EKS Auto Mode cluster from the included templates, or an existing cluster you name |
| **Your Glue job** | Never changed: it stays as the rollback path |
| **Skill file** | [`SKILL.md`](./SKILL.md) |
| **References** | [`references/`](./references/): argument mapping, the decisions you make, failure classification |

---

## Important constraints — read before running

These are intentional choices and limits in the current skill. Review each before pointing it at a real workload.

1. **Same versions, no upgrade.** The job runs on Ray 2.4.0 and Python 3.9, as on Glue. Upgrading Ray or Python, and the security posture of those versions, are your decisions and out of scope.
2. **Ray 2.4's dashboard and job API have no authentication.** Anyone who can reach port 8265 on the head can run code with the job's IAM role. The skill never exposes it and renders a NetworkPolicy that admits only the job's own pods and the KubeRay operator. That policy takes effect only on a cluster that enforces NetworkPolicies: a new cluster from the templates does; on an existing cluster you must confirm it.
3. **You own the EKS side.** The cluster, KubeRay, node patching, capacity and quotas, monitoring and alarms, base-image CVE patching and cost move from AWS Glue to you.
4. **Billable resources.** The migration creates an EKS cluster (if you choose a new one), EC2 instances per run, a NAT gateway, an ECR repository and, on machines that aren't arm64, an AWS CodeBuild project. The agent asks before each.
5. **No output validation.** A `SUCCEEDED` run means the job ran, not that its output is correct. Compare the test run's output with a Glue run's output in your own way (row counts, schema, aggregates).
6. **Grants on other resources don't move by themselves.** Access that a bucket policy, a KMS key policy or Lake Formation grants to the Glue job role doesn't cover the new pod role. The skill finds and lists these grants; you add the pod role to each.
7. **Capacity types.** The skill is designed for EKS Auto Mode on arm64. Managed node groups and amd64 are supported options; self-managed node groups need a node label (see `SKILL.md`).

---

## How it works

In order:

1. **Account and guardrails** — confirms the account and region, checks the EC2 vCPU quota and the local tools, and records every resource it creates in a ledger.
2. **Job definition** — reads the job, every policy of its role, the role's boundary and tags, and its latest successful run as the baseline. A job with no successful run stops here.
3. **Code** — downloads the script, the `--s3-py-modules` zips (unpacked the way Ray did on Glue) and the `--working-dir` zip.
4. **Analysis** — maps capacity, arguments, dependencies and IAM, lists the S3 locations the job uses, finds grants that other resources make to the Glue role, and flags Glue-runtime assumptions in the code.
5. **Dependencies** — resolves the Glue runtime's package set plus the job's own requirements inside the Ray 2.4.0 base image, natively on arm64, and checks every import.
6. **Review gate** — shows the report, the lockfile, the manifests, the pod role's policies (validated with IAM Access Analyzer) and the grants you must add. You name where the test run writes. Any code change the analysis needs is shown as a diff and applied only with your approval.
7. **Image** — builds and pushes the image (only the job's code and its dependencies) natively on arm64, locally or in AWS CodeBuild.
8. **Cluster** — creates a new EKS Auto Mode cluster from the templates, or checks the one you name; installs KubeRay; creates the pod role.
9. **Test run** — creates one `RayJob` that writes to your test locations, and confirms it succeeded at the same CPU capacity as on Glue.
10. **Report** — writes `MIGRATION-REPORT.md`: the mapping, your decisions, the IAM summary, the run result, how to operate the job, and the differences from Glue that apply to it.

---

## Prerequisites

### Install

The skill ships in this repository (see Quick start). The agent copies the scripts and templates into a working directory, `./glue-ray-eks-migration/<job-name>/`, and works there.

Tools on the machine running the agent: AWS CLI v2, `kubectl` (within one minor version of the cluster's Kubernetes version), `eksctl`, `helm` 3 or later, `jq`, Python 3.8 or later, and `docker` if you build images locally on an arm64 machine.

### IAM permissions on the local credential

| Service | Actions |
|---|---|
| AWS Glue | `GetJob`, `GetJobRuns`, `GetJobRun` |
| IAM | `GetRole`, `ListRolePolicies`, `GetRolePolicy`, `ListAttachedRolePolicies`, `GetPolicy`, `GetPolicyVersion`; to create the pod role: `CreateRole`, `TagRole`, `PutRolePolicy`, `AttachRolePolicy`, `PutRolePermissionsBoundary` |
| IAM Access Analyzer | `ValidatePolicy` |
| Amazon S3 | `GetObject` on the job's script and modules; `GetBucketPolicy`, `GetEncryptionConfiguration` on the job's buckets; read and write on the test-run locations you choose |
| AWS KMS | `GetKeyPolicy` on the keys of the job's buckets |
| AWS Lake Formation | `ListPermissions` |
| Amazon EKS, Amazon EC2, AWS CloudFormation | For a new cluster, the permissions `eksctl` needs; for an existing cluster, `DescribeCluster` and Kubernetes access |
| Amazon ECR | Create a repository and push an image (locally built images) |
| AWS CodeBuild | Create a project and start builds, and pass its service role (machines that aren't arm64) |
| Service Quotas, CloudWatch Logs | `GetServiceQuota`; read the cluster's log groups |

### Pre-flight checklist (you, before running)

- [ ] You know which AWS account and region the job is in, and the agent's credential is for that account.
- [ ] The job has at least one successful Glue run, and you know where that run wrote its output.
- [ ] You have chosen where the test run should write, separate from the job's real output.
- [ ] For an existing cluster: NetworkPolicy enforcement is on, and you can decide about the KubeRay operator it runs.
- [ ] You have read the **Important constraints** section above.

---

## Example: a Ray Data ETL job onto a new EKS Auto Mode cluster

A Glue for Ray job `taxi-etl` (`Z.2X` × 3, `--min-workers 1`, arguments `--input_path` and `--output_path`) reads trip data, computes tip rates and writes a Parquet summary.

```text
use the skill at <local-path>/aws-glue-samples/utilities/skills/migrating-glue-for-ray-jobs-to-amazon-eks/SKILL.md
to migrate my Glue for Ray job taxi-etl in ap-northeast-1 to Amazon EKS.
use aws profile dev for any aws call.
```

1. **Analysis** — no code findings; `--min-workers` becomes the worker floor (`minReplicas` 0, because Glue counts the head); the head runs tasks with `num-cpus: 8`, so full scale is 24 CPUs, as on Glue. The pod role copies the Glue role's policies and tags. The grant inventory finds no bucket, KMS key or Lake Formation grant that names the Glue role.
2. **Dependencies** — 21 packages: the Glue Ray2.4 runtime's versions where they differ from the Ray base image. The job has none of its own; every import loads.
3. **Review gate** — the agent asks for the cluster choice (new, EKS Auto Mode), the CIDRs allowed to reach the Kubernetes API, and where the test run writes (`.../output/eks/` instead of `.../output/glue/`), then shows the manifests, the pod role's policies (no Access Analyzer findings) and the cost items.
4. **Build and cluster** — CodeBuild on arm64 builds the image (`2.4.0 3.9.15 aarch64`); `eksctl` creates the cluster with the CloudWatch Observability add-on; the agent adds the S3 gateway endpoint, the Ray NodePool, NetworkPolicy enforcement and KubeRay 1.7.1, and creates the pod role.
5. **Test run** — `kubectl create -f test-run/manifests/rayjob.yaml`: `Complete` / `SUCCEEDED` about a minute and a half after creation, most of it node launch and image pull. The RayCluster is deleted about a minute after the job ends. The driver output is in the cluster's CloudWatch log group.
6. **Report** — `MIGRATION-REPORT.md` with the mapping, the decisions, the IAM summary, how to start runs (`kubectl create -f manifests/rayjob.yaml`), what moved to you, and the teardown commands. You compare the test output with the Glue output.

---

## Out of scope

- **Upgrading Ray or Python**, or changing the job's code beyond what the analysis shows can't work on a plain Ray container.
- **Output data validation.** Run your own checks.
- **Scheduling and invocation.** Whatever started the Glue job (triggers, workflows, Step Functions, Airflow) must create a `RayJob` instead; the report lists how.
- **Changing resources outside the migration.** The skill never changes your Glue job, its role, bucket or key policies, or Lake Formation permissions; it lists what you need to change.
- **Glue for Spark and Python shell jobs.**

---

## Trigger phrases

The skill can be invoked by an AI agent on any of:

- `migrate my glue for ray job to eks`
- `move glue ray job to kuberay`
- `glue ray to ray on eks`
- `glue for ray end of support migration`

---

## Reference

| | |
|---|---|
| Skill source | [`SKILL.md`](./SKILL.md) |
| Argument mapping | [`references/arg-mapping.md`](./references/arg-mapping.md) |
| Decisions you make | [`references/operator-decisions.md`](./references/operator-decisions.md) |
| Failure classification | [`references/troubleshooting.md`](./references/troubleshooting.md) |
| AWS Glue for Ray availability change and migration | <https://docs.aws.amazon.com/glue/latest/dg/awsglue-ray-jobs-availability-change.html> |
| KubeRay RayJob | <https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/rayjob-quick-start.html> |
| Ray security | <https://docs.ray.io/en/latest/ray-security/index.html> |

---

## Final reminder

The skill creates billable resources and runs your job on EKS. **Run it in a non-production account or against a copy of the job first.** Keep the Glue job until you have validated the EKS job's output against your data-quality criteria. Move production traffic only after that validation passes.
