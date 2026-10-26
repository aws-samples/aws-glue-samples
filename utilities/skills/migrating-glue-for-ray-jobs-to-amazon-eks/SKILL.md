---
name: migrating-glue-for-ray-jobs-to-amazon-eks
description: Migrate an AWS Glue for Ray job (Command.Name glueray, Runtime Ray2.4) to a Ray job on Amazon EKS with KubeRay, at the same Ray and Python versions (Ray 2.4.0 / Python 3.9) with equivalent functionality. Use when a customer must move Glue for Ray workloads to Ray on EKS before Glue for Ray end of life. Do NOT use for Glue for Spark (glueetl) or Python shell jobs.
argument-hint: "[glue-job-name] [aws-region]"
owner_team: AWS Glue
owner_cti: AWS/GlueRay/Deprecation Management
stages: [preprod]
version: 1
metadata:
  service: [glue, eks, ecr, iam, s3, codebuild, cloudwatch]
  task: [migrate]
  persona: [data-engineer, developer, platform-engineer]
  workload: [data-analytics, etl, machine-learning]
---

## Execution instructions

When the user asks you to migrate one or more named Glue for Ray jobs to Ray on EKS, run the
migration end to end with your tools. Don't just describe the steps. Work in
`./glue-ray-eks-migration/<job-name>/`.

This skill does the simplest, fastest migration: the job runs on EKS on the **same Ray and Python
versions** it used on Glue for Ray (Ray 2.4.0, Python 3.9), with the same dependency versions, and
**adds no code of its own**: the image holds only the job's code and its dependencies, so the
customer has nothing extra to maintain. The job's code is edited only where the analyzer finds a
Glue-runtime assumption that can't work on a plain Ray container, and only with the user's approval
(Step 3). Upgrading Ray or Python, and assessing the security posture of that Ray version, are the
customer's decisions and out of scope here; this skill reproduces what the customer already runs.

The migration produces:
1. a container image (ECR) with the job's code and its exact dependencies;
2. KubeRay manifests (`rayjob.yaml`, ServiceAccounts, NetworkPolicy) and the pod role's IAM policies;
3. a migration report;
4. optionally, a test run on the customer's EKS cluster (Step 9).

The customer runs and owns the EKS cluster. Say so up front, and again in the report (Step 10).

Run autonomously. Pause for the user only at these points:
- **must-ask infrastructure decisions**: new or existing cluster, VPC/subnets, encryption,
  observability stack, concurrency, and whether to expose the Ray dashboard (Step 5, see
  `references/operator-decisions.md`);
- **the test-run output location**: where the first run writes instead of the Glue job's outputs
  (Step 5);
- **the review gate**, before you build, push or create anything else (Step 5). On a machine that
  isn't arm64, the CodeBuild project the Step 4 resolve runs in is created before it, with the user's
  confirmation (Step 6);
- **any destructive or billable action** in the customer's account (cluster or node creation,
  deleting a resource): confirm the account and region first.

Never change or delete the customer's Glue job. It stays as the rollback path until the customer has
validated the EKS job and retires it.

Scripts are in `scripts/`, templates in `templates/` and references in `references/`, relative to
this skill. Copy the scripts and templates into the working directory before using them. Where a
step says to read a reference, read it before doing that step.

## Background

AWS Glue for Ray runs Ray 2.4.0 on Python 3.9 on Graviton (arm64) workers. It has one worker type,
`Z.2X` (8 vCPU, 64 GB memory, 128 GB disk). It closed to new customers on 2026-04-30. Accounts that
aren't allowed to use it get `Account not allowed to submit Glue Ray job Post Deprecation` from
`CreateJob` and `StartJobRun`, so you may **not** be able to re-run the Glue job to get a fresh
baseline. Use the outputs of its past runs instead.

A Glue for Ray job depends on runtime behavior that the public migration guide doesn't cover (how
job arguments, worker counts, dependencies, the object store and spilling work, and the runtime
user, HOME and working directory). `references/arg-mapping.md` documents it, and the steps below
reproduce it so the job behaves on EKS as it did on Glue.

## Parameter table

Wherever a step uses a `[TOKEN]`, substitute the value here.

| Token | Value |
|---|---|
| `[RAY_VERSION]` | 2.4.0 |
| `[PYTHON_VERSION]` | 3.9 |
| `[BASE_IMAGE_ARM64]` | `rayproject/ray:2.4.0-py39-aarch64` |
| `[BASE_IMAGE_AMD64]` | `rayproject/ray:2.4.0-py39` (amd64 only) |
| `[BASE_REQUIREMENTS]` | `templates/requirements-glue-ray24.txt` (the Glue Ray2.4 runtime's packages) |
| `[KUBERAY_VERSION]` | 1.7.1 (at least 1.7 and below 1.9; see below) |
| `[INSTANCE_TYPE]` | `r7g.2xlarge` (arm64) / `r7a.2xlarge` (amd64): the same 8 vCPU / 64 GiB as Z.2X; Graviton keeps parity with Glue |
| `[POD_CPU]` | 7: one Ray pod per node. Kubernetes reserves part of the node and the head also runs the autoscaler sidecar, so a full 8-CPU request never schedules. Ray is still told `num-cpus: 8` (Glue parity), so it schedules exactly as many tasks as on Glue |
| `[POD_MEMORY_GIB]` | 54: allocatable memory is about 61 GiB; this leaves room for the sidecar and node agents. Object-store percentages apply to this value |
| `[EPHEMERAL_STORAGE_GIB]` | 160 (node) / 100 (pod request): Z.2X had 128 GB of disk; local spilling and runtime-env downloads need it |
| `[K8S_VERSION]` | 1.36: in EKS standard support, and the Kubernetes version KubeRay 1.7 is built against |

Ray 2.4's autoscaler calls the KubeRay API at version `ray.io/v1alpha1`. KubeRay 1.7 deprecated that
version and plans to stop serving it in 1.9, so the KubeRay operator must be older than 1.9: a
property of the Ray version, not a choice. The manifests also use RayJob cleanup rules as in KubeRay
1.7, so the operator must be at least 1.7.

Every job maps to the same node shape; only the node count varies (from `NumberOfWorkers`). A
different instance size (for example for a job that was memory-bound on Glue) is a customer decision:
pass `--pod-memory-gib` / `--pod-cpu` to the analyzer and change `[INSTANCE_TYPE]`. Build arm64
images for Graviton parity. An amd64 image (with an amd64 node pool) is only for a user who chooses
it: every dependency the Glue job installed had an arm64 build, because Glue ran arm64.

## Steps to execute

### Step 0: Confirm the account and set guardrails

1. `aws sts get-caller-identity`. Confirm with the user that the account and region are the right
   ones. Treat any account you can't positively identify as production: read-only actions only
   until confirmed. Set `AWS_REGION=<REGION>` (or pass `--region`) for every command; the shell's
   default region may differ.
2. Check the EC2 On-Demand Standard vCPU quota, which replaces Glue's M-DPU quota. Peak need is
   about `NumberOfWorkers × 8` per concurrent job:
   `aws service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A`.
3. Track every resource you create in `./glue-ray-eks-migration/ledger.md`, with tags
   `migrated-from=glue-ray` and `glue-job=<name>`.
4. Check the tools before anything billable: `aws`, `python3` (3.8 or later), `kubectl` (within one minor
   version of `[K8S_VERSION]`), `eksctl`, `helm` 3 or later, `jq`, and `docker` if images are built on this machine. Stop
   and tell the user what is missing.

### Step 1: Get the job definition

The user must name the job, or a list of jobs, and the region. If they didn't, ask; don't search the
account for jobs. Migrate each job separately, in its own `./glue-ray-eks-migration/<job-name>/`.

- `aws glue get-job --job-name <JOB> --region <REGION> > job.json`. Confirm `Command.Name` is
  `glueray`; this skill migrates only Glue for Ray jobs. Check the baseline (below) next, before
  collecting anything else. `job.json` and `run-args.json` hold every
  argument value in plain text, including secrets: don't print them, and after Step 3 replace the
  values of the plan's `sensitive_args` in both files with a placeholder.
- Collect **every** policy of the job role, so the pod role gets exactly the same permissions
  (`aws iam list-role-policies`, `list-attached-role-policies`). Save them as a JSON list in
  `role-policies.json` for Step 3: for each inline policy the output of `get-role-policy`; for each
  attached policy, AWS-managed ones included, the output of `get-policy-version` (its default
  version, from `get-policy`) with a `"PolicyArn"` field added. Also save
  `aws iam get-role --role-name <ROLE> > role.json`: the pod role gets the same permissions boundary
  and tags.
- Find the baseline: `aws glue get-job-runs --job-name <JOB>`. Pick the latest `SUCCEEDED` run and
  save its arguments: `aws glue get-job-run --job-name <JOB> --run-id <RUN> --query JobRun.Arguments > run-args.json`.
  The output it wrote is the baseline for Step 9. `null` means the run used only the job's
  `DefaultArguments`, which the analyzer reads from `job.json`. If the job has no `SUCCEEDED` run,
  tell the user there is no successful run to migrate, and stop for that job: write a short
  `MIGRATION-REPORT.md` that says so, and why (for example, a pin with no arm64 build).
- Glue rejects connections and security configurations for Ray jobs, so a Glue for Ray job only
  reached public endpoints and relied on bucket default encryption. If its targets allowlisted Glue's
  IP ranges, they must allow the EKS cluster's traffic instead: the VPC's NAT gateway IPs for public
  endpoints, but for S3 through the VPC's gateway endpoint (Step 7) a bucket policy must use
  `aws:SourceVpce` or `aws:SourceVpc`, because those requests carry no `aws:SourceIp`.

### Step 2: Collect the job's code

Into `./glue-ray-eks-migration/<job>/source/`:
- `ScriptLocation` → `source/main.py`;
- each `--s3-py-modules` zip, and each `s3://` zip in `--pip-install` → download, then
  `python3 scripts/unpack_py_modules.py --out source/py_modules <zips...>`. It unpacks them the way
  Ray did on Glue: one directory per zip, the zip's single top-level directory stripped, and that
  directory on `sys.path`. A zip of `mypkg/` therefore never offered `import mypkg` on Glue. Keep the
  `PYTHONPATH` value it prints for Step 6. Each `.whl` → `wheels/`;
- the `--working-dir` zip → extract into `source/working_dir/` (for analysis only; at run time it
  stays an S3 `working_dir`).

Keep the code as it is here. The analyzer flags places where the Glue runtime differed from a plain
Ray container (Step 3); those are the only things that can make an unchanged script behave
differently on EKS.

### Step 3: Analyze

```bash
python3 scripts/analyze_glue_ray_job.py --job job.json --source-dir source \
  --out plan.json --report report.md --role-policies role-policies.json --role role.json \
  --run-args run-args.json
```

The report covers the capacity mapping (head + replicas, CPU parity, object-store bytes); the
environment variables injected, and env vars the script reads that no argument provides; the pod
role's policies, permissions boundary and tags (copied from the Glue role); the S3 locations the job
uses; configuration warnings; and code findings: Glue-runtime assumptions that don't hold on a plain
Ray container (hard-coded `/home/glue-ray-engine` or `/opt/amazon` paths, `sys.argv` use, a
hard-coded `ray.init()` address). These are not Ray version issues; the code otherwise runs
unchanged. Every Glue for Ray job runs on `Z.2X`, so the size defaults fit.

**Code changes.** If a finding means the job can't work on EKS as written (for example it asserts
`HOME=/home/glue-ray-engine` or reads files under `/opt/amazon`), propose the smallest edit that keeps
the job's behavior, as a diff, at the review gate (Step 5). Apply it only if the user approves: keep
the original as `source-glue/` and the diff as `code-changes.diff` in the job directory, and build
from the edited `source/`. Never add files, modules or wrappers of your own to the job's code.

**Grant inventory.** The pod role copies the Glue role's own policies, but access that another
resource grants by naming the Glue role (a bucket policy, a KMS key policy, a Lake Formation
permission) doesn't cover the pod role. Find those grants (read-only), into `resource-policies/`,
saving each call's output as `<name>.json` and its errors as `<name>.error` (`> <name>.json 2>
<name>.error`; the script reads the error when the call failed):
- for each bucket in the plan's `s3_access`: `aws s3api get-bucket-policy --bucket <B>` as
  `s3-<B>`, and its default encryption (`aws s3api get-bucket-encryption --bucket <B>`, as
  `s3-<B>-encryption.json`);
- for each customer-managed KMS key that default encryption names: `aws kms get-key-policy --key-id
  <KEY> --policy-name default` as `kms-<KEY>`. Keys named only in the Glue role's own policies need
  nothing more: the copied policies cover them. Objects encrypted with another key can't be found
  from the plan; if the user knows of one, check its key policy the same way;
- `aws lakeformation list-permissions`, every page, as `lakeformation-<n>` (all permissions the caller
  can see; the script keeps the Glue role's). The CLI doesn't page this call by itself, so follow
  `NextToken` (in bash):

```bash
n=1; tok=
while :; do
  aws lakeformation list-permissions --max-results 1000 ${tok:+--next-token "$tok"} \
    > resource-policies/lakeformation-$n.json 2> resource-policies/lakeformation-$n.error || break
  tok=$(jq -r '.NextToken // empty' resource-policies/lakeformation-$n.json); [ -n "$tok" ] || break; n=$((n+1))
done
```

```bash
python3 scripts/list_resource_grants.py --plan plan.json --dir resource-policies --out grants.md
```

`grants.md` lists every statement that names the Glue role, with the change the customer must make,
and the resources that couldn't be checked (owned by another account, or access denied). Never edit
those resources yourself: they belong to the customer and may be production. Show `grants.md` at the
review gate; the customer adds the pod role to each grant once Step 7 has created it, before the
test run (Step 8).

### Step 4: Resolve the dependencies

Copy `[BASE_REQUIREMENTS]` into the job directory unchanged. It holds the Glue Ray2.4 runtime's
packages that differ from the base image, and the modules Glue documents; the resolver installs
them first, as Glue's runtime had them. Then write `requirements.in` with the job's own requirements:
- the plan's `runtime_env.pip` entries (from `--pip-install` / `--additional-python-modules`);
- one line `<name>==<version>` for each plan `runtime_env.wheels` entry, after copying the wheel to
  `wheels/` (the resolver finds it through `--find-links /w/wheels`).

The resolver installs `requirements.in` on top with a plain `pip install`, as Glue did at job start:
an installed version that satisfies a requirement is kept, otherwise the job's requirement wins.

Run `scripts/resolve-deps.sh` **natively on the target architecture** inside the base image: read
`references/build.md` for the command (locally on an arm64 machine, or with AWS CodeBuild and
`PHASE=resolve`). It writes `out/final-requirements.txt` and `out/import-check.txt`. On failure,
classify it with `references/troubleshooting.md` (build time). `SOURCE-BUILT` is not a failure: no
wheel existed, so the sdist was built, as Glue did. For `NO-WHEEL` / `SDIST-BUILD-FAILED`, the Glue
job ran on arm64 Python 3.9, so it could not have installed the package either: check its run
history, and ask the user whether to drop the pin. For `RESOLVER-CONFLICT` and `TRANSITIVE-PIN`, ask
the user which pin to move. Never drop a pin silently.

### Step 5: Review gate (interaction)

Collect the must-ask decisions from `references/operator-decisions.md`: cluster, VPC/subnets,
encryption, observability, concurrency, and dashboard exposure. Then render the manifests:

```bash
python3 scripts/render_manifests.py --plan plan.json --out-dir manifests \
  --image <ECR_URI>:<TAG> --region <REGION> --namespace <NS> \
  --capacity automode --nodepool ray-z2x --arch arm64 --account-id <ACCOUNT_ID>
```

For a managed node group, pass `--capacity nodegroup --nodegroup <name>` (and `--toleration` for its
taints) instead of the NodePool flags; for amd64, `--arch amd64`. The ECR repository is created with
immutable tags, so the tag in the manifests always names the image that was reviewed. The pod role
is named `<job>-ray-pods` by default; pass `--role-arn` instead for a name the user gives. If KubeRay already runs on the cluster in a namespace other than `kuberay-system`, also pass
`--operator-namespace <ns>` (Step 7). The role is created under exactly that name in Step 7, so the
manifests reviewed here are the ones that run. The renderer prints the job's Kubernetes name
(`job`), the role ARN and the ServiceAccount names. The job's name is the Glue job name when that is
already DNS-safe and at most 32 characters; otherwise it is lowercased, made DNS-safe and shortened,
with a 5-character hash of the original name, so two jobs never share ServiceAccounts or a role
(`Sales_Daily_ETL` becomes `sales-daily-etl-<hash>`). Use exactly the printed names in the IAM trust
policy. Don't hand-edit rendered manifests, the entrypoint or the RayCluster spec.

Read `references/review-gate.md` and explain the points that apply to this job: how job arguments
reach the job (including secret-like ones and names that aren't shell identifiers), IAM parity and
the rewritten statements, S3 locations, privilege parity, dashboard exposure, the run deadline, and
where the logs are.

**Test-run output.** The first run (Steps 8 and 9) must not write where the Glue job writes, so it
can't corrupt production data. Show the user the job's output locations (the plan's
`s3_access.write`, any spill location, and Glue Catalog tables it writes, which arguments such as
`--glue-table` name) and ask where the test run
should write instead. Use exactly the locations the user names; they may name any location, including
a production one, but never choose one for them. Copy `run-args.json` (`{}` if it is `null`), set
the output arguments to the new locations (add those that come from `DefaultArguments`), re-run the
analyzer with it (`--run-args`) into `test-run/`, and render `test-run/manifests/` from that plan, so
its S3 policy and manifests agree. If the Glue role has a permissions boundary, the test-run
locations must be inside it. An output hard-coded in the script (analyzer `W-S3-LITERALS`, added as
WRITE) can't be redirected without a code change: tell the user that the test output then sits next
to the job's real output there (anything that reads that location sees both until the test files are
removed, and directory marker objects there may be rewritten), and run the test only if they explicitly accept that location; otherwise skip the test run.
`manifests/` keeps the Glue job's own locations, for cutover.

Validate each policy document in `manifests/iam/` (the `<name>.json` policies, not `role-tags.json`
or `trust.json`) and `test-run/manifests/s3-policy.json`:
`aws accessanalyzer validate-policy --policy-type IDENTITY_POLICY --policy-document file://<file>`.
Stop on an `ERROR` finding in a document the skill wrote or rewrote (`s3-policy.json`, or a
statement the analyzer rewrote). An `ERROR` or `SECURITY_WARNING` that the Glue role's own policy has
too is carried over: IAM accepted it there, so show it without changing anything (parity). If `ValidatePolicy` is
denied, say so and continue.

Present one bundle and wait for approval:
- `report.md` (mapping and findings);
- `code-changes.diff`, if a finding needs a code change (Step 3), applied only after approval;
- `final-requirements.txt` and `import-check.txt`;
- `manifests/rayjob.yaml`, `serviceaccounts.yaml`, `networkpolicy.yaml`, and the test-run
  `test-run/manifests/rayjob.yaml` with the locations the user named;
- `manifests/iam/` (the pod role's permissions, boundary and tags, as on the Glue role, with any
  rewritten statements) with the `validate-policy` results, and `test-run/manifests/s3-policy.json`
  if the test-run locations need it;
- `grants.md` (Step 3), with the grants the customer must add for the pod role;
- the capacity plan and what is billed: on Auto Mode, head + up to N−1 `[INSTANCE_TYPE]` on-demand
  instances per run and the EKS Auto Mode fee; on a node group, its nodes for as long as they run;
  plus the EKS cluster fee and NAT gateway data processing (see the Amazon EC2, EKS and VPC pricing
  pages). A fixed-size node group runs one job at a time unless it has `NumberOfWorkers` nodes for
  each job that may run at once; other runs stay `Pending` until nodes are free.

The user can edit `final-requirements.txt` and the manifests. Re-run the import check after
dependency edits.

### Step 6: Build, validate and push the image

Read `references/build.md`. The build context holds only the job's code and its dependencies
(`app/main.py`, `app/py_modules/`, `wheels/`, `final-requirements.txt`, the `Dockerfile` from
`templates/`). On an arm64 machine, build and push locally; on any other machine, use AWS CodeBuild
with `templates/buildspec.yml` (creating the project is billable and needs the user's confirmation),
first with `PHASE=resolve` for Step 4, then with `PHASE=build` from the reviewed lockfile. The image
check must print `2.4.0 3.9.x aarch64` (`x86_64` for an amd64 image). Share the ECR scan's base-image CVEs with the customer;
patching them, or moving to a newer Ray, is the customer's call.

### Step 7: Prepare the cluster

**Kubernetes context.** `kubectl` and `helm` act on whatever cluster the current kubeconfig context
points at, which may be another, even a production, cluster. Before the first command against the
cluster (right after `eksctl create cluster` for a new one), create a context for exactly the cluster
the user confirmed and check it:

```bash
aws eks update-kubeconfig --region <REGION> --name <C> --alias glue-ray-<C>
kubectl --context glue-ray-<C> config view --minify -o jsonpath='{.clusters[0].cluster.server}'
aws eks describe-cluster --region <REGION> --name <C> --query cluster.endpoint --output text
```

The two endpoints must match; show the context to the user. If the context already exists (another
migration created it), check its server the same way and reuse it. `update-kubeconfig` also makes the new
context the current one, which changes the user's own default: note `kubectl config current-context`
before it and switch back after (`kubectl config use-context <previous>`). From here on, pass
`--context glue-ray-<C>` to every `kubectl` command and `--kube-context glue-ray-<C>` to every `helm`
command (the commands in this skill leave it out for brevity, but always add it).

Read `references/cluster-setup.md` and follow it:
- **new cluster** (only after confirmation; this is billable): `eksctl` from
  `templates/cluster/eksctl-automode-cluster.yaml` (the user names the CIDRs allowed to reach the
  Kubernetes API; include this machine's public IP), an S3 gateway endpoint, the Ray NodePool, the
  NetworkPolicy ConfigMap, and the CloudWatch Observability add-on, which the template installs. A
  log pipeline is required: pod logs are deleted with their nodes shortly after each run;
- **existing cluster**: check its Kubernetes version, log pipeline, arm64 capacity (an Auto Mode
  NodePool, or a managed node group: `--capacity nodegroup --nodegroup <name>`, plus `--toleration`
  for its taints), OIDC provider, and the IMDS hop limit on node groups;
- **KubeRay**, pinned: an existing operator is shared, so don't install or upgrade over it without
  the user's explicit decision. Use it only if it is at least 1.7 and below 1.9; otherwise stop.
  On a cluster with no operator, install `[KUBERAY_VERSION]` with Helm;
- **the pod role (IRSA)**, created under the name the renderer printed, with the Glue role's
  policies, permissions boundary and tags, and the trust policy for the printed ServiceAccounts.

**NetworkPolicy enforcement (security), existing clusters.** Ray 2.4's dashboard and job API (port
8265 on the head) have no authentication: anyone who can reach that port can run code with the job's
IAM role. The rendered NetworkPolicy admits only the job's own pods and the KubeRay operator, but it
takes effect only if the cluster enforces NetworkPolicies, and EKS doesn't by default. Tell the user
this and ask them to confirm that enforcement is on before Step 8. If it isn't, point them to
[Use network policies with EKS Auto Mode](https://docs.aws.amazon.com/eks/latest/userguide/auto-net-pol.html)
or, on other clusters with the Amazon VPC CNI,
[Restrict Pod network traffic with Kubernetes network policies](https://docs.aws.amazon.com/eks/latest/userguide/cni-network-policy-configure.html);
background: [Ray security](https://docs.ray.io/en/latest/ray-security/index.html). Enabling it
changes the whole cluster, so it's the cluster owner's change. If the user continues without it,
record that in the report. A new cluster from the templates enforces it.

### Step 8: Submit and monitor

Before the first test run, confirm with the user that every grant in `grants.md` now also covers the
pod role, and that NetworkPolicy enforcement is confirmed (Step 7). The pod role exists now, so give
the user each change with its ARN: the statement to edit, or the `aws lakeformation
grant-permissions` command with `--principal DataLakePrincipalIdentifier=<pod role ARN>` and the row's
resource and permissions. Then check the ServiceAccounts:
if `kubectl -n <NS> get sa <SA>` finds one whose `eks.amazonaws.com/role-arn` annotation names
another role, stop. It belongs to another job or team, and applying would hand that job's pods this
role.

```bash
kubectl create namespace <NS> --dry-run=client -o yaml | kubectl apply -f -
# only if the plan has sensitive_args: one file per value, provided by the user
kubectl -n <NS> create secret generic <job>-job-args --from-file=<ARG>=<path> ...
kubectl apply -f manifests/serviceaccounts.yaml -f manifests/networkpolicy.yaml
kubectl create --dry-run=server -f test-run/manifests/rayjob.yaml
RAYJOB=$(kubectl create -f test-run/manifests/rayjob.yaml -o jsonpath='{.metadata.name}')   # test run
kubectl -n <NS> get rayjob $RAYJOB -w      # jobDeploymentStatus / jobStatus
kubectl -n <NS> logs -f job/$RAYJOB        # driver output (submitter); afterwards, the log pipeline
```

Success means `jobDeploymentStatus: Complete` and `jobStatus: SUCCEEDED`. On failure, classify it
with `references/troubleshooting.md` and tell the user the class, the evidence and the fix.

To end a run early (for example a stalled one), delete its RayJob: `kubectl -n <NS> delete rayjob
$RAYJOB` removes the RayCluster, the submitter Job and the Service with it. Don't use `ray job stop`:
it leaves the job `STOPPED`, which the cleanup rules can't match (KubeRay accepts only `SUCCEEDED`
and `FAILED` there), so the cluster keeps billing until `activeDeadlineSeconds`.

Every run is its own RayJob: run `kubectl create -f manifests/rayjob.yaml` again. The manifest uses
`generateName`, so each run gets a new name with no edits (use `create`, not `apply`). `JOB_RUN_ID`
is set to that run's RayCluster name: the RayJob's generated name plus a random suffix, so no two
current runs share it.

Cleanup is automatic, as on Glue. The RayJob's `deletionStrategy` deletes the RayCluster, and so
its nodes, 60 s after the job ends (`--ttl-seconds`), on success or failure, including a failed
submission. The RayJob object, with its submitter Job and `<RAYJOB>-head-svc` Service, costs nothing
and stays for a day (`--keep-seconds`) so its status can be read, then KubeRay deletes it. Whatever
now starts the runs (operator decision 17) only creates RayJobs and reads their status.

### Step 9: Confirm the run, and how the customer validates the output

The skill confirms that the migrated job runs:
- the RayJob finished with `jobDeploymentStatus: Complete` and `jobStatus: SUCCEEDED`;
- if the job scaled to its maximum, the peak `CPU` total equals the plan's
  `cluster.expected_total_cpus_at_max` (Glue parity). A lower peak is normal when the job never needed every
  worker. Check while the job runs: `kubectl exec <head> -c ray-head -- ray status`, where the head
  pod is `-l ray.io/cluster=<status.rayClusterName>,ray.io/node-type=head`. A job that ends within a
  minute or two may be gone before you can exec: the head pod's `autoscaler` container stream in the
  log pipeline records each resize instead;
- the run's duration against the Glue baseline run's `ExecutionTime`. Cluster start adds a minute or
  two; a run that is much slower beyond that is classified with troubleshooting `COLD-START` or
  `IDLE-SCALEDOWN`, and the options go in the report.

Validating the output is the customer's job: only they know their data contracts. Tell them how:
1. The test run (Step 8) read the **same input** as the Glue baseline run and wrote to the
   locations the user named in Step 5. At cutover, the job runs from `manifests/rayjob.yaml`, and any
   test-run inline policy is removed from the pod role.
2. Compare the two outputs in their own way: row counts, the schema, aggregates on key columns
   (sums, distinct counts, min/max), and spot checks of individual rows.
3. These can legitimately differ: row order and file layout; the last digits of floating-point
   results, because parallel tasks add numbers in a different order; and values that change per run
   (timestamps, UUIDs, run IDs, random sampling).
4. Anything else is a difference to investigate. The job and its dependency versions are unchanged,
   so the cause is configuration (CPU parity, object store, environment variables, S3 access), a
   Glue-runtime assumption the analyzer flagged (HOME, cwd, `sys.argv`), or a job that isn't
   deterministic.

### Step 10: Report

Write `./glue-ray-eks-migration/<job>/MIGRATION-REPORT.md` and show it. It contains:
- the source job, the Ray/Python versions, the image URI and digest, and the KubeRay version;
- any **code changes** the user approved (the finding, and `code-changes.diff`);
- a capacity and argument mapping table (from the analyzer report), and **dependency notes**:
  packages built from source, and any pin the user chose to move during resolution;
- the decisions made at the review gate: cluster, VPC and subnets, encryption, observability,
  concurrency, dashboard access, and any change to the idle timeout or instance size;
- an **IAM summary**: the pod role ARN, its policies, permissions boundary and tags, the
  ServiceAccounts it trusts, the `validate-policy` findings, and the grants from `grants.md` with
  whether the customer has added the pod role to each;
- whether NetworkPolicy enforcement was confirmed on the cluster (Step 7);
- the **run result**: the RayJob name, final status, duration, peak CPU against the Glue
  capacity, and where the test run wrote (the locations the user named); then what the customer
  compared on the output and what they found, or "not yet checked by the customer";
- **how to operate the job**: start a run with `kubectl create -f rayjob.yaml` (no per-run edits);
  the driver output is in the log pipeline (with the CloudWatch add-on: log group
  `/aws/containerinsights/<CLUSTER>/application`, the `ray-job-submitter` container's stream); the
  dashboard during a run is `kubectl port-forward svc/<RAYJOB>-head-svc 8265:8265`; cleanup is
  automatic (the RayCluster 60 s after the job ends, the RayJob object after a day);
- **differences from Glue that apply to this job**, only those the plan and analyzer found:
  - if the script reads the cluster size at start-up (`RAY-CLUSTER-SIZE`): on EKS it starts with
    the `--min-workers` floor already up, while on Glue it could start before all of those nodes had
    joined, so the value it reads can be larger;
  - task and actor output is in the driver stream (one CloudWatch stream), not in per-worker log
    streams as on Glue;
  - if the script reads `JOB_RUN_ID`: it is the run's RayCluster name (the RayJob's generated name
    plus a random suffix), not a `jr_...` id. No two current runs share it, and a repeat later is
    very unlikely but not impossible, unlike Glue's id;
  - tasks and actors can also see job arguments whose names are shell identifiers (on Glue only the
    driver could);
  - if the job has `--working-dir`: the driver's cwd is the working dir on EKS, while on Glue it was
    `/` (`references/arg-mapping.md`);
  - any Glue-role statement conditioned on the Glue credential keys, and how it was rewritten
    (`W-IAM-GLUE-CONDITION`);
- **What moved from AWS to the customer**: cluster provisioning and upgrades, KubeRay, node
  patching, capacity and quotas, IAM, networking, monitoring and alarms, base-image CVE patching, Ray
  version currency, and cost;
- what still calls the Glue job (triggers, workflows, Step Functions, Airflow). Each one must create
  a RayJob instead (`kubectl create -f rayjob.yaml`). Scheduling is the customer's choice: a
  Kubernetes CronJob, an orchestrator, or KubeRay `RayCronJob` (behind the `RayCronJob` feature gate,
  off by default);
- teardown commands for everything the migration created (the ledger). Leave the Glue job in place
  as the rollback path until the customer has validated the EKS job and retires it.

## Reference

- [AWS Glue for Ray end of support and migration guide](https://docs.aws.amazon.com/glue/latest/dg/awsglue-ray-jobs-availability-change.html)
- [Using job parameters in Ray jobs](https://docs.aws.amazon.com/glue/latest/dg/author-job-ray-job-parameters.html)
- [Providing files and Python libraries to Ray jobs](https://docs.aws.amazon.com/glue/latest/dg/edit-script-ray-env-dependencies.html)
- [KubeRay RayJob](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/rayjob-quick-start.html)
- [EKS Auto Mode NodeClass](https://docs.aws.amazon.com/eks/latest/userguide/create-node-class.html) / [NodePool](https://docs.aws.amazon.com/eks/latest/userguide/create-node-pool.html)
