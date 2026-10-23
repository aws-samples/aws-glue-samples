# What the rendered manifests do, for the review gate (Step 5)

Explain the points that apply to the job when you present the bundle.

## Job arguments

Job arguments are delivered as **container env** on the head and worker pods, so the driver and every
task and actor see them. On Glue only the driver did (tasks read `None`), so a job that worked on Glue
never needs them in tasks; the superset is harmless, except that secret-like arguments reach more
processes.

Names that are not shell identifiers (`--athena-workgroup`) would be dropped by the shells that start
Ray, so they reach the driver only, as on Glue: each value is a container env var with a valid name on
the head (`GLUE_ARG_<n>`), and the RayJob entrypoint sets the original name with `exec env`, and
removes the `GLUE_ARG_<n>` variables, before starting `python`. Don't edit that entrypoint by hand; its
quoting has to survive two shells. A name with characters a shell interprets (`;`, quotes, `$`, `=`) can't pass through it intact (analyzer
`W-ARG-NAME-SHELL`): tell the user. Arguments are not put in `runtimeEnvYAML`, because KubeRay passes
that through `bash -c`, which would expand `$` and backticks.

Arguments that look like secrets (plan `sensitive_args`) are rendered as `secretKeyRef` to Secret
`<job>-job-args`; their values are redacted from `plan.json`. Ask the user for the values and create the
Secret in Step 8 from files (`--from-file`), not `--from-literal`, so they stay out of shell history and
this transcript. Better: move them to Secrets Manager.

## Run deadline

Glue reports a `Timeout` of 480 minutes for every Ray job and records it on every run; it can't be set
for Ray jobs. The RayJob gets the same limit (`activeDeadlineSeconds: 28800`). Tell the user that a run
longer than 8 hours is stopped (troubleshooting `DEADLINE`), and use another value only if they choose
one (operator decision 15). Record the value in the report.

## IAM parity

The pod role gets the Glue role's permissions unchanged: `manifests/iam/` lists the managed policies to
attach as they are (`attach-policy-arns.txt`), holds a copy of every other policy, and the Glue role's
permissions boundary (`permissions-boundary.txt`) and tags (`role-tags.json`, plus `migrated-from` and
`glue-job`).

The one exception is a statement conditioned on a key that describes the Glue job's credentials:
`glue:CredentialIssuingService` (always `glue.amazonaws.com` on the job's requests) or
`glue:RoleAssumedBy` (never present on them). On EKS neither exists, so such a statement would evaluate
differently, and a Deny that relied on one would silently stop denying. The analyzer evaluates each
condition on those keys as it held for the Glue job and rewrites the statement to match: the keys
removed if they held, the statement dropped if they didn't (`W-IAM-GLUE-CONDITION`). Conditions on other
`glue:` keys (`glue:VpcIds`, ...) are request keys that apply the same way to the pods' Glue API calls,
so they stay. A `glue:` key or operator the analyzer doesn't know is an error (`W-IAM-GLUE-UNKNOWN`):
decide with the user how that statement should apply. Review every such warning with the user.

## S3 locations

The analyzer lists the S3 locations the job uses (`s3_access.read` / `s3_access.write`, from the
argument names and, for hard-coded URIs, the calls they're passed to; `W-S3-LITERALS`), and the renderer
turns them into `s3-policy.json`. The pod role doesn't need it for the job's own locations: the copied
policies cover them as the Glue role's did, and the grant inventory (`grants.md`) covers access that
came from the resources' own policies. It is for test-run locations that the Glue role can't reach. If
the Glue role has a permissions boundary, the boundary caps that grant too: test-run locations must be
inside it.

## Privilege parity

On Glue, the job's code and its dependencies ran as a non-root user (`glue-ray-engine`) with no `sudo`.
The `rayproject/ray` images run as user `ray` with passwordless `sudo`, which would give the job more
privilege than it had on Glue. So every Ray and submitter pod runs non-root with
`allowPrivilegeEscalation: false` and all capabilities dropped. This can't break a job that worked on
Glue, because it never had root there. The customer can relax it.

## Dashboard exposure

By default the skill does not expose the Ray dashboard: no Service of type LoadBalancer, NodePort or
Ingress, and a NetworkPolicy that admits only the job's own pods and the KubeRay operator's pods. On
Glue the customer had no network path to the dashboard, and this keeps that boundary. If the customer
wants to reach the dashboard, that exposure (and its security) is their decision. During a run,
`kubectl -n <NS> port-forward svc/<RAYJOB>-head-svc 8265:8265` reaches it through the Kubernetes API with
the user's own credentials, with no exposure, and works with the job's NetworkPolicy in place. It is gone
once the RayCluster is deleted.

## Where the logs are

- **Driver output**, which includes task and actor prints forwarded by Ray, is in the **submitter**:
  `kubectl -n <NS> logs [-f] job/<RAYJOB>`. It's readable only while the submitter pod and its node
  exist, which is shortly after the job ends; after that it is only in the log pipeline (Step 7).
- `kubectl logs` on the **head or worker pods** shows only Ray's start-up output, not the job's prints.
  (The public guide's `kubectl logs <pod-name>` is the head or worker case.)
- The driver output holds every line that tasks and actors print, each prefixed with the task or actor
  that printed it: the manifests set `RAY_DEDUP_LOGS=0`, because Ray's default deduplication of similar
  lines from different workers can drop lines and attribute others to the wrong task. This keeps the
  complete output that Glue shipped per worker, in one stream. Jobs whose tasks print a lot produce more
  log volume.
- `kubectl -n <NS> exec <head-pod> -c ray-head -- ray status` shows nodes and resources. `-c ray-head` is
  required because the head pod also runs the autoscaler container.

## The RayCluster spec

The renderer keeps the RayCluster spec to fields that the Ray 2.4 autoscaler's `ray.io/v1alpha1` client
tolerates: no `spec.rayClusterSpec` additions such as `metadata`, top-level `labels` or group
`resources`. Don't add them by hand. (The RayJob's own `metadata.labels` are fine.)
