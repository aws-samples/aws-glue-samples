#!/usr/bin/env python3
"""Render KubeRay manifests for a migrated Glue for Ray job from plan.json.

Standard library only. Writes, into --out-dir:
  rayjob.yaml           RayJob (ephemeral RayCluster per run, Glue-like); start each run with
                        `kubectl create -f rayjob.yaml` (generateName gives every run its own name)
  serviceaccounts.yaml  head / worker / submitter ServiceAccounts
  networkpolicy.yaml    deny-by-default ingress to the Ray pods of this job
  s3-policy.json        IAM policy for exactly the S3 locations in the plan (use it for a test run that
                        writes to locations the Glue role can't reach)
  iam/                  the pod role's permissions, as on the Glue role: attach-policy-arns.txt (managed
                        policies to attach unchanged), one <name>.json per policy to copy (inline, or
                        rewritten because of the Glue credential condition keys), permissions-boundary.txt
                        (the Glue role's boundary ARN, empty if none) and role-tags.json (its tags plus the
                        migration's own)
  runtime-env.yaml      the runtimeEnvYAML that is embedded in rayjob.yaml (for review)

Nothing of the skill runs in the cluster: the entrypoint is `python <script>`, and everything Glue did
around the script is expressed as manifest fields.
"""
import argparse
import hashlib
import json
import os
import re
import sys

# The migration keeps the Glue for Ray runtime version. The Ray 2.4 autoscaler speaks the KubeRay
# ray.io/v1alpha1 API, so the RayCluster spec stays within fields that client tolerates.
RAY_VERSION = "2.4.0"
MANAGED_LABEL = "glue-ray-migration/job"


# ---------------------------------------------------------------- minimal YAML emitter ----
def _scalar(v):
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v)
    if s == "" or not s.isprintable() or re.search(r"[:#{}\[\],&*?|<>=!%@`'\"]|^[\s-]|\s$|^(true|false|null|yes|no|on|off|y|n|~)$|"
                            r"^[0-9.+-]|[\x00-\x1f\x7f]", s, re.I):
        return json.dumps(s)
    return s


def _block_safe(v):
    # A literal block scalar round-trips only plain lines: the first line sets the indentation, and
    # control characters or several trailing newlines change the value. Anything else is JSON-quoted
    # (JSON strings are valid YAML).
    return (all(line.isprintable() for line in v.split("\n")) and not v[:1].isspace()
            and not v.endswith("\n\n"))


def to_yaml(obj, indent=0):
    pad = "  " * indent
    out = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, str) and "\n" in v and _block_safe(v):
                out.append(f"{pad}{k}: |-" if not v.endswith("\n") else f"{pad}{k}: |")
                out.extend(f"{pad}  {line}" for line in v.rstrip("\n").split("\n"))
            elif isinstance(v, (dict, list)) and v:
                out.append(f"{pad}{k}:")
                out.append(to_yaml(v, indent + 1))
            elif isinstance(v, (dict, list)):
                out.append(f"{pad}{k}: {'{}' if isinstance(v, dict) else '[]'}")
            else:
                out.append(f"{pad}{k}: {_scalar(v)}")
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, (dict, list)) and item:
                sub = to_yaml(item, indent + 1).split("\n")
                out.append(f"{pad}- {sub[0].lstrip()}")
                out.extend(sub[1:])
            else:
                out.append(f"{pad}- {_scalar(item)}")
    return "\n".join(out)


def dump_docs(path, docs):
    with open(path, "w") as f:
        f.write("\n---\n".join(to_yaml(d) for d in docs) + "\n")


# ----------------------------------------------------------------------------- helpers ----
def dns_name(s, max_len):
    s = re.sub(r"[^a-z0-9-]+", "-", s.lower()).strip("-")
    if not s or not s[0].isalpha():
        s = "g-" + s          # RayJob names must be DNS-1035 (start with a letter)
    return s[:max_len].rstrip("-")


def job_dns_name(name, max_len=32):
    """The job's Kubernetes name. Glue job names are case-sensitive and up to 255 characters, so when the
    DNS form differs from the name (case, characters, length), a short hash of the original keeps two jobs
    from sharing ServiceAccounts, the Secret, the NetworkPolicy and the label."""
    s = dns_name(name, max_len)
    if s == name:
        return s
    return dns_name(name, max_len - 6) + "-" + hashlib.sha1(name.encode()).hexdigest()[:5]


def placement(args):
    sel = {"kubernetes.io/arch": args.arch}
    tol = []
    if args.capacity == "automode":
        sel["karpenter.sh/nodepool"] = args.nodepool
        tol.append({"key": "workload", "operator": "Equal", "value": "ray", "effect": "NoSchedule"})
    elif args.capacity == "nodegroup":
        sel["eks.amazonaws.com/nodegroup"] = args.nodegroup
    for t in args.toleration or []:
        # key=value:Effect, for taints on a dedicated node group (Auto Mode's taint is added above)
        m = re.match(r"^([^=:]+)(?:=([^:]*))?:(NoSchedule|PreferNoSchedule|NoExecute)$", t)
        if not m:
            sys.exit(f"--toleration {t!r}: expected key=value:Effect")
        tol.append({"key": m.group(1), "operator": "Equal" if m.group(2) is not None else "Exists",
                    **({"value": m.group(2)} if m.group(2) is not None else {}), "effect": m.group(3)})
    return sel, tol


def container_resources(args, plan):
    mem = f"{int(plan['cluster']['pod_memory_gib'])}Gi"
    cpu = f"{float(plan['cluster']['pod_cpu']):g}"
    eph = f"{args.ephemeral_storage_gib}Gi"
    return {"requests": {"cpu": cpu, "memory": mem, "ephemeral-storage": eph},
            "limits": {"memory": mem, "ephemeral-storage": eph}}


def ray_env(args, plan, names, kind):
    # Glue exposed every job argument as an environment variable of the driver. They are set on the Ray
    # containers (so the driver, and also every task and actor, see them) rather than in runtimeEnvYAML:
    # KubeRay passes the runtime env through `bash -c`, which would expand `$` and backticks.
    env = []
    sensitive = set(plan.get("sensitive_args", []))
    for k, v in plan["runtime_env"]["env_vars"].items():
        if not ENV_NAME.match(k):
            continue  # see odd_args(): delivered through an alias on the head and the entrypoint
        env.append(_env_entry(k, k, v, sensitive, names))
    if kind == "head":
        for alias, k in odd_args(plan):
            env.append(_env_entry(alias, k, plan["runtime_env"]["env_vars"][k], sensitive, names))
    # Glue always injected a per-run id. Every run's RayCluster has its own name (the RayJob's generated
    # name plus a suffix), which KubeRay puts on every pod as the ray.io/cluster label.
    env.append({"name": "JOB_RUN_ID",
                "valueFrom": {"fieldRef": {"fieldPath": "metadata.labels['ray.io/cluster']"}}})
    env += [{"name": "AWS_REGION", "value": args.region},
            {"name": "AWS_DEFAULT_REGION", "value": args.region},
            # Glue shipped every worker's complete output (ray-worker-out-logs); on EKS the driver stream is the
            # only shipped copy, and Ray 2.4's line dedup can drop lines from it and misattribute others.
            {"name": "RAY_DEDUP_LOGS", "value": "0"}]
    if plan.get("spilling"):
        env.append({"name": "RAY_object_spilling_config", "value": plan["spilling"]["env_value"]})
    return env


def _env_entry(name, arg, value, sensitive, names):
    if arg in sensitive:
        return {"name": name, "valueFrom": {"secretKeyRef": {"name": names["secret"], "key": arg}}}
    return {"name": name, "value": str(value)}


def odd_args(plan):
    """(alias, name) for job arguments whose names are not shell identifiers, in a stable order."""
    odd = sorted(k for k in plan["runtime_env"]["env_vars"] if not ENV_NAME.match(k))
    return [(f"GLUE_ARG_{n}", k) for n, k in enumerate(odd, 1)]


def entrypoint(plan, args):
    """`python <script>`, prefixed with `env` for job arguments whose names are not shell identifiers.

    The entrypoint goes through two shells: KubeRay's submitter command, then `ray job submit`, which
    re-joins it with subprocess.list2cmdline, and Ray's job supervisor, which runs it with shell=True.
    Values are therefore never written here: each is a container env var with a valid name
    (GLUE_ARG_<n>) and is only referenced. The single quotes stop the submitter shell from expanding
    the reference; in the supervisor shell, `set -f` and an empty IFS keep the expanded value as one
    word with no globbing, and `exec env` sets the original name, which no shell would keep, and removes
    the GLUE_ARG_<n> aliases (`-u`), so the driver's environment is the one Glue gave it. `--` ends env's
    options, so a name that starts with `-` (a Glue argument such as `-a`) isn't read as one.
    """
    odd = odd_args(plan)
    if not odd:
        return f"python {args.script_path}"
    unset = " ".join(f"-u {alias}" for alias, _ in odd)
    pairs = " ".join(f"'{k}=${alias}'" for alias, k in odd)
    return f"set -f ';' IFS= ';' exec env {unset} -- {pairs} python {args.script_path}"


def start_params(num_cpus, os_bytes):
    p = {"num-cpus": str(num_cpus)}
    if os_bytes:
        p["object-store-memory"] = str(os_bytes)
    return p


# Glue ran the script as an unprivileged user that could not sudo. The Ray images give user `ray`
# (uid 1000, gid 100) passwordless sudo, so block privilege escalation at the pod level to keep parity.
POD_SECURITY = {"runAsNonRoot": True, "runAsUser": 1000, "runAsGroup": 100, "fsGroup": 100,
                "seccompProfile": {"type": "RuntimeDefault"}}
CONTAINER_SECURITY = {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}}


def pod_template(args, plan, kind, sa, job_label, names):
    sel, tol = placement(args)
    container = {
        "name": f"ray-{kind}",
        "image": args.image,
        "imagePullPolicy": "IfNotPresent",
        "resources": container_resources(args, plan),
        "env": ray_env(args, plan, names, kind),
        "securityContext": dict(CONTAINER_SECURITY),
    }
    if kind == "head":
        container["ports"] = [{"containerPort": 6379, "name": "gcs-server"},
                              {"containerPort": 8265, "name": "dashboard"},
                              {"containerPort": 10001, "name": "client"}]
    spec = {"serviceAccountName": sa, "securityContext": dict(POD_SECURITY), "containers": [container],
            "nodeSelector": sel}
    if tol:
        spec["tolerations"] = tol
    return {"metadata": {"labels": {MANAGED_LABEL: job_label},
                         "annotations": {"karpenter.sh/do-not-disrupt": "true"}},
            "spec": spec}


# Glue sets every job argument as an env var, including names like `athena-workgroup`. Shells drop such
# names (KubeRay starts Ray through bash; Ray 2.4 starts the job entrypoint with shell=True), so they
# would never reach Python. entrypoint() restores them for the driver, which is where Glue set them.
ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def runtime_env(plan):
    # Dependencies are baked into the image; only --working-dir is fetched at start, as on Glue.
    r = plan["runtime_env"]
    renv = {}
    if r.get("working_dir"):
        renv["working_dir"] = r["working_dir"]
        if r.get("setup_timeout_seconds"):
            renv["config"] = {"setup_timeout_seconds": int(r["setup_timeout_seconds"])}
    return renv


def build_rayjob(plan, args, names):
    c = plan["cluster"]
    job_label = names["job"]
    head = {
        "rayStartParams": start_params(c["head"]["num_cpus"], c["head"]["object_store_bytes"]),
        "template": pod_template(args, plan, "head", names["sa_head"], job_label, names),
    }
    worker = {
        "groupName": "workers",
        "replicas": c["workers"]["replicas"],
        "minReplicas": c["workers"]["min_replicas"],
        "maxReplicas": c["workers"]["max_replicas"],
        "rayStartParams": start_params(c["workers"]["num_cpus"], c["workers"]["object_store_bytes"]),
        "template": pod_template(args, plan, "worker", names["sa_worker"], job_label, names),
    }
    has_workers = c["workers"]["max_replicas"] > 0
    cluster_spec = {
        "rayVersion": RAY_VERSION,
        # A head-only job (NumberOfWorkers 1) has nothing to scale, so it runs without the autoscaler.
        "enableInTreeAutoscaling": has_workers,
        "headGroupSpec": head,
        "workerGroupSpecs": [worker] if has_workers else [],
    }
    if has_workers:
        cluster_spec["autoscalerOptions"] = {
            "idleTimeoutSeconds": (args.idle_timeout_seconds if args.idle_timeout_seconds is not None
                                   else c["idle_timeout_seconds"]),
            "upscalingMode": "Default",
            "resources": {"requests": {"cpu": "200m", "memory": "512Mi"}, "limits": {"memory": "512Mi"}},
            # The sidecar runs Ray's autoscaler from the head image; the job's --s3-py-modules on the image's
            # PYTHONPATH must not shadow the packages Ray itself imports.
            "env": [{"name": "PYTHONPATH", "value": ""}],
            "securityContext": dict(CONTAINER_SECURITY)}
    sel, tol = placement(args)
    submitter_spec = {
        "restartPolicy": "Never",
        "serviceAccountName": names["sa_submitter"],
        "automountServiceAccountToken": False,
        "securityContext": dict(POD_SECURITY),
        "containers": [{"name": "ray-job-submitter", "image": args.image,
                        "securityContext": dict(CONTAINER_SECURITY),
                        "resources": {"requests": {"cpu": "200m", "memory": "512Mi"},
                                      "limits": {"memory": "1Gi"}}}],
        "nodeSelector": sel,
    }
    if tol:
        submitter_spec["tolerations"] = tol

    spec = {"entrypoint": entrypoint(plan, args)}
    renv = runtime_env(plan)
    if renv:
        spec["runtimeEnvYAML"] = to_yaml(renv) + "\n"
    spec.update({
        "submissionMode": "K8sJobMode",
        # Glue cleaned up after every run. The RayCluster (and so its nodes) goes ttl-seconds after the job
        # ends, whatever the outcome; the RayJob object (no cost) stays keep-seconds so its status can be read.
        "deletionStrategy": {"deletionRules": [
            rule for cond in ({"jobStatus": "SUCCEEDED"}, {"jobStatus": "FAILED"},
                              {"jobDeploymentStatus": "Failed"})
            for rule in ({"condition": {**cond, "ttlSeconds": args.ttl_seconds}, "policy": "DeleteCluster"},
                         {"condition": {**cond, "ttlSeconds": args.keep_seconds}, "policy": "DeleteSelf"})]},
        "submitterPodTemplate": {"metadata": {"labels": {MANAGED_LABEL: job_label}},
                                 "spec": submitter_spec},
        "rayClusterSpec": cluster_spec,
    })
    if plan["run"].get("active_deadline_seconds"):
        spec["activeDeadlineSeconds"] = plan["run"]["active_deadline_seconds"]
    if plan["run"].get("backoff_limit"):
        spec["backoffLimit"] = plan["run"]["backoff_limit"]

    meta = {"generateName": names["rayjob_prefix"], "namespace": args.namespace,
            "labels": {MANAGED_LABEL: job_label}}
    return {"apiVersion": "ray.io/v1", "kind": "RayJob", "metadata": meta, "spec": spec}


def build_service_accounts(args, names):
    docs = []
    for sa, needs_aws in ((names["sa_head"], True), (names["sa_worker"], True),
                          (names["sa_submitter"], False)):
        meta = {"name": sa, "namespace": args.namespace}
        if needs_aws:
            meta["annotations"] = {"eks.amazonaws.com/role-arn": args.role_arn}
        doc = {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": meta}
        if not needs_aws:
            doc["automountServiceAccountToken"] = False
        docs.append(doc)
    return docs


def build_network_policy(args, names):
    # Only this job's own pods and the KubeRay operator's pods (the operator polls the dashboard for job
    # status) may reach the Ray pods. Everything else, including other pods in the VPC, is denied. Both
    # selectors sit in one `from` entry, so a pod must be in the operator namespace AND be the operator.
    return {
        "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
        "metadata": {"name": f"{names['job']}-ray-ingress", "namespace": args.namespace},
        "spec": {
            "podSelector": {"matchLabels": {MANAGED_LABEL: names["job"]}},
            "policyTypes": ["Ingress"],
            "ingress": [
                {"from": [{"podSelector": {"matchLabels": {MANAGED_LABEL: names["job"]}}}]},
                {"from": [{"namespaceSelector": {"matchLabels": {
                               "kubernetes.io/metadata.name": args.operator_namespace}},
                           "podSelector": {"matchLabels": {"app.kubernetes.io/name": "kuberay-operator"}}}],
                 "ports": [{"protocol": "TCP", "port": 8265}]},
            ],
        },
    }


def build_s3_policy(plan):
    def split(uris):
        out = []
        for u in uris:
            m = re.match(r"s3a?://([^/]+)/?(.*)", u)
            if m:
                out.append((m.group(1), m.group(2)))
        return out

    def object_arns(pairs):
        arns = set()
        for bucket, key in pairs:
            key = key.rstrip("/")
            if not key:
                arns.add(f"arn:aws:s3:::{bucket}/*")
                continue
            # A location may be an object or a prefix ("daily.parquet" is a directory for Ray Data
            # writes), so grant both the key and everything under it.
            arns.add(f"arn:aws:s3:::{bucket}/{key}")
            arns.add(f"arn:aws:s3:::{bucket}/{key}/*")
        return sorted(arns)

    read = split(plan["s3_access"]["read"])
    write = split(plan["s3_access"]["write"])
    if plan.get("spilling") and plan["spilling"].get("s3"):
        write += split(re.findall(r"s3://[^\"' ,}]+", plan["spilling"]["env_value"]))
    stmts = []
    # ListBucket stays bucket-wide: HeadBucket (pyarrow's S3FileSystem calls it before every write)
    # is authorized as s3:ListBucket and carries no s3:prefix, so a prefix condition denies it.
    # Object reads and writes below remain scoped to the job's locations.
    for n, bucket in enumerate(sorted({b for b, _ in read + write})):
        stmts.append({"Sid": f"ListBucket{n}", "Effect": "Allow",
                      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
                      "Resource": f"arn:aws:s3:::{bucket}"})
    if read:
        stmts.append({"Sid": "ReadJobInputsAndCode", "Effect": "Allow",
                      "Action": ["s3:GetObject"], "Resource": object_arns(read)})
    if write:
        stmts.append({"Sid": "WriteJobOutputs", "Effect": "Allow",
                      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                                 "s3:AbortMultipartUpload"], "Resource": object_arns(write)})
        # pyarrow's S3FileSystem.create_dir (Ray Data writes, pq.write_to_dataset) puts an empty
        # "<prefix>/" marker object for every parent of the output location, the location included.
        markers = set()
        for bucket, key in write:
            parts = [x for x in key.split("/") if x]
            for i in range(1, len(parts) + 1):
                markers.add(f"arn:aws:s3:::{bucket}/{'/'.join(parts[:i])}/")
        if markers:
            stmts.append({"Sid": "CreateDirMarkers", "Effect": "Allow", "Action": ["s3:PutObject"],
                          "Resource": sorted(markers)})
    return {"Version": "2012-10-17", "Statement": stmts}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--plan", required=True)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--image", required=True, help="ECR image built for this job")
    p.add_argument("--region", required=True)
    p.add_argument("--namespace", default="ray-jobs")
    p.add_argument("--operator-namespace", default="kuberay-system")
    p.add_argument("--arch", default="arm64", choices=["arm64", "amd64"])
    p.add_argument("--capacity", default="automode", choices=["automode", "nodegroup"],
                   help="automode: the Ray NodePool; nodegroup: a managed node group (--nodegroup)")
    p.add_argument("--nodepool", default="ray-z2x")
    p.add_argument("--nodegroup", default="ray-z2x")
    p.add_argument("--toleration", action="append",
                   help="key=value:Effect for each taint on the target nodes (repeatable); needed for a "
                        "tainted, dedicated node group")
    p.add_argument("--role-arn", default=None,
                   help="IAM role for the Ray pods (IRSA annotation); it may be created later, in Step 7. "
                        "Default: arn:aws:iam::<--account-id>:role/<job>-ray-pods")
    p.add_argument("--account-id", default=None, help="AWS account ID, for the default --role-arn")
    p.add_argument("--script-path", default="/home/ray/app/main.py")
    p.add_argument("--ttl-seconds", type=int, default=60,
                   help="seconds the RayCluster (and its nodes) stays up after the job ends, on success or "
                        "failure; pod logs go with it, so ship them to a log pipeline (cluster setup)")
    p.add_argument("--keep-seconds", type=int, default=86400,
                   help="seconds the RayJob object stays after the job ends, so its status can be read; "
                        "then KubeRay deletes it with its submitter Job and Service")
    p.add_argument("--ephemeral-storage-gib", type=int, default=100)
    p.add_argument("--idle-timeout-seconds", type=int, default=None,
                   help="override the autoscaler idle timeout from the plan (600 s unless the job set "
                        "--auto-scaling-executor-idle-timeout); see references/operator-decisions.md #20")
    a = p.parse_args()

    with open(a.plan) as f:
        plan = json.load(f)
    if a.keep_seconds < a.ttl_seconds:
        sys.exit("--keep-seconds must be at least --ttl-seconds")

    if not plan.get("job_name"):
        sys.exit("plan.json has no job_name: re-run the analyzer on the job's get-job output")
    # Kubernetes appends 5 random characters to generateName; KubeRay derives longer names from the
    # RayJob name, so job (<= 32) + "-" + 5 stays well within its limits.
    job = job_dns_name(plan["job_name"])
    if not a.role_arn:
        if not a.account_id:
            sys.exit("pass --role-arn, or --account-id for the default role name <job>-ray-pods")
        a.role_arn = f"arn:aws:iam::{a.account_id}:role/{job}-ray-pods"
    names = {"job": job, "rayjob_prefix": f"{job}-", "secret": f"{job}-job-args",
             "sa_head": f"{job}-ray-head", "sa_worker": f"{job}-ray-worker",
             "sa_submitter": f"{job}-ray-submitter"}

    os.makedirs(a.out_dir, exist_ok=True)
    rayjob = build_rayjob(plan, a, names)
    dump_docs(os.path.join(a.out_dir, "rayjob.yaml"), [rayjob])
    with open(os.path.join(a.out_dir, "runtime-env.yaml"), "w") as f:
        f.write(rayjob["spec"].get("runtimeEnvYAML", "# empty: dependencies are baked into the image\n"))
    dump_docs(os.path.join(a.out_dir, "serviceaccounts.yaml"), build_service_accounts(a, names))
    dump_docs(os.path.join(a.out_dir, "networkpolicy.yaml"), [build_network_policy(a, names)])
    s3_policy = build_s3_policy(plan)
    s3_policy_path = os.path.join(a.out_dir, "s3-policy.json")
    if s3_policy["Statement"]:
        with open(s3_policy_path, "w") as f:
            json.dump(s3_policy, f, indent=2)
    else:   # an IAM policy needs at least one statement; the plan lists no S3 locations
        if os.path.exists(s3_policy_path):
            os.remove(s3_policy_path)
        print("NOTE: the plan lists no S3 locations, so no s3-policy.json was written.", file=sys.stderr)
    iam_dir = os.path.join(a.out_dir, "iam")
    os.makedirs(iam_dir, exist_ok=True)
    with open(os.path.join(iam_dir, "attach-policy-arns.txt"), "w") as f:
        f.writelines(x["attach_arn"] + "\n" for x in plan.get("iam_policies", []) if "attach_arn" in x)
    for x in plan.get("iam_policies", []):
        if "document" in x:
            with open(os.path.join(iam_dir, re.sub(r"[^A-Za-z0-9+=,.@_-]", "-", x["name"]) + ".json"), "w") as f:
                json.dump(x["document"], f, indent=2)
    role = plan.get("iam_role") or {}
    with open(os.path.join(iam_dir, "permissions-boundary.txt"), "w") as f:
        f.write(role["permissions_boundary"] + "\n" if role.get("permissions_boundary") else "")
    ours = {"migrated-from": "glue-ray", "glue-job": plan["job_name"]}
    tags = [t for t in role.get("tags", []) if t["Key"] not in ours] + [{"Key": k, "Value": v} for k, v in ours.items()]
    with open(os.path.join(iam_dir, "role-tags.json"), "w") as f:
        json.dump(tags, f, indent=2)
    if not plan.get("iam_role"):
        print("WARNING: the plan has no Glue role details (analyzer --role); the pod role would get no "
              "permissions boundary or tags.", file=sys.stderr)
    if not plan.get("iam_policies"):
        print("WARNING: the plan has no Glue role policies (analyzer --role-policies); iam/ is empty.",
              file=sys.stderr)
    if plan.get("sensitive_args"):
        print(f"NOTE: {len(plan['sensitive_args'])} sensitive argument(s) are read from Secret "
              f"{names['secret']}. Create it before applying, from files holding the values: kubectl -n "
              f"{a.namespace} create secret generic {names['secret']} "
              + " ".join(f"--from-file={k}=<file>" for k in plan["sensitive_args"]),
              file=sys.stderr)
    print(json.dumps({"job": job, "role_arn": a.role_arn, "rayjob_generate_name": names["rayjob_prefix"], "namespace": a.namespace,
                      "iam": "irsa", "service_accounts": [names["sa_head"], names["sa_worker"]],
                      "out_dir": a.out_dir}))


if __name__ == "__main__":
    main()
