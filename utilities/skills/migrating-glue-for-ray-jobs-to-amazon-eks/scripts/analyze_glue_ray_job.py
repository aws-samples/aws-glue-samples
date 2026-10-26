#!/usr/bin/env python3
"""Analyze an AWS Glue for Ray job and produce a KubeRay migration plan.

Standard library only, so it runs anywhere Python 3.8+ is available.

Inputs:
  --job         JSON from `aws glue get-job` (either the full response or its `Job` object)
  --source-dir  directory holding the job's code: main.py, plus the extracted --working-dir
                contents and --s3-py-modules packages (anything else that ships with the job)
  --role-policies / --role       every policy of the Glue job role, and its get-role output
  --pod-memory-gib / --pod-cpu   allocatable size of one Ray pod (default: r7g.2xlarge ~= Z.2X)

Outputs:
  --out     plan.json consumed by render_manifests.py
  --report  human-readable migration report (markdown)

Exit code is 0 even when findings exist; findings are data for the review gate.
"""
import argparse
import ast
import fnmatch
import json
import os
import re
import urllib.parse
import sys

# --------------------------------------------------------------------------------------------
# Glue for Ray runtime contract (see references/arg-mapping.md)
# --------------------------------------------------------------------------------------------
GLUE_RAY_DEFAULT_IDLE_TIMEOUT_MIN = 1      # autoscaler idle_timeout_minutes
# On EKS a removed worker takes minutes to replace and loses the objects it holds (Glue's own controller managed
# its nodes), so without an explicit --auto-scaling-executor-idle-timeout the RayCluster keeps idle workers longer.
EKS_DEFAULT_IDLE_TIMEOUT_S = 600
GLUE_RAY_HEAD_CPUS = 8                     # head advertises CPU: 8 and runs tasks/actors
GLUE_RAY_WORKER_CPUS = 8                   # Z.2X = 8 vCPU, one Ray worker per vCPU
GLUE_RAY_NODE_MEMORY_GIB = 64              # Z.2X = 64 GB; object-store % is taken of this total
GLUE_RAY_PY_LIBS_INSTALL_TIMEOUT_S = 600   # --py-libs-install-timeout default

# Arguments the Glue for Ray runtime itself interprets. Everything else is a customer argument
# that Glue exposed to the script as an environment variable (name without the leading "--").
RUNTIME_ARGS = {
    "--min-workers", "--auto-scaling-ray-min-workers", "--auto-scaling-max-workers",
    "--auto-scaling-executor-idle-timeout", "--pip-install", "--additional-python-modules",
    "--s3-py-modules", "--working-dir", "--object_store_memory_head",
    "--object_store_memory_worker", "--object_spilling_config", "--logging_configuration",
    "--py-libs-install-timeout", "--shell-init", "--workers_on_head_node",
}
# Glue-platform arguments that have no meaning on Ray/EKS; dropped with a note.
PLATFORM_ARGS = {
    "--enable-metrics", "--enable-continuous-cloudwatch-log", "--enable-glue-datacatalog",
    "--enable-job-insights", "--enable-observability-metrics", "--job-language",
    "--TempDir", "--enable-spark-ui", "--spark-event-logs-path", "--job-bookmark-option",
    "--extra-py-files", "--extra-jars", "--extra-files", "--enable-auto-scaling",
    "--python-modules-installer-option",
}
# Argument names whose S3 URIs are written to (everything else is treated as read-only).
WRITE_HINT = re.compile(r"(^|[-_.])(out|dest|dst|target|sink|write|spill|result|export|save)", re.I)
VALID_BUCKET = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
GLUE_PATH = re.compile(r"(/home/glue-ray-engine|/opt/amazon/|glue_manta_controller|/var/log/glue)")
S3_URI = re.compile(r"s3a?://[A-Za-z0-9.\-_]+(/[^\s,\"'}\])]*)?")
# An argument that names a bucket without a URI (`--source_bucket my-data`) is read from as s3://<bucket>/.
BUCKET_ARG = re.compile(r"bucket", re.I)


def s3_uris(text):
    # Trailing sentence punctuation isn't part of a location ("... in s3://bucket/prefix.").
    return [m.group(0).rstrip(".") for m in S3_URI.finditer(text)]


def load_job(path):
    with open(path) as f:
        data = json.load(f)
    return data.get("Job", data)


def split_csv(value):
    return [v.strip() for v in (value or "").split(",") if v.strip()]


def to_int(value, default=None):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


# --------------------------------------------------------------------------------------------
# Argument and configuration mapping
# --------------------------------------------------------------------------------------------
# Matched against whole parts of an argument name (split at -, _, . and camelCase), and pairs of adjacent
# parts, so `db-password`, `apiKey` and `AUTH_TOKEN` match but `tokenizer` and `passthrough` don't.
SENSITIVE_PARTS = {"pass", "passwd", "password", "pwd", "secret", "token", "credential", "credentials", "creds",
                   "apikey", "privatekey", "dsn", "connstr"}
# Characters an unquoted shell word keeps as they are. A name that isn't a shell identifier reaches the driver
# through the RayJob entrypoint (render_manifests.py entrypoint()), whose last shell sees the name unquoted.
# A leading `-` is safe: the entrypoint ends env's options with `--`.
SHELL_SAFE_NAME = re.compile(r"^[A-Za-z0-9_.:/@%+,-]+$")


def is_sensitive(name):
    parts = [p.lower() for p in re.findall(r"[A-Z]+(?![a-z])|[A-Z]?[a-z]+|[0-9]+", name)]
    return any(p in SENSITIVE_PARTS for p in parts + [a + b for a, b in zip(parts, parts[1:])])


def map_job(job, pod_memory_gib, pod_cpu, warnings, run_args=None):
    # Effective arguments, as Glue merged them: defaults < run-time arguments < non-overridable.
    args = dict(job.get("DefaultArguments") or {})
    args.update(run_args or {})
    args.update(job.get("NonOverridableArguments") or {})
    args = {k: str(v) for k, v in args.items()}
    cmd = job.get("Command") or {}
    if cmd.get("Name") != "glueray":
        warnings.append({"id": "W-NOT-RAY", "severity": "error",
                         "message": f"Command.Name is {cmd.get('Name')!r}, not 'glueray'. This skill "
                                    "only migrates Glue for Ray jobs."})
    if job.get("WorkerType") not in (None, "Z.2X"):
        # Glue rejects any other worker type for Ray jobs, so this means the job.json isn't a Glue for
        # Ray job definition (edited by hand, or from another job type).
        warnings.append({"id": "W-WORKER-TYPE", "severity": "error",
                         "message": f"WorkerType {job.get('WorkerType')} is not Z.2X, which Glue for Ray "
                                    "requires. Check that job.json is the real get-job output."})

    # Worker counts. Glue counts the head node in both min and max (the autoscaler gets value - 1).
    n_workers = to_int(job.get("NumberOfWorkers"), None)
    max_arg = to_int(args.get("--auto-scaling-max-workers"), None)
    if not n_workers and not max_arg:
        # Glue requires NumberOfWorkers for Ray jobs, so this isn't the real get-job output.
        warnings.append({"id": "W-NO-WORKERS", "severity": "error",
                         "message": f"NumberOfWorkers is {job.get('NumberOfWorkers')!r} and "
                                    "--auto-scaling-max-workers isn't set, so the job's capacity is unknown. "
                                    "Glue requires NumberOfWorkers for Ray jobs. Check that job.json is the real "
                                    "get-job output."})
    total_max = min(x for x in (n_workers, max_arg) if x) if (n_workers or max_arg) else 1
    min_raw = args.get("--min-workers", args.get("--auto-scaling-ray-min-workers"))
    total_min = to_int(min_raw, 0)
    if total_min > total_max:
        warnings.append({"id": "W-MIN-GT-MAX", "severity": "error",
                         "message": f"--min-workers {total_min} exceeds NumberOfWorkers {total_max}; "
                                    "Glue rejects this. Fix the source values first."})
        total_min = total_max
    max_replicas = max(total_max - 1, 0)
    min_replicas = max(total_min - 1, 0)
    idle_arg = to_int(args.get("--auto-scaling-executor-idle-timeout"), None)
    glue_idle_min = idle_arg if idle_arg is not None else GLUE_RAY_DEFAULT_IDLE_TIMEOUT_MIN
    idle_s = idle_arg * 60 if idle_arg is not None else EKS_DEFAULT_IDLE_TIMEOUT_S

    workers_on_head = to_int(args.get("--workers_on_head_node"), 1)
    head_cpus = GLUE_RAY_HEAD_CPUS if workers_on_head != 0 else 0

    # Object store: Glue gives a percentage of the node's total memory; KubeRay wants bytes.
    # On EKS the pod memory limit is the real ceiling (/dev/shm is an emptyDir counted against it).
    pod_mem_bytes = int(pod_memory_gib * 1024 ** 3)

    def object_store_bytes(key):
        pct = to_int(args.get(key), None)
        if pct is None:
            return None, None
        if not 1 <= pct <= 100:
            warnings.append({"id": "W-OBJSTORE-RANGE", "severity": "error",
                             "message": f"{key}={pct} is outside 1..100."})
            return pct, None
        if pct > 80:
            warnings.append({"id": "W-OBJSTORE-HIGH", "severity": "warn",
                             "message": f"{key}={pct}% leaves little heap for tasks inside the pod "
                                        "limit. On Glue the % applied to 64 GB of host RAM; on EKS it "
                                        "applies to the pod limit and shares the cgroup with the "
                                        "Ray worker processes. Consider lowering it."})
        return pct, int(pod_mem_bytes * pct / 100)

    head_pct, head_os = object_store_bytes("--object_store_memory_head")
    worker_pct, worker_os = object_store_bytes("--object_store_memory_worker")

    # Object spilling: passed straight through by Glue; on Ray it is a system config, not a flag.
    spilling = None
    raw_spill = args.get("--object_spilling_config")
    if raw_spill:
        try:
            spill = json.loads(raw_spill)
            spilling = {"config": spill, "env_value": json.dumps(spill, separators=(",", ":"))}
            if "smart_open" in raw_spill:
                spilling["needs"] = ["smart_open[s3]", "boto3"]
                spilling["s3"] = True
            warnings.append({"id": "W-SPILL", "severity": "info",
                             "message": "Object spilling is set through the RAY_object_spilling_config "
                                        "env var on every Ray container (there is no ray start flag). "
                                        "Spill objects get the spill bucket's default encryption."})
            if idle_s < EKS_DEFAULT_IDLE_TIMEOUT_S:
                warnings.append({"id": "W-IDLE-SPILL", "severity": "warn",
                                 "message": f"The job spills objects and sets an idle timeout of {idle_s} s. On EKS "
                                            "the Ray 2.4 autoscaler can remove a worker whose CPUs sit idle while it "
                                            "waits on spilled objects, losing the objects it holds; Ray then tries to "
                                            "rebuild them from lineage and the run can slow down badly or stall. Ask "
                                            "the user about the default (render_manifests.py --idle-timeout-seconds "
                                            f"{EKS_DEFAULT_IDLE_TIMEOUT_S}; operator decision 20)."})
        except ValueError:
            warnings.append({"id": "W-SPILL-JSON", "severity": "error",
                             "message": "--object_spilling_config is not valid JSON."})

    # Dependencies. Glue merges --pip-install and --additional-python-modules; s3:// entries become
    # py_modules, everything else is a pip requirement.
    pip, py_modules, wheels = [], [], []
    for key in ("--pip-install", "--additional-python-modules"):
        for entry in split_csv(args.get(key)):
            (py_modules if entry.lower().startswith("s3://") else pip).append(entry)
    py_modules += split_csv(args.get("--s3-py-modules"))
    # .whl files are installed into the image (resolve-deps.sh), not shipped as py_modules zips
    wheels = [m for m in py_modules if m.lower().endswith(".whl")]
    py_modules = [m for m in py_modules if not m.lower().endswith(".whl")]
    working_dir = args.get("--working-dir")
    if working_dir and not re.match(r"^s3://.+\.zip$", working_dir, re.I):
        warnings.append({"id": "W-WORKDIR", "severity": "error",
                         "message": "--working-dir must be an s3://...zip (Glue fails other values)."})

    for field, what in (("Connections", "connections"), ("SecurityConfiguration", "a security configuration")):
        if job.get(field):
            warnings.append({"id": f"W-{field.upper()}", "severity": "warn",
                             "message": f"The job definition lists {what}, but Glue rejects {what} for Ray "
                                        "jobs (CreateJob: 'not supported for Glue Ray jobs'), so it never "
                                        "applied. Nothing to migrate; confirm with the user."})
    if args.get("--extra-py-files"):
        warnings.append({"id": "W-EXTRA-PY", "severity": "warn",
                         "message": "--extra-py-files has no effect on Glue for Ray, so the job never "
                                    "used those files. They are not migrated. If the script imports "
                                    "them, it was getting them another way; check before shipping."})
    if args.get("--shell-init"):
        warnings.append({"id": "W-SHELL-INIT", "severity": "warn",
                         "message": "--shell-init has no KubeRay equivalent. Bake the commands into the "
                                    "image (Dockerfile RUN) or a container lifecycle postStart hook."})

    # Environment variables: Glue sets every job argument as os.environ[name-without-dashes],
    # except its own keys (containing MANTA) and object_spilling_config.
    env_vars = {}
    for key, value in args.items():
        name = key[2:] if key.startswith("--") else key
        if "MANTA" in name.upper() or name == "object_spilling_config":
            continue
        env_vars[name] = str(value)
    env_vars["JOB_NAME"] = job.get("Name", "")
    sensitive = sorted(k for k in env_vars if is_sensitive(k))
    for k in sensitive:   # the value is never needed again (rendered as a secretKeyRef); keep it out of plan.json
        env_vars[k] = "<redacted: provide it in Secret <job>-job-args>"
    if sensitive:
        warnings.append({"id": "W-SENSITIVE-ARG", "severity": "warn",
                         "message": "Arguments that look like secrets: " + ", ".join(sensitive) + ". They "
                                    "are rendered as secretKeyRef env vars (Secret <job>-job-args), never "
                                    "inline. Better: move them to Secrets Manager and read them at run time."})
    if any(re.search(r"[$`]", v) for v in env_vars.values()):
        warnings.append({"id": "W-SHELL-CHARS", "severity": "info",
                         "message": "Some argument values contain $ or backticks. They are delivered as "
                                    "container env (no shell), so they arrive unchanged."})
    odd = sorted(k for k in env_vars if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", k))
    if odd:
        warnings.append({"id": "W-ENV-NAME", "severity": "info",
                         "message": "Job arguments whose env var names are not shell identifiers: "
                                    + ", ".join(odd) + ". Glue exposed them as-is (os.environ['a-b']). "
                                    "Shells drop such names, so on KubeRay each value is a GLUE_ARG_<n> env var on "
                                    "the head and the RayJob entrypoint sets the original name for the driver "
                                    "(where Glue set it)."})
    unsafe = [k for k in odd if not SHELL_SAFE_NAME.match(k)]
    if unsafe:
        warnings.append({"id": "W-ARG-NAME-SHELL", "severity": "warn",
                         "message": "Job argument names with characters a shell interprets: "
                                    + ", ".join(repr(k) for k in unsafe) + ". On Glue they were plain environment "
                                    "variables. On EKS the driver gets them through the RayJob entrypoint, which "
                                    "a shell runs, so these names won't reach the driver intact and can break the "
                                    "entrypoint. Tell the user at the review gate: renaming the argument (and where "
                                    "the script reads it) avoids this."})
    env_vars["MODIN_ENGINE"] = "ray"
    platform_dropped = sorted(k for k in args if k in PLATFORM_ARGS)
    custom_args = sorted(k for k in args if k not in RUNTIME_ARGS and k not in PLATFORM_ARGS)

    # S3 locations the job touches -> least-privilege policy input.
    # The script, py_modules zips and wheels are baked into the image, so the pods never read them from S3.
    s3_read, s3_write = set(), set()
    for key, value in args.items():
        # Glue's own locations (--TempDir, --spark-event-logs-path, the logging configuration) aren't the job's.
        if key in ("--pip-install", "--additional-python-modules", "--s3-py-modules", "--logging_configuration") \
                or key in PLATFORM_ARGS:
            continue
        uris = s3_uris(str(value))
        if not uris and BUCKET_ARG.search(key) and VALID_BUCKET.match(str(value).strip()):
            uris = [f"s3://{str(value).strip()}/"]
        for u in uris:
            (s3_write if WRITE_HINT.search(key) else s3_read).add(u)

    timeout_min = to_int(job.get("Timeout"), None)
    return {
        "job_name": job.get("Name"),
        "glue": {
            "number_of_workers": n_workers, "auto_scaling_max_workers": max_arg,
            "min_workers": total_min, "worker_type": job.get("WorkerType"),
            "runtime": cmd.get("Runtime"), "script_location": cmd.get("ScriptLocation"),
            "timeout_minutes": timeout_min, "max_retries": to_int(job.get("MaxRetries"), 0),
            "role": job.get("Role"), "logging_configuration": args.get("--logging_configuration"),
            "security_configuration": job.get("SecurityConfiguration"),
            "connections": (job.get("Connections") or {}).get("Connections", []),
        },
        "cluster": {
            "head": {"num_cpus": head_cpus, "object_store_pct": head_pct,
                     "object_store_bytes": head_os},
            "workers": {"min_replicas": min_replicas, "max_replicas": max_replicas,
                        "replicas": min_replicas, "num_cpus": GLUE_RAY_WORKER_CPUS,
                        "object_store_pct": worker_pct, "object_store_bytes": worker_os},
            "pod_memory_gib": pod_memory_gib, "pod_cpu": pod_cpu,
            "idle_timeout_seconds": idle_s, "glue_idle_timeout_minutes": glue_idle_min,
            "expected_total_cpus_at_max": head_cpus + GLUE_RAY_WORKER_CPUS * max_replicas,
        },
        "runtime_env": {
            "pip": pip, "py_modules": py_modules, "wheels": wheels, "working_dir": working_dir,
            "env_vars": env_vars,
            "setup_timeout_seconds": to_int(args.get("--py-libs-install-timeout"),
                                            GLUE_RAY_PY_LIBS_INSTALL_TIMEOUT_S),
        },
        "spilling": spilling,
        "sensitive_args": sensitive,
        "args": {"custom": custom_args, "platform_dropped": platform_dropped},
        "s3_access": {"read": sorted(s3_read), "write": sorted(s3_write)},
        "run": {"active_deadline_seconds": timeout_min * 60 if timeout_min else None,
                "backoff_limit": to_int(job.get("MaxRetries"), 0)},
    }


# --------------------------------------------------------------------------------------------
# Code scan
# --------------------------------------------------------------------------------------------
class Scanner(ast.NodeVisitor):
    """Flags only places where the Glue for Ray runtime differed from a plain Ray container, which is
    all that can make an unchanged script behave differently on EKS. It does NOT scan for Ray/pandas/
    numpy version changes: the migration keeps the same versions, so the code runs as it did on Glue.
    It also collects S3 literals and the AWS services the code calls, for the report."""

    def __init__(self, path, findings, env_reads, services, docstrings=()):
        self.path, self.findings, self.env_reads = path, findings, env_reads
        self.services = services
        self.docstrings = docstrings   # ids of docstring constants: prose, not code

    def add(self, node, rule, severity, message, fix=None):
        self.findings.append({"file": self.path, "line": getattr(node, "lineno", 0), "rule": rule,
                              "severity": severity, "message": message, "fix": fix})

    @staticmethod
    def dotted(node):
        parts = []
        while isinstance(node, ast.Attribute):
            parts.append(node.attr)
            node = node.value
        if isinstance(node, ast.Name):
            parts.append(node.id)
        return ".".join(reversed(parts))

    def visit_Constant(self, node):
        if isinstance(node.value, str) and id(node) not in self.docstrings:
            for u in s3_uris(node.value):
                if VALID_BUCKET.match(u.split("://", 1)[1].split("/", 1)[0]):  # skip "s3://..." in prose
                    self.services.add("s3-literal:" + u)
            if GLUE_PATH.search(node.value):
                self.add(node, "GLUE-PATH", "warn",
                         f"Path from the Glue for Ray runtime: {node.value!r}. On Glue the job ran as user "
                         "glue-ray-engine (HOME=/home/glue-ray-engine) next to Glue's own code under "
                         "/opt/amazon. None of these exist in the Ray image (user ray, HOME=/home/ray).",
                         "Derive paths from os.environ['HOME'] / tempfile, or drop checks of Glue internals. "
                         "Do not fake HOME with an env var: the directory does not exist in the image.")
        self.generic_visit(node)

    def visit_Subscript(self, node):
        # os.environ["x"]
        if self.dotted(node.value) == "os.environ":
            key = node.slice.value if isinstance(node.slice, ast.Constant) else None
            if key is None and hasattr(node.slice, "value") and isinstance(node.slice.value, ast.Constant):
                key = node.slice.value.value  # Python 3.8 ast.Index
            if isinstance(key, str):
                self.env_reads.add(key)
        self.generic_visit(node)

    def argv_finding(self, node, what):
        if any(f["rule"] == "GLUE-ARGV" and f["file"] == self.path and f["line"] == node.lineno
               for f in self.findings):
            return
        self.add(node, "GLUE-ARGV", "warn",
                 f"{what}. Glue passes job arguments as environment variables, not on the command line, and "
                 "on EKS sys.argv is just [script], so arguments read this way differ from Glue.",
                 "Read job arguments from os.environ (the skill sets them as env vars).")

    def visit_Attribute(self, node):
        if self.dotted(node) == "sys.argv":
            self.argv_finding(node, "sys.argv is read")
        self.generic_visit(node)

    def visit_ImportFrom(self, node):
        if node.module == "sys" and any(a.name == "argv" for a in node.names):
            self.argv_finding(node, "sys.argv is imported")
        self.generic_visit(node)

    def visit_Compare(self, node):
        # "x" in os.environ / "x" not in os.environ
        for op, right in zip(node.ops, node.comparators):
            if isinstance(op, (ast.In, ast.NotIn)) and self.dotted(right) == "os.environ" and \
                    isinstance(node.left, ast.Constant) and isinstance(node.left.value, str):
                self.env_reads.add(node.left.value)
        self.generic_visit(node)

    WRITE_CALLS = re.compile(r"^(write_\w+|to_parquet|to_csv|to_json|upload_file)$")

    def visit_Call(self, node):
        name = self.dotted(node.func)
        short = name.rsplit(".", 1)[-1]
        kwargs = {k.arg for k in node.keywords if k.arg}
        # S3 URIs passed straight to a write call (ds.write_parquet("s3://..."), df.to_csv(...), s3_output=...)
        for i, v in enumerate(list(node.args) + [k.value for k in node.keywords]):
            kw = node.keywords[i - len(node.args)].arg if i >= len(node.args) else None
            if (self.WRITE_CALLS.match(short) or kw == "s3_output") and isinstance(v, ast.Constant) and \
                    isinstance(v.value, str):
                for u in s3_uris(v.value):
                    self.services.add("s3-literal-write:" + u)

        if short in ("client", "resource") and name.startswith("boto3") and node.args and \
                isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
            self.services.add(node.args[0].value)
        for prefix, svc in (("wr.athena.", "athena+glue"), ("wr.catalog.", "glue"),
                            ("wr.dynamodb.", "dynamodb"), ("wr.redshift.", "redshift"),
                            ("wr.timestream.", "timestream"), ("wr.opensearch.", "opensearch"),
                            ("wr.secretsmanager.", "secretsmanager"), ("wr.mysql.", "rds/connection"),
                            ("wr.postgresql.", "rds/connection")):
            if name.startswith(prefix):
                self.services.add(svc)
        if name == "wr.s3.to_parquet" and {"database", "table"} & kwargs:
            self.services.add("glue")
        if name in ("os.environ.get", "os.getenv") and node.args and \
                isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
            self.env_reads.add(node.args[0].value)

        # parser.parse_args() / parse_known_args() with no arguments parse sys.argv
        if short in ("parse_args", "parse_known_args") and not node.args and "args" not in kwargs:
            self.argv_finding(node, f"{short}() parses the command line (sys.argv)")

        if name in ("ray.cluster_resources", "ray.available_resources", "ray.nodes"):
            self.add(node, "RAY-CLUSTER-SIZE", "info",
                     f"{name}() reads the cluster size. On Glue the script could start before all --min-workers "
                     "nodes had joined; on EKS the RayJob starts it once the minimum workers are up, so a "
                     "start-up read can see more nodes than on Glue.",
                     "If the script checks or sizes its work from this value at start-up, tell the user.")

        if name == "ray.init":
            if "runtime_env" in kwargs:
                self.add(node, "RAY-INIT-RUNTIME-ENV", "warn",
                         "ray.init(runtime_env=...) in the driver. On Glue a runtime_env hook merged the "
                         "job's dependencies into every ray.init(). On EKS the RayJob's runtime env and "
                         "this one must not conflict.",
                         "Move these fields into the RayJob runtimeEnvYAML, or confirm the merge.")
            addr = next((k.value.value for k in node.keywords
                         if k.arg == "address" and isinstance(k.value, ast.Constant)), None)
            if addr is None and node.args and isinstance(node.args[0], ast.Constant):
                addr = node.args[0].value
            if addr not in (None, "auto"):
                self.add(node, "RAY-INIT-ADDRESS", "warn",
                         f"ray.init({addr!r}) uses a hard-coded address.",
                         "Use ray.init() or ray.init('auto'); the RayJob sets RAY_ADDRESS.")
        self.generic_visit(node)


# Condition keys that describe the Glue job's own credentials, and their value on the job's requests (None:
# absent). Glue sets glue:CredentialIssuingService on the sessions it gives the job, and glue:RoleAssumedBy
# only on the sessions it uses itself to call other services, never on the job's. Neither exists on EKS.
GLUE_SESSION_KEYS = {"glue:credentialissuingservice": "glue.amazonaws.com", "glue:roleassumedby": None}
# Request keys: set on calls to the Glue API from any caller, a pod included, so IAM evaluates them the same
# way on EKS and statements conditioned on them are copied unchanged.
GLUE_REQUEST_KEYS = {"glue:vpcids", "glue:subnetids", "glue:securitygroupids", "glue:lakeformationpermissions",
                     "glue:federatedauthorizationsource", "glue:enabledforredshiftautodiscovery"}
_STRING_OPS = {"StringEquals", "StringNotEquals", "StringEqualsIgnoreCase", "StringNotEqualsIgnoreCase",
               "StringLike", "StringNotLike"}


def _glue_cond_held_on_glue(op, key, value):
    """Whether a condition on a GLUE_SESSION_KEYS key evaluated true for the Glue job's own requests, under
    IAM's rules for a single-valued key. Returns None for an operator this doesn't evaluate."""
    actual = GLUE_SESSION_KEYS[key.lower()]
    vals = [str(v) for v in (value if isinstance(value, list) else [value])]
    prefix, _, base = op.rpartition(":")
    if_exists = base.endswith("IfExists")
    base = base[:-len("IfExists")] if if_exists else base
    if prefix not in ("", "ForAnyValue", "ForAllValues"):
        return None
    if base == "Null":
        return any(v.lower() == ("true" if actual is None else "false") for v in vals)
    if base not in _STRING_OPS:
        return None
    negated = "Not" in base
    if actual is None:
        # A missing key: IfExists and ForAllValues hold, ForAnyValue fails, negated operators hold.
        return if_exists or prefix == "ForAllValues" or (negated and prefix != "ForAnyValue")
    if "Like" in base:
        hit = any(fnmatch.fnmatchcase(actual, v) for v in vals)
    elif "IgnoreCase" in base:
        hit = any(actual.lower() == v.lower() for v in vals)
    else:
        hit = actual in vals
    return not hit if negated else hit


def _policy_body(doc):
    # Accept get-role-policy output, get-policy-version output, or a bare policy document.
    if "PolicyVersion" in doc:
        body = doc["PolicyVersion"].get("Document", {})
    else:
        body = doc.get("PolicyDocument", doc)
    if isinstance(body, str):
        body = json.loads(urllib.parse.unquote(body))
    return body


def _strip_glue_keys(st):
    cond = {}
    for op, kv in (st.get("Condition") or {}).items():
        rest = {k: v for k, v in kv.items() if k.lower() not in GLUE_SESSION_KEYS}
        if rest:
            cond[op] = rest
    out = {k: v for k, v in st.items() if k != "Condition"}
    if cond:
        out["Condition"] = cond
    return out


def pod_role_policies(docs, warnings):
    """The pod role gets the Glue role's permissions unchanged (parity), except statements conditioned on
    the keys that describe the Glue job's credentials (GLUE_SESSION_KEYS): on EKS they would evaluate
    differently. Each such statement is rewritten to behave as it did for the Glue job's own requests: if
    its conditions on those keys held there, the keys are removed (the Deny keeps denying, the Allow keeps
    allowing); if they never held there, it is dropped. Conditions on other glue: keys are request keys and
    stay. An unknown glue: key or an operator this can't evaluate is an error, and the statement is copied
    unchanged for the user to decide. Returns one entry per policy: AWS- or customer-managed policies
    without rewritten statements are attached by ARN, everything else is copied as a policy document."""
    out = []
    for doc in docs:
        arn = doc.get("PolicyArn")
        name = doc.get("PolicyName") or (arn.rsplit("/", 1)[-1] if arn else "policy")
        body = _policy_body(doc)
        stmts = body.get("Statement", [])
        stmts = stmts if isinstance(stmts, list) else [stmts]
        if not stmts:
            warnings.append({"id": "W-IAM-POLICY-EMPTY", "severity": "warn",
                             "message": f"Policy {name}: no statements found; check the input format "
                                        "(get-role-policy, or get-policy-version with its PolicyArn)."})
        kept, changed = [], False
        for st in stmts:
            conds = [(op, k, v) for op, kv in (st.get("Condition") or {}).items() for k, v in kv.items()
                     if k.lower().startswith("glue:")]
            sid = st.get("Sid", "(no Sid)")
            unknown = sorted({k for _, k, _ in conds
                              if k.lower() not in GLUE_SESSION_KEYS and k.lower() not in GLUE_REQUEST_KEYS})
            session = [(op, k, v) for op, k, v in conds if k.lower() in GLUE_SESSION_KEYS]
            held = [_glue_cond_held_on_glue(op, k, v) for op, k, v in session]
            if unknown or None in held:
                what = (f"the unknown key(s) {', '.join(unknown)}" if unknown
                        else "an operator the analyzer can't evaluate")
                warnings.append({"id": "W-IAM-GLUE-UNKNOWN", "severity": "error",
                                 "message": f"Policy {name} statement {sid} is conditioned on {what}. It is copied "
                                            "unchanged, but on EKS it may evaluate differently than on Glue. Decide "
                                            "with the user how it should apply to the pod role."})
                kept.append(st)
                continue
            if not session:
                kept.append(st)
                continue
            changed = True
            keys = ", ".join(sorted({k for _, k, _ in session}))
            if all(held):
                kept.append(_strip_glue_keys(st))
                what = "kept without those keys, so it applies on EKS as it did on Glue"
            else:
                what = "dropped: it never applied to the Glue job's own requests, but would apply to every pod request"
            warnings.append({"id": "W-IAM-GLUE-CONDITION", "severity": "warn",
                             "message": f"Policy {name} statement {sid}: {st.get('Effect')} conditioned on "
                                        f"{keys}, which describes the Glue job's credentials. In the pod role it is "
                                        f"{what}. Review it."})
        if arn and not changed:
            out.append({"name": name, "attach_arn": arn})
        else:
            out.append({"name": name, "source_arn": arn, "rewritten": changed,
                        "document": {"Version": body.get("Version", "2012-10-17"), "Statement": kept}})
    return out


def glue_role(doc, warnings):
    """What the pod role copies from the Glue role besides its policies: the permissions boundary (it caps
    the role's effective permissions, the inline test-run grant included) and the tags (aws:PrincipalTag
    conditions read them). Tags in the reserved aws: namespace can't be set and are left out."""
    role = doc.get("Role", doc)
    boundary = (role.get("PermissionsBoundary") or {}).get("PermissionsBoundaryArn")
    tags = [t for t in role.get("Tags") or [] if not t.get("Key", "").lower().startswith("aws:")]
    if boundary:
        warnings.append({"id": "W-IAM-BOUNDARY", "severity": "info",
                         "message": f"The Glue role has the permissions boundary {boundary}; the pod role gets the "
                                    "same one. A boundary caps the inline test-run grant too, so test-run "
                                    "locations must be inside it."})
    return {"arn": role.get("Arn"), "role_id": role.get("RoleId"), "permissions_boundary": boundary,
            "tags": tags}


def scan_tree(source_dir):
    findings, env_reads, parse_errors, services = [], set(), [], set()
    for root, _dirs, files in os.walk(source_dir):
        for f in files:
            path = os.path.join(root, f)
            rel = os.path.relpath(path, source_dir)
            if not f.endswith(".py"):
                continue
            try:
                with open(path, encoding="utf-8", errors="replace") as fh:
                    tree = ast.parse(fh.read(), filename=rel)
            except SyntaxError as e:
                parse_errors.append(f"{rel}:{e.lineno}: {e.msg}")
                continue
            docstrings = {id(n.body[0].value) for n in ast.walk(tree)
                          if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
                          and n.body and isinstance(n.body[0], ast.Expr) and isinstance(n.body[0].value, ast.Constant)
                          and isinstance(n.body[0].value.value, str)}
            Scanner(rel, findings, env_reads, services, docstrings).visit(tree)
    order = {"error": 0, "warn": 1, "info": 2}
    findings.sort(key=lambda x: (order.get(x["severity"], 3), x["file"], x["line"]))
    return findings, sorted(env_reads), parse_errors, sorted(services)


# --------------------------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------------------------
def gib(b):
    return f"{b / 1024 ** 3:.1f} GiB" if b else "Ray default"


def render_report(plan):
    c, g, r = plan["cluster"], plan["glue"], plan["runtime_env"]
    lines = [f"# Migration analysis: `{plan['job_name']}`", ""]
    lines += ["## Capacity mapping", "",
              "| Glue for Ray | Value | KubeRay | Value |", "|---|---|---|---|",
              f"| NumberOfWorkers (includes head) | {g['number_of_workers']} | head + workerGroup "
              f"maxReplicas | 1 + {c['workers']['max_replicas']} |",
              f"| --min-workers (includes head) | {g['min_workers']} | minReplicas / replicas (a floor) "
              f"| {c['workers']['min_replicas']} |",
              f"| head runs tasks (CPU 8) | yes | head rayStartParams num-cpus | {c['head']['num_cpus']} |",
              f"| autoscaler idle timeout | {c['glue_idle_timeout_minutes']} min | "
              f"autoscalerOptions.idleTimeoutSeconds | {c['idle_timeout_seconds']} |",
              f"| --object_store_memory_head | {c['head']['object_store_pct'] or '-'}% | "
              f"head object-store-memory | {gib(c['head']['object_store_bytes'])} |",
              f"| --object_store_memory_worker | {c['workers']['object_store_pct'] or '-'}% | "
              f"worker object-store-memory | {gib(c['workers']['object_store_bytes'])} |",
              f"| Timeout (480 for every Ray job and run on Glue; not settable) | {g['timeout_minutes'] or '-'} min | activeDeadlineSeconds | "
              f"{plan['run']['active_deadline_seconds'] or '-'} |",
              f"| MaxRetries | {g['max_retries']} | backoffLimit | {plan['run']['backoff_limit']} |",
              "", f"Expected `ray.cluster_resources()['CPU']` at max scale: "
                  f"**{c['expected_total_cpus_at_max']}** (Glue parity).", ""]
    lines += ["## Dependencies", "",
              f"- pip: {', '.join(r['pip']) or '(none)'}",
              f"- py_modules: {', '.join(r['py_modules']) or '(none)'}",
              f"- wheels (installed into the image): {', '.join(r.get('wheels', [])) or '(none)'}",
              f"- working_dir: {r['working_dir'] or '(none)'}",
              f"- runtime_env setup timeout: {r['setup_timeout_seconds']} s", ""]
    lines += ["## Environment variables injected (Glue exposed every job argument this way)", ""]
    lines += [f"- `{k}`" for k in sorted(r["env_vars"])]
    lines += ["- `JOB_RUN_ID` (the run's RayCluster name), `AWS_REGION`, `AWS_DEFAULT_REGION`, "
              "`RAY_DEDUP_LOGS=0`, and `RAY_object_spilling_config` if the job spills (added by the renderer)", ""]
    if plan["env_reads_unmapped"]:
        lines += ["**Script reads env vars that no job argument provides:** " +
                  ", ".join(f"`{e}`" for e in plan["env_reads_unmapped"]) +
                  ". Confirm their source.", ""]
    if plan["spilling"]:
        lines += ["## Object spilling", "", "```json", json.dumps(plan["spilling"]["config"], indent=2),
                  "```", ""]
    lines += ["## Pod role (same permissions as the Glue job role)", ""]
    for x in plan.get("iam_policies", []):
        lines.append(f"- `{x['name']}`: " + ("attached unchanged" if "attach_arn" in x else
                                            "copied, statements on the Glue credential keys rewritten"
                                            if x.get("rewritten") else "copied unchanged"))
    if not plan.get("iam_policies"):
        lines.append("- (not collected: run the analyzer with --role-policies)")
    role = plan.get("iam_role")
    if role:
        lines.append(f"- permissions boundary: {role['permissions_boundary'] or '(none)'}")
        lines.append("- tags: " + (", ".join(f"`{t['Key']}={t.get('Value', '')}`" for t in role["tags"])
                                   or "(none)"))
    else:
        lines.append("- boundary and tags: not read (run the analyzer with --role)")
    lines += ["", "## S3 locations the job uses", "",
              "Read: " + (", ".join(plan["s3_access"]["read"]) or "(none)"),
              "", "Write: " + (", ".join(plan["s3_access"]["write"]) or "(none)"), "",
              "The pod role's copied policies cover these as far as the Glue role's own policies did. Access "
              "that came from a bucket policy, a KMS key policy or Lake Formation names the Glue role and "
              "doesn't carry over: see the grant inventory (Step 3). s3-policy.json grants exactly these "
              "locations (for test-run locations the Glue role can't reach).", ""]
    if plan.get("aws_services"):
        lines += ["**AWS services the code calls besides S3:** "
                  + ", ".join(f"`{x}`" for x in plan["aws_services"]), ""]
    if plan["warnings"]:
        lines += ["## Configuration warnings", ""]
        lines += [f"- **{w['severity']}** `{w['id']}`: {w['message']}" + (f" Fix: {w['fix']}" if w.get("fix") else "")
                  for w in plan["warnings"]] + [""]
    lines += ["## Code findings", ""]
    if not plan["findings"]:
        lines += ["None.", ""]
    else:
        lines += ["| Severity | Rule | Location | Finding | Fix |", "|---|---|---|---|---|"]
        for f in plan["findings"]:
            lines.append(f"| {f['severity']} | {f['rule']} | {f['file']}:{f['line']} | "
                         f"{f['message']} | {f['fix'] or ''} |")
        lines.append("")
    if plan["parse_errors"]:
        lines += ["## Files that did not parse", ""] + [f"- {e}" for e in plan["parse_errors"]] + [""]
    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--job", required=True)
    p.add_argument("--source-dir", required=True)
    p.add_argument("--pod-memory-gib", type=float, default=54.0)
    p.add_argument("--pod-cpu", type=float, default=7.0)
    p.add_argument("--out", required=True)
    p.add_argument("--report", required=True)
    p.add_argument("--run-args", default=None,
                   help="JSON of a run's Arguments (aws glue get-job-run ... --query JobRun.Arguments)")
    p.add_argument("--role-policies", default=None,
                   help="JSON list of every policy of the Glue job role: inline ones as returned by "
                        "get-role-policy, attached (AWS- and customer-managed) ones as get-policy-version "
                        "output plus their PolicyArn")
    p.add_argument("--role", default=None,
                   help="JSON from `aws iam get-role` for the Glue job role (permissions boundary, tags)")
    a = p.parse_args()

    warnings = []
    run_args = None
    if a.run_args:
        with open(a.run_args) as f:
            run_args = json.load(f)
    plan = map_job(load_job(a.job), a.pod_memory_gib, a.pod_cpu, warnings, run_args)
    if a.role_policies:
        with open(a.role_policies) as f:
            plan["iam_policies"] = pod_role_policies(json.load(f), warnings)
    else:
        warnings.append({"id": "W-IAM-NOT-SCANNED", "severity": "error",
                         "message": "No --role-policies: the pod role can't get the Glue role's permissions. "
                                    "Collect every policy of the Glue role (Step 1) and re-run."})
    if a.role:
        with open(a.role) as f:
            plan["iam_role"] = glue_role(json.load(f), warnings)
    else:
        warnings.append({"id": "W-IAM-ROLE-NOT-READ", "severity": "error",
                         "message": "No --role: the pod role can't get the Glue role's permissions boundary and "
                                    "tags. Save `aws iam get-role` for the Glue role (Step 1) and re-run."})
    findings, env_reads, parse_errors, services = scan_tree(a.source_dir)
    written = sorted({x.split(":", 1)[1] for x in services if x.startswith("s3-literal-write:")})
    literals = sorted({x.split(":", 1)[1] for x in services if x.startswith("s3-literal:")} - set(written))
    services = [x for x in services if not x.startswith("s3-literal")]
    if written:
        plan["s3_access"]["write"] = sorted(set(plan["s3_access"]["write"]) | set(written))
    if literals or written:
        plan["s3_access"]["read"] = sorted(set(plan["s3_access"]["read"]) | set(literals))
        warnings.append({"id": "W-S3-LITERALS", "severity": "warn",
                         "message": "S3 URIs hard-coded in the code: "
                                    + ", ".join(written + literals) + ". Those passed to a write call ("
                                    + (", ".join(written) or "none") + ") were added as WRITE access, the "
                                    "rest as READ. Move any other location the job writes to s3_access.write "
                                    "in plan.json before rendering. A hard-coded output can't be redirected "
                                    "for a test run without a code change."})
    provided = set(plan["runtime_env"]["env_vars"]) | {"JOB_RUN_ID", "AWS_REGION", "AWS_DEFAULT_REGION"}
    plan.update({"warnings": warnings, "findings": findings, "env_reads": env_reads,
                 "env_reads_unmapped": [e for e in env_reads if e not in provided],
                 "parse_errors": parse_errors, "aws_services": services})
    with open(a.out, "w") as f:
        json.dump(plan, f, indent=2)
    with open(a.report, "w") as f:
        f.write(render_report(plan))
    errors = sum(1 for x in findings + warnings if x["severity"] == "error")
    print(f"plan: {a.out}  report: {a.report}  code findings: {len(findings)}  configuration warnings: "
          f"{len(warnings)}  errors (both): {errors}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
