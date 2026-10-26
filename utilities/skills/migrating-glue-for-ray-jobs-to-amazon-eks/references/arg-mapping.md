# Glue for Ray → KubeRay mapping

`scripts/analyze_glue_ray_job.py` implements this table. It records how the Glue for Ray runtime
(Ray2.4) actually behaves, which the public migration guide leaves out in places.

## Runtime contract

| Glue for Ray behavior | Consequence on KubeRay | How the skill reproduces it |
|---|---|---|
| The script is fetched from `ScriptLocation` and `exec`'d as `__main__` inside a REPL on the head node | The driver runs on the head pod | The RayJob `entrypoint` is `python /home/ray/app/main.py`: the script runs as `__main__` in the driver, with `sys.argv` = [script] |
| **Every job argument becomes an environment variable of the driver** named without `--` (`--input_path` → `os.environ["input_path"]`, `--athena-workgroup` → `os.environ["athena-workgroup"]`), along with `JOB_NAME`, `JOB_RUN_ID`, `AWS_REGION`, `MODIN_ENGINE=ray`. Glue's own keys (containing `MANTA`) and `object_spilling_config` are skipped. Tasks and actors do **not** see them | Scripts never parse `sys.argv` | Container `env` on the head and worker pods carries all of them, so the driver, tasks and actors see them (a superset of Glue). Names that are not shell identifiers, which shells drop, reach the driver only (as on Glue): the value is a `GLUE_ARG_<n>` env var on the head, and the entrypoint sets the original name with `exec env` and removes the `GLUE_ARG_<n>` variables from the driver's environment. `JOB_RUN_ID` is the run's RayCluster name (Downward API, label `ray.io/cluster`). Secret-looking arguments use `secretKeyRef` |
| `sys.argv` does not contain the job arguments | — | `python main.py` gives `sys.argv = [script]`. The analyzer flags reads of `sys.argv` and command-line parsing with `parse_args()` (`GLUE-ARGV`) |
| `--pip-install`, `--s3-py-modules` and `--working-dir` are applied to **every** `ray.init()` through `RAY_RUNTIME_ENV_HOOK`. The driver's `sys.path` already holds the pip virtualenv, each py_modules directory and the working_dir directory before the script starts, so imports work before `ray.init()` | On EKS, dependencies have to exist where the driver runs | Dependencies are baked into the image (both driver and workers), one `PYTHONPATH` entry per py_modules zip. `working_dir` stays in the runtime env |
| The driver's **cwd is `/`**. `--working-dir` becomes the cwd of tasks and actors only | A RayJob with `working_dir` runs the driver inside the working dir, so driver code that opens working-dir files by relative path works on EKS but failed on Glue | Nothing to reproduce; relative-path driver code that fails on Glue was already broken |
| The job runs as `glue-ray-engine` (uid 994, `HOME=/home/glue-ray-engine`) and cannot `sudo` | The Ray images run as `ray` (uid 1000, `HOME=/home/ray`) with passwordless sudo | Pod and container `securityContext`: `runAsNonRoot`, `allowPrivilegeEscalation: false`, all capabilities dropped. The analyzer flags hard-coded Glue paths (`GLUE-PATH`) |
| The job runs in **UTC** (local time from `datetime.now()`, `time.localtime()` and log timestamps) | The `rayproject/ray` images set `TZ=America/Los_Angeles`, so local time would be Pacific | The Dockerfile template sets `TZ=UTC` |
| `--min-workers` (head included) sets the cluster's minimum size; the script can start before all of those nodes have joined | — | `minReplicas` keeps the floor. The RayJob starts the script once the RayCluster is ready, with its minimum workers up, so a start-up read of the cluster size can see more nodes than on Glue. The analyzer notes such reads (`RAY-CLUSTER-SIZE`) |
| The head advertises `CPU: 8` and runs tasks and actors | A head pod with default `num-cpus` from the container limit (about 7) gives less capacity | Head `rayStartParams.num-cpus: "8"` |
| `NumberOfWorkers` and `--min-workers` **include the head**. The autoscaler gets `value − 1` | Taken literally, the guide adds one node | `maxReplicas = NumberOfWorkers − 1`, `minReplicas = max(min-workers − 1, 0)` |
| Autoscaler idle timeout is 1 min (`--auto-scaling-executor-idle-timeout` overrides it, in minutes), but Glue's own controller managed the nodes | On EKS a removed worker takes minutes to replace, and the objects it held are lost | `autoscalerOptions.idleTimeoutSeconds: 600` by default; an explicit `--auto-scaling-executor-idle-timeout` is kept |
| `--object_store_memory_*` is a % of **total** node memory (64 GB). `/dev/shm` is resized to match | On EKS, `/dev/shm` is a memory emptyDir that KubeRay auto-mounts. It counts against the pod memory limit | `object-store-memory = pct × pod memory limit` (bytes). A warning above 80% |
| `--object_spilling_config` is passed to Ray. Glue rejects security configurations and connections for Ray jobs, so spills and outputs only ever had the bucket's default encryption, and jobs only reached public endpoints | `ray start` has **no** spilling flag. The public guide's `rayStartParams.object-spilling-config` doesn't configure spilling | The `RAY_object_spilling_config` env var on every Ray container. Encryption stays the bucket default. On EKS, traffic to public endpoints leaves through the VPC's NAT gateway: targets that allowlisted Glue's IP ranges must allow the NAT IPs instead |
| Logs go to `/aws-glue/ray/jobs/*` in CloudWatch. The run result, including the driver's stdout, goes to `job-result/`, and each node's Ray logs to `cluster_logs/`, under `jobs/<name>/<runId>/` in the S3 folder of `ScriptLocation` | The driver output (submitter pod) is deleted with its pod shortly after the run | Required: the CloudWatch Observability add-on or another log pipeline (Step 7). Consumers of `job-result/` must change |
| `Timeout` (minutes), `MaxRetries` | Glue rejects `Timeout` on CreateJob for Ray jobs (`Timeout not supported for Ray jobs`); `GetJob` and every job run report the 480-minute default. It is the run's limit on Glue, not a customer choice | `activeDeadlineSeconds: 28800` from that default, shown to the user at the review gate; another value only if the user picks one. `backoffLimit` = `MaxRetries` |

## Job arguments

| Argument | Glue for Ray meaning | KubeRay / Ray equivalent |
|---|---|---|
| `NumberOfWorkers` (job field) | Maximum nodes, including the head | `workerGroupSpecs[0].maxReplicas = N − 1` |
| `--auto-scaling-max-workers` (undocumented) | Same as above, as an argument | The lower of this and NumberOfWorkers, minus 1 |
| `--min-workers` (`--auto-scaling-ray-min-workers` in preview) | Minimum nodes, including the head | `minReplicas = replicas = max(m − 1, 0)` (a floor; the script starts once they are up) |
| `--auto-scaling-executor-idle-timeout` | Idle minutes before scale-down | `autoscalerOptions.idleTimeoutSeconds = 60 × value` (without it: 600) |
| `--workers_on_head_node` | `0` keeps work off the head | Head `num-cpus: "0"` |
| `--pip-install` / `--additional-python-modules` | Comma list of pip requirements. `s3://` entries become `py_modules` | Resolved by `resolve-deps.sh` and baked into the image |
| `--s3-py-modules` | S3 zips. Ray unpacks each into its own directory, **strips the zip's single top-level directory**, and puts that directory on `sys.path`: a zip of `mypkg/` exposes mypkg's modules, a zip of `dist/mypkg/` exposes `import mypkg` (`.whl` entries are installed into the image by `resolve-deps.sh`) | `scripts/unpack_py_modules.py` reproduces this layout under `/home/ray/app/py_modules/<zip>/`, one `PYTHONPATH` entry each, baked into the image (operator decision 7) |
| `--working-dir` | S3 zip with a single top-level directory. Its contents become the task cwd on every node | `runtime_env.working_dir: s3://...zip`. Needs `smart_open` + `boto3` **in the image**, plus S3 read access for the pod role |
| `--object_store_memory_head` / `_worker` | % of total memory | `rayStartParams.object-store-memory` in bytes (string) |
| `--object_spilling_config` | Ray spilling JSON (filesystem or `smart_open` S3) | `RAY_object_spilling_config` env var. S3 spilling needs `smart_open[s3]` + `boto3` in the image and write access to the spill prefix |
| `--logging_configuration` | Turns Glue's CloudWatch / S3 log shipping on or off | Configure the cluster log pipeline instead. The argument is not migrated |
| `--py-libs-install-timeout` | pip install timeout (600 s) | `runtime_env.config.setup_timeout_seconds`, rendered only when the runtime env sets something up (`working_dir`) |
| `--shell-init` | Commands run before the script | A Dockerfile `RUN`, or a container `postStart` hook |
| `--extra-py-files` | **No effect on Glue for Ray** | Not migrated. The analyzer warns |
| Any other `--name` | Custom argument → `os.environ["name"]` | Container `env` (name without `--`) |

## Worker type → pod / node

| Glue | EKS (arm64, Graviton parity) | EKS (amd64) | Pod resources (one Ray pod per node) |
|---|---|---|---|
| Z.2X (8 vCPU, 64 GB, 128 GB disk) | `r7g.2xlarge` | `r7a.2xlarge` | requests cpu 7, memory 54Gi (limit 54Gi; leaves room for the autoscaler sidecar and node DaemonSets), ephemeral-storage 100Gi; `num-cpus` "8" |

These sizes leave room for kubelet, daemonsets and the autoscaler sidecar on the head
(200m / 512Mi). Requesting the full 64Gi leaves the pod Pending: allocatable on r7g.2xlarge is about 61Gi.
