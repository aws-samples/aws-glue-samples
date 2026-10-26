# Resolving dependencies and building the image (Steps 4 and 6)

Run both natively on the target architecture (arm64 for Graviton). No emulation: it hides wheel gaps
and is slow. On an arm64 machine (for example Apple silicon) run them locally; on any other machine use
AWS CodeBuild with `templates/buildspec.yml`.

## Step 4 locally: the resolve

Run as your own UID, so the mounted directory stays writable, with group 100 (`users`): the Ray images
keep Python under `/home/ray` (mode 750, group `users`), which other groups cannot reach.

```bash
docker run --rm -u $(id -u):100 -e HOME=/tmp \
  -v $(pwd):/w -v $(pwd)/scripts:/w/scripts:ro \
  [BASE_IMAGE_ARM64] bash /w/scripts/resolve-deps.sh
```

Outputs: `out/final-requirements.txt` (exact pins of every package added or changed against the base
image) and `out/import-check.txt` (every third-party import must load; `SHADOW` lines warn that a
`--s3-py-modules` module replaces one the image has, for every Python process in the image). The resolver
pins `ray` to the base image's version, as Glue did: `--pip-install` could never change Ray.

## Step 6 locally: build, check and push

The build context (one directory) holds only the job's code and its dependencies: `app/main.py` (from
`source/main.py`), `app/py_modules/` (Step 2), `wheels/`, `final-requirements.txt` (copied from
`out/` to the context root) and `Dockerfile` from `templates/`.

```bash
docker build --build-arg BASE_IMAGE=[BASE_IMAGE_ARM64] --build-arg PY_MODULES_PATH=<value from Step 2> \
  -t <ECR_URI>:<TAG> -f Dockerfile .
docker run --rm <ECR_URI>:<TAG> python -c "import ray, platform, sys; print(ray.__version__, sys.version.split()[0], platform.machine())"
aws ecr create-repository --repository-name <REPO> --image-scanning-configuration scanOnPush=true --image-tag-mutability IMMUTABLE   # if needed
aws ecr get-login-password | docker login --username AWS --password-stdin <ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com
docker push <ECR_URI>:<TAG>
```

The check must print `2.4.0 3.9.x aarch64` (`x86_64` for an amd64 image).

## On any other machine: AWS CodeBuild

`templates/buildspec.yml` runs the Step 4 resolve and the Step 6 commands, and pushes with the
project's service role, so no AWS credentials are copied anywhere. Creating the project is billable and
needs the user's confirmation (interaction). On a machine that isn't arm64, create it before Step 4,
because the resolve runs in it.
- Environment type `ARM_CONTAINER` with an `aws/codebuild/amazonlinux-aarch64-standard` image (arm64),
  or `LINUX_CONTAINER` with an `aws/codebuild/amazonlinux-x86_64-standard` image for an amd64 image
  (`BASE_IMAGE` = `[BASE_IMAGE_AMD64]`); privileged mode (Docker).
- A service role that can read the source zip, write the build artifacts (`out/`, `wheels/`) to S3,
  write CloudWatch Logs, and push to the ECR repository (`ecr:GetAuthorizationToken`,
  `ecr:CreateRepository`, `ecr:TagResource`, `ecr:DescribeRepositories`, and the push actions on that
  repository).
- Source: the work directory zipped to S3 (`requirements-glue-ray24.txt`, `requirements.in`, `source/`,
  `wheels/`, `Dockerfile`, `scripts/resolve-deps.sh`). Environment variables `BASE_IMAGE`,
  `PY_MODULES_PATH`, `REPO`, `TAG`, `GLUE_JOB`, and `PHASE`, which every build must set.

Run it with `PHASE=resolve` for Step 4 and review `out/` (the build's artifacts). Then add the reviewed
`out/final-requirements.txt` and the artifacts' `wheels/` to the source zip and run it with
`PHASE=build`: it installs exactly that lockfile, without resolving again, and fails if the lockfile is
missing. On a project shared by several jobs, override the source location, the buildspec (put
`buildspec.yml` in the zip and pass `--buildspec-override buildspec.yml`), the artifacts location and
the environment variables on each `start-build`.

## Base-image CVEs

The Ray 2.4 base image is old (Ubuntu focal), so ECR scanning will report CVEs in the base image
(`aws ecr describe-image-scan-findings --repository-name <REPO> --image-id imageTag=<TAG>`); record
them and share them with the customer. Patching the base image, or moving to a newer Ray, is the
customer's call and out of scope here.
