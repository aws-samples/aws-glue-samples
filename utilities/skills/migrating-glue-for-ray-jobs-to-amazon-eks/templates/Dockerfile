# Image for one migrated Glue for Ray job. Built natively for the target architecture
# (arm64 for Graviton parity). Dependencies are resolved first by scripts/resolve-deps.sh.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

# Only if you add OS packages: the Ray 2.4.0 base image still lists the retired
# apt.kubernetes.io repository, so `apt-get update` fails until it is removed.
# USER root
# RUN rm -f /etc/apt/sources.list.d/kubernetes.list && apt-get update \
#     && apt-get install -y --no-install-recommends <packages> && rm -rf /var/lib/apt/lists/*
# USER ray

# Exactly the reviewed dependency delta, no re-resolution (--no-deps): the lockfile is authoritative.
COPY --chown=ray:users wheels/ /tmp/wheels/
COPY --chown=ray:users final-requirements.txt /tmp/final-requirements.txt
RUN python -m pip install --no-cache-dir --no-deps --find-links /tmp/wheels \
      -r /tmp/final-requirements.txt && rm -rf /tmp/wheels

# Job code only: main.py and one directory per --s3-py-modules zip under py_modules/, unpacked by
# scripts/unpack_py_modules.py. PY_MODULES_PATH is the colon list it prints; each directory is its own
# sys.path entry, as on Glue.
ARG PY_MODULES_PATH=""
COPY --chown=ray:users app/ /home/ray/app/
# TZ: the Ray base image sets TZ=America/Los_Angeles; Glue for Ray ran in UTC (local time in datetime.now(),
# time.localtime() and log timestamps).
ENV PYTHONPATH=${PY_MODULES_PATH} \
    PYTHONUNBUFFERED=1 \
    TZ=UTC
WORKDIR /home/ray/app
