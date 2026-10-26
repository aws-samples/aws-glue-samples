#!/bin/bash
# Resolve a migrated job's dependency delta on top of the target Ray base image.
# Runs INSIDE the base image (docker run ... [BASE_IMAGE] bash /w/scripts/resolve-deps.sh).
#
# Inputs (mounted at /w):
#   /w/requirements-glue-ray24.txt  the Glue Ray2.4 runtime's packages (templates/), installed first
#   /w/requirements.in   the job's own requirements (--pip-install / --additional-python-modules, wheels)
#   /w/source/           the job's code: main.py, working-dir contents, py_modules packages
#   /w/wheels/           optional local wheels (from --s3-py-modules .whl entries)
# Env:
#   PIP_EXTRA            optional extra pip options (e.g. "--index-url https://...")
# Outputs (/w/out/):
#   final-requirements.txt   exact pins of every package added or changed vs. the base image
#   import-check.txt         result of importing every third-party module the code imports
#   pip-resolve.log          full pip log (inspect on failure)
set -uo pipefail
mkdir -p /w/out
cd /tmp

# Glue for Ray never let --pip-install change ray, so pin it to the base image's version.
RAY_VER=$(python -c "import ray; print(ray.__version__)")
echo "ray==${RAY_VER}" > /tmp/ray-constraint.txt
python -m venv --system-site-packages /tmp/resolve-venv
# shellcheck disable=SC1091
source /tmp/resolve-venv/bin/activate
# The pip of the Glue Ray2.4 runtime, so the job's requirements resolve as they did there.
python -m pip install -q "pip==23.1.2" > /w/out/pip-tools.log 2>&1 \
  || { echo "=== Installing pip 23.1.2 (Glue's pip) failed; see /w/out/pip-tools.log ==="; tail -n 15 /w/out/pip-tools.log; exit 6; }
# Build backends for the no-build-isolation retry below, each installed (pinned) only where the base lacks
# it, so the base's own versions stay untouched. Installed before the base freeze, so they are build-time
# tools only and never part of the image delta. (Step 1 installs Glue's setuptools 67.7.2 anyway.)
for tool in setuptools==67.7.2 wheel==0.37.1 poetry-core==2.2.1; do
  python -m pip show -q "${tool%%==*}" >/dev/null 2>&1 && continue
  python -m pip install -q "${tool}" >> /w/out/pip-tools.log 2>&1 \
    || { echo "=== Installing the build tool ${tool} failed; see /w/out/pip-tools.log ==="; tail -n 15 /w/out/pip-tools.log; exit 6; }
done
# Freeze the base AFTER installing that pip, with the same pip that freezes the result (identical format).
python -m pip freeze --all 2>/dev/null | sort > /w/out/base-freeze.txt
[ -s /w/out/base-freeze.txt ] || { echo "=== ERROR: pip freeze of the base image returned nothing ==="; exit 6; }

# 1. The Glue runtime's package set, exactly (it is consistent as Glue shipped it, hence --no-deps).
# shellcheck disable=SC2086
python -m pip install --no-deps ${PIP_EXTRA:-} -c /tmp/ray-constraint.txt \
  -r /w/requirements-glue-ray24.txt > /w/out/pip-glue-runtime.log 2>&1 \
  || { echo "=== Installing the Glue runtime set failed; see /w/out/pip-glue-runtime.log ==="; tail -n 25 /w/out/pip-glue-runtime.log; exit 5; }

# Wheels built from source so far belong to the Glue runtime set; the ones built in step 2 are the job's.
find "${HOME:-/tmp}/.cache/pip/wheels" -name '*.whl' 2>/dev/null | sort > /tmp/wheels-after-glue-set.txt

# 2. The job's requirements on top, with the same plain `pip install` Glue ran: a package already
# installed that satisfies a requirement is kept, otherwise pip installs another version. A package with
# no wheel for the Python/arch is built from its sdist, as on Glue; every wheel built from source is
# saved into /w/wheels so the image build installs the same artifact without a compiler.
STATUS=0
if grep -qvE '^\s*(#|$)' /w/requirements.in 2>/dev/null; then
  # shellcheck disable=SC2086
  python -m pip install --find-links /w/wheels ${PIP_EXTRA:-} \
    -c /tmp/ray-constraint.txt -r /w/requirements.in > /w/out/pip-resolve.log 2>&1
  STATUS=$?
else
  echo "(no job requirements)" > /w/out/pip-resolve.log
fi
# Some old sdists run a build script with the target interpreter (for example `python build.py`)
# that imports setuptools, which pip's isolated build environment hides. Retry those without isolation.
if [ ${STATUS} -ne 0 ] && grep -qE "Failed building wheel for|metadata-generation-failed|did not run successfully" /w/out/pip-resolve.log; then
  echo "=== sdist build failed in an isolated build env; retrying with --no-build-isolation ==="
  mv /w/out/pip-resolve.log /w/out/pip-resolve-isolated.log
  # shellcheck disable=SC2086
  python -m pip install --no-build-isolation --find-links /w/wheels ${PIP_EXTRA:-} \
    -c /tmp/ray-constraint.txt -r /w/requirements.in > /w/out/pip-resolve.log 2>&1
  STATUS=$?
fi
if [ ${STATUS} -eq 0 ] && [ -d "${HOME:-/tmp}/.cache/pip/wheels" ]; then
  BUILT=$(find "${HOME:-/tmp}/.cache/pip/wheels" -name '*.whl' | sort)
  if [ -n "${BUILT}" ]; then
    # Every wheel built from source goes into the image build, the Glue runtime set's included.
    mkdir -p /w/wheels && cp ${BUILT} /w/wheels/
    JOB_BUILT=$(echo "${BUILT}" | comm -13 /tmp/wheels-after-glue-set.txt -)
    GLUE_BUILT=$(echo "${BUILT}" | comm -12 /tmp/wheels-after-glue-set.txt -)
    if [ -n "${JOB_BUILT}" ]; then
      echo "=== SOURCE-BUILT (no wheel for this Python/arch; built from the sdist and saved to wheels/):"
      for w in ${JOB_BUILT}; do basename "$w"; done
      echo "    Glue built these the same way. Check the build log in pip-resolve.log for skipped C"
      echo "    extensions (slower pure-Python fallbacks). ==="
    fi
    if [ -n "${GLUE_BUILT}" ]; then
      echo "=== Built from source for the Glue runtime set (saved to wheels/):"
      for w in ${GLUE_BUILT}; do basename "$w"; done
    fi
  fi
fi
if [ ${STATUS} -ne 0 ]; then
  echo "=== RESOLVE FAILED (exit ${STATUS}); last lines of /w/out/pip-resolve.log: ==="
  tail -n 25 /w/out/pip-resolve.log
  echo "=== CLASSIFY (references/troubleshooting.md):"
  echo "    'No matching distribution found for X' where X is a line of requirements.in => NO-WHEEL"
  echo "    (no wheel and no buildable sdist for this Python);"
  echo "    'Failed building wheel for X' / 'subprocess-exited-with-error' => SDIST-BUILD-FAILED;"
  echo "    for either, if X's 'Collecting X' line says '(from <pkg>)', X is a dependency => TRANSITIVE-PIN;"
  echo "    (upgrade <pkg>, usually by unpinning it); 'ResolutionImpossible' => RESOLVER-CONFLICT"
  echo "    (if it names ray==${RAY_VER} => RAY-PIN-CONFLICT); 'Could not fetch URL'/'Connection'"
  echo "    => INDEX-UNREACHABLE. ==="
  exit ${STATUS}
fi

# Delta vs. the base image = exactly what the image build must install (with --no-deps).
python -m pip freeze --all 2>/dev/null | sort > /w/out/venv-freeze.txt
[ -s /w/out/venv-freeze.txt ] || { echo "=== ERROR: pip freeze of the resolved environment returned nothing ==="; exit 6; }
comm -13 /w/out/base-freeze.txt /w/out/venv-freeze.txt \
  | grep -vE '^pip==' > /w/out/final-requirements.txt
# Wheels installed from /w/wheels freeze as "name @ file:///w/wheels/..."; pin them by version so the
# image build finds them with --find-links. Any other local path is not reproducible.
python - <<'PYEOF'
import importlib.metadata as md, re
p = "/w/out/final-requirements.txt"
out = []
for line in open(p).read().splitlines():
    m = re.match(r"^([A-Za-z0-9_.-]+(?:\[[^\]]*\])?) @ file://(/w/wheels/\S+)$", line)
    out.append(f"{m.group(1).split('[')[0]}=={md.version(m.group(1).split('[')[0])}" if m else line)
open(p, "w").write("\n".join(out) + ("\n" if out else ""))
PYEOF
if grep -q ' @ file:' /w/out/final-requirements.txt; then
  echo "=== ERROR: final-requirements.txt has local 'name @ file:' entries outside /w/wheels:"
  grep ' @ file:' /w/out/final-requirements.txt
  echo "    Put the wheel in /w/wheels instead. ==="
  exit 4
fi
echo "=== final-requirements.txt ($(wc -l < /w/out/final-requirements.txt) packages added/changed) ==="
cat /w/out/final-requirements.txt

# Import check: every top-level module the code imports must load, except modules that ship with
# the job itself (main script, working-dir files, py_modules packages).
python - > /w/out/import-check.txt 2>&1 <<'PYEOF'
import ast, importlib, importlib.util, os, sys, sysconfig
src = "/w/source"


def top_level(d):
    # Module and package names importable from directory d (regular and namespace packages).
    names = set()
    for e in (os.listdir(d) if os.path.isdir(d) else []):
        p = os.path.join(d, e)
        if os.path.isdir(p) and not e.startswith(("__", ".")):
            names.add(e)
        elif e.endswith(".py") or e.endswith(".so"):
            names.add(e.split(".")[0])
    return names


# Modules that ship with the job: the script's directory, each py_modules directory (on sys.path),
# and the working dir (its single top-level directory stripped, as Ray does).
local = top_level(src)
pm = os.path.join(src, "py_modules")
for z in (os.listdir(pm) if os.path.isdir(pm) else []):
    local |= top_level(os.path.join(pm, z))
wd = os.path.join(src, "working_dir")
local |= top_level(wd)
subs = [e for e in (os.listdir(wd) if os.path.isdir(wd) else []) if e != "__MACOSX"]
if len(subs) == 1 and os.path.isdir(os.path.join(wd, subs[0])):
    local |= top_level(os.path.join(wd, subs[0]))
mods, optional = set(), set()
for root, dirs, files in os.walk(src):
    for f in files:
        if f.endswith(".py"):
            try:
                tree = ast.parse(open(os.path.join(root, f), encoding="utf-8", errors="replace").read())
            except SyntaxError:
                continue
            guarded = set()   # imports inside `try: ... except ImportError` are optional
            for t in ast.walk(tree):
                if isinstance(t, ast.Try) and any(
                        h.type is None or any(isinstance(x, ast.Name) and x.id in ("ImportError", "ModuleNotFoundError", "Exception")
                                              for x in (h.type.elts if isinstance(h.type, ast.Tuple) else [h.type]))
                        for h in t.handlers):
                    for b in t.body:
                        guarded.update(id(x) for x in ast.walk(b))
            for n in ast.walk(tree):
                names = []
                if isinstance(n, ast.Import):
                    names = [a.name.split(".")[0] for a in n.names]
                elif isinstance(n, ast.ImportFrom) and n.level == 0 and n.module:
                    names = [n.module.split(".")[0]]
                (optional if id(n) in guarded else mods).update(names)
stdlib = set(getattr(sys, "stdlib_module_names", ())) | set(sys.builtin_module_names)
STDLIB_DIR = sysconfig.get_paths()["stdlib"]


def in_stdlib(m):  # sys.stdlib_module_names is Python 3.10+; on 3.9 check where the module lives
    if m in stdlib:
        return True
    try:
        o = getattr(importlib.util.find_spec(m), "origin", None) or ""
    except Exception:
        return False
    return o in ("built-in", "frozen") or (o.startswith(STDLIB_DIR) and "site-packages" not in o)


failed = []
for m in sorted(m for m in mods - local - {"__future__"} if not in_stdlib(m)):
    try:
        importlib.import_module(m)
        print(f"OK    {m}")
    except Exception as e:  # report every failure, keep going
        failed.append(m)
        print(f"FAIL  {m}: {type(e).__name__}: {e}")
for m in sorted(optional - mods - local - stdlib - {"__future__"}):
    try:
        importlib.import_module(m)
        print(f"OK    {m} (optional)")
    except Exception as e:
        print(f"SKIP  {m} (optional import, guarded by try/except): {type(e).__name__}")
# The image puts each py_modules directory on PYTHONPATH, which comes before the standard library and
# site-packages for every Python process in the image, Ray's own included. A module there with the name of
# one the image already has replaces it for those processes too (on Glue it was the job's processes only).
for z in sorted(os.listdir(pm) if os.path.isdir(pm) else []):
    for m in sorted(top_level(os.path.join(pm, z))):
        try:
            spec = importlib.util.find_spec(m)
        except Exception:
            spec = None
        if spec is not None:
            print(f"SHADOW {m}: py_modules/{z} replaces {spec.origin or 'a package'} of the image for every "
                  "Python process (warning)")
print("RESULT:", "FAILED " + " ".join(failed) if failed else "all imports OK")
PYEOF
cat /w/out/import-check.txt
grep -q "^RESULT: all imports OK" /w/out/import-check.txt || exit 3
