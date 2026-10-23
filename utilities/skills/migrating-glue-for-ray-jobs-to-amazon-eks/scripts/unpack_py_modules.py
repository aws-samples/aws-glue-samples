"""Unpack --s3-py-modules zips (and s3:// zips from --pip-install) exactly as Glue for Ray did.

Glue passed these zips to Ray as runtime_env py_modules. Ray unpacks each zip into its own
directory, strips the zip's single top-level directory if it has exactly one, and puts that
directory itself on sys.path (driver and workers). So a zip of `mypkg/` exposes mypkg's modules as
top-level modules, not `import mypkg`; a zip of `dist/mypkg/` exposes `import mypkg`.

Standard library only.

Usage:
  python3 unpack_py_modules.py --out source/py_modules zip1.zip [zip2.zip ...]

Prints the PYTHONPATH value for the image (one directory per zip, under /home/ray/app/py_modules),
in the order given. Pass it to `docker build --build-arg PY_MODULES_PATH=...`.
"""
import argparse
import os
import re
import shutil
import sys
import zipfile

IMAGE_ROOT = "/home/ray/app/py_modules"


def unpack(zip_path, out_root, name):
    dest = os.path.join(out_root, name)
    if os.path.exists(dest):
        shutil.rmtree(dest)
    os.makedirs(dest)
    with zipfile.ZipFile(zip_path) as z:
        names = [n for n in z.namelist() if not n.startswith("__MACOSX/")]
        tops = {n.split("/", 1)[0] for n in names}
        strip = len(tops) == 1 and any("/" in n for n in names)
        for n in names:
            rel = n.split("/", 1)[1] if strip and "/" in n else ("" if strip else n)
            if not rel or n.endswith("/"):
                continue
            target = os.path.realpath(os.path.join(dest, rel))
            if not target.startswith(os.path.realpath(dest) + os.sep):
                # Absolute or "../" entries would land outside the module directory; zipfile.extractall
                # (which Ray uses) drops them too.
                print(f"{zip_path}: skipped unsafe path {n!r}", file=sys.stderr)
                continue
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with z.open(n) as src, open(target, "wb") as dst:
                shutil.copyfileobj(src, dst)
    importable = sorted(e[:-3] if e.endswith(".py") else e for e in os.listdir(dest)
                        if (e.endswith(".py") and e != "__init__.py")
                        or os.path.isfile(os.path.join(dest, e, "__init__.py")))
    return strip, importable


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", required=True)
    p.add_argument("zips", nargs="+")
    a = p.parse_args()
    os.makedirs(a.out, exist_ok=True)
    paths, used = [], set()
    for i, z in enumerate(a.zips):
        name = re.sub(r"[^A-Za-z0-9_.-]", "_", os.path.splitext(os.path.basename(z))[0])
        if name in used:
            name = f"{name}_{i}"
        used.add(name)
        strip, importable = unpack(z, a.out, name)
        paths.append(f"{IMAGE_ROOT}/{name}")
        print(f"{z}: {'stripped single top-level dir; ' if strip else ''}importable as: "
              f"{', '.join(importable) or '(nothing importable at top level)'}", file=sys.stderr)
    print(":".join(paths))
    return 0


if __name__ == "__main__":
    sys.exit(main())
