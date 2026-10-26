#!/usr/bin/env python3
"""List the access other resources grant to the Glue job role by naming it in their own policies.

The pod role copies the Glue role's identity policies, but a bucket policy, a KMS key policy or a Lake
Formation permission that names the Glue role doesn't cover the pod role. This reads the policies the
agent saved (Step 3) and lists every statement that names the Glue role, for the customer to grant again
to the pod role. It changes nothing. Standard library only.

Files in --dir (one per resource: stdout as <name>.json, stderr as <name>.error; an empty file counts as absent):
  s3-<bucket>.json        aws s3api get-bucket-policy --bucket <bucket>
  kms-<key-id>.json       aws kms get-key-policy --key-id <key-id> --policy-name default
  lakeformation-<n>.json  aws lakeformation list-permissions --max-results 1000 [--next-token <token>], one file
                          per page (the CLI doesn't page this call by itself). All permissions the caller can see;
                          this keeps the Glue role's. Filtering by --principal needs --resource, which the job
                          doesn't name. A single lakeformation.json is read as page 1.

A statement names the Glue role when one of its values is the role's ARN or role ID (`aws:userid` values
`<role-id>:<session>` too) or starts with the role's assumed-role ARN. Wildcard patterns (`ArnLike` with `*`) aren't
matched, so the report says to check them.
"""
import argparse
import glob
import json
import os
import re
import sys


def role_markers(plan):
    """(role ARN, role ID, assumed-role ARN prefix) of the Glue role; missing ones are None."""
    role = plan.get("iam_role") or {}
    arn = role.get("arn") or plan["glue"].get("role") or ""
    m = re.match(r"arn:([^:]+):iam::(\d+):role/(?:.*/)?([^/]+)$", arn)
    sts = f"arn:{m.group(1)}:sts::{m.group(2)}:assumed-role/{m.group(3)}/" if m else None
    # A policy shows the role ID instead of the ARN once the role it named was deleted and recreated.
    return arn or None, role.get("role_id") or None, sts


def names_role(value, markers):
    arn, role_id, sts = markers
    return (value == arn or (role_id and (value == role_id or value.startswith(role_id + ":")))
            or (sts and value.startswith(sts)))


def strings(node, path=""):
    """(path, value) for every string in a statement; list items keep their parent's path."""
    if isinstance(node, dict):
        for k, v in node.items():
            yield from strings(v, f"{path}.{k}" if path else k)
    elif isinstance(node, list):
        for v in node:
            yield from strings(v, path)
    elif isinstance(node, str):
        yield path, node


def statements_naming(policy, markers):
    """(effect, sid, actions, where, exempt) for each statement that names the Glue role.

    `where` is the element and value that matched. `exempt` is True when the statement names the role to leave
    it out (NotPrincipal, or a negated condition such as StringNotEquals aws:PrincipalArn).
    """
    stmts = policy.get("Statement", [])
    for st in stmts if isinstance(stmts, list) else [stmts]:
        if not isinstance(st, dict):
            continue
        hits = [(path, v) for path, v in strings({k: st[k] for k in ("Principal", "NotPrincipal", "Condition")
                                                  if k in st}) if names_role(v, markers)]
        if not hits:
            continue
        actions = st.get("Action", st.get("NotAction", []))
        exempt = any(path.startswith("NotPrincipal") or re.match(r"Condition\.(ForAllValues:|ForAnyValue:)?"
                                                                  r"(StringNot|ArnNot)", path) for path, _ in hits)
        where = "; ".join(f"{path} = {v}" for path, v in hits)
        yield (st.get("Effect"), st.get("Sid", "(no Sid)"), actions if isinstance(actions, list) else [actions],
               where, exempt)


def read(path):
    if not os.path.exists(path):
        return ""
    with open(path) as f:
        return f.read()


def parse(path, field=None):
    """The saved JSON (the decoded string in `field`, if given), or None if it doesn't parse."""
    try:
        doc = json.loads(read(path))
        return json.loads(doc[field]) if field else doc
    except (ValueError, KeyError, TypeError):
        return None


UNPARSED = "Save it again and run this again"
LF = ("Lake Formation", "permissions")


def lake_formation_rows(d, markers):
    """Rows for the saved list-permissions pages (lakeformation-<n>.json, or a single lakeformation.json)."""
    numbered = {}
    for f in glob.glob(os.path.join(d, "lakeformation-*.json")) + glob.glob(os.path.join(d, "lakeformation-*.error")):
        m = re.search(r"-(\d+)\.(json|error)$", f)
        if m:
            numbered[int(m.group(1))] = f[:-len(m.group(2)) - 1]
    pages = [numbered[n] for n in sorted(numbered)] or [os.path.join(d, "lakeformation")]
    perms, more = [], sorted(numbered) != list(range(1, len(numbered) + 1))   # a page is missing
    for base in pages:
        err = read(base + ".error").strip()
        if err:
            return [LF + ("couldn't check: " + err.splitlines()[-1][:160], "Ask the data lake administrator")]
        if not read(base + ".json").strip():
            return [LF + ("not checked", "Save every page of list-permissions and run this again")]
        doc = parse(base + ".json")
        listed = doc.get("PrincipalResourcePermissions") if isinstance(doc, dict) else None
        if not isinstance(listed, list):
            return [LF + ("couldn't parse the saved permissions", UNPARSED)]
        perms += [x for x in listed if isinstance(x, dict)
                  and (x.get("Principal") or {}).get("DataLakePrincipalIdentifier") == markers[0]]
    more = more or bool(doc.get("NextToken"))
    rows = [(json.dumps(x.get("Resource", {}), separators=(",", ":"))[:200], "Lake Formation permission",
             ", ".join(x.get("Permissions", [])), "`aws lakeformation grant-permissions` to the pod role") for x in perms]
    if more:
        rows.append(LF + ("incomplete: the saved listing has more pages",
                          "Save every page (--next-token) and run this again"))
    elif not perms:
        rows.append(LF + ("none visible to this caller", ""))
    return rows


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--plan", required=True)
    p.add_argument("--dir", required=True, help="directory with the saved resource policies")
    p.add_argument("--out", required=True, help="markdown table for the report and the review gate")
    a = p.parse_args()
    plan = json.load(open(a.plan))
    markers = role_markers(plan)
    if not markers:
        sys.exit("plan.json has no Glue role ARN (run the analyzer with --role)")
    rows = []
    buckets = sorted({re.match(r"s3a?://([^/]+)", u).group(1)
                      for u in plan["s3_access"]["read"] + plan["s3_access"]["write"]})
    resources = [(f"s3://{b}", "bucket policy", os.path.join(a.dir, f"s3-{b}")) for b in buckets]
    resources += [(os.path.basename(f)[4:-5], "KMS key policy", f[:-5])
                  for f in sorted(set(glob.glob(os.path.join(a.dir, "kms-*.json")) +
                                      [e[:-6] + ".json" for e in glob.glob(os.path.join(a.dir, "kms-*.error"))]))]
    for name, kind, base in resources:
        err = read(base + ".error").strip()
        if err:
            if "NoSuchBucketPolicy" in err:
                rows.append((name, kind, "none (no bucket policy)", ""))
            else:
                rows.append((name, kind, "couldn't check: " + err.splitlines()[-1][:160],
                             "Ask the resource's owner whether its policy names the Glue role"))
            continue
        if not read(base + ".json").strip():
            rows.append((name, kind, "not checked", "Save its policy and run this again"))
            continue
        policy = parse(base + ".json", "Policy")
        if not isinstance(policy, dict):
            rows.append((name, kind, "couldn't parse the saved policy", UNPARSED))
            continue
        found = list(statements_naming(policy, markers))
        if not found:
            rows.append((name, kind, "none", ""))
        for effect, sid, actions, where, exempt in found:
            if exempt:
                change = ("The Glue role is exempted here; add the pod role next to it, or this statement applies "
                          "to the pod role" + (" and denies it" if effect == "Deny" else ""))
            elif effect == "Allow":
                change = "Add the pod role's ARN next to the Glue role's in this statement"
            else:
                change = "This Deny covers the Glue role only; add the pod role to keep the same restriction"
            rows.append((name, kind, f"{effect} `{sid}` (`{where}`): {', '.join(actions)}", change))
    rows += lake_formation_rows(a.dir, markers)
    lines = ["| Resource | Kind | Grants the Glue role | Change for the customer |", "|---|---|---|---|"]
    lines += [f"| {r[0]} | {r[1]} | {r[2]} | {r[3]} |" for r in rows]
    lines += ["", "Statements that cover the Glue role only through a wildcard (for example `ArnLike` "
                  "`aws:PrincipalArn` `...:role/glue*`) aren't listed, and a wildcard that covers the Glue role may not "
                  "cover the pod role's name; check such statements in the policies above. Secrets Manager resource "
                  "policies and roles in other accounts that trust the Glue role can't be found from the job "
                  "definition; check them if the job uses any."]
    with open(a.out, "w") as f:
        f.write("\n".join(lines) + "\n")
    needs = sum(1 for r in rows if r[3])
    print(f"grants: {a.out}  resources: {len(rows)}  needing the customer's attention: {needs}")


if __name__ == "__main__":
    main()
