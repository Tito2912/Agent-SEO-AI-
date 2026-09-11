# -*- coding: utf-8 -*-
"""Run the corrector across every gauntlet family, onto ONE branch.

The point is not to open thirty pull requests. It is to find, before any customer does, the
families whose targeting or hint is wrong — which is where seven of today's eight defects were.
Each family is prepared and patched exactly as the endpoint does it; the branch accumulates, so
a later family sees the earlier fixes (that is also what a real multi-issue run does).
"""
import datetime as dt
import json
import os
import sys

sys.path.insert(0, "seo-agent-web")
for line in open("seo-agent-web.env", encoding="utf-8", errors="replace"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "x" * 20)
from backend import app as m  # noqa: E402
from backend import audit_dashboard as dash  # noqa: E402
from backend import repo_index  # noqa: E402

# Where the "before" crawl was written, and where this run's result goes. Passed in, because a
# scratchpad is session-scoped and this bench has to outlive the session that built it.
SD = os.environ["GAUNTLET_WORKDIR"]
TOKEN = os.environ["FIXTURE_TOKEN"]
# La stack se choisit par `GAUNTLET_STACK`. Le parcours n'a plus rien de propre au HTML statique
# depuis qu'il existe dans les neuf idiomes, et c'est justement d'une stack a l'autre que les
# defauts de ciblage se voient : un objet `metadata` TypeScript ne se corrige pas comme du
# front matter YAML.
STACK = os.environ.get("GAUNTLET_STACK", "static-html")
OWNER, BRANCH = "pployeraffiliation-a11y", "main"
REPO = f"noyaru-stack-{STACK}"
SITE = f"{REPO}.netlify.app"

report = json.load(open(os.path.join(SD, "before", "report.json"), encoding="utf-8"))
issues = report["issues"]
handled = set(m._handled_issue_keys())
shown = lambda k: (k in dash.ISSUE_CATALOG and k not in dash.NON_ISSUE_KEYS  # noqa: E731
                   and not dash.is_delta_issue_key(k))
families = [k for k, v in issues.items()
            if isinstance(v, dict) and v.get("count") and shown(k) and k in handled]
# One representative per length/indexability family: the endpoint groups them itself.
seen_group: set[str] = set()
ordered: list[str] = []
for k in sorted(families):
    grp = m._length_family_name(k) or k.removesuffix("_indexable").removesuffix("_not_indexable")
    if grp in seen_group:
        continue
    seen_group.add(grp)
    ordered.append(k)
print(f"familles a exercer : {len(ordered)}")

tree = m._github_api_get(m._github_api_path("repos", OWNER, REPO, "git", "trees", BRANCH),
                         token=TOKEN, params={"recursive": "1"})
paths = [b["path"] for b in tree.get("tree", []) if b.get("type") == "blob"]
idx = repo_index.build_repo_index(paths)
base = m._github_api_get(m._github_api_path("repos", OWNER, REPO, "git", "ref", f"heads/{BRANCH}"),
                         token=TOKEN)["object"]["sha"]
fix_branch = f"gauntlet-{STACK}-{dt.datetime.now(dt.UTC).strftime('%Y%m%d-%H%M%S')}"
m._github_api_post(m._github_api_path("repos", OWNER, REPO, "git", "refs"), token=TOKEN,
                   json_body={"ref": f"refs/heads/{fix_branch}", "sha": base})
print("branche :", fix_branch)

file_state: dict[str, dict[str, str]] = {}
results = []
for key in ordered:
    label = (dash.ISSUE_CATALOG[key].label if key in dash.ISSUE_CATALOG else key)
    fam = m._length_family_keys(key)
    impacted: set[str] = set()
    for k in fam:
        if k in issues:
            impacted |= dash.extract_impacted_pages(k, issues.get(k))
    impacted_l = sorted(impacted)
    try:
        prep = m._prepare_issue_fix(issue_key=key, issues=issues, impacted=impacted_l,
                                    all_paths=paths, site_name=SITE, owner=OWNER, repo_name=REPO,
                                    branch=BRANCH, token=TOKEN, model_override="",
                                    pages=report.get("pages"))
    except Exception as exc:
        results.append((key, "ERREUR prepare", str(exc)[:80], 0, 0))
        continue
    if prep["refusal"]:
        results.append((key, "refus", prep["refusal"][:70], 0, 0))
        continue
    try:
        patched, skipped, targets, ai = m._deep_patch_issue_files(
            owner=OWNER, repo_name=REPO, branch=BRANCH, token=TOKEN, fix_branch=fix_branch,
            all_paths=paths, issue_key=key, issue_label=label, impacted_urls=impacted_l,
            site_name=SITE, file_state=file_state, max_files=6, evidence=prep["evidence"],
            extra_hint=prep["extra_hint"], model_override="",
            link_rewriter=prep["link_rewriter"], rewriter_ai_fallback=prep["rewriter_ai_fallback"],
            rewriter_is_ai=bool(prep["rewriter_is_ai"]), index=idx,
            targets_override=prep.get("targets_override"),
            page_side=bool(prep.get("page_side")),
            # Le meme fait que l'endpoint passe : la langue mesuree sur le site. Sans elle, le
            # banc ne testerait pas ce que le produit fait.
            site_lang=m._dominant_site_lang(report.get("pages")))
    except Exception as exc:
        results.append((key, "ERREUR patch", str(exc)[:80], 0, 0))
        continue
    verdict = "ok" if patched else ("AUCUN PATCH" if targets else "AUCUNE CIBLE")
    results.append((key, verdict, ",".join(targets[:2])[:70], len(patched), len(ai)))
    print(f"  {verdict:<12} {key:<46} cibles={len(targets)} patches={len(patched)} ia={len(ai)}")

json.dump({"branch": fix_branch, "results": results},
          open(os.path.join(SD, "gauntlet_run.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=1)
print("\n--- bilan ---")
for v in ("ok", "AUCUN PATCH", "AUCUNE CIBLE", "refus", "ERREUR prepare", "ERREUR patch"):
    n = [r for r in results if r[1] == v]
    if n:
        print(f"{len(n):>3}  {v}")
        if v != "ok":
            for r in n:
                print(f"       {r[0]} — {r[2]}")
