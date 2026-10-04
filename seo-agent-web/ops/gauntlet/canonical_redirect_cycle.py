"""Owned static fixture: two canonicals sharing a 404 and one forced self-redirect.

Zero models, bounded QA writes, exact builds, raw and controlled crawls, closed PRs.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
from pathlib import Path
import sys
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from defusedxml import ElementTree as XML  # noqa: E402
from ops.gauntlet import hreflang_cycle as projection, title_cycle as titles  # noqa: E402
from ops.gauntlet.ai_budget import ClaudeBudget  # noqa: E402

live = projection.live
REPO = "noyaru-stack-static-html"
MAIN = "ba041ef05c0f51c1c228e3442928c896fbc00102"
SITE = f"https://{REPO}.netlify.app/"
NAMES = ("qa-canonical-a", "qa-canonical-b", "qa-self-loop")
ROUTES = {name: "/gauntlet/" + name for name in NAMES}
FILES = {name: "gauntlet/" + name + ".html" for name in NAMES}
DEAD = SITE + "gauntlet/qa-shared-disappeared"
LOOP = ROUTES[NAMES[2]]
RULE = f"{LOOP} {LOOP} 301!\n"
KEYS = ("canonical_points_to_4xx", "redirect_3xx")
SETUP = {"index.html", "_redirects", "sitemap.xml", *FILES.values()}
CORRECTION = {FILES[NAMES[0]], FILES[NAMES[1]], "_redirects"}


def page(name: str) -> str:
    if name not in NAMES:
        raise ValueError("Only the three owned fixture page names are accepted.")
    own = SITE.rstrip("/") + ROUTES[name]
    canonical = DEAD if name != NAMES[2] else own
    title = "Noyaru controle " + name
    description = ("Validation technique de " + name + " sur le depot de test Noyaru. "
                   "Cette page conserve ses contenus pendant le controle des balises et des redirections.")
    meta = f'<meta name="description" content="{description}" />'
    for prefix in ("og", "twitter"):
        for key, value in {"title": title, "description": description, "image": SITE + "og.png"}.items():
            meta += f'<meta {"property" if prefix == "og" else "name"}="{prefix}:{key}" content="{value}" />'
    return ('<!doctype html><html lang="fr"><head><meta charset="utf-8" />'
        '<meta name="viewport" content="width=device-width, initial-scale=1" />'
        + f'<title>{title}</title>{meta}<meta property="og:type" content="website" />'
        + f'<meta property="og:url" content="{own}" /><meta name="twitter:card" content="summary_large_image" />'
        + f'<link rel="canonical" href="{canonical}" />'
        + f'<link rel="alternate" hreflang="fr" href="{DEAD}" /></head>'
        + f'<body><h1>{title}</h1><p>{description}</p><p><a href="/gauntlet/qa-shared-disappeared">Destination temoin</a></p>'
        + '<p><a href="/">Accueil</a></p></body></html>\n')


def observed_response(preview: str, work: Path) -> dict:
    rows = {}
    for name, route in {**ROUTES, "dead": "/gauntlet/qa-shared-disappeared"}.items():
        response = live.requests.get(preview.rstrip("/") + route, timeout=30, allow_redirects=False)
        (work / (name + ".http")).write_bytes(response.content)
        rows[name] = {"status": response.status_code, "location": response.headers.get("location", ""),
            "x_robots_tag": response.headers.get("x-robots-tag", ""),
            "sha256": hashlib.sha256(response.content).hexdigest()}
    live.save(work / "http.json", rows)
    return rows


def checks(before: dict, after: dict) -> dict:
    bp, ap = live._html_pages(before), live._html_pages(after)
    for name in NAMES[:2]:
        route = ROUTES[name]
        if bp[route]["canonical"] != DEAD or ap[route]["canonical"] != SITE.rstrip("/") + route:
            raise ValueError("canonical_positive_control_or_exact_destination_failed")
    if not bp.keys() <= ap.keys() or LOOP not in ap:
        raise ValueError("observed_html_routes_lost_or_loop_not_served")
    fields = titles.PRESERVED + ("title", "title_tag_count", "meta_description", "meta_description_tag_count",
        "h1_tag_count", "h2_tag_count", "content_sketch", "text_word_count", "internal_link_items",
        "image_urls", "ld_json_blocks", "schema_org_errors")
    changes = []
    for route, old in bp.items():
        for field in fields:
            if old.get(field) != ap[route].get(field) and not (field == "canonical" and route in {ROUTES[n] for n in NAMES[:2]}):
                changes.append({"route": route, "field": field})
    if changes:
        raise ValueError("collateral_html_changed")
    return {"before_html_routes": len(bp), "after_html_routes": len(ap), "preserved_existing_html": True,
        "selected_dead_canonical_occurrences": [2, 0], "self_loop_served_after": True,
        "increased_counts": {k: [before["issues"].get(k, {}).get("count", 0), v.get("count", 0)]
            for k, v in after["issues"].items() if v.get("count", 0) > before["issues"].get(k, {}).get("count", 0)}}


def cycle(m, token: str, work: Path, budget: ClaudeBudget) -> dict:
    if budget.limit != 0:
        raise ValueError("Only zero provider calls are allowed.")
    result = {"status": "unverified", "stage": "identity", "pull_requests": [], "writes": [],
              "repository": f"{live.OWNER}/{REPO}", "universal_certification": False}
    original_put = m._github_api_put
    writable, allowed = "", set()

    def ref(branch):
        return m._github_api_get(m._github_ref_api_path(live.OWNER, REPO, branch), token=token)["object"]["sha"]

    def read(path, branch):
        data = m._github_api_get(m._github_content_api_path(live.OWNER, REPO, path), token=token, params={"ref": branch})
        return data["sha"], base64.b64decode(data["content"]).decode()

    def guarded_put(path, **kw):
        known = {m._github_content_api_path(live.OWNER, REPO, name): name for name in allowed}
        if path not in known or kw["json_body"].get("branch") != writable or not writable or len(result["writes"]) >= 9:
            raise ValueError("outside_owned_qa_write_scope")
        data = original_put(path, **kw)
        result["writes"].append({"branch": writable, "path": known[path]})
        return data

    def put(path, content, sha=""):
        body = {"message": "QA canonical/redirect fixture (never merge)", "branch": writable,
                "content": base64.b64encode(content.encode()).decode()}
        if sha:
            body["sha"] = sha
        guarded_put(m._github_content_api_path(live.OWNER, REPO, path), token=token, json_body=body)

    def branch(name, sha):
        if not name.startswith("gauntlet-static-html-canonical-") or not m._github_branch_allowed(name):
            raise ValueError("outside_owned_qa_branch_scope")
        m._github_api_post(m._github_api_path("repos", live.OWNER, REPO, "git", "refs"), token=token,
            json_body={"ref": "refs/heads/" + name, "sha": sha})

    def preview(kind, head):
        result["stage"] = kind
        pr = m._ouvrir_pull_request(owner=live.OWNER, repo=REPO, token=token, head=head, base="main", draft=True,
            title="QA canonical/redirect " + kind + " (never merge)", body="Owned fixture only. Close without merging.")
        row = {"number": pr["number"], "url": pr["html_url"], "kind": kind}
        result["pull_requests"].append(row)
        live.save(work / "cycle.json", result)
        expected = ref(head)
        row["build"] = live.wait_build(m, REPO, token, pr["number"], expected_sha=expected)
        if row["build"]["head_sha"] != expected or ref(head) != expected:
            raise ValueError("exact_head_changed")
        url = f"https://deploy-preview-{pr['number']}--{REPO}.netlify.app/"
        sitemap = titles.preview_sitemap(url)
        out = work / kind
        out.mkdir()
        (out / "sitemap.xml").write_bytes(sitemap)
        observations = observed_response(url, out)
        live.crawl("static-html", url, out / "raw", 90, preview=True)
        report = projection.rescore(out / "raw/report.json", url, SITE, sitemap, out / "controlled")
        return report, observations

    m._github_api_put = guarded_put
    try:
        if ref("main") != MAIN:
            raise ValueError("fixture_main_identity_changed")
        stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d-%H%M%S")
        baseline = "gauntlet-static-html-canonical-baseline-" + stamp
        corrected = "gauntlet-static-html-canonical-correction-" + stamp
        branch(baseline, MAIN)
        result.update(main_sha=MAIN, baseline_branch=baseline, correction_branch=corrected)
        tree = m._github_api_get(m._github_api_path("repos", live.OWNER, REPO, "git", "trees", MAIN),
                                token=token, params={"recursive": "1"})
        if tree.get("truncated"):
            raise ValueError("incomplete_fixture_tree")
        paths = [r["path"] for r in tree["tree"] if r["type"] == "blob"]
        if set(FILES.values()) & set(paths):
            raise ValueError("positive_control_pages_already_exist")
        writable, allowed = baseline, SETUP
        for name, path in FILES.items():
            put(path, page(name))
            paths.append(path)
        sha, source = read("index.html", baseline)
        if source.count("</body>") != 1:
            raise ValueError("unresolved_fixture_navigation")
        put("index.html", source.replace("</body>", "".join(
            f'<p><a href="{route}">{name}</a></p>' for name, route in ROUTES.items()) + "</body>"), sha)
        sha, source = read("sitemap.xml", baseline)
        if XML.fromstring(source).tag != "{http://www.sitemaps.org/schemas/sitemap/0.9}urlset" or source.count("</urlset>") != 1:
            raise ValueError("unresolved_fixture_sitemap")
        put("sitemap.xml", source.replace("</urlset>", "".join(
            f'<url><loc>{SITE.rstrip("/") + route}</loc></url>\n' for route in ROUTES.values()) + "</urlset>"), sha)
        sha, source = read("_redirects", baseline)
        put("_redirects", RULE + source, sha)
        before, http = preview("baseline", baseline)
        if http["dead"]["status"] != 404 or http[NAMES[2]]["status"] != 301:
            raise ValueError("missing_http_positive_controls")
        from backend import repo_index
        index, state = repo_index.build_repo_index(paths), {}
        scopes = {KEYS[0]: [SITE.rstrip("/") + ROUTES[name] for name in NAMES[:2]], KEYS[1]: [SITE.rstrip("/") + LOOP]}
        branch(corrected, ref(baseline))
        writable, allowed = corrected, CORRECTION
        for key, urls in scopes.items():
            result["stage"] = key
            prep = m._prepare_issue_fix(issue_key=key, issues=before["issues"], impacted=urls, all_paths=paths,
                site_name=urlsplit(SITE).netloc, owner=live.OWNER, repo_name=REPO, branch=baseline, token=token,
                pages=before["pages"])
            if prep["refusal"]:
                raise ValueError("repair_refused_" + key)
            if key == KEYS[0]:
                selected = [p for p in prep["canonical_pairs"] if p["page"] in urls]
                if len(selected) != 2:
                    raise ValueError("missing_shared_404_positive_controls")
                prep["canonical_pairs"] = selected
            elif prep["loop_paths"] != [LOOP]:
                raise ValueError("missing_tracked_self_loop_positive_control")
            applied = m._apply_prepared_issue_fix(owner=live.OWNER, repo_name=REPO, branch=baseline, token=token,
                fix_branch=corrected, all_paths=paths, issue_key=key, issue_label=key, impacted=urls, site_name=urlsplit(SITE).netloc,
                file_state=state, max_files=3, prep=prep, pages=before["pages"], index=index, allow_ai_targeting=False)
            result.setdefault("repairs", {})[key] = applied
            expected = CORRECTION - {"_redirects"} if key == KEYS[0] else {"_redirects"}
            if applied["ai_files"] or applied["skipped"] or set(applied["patched"] + applied["config_changes"]) != expected:
                raise ValueError("unexpected_repair_scope_" + key)
        after, http = preview("final", corrected)
        if http[NAMES[2]]["status"] != 200 or http[NAMES[2]]["x_robots_tag"] != "noindex":
            raise ValueError("loop_not_repaired_or_preview_indexing_changed")
        result["checks"] = checks(before, after)
        for name in NAMES[:2]:
            old = read(FILES[name], baseline)[1]
            new = read(FILES[name], corrected)[1]
            if new != old.replace('rel="canonical" href="' + DEAD,
                                  'rel="canonical" href="' + SITE.rstrip("/") + ROUTES[name]):
                raise ValueError("canonical_source_has_collateral_changes")
        if read("_redirects", corrected)[1] != read("_redirects", baseline)[1].removeprefix(RULE):
            raise ValueError("unrelated_redirect_rules_changed")
        result.update(status="measured_selected_canonical_and_self_loop", final_sha=ref(corrected),
            target_counts={k: [before["issues"][k]["count"], after["issues"][k]["count"]] for k in KEYS})
    except Exception as exc:
        result.update(status="failed", error_type=type(exc).__name__)
        if isinstance(exc, ValueError) and str(exc).replace("_", "").isalnum():
            result["failure_code"] = str(exc)
        print(result["status"], result["stage"], result.get("failure_code", result["error_type"]), flush=True)
    finally:
        m._github_api_put = original_put
        result["ai_budget"] = budget.summary()
        result["cleanup"] = live.close_prs(REPO, token, result["pull_requests"])
        result["prs_closed_unmerged"] = bool(result["pull_requests"]) and all(
            r.get("state") == "closed" and r.get("merged") is False for r in result["cleanup"])
        result["main_unchanged"] = ref("main") == MAIN
        if not result["main_unchanged"] or not result["prs_closed_unmerged"] or budget.summary()["attempted"] or budget.summary()["denied"]:
            result["status"] = "unverified"
        live.save(work / "cycle.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", type=Path, required=True)
    args = parser.parse_args()
    args.workdir.mkdir(parents=True, exist_ok=True)
    if any(args.workdir.iterdir()):
        parser.error("Use an empty temporary directory, never customer data.")
    m, token = live._load_backend(args.workdir)
    budget = ClaudeBudget(0)
    budget.install(m)
    result = cycle(m, token, args.workdir, budget)
    print(result["status"], flush=True)
    return int(result["status"] != "measured_selected_canonical_and_self_loop")


if __name__ == "__main__":
    raise SystemExit(main())
