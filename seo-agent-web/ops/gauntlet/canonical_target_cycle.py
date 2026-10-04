"""Measure redirect and noncanonical destinations on the owned static fixture only."""

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
from ops.gauntlet import canonical_redirect_cycle as previous  # noqa: E402
from ops.gauntlet.ai_budget import ClaudeBudget  # noqa: E402

live, projection, titles = previous.live, previous.projection, previous.titles
REPO, MAIN, SITE = previous.REPO, previous.MAIN, previous.SITE
NAMES = ("qa-hop-source", "qa-chain-source", "qa-relay", "qa-master", "qa-control")
ROUTES = {name: "/gauntlet/" + name for name in NAMES}
FILES = {name: "gauntlet/" + name + ".html" for name in NAMES}
URLS = {name: SITE.rstrip("/") + route for name, route in ROUTES.items()}
OLD = "/gauntlet/qa-hop-old"
RULE = f"{OLD} {ROUTES['qa-master']} 301!\n"
SCOPES = {"canonical_points_to_redirect": NAMES[0], "non_canonical_page_specified_as_canonical_one": NAMES[1]}
BEFORE = {NAMES[0]: SITE.rstrip("/") + OLD, NAMES[1]: URLS["qa-relay"],
          "qa-relay": URLS["qa-master"], "qa-master": URLS["qa-master"], "qa-control": URLS["qa-control"]}
SETUP = {"index.html", "sitemap.xml", "_redirects", *FILES.values()}
CORRECTION = {FILES[name] for name in SCOPES.values()}


def page(name: str) -> str:
    if name not in NAMES:
        raise ValueError("Only owned QA page names are permitted.")
    source = previous.page(previous.NAMES[0])
    source = source.replace(previous.NAMES[0], name).replace(previous.DEAD, BEFORE[name])
    source = source.replace(URLS[name], URLS["qa-master"]) if name in SCOPES.values() else source
    links = "".join(f'<p><a href="{route}">{peer}</a></p>' for peer, route in ROUTES.items())
    links += f'<p><a href="{OLD}">Redirection temoin</a></p>'
    source = source.replace('<p><a href="/gauntlet/qa-shared-disappeared">Destination temoin</a></p>', links)
    return source


def observations(preview: str, work: Path) -> dict:
    rows = {}
    for name, route in {**ROUTES, "old": OLD}.items():
        response = live.requests.get(preview.rstrip("/") + route, timeout=30, allow_redirects=False)
        (work / (name + ".http")).write_bytes(response.content)
        rows[name] = {"status": response.status_code, "location": response.headers.get("location", ""),
            "x_robots_tag": response.headers.get("x-robots-tag", ""),
            "sha256": hashlib.sha256(response.content).hexdigest()}
    live.save(work / "http.json", rows)
    return rows


def checks(before: dict, after: dict) -> dict:
    bp, ap = live._html_pages(before), live._html_pages(after)
    if bp.keys() != ap.keys():
        raise ValueError("html_route_set_changed")
    fields = titles.PRESERVED + ("title", "title_tag_count", "meta_description", "meta_description_tag_count",
        "h1_tag_count", "h2_tag_count", "content_sketch", "text_word_count", "internal_link_items", "ld_json_blocks")
    for name in NAMES:
        if bp[ROUTES[name]]["canonical"] != BEFORE[name]:
            raise ValueError("missing_canonical_positive_control")
        expected = URLS["qa-master"] if name in SCOPES.values() else BEFORE[name]
        if ap[ROUTES[name]]["canonical"] != expected:
            raise ValueError("incorrect_canonical_destination")
    for route, old in bp.items():
        for field in fields:
            if field == "canonical" and route in {ROUTES[name] for name in SCOPES.values()}:
                continue
            if old.get(field) != ap[route].get(field):
                raise ValueError("collateral_observed_html_changed")
    for key, name in SCOPES.items():
        items = before["issues"][key].get("evidence", {}).get("items", [])
        witness = {"page": URLS[name], "from": BEFORE[name], "to": URLS["qa-master"]}
        if witness not in items:
            raise ValueError("missing_tracked_positive_control")
        if any(p.get("page") == URLS[name] for p in after["issues"][key].get("evidence", {}).get("items", [])):
            raise ValueError("selected_canonical_defect_remains")
    return {"html_routes_before": len(bp), "html_routes_after": len(ap), "observed_fields_preserved": True,
        "selected_occurrences": {key: [1, 0] for key in SCOPES},
        "target_counts": {key: [before["issues"][key]["count"], after["issues"][key]["count"]] for key in SCOPES},
        "increased_counts": {k: [before["issues"].get(k, {}).get("count", 0), value.get("count", 0)]
            for k, value in after["issues"].items() if value.get("count", 0) > before["issues"].get(k, {}).get("count", 0)}}


def cycle(m, token: str, work: Path, budget: ClaudeBudget) -> dict:
    if budget.limit != 0:
        raise ValueError("Only a zero provider budget is permitted.")
    result = {"status": "unverified", "stage": "identity", "pull_requests": [], "writes": [],
              "repository": f"{live.OWNER}/{REPO}", "universal_certification": False}
    original_put = m._github_api_put
    writable, allowed = "", set()

    def ref(branch):
        return m._github_api_get(m._github_ref_api_path(live.OWNER, REPO, branch), token=token)["object"]["sha"]

    def read(path, branch):
        row = m._github_api_get(m._github_content_api_path(live.OWNER, REPO, path), token=token, params={"ref": branch})
        return row["sha"], base64.b64decode(row["content"]).decode()

    def guarded_put(path, **kwargs):
        known = {m._github_content_api_path(live.OWNER, REPO, p): p for p in allowed}
        if path not in known or not writable or kwargs["json_body"].get("branch") != writable or len(result["writes"]) >= 10:
            raise ValueError("outside_owned_qa_write_scope")
        response = original_put(path, **kwargs)
        result["writes"].append({"branch": writable, "path": known[path]})
        return response

    def put(path, source, sha=""):
        body = {"message": "QA canonical destinations (never merge)", "branch": writable,
                "content": base64.b64encode(source.encode()).decode()}
        if sha:
            body["sha"] = sha
        guarded_put(m._github_content_api_path(live.OWNER, REPO, path), token=token, json_body=body)

    def branch(name, sha):
        if not name.startswith("gauntlet-static-html-targets-") or not m._github_branch_allowed(name):
            raise ValueError("outside_owned_qa_branch_scope")
        m._github_api_post(m._github_api_path("repos", live.OWNER, REPO, "git", "refs"), token=token,
            json_body={"ref": "refs/heads/" + name, "sha": sha})

    def preview(kind, head):
        result["stage"] = kind
        pr = m._ouvrir_pull_request(owner=live.OWNER, repo=REPO, token=token, head=head, base="main", draft=True,
            title="QA canonical destinations " + kind + " (never merge)", body="Owned fixture only. Close without merging.")
        row = {"number": pr["number"], "url": pr["html_url"], "kind": kind}
        result["pull_requests"].append(row)
        live.save(work / "cycle.json", result)
        expected = ref(head)
        row["build"] = live.wait_build(m, REPO, token, pr["number"], expected_sha=expected)
        if row["build"]["head_sha"] != expected or ref(head) != expected:
            raise ValueError("exact_head_changed")
        url = f"https://deploy-preview-{pr['number']}--{REPO}.netlify.app/"
        out = work / kind
        out.mkdir()
        sitemap = titles.preview_sitemap(url)
        (out / "sitemap.xml").write_bytes(sitemap)
        http = observations(url, out)
        if http["old"]["status"] != 301 or http["old"]["location"] != ROUTES["qa-master"]:
            raise ValueError("legitimate_redirect_witness_changed")
        if any(http[name]["status"] != 200 or http[name]["x_robots_tag"] != "noindex" for name in NAMES):
            raise ValueError("qa_pages_or_preview_policy_changed")
        live.crawl("static-html", url, out / "raw", 90, preview=True)
        return projection.rescore(out / "raw/report.json", url, SITE, sitemap, out / "controlled")

    m._github_api_put = guarded_put
    try:
        if ref("main") != MAIN:
            raise ValueError("fixture_main_identity_changed")
        stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d-%H%M%S")
        baseline, corrected = ("gauntlet-static-html-targets-" + mode + "-" + stamp for mode in ("baseline", "correction"))
        branch(baseline, MAIN)
        result.update(main_sha=MAIN, baseline_branch=baseline, correction_branch=corrected)
        tree = m._github_api_get(m._github_api_path("repos", live.OWNER, REPO, "git", "trees", MAIN),
                                token=token, params={"recursive": "1"})
        if tree.get("truncated"):
            raise ValueError("incomplete_fixture_tree")
        paths = [r["path"] for r in tree["tree"] if r["type"] == "blob"]
        if set(FILES.values()) & set(paths):
            raise ValueError("positive_controls_already_exist")
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
            f'<url><loc>{url}</loc></url>\n' for url in URLS.values()) + "</urlset>"), sha)
        sha, source = read("_redirects", baseline)
        put("_redirects", RULE + source, sha)
        before = preview("baseline", baseline)
        branch(corrected, ref(baseline))
        writable, allowed = corrected, CORRECTION
        from backend import repo_index
        index, state = repo_index.build_repo_index(paths), {}
        for key, name in SCOPES.items():
            result["stage"] = key
            prep = m._prepare_issue_fix(issue_key=key, issues=before["issues"], impacted=[URLS[name]], all_paths=paths,
                site_name=urlsplit(SITE).netloc, owner=live.OWNER, repo_name=REPO, branch=baseline, token=token,
                pages=before["pages"])
            if prep["refusal"]:
                raise ValueError("repair_refused_" + key)
            prep["canonical_pairs"] = [p for p in prep["canonical_pairs"] if p["page"] == URLS[name]]
            if prep["canonical_pairs"] != [{"page": URLS[name], "from": BEFORE[name], "to": URLS["qa-master"]}]:
                raise ValueError("missing_tracked_positive_control")
            applied = m._apply_prepared_issue_fix(owner=live.OWNER, repo_name=REPO, branch=baseline, token=token,
                fix_branch=corrected, all_paths=paths, issue_key=key, issue_label=key, impacted=[URLS[name]],
                site_name=urlsplit(SITE).netloc, file_state=state, max_files=1, prep=prep,
                pages=before["pages"], index=index, allow_ai_targeting=False)
            result.setdefault("repairs", {})[key] = applied
            if applied["ai_files"] or applied["skipped"] or applied["config_changes"] or applied["patched"] != [FILES[name]]:
                raise ValueError("unexpected_repair_scope")
        after = preview("final", corrected)
        result["checks"] = checks(before, after)
        for name in NAMES:
            old, new = read(FILES[name], baseline)[1], read(FILES[name], corrected)[1]
            expected = old.replace('rel="canonical" href="' + BEFORE[name],
                'rel="canonical" href="' + URLS["qa-master"]) if name in SCOPES.values() else old
            if new != expected:
                raise ValueError("collateral_source_changed")
        if read("_redirects", baseline)[1] != read("_redirects", corrected)[1]:
            raise ValueError("legitimate_redirect_rules_changed")
        result.update(status="measured_two_verified_canonical_destinations", final_sha=ref(corrected))
    except Exception as exc:
        result.update(status="failed", error_type=type(exc).__name__)
        if isinstance(exc, ValueError) and str(exc).replace("_", "").isalnum():
            result["failure_code"] = str(exc)
        print(result["status"], result["stage"], result.get("failure_code", result["error_type"]), flush=True)
    finally:
        m._github_api_put = original_put
        result["ai_budget"] = budget.summary()
        result["cleanup"] = live.close_prs(REPO, token, result["pull_requests"])
        numbers = [p["number"] for p in result["pull_requests"]]
        result["prs_closed_unmerged"] = bool(numbers) and [r.get("number") for r in result["cleanup"]] == numbers and all(
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
    return int(result["status"] != "measured_two_verified_canonical_destinations")


if __name__ == "__main__":
    raise SystemExit(main())
