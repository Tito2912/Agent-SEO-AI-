"""Measure exact removal of two still-missing entries on an owned rebuilt fixture."""

from __future__ import annotations

import argparse
import base64
import datetime as dt
from pathlib import Path
import sys
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ops.gauntlet import sitemap_noindex_cycle as previous  # noqa: E402
from ops.gauntlet.ai_budget import ClaudeBudget  # noqa: E402
from ops.gauntlet.https_canonical_cycle import complete  # noqa: E402
from ops.gauntlet.canonical_completion_cycle import closed_cleanup  # noqa: E402

live, projection, titles = previous.live, previous.projection, previous.titles
REPO, MAIN, SITE = previous.REPO, previous.MAIN, previous.SITE
SEED_BRANCH = "gauntlet-static-html-sitemap-noindex-correction-20261005-061724"
SEED, SEED_PR = "86837f38ec7298bfff5a66659dcb765a46a9cc24", 68
PREFIX, KEY = "gauntlet-static-html-sitemap-error-", "sitemap_4xx_page"
TARGETS = {SITE + "gauntlet/qa-sitemap-missing-a", SITE + "gauntlet/qa-sitemap-missing-B"}
SETUP = {"index.html", "sitemap.xml"}
locs = previous.locs


def entry(url):
    if url not in TARGETS:
        raise ValueError("outside_owned_missing_url_scope")
    return '<url><loc>' + url + '</loc><lastmod>2001-01-01</lastmod><priority>0.1</priority></url>'


def setup_sources(original):
    if (set(original) != SETUP or len(locs(original["sitemap.xml"])) != 48
            or original["index.html"].count("</body>") != 1 or original["sitemap.xml"].count("</urlset>") != 1
            or any("qa-sitemap-missing-" in raw for raw in original.values())):
        raise ValueError("unexpected_pinned_setup_shape")
    links = ''.join('<p><a href="' + urlsplit(url).path + '">QA missing sitemap ' + urlsplit(url).path.rsplit('/', 1)[-1]
                    + '</a></p>\n' for url in sorted(TARGETS))
    return {"index.html": original["index.html"].replace('</body>', links + '</body>'),
            "sitemap.xml": original["sitemap.xml"].replace('</urlset>', '\n'.join(entry(url) for url in sorted(TARGETS)) + '\n</urlset>')}


def expected_source(source):
    values = locs(source)
    if len(values) != 50 or any(values.count(url) != 1 for url in TARGETS):
        raise ValueError("missing_unique_error_entries")
    output = source
    for url in TARGETS:
        block = entry(url)
        if source.count(block) != 1:
            raise ValueError("unexpected_owned_entry_bytes")
        output = output.replace(block, "")
    if locs(output) != [url for url in values if url not in TARGETS]:
        raise ValueError("collateral_sitemap_change")
    return output


def write_allowed(path, body, branch, attempts, expected, sha):
    if (not branch.startswith(PREFIX) or body.get("branch") != branch or attempts != 0
            or path not in expected or not sha or body.get("sha") != sha):
        return False
    try:
        return base64.b64decode(body.get("content", ""), validate=True).decode("utf-8") == expected[path]
    except (ValueError, TypeError, UnicodeError):
        return False


def checks(before, after):
    complete(before)
    complete(after)
    bp, ap = live._html_pages(before), live._html_pages(after)
    if bp.keys() != ap.keys() or len(bp) != 52:
        raise ValueError("html_routes_lost")
    for report, expected in ((before, TARGETS), (after, set())):
        block = report["issues"][KEY]
        if block.get("count") != len(expected) or set(block.get("examples", [])) != expected:
            raise ValueError("sitemap_error_family_not_measured")
        rows = {row["url"]: row for row in report["pages"]}
        if not TARGETS <= rows.keys():
            raise ValueError("missing_witnesses_lost_after_delisting")
        for url in TARGETS:
            row = rows[url]
            if (type(row.get("status_code")) is not int or row["status_code"] != 404 or row.get("error")
                    or row.get("blocked_by_host") or row.get("final_url") != url
                    or row.get("redirect_chain") or row.get("redirect_statuses")):
                raise ValueError("direct_missing_witness_changed")
        if report["issues"][previous.KEY]["count"] != 0 or report["issues"][previous.KEY]["examples"]:
            raise ValueError("previous_noindex_repair_lost")
        redirect = previous.previous
        remaining = set(redirect.NEGATIVES) - {previous.ALIAS}
        if report["issues"][redirect.KEY]["count"] != 1 or set(report["issues"][redirect.KEY]["examples"]) != remaining:
            raise ValueError("previous_redirect_control_changed")
        for bench in (redirect.previous, redirect.previous.previous):
            if report["issues"][bench.KEY]["count"] != 2 or set(report["issues"][bench.KEY]["examples"]) != set(bench.NEGATIVES):
                raise ValueError("previous_negative_controls_changed")
    fields = titles.PRESERVED + ("title", "title_tag_count", "meta_description", "meta_description_tag_count",
        "h1_tag_count", "h2_tag_count", "content_sketch", "text_word_count", "internal_link_items", "ld_json_blocks",
        "meta_robots_tag_count", "status_code", "content_type", "error", "blocked_by_host")
    if any(old.get(field) != ap[route].get(field) for route, old in bp.items() for field in fields):
        raise ValueError("collateral_html_fields_changed")
    for url in previous.TARGETS:
        row = next((row for row in after["pages"] if row["url"] == url), {})
        if row.get("meta_robots") != "noindex, follow" or row.get("meta_robots_tag_count") != 1:
            raise ValueError("previous_noindex_instruction_lost")
    return {"html_routes": [52, 52], "selected_occurrences": [2, 0], "target_counts": {KEY: [2, 0]},
        "missing_pages_still_observed": True, "observed_html_fields_preserved": True,
        "previous_repairs_and_negative_controls_preserved": True,
        "increased_counts": {key: [before["issues"].get(key, {}).get("count", 0), value.get("count", 0)]
            for key, value in after["issues"].items() if value.get("count", 0) > before["issues"].get(key, {}).get("count", 0)}}


def cycle(m, token, work, budget):
    if budget.limit != 0:
        raise ValueError("nonzero_provider_budget")
    result = {"status": "unverified", "stage": "identity", "repository": f"{live.OWNER}/{REPO}", "seed_sha": SEED,
              "pull_requests": [], "writes": [], "write_attempts": [], "http_rechecks": [], "universal_certification": False}
    original_put, original_status = m._github_api_put, m._sitemap_error_status
    writable, expected, shas, baseline_preview = "", {}, {}, ""

    def get(*parts, **kwargs):
        return m._github_api_get(m._github_api_path("repos", live.OWNER, REPO, *parts), token=token, **kwargs)

    def ref(branch):
        return get("git", "ref", "heads", branch)["object"]["sha"]

    def read(path, sha):
        row = get("contents", path, params={"ref": sha})
        return row["sha"], base64.b64decode(row["content"]).decode("utf-8")

    def guarded_put(api, **kwargs):
        allowed = {m._github_content_api_path(live.OWNER, REPO, path): path for path in expected}
        path = allowed.get(api, "")
        attempts = result["write_attempts"].count({"branch": writable, "path": path})
        if len(result["write_attempts"]) >= 3 or not write_allowed(path, kwargs.get("json_body", {}), writable, attempts, expected, shas.get(path)):
            raise ValueError("outside_exact_qa_write_scope")
        row = {"branch": writable, "path": path}
        result["write_attempts"].append(row)
        live.save(work / "cycle.json", result)
        response = original_put(api, **kwargs)
        result["writes"].append(row)
        return response

    def current_status(url):
        if url not in TARGETS or not baseline_preview:
            raise ValueError("outside_exact_qa_http_scope")
        observed_url = baseline_preview.rstrip('/') + urlsplit(url).path
        code = original_status(observed_url)
        result["http_rechecks"].append({"source": url, "observed_preview_url": observed_url, "status": code})
        return code

    def branch(name, sha):
        if not name.startswith(PREFIX) or not m._github_branch_allowed(name):
            raise ValueError("outside_owned_qa_branch")
        m._github_api_post(m._github_api_path("repos", live.OWNER, REPO, "git", "refs"), token=token,
                          json_body={"ref": "refs/heads/" + name, "sha": sha})

    def preview(kind, head, sha):
        nonlocal baseline_preview
        result["stage"] = kind
        print("STAGE=" + kind, flush=True)
        pr = m._ouvrir_pull_request(owner=live.OWNER, repo=REPO, token=token, head=head, base="main", draft=True,
            title="QA sitemap missing entries " + kind + " (never merge)", body="Owned fixture only. Do not create pages. Close without merging.")
        row = {"number": pr["number"], "url": pr["html_url"], "kind": kind}
        result["pull_requests"].append(row)
        live.save(work / "cycle.json", result)
        row["build"] = live.wait_build(m, REPO, token, pr["number"], expected_sha=sha)
        if ref(head) != sha or row["build"]["head_sha"] != sha:
            raise ValueError("exact_head_changed")
        url = f"https://deploy-preview-{pr['number']}--{REPO}.netlify.app/"
        if kind == "baseline":
            baseline_preview = url
        out = work / kind
        out.mkdir()
        sitemap = titles.preview_sitemap(url)
        (out / "sitemap.xml").write_bytes(sitemap)
        observations = {}
        for source in sorted(TARGETS | {SITE + "gauntlet/qa-master"}):
            response = live.requests.get(url.rstrip('/') + urlsplit(source).path, timeout=30, allow_redirects=False)
            name = urlsplit(source).path.rsplit('/', 1)[-1]
            (out / (name + ".http")).write_bytes(response.content)
            observations[source] = {"status": response.status_code, "location": response.headers.get("location", ""),
                "content_type": response.headers.get("content-type", ""), "x_robots_tag": response.headers.get("x-robots-tag", "")}
            if source in TARGETS:
                if response.status_code != 404 or response.headers.get("location"):
                    raise ValueError("missing_direct_404_control")
            elif response.status_code != 200 or response.headers.get("x-robots-tag") != "noindex":
                raise ValueError("healthy_master_or_preview_policy_changed")
        live.save(out / "http.json", observations)
        live.crawl("static-html", url, out / "raw", 90, preview=True)
        return projection.rescore(out / "raw/report.json", url, SITE, sitemap, out / "controlled")

    m._github_api_put, m._sitemap_error_status = guarded_put, current_status
    try:
        if ref("main") != MAIN or ref(SEED_BRANCH) != SEED:
            raise ValueError("fixture_identity_changed")
        parent = get("pulls", str(SEED_PR))
        if parent["state"] != "closed" or parent["merged"] or not parent["draft"] or parent["head"]["sha"] != SEED:
            raise ValueError("unverified_seed_pr")
        live.wait_build(m, REPO, token, SEED_PR, expected_sha=SEED)
        if get("compare", MAIN + "..." + SEED)["merge_base_commit"]["sha"] != MAIN:
            raise ValueError("unverified_seed_ancestry")
        tree = get("git", "trees", SEED, params={"recursive": "1"})
        if tree.get("truncated"):
            raise ValueError("incomplete_fixture_tree")
        paths = [p["path"] for p in tree["tree"] if p["type"] == "blob"]
        from backend import repo_index
        index = repo_index.build_repo_index(paths)
        if any(repo_index.route_files(index, url) for url in TARGETS):
            raise ValueError("missing_fixture_url_has_source")
        pinned = {path: read(path, SEED) for path in SETUP}
        setup = setup_sources({path: row[1] for path, row in pinned.items()})
        stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d-%H%M%S")
        baseline, correction = (PREFIX + kind + "-" + stamp for kind in ("baseline", "correction"))
        result.update(main_sha=MAIN, baseline_branch=baseline, correction_branch=correction)
        branch(baseline, SEED)
        writable, expected, shas = baseline, setup, {path: row[0] for path, row in pinned.items()}
        for path in sorted(SETUP):
            guarded_put(m._github_content_api_path(live.OWNER, REPO, path), token=token,
                json_body={"branch": baseline, "sha": shas[path], "message": "QA missing sitemap witnesses (never merge)",
                           "content": base64.b64encode(expected[path].encode()).decode()})
        baseline_sha = ref(baseline)
        result["baseline_sha"] = baseline_sha
        before = preview("baseline", baseline, baseline_sha)
        prep = m._prepare_issue_fix(issue_key=KEY, issues=before["issues"], impacted=before["issues"][KEY]["examples"],
            all_paths=paths, site_name=urlsplit(SITE).netloc, owner=live.OWNER, repo_name=REPO,
            branch=baseline, token=token, pages=before["pages"])
        if prep["refusal"] or prep["rewriter_ai_fallback"] or prep.get("sitemap_error_statuses") != {url: 404 for url in TARGETS}:
            raise ValueError("direct_missing_pages_not_verified")
        branch(correction, baseline_sha)
        blob, source = read("sitemap.xml", baseline_sha)
        writable, expected, shas = correction, {"sitemap.xml": expected_source(source)}, {"sitemap.xml": blob}
        repair = m._apply_prepared_issue_fix(owner=live.OWNER, repo_name=REPO, branch=baseline, token=token,
            fix_branch=correction, all_paths=paths, issue_key=KEY, issue_label=KEY, impacted=before["issues"][KEY]["examples"],
            site_name=urlsplit(SITE).netloc, file_state={}, max_files=1, prep=prep, pages=before["pages"], index=index, allow_ai_targeting=False)
        result["repair"] = repair
        if repair["patched"] != ["sitemap.xml"] or repair["skipped"] or repair["ai_files"] or repair["config_changes"]:
            raise ValueError("unexpected_correction_scope")
        if len(result["http_rechecks"]) != 2 or {row["source"] for row in result["http_rechecks"]} != TARGETS or any(row["status"] != 404 for row in result["http_rechecks"]):
            raise ValueError("missing_current_http_proof")
        final_sha = ref(correction)
        comparison = get("compare", baseline_sha + "..." + final_sha)
        if comparison["total_commits"] != 1 or [f["filename"] for f in comparison["files"]] != ["sitemap.xml"]:
            raise ValueError("collateral_source_changed")
        if read("sitemap.xml", final_sha)[1] != expected["sitemap.xml"] or read("index.html", final_sha)[1] != setup["index.html"] or read("_redirects", final_sha)[1] != read("_redirects", SEED)[1]:
            raise ValueError("exact_source_check_failed")
        after = preview("final", correction, final_sha)
        result.update(status="measured_verified_sitemap_errors_resolved", final_sha=final_sha, checks=checks(before, after), sitemap_entries=[50, 48])
    except Exception as exc:
        result.update(status="failed", error_type=type(exc).__name__)
        if isinstance(exc, ValueError) and str(exc).replace("_", "").isalnum():
            result["failure_code"] = str(exc)
        print(result["status"], result["stage"], result.get("failure_code", result["error_type"]), flush=True)
    finally:
        m._github_api_put, m._sitemap_error_status = original_put, original_status
        result["ai_budget"] = budget.summary()
        result["cleanup"] = live.close_prs(REPO, token, result["pull_requests"])
        result["prs_closed_unmerged"] = closed_cleanup(result["pull_requests"], result["cleanup"])
        result["main_unchanged"] = ref("main") == MAIN
        if not result["prs_closed_unmerged"] or not result["main_unchanged"] or budget.summary()["attempted"] or budget.summary()["denied"]:
            result["status"] = "unverified"
        live.save(work / "cycle.json", result)
    return result


def main():
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
    return int(result["status"] != "measured_verified_sitemap_errors_resolved")


if __name__ == "__main__":
    raise SystemExit(main())
