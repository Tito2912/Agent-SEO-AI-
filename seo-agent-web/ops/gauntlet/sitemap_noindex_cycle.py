"""Delist three literal noindex witnesses on the owned fixture without changing their instructions."""

from __future__ import annotations

import argparse
import base64
import datetime as dt
from pathlib import Path
import sys
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ops.gauntlet import sitemap_redirect_cycle as previous  # noqa: E402
from ops.gauntlet.ai_budget import ClaudeBudget  # noqa: E402
from ops.gauntlet.https_canonical_cycle import complete  # noqa: E402
from ops.gauntlet.canonical_completion_cycle import closed_cleanup  # noqa: E402

live, projection, titles = previous.live, previous.projection, previous.titles
REPO, MAIN, SITE = previous.REPO, previous.MAIN, previous.SITE
SEED_BRANCH = "gauntlet-static-html-sitemap-redirect-correction-20261005-054346"
SEED, SEED_PR = "751fafff93a5def142562f1a3d1bed6ba8930f85", 66
PREFIX, KEY, FILE = "gauntlet-static-html-sitemap-noindex-", "sitemap_noindex_page", "sitemap.xml"
DIRECT = {SITE + "gauntlet/noindex-long", SITE + "gauntlet/noindex-no-description"}
ALIAS = SITE + "gauntlet/qa-sitemap-noindex"
TARGETS = DIRECT | {ALIAS}
locs = previous.previous.locs


def expected_source(source: str) -> str:
    values = locs(source)
    if len(values) != 51 or any(values.count(url) != 1 for url in TARGETS):
        raise ValueError("missing_unique_noindex_entries")
    output = source
    for url in TARGETS:
        block = previous.alias_entry(url) if url == ALIAS else "<url><loc>" + url + "</loc></url>"
        if source.count(block) != 1:
            raise ValueError("unexpected_owned_entry_bytes")
        output = output.replace(block, "")
    if locs(output) != [url for url in values if url not in TARGETS]:
        raise ValueError("collateral_sitemap_change")
    return output


def write_allowed(path, body, branch, attempts, expected, sha):
    if (path != FILE or not branch.startswith(PREFIX + "correction-") or body.get("branch") != branch
            or attempts != 0 or not sha or body.get("sha") != sha):
        return False
    try:
        return base64.b64decode(body.get("content", ""), validate=True).decode("utf-8") == expected
    except (ValueError, TypeError, UnicodeError):
        return False


def checks(before: dict, after: dict) -> dict:
    complete(before)
    complete(after)
    bp, ap = live._html_pages(before), live._html_pages(after)
    if bp.keys() != ap.keys() or len(bp) != 52:
        raise ValueError("html_routes_lost")
    for report, expected, redirects in ((before, TARGETS, set(previous.NEGATIVES)),
                                       (after, set(), set(previous.NEGATIVES) - {ALIAS})):
        block = report["issues"][KEY]
        if block.get("count") != len(expected) or set(block.get("examples", [])) != expected:
            raise ValueError("noindex_sitemap_family_not_measured")
        if report["issues"][previous.KEY]["count"] != len(redirects) or set(report["issues"][previous.KEY]["examples"]) != redirects:
            raise ValueError("redirect_controls_changed")
        for bench in (previous.previous, previous.previous.previous):
            if report["issues"][bench.KEY]["count"] != 2 or set(report["issues"][bench.KEY]["examples"]) != set(bench.NEGATIVES):
                raise ValueError("previous_negative_controls_changed")
        requested = {row["url"]: row for row in report["pages"]}
        if not TARGETS <= requested.keys():
            raise ValueError("noindex_observations_lost_after_delisting")
        for url in TARGETS:
            row = requested[url]
            if (row.get("status_code") != 200 or row.get("meta_robots") != "noindex, follow"
                    or row.get("meta_robots_tag_count") != 1):
                raise ValueError("literal_noindex_instruction_lost")
    fields = titles.PRESERVED + ("title", "title_tag_count", "meta_description", "meta_description_tag_count",
        "h1_tag_count", "h2_tag_count", "content_sketch", "text_word_count", "internal_link_items", "ld_json_blocks",
        "meta_robots_tag_count", "status_code", "content_type", "error", "blocked_by_host")
    if any(old.get(field) != ap[route].get(field) for route, old in bp.items() for field in fields):
        raise ValueError("collateral_html_fields_changed")
    requested_before = {row["url"]: row for row in before["pages"]}
    requested_after = {row["url"]: row for row in after["pages"]}
    redirect_fields = ("final_url", "status_code", "content_type", "error", "blocked_by_host", "canonical",
                       "redirect_chain", "redirect_statuses", "meta_robots", "meta_robots_tag_count")
    if any(requested_before[url].get(field) != requested_after[url].get(field) for url in previous.ALIASES for field in redirect_fields):
        raise ValueError("redirect_or_robots_observations_changed")
    alias = requested_after[ALIAS]
    if (alias.get("final_url") != SITE + "gauntlet/noindex-long" or alias.get("redirect_chain") != [ALIAS]
            or alias.get("redirect_statuses") != [302]):
        raise ValueError("noindex_redirect_witness_changed")
    return {"html_routes": [52, 52], "selected_occurrences": [3, 0], "target_counts": {KEY: [3, 0], previous.KEY: [2, 1]},
        "noindex_instructions_preserved": True, "noindex_pages_still_observed": True,
        "observed_html_fields_preserved": True, "redirect_observations_preserved": True,
        "increased_counts": {key: [before["issues"].get(key, {}).get("count", 0), value.get("count", 0)]
            for key, value in after["issues"].items() if value.get("count", 0) > before["issues"].get(key, {}).get("count", 0)}}


def cycle(m, token: str, work: Path, budget: ClaudeBudget) -> dict:
    if budget.limit != 0:
        raise ValueError("nonzero_provider_budget")
    result = {"status": "unverified", "stage": "identity", "repository": f"{live.OWNER}/{REPO}", "seed_sha": SEED,
              "pull_requests": [], "writes": [], "write_attempts": 0, "universal_certification": False}
    original_put, corrected, expected, blob_sha = m._github_api_put, "", "", ""

    def get(*parts, **kwargs):
        return m._github_api_get(m._github_api_path("repos", live.OWNER, REPO, *parts), token=token, **kwargs)

    def ref(branch):
        return get("git", "ref", "heads", branch)["object"]["sha"]

    def read(path, sha):
        row = get("contents", path, params={"ref": sha})
        return row["sha"], base64.b64decode(row["content"]).decode("utf-8")

    def guarded_put(api, **kwargs):
        if api != m._github_content_api_path(live.OWNER, REPO, FILE) or not write_allowed(
                FILE, kwargs.get("json_body", {}), corrected, result["write_attempts"], expected, blob_sha):
            raise ValueError("outside_exact_single_sitemap_write_scope")
        result["write_attempts"] += 1
        live.save(work / "cycle.json", result)
        response = original_put(api, **kwargs)
        result["writes"].append({"branch": corrected, "path": FILE})
        return response

    def branch(name):
        if not name.startswith(PREFIX) or not m._github_branch_allowed(name):
            raise ValueError("outside_owned_qa_branch")
        m._github_api_post(m._github_api_path("repos", live.OWNER, REPO, "git", "refs"), token=token,
                          json_body={"ref": "refs/heads/" + name, "sha": SEED})

    def preview(kind, head, sha):
        result["stage"] = kind
        print("STAGE=" + kind, flush=True)
        pr = m._ouvrir_pull_request(owner=live.OWNER, repo=REPO, token=token, head=head, base="main", draft=True,
            title="QA sitemap noindex " + kind + " (never merge)", body="Owned fixture only. Keep noindex; close without merging.")
        row = {"number": pr["number"], "url": pr["html_url"], "kind": kind}
        result["pull_requests"].append(row)
        live.save(work / "cycle.json", result)
        row["build"] = live.wait_build(m, REPO, token, pr["number"], expected_sha=sha)
        if ref(head) != sha or row["build"]["head_sha"] != sha:
            raise ValueError("exact_head_changed")
        url = f"https://deploy-preview-{pr['number']}--{REPO}.netlify.app/"
        out = work / kind
        out.mkdir()
        sitemap = titles.preview_sitemap(url)
        (out / FILE).write_bytes(sitemap)
        observations = {}
        for source in sorted(TARGETS):
            response = live.requests.get(url.rstrip("/") + urlsplit(source).path, timeout=30, allow_redirects=False)
            if source == ALIAS:
                if response.status_code != 302 or response.headers.get("location") != "/gauntlet/noindex-long":
                    raise ValueError("native_noindex_redirect_changed")
            elif response.status_code != 200 or response.headers.get("x-robots-tag") != "noindex":
                raise ValueError("html_or_native_preview_policy_changed")
            name = urlsplit(source).path.rsplit("/", 1)[-1]
            (out / (name + ".http")).write_bytes(response.content)
            observations[source] = {"status": response.status_code, "location": response.headers.get("location", ""),
                                   "x_robots_tag": response.headers.get("x-robots-tag", "")}
        live.save(out / "http.json", observations)
        live.crawl("static-html", url, out / "raw", 90, preview=True)
        return projection.rescore(out / "raw/report.json", url, SITE, sitemap, out / "controlled")

    m._github_api_put = guarded_put
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
        blob_sha, source = read(FILE, SEED)
        expected = expected_source(source)
        stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d-%H%M%S")
        baseline, corrected = (PREFIX + kind + "-" + stamp for kind in ("baseline", "correction"))
        result.update(main_sha=MAIN, baseline_branch=baseline, correction_branch=corrected, baseline_sha=SEED, sitemap_entries=[51, 48])
        branch(baseline)
        before = preview("baseline", baseline, SEED)
        prep = m._prepare_issue_fix(issue_key=KEY, issues=before["issues"], impacted=before["issues"][KEY]["examples"],
            all_paths=paths, site_name=urlsplit(SITE).netloc, owner=live.OWNER, repo_name=REPO,
            branch=baseline, token=token, pages=before["pages"])
        if prep["refusal"] or prep["rewriter_ai_fallback"] or set(prep.get("sitemap_noindex_urls", [])) != TARGETS:
            raise ValueError("literal_noindex_not_verified")
        branch(corrected)
        from backend import repo_index
        repair = m._apply_prepared_issue_fix(owner=live.OWNER, repo_name=REPO, branch=baseline, token=token,
            fix_branch=corrected, all_paths=paths, issue_key=KEY, issue_label=KEY, impacted=before["issues"][KEY]["examples"],
            site_name=urlsplit(SITE).netloc, file_state={}, max_files=1, prep=prep, pages=before["pages"],
            index=repo_index.build_repo_index(paths), allow_ai_targeting=False)
        result["repair"] = repair
        if repair["patched"] != [FILE] or repair["skipped"] or repair["ai_files"] or repair["config_changes"]:
            raise ValueError("unexpected_correction_scope")
        final_sha = ref(corrected)
        comparison = get("compare", SEED + "..." + final_sha)
        if comparison["total_commits"] != 1 or [f["filename"] for f in comparison["files"]] != [FILE]:
            raise ValueError("collateral_source_changed")
        if read(FILE, final_sha)[1] != expected or any(read(path, SEED)[1] != read(path, final_sha)[1] for path in previous.SETUP - {FILE}):
            raise ValueError("exact_source_check_failed")
        after = preview("final", corrected, final_sha)
        result.update(status="measured_verified_sitemap_noindex_resolved", final_sha=final_sha, checks=checks(before, after))
    except Exception as exc:
        result.update(status="failed", error_type=type(exc).__name__)
        if isinstance(exc, ValueError) and str(exc).replace("_", "").isalnum():
            result["failure_code"] = str(exc)
        print(result["status"], result["stage"], result.get("failure_code", result["error_type"]), flush=True)
    finally:
        m._github_api_put = original_put
        result["ai_budget"] = budget.summary()
        result["cleanup"] = live.close_prs(REPO, token, result["pull_requests"])
        result["prs_closed_unmerged"] = closed_cleanup(result["pull_requests"], result["cleanup"])
        result["main_unchanged"] = ref("main") == MAIN
        if not result["prs_closed_unmerged"] or not result["main_unchanged"] or budget.summary()["attempted"] or budget.summary()["denied"]:
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
    return int(result["status"] != "measured_verified_sitemap_noindex_resolved")


if __name__ == "__main__":
    raise SystemExit(main())
