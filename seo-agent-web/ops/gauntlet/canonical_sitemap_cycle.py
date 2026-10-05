"""Clean only verified canonical aliases from the pinned owned fixture's sitemap."""

from __future__ import annotations

import argparse
import base64
import datetime as dt
from pathlib import Path
import sys
from urllib.parse import urlsplit

from defusedxml import ElementTree as XML

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ops.gauntlet import duplicate_canonical_cycle as previous  # noqa: E402
from ops.gauntlet.ai_budget import ClaudeBudget  # noqa: E402

live, projection, titles = previous.live, previous.projection, previous.titles
REPO, MAIN, SITE = previous.REPO, previous.MAIN, previous.SITE
SEED_BRANCH = "gauntlet-static-html-duplicate-canonical-correction-20261005-043035"
SEED, SEED_PR = "c31d4b1370735f7938b96bc45e841d81cb0b7fc8", 62
PREFIX = "gauntlet-static-html-canonical-sitemap-"
KEY, FILE = "sitemap_non_canonical_page", "sitemap.xml"
TARGETS = {SITE + "gauntlet/" + source: SITE + "gauntlet/" + target for source, target in (
    ("canonical-relay", "missing-h1"), ("canonical-other", "missing-h1"),
    ("qa-hop-source", "qa-master"), ("qa-relay", "qa-master"),
    ("qa-chain-source", "qa-master"), ("qa-dupe-b", "qa-dupe-a"))}
PAIRS = [{"page": source, "from": source, "to": target} for source, target in sorted(TARGETS.items())]
NEGATIVES = {SITE + "blog", SITE + "gauntlet/canonical-404"}


def locs(source: str) -> list[str]:
    root = XML.fromstring(source, forbid_dtd=True)
    if root.tag != "{http://www.sitemaps.org/schemas/sitemap/0.9}urlset":
        raise ValueError("unexpected_sitemap_root")
    return [(node.text or "").strip() for node in root.findall("./{*}url/{*}loc")]


def expected_source(source: str) -> str:
    values = locs(source)
    if any(values.count(url) != 1 for url in {*TARGETS, *TARGETS.values(), *NEGATIVES}):
        raise ValueError("missing_unique_alias_master_or_negative_entry")
    output = source
    for url in TARGETS:
        block = "<url><loc>" + url + "</loc></url>"
        if source.count(block) != 1:
            raise ValueError("unexpected_owned_alias_format")
        output = output.replace(block, "")
    if locs(output) != [value for value in values if value not in TARGETS]:
        raise ValueError("collateral_sitemap_changed")
    return output


def write_allowed(path: str, body: dict, branch: str, attempts: int, expected: str, sha: str) -> bool:
    if (path != FILE or not branch.startswith(PREFIX + "correction-") or body.get("branch") != branch
            or attempts != 0 or not sha or body.get("sha") != sha):
        return False
    try:
        return base64.b64decode(body.get("content", ""), validate=True).decode("utf-8") == expected
    except (ValueError, TypeError, UnicodeError):
        return False


def checks(before: dict, after: dict) -> dict:
    previous.previous.complete(before)
    previous.previous.complete(after)
    bp, ap = live._html_pages(before), live._html_pages(after)
    if bp.keys() != ap.keys() or len(bp) != 52:
        raise ValueError("html_route_set_changed")
    for report, expected in ((before, set(TARGETS) | NEGATIVES), (after, NEGATIVES)):
        block = report.get("issues", {}).get(KEY, {})
        if block.get("count") != len(expected) or set(block.get("examples", [])) != expected:
            raise ValueError("missing_selected_or_refused_witnesses")
        if report["issues"][previous.KEY]["count"] != 2 or set(report["issues"][previous.KEY]["examples"]) != set(previous.NEGATIVES):
            raise ValueError("previous_duplicate_controls_lost")
    fields = titles.PRESERVED + ("title", "title_tag_count", "meta_description", "meta_description_tag_count",
        "h1_tag_count", "h2_tag_count", "content_sketch", "text_word_count", "internal_link_items", "ld_json_blocks",
        "status_code", "content_type", "error", "blocked_by_host")
    for route, row in bp.items():
        if any(row.get(field) != ap[route].get(field) for field in fields):
            raise ValueError("collateral_observed_html_changed")
    for name in previous.NAMES[:2]:
        if ap[previous.ROUTES[name]].get("canonical") != previous.URLS[previous.NAMES[0]]:
            raise ValueError("previous_canonical_master_lost")
    return {"html_routes_before": len(bp), "html_routes_after": len(ap), "observed_html_fields_preserved": True,
        "selected_occurrences": [6, 0], "target_counts": {KEY: [8, 2]}, "refused_entries_remaining": sorted(NEGATIVES),
        "increased_counts": {k: [before["issues"].get(k, {}).get("count", 0), value.get("count", 0)]
            for k, value in after["issues"].items() if value.get("count", 0) > before["issues"].get(k, {}).get("count", 0)}}


def cycle(m, token: str, work: Path, budget: ClaudeBudget) -> dict:
    if budget.limit != 0:
        raise ValueError("nonzero_provider_budget")
    result = {"status": "unverified", "stage": "identity", "repository": f"{live.OWNER}/{REPO}",
        "seed_sha": SEED, "pull_requests": [], "writes": [], "write_attempts": 0, "universal_certification": False}
    original_put = m._github_api_put
    corrected, expected, blob_sha = "", "", ""

    def get(*parts, **kwargs):
        return m._github_api_get(m._github_api_path("repos", live.OWNER, REPO, *parts), token=token, **kwargs)

    def ref(branch):
        return get("git", "ref", "heads", branch)["object"]["sha"]

    def source(path, sha):
        row = get("contents", path, params={"ref": sha})
        return row["sha"], base64.b64decode(row["content"]).decode("utf-8")

    def guarded_put(path, **kwargs):
        if path != m._github_content_api_path(live.OWNER, REPO, FILE) or not write_allowed(
                FILE, kwargs.get("json_body", {}), corrected, result["write_attempts"], expected, blob_sha):
            raise ValueError("outside_single_sitemap_write_scope")
        result["write_attempts"] += 1
        live.save(work / "cycle.json", result)
        response = original_put(path, **kwargs)
        result["writes"].append({"branch": corrected, "path": FILE})
        return response

    def branch(name):
        if not name.startswith(PREFIX) or not m._github_branch_allowed(name):
            raise ValueError("outside_owned_qa_branch_scope")
        m._github_api_post(m._github_api_path("repos", live.OWNER, REPO, "git", "refs"), token=token,
                          json_body={"ref": "refs/heads/" + name, "sha": SEED})

    def preview(kind, head, sha):
        result["stage"] = kind
        print("STAGE=" + kind, flush=True)
        pr = m._ouvrir_pull_request(owner=live.OWNER, repo=REPO, token=token, head=head, base="main", draft=True,
            title="QA canonical sitemap " + kind + " (never merge)", body="Owned fixture only. Close without merging.")
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
        (out / "sitemap.xml").write_bytes(sitemap)
        previous.previous.previous.observations(url, out)
        for name, route in previous.ROUTES.items():
            response = live.requests.get(url.rstrip("/") + route, timeout=30, allow_redirects=False)
            (out / (name + ".http")).write_bytes(response.content)
            if response.status_code != 200 or response.headers.get("x-robots-tag") != "noindex":
                raise ValueError("html_or_native_preview_policy_changed")
        live.crawl("static-html", url, out / "raw", 90, preview=True)
        return projection.rescore(out / "raw/report.json", url, SITE, sitemap, out / "controlled")

    m._github_api_put = guarded_put
    try:
        if ref("main") != MAIN or ref(SEED_BRANCH) != SEED:
            raise ValueError("fixture_identity_changed")
        parent = get("pulls", str(SEED_PR))
        if parent["state"] != "closed" or parent["merged"] or not parent["draft"] or parent["head"]["sha"] != SEED:
            raise ValueError("unverified_parent_cycle")
        live.wait_build(m, REPO, token, SEED_PR, expected_sha=SEED)
        if get("compare", MAIN + "..." + SEED)["merge_base_commit"]["sha"] != MAIN:
            raise ValueError("unverified_fixture_ancestry")
        tree = get("git", "trees", SEED, params={"recursive": "1"})
        if tree.get("truncated"):
            raise ValueError("incomplete_fixture_tree")
        paths = [p["path"] for p in tree["tree"] if p["type"] == "blob"]
        blob_sha, original = source(FILE, SEED)
        expected = expected_source(original)
        stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d-%H%M%S")
        baseline, corrected = (PREFIX + kind + "-" + stamp for kind in ("baseline", "correction"))
        result.update(main_sha=MAIN, baseline_branch=baseline, correction_branch=corrected, baseline_sha=SEED,
                      sitemap_entries_before=len(locs(original)), sitemap_entries_after=len(locs(expected)))
        branch(baseline)
        before = preview("baseline", baseline, SEED)
        prep = m._prepare_issue_fix(issue_key=KEY, issues=before["issues"], impacted=before["issues"][KEY]["examples"],
            all_paths=paths, site_name=urlsplit(SITE).netloc, owner=live.OWNER, repo_name=REPO,
            branch=baseline, token=token, pages=before["pages"])
        if prep["refusal"] or prep["rewriter_ai_fallback"] or sorted(prep.get("sitemap_canonical_pairs", []),
                key=lambda p: p["from"]) != PAIRS or not all(u in prep["side_effects"] for u in NEGATIVES):
            raise ValueError("positive_and_negative_pairs_not_verified")
        branch(corrected)
        from backend import repo_index
        applied = m._apply_prepared_issue_fix(owner=live.OWNER, repo_name=REPO, branch=baseline, token=token,
            fix_branch=corrected, all_paths=paths, issue_key=KEY, issue_label=KEY, impacted=before["issues"][KEY]["examples"],
            site_name=urlsplit(SITE).netloc, file_state={}, max_files=1, prep=prep, pages=before["pages"],
            index=repo_index.build_repo_index(paths), allow_ai_targeting=False)
        result["repair"] = applied
        if applied["patched"] != [FILE] or applied["skipped"] or applied["ai_files"] or applied["config_changes"]:
            raise ValueError("unexpected_repair_scope")
        final_sha = ref(corrected)
        comparison = get("compare", SEED + "..." + final_sha)
        if comparison["total_commits"] != 1 or [p["filename"] for p in comparison["files"]] != [FILE]:
            raise ValueError("collateral_repository_changed")
        if source(FILE, final_sha)[1] != expected or source("_redirects", SEED)[1] != source("_redirects", final_sha)[1]:
            raise ValueError("collateral_source_changed")
        after = preview("final", corrected, final_sha)
        result.update(checks=checks(before, after), final_sha=final_sha, status="measured_verified_sitemap_aliases_resolved")
    except Exception as exc:
        result.update(status="failed", error_type=type(exc).__name__)
        if isinstance(exc, ValueError) and str(exc).replace("_", "").isalnum():
            result["failure_code"] = str(exc)
        print(result["status"], result["stage"], result.get("failure_code", result["error_type"]), flush=True)
    finally:
        m._github_api_put = original_put
        result["ai_budget"] = budget.summary()
        result["cleanup"] = live.close_prs(REPO, token, result["pull_requests"])
        result["prs_closed_unmerged"] = previous.previous.previous.closed_cleanup(result["pull_requests"], result["cleanup"])
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
    return int(result["status"] != "measured_verified_sitemap_aliases_resolved")


if __name__ == "__main__":
    raise SystemExit(main())
