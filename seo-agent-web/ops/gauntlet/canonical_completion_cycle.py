"""Complete the residual canonical on a pinned, already-corrected owned fixture."""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
from pathlib import Path
import sys
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ops.gauntlet import canonical_target_cycle as previous  # noqa: E402
from ops.gauntlet.ai_budget import ClaudeBudget  # noqa: E402

live, projection, titles = previous.live, previous.projection, previous.titles
REPO, MAIN, SITE = previous.REPO, previous.MAIN, previous.SITE
SEED_BRANCH = "gauntlet-static-html-targets-correction-20261004-164955"
SEED = "cd2f28d30005072aef0a16b32b67c7bfdfa41947"
SEED_PR = 56
PREFIX = "gauntlet-static-html-completion-"
KEY = "non_canonical_page_specified_as_canonical_one"
REDIRECT_KEY = "canonical_points_to_redirect"
FILE = "gauntlet/canonical-other.html"
ROUTE = "/gauntlet/canonical-other"
SOURCE = SITE.rstrip("/") + ROUTE
OLD = SITE + "gauntlet/canonical-relay"
DESTINATION = SITE + "gauntlet/missing-h1"
PAIR = {"page": SOURCE, "from": OLD, "to": DESTINATION}
EXTRA = {"canonical-other": ROUTE, "canonical-relay": "/gauntlet/canonical-relay", "missing-h1": "/gauntlet/missing-h1"}


def expected_source(source: str) -> str:
    old = 'rel="canonical" href="' + OLD + '"'
    if source.count(old) != 1:
        raise ValueError("missing_unique_literal_canonical")
    return source.replace(old, 'rel="canonical" href="' + DESTINATION + '"')


def write_allowed(path: str, body: dict, branch: str, attempts: int, expected: str, sha: str) -> bool:
    if (path != FILE or not branch.startswith(PREFIX + "correction-") or body.get("branch") != branch
            or attempts != 0 or not sha or body.get("sha") != sha):
        return False
    try:
        return base64.b64decode(body.get("content", ""), validate=True).decode("utf-8") == expected
    except (ValueError, TypeError, UnicodeError):
        return False


def closed_cleanup(prs: list[dict], cleanup: list[dict]) -> bool:
    numbers = [p["number"] for p in prs]
    return len(numbers) == 2 and len(set(numbers)) == 2 and [p.get("number") for p in cleanup] == numbers and all(
        p.get("state") == "closed" and p.get("merged") is False for p in cleanup)


def baseline_checks(report: dict) -> None:
    meta, issues, pages = report.get("meta", {}), report.get("issues", {}), live._html_pages(report)
    if meta.get("max_pages") != 90 or any(meta.get(k) for k in ("stopped_on_time_budget", "urls_uncrawled")) or (
            meta.get("blocked_by_host") or {}).get("count"):
        raise ValueError("incomplete_or_incomparable_crawl")
    block = issues.get(KEY, {})
    if block.get("count") != 1 or block.get("evidence", {}).get("items") != [PAIR]:
        raise ValueError("missing_single_residual_witness")
    if issues.get(REDIRECT_KEY, {}).get("count") != 0:
        raise ValueError("previous_redirect_correction_lost")
    expected = {**{previous.ROUTES[n]: previous.URLS["qa-master"] if n in previous.SCOPES.values()
                  else previous.BEFORE[n] for n in previous.NAMES},
                ROUTE: OLD, EXTRA["canonical-relay"]: DESTINATION, EXTRA["missing-h1"]: DESTINATION}
    if any(pages.get(route, {}).get("canonical") != canonical for route, canonical in expected.items()):
        raise ValueError("missing_baseline_canonical_controls")


def checks(before: dict, after: dict) -> dict:
    baseline_checks(before)
    bp, ap = live._html_pages(before), live._html_pages(after)
    if bp.keys() != ap.keys():
        raise ValueError("html_route_set_changed")
    if before["meta"].get("max_pages") != after.get("meta", {}).get("max_pages") or any(
            after.get("meta", {}).get(k) for k in ("stopped_on_time_budget", "urls_uncrawled")) or (
            after.get("meta", {}).get("blocked_by_host") or {}).get("count"):
        raise ValueError("incomplete_or_incomparable_crawl")
    if ap[ROUTE].get("canonical") != DESTINATION:
        raise ValueError("incorrect_canonical_destination")
    fields = titles.PRESERVED + ("title", "title_tag_count", "meta_description", "meta_description_tag_count",
        "h1_tag_count", "h2_tag_count", "content_sketch", "text_word_count", "internal_link_items", "ld_json_blocks",
        "status_code", "content_type", "error", "blocked_by_host")
    for route, page in bp.items():
        for field in fields:
            if field == "canonical" and route == ROUTE:
                continue
            if page.get(field) != ap[route].get(field):
                raise ValueError("collateral_observed_html_changed")
    for key in (KEY, REDIRECT_KEY):
        if after.get("issues", {}).get(key, {}).get("count") != 0 or after["issues"][key].get("evidence", {}).get("items"):
            raise ValueError("canonical_family_not_resolved")
    return {"html_routes_before": len(bp), "html_routes_after": len(ap), "observed_fields_preserved": True,
        "target_counts": {k: [before["issues"][k]["count"], after["issues"][k]["count"]] for k in (KEY, REDIRECT_KEY)},
        "increased_counts": {k: [before["issues"].get(k, {}).get("count", 0), v.get("count", 0)]
            for k, v in after["issues"].items() if v.get("count", 0) > before["issues"].get(k, {}).get("count", 0)}}


def observations(preview: str, out: Path) -> dict:
    rows = previous.observations(preview, out)
    for name, route in EXTRA.items():
        response = live.requests.get(preview.rstrip("/") + route, timeout=30, allow_redirects=False)
        (out / (name + ".http")).write_bytes(response.content)
        rows[name] = {"status": response.status_code, "location": response.headers.get("location", ""),
            "x_robots_tag": response.headers.get("x-robots-tag", ""), "sha256": hashlib.sha256(response.content).hexdigest()}
    live.save(out / "http.json", rows)
    if rows["old"]["status"] != 301 or rows["old"]["location"] != previous.ROUTES["qa-master"]:
        raise ValueError("legitimate_redirect_changed")
    if any(rows[n]["status"] != 200 or rows[n]["x_robots_tag"] != "noindex" for n in (*previous.NAMES, *EXTRA)):
        raise ValueError("html_or_native_preview_policy_changed")
    return rows


def cycle(m, token: str, work: Path, budget: ClaudeBudget) -> dict:
    if budget.limit != 0:
        raise ValueError("Only a zero provider budget is permitted.")
    result = {"status": "unverified", "stage": "identity", "repository": f"{live.OWNER}/{REPO}",
        "seed_sha": SEED, "pull_requests": [], "writes": [], "write_attempts": 0, "universal_certification": False}
    original_put = m._github_api_put
    corrected, expected, blob_sha = "", "", ""

    def get(*parts, **kwargs):
        return m._github_api_get(m._github_api_path("repos", live.OWNER, REPO, *parts), token=token, **kwargs)

    def ref(branch):
        return get("git", "ref", "heads", branch)["object"]["sha"]

    def source(path, head):
        row = get("contents", path, params={"ref": head})
        return row["sha"], base64.b64decode(row["content"]).decode("utf-8")

    def guarded_put(path, **kwargs):
        if path != m._github_content_api_path(live.OWNER, REPO, FILE) or not write_allowed(
                FILE, kwargs.get("json_body", {}), corrected, result["write_attempts"], expected, blob_sha):
            raise ValueError("outside_single_canonical_write_scope")
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
            title="QA residual canonical " + kind + " (never merge)", body="Owned fixture only. Close without merging.")
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
        observations(url, out)
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
        blob_sha, old_source = source(FILE, SEED)
        expected = expected_source(old_source)
        stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d-%H%M%S")
        baseline, corrected = (PREFIX + mode + "-" + stamp for mode in ("baseline", "correction"))
        result.update(main_sha=MAIN, baseline_branch=baseline, correction_branch=corrected)
        branch(baseline)
        before = preview("baseline", baseline, SEED)
        baseline_checks(before)
        result["stage"] = "prepare"
        prep = m._prepare_issue_fix(issue_key=KEY, issues=before["issues"], impacted=[SOURCE], all_paths=paths,
            site_name=urlsplit(SITE).netloc, owner=live.OWNER, repo_name=REPO, branch=baseline, token=token,
            pages=before["pages"])
        if prep["refusal"] or prep["rewriter_ai_fallback"] or prep.get("canonical_pairs") != [PAIR]:
            raise ValueError("residual_repair_unverified")
        branch(corrected)
        from backend import repo_index
        applied = m._apply_prepared_issue_fix(owner=live.OWNER, repo_name=REPO, branch=baseline, token=token,
            fix_branch=corrected, all_paths=paths, issue_key=KEY, issue_label=KEY, impacted=[SOURCE],
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
        result.update(checks=checks(before, after), final_sha=final_sha, status="measured_residual_canonical_resolved")
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
    return int(result["status"] != "measured_residual_canonical_resolved")


if __name__ == "__main__":
    raise SystemExit(main())
