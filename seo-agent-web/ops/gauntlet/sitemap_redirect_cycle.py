"""Measure sitemap redirects with owned flat rules, live negative controls and zero AI."""

from __future__ import annotations

import argparse
import base64
import datetime as dt
from pathlib import Path
import sys
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ops.gauntlet import canonical_sitemap_cycle as previous  # noqa: E402
from ops.gauntlet.ai_budget import ClaudeBudget  # noqa: E402

live, projection, titles = previous.live, previous.projection, previous.titles
REPO, MAIN, SITE = previous.REPO, previous.MAIN, previous.SITE
SEED_BRANCH = "gauntlet-static-html-canonical-sitemap-correction-20261005-050823"
SEED, SEED_PR = "ab31afaddafdf86c26649a1f3e908647b22045ed", 64
PREFIX, KEY = "gauntlet-static-html-sitemap-redirect-", "sitemap_3xx_redirect"
TARGETS = {SITE + "gauntlet/ancienne-page": SITE + "gauntlet/",
           SITE + "gauntlet/qa-hop-old": SITE + "gauntlet/qa-master",
           SITE + "gauntlet/qa-sitemap-chain": SITE + "gauntlet/qa-master"}
NEGATIVES = {SITE + "gauntlet/qa-sitemap-noindex": SITE + "gauntlet/noindex-long",
             SITE + "gauntlet/qa-sitemap-noncanonical": SITE + "gauntlet/canonical-other"}
ALIASES = {**TARGETS, **NEGATIVES}
PAIRS = [{"page": source, "from": source, "to": target} for source, target in sorted(TARGETS.items())]
RULES = ("/gauntlet/qa-sitemap-chain /gauntlet/qa-hop-old 302\n"
         "/gauntlet/qa-sitemap-noindex /gauntlet/noindex-long 302\n"
         "/gauntlet/qa-sitemap-noncanonical /gauntlet/canonical-other 302\n")
SETUP = {"index.html", "_redirects", "sitemap.xml"}


def alias_entry(url: str) -> str:
    if url not in ALIASES:
        raise ValueError("outside_owned_sitemap_alias_scope")
    return "<url><loc>" + url + "</loc><lastmod>2001-01-01</lastmod><priority>0.1</priority></url>"


def setup_sources(original: dict[str, str]) -> dict[str, str]:
    if set(original) != SETUP or len(previous.locs(original["sitemap.xml"])) != 49:
        raise ValueError("unexpected_pinned_source_shape")
    if any(original[path].count(marker) != 1 for path, marker in (("index.html", "</body>"), ("sitemap.xml", "</urlset>"))):
        raise ValueError("ambiguous_fixture_insert_point")
    values = previous.locs(original["sitemap.xml"])
    if set(ALIASES) & set(values) or any(values.count(url) != 1 for url in set(TARGETS.values())):
        raise ValueError("missing_masters_or_existing_aliases")
    if any("qa-sitemap-" in original[path] for path in SETUP):
        raise ValueError("positive_controls_already_present")
    links = "".join('<p><a href="' + urlsplit(url).path + '">QA sitemap ' + urlsplit(url).path.rsplit("/", 1)[-1]
                    + '</a></p>\n' for url in sorted(ALIASES))
    return {"index.html": original["index.html"].replace("</body>", links + "</body>"),
            "_redirects": RULES + original["_redirects"],
            "sitemap.xml": original["sitemap.xml"].replace("</urlset>", "\n".join(alias_entry(url) for url in sorted(ALIASES)) + "\n</urlset>")}


def corrected_source(source: str) -> str:
    values = previous.locs(source)
    if len(values) != 54 or any(values.count(url) != 1 for url in {*ALIASES, *TARGETS.values()}):
        raise ValueError("missing_unique_alias_or_master_entry")
    output = source
    for url in TARGETS:
        block = alias_entry(url)
        if source.count(block) != 1:
            raise ValueError("unexpected_alias_entry_bytes")
        output = output.replace(block, "")
    if previous.locs(output) != [url for url in values if url not in TARGETS]:
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


def checks(before: dict, after: dict) -> dict:
    previous.previous.previous.complete(before)
    previous.previous.previous.complete(after)
    bp, ap = live._html_pages(before), live._html_pages(after)
    if bp.keys() != ap.keys() or len(bp) != 52:
        raise ValueError("html_routes_lost")
    for report, expected in ((before, set(ALIASES)), (after, set(NEGATIVES))):
        block = report["issues"][KEY]
        if block.get("count") != len(expected) or set(block.get("examples", [])) != expected:
            raise ValueError("missing_positive_or_refused_witnesses")
        requested = {row.get("url") for row in report["pages"]}
        if not set(ALIASES) <= requested:
            raise ValueError("redirect_observations_lost_after_delisting")
        for key, examples in ((previous.KEY, previous.NEGATIVES), (previous.previous.KEY, previous.previous.NEGATIVES)):
            if report["issues"][key]["count"] != 2 or set(report["issues"][key]["examples"]) != set(examples):
                raise ValueError("previous_negative_controls_changed")
    fields = titles.PRESERVED + ("title", "title_tag_count", "meta_description", "meta_description_tag_count",
        "h1_tag_count", "h2_tag_count", "content_sketch", "text_word_count", "internal_link_items", "ld_json_blocks")
    if any(old.get(field) != ap[route].get(field) for route, old in bp.items() for field in fields):
        raise ValueError("collateral_html_fields_changed")
    requested_before = {row["url"]: row for row in before["pages"]}
    requested_after = {row["url"]: row for row in after["pages"]}
    redirect_fields = ("final_url", "status_code", "content_type", "error", "blocked_by_host", "canonical", "redirect_chain", "redirect_statuses")
    if any(requested_before[url].get(field) != requested_after[url].get(field) for url in ALIASES for field in redirect_fields):
        raise ValueError("redirect_chain_observations_changed")
    return {"html_routes": [52, 52], "selected_occurrences": [3, 0], "target_counts": {KEY: [5, 2]},
        "refused_entries_remaining": sorted(NEGATIVES), "all_redirect_witnesses_still_observed": True,
        "observed_html_fields_preserved": True, "redirect_chain_observations_preserved": True,
        "increased_counts": {key: [before["issues"].get(key, {}).get("count", 0), value.get("count", 0)]
            for key, value in after["issues"].items() if value.get("count", 0) > before["issues"].get(key, {}).get("count", 0)}}


def cycle(m, token: str, work: Path, budget: ClaudeBudget) -> dict:
    if budget.limit != 0:
        raise ValueError("nonzero_provider_budget")
    result = {"status": "unverified", "stage": "identity", "repository": f"{live.OWNER}/{REPO}", "seed_sha": SEED,
              "pull_requests": [], "writes": [], "write_attempts": [], "universal_certification": False}
    original_put, writable, expected, shas = m._github_api_put, "", {}, {}

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
        if len(result["write_attempts"]) >= 4 or not write_allowed(path, kwargs.get("json_body", {}), writable, attempts, expected, shas.get(path)):
            raise ValueError("outside_exact_qa_write_scope")
        row = {"branch": writable, "path": path}
        result["write_attempts"].append(row)
        live.save(work / "cycle.json", result)
        response = original_put(api, **kwargs)
        result["writes"].append(row)
        return response

    def branch(name, sha):
        if not name.startswith(PREFIX) or not m._github_branch_allowed(name):
            raise ValueError("outside_owned_qa_branch")
        m._github_api_post(m._github_api_path("repos", live.OWNER, REPO, "git", "refs"), token=token,
                          json_body={"ref": "refs/heads/" + name, "sha": sha})

    def preview(kind, head, sha):
        result["stage"] = kind
        print("STAGE=" + kind, flush=True)
        pr = m._ouvrir_pull_request(owner=live.OWNER, repo=REPO, token=token, head=head, base="main", draft=True,
            title="QA sitemap redirects " + kind + " (never merge)", body="Owned fixture only. Close without merging.")
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
        observations = {}
        for alias, destination in ALIASES.items():
            response = live.requests.get(url.rstrip("/") + urlsplit(alias).path, timeout=30, allow_redirects=False)
            location = response.headers.get("location", "")
            observations[alias] = {"status": response.status_code, "location": location}
            if response.status_code not in {301, 302} or not location:
                raise ValueError("missing_native_redirect_control")
            response = live.requests.get(url.rstrip("/") + urlsplit(destination).path, timeout=30, allow_redirects=False)
            (out / (urlsplit(alias).path.rsplit("/", 1)[-1] + ".http")).write_bytes(response.content)
            if response.status_code != 200 or response.headers.get("x-robots-tag") != "noindex":
                raise ValueError("destination_or_native_preview_policy_changed")
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
        pinned = {path: read(path, SEED) for path in SETUP}
        setup = setup_sources({path: row[1] for path, row in pinned.items()})
        stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d-%H%M%S")
        baseline, correction = (PREFIX + kind + "-" + stamp for kind in ("baseline", "correction"))
        result.update(main_sha=MAIN, baseline_branch=baseline, correction_branch=correction)
        branch(baseline, SEED)
        writable, expected, shas = baseline, setup, {path: row[0] for path, row in pinned.items()}
        for path in sorted(SETUP):
            guarded_put(m._github_content_api_path(live.OWNER, REPO, path), token=token,
                json_body={"branch": baseline, "sha": shas[path], "message": "QA sitemap redirect controls (never merge)",
                           "content": base64.b64encode(expected[path].encode()).decode()})
        baseline_sha = ref(baseline)
        result["baseline_sha"] = baseline_sha
        before = preview("baseline", baseline, baseline_sha)
        prep = m._prepare_issue_fix(issue_key=KEY, issues=before["issues"], impacted=before["issues"][KEY]["examples"],
            all_paths=paths, site_name=urlsplit(SITE).netloc, owner=live.OWNER, repo_name=REPO,
            branch=baseline, token=token, pages=before["pages"])
        if prep["refusal"] or prep["rewriter_ai_fallback"] or sorted(prep.get("sitemap_redirect_pairs", []), key=lambda p: p["from"]) != PAIRS:
            raise ValueError("positive_pairs_not_verified")
        if not all(url in prep["side_effects"] for url in NEGATIVES):
            raise ValueError("refusals_not_reported")
        branch(correction, baseline_sha)
        blob, source = read("sitemap.xml", baseline_sha)
        writable, expected, shas = correction, {"sitemap.xml": corrected_source(source)}, {"sitemap.xml": blob}
        from backend import repo_index
        repair = m._apply_prepared_issue_fix(owner=live.OWNER, repo_name=REPO, branch=baseline, token=token,
            fix_branch=correction, all_paths=paths, issue_key=KEY, issue_label=KEY, impacted=before["issues"][KEY]["examples"],
            site_name=urlsplit(SITE).netloc, file_state={}, max_files=1, prep=prep, pages=before["pages"],
            index=repo_index.build_repo_index(paths), allow_ai_targeting=False)
        result["repair"] = repair
        if repair["patched"] != ["sitemap.xml"] or repair["skipped"] or repair["ai_files"] or repair["config_changes"]:
            raise ValueError("unexpected_correction_scope")
        final_sha = ref(correction)
        comparison = get("compare", baseline_sha + "..." + final_sha)
        if comparison["total_commits"] != 1 or [f["filename"] for f in comparison["files"]] != ["sitemap.xml"]:
            raise ValueError("collateral_source_changed")
        if read("sitemap.xml", final_sha)[1] != expected["sitemap.xml"] or any(read(path, final_sha)[1] != setup[path] for path in SETUP - {"sitemap.xml"}):
            raise ValueError("exact_source_check_failed")
        after = preview("final", correction, final_sha)
        result.update(status="measured_verified_sitemap_redirects_resolved", final_sha=final_sha, checks=checks(before, after), sitemap_entries=[54, 51])
    except Exception as exc:
        result.update(status="failed", error_type=type(exc).__name__)
        if isinstance(exc, ValueError) and str(exc).replace("_", "").isalnum():
            result["failure_code"] = str(exc)
        print(result["status"], result["stage"], result.get("failure_code", result["error_type"]), flush=True)
    finally:
        m._github_api_put = original_put
        result["ai_budget"] = budget.summary()
        result["cleanup"] = live.close_prs(REPO, token, result["pull_requests"])
        result["prs_closed_unmerged"] = previous.previous.previous.previous.closed_cleanup(result["pull_requests"], result["cleanup"])
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
    return int(result["status"] != "measured_verified_sitemap_redirects_resolved")


if __name__ == "__main__":
    raise SystemExit(main())
