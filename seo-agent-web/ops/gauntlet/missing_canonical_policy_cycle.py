"""Read-only refusal measurement, not a missing-canonical repair or readiness verdict."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path
import sys
from urllib.parse import urlsplit

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ops.gauntlet import short_description_cycle as previous  # noqa: E402
from ops.gauntlet.ai_budget import ClaudeBudget  # noqa: E402

live, shared = previous.live, previous.shared
REPO, MAIN, SITE = previous.REPO, previous.MAIN, previous.SITE
BRANCH = "gauntlet-static-html-short-description-correction-20261008-034500"
HEAD, PR = "3a4a8c19c1d47c6d8a9453b5288545ec54349c8c", 96
PREVIEW = f"https://deploy-preview-{PR}--{REPO}.netlify.app/"
KEY = "missing_canonical"
FILES = ("gauntlet/no-canonical-a.html", "gauntlet/no-canonical-b.html")
URLS = tuple(SITE + path.removesuffix(".html") for path in FILES)
FIELDS = previous.titles.PRESERVED + (
    "url", "final_url", "redirect_chain", "redirect_statuses", "title", "title_tag_count",
    "meta_description", "meta_description_tag_count", "h1_tag_count", "h2_tag_count", "content_sketch",
    "text_word_count", "internal_link_items", "links_without_anchor_text", "ld_json_blocks",
    "meta_robots_tag_count", "status_code", "content_type", "error", "blocked_by_host", "meta_viewport",
    "meta_viewport_tag_count", "og_title", "og_description", "twitter_title", "twitter_description",
)


def observe(raw: bytes) -> dict:
    soup = BeautifulSoup(raw.decode("utf-8", errors="strict"), "html.parser")
    if len(soup.find_all("head")) != 1 or len(soup.find_all("html")) != 1:
        raise ValueError("ambiguous_control_html")
    titles = soup.head.find_all("title")
    canonicals = soup.find_all("link", rel=lambda value: value and "canonical" in value.lower().split())
    return {"canonical_count": len(canonicals), "canonicals": [tag.get("href") for tag in canonicals],
            "title": titles[0].get_text() if len(titles) == 1 else None,
            "lang": soup.html.get("lang"), "h1": [tag.get_text() for tag in soup.find_all("h1")]}


def default_native_rejections(m) -> int:
    return sum(m._sitemap_https_page(PREVIEW.rstrip("/") + route, canonical) is False
               for route, canonical in zip(previous.ROUTES[:2], (previous.SOURCE, previous.ABSENT)))


def checks(m, before: dict, after: dict) -> dict:
    shared.complete(before)
    shared.complete(after)
    bp, ap = live._html_pages(before), live._html_pages(after)
    if bp.keys() != ap.keys() or len(bp) != 64:
        raise ValueError("html_routes_lost")
    for report in (before, after):
        block = report.get("issues", {}).get(KEY, {})
        if type(block.get("count")) is not int or block["count"] != 2 or sorted(block.get("examples", [])) != sorted(URLS):
            raise ValueError("diagnostic_erased_or_control_lost")
        for url in URLS:
            row = bp.get(urlsplit(url).path) if report is before else ap.get(urlsplit(url).path)
            if not row or row.get("canonical") is not None or row.get("status_code") != 200:
                raise ValueError("legacy_control_modified")
        if any(row["key"].strip().lower() in m.dash.LEGACY_CANONICAL_KEYS
               for row in m.dash.summarize_report(report)["issues"]):
            raise ValueError("legacy_diagnostic_shown_as_issue")
        if any(m._github_issue_auto_fixable(key) for key in m.dash.LEGACY_CANONICAL_KEYS):
            raise ValueError("legacy_diagnostic_offered_for_correction")
    if any(old.get(field) != ap[route].get(field) for route, old in bp.items() for field in FIELDS):
        raise ValueError("collateral_html_observation_changed")
    for key in before["issues"].keys() | after["issues"].keys():
        old, new = before["issues"].get(key, {}), after["issues"].get(key, {})
        examples = lambda block: sorted(json.dumps(item, sort_keys=True) for item in block.get("examples", []))
        if old.get("count", 0) != new.get("count", 0) or examples(old) != examples(new):
            raise ValueError("issue_count_or_examples_changed")
    return {"healthy_html_routes": [64, 64], "diagnostic_counts_retained": [2, 2],
            "automatic_canonical_repair_performed": False, "observed_html_fields_preserved": True,
            "other_issue_counts_and_examples_preserved": True, "legacy_diagnostic_hidden_and_not_offered": True}


def cycle(m, token: str, work: Path, budget: ClaudeBudget) -> dict:
    if budget.limit != 0:
        raise ValueError("zero_provider_budget_required")
    result = {"status": "unverified", "repository": f"{live.OWNER}/{REPO}", "head_sha": HEAD,
        "existing_closed_draft_unmerged_pr": PR, "writes": [], "write_attempts": 0,
        "automatic_canonical_repair_performed": False, "universal_certification": False}
    originals = {name: getattr(m, name) for name in ("_github_api_post", "_github_api_put")}
    original_request = live.requests.sessions.Session.request

    def deny_write(*args, **kwargs):
        result["write_attempts"] += 1
        raise ValueError("read_only_measurement")

    def read_only_request(session, method, url, *args, **kwargs):
        if method.upper() not in {"GET", "HEAD"}:
            return deny_write()
        return original_request(session, method, url, *args, **kwargs)

    def get(*parts, **kwargs):
        return m._github_api_get(m._github_api_path("repos", live.OWNER, REPO, *parts), token=token, **kwargs)

    def identity():
        if get("git", "ref", "heads", "main")["object"]["sha"] != MAIN or (
                get("git", "ref", "heads", BRANCH)["object"]["sha"] != HEAD):
            raise ValueError("fixture_identity_changed")
        pr = get("pulls", str(PR))
        if pr["state"] != "closed" or pr["merged"] or not pr["draft"] or pr["head"]["sha"] != HEAD:
            raise ValueError("fixture_pr_changed")
        return live.wait_build(m, REPO, token, PR, timeout=30, expected_sha=HEAD)

    def snapshot(kind):
        out = work / kind
        out.mkdir()
        hashes, observations = {}, {}
        for path in (*FILES, "sitemap.xml"):
            row = get("contents", path, params={"ref": HEAD})
            source = base64.b64decode("".join(row["content"].split()), validate=True)
            response = live.requests.get(PREVIEW + path.removesuffix(".html"), timeout=30, allow_redirects=False)
            try:
                if response.status_code != 200 or response.headers.get("location"):
                    raise ValueError("unhealthy_control")
                (out / path.replace("/", "_")).write_bytes(source)
                (out / (path.replace("/", "_") + ".http")).write_bytes(response.content)
                hashes[path] = {"blob_sha": row["sha"], "source_sha256": hashlib.sha256(source).hexdigest(),
                                "http_sha256": hashlib.sha256(response.content).hexdigest()}
                if path in FILES:
                    if "html" not in response.headers.get("content-type", "").lower() or response.headers.get("x-robots-tag") != "noindex":
                        raise ValueError("native_preview_policy_changed")
                    source_tags, served_tags = observe(source), observe(response.content)
                    if source_tags != served_tags or source_tags["canonical_count"] != 0 or not source_tags["title"]:
                        raise ValueError("literal_control_changed")
                    observations[path] = served_tags
                else:
                    (out / "sitemap.xml").write_bytes(response.content)
            finally:
                response.close()
        live.crawl("static-html", PREVIEW, out / "raw", 90, preview=True)
        report = shared.previous.rescore(out / "raw/report.json", PREVIEW, (out / "sitemap.xml").read_bytes(), out / "controlled")
        live.save(out / "snapshot.json", {"hashes": hashes, "observations": observations})
        return report, hashes

    for name in originals:
        setattr(m, name, deny_write)
    live.requests.sessions.Session.request = read_only_request
    try:
        result["build_before"] = identity()
        print("STAGE=read_only_before", flush=True)
        before, initial = snapshot("before")
        for key in sorted(m.dash.LEGACY_CANONICAL_KEYS):
            prep = m._prepare_issue_fix(issue_key=key, issues=before["issues"], impacted=list(URLS),
                all_paths=list(FILES), site_name=urlsplit(SITE).netloc, owner=live.OWNER, repo_name=REPO,
                branch=BRANCH, token=token, pages=before["pages"])
            applied = m._apply_prepared_issue_fix(owner=live.OWNER, repo_name=REPO, branch=BRANCH, token=token,
                fix_branch=BRANCH, all_paths=list(FILES), issue_key=key, issue_label=key, impacted=list(URLS),
                site_name=urlsplit(SITE).netloc, file_state={}, max_files=8, prep={}, pages=before["pages"], index=None)
            if not prep["refusal"] or not applied["error"] or applied["patched"] or applied["ai_files"]:
                raise ValueError("legacy_preparation_or_executor_not_refused")
        rows = live._html_pages(before)
        if any(rows.get(route, {}).get("canonical") != canonical
               for route, canonical in zip(previous.ROUTES[:2], (previous.SOURCE, previous.ABSENT))):
            raise ValueError("native_probe_selfcanonical_control_changed")
        result["default_native_preview_probes_rejected"] = default_native_rejections(m)
        if result["default_native_preview_probes_rejected"] != 2:
            raise ValueError("default_preview_policy_weakened")
        print("STAGE=read_only_after", flush=True)
        after, final = snapshot("after")
        if initial != final:
            raise ValueError("source_blob_xml_or_served_bytes_changed")
        result["checks"] = checks(m, before, after)
        result["build_after"] = identity()
        result["snapshots_unchanged"] = initial
        if result["write_attempts"] or budget.summary()["attempted"] or budget.summary()["denied"]:
            raise ValueError("unexpected_write_or_provider_attempt")
        result.update(status="passed_read_only_refusal_measurement", main_unchanged=True,
            native_noindex_preserved=True, real_crawls=2, test_only_preview_projection=True)
    except Exception as exc:
        result.update(status="failed", error=type(exc).__name__ + ": " + str(exc))
    finally:
        for name, original in originals.items():
            setattr(m, name, original)
        live.requests.sessions.Session.request = original_request
        result["claude_budget"] = budget.summary()
        live.save(work / "cycle.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", type=Path, required=True)
    args = parser.parse_args()
    if args.workdir.exists() and any(args.workdir.iterdir()):
        parser.error("Use an empty directory; no existing evidence is overwritten.")
    args.workdir.mkdir(parents=True, exist_ok=True)
    m, token = live._load_backend(args.workdir)
    budget = ClaudeBudget(0)
    budget.install(m)
    result = cycle(m, token, args.workdir, budget)
    print("STATUS=" + result["status"], flush=True)
    return 0 if result["status"] == "passed_read_only_refusal_measurement" else 1


if __name__ == "__main__":
    raise SystemExit(main())
