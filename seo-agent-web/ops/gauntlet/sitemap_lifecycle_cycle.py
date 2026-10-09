"""Owned static fixture: missing/broken sitemap, real recrawl, then robots declaration.

Zero provider calls. Only sitemap.xml/robots.txt on fresh QA branches may change.
Raw previews stay noindex; controlled page scoring is explicitly counterfactual.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import importlib.util
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from defusedxml import ElementTree as XML  # noqa: E402
from defusedxml.common import DefusedXmlException  # noqa: E402

from ops.gauntlet import duplicate_batch_cycle as batches  # noqa: E402
from ops.gauntlet import hreflang_cycle as projection  # noqa: E402
from ops.gauntlet.ai_budget import ClaudeBudget  # noqa: E402

live = batches.live
REPO = "noyaru-stack-static-html"
MAIN_SHA = "ba041ef05c0f51c1c228e3442928c896fbc00102"
SITE = f"https://{REPO}.netlify.app/"
KEYS = {"missing": "sitemap_xml_not_found", "invalid": "sitemap_invalid_format"}
SYSTEM_KEYS = (*KEYS.values(), "sitemap_not_in_robots")
NS = "{http://www.sitemaps.org/schemas/sitemap/0.9}"


def locs(body: bytes) -> list[str] | None:
    try:
        root = XML.fromstring(body)
    except (XML.ParseError, DefusedXmlException):
        return None
    if root.tag != NS + "urlset":
        raise ValueError("This fixture bench only accepts a sitemap URL set.")
    return [node.text or "" for node in root.findall(NS + "url/" + NS + "loc")]


def declarations(robots: str) -> list[str]:
    return [line.split(":", 1)[1].strip() for line in robots.splitlines()
            if line.strip().lower().startswith("sitemap:")]


def identity(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc.lower(), parts.path or "/", parts.query, ""))


def eligible_urls(report: dict) -> set[str]:
    return {identity(p.get("final_url") or p["url"]) for p in report["pages"]
            if type(p.get("status_code")) is int and p["status_code"] == 200
            and "html" in str(p.get("content_type") or "").lower() and not p.get("error")
            and not p.get("blocked_by_host") and not any("noindex" in str(p.get(k) or "").lower()
                for k in ("meta_robots", "x_robots_tag"))
            and (not p.get("canonical") or identity(p["canonical"]) == identity(p.get("final_url") or p["url"]))}


def file_snapshot(url: str, work: Path) -> dict:
    work.mkdir(parents=True, exist_ok=True)
    result = {}
    for name in ("sitemap.xml", "robots.txt"):
        response = live.requests.get(url + name, timeout=30, allow_redirects=False)
        body = response.content
        (work / name).write_bytes(body)
        result[name] = {"status_code": response.status_code,
                        "content_type": response.headers.get("content-type", ""),
                        "sha256": hashlib.sha256(body).hexdigest()}
    live.save(work / "files.json", result)
    return result


def controlled(raw: Path, preview: str, work: Path) -> dict:
    report, removed = projection.project(live.load(raw), preview, SITE)
    body = (work / "sitemap.xml").read_bytes()
    entries = locs(body) if live.load(work / "files.json")["sitemap.xml"]["status_code"] == 200 else []
    projected, _ = projection.project({"entries": entries or []}, preview, SITE)
    spec = importlib.util.spec_from_file_location("gauntlet_sitemap_audit", live.AUDITOR)
    auditor = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = auditor
    spec.loader.exec_module(auditor)
    output = work / "controlled"
    output.mkdir()
    report["issues"] = auditor._score_issues(auditor._pages_from_report(report),
        sitemap_urls=set(projected["entries"]), sitemap_urlsets={SITE + "sitemap.xml": projected["entries"]},
        base_url=SITE, output_dir=str(output))
    files = live.load(work / "files.json")
    robots = (work / "robots.txt").read_text(encoding="utf-8")
    counts = {"sitemap_xml_not_found": int(files["sitemap.xml"]["status_code"] >= 400),
              "sitemap_invalid_format": int(files["sitemap.xml"]["status_code"] == 200 and entries is None),
              "sitemap_not_in_robots": int(files["robots.txt"]["status_code"] == 200 and not declarations(robots))}
    for key, count in counts.items():
        report["issues"][key] = {"count": count, "examples": [SITE + ("robots.txt" if key.endswith("robots") else "sitemap.xml")] if count else []}
    live.save(output / "report.json", report)
    live.save(output / "projection.json", {"measurement_mode": report["validation_mode"],
        "removed_exact_preview_noindex_headers": removed, "sitemap_entries_from_exact_served_preview": projected["entries"],
        "system_counts_from_saved_http_bodies": counts, "raw_crawl_discovery_is_not_projected": True,
        "production_indexing_certified": False})
    return report


def cycle(m, token: str, case: str, work: Path, budget: ClaudeBudget) -> dict:
    if case not in KEYS or budget.limit != 0:
        raise ValueError("Only the two owned fixture cases and zero provider calls are permitted.")
    result = {"case": case, "repository": f"{live.OWNER}/{REPO}", "status": "unverified",
              "pull_requests": [], "writes": [], "stage": "seed_identity", "universal_certification": False}
    original_put = m._github_api_put
    writable = ""

    def ref(branch):
        return m._github_api_get(m._github_ref_api_path(live.OWNER, REPO, branch), token=token)["object"]["sha"]

    def branch(name, sha):
        if not name.startswith("gauntlet-static-html-sitemap-") or not m._github_branch_allowed(name):
            raise ValueError("Only fresh dedicated sitemap QA branches may be created.")
        m._github_api_post(m._github_api_path("repos", live.OWNER, REPO, "git", "refs"), token=token,
                          json_body={"ref": "refs/heads/" + name, "sha": sha})

    def guarded_put(path, **kw):
        allowed = {m._github_content_api_path(live.OWNER, REPO, name): name for name in ("sitemap.xml", "robots.txt")}
        if not writable or path not in allowed or kw["json_body"].get("branch") != writable or len(result["writes"]) >= 4:
            raise ValueError("Only four sitemap/robots fixture PUTs are permitted per case.")
        reply = original_put(path, **kw)
        result["writes"].append({"branch": writable, "path": allowed[path]})
        return reply

    def preview(kind, name, *, existing=None):
        if existing is None:
            pr = m._ouvrir_pull_request(owner=live.OWNER, repo=REPO, token=token, head=name, base="main", draft=True,
                title=f"QA sitemap lifecycle {case}: {kind} (never merge)", body="Owned fixture only. Close without merging.")
            existing = {"kind": kind, "number": pr["number"], "url": pr["html_url"], "builds": []}
            result["pull_requests"].append(existing)
        result["stage"] = kind
        expected_sha = ref(name)
        result["expected_build_sha"] = expected_sha
        live.save(work / "cycle.json", result)
        build = live.wait_build(m, REPO, token, existing["number"], expected_sha=expected_sha)
        if build["head_sha"] != expected_sha or ref(name) != expected_sha:
            raise ValueError("Preview build does not match the exact current branch head.")
        existing["builds"].append({"stage": kind, **build})
        url = f"https://deploy-preview-{existing['number']}--{REPO}.netlify.app/"
        root = live.requests.get(url, timeout=30, allow_redirects=False)
        if root.status_code != 200 or "html" not in root.headers.get("content-type", "").lower():
            raise ValueError("The exact fixture preview root is not usable.")
        file_snapshot(url, work / kind)
        live.crawl("static-html", url, work / kind / "raw", 90, preview=True)
        return controlled(work / kind / "raw/report.json", url, work / kind), existing

    m._github_api_put = guarded_put
    try:
        if ref("main") != MAIN_SHA:
            raise ValueError("The fixture main no longer matches the inspected seed.")
        result["main_sha"] = MAIN_SHA
        tree = m._github_api_get(m._github_api_path("repos", live.OWNER, REPO, "git", "trees", MAIN_SHA),
                                token=token, params={"recursive": "1"})
        if tree.get("truncated"):
            raise ValueError("Incomplete fixture tree.")
        paths = [r["path"] for r in tree["tree"] if r["type"] == "blob"]
        if m._fichiers_sitemap_du_depot(paths) != ["sitemap.xml"] or "robots.txt" not in paths:
            raise ValueError("The original fixture sitemap/robots layout is not the inspected one.")
        stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d-%H%M%S")
        baseline = f"gauntlet-static-html-sitemap-{case}-baseline-{stamp}"
        corrected = f"gauntlet-static-html-sitemap-{case}-correction-{stamp}"
        branch(baseline, MAIN_SHA)
        result.update(baseline_branch=baseline, correction_branch=corrected)
        writable = baseline
        for name in ("sitemap.xml", "robots.txt"):
            source = m._github_api_get(m._github_content_api_path(live.OWNER, REPO, name), token=token, params={"ref": baseline})
            if name == "sitemap.xml" and case == "missing":
                # Fixture setup only: one deletion on a fresh QA branch, never on main.
                response = live.requests.delete(m._github_api_url(m._github_content_api_path(live.OWNER, REPO, name)),
                    headers={"Authorization": "Bearer " + token, "Accept": "application/vnd.github+json"},
                    json={"message": "QA: remove fixture sitemap (never merge)", "branch": baseline, "sha": source["sha"]}, timeout=30)
                response.raise_for_status()
                result["setup_deleted_path"] = name
                paths.remove(name)
            else:
                old = base64.b64decode(source["content"]).decode("utf-8")
                new = ("<urlset><url><loc>broken" if name == "sitemap.xml" else
                       "".join(line for line in old.splitlines(keepends=True) if not line.strip().lower().startswith("sitemap:")))
                if new != old:
                    guarded_put(m._github_content_api_path(live.OWNER, REPO, name), token=token,
                        json_body={"message": "QA sitemap positive control (never merge)", "branch": baseline,
                                   "sha": source["sha"], "content": base64.b64encode(new.encode()).decode()})
        before, _ = preview("baseline", baseline)
        key = KEYS[case]
        if before["issues"][key]["count"] != 1 or before["issues"]["sitemap_not_in_robots"]["count"] != 1:
            raise ValueError("Missing positive sitemap/robots controls.")
        expected = eligible_urls(before)
        if not expected:
            raise ValueError("No measured indexable fixture pages.")
        branch(corrected, ref(baseline))
        writable = corrected
        from backend import repo_index
        index, state = repo_index.build_repo_index(paths), {}

        def apply(key, report):
            prep = m._prepare_issue_fix(issue_key=key, issues=report["issues"], impacted=[], all_paths=paths,
                site_name=urlsplit(SITE).netloc, owner=live.OWNER, repo_name=REPO, branch=baseline, token=token, pages=report["pages"])
            if prep["refusal"]:
                raise ValueError("Fixture repair was refused.")
            applied = m._apply_prepared_issue_fix(owner=live.OWNER, repo_name=REPO, branch=baseline, token=token,
                fix_branch=corrected, all_paths=paths, issue_key=key, issue_label=key, impacted=[], site_name=urlsplit(SITE).netloc,
                file_state=state, max_files=1, prep=prep, pages=report["pages"], index=index, allow_ai_targeting=False)
            result.setdefault("repairs", []).append({"key": key, **applied})
            expected_path = "robots.txt" if key == "sitemap_not_in_robots" else "sitemap.xml"
            if applied["skipped"] or applied["ai_files"] or set(applied["patched"] + applied["config_changes"]) != {expected_path}:
                raise ValueError("A fixture repair did not perform its single expected model-free write.")

        apply(key, before)
        if case == "invalid":
            changes, notes = m._deep_reparer_le_sitemap(owner=live.OWNER, repo_name=REPO, token=token,
                fix_branch=corrected, all_paths=paths, pages=before["pages"])
            if changes or not notes:
                raise ValueError("A stale report overwrote an already repaired source.")
            result["stale_repair_refused_before_put"] = True
        middle, correction_pr = preview("sitemap_repaired", corrected)
        if middle["issues"][key]["count"] or middle["issues"]["sitemap_not_in_robots"]["count"] != 1:
            raise ValueError("The real intermediate crawl did not unblock the robots repair.")
        if locs((work / "sitemap_repaired/sitemap.xml").read_bytes()) != sorted(expected):
            raise ValueError("The served sitemap does not exactly contain the measured eligible pages.")
        apply("sitemap_not_in_robots", middle)
        after, _ = preview("final", corrected, existing=correction_pr)
        result["final_sha"] = ref(corrected)
        if (any(after["issues"][k]["count"] for k in SYSTEM_KEYS)
                or declarations((work / "final/robots.txt").read_text(encoding="utf-8")) != [SITE + "sitemap.xml"]
                or (work / "sitemap_repaired/sitemap.xml").read_bytes() != (work / "final/sitemap.xml").read_bytes()):
            raise ValueError("The final sitemap/robots HTTP observations are not repaired.")
        old_robots = (work / "baseline/robots.txt").read_text(encoding="utf-8")
        new_robots = (work / "final/robots.txt").read_text(encoding="utf-8")
        if new_robots.replace("Sitemap: " + SITE + "sitemap.xml\n", "").strip() != old_robots.strip():
            raise ValueError("Unrelated robots policy changed.")
        bp, ap = live._html_pages(before), live._html_pages(after)
        preserved = batches.titles.PRESERVED + batches.BODY_FIELDS + ("title", "meta_description")
        if not bp or not bp.keys() <= ap.keys() or any(old.get(k) != ap[r].get(k) for r, old in bp.items() for k in preserved):
            raise ValueError("Previously observed HTML routes or metadata/content changed.")
        result.update(status="measured_observed_sitemap_lifecycle", observed_html_pages_before=len(bp),
            observed_html_pages_after=len(ap), sitemap_entries=len(expected), unchanged_observed_html=True,
            robots_policy_preserved=True, target_counts={k: [before["issues"][k]["count"], after["issues"][k]["count"]] for k in SYSTEM_KEYS})
    except Exception as exc:
        result.update(status="failed", error_type=type(exc).__name__)
        print(case, "failed", type(exc).__name__, flush=True)
    finally:
        m._github_api_put = original_put
        result["ai_budget"] = budget.summary()
        result["cleanup"] = live.close_prs(REPO, token, result["pull_requests"])
        result["prs_closed_unmerged"] = bool(result["pull_requests"]) and len(result["cleanup"]) == len(result["pull_requests"]) and all(
            p.get("state") == "closed" and p.get("merged") is False for p in result["cleanup"])
        result["main_unchanged"] = ref("main") == MAIN_SHA
        if not result["main_unchanged"] or budget.summary()["attempted"] or budget.summary()["denied"]:
            result["status"] = "unverified"
        if result["status"] == "measured_observed_sitemap_lifecycle" and not result["prs_closed_unmerged"]:
            result["status"] = "unverified"
        live.save(work / "cycle.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--case", choices=KEYS)
    args = parser.parse_args()
    args.workdir.mkdir(parents=True, exist_ok=True)
    if any(args.workdir.iterdir()):
        parser.error("Use an empty temporary directory, never customer data.")
    if not args.case:
        for case in KEYS:
            with (args.workdir / (case + ".log")).open("w", encoding="utf-8") as log:
                proc = subprocess.run([sys.executable, __file__, "--case", case, "--workdir", str(args.workdir / case)],
                    cwd=live.WEB_ROOT, stdout=log, stderr=subprocess.STDOUT, timeout=1800)
            result = live.load(args.workdir / case / "cycle.json")
            print(case, result["status"], flush=True)
            if proc.returncode:
                return 1
        return 0
    m, token = live._load_backend(args.workdir)
    budget = ClaudeBudget(0)
    budget.install(m)
    result = cycle(m, token, args.case, args.workdir, budget)
    return int(result["status"] != "measured_observed_sitemap_lifecycle" or not result["prs_closed_unmerged"])


if __name__ == "__main__":
    raise SystemExit(main())
