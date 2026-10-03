"""Build and recrawl fixture corrections, with an unchanged preview as positive control.

Run from anywhere with the application's Python environment. Only the nine fixture repos
are allowed. Draft PRs are always closed without merging, including on a failed build.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit

WEB_ROOT = Path(__file__).resolve().parents[2]
ROOT = WEB_ROOT.parent
OWNER = "pployeraffiliation-a11y"
AUDITOR = ROOT / "skills/public/seo-autopilot/scripts/seo_audit.py"
sys.path.insert(0, str(WEB_ROOT))

import requests  # noqa: E402

from ops.gauntlet.verify_correction import STACKS, base, comptes  # noqa: E402


def save(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _route(url: str) -> str:
    parsed = urlsplit(url)
    return (parsed.path or "/") + ("?" + parsed.query if parsed.query else "")


def _html_pages(report: dict) -> dict[str, dict]:
    return {
        _route(row.get("final_url") or row["url"]): row
        for row in report.get("pages", [])
        if row.get("url") and not row.get("error")
        and isinstance(row.get("status_code"), int) and 200 <= row["status_code"] < 300
        and "html" in str(row.get("content_type") or "").lower()
    }


def _noindex(value: str | None) -> bool:
    return "noindex" in str(value or "").lower().replace(" ", "").split(",")


def _impacted_routes(report: dict, family: str) -> set[str]:
    from backend.audit_dashboard import extract_impacted_pages

    urls = set()
    for key, block in report.get("issues", {}).items():
        if base(key) == family:
            urls.update(extract_impacted_pages(key, block))
    return {_route(url) for url in urls}


def html_checks(production: dict, baseline: dict, corrected: dict, families: set[str]) -> list[dict]:
    """Observe literal defects that the preview's injected noindex disables in the scorer."""
    pp, bp, ap = map(_html_pages, (production, baseline, corrected))
    production_host = urlsplit(production.get("meta", {}).get("base_url", "")).netloc
    preview_host = urlsplit(corrected.get("meta", {}).get("base_url", "")).netloc
    checks = []
    for family in sorted(families & {"served_html_lang_mismatch", "canonical_points_to_4xx",
                                     "https_page_has_internal_links_to_http"}):
        routes = _impacted_routes(production, family)
        expected_count = max((int(v.get("count") or 0) for k, v in production.get("issues", {}).items()
                              if base(k) == family and isinstance(v, dict)), default=0)
        observations = []
        for route in sorted(routes):
            prod, old, new = pp.get(route), bp.get(route), ap.get(route)
            row = {"route": route, "baseline_defect_observed": False, "after_correct": None}
            if prod and old and new:
                if family == "served_html_lang_mismatch":
                    codes = {code.lower().split("-", 1)[0]
                             for code, href in (prod.get("hreflang") or {}).items()
                             if code != "x-default" and href == (prod.get("final_url") or prod["url"])}
                    if len(codes) == 1:
                        code = next(iter(codes))
                        row["expected_language"] = code
                        served = str(old.get("served_lang") or "").lower().split("-", 1)[0]
                        new_served = str(new.get("served_lang") or "").lower().split("-", 1)[0]
                        row["baseline_defect_observed"] = bool(served and served != code)
                        row["after_correct"] = bool(new_served == code and any(
                            c.lower().split("-", 1)[0] == code and _route(h) == route
                            and urlsplit(h).netloc in {production_host, preview_host}
                            for c, h in (new.get("hreflang") or {}).items() if c != "x-default"))
                elif family == "canonical_points_to_4xx":
                    original = str(prod.get("canonical") or "")
                    broken = {p.get("final_url") or p.get("url")
                              for p in production.get("pages", [])
                              if isinstance(p.get("status_code"), int) and 400 <= p["status_code"] < 500}
                    canonical = str(new.get("canonical") or "")
                    parsed = urlsplit(canonical)
                    row["baseline_defect_observed"] = bool(original in broken and old.get("canonical") == original)
                    row["after_correct"] = bool(canonical and canonical != original
                                                and parsed.scheme == "https"
                                                and parsed.netloc in {production_host, preview_host}
                                                and _route(canonical) in ap)
                else:
                    links = lambda p: set(p.get("internal_links", []) + p.get("external_links", []))  # noqa: E731
                    broken = {url for url in links(prod) if urlsplit(url).scheme == "http"
                              and urlsplit(url).netloc == production_host}
                    row["baseline_defect_observed"] = bool(broken and broken <= links(old))
                    row["after_correct"] = bool(broken and not (broken & links(new)) and all(
                        "https://" + url.removeprefix("http://") in links(new) for url in broken))
                if (_noindex(new.get("meta_robots")) and not _noindex(old.get("meta_robots"))) or (
                    _noindex(new.get("x_robots_tag")) and not _noindex(old.get("x_robots_tag"))
                ):
                    row["after_correct"] = None
            observations.append(row)
        complete = bool(expected_count and len(observations) >= expected_count and all(
            p["baseline_defect_observed"] and p["after_correct"] is not None for p in observations))
        verdict = ("unverified" if not complete else "resolved_on_observed_html"
                   if all(p["after_correct"] for p in observations) else "still_present")
        checks.append({"family": family, "verdict": verdict, "observations": observations})
    return checks


def compare(production: Path, baseline: Path, corrected: Path, run: dict) -> dict:
    from backend import audit_dashboard as dash

    prod, before, after = load(production), load(baseline), load(corrected)
    pc, bc, ac = comptes(str(production)), comptes(str(baseline)), comptes(str(corrected))
    bp, ap = _html_pages(before), _html_pages(after)
    reasons = []
    bm, am = before.get("meta", {}), after.get("meta", {})
    if not bp or len(ap) < max(1, int(len(bp) * 0.66)):
        reasons.append("too_few_successful_html_pages")
    if not dash._comparable_meta(am, bm) or am.get("max_pages") != bm.get("max_pages"):
        reasons.append("different_crawl_settings")
    for meta in (bm, am):
        if meta.get("stopped_on_time_budget") or meta.get("urls_uncrawled"):
            reasons.append("incomplete_crawl")
        if (meta.get("blocked_by_host") or {}).get("count"):
            reasons.append("blocked_by_host")
    targeted = sorted({base(row[0]) for row in run.get("results", [])
                       if row[1] in {"ok", "DEJA CORRIGE", "AUCUN PATCH", "AUCUNE CIBLE"}})
    rows = []
    for family in targeted:
        was, now = bc.get(family, 0), ac.get(family, 0)
        why = list(dict.fromkeys(reasons))
        if not was:
            why.append("defect_absent_from_unchanged_preview")
        routes = _impacted_routes(before, family) & bp.keys()
        if routes - ap.keys():
            why.append("affected_html_pages_not_successfully_revisited")
        for route in routes & ap.keys():
            old, new = bp[route], ap[route]
            if (_noindex(new.get("meta_robots")) and not _noindex(old.get("meta_robots"))) or (
                _noindex(new.get("x_robots_tag")) and not _noindex(old.get("x_robots_tag"))
            ):
                why.append("added_noindex_on_affected_page")
                break
        verdict = ("unverified" if why else "resolved_in_preview_scope" if now == 0
                   else "partial" if now < was else "still_present")
        rows.append({"family": family, "production_before": pc.get(family, 0),
                     "preview_before": was, "preview_after": now, "verdict": verdict,
                     "reasons": list(dict.fromkeys(why))})
    visible = {base(k) for k in dash.ISSUE_CATALOG
               if k not in dash.NON_ISSUE_KEYS and not dash.is_delta_issue_key(k)}
    increases = [{"family": key, "before": bc.get(key, 0), "after": value}
                 for key, value in sorted(ac.items())
                 if key in visible and value > bc.get(key, 0)]
    introduced = []
    for family in sorted(visible & ac.keys()):
        added = _impacted_routes(after, family) - _impacted_routes(before, family)
        if added:
            introduced.append({"family": family, "routes": sorted(added)})
    direct = html_checks(prod, before, after, set(targeted))
    if reasons:
        for check in direct:
            check["verdict"] = "unverified"
    return {"comparable": not reasons, "reasons": list(dict.fromkeys(reasons)),
            "before_html_pages": len(bp), "after_html_pages": len(ap),
            "missing_html_routes": sorted(bp.keys() - ap.keys()),
            "targeted_families": rows, "html_field_checks": direct, "increases": increases,
            "new_issue_routes": introduced}


def _load_backend(workdir: Path):
    # Never inherit the production database, even if credentials live in the same env file.
    os.environ.update({"DATABASE_URL": "sqlite:///" + (workdir / "bench.db").as_posix(),
                       "SEO_AGENT_DATA_DIR": str(workdir / "data"),
                       "SEO_AGENT_RUNS_DIR": str(workdir / "runs"),
                       "SEO_AGENT_DISABLE_WORKER": "true",
                       "SEO_AGENT_SECRET_KEY": "fixture-only-session-secret",
                       "SEO_CORRECTION_AI_PROVIDER": "anthropic", "OPENAI_API_KEY": ""})
    from backend import app

    app._load_env_file(ROOT / "seo-agent-web.env", override=False)
    if not os.environ.get("FIXTURE_TOKEN"):
        raise RuntimeError("FIXTURE_TOKEN is missing; never paste a token into chat.")
    return app, os.environ["FIXTURE_TOKEN"]


def crawl(stack: str, url: str, out: Path, max_pages: int, *, preview: bool) -> None:
    out.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, str(AUDITOR), url, "--max-pages", str(max_pages),
           "--workers", "3", "--check-resources", "--output-dir", str(out)]
    if preview:
        cmd += ["--canonical-host-alias", f"noyaru-stack-{stack}.netlify.app"]
    with (out / "crawl.log").open("w", encoding="utf-8") as log:
        subprocess.run(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                       check=True, timeout=600)


def repair(stack: str, workdir: Path, *, branch: str = "", families: str = "",
           max_files: int = 40) -> dict:
    env = dict(os.environ, GAUNTLET_STACK=stack, GAUNTLET_WORKDIR=str(workdir),
               GAUNTLET_FREE="" if families else "1", GAUNTLET_ONLY=families,
               GAUNTLET_BRANCH=branch, GAUNTLET_MAX_FILES=str(max_files))
    with (workdir / ("claude.log" if families else "mechanical.log")).open(
        "w", encoding="utf-8"
    ) as log:
        subprocess.run([sys.executable, str(WEB_ROOT / "ops/gauntlet/run.py")], cwd=ROOT,
                       env=env, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=1800)
    return load(workdir / "gauntlet_run.json")


def preview_status(statuses: list[dict]) -> str:
    relevant = [s for s in statuses if "netlify" in str(s.get("context", "")).lower()
                and "deploy-preview" in str(s.get("context", "")).lower()]
    if not relevant:
        return "pending"
    latest = max(relevant, key=lambda row: row.get("id", 0))
    return str(latest.get("state", "pending"))


def wait_build(m, repo: str, token: str, number: int, *, timeout: int = 600) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        pr = m._github_api_get(m._github_api_path("repos", OWNER, repo, "pulls", str(number)),
                              token=token)
        sha = pr["head"]["sha"]
        status = m._github_api_get(m._github_api_path("repos", OWNER, repo, "commits", sha, "status"),
                                  token=token)
        verdict = preview_status(status.get("statuses", []))
        if verdict == "success":
            return {"head_sha": sha, "deploy_preview_status": verdict}
        if verdict in {"failure", "error"}:
            raise RuntimeError(f"Netlify build failed for {repo} PR #{number} ({sha}).")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Netlify preview timeout for {repo} PR #{number}.")
        time.sleep(10)


def close_prs(repo: str, token: str, prs: list[dict]) -> list[dict]:
    outcomes = []
    for pr in prs:
        number = pr["number"]
        try:
            response = requests.patch(f"https://api.github.com/repos/{OWNER}/{repo}/pulls/{number}",
                                      headers={"Authorization": "Bearer " + token,
                                               "Accept": "application/vnd.github+json"},
                                      json={"state": "closed"}, timeout=30)
            response.raise_for_status()
            data = response.json()
            outcomes.append({"number": number, "state": data["state"],
                             "merged": bool(data.get("merged"))})
        except Exception as exc:
            outcomes.append({"number": number, "cleanup_error": type(exc).__name__})
    return outcomes


def cycle(m, token: str, stack: str, root: Path, *, max_pages: int = 90,
          reuse: bool = False, ai_families: str = "", ai_max_files: int = 2) -> dict:
    if stack not in STACKS:
        raise ValueError("Only the allowlisted fixture stacks can be tested.")
    repo = f"noyaru-stack-{stack}"
    workdir = root / stack
    workdir.mkdir(parents=True, exist_ok=True)
    result = {"stack": stack, "repository": f"{OWNER}/{repo}", "max_pages": max_pages,
              "ai_families_requested": [key for key in ai_families.split(",") if key],
              "ai_max_files_per_family": ai_max_files,
              "started_at": dt.datetime.now(dt.UTC).isoformat(), "pull_requests": []}
    try:
        main = m._github_api_get(m._github_ref_api_path(OWNER, repo, "main"), token=token)
        result["main_sha"] = main["object"]["sha"]
        before = workdir / "before/report.json"
        if not reuse or not before.exists():
            print(f"[{stack}] Production reference", flush=True)
            crawl(stack, f"https://{repo}.netlify.app/", before.parent, max_pages, preview=False)
        if load(before).get("meta", {}).get("max_pages") != max_pages:
            raise ValueError("Reference max_pages differs from verification max_pages.")
        existing = workdir / "gauntlet_run.json"
        if reuse and existing.exists():
            run = load(existing)
        else:
            print(f"[{stack}] Mechanical corrections", flush=True)
            run = repair(stack, workdir)
        branch = run["branch"]
        if not branch.startswith(f"gauntlet-{stack}-") or not m._github_branch_allowed(branch):
            raise ValueError("Corrections must use a fixture branch, never main.")
        ancestry = m._github_api_get(
            m._github_api_path("repos", OWNER, repo, "compare", result["main_sha"] + "..." + branch),
            token=token)
        if (ancestry.get("merge_base_commit") or {}).get("sha") != result["main_sha"]:
            raise ValueError("The reused correction branch is based on an older fixture main.")
        if ai_families:
            print(f"[{stack}] Bounded Claude repairs ({ai_max_files} files/family)", flush=True)
            run = repair(stack, workdir, branch=branch, families=ai_families, max_files=ai_max_files)
        result["repairs"] = run
        stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d-%H%M%S")
        baseline_branch = f"gauntlet-{stack}-baseline-{stamp}"
        m._github_api_post(m._github_api_path("repos", OWNER, repo, "git", "refs"), token=token,
                           json_body={"ref": "refs/heads/" + baseline_branch,
                                      "sha": result["main_sha"]})
        m._github_api_put(m._github_content_api_path(OWNER, repo, "_qa-baseline.txt"), token=token,
                          json_body={"message": "test: unchanged SEO preview baseline",
                                     "branch": baseline_branch, "content": base64.b64encode(
                                         b"Fixture QA baseline only. Never merge this branch.\n").decode()})
        for kind, head in (("baseline", baseline_branch), ("correction", branch)):
            pr = m._ouvrir_pull_request(
                owner=OWNER, repo=repo, token=token, title=f"QA {stack}: {kind} (never merge)",
                body="Fixture validation only. Close without merging; main must retain its defects.",
                head=head, base="main", draft=True)
            result["pull_requests"].append({"kind": kind, "number": pr["number"], "url": pr["html_url"]})
            save(workdir / "cycle.json", result)
        for pr in result["pull_requests"]:
            print(f"[{stack}] Awaiting {pr['kind']} preview #{pr['number']}", flush=True)
            pr["build"] = wait_build(m, repo, token, pr["number"])
            url = f"https://deploy-preview-{pr['number']}--{repo}.netlify.app/"
            crawl(stack, url, workdir / pr["kind"], max_pages, preview=True)
        main_after = m._github_api_get(m._github_ref_api_path(OWNER, repo, "main"), token=token)
        if main_after["object"]["sha"] != result["main_sha"]:
            raise RuntimeError("Fixture main changed during validation; comparison invalid.")
        result["comparison"] = compare(before, workdir / "baseline/report.json",
                                       workdir / "correction/report.json", run)
        comparison = result["comparison"]
        result["status"] = ("unverified" if not comparison["comparable"] else "regression_detected"
                            if comparison["increases"] or comparison["new_issue_routes"] else "measured")
    except Exception as exc:
        result["status"] = "failed"
        result["error_type"] = type(exc).__name__
        # No exception bodies here: SDK and HTTP errors may contain request credentials.
        print(f"[{stack}] Failed: {type(exc).__name__}; inspect the scoped crawl/repair log.", flush=True)
    finally:
        result["cleanup"] = close_prs(repo, token, result["pull_requests"])
        result["pull_requests_closed_without_merge"] = all(
            row.get("state") == "closed" and row.get("merged") is False
            for row in result["cleanup"])
        result["finished_at"] = dt.datetime.now(dt.UTC).isoformat()
        save(workdir / "cycle.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--stacks", nargs="+", choices=STACKS, default=["astro", "next-app"])
    parser.add_argument("--max-pages", type=int, default=90)
    parser.add_argument("--reuse", action="store_true", help="Reuse this workdir's reference/fix branch.")
    parser.add_argument("--ai-families", default="", help="Explicit comma-separated families; Claude only.")
    parser.add_argument("--ai-max-files", type=int, default=2)
    args = parser.parse_args()
    if args.max_pages < 1 or not 1 <= args.ai_max_files <= 5:
        parser.error("max-pages must be positive; ai-max-files must be between 1 and 5.")
    root = args.workdir.resolve()
    root.mkdir(parents=True, exist_ok=True)
    m, token = _load_backend(root)
    results = []
    for stack in args.stacks:
        row = cycle(m, token, stack, root, max_pages=args.max_pages, reuse=args.reuse,
                    ai_families=args.ai_families, ai_max_files=args.ai_max_files)
        results.append(row)
        save(root / "cycles.json", {"cycles": results})
        print(json.dumps({"stack": stack, "status": row["status"],
                          "cleanup_ok": row["pull_requests_closed_without_merge"]}), flush=True)
    return int(any(r["status"] != "measured" or not r["pull_requests_closed_without_merge"]
                   for r in results))


if __name__ == "__main__":
    raise SystemExit(main())
