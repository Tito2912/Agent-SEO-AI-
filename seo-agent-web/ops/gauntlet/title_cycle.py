"""Bounded title and duplicate-description repairs on owned Hugo/Nuxt fixtures.

Duplicate groups are deliberately partial: only duplicate-a/b are selected. Raw
preview crawls stay noindex; the separate projection is not a production crawl.
"""

from __future__ import annotations

import argparse
import datetime as dt
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from threading import Lock
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ops.gauntlet import hreflang_cycle as previous  # noqa: E402
from ops.gauntlet.ai_budget import ClaudeBudget  # noqa: E402

live = previous.live
SEEDS = {
    "hugo": {"branch": "gauntlet-hugo-hreflang-ai-correction-20261004-033911",
             "sha": "6828ab00a3d93a276bb370bfde49812f2add0b0e"},
    "nuxt": {"branch": "gauntlet-nuxt-hreflang-ai-correction-20261004-034125",
             "sha": "15c30e72377955ce73ad9844d44540ef0d146246"},
}
KEYS = ("missing_title", "multiple_title_tags", "title_too_short", "duplicate_titles", "duplicate_meta_descriptions")
DUPLICATES = {"duplicate_titles", "duplicate_meta_descriptions"}
PAIR = {"/gauntlet/duplicate-a/", "/gauntlet/duplicate-b/"}
PRESERVED = ("canonical", "meta_robots", "x_robots_tag", "lang", "served_lang", "hreflang", "hreflang_raw",
             "h1", "h2", "og_type", "og_url", "og_image", "twitter_card", "twitter_image")


def preview_sitemap(url: str, *, timeout: int = 120) -> bytes:
    deadline = time.monotonic() + timeout
    while True:
        try:
            root = live.requests.get(url, timeout=15, allow_redirects=False)
            sitemap = live.requests.get(url + "sitemap.xml", timeout=15, allow_redirects=False)
            if (root.status_code == sitemap.status_code == 200
                    and "html" in root.headers.get("content-type", "").lower()
                    and "xml" in sitemap.headers.get("content-type", "").lower()):
                return sitemap.content
        except live.requests.RequestException:
            pass
        if time.monotonic() >= deadline:
            raise TimeoutError("The exact preview root and sitemap are not ready.")
        time.sleep(5)


def select_urls(m, before: dict, key: str) -> tuple[list[str], list[str]]:
    from backend import audit_dashboard as dash

    all_urls = sorted({u for k in m._length_family_keys(key)
                       for u in dash.extract_impacted_pages(k, before.get("issues", {}).get(k, {}))})
    selected = [u for u in all_urls if live._route(u) in PAIR] if key in DUPLICATES else all_urls
    if key in DUPLICATES and {live._route(u) for u in selected} != PAIR:
        raise ValueError("Both controlled duplicate pages must be observed before repair.")
    return selected, [u for u in all_urls if u not in selected]


def html_checks(before: dict, after: dict, scopes: dict, *, duplicate_routes: set[str] | None = None) -> list[dict]:
    expected_duplicates = PAIR if duplicate_routes is None else duplicate_routes
    bp, ap = live._html_pages(before), live._html_pages(after)
    checks, permitted = [], {}
    for key, scope in scopes.items():
        field = "meta_description" if key == "duplicate_meta_descriptions" else "title"
        lo, hi = (100, 160) if field == "meta_description" else (15, 70)
        count_field = field + "_tag_count"
        old_counts = Counter(str(p.get(field) or "").strip() for p in bp.values())
        new_counts = Counter(str(p.get(field) or "").strip() for p in ap.values())
        rows = []
        for url in scope["impacted_urls"]:
            route = live._route(url)
            permitted.setdefault(route, set()).add(field)
            old, new = bp.get(route), ap.get(route)
            row = {"route": route, "baseline_defect_observed": False, "after_correct": False}
            if old and new:
                was, now = (str(p.get(field) or "").strip() for p in (old, new))
                n0, n1 = old.get(count_field), new.get(count_field)
                counts_known = all(isinstance(n, int) and not isinstance(n, bool) for n in (n0, n1))
                defect = (old_counts[was] > 1 and bool(was) if key in DUPLICATES
                          else not was and n0 == 0 if key == "missing_title"
                          else counts_known and n0 > 1 if key == "multiple_title_tags"
                          else bool(was) and not lo <= len(was) <= hi)
                row.update(before_length=len(was), after_length=len(now), before_tags=n0, after_tags=n1,
                           baseline_defect_observed=bool(counts_known and defect),
                           after_correct=bool(counts_known and new.get("status_code") == 200 and n1 == 1
                                              and lo <= len(now) <= hi
                                              and (new_counts[now] == 1 and now != was if key in DUPLICATES else True)))
            rows.append(row)
        expected = len(scope["impacted_urls"])
        # Family-wide counts are not the selected scope: duplicate groups remain partial.
        positive = (expected > 0 and before.get("issues", {}).get(key, {}).get("count", 0) > 0
                    and len({r["route"] for r in rows}) == expected
                    and (key not in DUPLICATES or {r["route"] for r in rows} == expected_duplicates))
        checks.append({"family": key, "verdict": "resolved_on_selected_observed_html"
                       if positive and all(r["baseline_defect_observed"] and r["after_correct"] for r in rows)
                       else "unverified", "observations": rows})
    collateral = []
    for route, old in bp.items():
        new = ap.get(route)
        ok = bool(new and new.get("status_code") == 200 and all(old.get(k) == new.get(k) for k in PRESERVED))
        if new:
            for field in ("title", "meta_description"):
                if field not in permitted.get(route, set()):
                    ok = ok and old.get(field) == new.get(field) and old.get(field + "_tag_count") == new.get(field + "_tag_count")
                social = ("og_title", "twitter_title") if field == "title" else ("og_description", "twitter_description")
                for k in social:
                    coherent = (field in permitted.get(route, set()) and old.get(k) == old.get(field)
                                and new.get(k) == new.get(field))
                    ok = ok and (old.get(k) == new.get(k) or coherent)
        if not ok:
            collateral.append(route)
    checks.append({"family": "collateral_metadata", "verdict": "unchanged_or_coherent_social_updates"
                   if bp and not collateral and bp.keys() == ap.keys() else "unverified", "changed_routes": collateral})
    return checks


def cycle(m, token: str, stack: str, work: Path, budget: ClaudeBudget) -> dict:
    if stack not in SEEDS:
        raise ValueError("Only Hugo/Nuxt fixtures are allowed.")
    repo = f"noyaru-stack-{stack}"
    production = f"https://{repo}.netlify.app/"
    result = {"stack": stack, "repository": f"{live.OWNER}/{repo}", "pull_requests": [],
              "families": list(KEYS), "not_exercised": [], "repair_scopes": {},
              "measurement_mode": "counterfactual_fixture_preview_without_injected_header"}

    def ref(name):
        return m._github_api_get(m._github_ref_api_path(live.OWNER, repo, name), token=token)["object"]["sha"]

    def branch(name, sha):
        if not name.startswith(f"gauntlet-{stack}-titles-") or not m._github_branch_allowed(name):
            raise ValueError("Only dedicated fixture branches may be written.")
        m._github_api_post(m._github_api_path("repos", live.OWNER, repo, "git", "refs"), token=token,
                           json_body={"ref": "refs/heads/" + name, "sha": sha})

    def preview(kind, head):
        pr = m._ouvrir_pull_request(owner=live.OWNER, repo=repo, token=token, head=head, base="main", draft=True,
                                   title=f"QA titles Claude {stack}: {kind} (never merge)",
                                   body="Fixture-only bounded validation. Close without merging; duplicates outside the selected pair remain.")
        row = {"kind": kind, "number": pr["number"], "url": pr["html_url"]}
        result["pull_requests"].append(row)
        live.save(work / "cycle.json", result)
        print(f"[{stack}] Awaiting {kind} #{pr['number']}", flush=True)
        row["build"] = live.wait_build(m, repo, token, pr["number"])
        if row["build"]["head_sha"] != ref(head):
            raise ValueError("The built preview does not match its branch head.")
        url = f"https://deploy-preview-{pr['number']}--{repo}.netlify.app/"
        sitemap = preview_sitemap(url)
        live.crawl(stack, url, work / kind / "raw", 90, preview=True)
        (work / kind / "sitemap.xml").write_bytes(sitemap)
        return previous.rescore(work / kind / "raw/report.json", url, production, sitemap,
                                work / kind / "controlled")

    try:
        result["main_sha"] = ref("main")
        seed = SEEDS[stack]
        if ref(seed["branch"]) != seed["sha"]:
            raise ValueError("The validated seed head changed.")
        ancestry = m._github_api_get(m._github_api_path("repos", live.OWNER, repo, "compare",
                                                       result["main_sha"] + "..." + seed["sha"]), token=token)
        if ancestry["merge_base_commit"]["sha"] != result["main_sha"]:
            raise ValueError("The fixture main ancestry changed.")
        stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d-%H%M%S")
        baseline = f"gauntlet-{stack}-titles-baseline-{stamp}"
        branch(baseline, seed["sha"])
        result["baseline_branch"] = baseline
        before = preview("baseline", baseline)
        corrected = f"gauntlet-{stack}-titles-correction-{stamp}"
        branch(corrected, seed["sha"])
        result["correction_branch"] = corrected
        from backend import audit_dashboard as dash, repo_index

        tree = m._github_api_get(m._github_api_path("repos", live.OWNER, repo, "git", "trees", baseline),
                                token=token, params={"recursive": "1"})
        if tree.get("truncated"):
            raise ValueError("The fixture tree is incomplete.")
        paths = [r["path"] for r in tree["tree"] if r["type"] == "blob"]
        index, state = repo_index.build_repo_index(paths), {}
        run = {"results": []}
        for key in KEYS:
            selected, unselected = select_urls(m, before, key)
            if not selected:
                result["not_exercised"].append({"family": key, "reason": "absent_positive_control"})
                continue
            expected = {p for url in selected for p in repo_index.route_files(index, url)}
            if not expected or len(expected) != len(selected) or len(expected) > 6:
                raise ValueError("The selected repair scope is unresolved or too large.")
            prep = m._prepare_issue_fix(issue_key=key, issues=before["issues"], impacted=selected,
                                        all_paths=paths, site_name=urlsplit(production).netloc,
                                        owner=live.OWNER, repo_name=repo, branch=baseline,
                                        token=token, pages=before["pages"])
            if prep["refusal"]:
                raise ValueError("The selected fixture repair was refused.")
            capped = []
            applied = m._apply_prepared_issue_fix(
                owner=live.OWNER, repo_name=repo, branch=baseline, token=token, fix_branch=corrected,
                all_paths=paths, issue_key=key, issue_label=dash.ISSUE_CATALOG[key].label,
                impacted=selected, site_name=urlsplit(production).netloc, file_state=state,
                max_files=len(expected), prep=prep, pages=before["pages"], index=index,
                allow_ai_targeting=False, ecartes=capped)
            result["repair_scopes"][key] = {"impacted_urls": selected, "unselected_impacted_urls": unselected,
                "expected_files": sorted(expected), "patched": applied["patched"], "targets": applied["targets"],
                "ai_files": applied["ai_files"], "skipped": applied["skipped"], "capped_files": capped}
            live.save(work / "cycle.json", result)
            if (applied.get("fatal") or set(applied["targets"]) != expected
                    or set(applied["patched"]) != expected or applied["skipped"] or capped):
                raise ValueError("The corrector did not repair exactly all selected fixture files.")
            for active in m._length_family_keys(key):
                if before["issues"].get(active, {}).get("count"):
                    run["results"].append([active, "ok"])
        after = preview("correction", corrected)
        bp, ap = (work / kind / "controlled/report.json" for kind in ("baseline", "correction"))
        result["comparison"] = comparison = live.compare(bp, bp, ap, run)
        result["html_checks"] = checks = html_checks(before, after, result["repair_scopes"])
        result["main_unchanged"] = ref("main") == result["main_sha"]
        result["status"] = ("measured_selected_occurrences" if result["main_unchanged"] and comparison["comparable"]
                            and not comparison["increases"] and not comparison["new_issue_routes"]
                            and not comparison["missing_html_routes"] and not budget.summary()["exhausted"]
                            and all(r["verdict"] in ({"partial", "resolved_in_preview_scope"} if r["family"] in DUPLICATES
                                                     else {"resolved_in_preview_scope"}) for r in comparison["targeted_families"])
                            and all(r["verdict"] != "unverified" for r in checks) else "unverified")
    except Exception as exc:
        result["status"], result["error_type"] = "failed", type(exc).__name__
        print(f"[{stack}] Failed: {type(exc).__name__}", flush=True)
    finally:
        result["ai_budget"] = budget.summary()
        result["cleanup"] = live.close_prs(repo, token, result["pull_requests"])
        result["pull_requests_closed_without_merge"] = (bool(result["pull_requests"])
            and {r["number"] for r in result["cleanup"]} == {r["number"] for r in result["pull_requests"]}
            and all(r.get("state") == "closed" and r.get("merged") is False for r in result["cleanup"]))
        live.save(work / "cycle.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--stack", choices=SEEDS)
    parser.add_argument("--ai-max-calls", type=int, default=12)
    args = parser.parse_args()
    if not 0 <= args.ai_max_calls <= 12:
        parser.error("Claude request limit must be between 0 and 12 per stack.")
    args.workdir.mkdir(parents=True, exist_ok=True)
    if any(args.workdir.iterdir()):
        parser.error("Use an empty temporary directory, never client data.")
    if not args.stack:
        results = []
        for stack in SEEDS:
            with (args.workdir / (stack + ".log")).open("w", encoding="utf-8") as log:
                proc = subprocess.run([sys.executable, str(Path(__file__).resolve()), "--stack", stack,
                                       "--workdir", str(args.workdir / stack), "--ai-max-calls", str(args.ai_max_calls)],
                                      cwd=live.WEB_ROOT, stdout=log, stderr=subprocess.STDOUT, timeout=2700)
            results.append(live.load(args.workdir / stack / "cycle.json"))
            live.save(args.workdir / "cycles.json", {"cycles": results})
            print(stack, results[-1]["status"], flush=True)
            if proc.returncode:
                return 1
        return 0
    for stream in (sys.stdout, sys.stderr):
        stream.reconfigure(encoding="utf-8", errors="replace")
    m, token = live._load_backend(args.workdir)
    budget = ClaudeBudget(args.ai_max_calls)
    budget.install(m)
    usage, lock = [], Lock()
    original = m._noter_consommation_ia

    def record_usage(*, fournisseur, modele, data):
        values = (data or {}).get("usage") or {}
        row = {"provider": fournisseur, "model": modele,
               "input_tokens": values.get("input_tokens", 0), "output_tokens": values.get("output_tokens", 0),
               "cache_read_tokens": values.get("cache_read_input_tokens", 0),
               "cache_write_tokens": values.get("cache_creation_input_tokens", 0)}
        row["estimated_cost_usd"] = m._cout_ia_usd(modele, entree=row["input_tokens"], sortie=row["output_tokens"],
                                                cache_lu=row["cache_read_tokens"], cache_ecrit=row["cache_write_tokens"])
        with lock:
            usage.append(row)
        original(fournisseur=fournisseur, modele=modele, data=data)

    m._noter_consommation_ia = record_usage
    result = cycle(m, token, args.stack, args.workdir, budget)
    live.save(args.workdir / "usage.json", {"calls": usage, "budget": budget.summary(),
                                          "estimated_cost_is_provider_invoice": False})
    return int(result["status"] != "measured_selected_occurrences" or not result["pull_requests_closed_without_merge"])


if __name__ == "__main__":
    raise SystemExit(main())
