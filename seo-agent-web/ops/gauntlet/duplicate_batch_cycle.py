"""Real Claude repairs on eight owned fixture pages, in capped batches of six/two.

Both duplicate fields are exercised. Other group members remain explicitly outside
this scope. Preview projection is counterfactual, never proof of indexing or deployment.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import subprocess
import sys
from pathlib import Path
from threading import Lock
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ops.gauntlet import title_cycle as titles  # noqa: E402
from ops.gauntlet.ai_budget import ClaudeBudget  # noqa: E402
from ops.gauntlet.source_capture import FixtureSourceCapture  # noqa: E402

live = titles.live
SEEDS = {
    "hugo": {"branch": "gauntlet-hugo-titles-correction-20261004-075751",
             "sha": "929f8d04e8281d82b7ac7b8e68afb1b4b255db41"},
    "nuxt": {"branch": "gauntlet-nuxt-titles-correction-20261004-080052",
             "sha": "b94d21a119464df261210672b92da01e28d01607"},
}
ROUTES = {f"/gauntlet/{name}/" for name in (
    "link-http", "missing-alt", "missing-h1", "mixed-css", "mixed-image",
    "mixed-js", "multiple-h1", "redirected-css")}
KEYS = ("duplicate_titles", "duplicate_meta_descriptions")
BATCH_SIZE = 6
BODY_FIELDS = ("h1_tag_count", "h2_tag_count", "internal_links", "internal_link_items",
               "external_links", "internal_links_dofollow", "internal_links_nofollow",
               "external_links_dofollow", "external_links_nofollow", "image_urls",
               "images_missing_alt", "images_total", "image_srcs_missing_alt",
               "meta_refresh", "meta_refresh_tag_count", "meta_viewport", "meta_viewport_tag_count",
               "ld_json_blocks", "schema_types", "schema_org_errors", "links_without_anchor_text")


def select_urls(before: dict, key: str, production: str) -> tuple[list[str], list[str]]:
    from backend import audit_dashboard as dash

    if key not in KEYS:
        raise ValueError("Only the two duplicate families are allowed.")
    block = before.get("issues", {}).get(key, {})
    all_urls = sorted(dash.extract_impacted_pages(key, block))
    selected = [url for url in all_urls if live._route(url) in ROUTES]
    pages = live._html_pages(before)
    field = "title" if key == "duplicate_titles" else "meta_description"
    values = set()
    for url in selected:
        page = pages.get(live._route(url)) or {}
        if (urlsplit(url).scheme != "https" or urlsplit(url).netloc != urlsplit(production).netloc
                or page.get("status_code") != 200 or page.get("blocked_by_host")
                or page.get("canonical") != url or (page.get("final_url") or page.get("url")) != url
                or type(page.get(field + "_tag_count")) is not int or page.get(field + "_tag_count") != 1
                or any("noindex" in str(page.get(k) or "").lower() for k in ("meta_robots", "x_robots_tag"))):
            raise ValueError("The selected duplicate page is not an eligible positive control.")
        values.add(str(page.get(field) or "").strip())
    if (len(selected) != len(ROUTES) or {live._route(url) for url in selected} != ROUTES
            or len(values) != 1 or not all(values) or block.get("count", 0) < len(ROUTES)):
        raise ValueError("The eight-page duplicate group is not completely observed.")
    return selected, [url for url in all_urls if url not in selected]


def source_checks(m, state: dict, paths: list[str], key: str) -> dict:
    kind = "title" if key == "duplicate_titles" else "description"
    lo, hi = (15, 70) if kind == "title" else (100, 160)
    values, rows = [], []
    for path in sorted(paths):
        raw = state.get(path, {}).get("content", "")
        found = m._find_head_text_value(raw, kind)
        if not found or m._refus_de_format(path, raw):
            raise ValueError("A committed fixture value is missing or its source is invalid.")
        literal, value = found
        value = (m.html.unescape(value) if literal.lstrip().startswith("<") else m._js_unescape(value)).strip()
        if not lo <= len(value) <= hi:
            raise ValueError("A committed fixture value is outside crawler bounds.")
        values.append(value)
        rows.append({"path": path, "length": len(value), "blob_sha": state[path]["sha"],
                     "source_sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest()})
    if len(values) != len(set(values)):
        raise ValueError("A fixture value collides with a preceding batch.")
    return {"unique_across_completed_batches": bool(rows), "sources": rows}


def body_checks(before: dict, after: dict) -> dict:
    bp, ap = live._html_pages(before), live._html_pages(after)
    changes = [{"route": route, "fields": [k for k in BODY_FIELDS if old.get(k) != ap.get(route, {}).get(k)]}
               for route, old in bp.items()]
    changes = [row for row in changes if row["fields"]]
    return {"family": "collateral_observed_content", "fields": list(BODY_FIELDS),
            "verdict": "unchanged" if bp and bp.keys() == ap.keys() and not changes else "unverified",
            "changes": changes}


def apply_family(m, token, stack, before, production, baseline, corrected, paths, index, state, key, scope):
    from backend import audit_dashboard as dash, repo_index

    remaining = list(scope["impacted_urls"])
    done = set()
    for number in (1, 2):
        expected = {p for url in remaining for p in repo_index.route_files(index, url)}
        prep = m._prepare_issue_fix(issue_key=key, issues=before["issues"], impacted=remaining,
                                    all_paths=paths, site_name=urlsplit(production).netloc, owner=live.OWNER,
                                    repo_name=f"noyaru-stack-{stack}", branch=baseline, token=token, pages=before["pages"])
        if prep["refusal"]:
            raise ValueError("The selected fixture repair was refused.")
        excluded = []
        applied = m._apply_prepared_issue_fix(
            owner=live.OWNER, repo_name=f"noyaru-stack-{stack}", branch=baseline, token=token,
            fix_branch=corrected, all_paths=paths, issue_key=key, issue_label=dash.ISSUE_CATALOG[key].label,
            impacted=remaining, site_name=urlsplit(production).netloc, file_state=state, max_files=BATCH_SIZE,
            prep=prep, pages=before["pages"], index=index, allow_ai_targeting=False, ecartes=excluded)
        patched, deferred = set(applied["patched"]), set(excluded)
        row = {"number": number, "max_files": BATCH_SIZE, "input_urls": remaining,
               "patched": applied["patched"], "targets": applied["targets"], "ai_files": applied["ai_files"],
               "skipped": applied["skipped"], "deferred_files": excluded}
        scope["batches"].append(row)
        if (applied.get("fatal") or applied.get("error") or applied["skipped"] or applied["config_changes"]
                or len(patched) != min(BATCH_SIZE, len(expected)) or len(applied["patched"]) != len(patched)
                or set(applied["targets"]) != patched or set(applied["ai_files"]) != patched
                or patched & deferred or patched | deferred != expected or done & patched):
            raise ValueError("The capped batch did not partition exactly its selected fixture files.")
        done.update(patched)
        row["source_checks"] = source_checks(m, state, sorted(done), key)
        remaining = [url for url in remaining if set(repo_index.route_files(index, url)) <= deferred]
        print(f"[{stack}] {key} batch {number}: {len(patched)} patched, {len(deferred)} deferred", flush=True)
    if remaining or done != set(scope["expected_files"]):
        raise ValueError("The second batch did not finish all eight selected pages.")


def cycle(m, token: str, stack: str, work: Path, budget: ClaudeBudget) -> dict:
    if stack not in SEEDS:
        raise ValueError("Only owned Hugo/Nuxt fixtures are allowed.")
    if not 0 <= budget.limit <= 24:
        raise ValueError("The fixture budget must be between zero and 24 calls.")
    repo = f"noyaru-stack-{stack}"
    production = f"https://{repo}.netlify.app/"
    result = {"stack": stack, "repository": f"{live.OWNER}/{repo}", "pull_requests": [],
              "repair_scopes": {}, "selected_routes": sorted(ROUTES), "max_files_per_batch": BATCH_SIZE,
              "measurement_mode": "counterfactual_fixture_preview_without_injected_header",
              "all_group_occurrences_certified": False, "stage": "seed_identity"}
    original_put = m._github_api_put
    writable_branch, allowed_paths, writes = "", set(), []

    def guarded_put(path, **kw):
        allowed = {m._github_content_api_path(live.OWNER, repo, p): p for p in allowed_paths}
        if (not writable_branch or path not in allowed or kw.get("json_body", {}).get("branch") != writable_branch
                or len(writes) >= len(ROUTES) * len(KEYS)):
            raise ValueError("Only sixteen selected fixture content writes are permitted.")
        reply = original_put(path, **kw)
        writes.append(allowed[path])
        return reply

    def ref(name):
        return m._github_api_get(m._github_ref_api_path(live.OWNER, repo, name), token=token)["object"]["sha"]

    def branch(name, sha):
        if not name.startswith(f"gauntlet-{stack}-duplicate-batches-") or not m._github_branch_allowed(name):
            raise ValueError("Only dedicated fixture branches may be created.")
        m._github_api_post(m._github_api_path("repos", live.OWNER, repo, "git", "refs"), token=token,
                           json_body={"ref": "refs/heads/" + name, "sha": sha})

    def preview(kind, head):
        pr = m._ouvrir_pull_request(owner=live.OWNER, repo=repo, token=token, head=head, base="main", draft=True,
                                   title=f"QA duplicate batches Claude {stack}: {kind} (never merge)",
                                   body="Fixture-only eight-page validation in batches of six/two. Close without merging.")
        row = {"kind": kind, "number": pr["number"], "url": pr["html_url"]}
        result["pull_requests"].append(row)
        live.save(work / "cycle.json", result)
        print(f"[{stack}] Awaiting {kind} #{pr['number']}", flush=True)
        row["build"] = live.wait_build(m, repo, token, pr["number"])
        if row["build"]["head_sha"] != ref(head):
            raise ValueError("The preview does not match its exact branch head.")
        url = f"https://deploy-preview-{pr['number']}--{repo}.netlify.app/"
        sitemap = titles.preview_sitemap(url)
        live.crawl(stack, url, work / kind / "raw", 90, preview=True)
        (work / kind / "sitemap.xml").write_bytes(sitemap)
        return titles.previous.rescore(work / kind / "raw/report.json", url, production, sitemap,
                                       work / kind / "controlled")

    m._github_api_put = guarded_put
    try:
        seed = SEEDS[stack]
        result["main_sha"] = ref("main")
        if ref(seed["branch"]) != seed["sha"]:
            raise ValueError("The validated seed head changed.")
        ancestry = m._github_api_get(m._github_api_path("repos", live.OWNER, repo, "compare",
                                                       result["main_sha"] + "..." + seed["sha"]), token=token)
        if ancestry["merge_base_commit"]["sha"] != result["main_sha"]:
            raise ValueError("The fixture main ancestry changed.")
        stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d-%H%M%S")
        baseline = f"gauntlet-{stack}-duplicate-batches-baseline-{stamp}"
        corrected = f"gauntlet-{stack}-duplicate-batches-correction-{stamp}"
        branch(baseline, seed["sha"])
        result["baseline_branch"], result["correction_branch"] = baseline, corrected
        result["stage"] = "baseline_preview"
        before = preview("baseline", baseline)
        from backend import repo_index

        tree = m._github_api_get(m._github_api_path("repos", live.OWNER, repo, "git", "trees", baseline),
                                token=token, params={"recursive": "1"})
        if tree.get("truncated"):
            raise ValueError("The fixture tree is incomplete.")
        paths = [r["path"] for r in tree["tree"] if r["type"] == "blob"]
        index, state = repo_index.build_repo_index(paths), {}
        result["stage"] = "positive_control_and_routes"
        allowed_paths = {(f"content{route.rstrip('/')}.md" if stack == "hugo"
                          else f"pages{route.rstrip('/')}.vue") for route in ROUTES}
        for key in KEYS:
            selected, unselected = select_urls(before, key, production)
            if any(set(repo_index.route_files(index, url)) != {
                    f"content{live._route(url).rstrip('/')}.md" if stack == "hugo"
                    else f"pages{live._route(url).rstrip('/')}.vue"} for url in selected):
                raise ValueError("The eight routes do not map exactly to their allowed source files.")
            result["repair_scopes"][key] = {"impacted_urls": selected, "unselected_impacted_urls": unselected,
                                            "expected_files": sorted(allowed_paths), "batches": []}
        branch(corrected, seed["sha"])
        writable_branch = corrected
        with FixtureSourceCapture(m, work / "source_diagnostics", stack=stack, paths=allowed_paths):
            for key, scope in result["repair_scopes"].items():
                result["stage"] = key
                apply_family(m, token, stack, before, production, baseline, corrected, paths, index, state, key, scope)
                live.save(work / "cycle.json", result)
        result["final_sources"] = source_checks(m, state, sorted(allowed_paths), KEYS[-1])["sources"]
        result["file_writes"] = writes
        result["stage"] = "correction_preview"
        after = preview("correction", corrected)
        bp, ap = (work / kind / "controlled/report.json" for kind in ("baseline", "correction"))
        result["comparison"] = comp = live.compare(bp, bp, ap, {"results": [[key, "ok"] for key in KEYS]})
        result["html_checks"] = checks = titles.html_checks(before, after, result["repair_scopes"], duplicate_routes=ROUTES)
        checks.append(body_checks(before, after))
        result["main_unchanged"] = ref("main") == result["main_sha"]
        result["stage"] = "final_measurement"
        result["status"] = ("measured_selected_occurrences" if result["main_unchanged"] and comp["comparable"]
                            and not comp["increases"] and not comp["new_issue_routes"] and not comp["missing_html_routes"]
                            and len(writes) == len(ROUTES) * len(KEYS) and not budget.summary()["exhausted"]
                            and len(comp["targeted_families"]) == len(KEYS)
                            and all(r["verdict"] in {"partial", "resolved_in_preview_scope"} for r in comp["targeted_families"])
                            and all(r["verdict"] != "unverified" for r in checks) else "unverified")
    except Exception as exc:
        result["status"], result["error_type"] = "failed", type(exc).__name__
        print(f"[{stack}] Failed: {type(exc).__name__}", flush=True)
    finally:
        m._github_api_put = original_put
        result["ai_budget"], result["file_writes"] = budget.summary(), writes
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
    parser.add_argument("--ai-max-calls", type=int, default=24)
    args = parser.parse_args()
    if not 0 <= args.ai_max_calls <= 24:
        parser.error("Claude request limit must be between zero and 24 per stack.")
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
