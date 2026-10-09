"""Bounded Claude checks for malformed hreflang codes and missing x-default.

Only the owned Hugo/Nuxt fixtures are allowed. Raw preview reports remain intact;
separately labelled counterfactual scores never certify production indexability.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import subprocess
import sys
from pathlib import Path
from threading import Lock
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ops.gauntlet import hreflang_cycle as previous  # noqa: E402
from ops.gauntlet.ai_budget import ClaudeBudget  # noqa: E402

live = previous.live
SEEDS = {
    "hugo": {"branch": "gauntlet-hugo-hreflang-correction-20261003-180335",
             "sha": "b58d1761fce15f579920e2ac13c055feff1e1ba2"},
    "nuxt": {"branch": "gauntlet-nuxt-hreflang-correction-20261003-180538",
             "sha": "0f68d1eaa85e210b50e91a92321aa00ddc5e606d"},
}
KEYS = ("hreflang_annotation_invalid", "x_default_hreflang_missing")
PRESERVED = ("title", "meta_description", "meta_description_tag_count", "canonical", "lang", "served_lang",
             "meta_robots", "x_robots_tag", "og_title", "og_description", "og_image", "og_url", "og_type",
             "twitter_card", "twitter_title", "twitter_description", "twitter_image", "h1", "h2")


def remove_control_default(m, content: str, stack: str, expected: str) -> str:
    candidates = []
    for line in content.splitlines(keepends=True):
        stripped = line.rstrip("\r\n")
        if stack == "hugo":
            match = m._HREFLANG_LIEN_RE.fullmatch(stripped)
            if match and match.group("code") == "x-default" and match.group("href") == expected:
                candidates.append(line)
        elif stack == "nuxt":
            match = m._HREFLANG_OBJECT_LINE_RE.fullmatch(stripped)
            if match:
                fields = m._HREFLANG_OBJECT_FIELD_RE.finditer(match.group("object"))
                values = {f.group("key"): m._js_unescape(f.group("value")) for f in fields}
                if values == {"rel": "alternate", "hreflang": "x-default", "href": expected}:
                    candidates.append(line)
        else:
            raise ValueError("Only Hugo/Nuxt controls are allowed.")
    if len(candidates) != 1:
        raise ValueError("Exactly one existing controlled default annotation is required.")
    return content.replace(candidates[0], "", 1)


def html_checks(before: dict, after: dict, production: str) -> list[dict]:
    bp, ap = live._html_pages(before), live._html_pages(after)
    checks = []
    for key in KEYS:
        routes = live._impacted_routes(before, key)
        observations = []
        for route in sorted(routes):
            old, new = bp.get(route), ap.get(route)
            row = {"route": route, "resolved": False}
            if old and new:
                fields_ok = (old.get("status_code") == new.get("status_code") == 200
                             and all(old.get(k) == new.get(k) for k in PRESERVED)
                             and not live._noindex(new.get("meta_robots"))
                             and not live._noindex(new.get("x_robots_tag")))
                old_map = {k.lower(): v for k, v in (old.get("hreflang") or {}).items()}
                new_map = {k.lower(): v for k, v in (new.get("hreflang") or {}).items()}
                expected = {k.replace("_", "-") if k == "fr_fr" else k: v for k, v in old_map.items()}
                without_default = {k: v for k, v in new_map.items() if k != "x-default"}
                if key == KEYS[0]:
                    row["resolved"] = fields_ok and "fr_fr" in old_map and without_default == expected
                else:
                    defaults = [r.get("href") for r in new.get("hreflang_raw", [])
                                if str(r.get("hreflang") or "").lower() == "x-default"]
                    target = new_map.get("x-default", "")
                    # Existing legacy fixtures permit the site's root or an existing FR edition.
                    allowed = {production, *(v for k, v in expected.items() if k.split("-", 1)[0] == "fr")}
                    if route == "/gauntlet/qa-reciprocal-en/":
                        allowed = {production + "gauntlet/qa-reciprocal-fr/"}
                    target_page = ap.get(live._route(target)) if target else None
                    target_ok = (target in allowed and target_page and target_page.get("status_code") == 200
                                 and target_page.get("canonical") == target
                                 and not live._noindex(target_page.get("meta_robots"))
                                 and not live._noindex(target_page.get("x_robots_tag")))
                    row.update({"default_url": target, "allowed_fixture_defaults": sorted(allowed)})
                    row["resolved"] = bool(fields_ok and without_default == expected and len(defaults) == 1
                                           and defaults[0] == target and target_ok)
            observations.append(row)
        count = int(before.get("issues", {}).get(key, {}).get("count") or 0)
        checks.append({"family": key, "verdict": "resolved_on_observed_html"
                       if count and len(observations) >= count and all(r["resolved"] for r in observations)
                       else "unverified", "observations": observations})
    return checks


def cycle(m, token: str, stack: str, work: Path, budget: ClaudeBudget) -> dict:
    if stack not in SEEDS:
        raise ValueError("Only Hugo/Nuxt fixture repositories are allowed.")
    repo = f"noyaru-stack-{stack}"
    production = f"https://{repo}.netlify.app/"
    result = {"stack": stack, "repository": f"{live.OWNER}/{repo}", "pull_requests": [],
              "families": list(KEYS), "measurement_mode": "counterfactual_fixture_preview_without_injected_header"}

    def ref(name):
        return m._github_api_get(m._github_ref_api_path(live.OWNER, repo, name), token=token)["object"]["sha"]

    def branch(name, sha):
        if not name.startswith(f"gauntlet-{stack}-") or not m._github_branch_allowed(name):
            raise ValueError("Only dedicated fixture branches may be written.")
        m._github_api_post(m._github_api_path("repos", live.OWNER, repo, "git", "refs"), token=token,
                           json_body={"ref": "refs/heads/" + name, "sha": sha})

    def preview(kind, head):
        pr = m._ouvrir_pull_request(owner=live.OWNER, repo=repo, token=token, head=head, base="main", draft=True,
                                   title=f"QA hreflang Claude {stack}: {kind} (never merge)",
                                   body="Fixture validation only. Close without merging; main retains its defects.")
        record = {"kind": kind, "number": pr["number"], "url": pr["html_url"]}
        result["pull_requests"].append(record)
        live.save(work / "cycle.json", result)
        print(f"[{stack}] Awaiting {kind} #{pr['number']}", flush=True)
        record["build"] = live.wait_build(m, repo, token, pr["number"])
        if record["build"]["head_sha"] != ref(head):
            raise ValueError("The built preview does not match its branch head.")
        url = f"https://deploy-preview-{pr['number']}--{repo}.netlify.app/"
        live.crawl(stack, url, work / kind / "raw", 90, preview=True)
        response = live.requests.get(url + "sitemap.xml", timeout=30, allow_redirects=False)
        response.raise_for_status()
        (work / kind / "sitemap.xml").write_bytes(response.content)
        return previous.rescore(work / kind / "raw/report.json", url, production, response.content,
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
        baseline = f"gauntlet-{stack}-hreflang-ai-baseline-{stamp}"
        branch(baseline, seed["sha"])
        control = ("content/gauntlet/qa-reciprocal-en.md" if stack == "hugo"
                   else "pages/gauntlet/qa-reciprocal-en.vue")
        row = m._github_api_get(m._github_content_api_path(live.OWNER, repo, control),
                               token=token, params={"ref": baseline})
        raw = base64.b64decode(row["content"]).decode("utf-8")
        content = remove_control_default(m, raw, stack, production + "gauntlet/qa-reciprocal-fr/")
        m._github_api_put(m._github_content_api_path(live.OWNER, repo, control), token=token,
                          json_body={"branch": baseline, "sha": row["sha"],
                                     "message": "test: missing x-default on a known translation group",
                                     "content": base64.b64encode(content.encode()).decode("ascii")})
        result.update(baseline_branch=baseline, baseline_sha=ref(baseline))
        before = preview("baseline", baseline)
        if (not all(before["issues"].get(k, {}).get("count") for k in KEYS)
                or "/gauntlet/qa-reciprocal-en/" not in live._impacted_routes(before, KEYS[1])):
            raise ValueError("Both families and the known fallback control must be observed before repair.")
        corrected = f"gauntlet-{stack}-hreflang-ai-correction-{stamp}"
        branch(corrected, result["baseline_sha"])
        result["correction_branch"] = corrected
        from backend import audit_dashboard as dash, repo_index

        tree = m._github_api_get(m._github_api_path("repos", live.OWNER, repo, "git", "trees", baseline),
                                token=token, params={"recursive": "1"})
        if tree.get("truncated"):
            raise ValueError("The fixture tree is incomplete.")
        paths = [r["path"] for r in tree["tree"] if r["type"] == "blob"]
        index, state = repo_index.build_repo_index(paths), {}
        scopes, run = {}, {"results": []}
        result["repair_scopes"] = scopes
        for key in KEYS:
            impacted = sorted(dash.extract_impacted_pages(key, before["issues"][key]))
            expected = {p for url in impacted for p in repo_index.route_files(index, url)}
            if not expected or len(expected) > 6:
                raise ValueError("The repair scope is unresolved or exceeds the fixture file budget.")
            prep = m._prepare_issue_fix(issue_key=key, issues=before["issues"], impacted=impacted,
                                        all_paths=paths, site_name=urlsplit(production).netloc,
                                        owner=live.OWNER, repo_name=repo, branch=baseline,
                                        token=token, pages=before["pages"])
            if prep["refusal"]:
                raise ValueError("The fixture repair was refused.")
            capped = []
            applied = m._apply_prepared_issue_fix(
                owner=live.OWNER, repo_name=repo, branch=baseline, token=token, fix_branch=corrected,
                all_paths=paths, issue_key=key, issue_label=dash.ISSUE_CATALOG[key].label,
                impacted=impacted, site_name=urlsplit(production).netloc, file_state=state,
                max_files=len(expected), prep=prep, pages=before["pages"], index=index,
                allow_ai_targeting=False, ecartes=capped)
            scopes[key] = {"impacted_urls": impacted, "expected_files": sorted(expected),
                           "patched": applied["patched"], "targets": applied["targets"],
                           "ai_files": applied["ai_files"], "skipped": applied["skipped"],
                           "capped_files": capped}
            if applied.get("fatal") or set(applied["targets"]) != expected or set(applied["patched"]) != expected:
                raise ValueError("The corrector did not repair exactly all expected fixture files.")
            run["results"].append([key, "ok"])
            live.save(work / "cycle.json", result)
        after = preview("correction", corrected)
        bp, ap = (work / kind / "controlled/report.json" for kind in ("baseline", "correction"))
        comparison = live.compare(bp, bp, ap, run)
        result["comparison"], result["html_checks"] = comparison, html_checks(before, after, production)
        result["main_unchanged"] = ref("main") == result["main_sha"]
        result["status"] = ("measured" if result["main_unchanged"] and comparison["comparable"]
                            and not comparison["increases"] and not comparison["new_issue_routes"]
                            and not comparison["missing_html_routes"]
                            and all(row["verdict"] == "resolved_in_preview_scope" for row in comparison["targeted_families"])
                            and all(row["verdict"] == "resolved_on_observed_html" for row in result["html_checks"])
                            and not budget.summary()["exhausted"] else "unverified")
    except Exception as exc:
        result["status"], result["error_type"] = "failed", type(exc).__name__
        print(f"[{stack}] Failed: {type(exc).__name__}", flush=True)
    finally:
        result["ai_budget"] = budget.summary()
        result["cleanup"] = live.close_prs(repo, token, result["pull_requests"])
        result["pull_requests_closed_without_merge"] = all(
            row.get("state") == "closed" and row.get("merged") is False for row in result["cleanup"])
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
            work = args.workdir / stack
            with (args.workdir / (stack + ".log")).open("w", encoding="utf-8") as log:
                proc = subprocess.run([sys.executable, str(Path(__file__).resolve()), "--stack", stack,
                                       "--workdir", str(work), "--ai-max-calls", str(args.ai_max_calls)],
                                      cwd=live.WEB_ROOT, stdout=log, stderr=subprocess.STDOUT, timeout=2700)
            results.append(live.load(work / "cycle.json"))
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
        row["estimated_cost_usd"] = m._cout_ia_usd(
            modele, entree=row["input_tokens"], sortie=row["output_tokens"],
            cache_lu=row["cache_read_tokens"], cache_ecrit=row["cache_write_tokens"])
        with lock:
            usage.append(row)
        original(fournisseur=fournisseur, modele=modele, data=data)

    m._noter_consommation_ia = record_usage
    result = cycle(m, token, args.stack, args.workdir, budget)
    live.save(args.workdir / "usage.json", {"calls": usage, "budget": budget.summary(),
                                          "estimated_cost_is_provider_invoice": False})
    return int(result["status"] != "measured" or not result["pull_requests_closed_without_merge"])


if __name__ == "__main__":
    raise SystemExit(main())
