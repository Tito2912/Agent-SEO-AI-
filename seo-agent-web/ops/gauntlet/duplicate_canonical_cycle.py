"""Measure true duplicate consolidation while retaining metadata-only negative controls."""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
from pathlib import Path
import sys
from urllib.parse import unquote, urlsplit
from xml.etree import ElementTree as ET

from defusedxml import ElementTree as XML

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ops.gauntlet import https_canonical_cycle as previous  # noqa: E402
from ops.gauntlet.ai_budget import ClaudeBudget  # noqa: E402

live, projection, titles = previous.live, previous.projection, previous.titles
REPO, MAIN, SITE = previous.REPO, previous.MAIN, previous.SITE
SEED_BRANCH = "gauntlet-static-html-https-canonical-correction-20261004-183326"
SEED = "dd9cea5389a4ce6c1242696d63eb2f2bcae66ff8"
SEED_PR = 60
PREFIX = "gauntlet-static-html-duplicate-canonical-"
KEY = "duplicate_pages_without_canonical"
NAMES = ("qa-dupe-a", "qa-dupe-b", "qa-dupe-control")
ROUTES = {n: "/gauntlet/" + n for n in NAMES}
FILES = {n: "gauntlet/" + n + ".html" for n in NAMES}
URLS = {n: SITE.rstrip("/") + r for n, r in ROUTES.items()}
NEGATIVES = tuple(SITE + "gauntlet/no-canonical-" + n for n in ("a", "b"))
TWINS = {URLS[n] for n in NAMES[:2]}
MASTERS = {u: URLS[NAMES[0]] for u in sorted(TWINS)}
SETUP = {"index.html", "sitemap.xml", *FILES.values()}
CORRECTION = {FILES[n] for n in NAMES[:2]}


def page(name: str) -> str:
    if name not in NAMES:
        raise ValueError("outside_owned_page_names")
    title = "Temoin de consolidation de copies HTML identiques"
    description = "Deux copies strictement identiques de ce contenu permettent de verifier une consolidation canonical sans modifier leurs textes ni leurs liens."
    heading = "Copies identiques pour le controle canonical"
    if name == NAMES[2]:
        title, heading = "Controle distinct pour la consolidation canonical", "Une page de controle distincte"
        description = "Cette page de controle possede son propre contenu et son propre canonical ; elle ne doit jamais etre consolidee avec les copies du temoin."
    canonical = '<link rel="canonical" href="' + URLS[name] + '" />\n' if name == NAMES[2] else ""
    return ('<!doctype html><html lang="fr"><head>\n<meta charset="utf-8" />\n'
        '<meta name="viewport" content="width=device-width" />\n<title>' + title + '</title>\n'
        '<meta name="description" content="' + description + '" />\n' + canonical
        + '<meta property="og:type" content="website" />\n<meta property="og:title" content="' + title + '" />\n'
        '<meta property="og:description" content="' + description + '" />\n'
        '<meta property="og:url" content="' + URLS[name] + '" />\n'
        '<meta property="og:image" content="' + SITE + 'og.png" />\n'
        '<meta name="twitter:card" content="summary_large_image" />\n'
        '<meta name="twitter:title" content="' + title + '" />\n'
        '<meta name="twitter:description" content="' + description + '" />\n'
        '<meta name="twitter:image" content="' + SITE + 'og.png" />\n'
        '</head><body><h1>' + heading + '</h1>\n'
        '<p>Le contenu de cette page est un temoin appartenant au banc de verification. '
        'Les copies reprennent exactement les memes phrases, le meme titre principal et le meme lien vers la racine. '
        'La correction doit seulement declarer la destination canonical commune et aligner la valeur sociale existante. '
        'Aucun paragraphe ne doit etre reecrit et aucune page distincte ne doit perdre son identite. '
        'La convention de choix du maitre reste soumise a la relecture humaine.</p>\n'
        '<nav><a href="/">Accueil du temoin</a></nav>\n</body></html>\n')


def expected_source(name: str) -> str:
    if name not in NAMES[:2]:
        raise ValueError("outside_duplicate_correction")
    return page(name).replace('property="og:url" content="' + URLS[name],
        'property="og:url" content="' + URLS[NAMES[0]]).replace("</head>",
        '<link rel="canonical" href="' + URLS[NAMES[0]] + '" />\n</head>')


def write_allowed(path: str, body: dict, branch: str, phase: str, written: set[str],
                  expected: dict[str, str], shas: dict[str, str]) -> bool:
    scope = SETUP if phase == "baseline" else CORRECTION if phase == "correction" else set()
    if (path not in scope or path in written or path not in expected or path not in shas
            or not branch.startswith(PREFIX + phase + "-") or body.get("branch") != branch
            or body.get("sha", "") != shas[path]):
        return False
    try:
        return base64.b64decode(body.get("content", ""), validate=True).decode("utf-8") == expected[path]
    except (ValueError, TypeError, UnicodeError):
        return False


def checks(before: dict, after: dict) -> dict:
    previous.complete(before)
    previous.complete(after)
    bp, ap = live._html_pages(before), live._html_pages(after)
    if bp.keys() != ap.keys() or len(bp) != 52:
        raise ValueError("html_route_set_changed")
    for report, expected in ((before, set(NEGATIVES) | TWINS), (after, set(NEGATIVES))):
        block = report.get("issues", {}).get(KEY, {})
        if block.get("count") != len(expected) or set(block.get("examples", [])) != expected:
            raise ValueError("missing_positive_or_negative_witnesses")
    fields = titles.PRESERVED + ("title", "title_tag_count", "meta_description", "meta_description_tag_count",
        "h1_tag_count", "h2_tag_count", "content_sketch", "text_word_count", "internal_link_items", "ld_json_blocks",
        "status_code", "content_type", "error", "blocked_by_host")
    for route, page_row in bp.items():
        twin = SITE.rstrip("/") + route in TWINS
        if twin and (page_row.get("canonical") or ap[route].get("canonical") != URLS[NAMES[0]]
                     or ap[route].get("og_url") != URLS[NAMES[0]]):
            raise ValueError("incorrect_duplicate_canonical")
        for field in fields:
            if twin and field in {"canonical", "og_url"}:
                continue
            if page_row.get(field) != ap[route].get(field):
                raise ValueError("collateral_observed_html_changed")
    if ap[previous.ROUTE].get("canonical") != previous.SOURCE:
        raise ValueError("previous_https_repair_lost")
    return {"html_routes_before": len(bp), "html_routes_after": len(ap), "observed_fields_preserved": True,
        "selected_occurrences": [2, 0], "target_counts": {KEY: [4, 2]}, "negative_controls_remaining": list(NEGATIVES),
        "increased_counts": {k: [before["issues"].get(k, {}).get("count", 0), v.get("count", 0)]
            for k, v in after["issues"].items() if v.get("count", 0) > before["issues"].get(k, {}).get("count", 0)}}


def cycle(m, token: str, work: Path, budget: ClaudeBudget) -> dict:
    if budget.limit != 0:
        raise ValueError("nonzero_provider_budget")
    result = {"status": "unverified", "stage": "identity", "repository": f"{live.OWNER}/{REPO}",
        "seed_sha": SEED, "pull_requests": [], "writes": [], "write_attempts": 0, "universal_certification": False}
    original_put = m._github_api_put
    active_branch, phase, expected, shas, written = "", "", {}, {}, set()

    def get(*parts, **kwargs):
        return m._github_api_get(m._github_api_path("repos", live.OWNER, REPO, *parts), token=token, **kwargs)

    def ref(branch):
        return get("git", "ref", "heads", branch)["object"]["sha"]

    def source(path, head):
        row = get("contents", path, params={"ref": head})
        return row["sha"], base64.b64decode(row["content"]).decode("utf-8")

    def guarded_put(path, **kwargs):
        prefix = m._github_api_path("repos", live.OWNER, REPO, "contents") + "/"
        file = unquote(path[len(prefix):]) if path.startswith(prefix) else ""
        if not write_allowed(file, kwargs.get("json_body", {}), active_branch, phase, written, expected, shas):
            raise ValueError("outside_owned_duplicate_write_scope")
        written.add(file)
        result["write_attempts"] += 1
        live.save(work / "cycle.json", result)
        response = original_put(path, **kwargs)
        result["writes"].append({"branch": active_branch, "path": file})
        return response

    def branch(name, sha):
        if not name.startswith(PREFIX) or not m._github_branch_allowed(name) or sha not in {SEED, result.get("baseline_sha")}:
            raise ValueError("outside_owned_qa_branch_scope")
        m._github_api_post(m._github_api_path("repos", live.OWNER, REPO, "git", "refs"), token=token,
            json_body={"ref": "refs/heads/" + name, "sha": sha})

    def preview(kind, head, sha):
        result["stage"] = kind
        print("STAGE=" + kind, flush=True)
        pr = m._ouvrir_pull_request(owner=live.OWNER, repo=REPO, token=token, head=head, base="main", draft=True,
            title="QA duplicate canonical " + kind + " (never merge)", body="Owned fixture only. Close without merging.")
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
        previous.previous.observations(url, out)
        observations = {}
        for route in [*ROUTES.values(), *(urlsplit(u).path for u in NEGATIVES)]:
            response = live.requests.get(url.rstrip("/") + route, timeout=30, allow_redirects=False)
            (out / (route.rsplit("/", 1)[-1] + ".http")).write_bytes(response.content)
            if response.status_code != 200 or response.headers.get("x-robots-tag") != "noindex":
                raise ValueError("html_or_native_preview_policy_changed")
            observations[route] = {"status": response.status_code, "x_robots_tag": "noindex",
                                   "sha256": hashlib.sha256(response.content).hexdigest()}
        live.save(out / "duplicate-http.json", observations)
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
        if set(FILES.values()) & set(paths):
            raise ValueError("qa_pages_already_exist")
        stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d-%H%M%S")
        baseline, corrected = (PREFIX + kind + "-" + stamp for kind in ("baseline", "correction"))
        result.update(main_sha=MAIN, baseline_branch=baseline, correction_branch=corrected)
        phase, active_branch = "baseline", baseline
        for path in ("index.html", "sitemap.xml"):
            shas[path], expected[path] = source(path, SEED)
        if expected["index.html"].count("</nav>") != 1:
            raise ValueError("unverified_root_navigation")
        links = " ".join('<a href="' + ROUTES[n] + '">' + n + '</a>' for n in NAMES)
        expected["index.html"] = expected["index.html"].replace("</nav>", " " + links + "</nav>")
        ns = "http://www.sitemaps.org/schemas/sitemap/0.9"
        sitemap = XML.fromstring(expected["sitemap.xml"])
        if sitemap.tag != "{" + ns + "}urlset":
            raise ValueError("unverified_sitemap_format")
        ET.register_namespace("", ns)
        for name in NAMES:
            ET.SubElement(ET.SubElement(sitemap, "{" + ns + "}url"), "{" + ns + "}loc").text = URLS[name]
            shas[FILES[name]], expected[FILES[name]] = "", page(name)
        expected["sitemap.xml"] = ET.tostring(sitemap, encoding="unicode") + "\n"
        branch(baseline, SEED)
        for path in sorted(SETUP):
            body = {"branch": baseline, "message": "test: owned canonical duplicate witnesses", "content":
                    base64.b64encode(expected[path].encode()).decode()}
            if shas[path]:
                body["sha"] = shas[path]
            guarded_put(m._github_content_api_path(live.OWNER, REPO, path), token=token, json_body=body)
        baseline_sha = result["baseline_sha"] = ref(baseline)
        comparison = get("compare", SEED + "..." + baseline_sha)
        if comparison["total_commits"] != 5 or {p["filename"] for p in comparison["files"]} != SETUP:
            raise ValueError("collateral_setup_changed")
        before = preview("baseline", baseline, baseline_sha)
        impacted = before["issues"][KEY]["examples"]
        paths.extend(FILES.values())
        prep = m._prepare_issue_fix(issue_key=KEY, issues=before["issues"], impacted=impacted, all_paths=paths,
            site_name=urlsplit(SITE).netloc, owner=live.OWNER, repo_name=REPO, branch=baseline, token=token,
            pages=before["pages"])
        if prep["refusal"] or prep["rewriter_ai_fallback"] or prep.get("canonical_masters") != MASTERS or (
                set(prep.get("duplicate_refusals", [])) != set(NEGATIVES)):
            raise ValueError("positive_and_negative_groups_not_verified")
        result["refused_pages"] = prep["duplicate_refusals"]
        branch(corrected, baseline_sha)
        phase, active_branch, written = "correction", corrected, set()
        expected, shas = {}, {}
        for name in NAMES[:2]:
            shas[FILES[name]], raw = source(FILES[name], baseline_sha)
            if raw != page(name):
                raise ValueError("qa_source_changed")
            expected[FILES[name]] = expected_source(name)
        from backend import repo_index
        applied = m._apply_prepared_issue_fix(owner=live.OWNER, repo_name=REPO, branch=baseline, token=token,
            fix_branch=corrected, all_paths=paths, issue_key=KEY, issue_label=KEY, impacted=impacted,
            site_name=urlsplit(SITE).netloc, file_state={}, max_files=2, prep=prep, pages=before["pages"],
            index=repo_index.build_repo_index(paths), allow_ai_targeting=False)
        result["repair"] = applied
        if set(applied["patched"]) != CORRECTION or applied["skipped"] or applied["ai_files"] or applied["config_changes"]:
            raise ValueError("unexpected_repair_scope")
        final_sha = ref(corrected)
        comparison = get("compare", baseline_sha + "..." + final_sha)
        if comparison["total_commits"] != 2 or {p["filename"] for p in comparison["files"]} != CORRECTION:
            raise ValueError("collateral_repository_changed")
        if any(source(p, final_sha)[1] != expected[p] for p in CORRECTION):
            raise ValueError("unexpected_final_source")
        for path in ("_redirects", *(urlsplit(u).path.lstrip("/") + ".html" for u in NEGATIVES)):
            if source(path, SEED)[1] != source(path, final_sha)[1]:
                raise ValueError("negative_control_or_redirect_changed")
        after = preview("final", corrected, final_sha)
        result.update(checks=checks(before, after), final_sha=final_sha, status="measured_true_duplicates_resolved")
    except Exception as exc:
        result.update(status="failed", error_type=type(exc).__name__)
        if isinstance(exc, ValueError) and str(exc).replace("_", "").isalnum():
            result["failure_code"] = str(exc)
        print(result["status"], result["stage"], result.get("failure_code", result["error_type"]), flush=True)
    finally:
        m._github_api_put = original_put
        result["ai_budget"] = budget.summary()
        result["cleanup"] = live.close_prs(REPO, token, result["pull_requests"])
        result["prs_closed_unmerged"] = previous.previous.closed_cleanup(result["pull_requests"], result["cleanup"])
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
    return int(result["status"] != "measured_true_duplicates_resolved")


if __name__ == "__main__":
    raise SystemExit(main())
