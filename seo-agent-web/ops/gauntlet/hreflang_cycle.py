"""Validate real return-tag repairs on isolated Hugo/Nuxt fixture branches.

Netlify previews inject noindex. Raw crawls are retained; a separately labelled
counterfactual re-scores observed pages in the production URL namespace. It is not
a production deployment or proof that a preview is indexable.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import importlib.util
import json
import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from xml.etree import ElementTree as XML

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from defusedxml import ElementTree as SafeXML  # noqa: E402

from ops.gauntlet import live_cycle as live  # noqa: E402
from ops.gauntlet.ai_budget import ClaudeBudget  # noqa: E402

SEEDS = {
    "hugo": {"branch": "gauntlet-hugo-20261002-111009",
             "sha": "8e6700c2899b42a7baaf66ae180d855f92c7541f"},
    "nuxt": {"branch": "gauntlet-nuxt-20261002-111431",
             "sha": "f11930be69231409e359cf3f82dd3872f9e57ac5"},
}
KEY = "missing_reciprocal_hreflang"
NS = "http://www.sitemaps.org/schemas/sitemap/0.9"


def seed_pages(stack: str) -> dict[str, str]:
    if stack not in SEEDS:
        raise ValueError("Only Hugo/Nuxt fixture repositories are allowed.")
    host = f"https://noyaru-stack-{stack}.netlify.app"
    urls = {code: host + f"/gauntlet/qa-reciprocal-{code}/" for code in ("fr", "en", "de")}
    titles = {"fr": "Validation des traductions francaises Noyaru",
              "en": "Noyaru English translation validation page",
              "de": "Noyaru deutsche Uebersetzungen kontrollieren"}
    descriptions = {
        "fr": "Controle des traductions francaises du parcours Noyaru, avec des liens vers les editions anglaise et allemande pour verifier leurs balises de retour.",
        "en": "Check the English edition of the Noyaru validation guide, with links to its French and German translations and a controlled missing return annotation.",
        "de": "Kontrolle der deutschen Ausgabe des Noyaru Testleitfadens mit Verweisen auf die englische und franzoesische Uebersetzung sowie ihren Sprachangaben.",
    }
    output = {}
    for code in urls:
        peers = {"fr": ("fr", "en"), "en": ("en", "de"), "de": ("de", "en")}[code]
        links = [{"rel": "canonical", "href": urls[code]}]
        links += [{"rel": "alternate", "hreflang": p, "href": urls[p]} for p in peers]
        links += [{"rel": "alternate", "hreflang": "x-default", "href": urls["fr"]}]
        meta = [{"name": "viewport", "content": "width=device-width"},
                {"name": "description", "content": descriptions[code]}]
        for prefix, fields in (("og", {"type": "article", "title": titles[code],
                                      "description": descriptions[code], "url": urls[code],
                                      "image": host + "/og.png"}),
                               ("twitter", {"card": "summary_large_image", "title": titles[code],
                                            "description": descriptions[code], "image": host + "/og.png"})):
            meta += [{"property" if prefix == "og" else "name": prefix + ":" + k, "content": v}
                     for k, v in fields.items()]
        body = (f"<main><h1>{titles[code]}</h1><p>{descriptions[code]}</p>"
                + "".join(f'<p><a href="{urls[p]}">{titles[p]}</a></p>' for p in urls)
                + '<p><a href="/">Noyaru</a></p></main>')
        if stack == "nuxt":
            def literal(rows):
                return ",\n".join("    { " + ", ".join(k + ": " + json.dumps(v) for k, v in row.items())
                                  + " }" for row in rows)
            output[f"pages/gauntlet/qa-reciprocal-{code}.vue"] = (
                f"<script setup>\nuseHead({{\n  title: {json.dumps(titles[code])},\n"
                f"  htmlAttrs: {{ lang: '{code}' }},\n  meta: [\n{literal(meta)}\n  ],\n"
                f"  link: [\n{literal(links)}\n  ]\n}});\n</script>\n<template>{body}</template>\n")
        else:
            tags = [f"<title>{titles[code]}</title>"]
            for tag, rows in (("meta", meta), ("link", links)):
                tags += ["  <" + tag + " " + " ".join(f'{k}="{v}"' for k, v in row.items()) + " />"
                         for row in rows]
            output[f"content/gauntlet/qa-reciprocal-{code}.md"] = (
                f'+++\nurl = "/gauntlet/qa-reciprocal-{code}/"\ntitle = "{titles[code]}"\n'
                f"html_attrs = ' lang=\"{code}\"'\nraw_head = '''\n" + "\n".join(tags)
                + "\n'''\nraw_body = '''\n" + body + "\n'''\n+++\n")
    return output


def project(report: dict, preview: str, production: str) -> tuple[dict, list[str]]:
    """Copy structured URL fields, preserving paths/case/query and unrelated hosts."""
    old, new = urlsplit(preview), urlsplit(production)
    if old.scheme != "https" or new.scheme != "https" or old.hostname == new.hostname:
        raise ValueError("A distinct HTTPS fixture preview is required.")

    def transform(value):
        if isinstance(value, dict):
            return {transform(k): transform(v) for k, v in value.items()}
        if isinstance(value, list):
            return [transform(v) for v in value]
        if isinstance(value, str):
            parsed = urlsplit(value) if value.startswith(("https://", "http://")) else None
            if parsed and parsed.netloc == old.netloc and parsed.scheme == old.scheme:
                return urlunsplit((new.scheme, new.netloc, parsed.path, parsed.query, parsed.fragment))
        return value

    result = transform(report)
    removed = []
    for source, row in zip(report.get("pages", []), result.get("pages", [])):
        if (urlsplit(str(source.get("url") or "")).netloc == old.netloc
                and urlsplit(str(source.get("final_url") or source.get("url") or "")).netloc == old.netloc
                and source.get("x_robots_tag") == "noindex"):
            row["x_robots_tag"] = None
            removed.append(source["url"])
    result["canonical_host_alias"] = ""
    result["validation_mode"] = "counterfactual_fixture_preview_without_injected_header"
    return result, removed


def seed_navigation(stack: str, text: str) -> str:
    if stack == "hugo":
        return text + "\n" + "\n".join(
            f"- [qa-reciprocal-{code}](/gauntlet/qa-reciprocal-{code}/)" for code in ("fr", "en", "de")) + "\n"
    if stack != "nuxt" or text.count("</ul>") != 1:
        raise ValueError("The fixture navigation cannot be safely extended.")
    links = "".join(f'<li><a href="/gauntlet/qa-reciprocal-{code}/">qa-reciprocal-{code}</a></li>\n'
                    for code in ("fr", "en", "de"))
    return text.replace("</ul>", links + "</ul>", 1)


def rescore(report_path: Path, preview: str, production: str, sitemap_body: bytes,
            output: Path) -> dict:
    xml = SafeXML.fromstring(sitemap_body)
    if xml.tag != f"{{{NS}}}urlset":
        raise ValueError("This bounded benchmark requires a sitemap URL set, not an index.")
    entries = [node.text for node in xml.findall(f"{{{NS}}}url/{{{NS}}}loc") if node.text]
    projected, removed = project(live.load(report_path), preview, production)
    entries_report, _ = project({"entries": entries}, preview, production)
    spec = importlib.util.spec_from_file_location("gauntlet_hreflang_audit", live.AUDITOR)
    auditor = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = auditor
    spec.loader.exec_module(auditor)
    output.mkdir(parents=True, exist_ok=True)
    projected["issues"] = auditor._score_issues(
        auditor._pages_from_report(projected), sitemap_urls=set(entries_report["entries"]),
        sitemap_urlsets={production + "sitemap.xml": entries_report["entries"]},
        base_url=production, output_dir=str(output))
    live.save(output / "report.json", projected)
    live.save(output / "projection.json", {
        "mode": projected["validation_mode"], "raw_report": str(report_path),
        "preview": preview, "production_namespace": production,
        "removed_exact_preview_noindex_header_urls": removed,
        "meta_robots_preserved": True, "sitemap_entries": entries_report["entries"],
        "not_a_production_deployment": True})
    return projected


def cycle(m, token: str, stack: str, root: Path) -> dict:
    if stack not in SEEDS:
        raise ValueError("Only Hugo/Nuxt fixture repositories are allowed.")
    repo, production = f"noyaru-stack-{stack}", f"https://noyaru-stack-{stack}.netlify.app/"
    work = root / stack
    work.mkdir(exist_ok=False)
    result = {"stack": stack, "repository": f"{live.OWNER}/{repo}", "pull_requests": [],
              "family": KEY, "mode": "counterfactual_fixture_preview_without_injected_header"}

    def ref(branch):
        return m._github_api_get(m._github_ref_api_path(live.OWNER, repo, branch), token=token)["object"]["sha"]

    def read(path, branch):
        row = m._github_api_get(m._github_content_api_path(live.OWNER, repo, path),
                               token=token, params={"ref": branch})
        return row, base64.b64decode(row["content"]).decode("utf-8")

    def put(path, text, branch, sha=None):
        payload = {"message": "test: controlled reciprocal hreflang fixture", "branch": branch,
                   "content": base64.b64encode(text.encode("utf-8")).decode("ascii")}
        if sha:
            payload["sha"] = sha
        m._github_api_put(m._github_content_api_path(live.OWNER, repo, path), token=token, json_body=payload)

    def branch(name, sha):
        if not name.startswith(f"gauntlet-{stack}-") or not m._github_branch_allowed(name):
            raise ValueError("Only dedicated fixture branches can be written.")
        m._github_api_post(m._github_api_path("repos", live.OWNER, repo, "git", "refs"),
                           token=token, json_body={"ref": "refs/heads/" + name, "sha": sha})

    def preview(kind, head):
        pr = m._ouvrir_pull_request(owner=live.OWNER, repo=repo, token=token,
                                   title=f"QA hreflang {stack}: {kind} (never merge)",
                                   body="Controlled fixture only. Never merge; close after validation.",
                                   head=head, base="main", draft=True)
        record = {"kind": kind, "number": pr["number"], "url": pr["html_url"]}
        result["pull_requests"].append(record)
        live.save(work / "cycle.json", result)
        print(f"[{stack}] Awaiting {kind} #{pr['number']}", flush=True)
        record["build"] = live.wait_build(m, repo, token, pr["number"])
        if record["build"]["head_sha"] != ref(head):
            raise ValueError("The built preview no longer matches its branch head.")
        url = f"https://deploy-preview-{pr['number']}--{repo}.netlify.app/"
        live.crawl(stack, url, work / kind / "raw", 90, preview=True)
        response = live.requests.get(url + "sitemap.xml", timeout=30, allow_redirects=False)
        response.raise_for_status()
        (work / kind / "sitemap.xml").write_bytes(response.content)
        return rescore(work / kind / "raw/report.json", url, production, response.content,
                       work / kind / "controlled")

    try:
        result["main_sha"] = ref("main")
        seed = SEEDS[stack]
        if ref(seed["branch"]) != seed["sha"]:
            raise ValueError("The previously validated seed head changed.")
        ancestry = m._github_api_get(m._github_api_path("repos", live.OWNER, repo, "compare",
                                                       result["main_sha"] + "..." + seed["sha"]), token=token)
        if ancestry["merge_base_commit"]["sha"] != result["main_sha"]:
            raise ValueError("Fixture main no longer matches the validated seed ancestry.")
        stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d-%H%M%S")
        baseline = f"gauntlet-{stack}-hreflang-baseline-{stamp}"
        branch(baseline, seed["sha"])
        pages = seed_pages(stack)
        for path, text in pages.items():
            put(path, text, baseline)
        nav_path = "public/gauntlet/index.html" if stack == "nuxt" else "content/gauntlet/_index.md"
        nav_row, nav_text = read(nav_path, baseline)
        put(nav_path, seed_navigation(stack, nav_text), baseline, nav_row["sha"])
        sitemap_path = "public/sitemap.xml" if stack == "nuxt" else "static/sitemap.xml"
        row, text = read(sitemap_path, baseline)
        xml = SafeXML.fromstring(text)
        if xml.tag != f"{{{NS}}}urlset":
            raise ValueError("Only a fixture sitemap URL set may be extended.")
        XML.register_namespace("", NS)
        for code in ("fr", "en", "de"):
            entry = XML.SubElement(xml, f"{{{NS}}}url")
            XML.SubElement(entry, f"{{{NS}}}loc").text = production + f"gauntlet/qa-reciprocal-{code}/"
        put(sitemap_path, XML.tostring(xml, encoding="unicode"), baseline, row["sha"])
        result["baseline_branch"], result["baseline_sha"] = baseline, ref(baseline)
        before = preview("baseline", baseline)
        item = {"page": production + "gauntlet/qa-reciprocal-en/", "field": "fr",
                "value": production + "gauntlet/qa-reciprocal-fr/"}
        if item not in before["issues"].get(KEY, {}).get("evidence", {}).get("items", []):
            raise ValueError("The production scorer did not observe the controlled missing return tag.")
        corrected = f"gauntlet-{stack}-hreflang-correction-{stamp}"
        branch(corrected, result["baseline_sha"])
        from backend import audit_dashboard as dash, repo_index

        tree = m._github_api_get(m._github_api_path("repos", live.OWNER, repo, "git", "trees", baseline),
                                token=token, params={"recursive": "1"})
        if tree.get("truncated"):
            raise ValueError("The fixture tree is incomplete.")
        paths = [b["path"] for b in tree["tree"] if b["type"] == "blob"]
        impacted = sorted(dash.extract_impacted_pages(KEY, before["issues"][KEY]))
        prep = m._prepare_issue_fix(issue_key=KEY, issues=before["issues"], impacted=impacted,
                                    all_paths=paths, site_name=urlsplit(production).netloc,
                                    owner=live.OWNER, repo_name=repo, branch=baseline,
                                    token=token, pages=before["pages"])
        expected_path = next(path for path in pages if "-en." in path)
        if prep["refusal"] or prep["targets_override"] != [expected_path]:
            raise ValueError("The corrector must isolate the repairable translation and skip the legacy collision.")
        applied = m._apply_prepared_issue_fix(
            owner=live.OWNER, repo_name=repo, branch=baseline, token=token, fix_branch=corrected,
            all_paths=paths, issue_key=KEY, issue_label=dash.ISSUE_CATALOG[KEY].label,
            impacted=impacted, site_name=urlsplit(production).netloc, file_state={}, max_files=1,
            prep=prep, pages=before["pages"], index=repo_index.build_repo_index(paths), allow_ai_targeting=False)
        result["repair"] = {"patched": applied["patched"], "targets": applied["targets"],
                            "ai_files": applied["ai_files"], "collision_warning": prep["side_effects"]}
        if applied.get("fatal") or applied["patched"] != [expected_path] or applied["ai_files"]:
            raise ValueError("Exactly the target translation must be patched without AI.")
        after = preview("correction", corrected)
        bpath, apath = (work / kind / "controlled/report.json" for kind in ("baseline", "correction"))
        comparison = live.compare(bpath, bpath, apath, {"results": [[KEY, "ok"]]})
        result["comparison"] = comparison
        remaining = after["issues"].get(KEY, {}).get("evidence", {}).get("items", [])
        result["positive_control"] = {"before_observed": True, "after_absent": item not in remaining}
        result["main_unchanged"] = ref("main") == result["main_sha"]
        result["status"] = ("measured" if item not in remaining and result["main_unchanged"]
                            and comparison["comparable"] and not comparison["missing_html_routes"]
                            and not comparison["increases"] and not comparison["new_issue_routes"] else "unverified")
    except Exception as exc:
        result["status"], result["error_type"] = "failed", type(exc).__name__
        print(f"[{stack}] Failed: {type(exc).__name__}", flush=True)
        raise
    finally:
        result["cleanup"] = live.close_prs(repo, token, result["pull_requests"])
        result["pull_requests_closed_without_merge"] = all(
            p.get("state") == "closed" and p.get("merged") is False for p in result["cleanup"])
        live.save(work / "cycle.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", type=Path, required=True)
    args = parser.parse_args()
    args.workdir.mkdir(parents=True, exist_ok=True)
    if any(args.workdir.iterdir()):
        parser.error("Use an empty temporary directory, never client data.")
    m, token = live._load_backend(args.workdir)
    budget = ClaudeBudget(0)
    budget.install(m)
    results = []
    for stack in SEEDS:
        try:
            results.append(cycle(m, token, stack, args.workdir))
        except Exception:
            # cycle.json already records the type and cleanup; HTTP bodies can contain secrets.
            return 1
    live.save(args.workdir / "cycles.json", {"cycles": results, "ai_budget": budget.summary()})
    return int(any(r["status"] != "measured" or not r["pull_requests_closed_without_merge"]
                   for r in results) or budget.summary()["attempted"] or budget.summary()["denied"])


if __name__ == "__main__":
    raise SystemExit(main())
