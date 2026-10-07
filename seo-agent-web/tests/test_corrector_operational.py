"""Read-only endpoint comparison: GitHub writes are recorded in memory."""

import base64
import json
import os
import sys
import tempfile
import uuid
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import unquote

import pytest
from starlette.requests import Request

WEB = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB))
ROOT = Path(tempfile.mkdtemp(prefix="seo-operational-probes-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
from backend import app as m

ORIGINAL_CANDIDATES = m._github_fixable_issue_candidates

URL = "https://site.test/"
DESCRIPTION = "Une description de cette page qui contient assez de texte pour rester au-dessus du seuil de cent caracteres du correcteur."
HTML = (
    '<!doctype html><html lang="fr"><head>\n'
    '<title>Une page de test parfaitement lisible</title>\n'
    '<meta name="description" content="' + DESCRIPTION + '" />\n'
    '<link rel="canonical" href="' + URL + '" />\n'
    '</head><body><h1>Une page de test</h1></body></html>\n'
)


@pytest.fixture
def harness(monkeypatch):
    state = {"sources": {}, "written": {}, "ai_calls": [], "report": {}, "key": "",
             "budget": 40, "pr_bodies": [], "charges": [], "crawl_ts": "20261001-090000"}
    user = SimpleNamespace(id=str(uuid.uuid4()), is_admin=True)
    project = SimpleNamespace(id=str(uuid.uuid4()), site_name="site.test", slug="review")
    from backend.models import User, Project
    m.DB.create_tables()
    with m.DB.session() as db:
        db.add(User(id=user.id, email=user.id + "@example.invalid", password_hash="test", is_admin=True))
        db.commit()
        db.add(Project(id=project.id, owner_user_id=user.id, slug=project.slug,
                       base_url=URL, site_name=project.site_name))
        db.commit()
    state.update({"user": user, "project": project})
    request = Request({"type": "http", "method": "POST", "path": "/", "headers": []})
    request.state.user = user

    monkeypatch.setattr(m, "_db_project_or_404", lambda *a, **k: project)
    monkeypatch.setattr(m, "_project_github_cfg", lambda *a, **k: {
        "repo": "review/site", "branch": "main", "mode": "review"})
    monkeypatch.setattr(m, "_compte_payeur", lambda *a, **k: user.id)
    monkeypatch.setattr(m, "_effective_user_connection_value", lambda **k: ("fake-token", "user"))
    monkeypatch.setattr(m, "_rate_limit_retry_after", lambda **k: None)
    monkeypatch.setattr(m, "_correction_gate", lambda *a, **k: (True, "", state["budget"], ""))
    monkeypatch.setattr(m, "_plafond_de_correction", lambda *a, **k: m._Plafond(state["budget"], state["budget"], None, "", True))
    monkeypatch.setattr(m, "_correction_charge", lambda user, count, **k: state["charges"].append(count))
    monkeypatch.setattr(m, "_open_pr_for_issue", lambda **k: "")
    monkeypatch.setattr(m, "_runs_dir_pour_slug", lambda *a, **k: ROOT / "runs")
    monkeypatch.setattr(m.dash, "list_project_crawls", lambda *a, **k: [state["crawl_ts"]])
    monkeypatch.setattr(m.dash, "load_report_json", lambda *a, **k: state["report"])
    monkeypatch.setattr(m, "_github_fixable_issue_candidates", lambda **k: [{
        "key": state["key"], "label": state["key"], "url": URL}])
    monkeypatch.setattr(m, "_ai_pick_repo_files", lambda *a, **k: [])
    monkeypatch.setattr(m, "_ai_map_urls_to_files", lambda *a, **k: [])
    monkeypatch.setattr(m, "_github_code_search_paths", lambda *a, **k: [])
    monkeypatch.setattr(m, "_github_grep_repo_for_terms", lambda *a, **k: [])
    monkeypatch.setattr(m, "_github_tarball_grep", lambda *a, **k: [
        path for path, raw in state["sources"].items() if any(term in raw for term in a[4])])
    monkeypatch.setattr(m, "_github_api_post", lambda *a, **k: {"ok": True})
    def open_pr(**kwargs):
        state["pr_bodies"].append(kwargs["body"])
        assert kwargs["draft"] is True
        return {"html_url": "https://github.com/review/site/pull/1", "number": 1}

    monkeypatch.setattr(m, "_ouvrir_pull_request", open_pr)

    def get(path, **kwargs):
        if "/git/trees/" in path:
            return {"tree": [{"type": "blob", "path": p} for p in state["sources"]]}
        if "/git/ref" in path:
            return {"object": {"sha": "base"}}
        file_path = unquote(path.split("/contents/", 1)[1])
        raw = state["written"].get(file_path, state["sources"].get(file_path))
        if raw is None:
            raise FileNotFoundError(file_path)
        return {"sha": "original", "content": base64.b64encode(raw.encode()).decode()}

    def put(path, *, json_body, **kwargs):
        file_path = unquote(path.split("/contents/", 1)[1])
        state["written"][file_path] = base64.b64decode(json_body["content"]).decode()
        return {"content": {"sha": "patched"}, "commit": {"sha": "commit"}}

    def model(**kwargs):
        state["ai_calls"].append(kwargs["file_path"])
        return {"no_change": True, "patched_content": kwargs["file_content"]}

    monkeypatch.setattr(m, "_github_api_get", get)
    monkeypatch.setattr(m, "_github_api_put", put)
    monkeypatch.setattr(m, "_openai_generate_file_patch", model)

    def run(mode):
        state["written"].clear()
        state["ai_calls"].clear()
        if mode == "individual":
            response = m.api_issue_deep_fix(request, "review", state["key"],
                                          m._DeepFixBody(crawl_ts=state["crawl_ts"], url=URL))
        else:
            response = m.api_github_bulk_fix(request, "review")
        result = {
            "status": response.status_code,
            "response": json.loads(response.body),
            "written": dict(state["written"]),
            "ai_calls": list(state["ai_calls"]),
        }
        return result

    return state, run


@pytest.mark.parametrize("case,key", [
    ("http_links_control", "https_page_has_internal_links_to_http"),
    ("og_url", "open_graph_url_not_matching_canonical"),
    ("og_url_after_https_repair", "open_graph_url_not_matching_canonical"),
    ("sitemap_missing", "sitemap_xml_not_found"),
    ("sitemap_invalid", "sitemap_invalid_format"),
    ("served_lang_next", "served_html_lang_mismatch"),
    ("hreflang_in_page", "more_than_one_page_for_same_language_in_hreflang"),
])
def test_bulk_matches_individual_on_mechanical_cases(harness, case, key):
    state, run = harness
    state["key"] = key
    state["sources"] = {"index.html": HTML, "robots.txt": "User-agent: *\nAllow: /\n"}
    block = {"count": 1, "examples": [URL]}
    page = {"url": URL, "final_url": URL, "status_code": 200, "content_type": "text/html", "canonical": URL, "lang": "fr"}
    if case == "http_links_control":
        state["sources"]["index.html"] = HTML.replace("</body>", '<a href="http://site.test/contact">Contact</a></body>')
    elif case == "og_url":
        state["sources"]["index.html"] = HTML.replace("</head>", '<meta property="og:url" content="https://site.test/obsolete" />\n</head>')
        page["og_url"] = "https://site.test/obsolete"
    elif case == "og_url_after_https_repair":
        state["sources"]["index.html"] = HTML.replace("</head>", '<meta property="og:url" content="https://site.test" />\n</head>')
        page["canonical"] = "http://site.test/"
        page["og_url"] = "https://site.test"
    elif case == "sitemap_invalid":
        state["sources"]["sitemap.xml"] = "<urlset><url><loc>broken"
    elif case == "served_lang_next":
        state["sources"] = {
            "package.json": json.dumps({"name": "review", "scripts": {"build": "next build"}}),
            "app/layout.tsx": 'export default function Layout({children}) { return <html lang="fr"><body>{children}</body></html>; }\n',
            "app/page.tsx": 'export default function Page() { return <h1>Une page de test</h1>; }\n',
            "next.config.js": "module.exports = { output: 'export' };\n",
        }
        block["evidence"] = {"kind": "page_values", "items": [{"page": URL, "field": "lang", "value": "en"}]}
    elif case == "hreflang_in_page":
        state["sources"]["index.html"] = HTML.replace("</head>",
            '<link rel="alternate" hreflang="fr" href="' + URL + '" />\n'
            '<link rel="alternate" hreflang="fr" href="https://site.test/other" />\n</head>')
        state["sources"]["sitemap.xml"] = m._sitemap_xml([URL])
        block["evidence"] = {"kind": "hreflang_pairs", "items": [{
            "page": URL, "code": "fr", "from": "https://site.test/other", "to": URL, "where": "page"}]}
    state["report"] = {"issues": {key: block}, "pages": [page]}
    individual = run("individual")
    bulk = run("bulk")
    print(json.dumps({"case": case, "individual": {"status": individual["status"], "files": list(individual["written"])},
                      "bulk": {"status": bulk["status"], "files": list(bulk["written"]), "ai_calls": bulk["ai_calls"]}}))
    assert individual["status"] == 200 and individual["written"], individual
    assert bulk["status"] == 200 and bulk["written"], bulk
    assert individual["written"] == bulk["written"]
    assert len(set(individual["response"]["files"])) == individual["response"]["files_count"]
    assert not individual["ai_calls"] and not bulk["ai_calls"]
    assert state["charges"] == [0, 0]


def test_og_url_does_not_rewrite_a_different_scheme():
    raw = '<meta property="og:url" content="http://site.test/blog" />'
    output, count = m._rewrite_og_url(raw, [{"from": "https://site.test/blog", "to": "https://site.test/blog/"}])
    assert count == 0 and output == raw, "A value absent from the exact measured pair was rewritten."


@pytest.mark.parametrize("mode", ["individual", "url_preview", "github_preview", "github_confirm"])
@pytest.mark.parametrize("variant", ["", "_indexable", "_not_indexable", "uppercase", "whitespace"])
def test_http_served_https_canonical_refuses_manual_endpoints_without_writes_or_charges(harness, monkeypatch, mode, variant):
    state, run = harness
    key = "canonical_from_http_to_https" + (variant if variant.startswith("_") else "")
    if variant == "uppercase":
        key = key.upper()
    elif variant == "whitespace":
        key = " " + key + " "
    state["key"] = key
    state["sources"] = {"index.html": HTML, "netlify.toml": "[build]\ncommand = 'build'\n"}
    state["report"] = {"issues": {key: {"count": 1, "examples": ["http://site.test/"]}}}

    def forbidden(*args, **kwargs):
        pytest.fail("No GitHub, model, quota gate or charge for hosting advice")

    for name in ("_github_api_get", "_github_api_post", "_github_api_put", "_openai_generate_file_patch",
                 "_openai_url_fix", "_correction_gate", "_correction_charge"):
        monkeypatch.setattr(m, name, forbidden)
    for name in ("find", "pending", "operation"):
        monkeypatch.setattr(m.correction_journal, name, forbidden)
    if mode == "individual":
        result = run("individual")
        assert result["status"] == 422 and not result["written"] and not result["ai_calls"]
        body = result["response"]
    else:
        request = Request({"type": "http", "method": "POST", "path": "/", "headers": []})
        request.state.user = state["user"]
        if mode == "url_preview":
            response = m.api_issue_url_fix(request, "review", key, url="http://site.test/")
        else:
            response = m.api_github_fix(request, "review", key, m._GithubFixBody(url="http://site.test/",
                confirm=mode == "github_confirm", file_path="index.html", patched_content=HTML))
        assert response.status_code == 422
        body = json.loads(response.body)
    assert body["advisory"] is True and "canonical" in body["error"]
    assert not state["charges"] and not state["pr_bodies"]


@pytest.mark.parametrize("mode", ["individual", "bulk"])
def test_shared_dead_canonical_keeps_each_page_and_its_alternates(harness, mode):
    state, run = harness
    key, dead, other = "canonical_points_to_4xx", URL + "gone", URL + "other"
    source = HTML.replace('rel="canonical" href="' + URL, 'rel="canonical" href="' + dead).replace(
        "</head>", '<link rel="alternate" hreflang="fr" href="' + dead + '" /></head>')
    state.update(key=key, sources={"index.html": source, "other.html": source, "control.html": source},
        report={"issues": {key: {"count": 2, "examples": [u + " -> " + dead for u in (URL, other)]}},
                "pages": [{"url": u, "status_code": 200, "canonical": dead} for u in (URL, other)]
                         + [{"url": dead, "status_code": 404}]})
    result = run(mode)
    assert result["status"] == 200 and set(result["written"]) == {"index.html", "other.html"}
    for name, own in (("index.html", URL), ("other.html", other)):
        assert result["written"][name] == source.replace('rel="canonical" href="' + dead,
                                                          'rel="canonical" href="' + own)
    assert not result["ai_calls"] and state["charges"] == [0]


@pytest.mark.parametrize("mode", ["individual", "bulk"])
@pytest.mark.parametrize("key", ["canonical_points_to_redirect", "non_canonical_page_specified_as_canonical_one"])
def test_verified_canonical_destination_is_mechanical_in_both_endpoints(harness, mode, key):
    state, run = harness
    old, destination = URL + "old", URL + "master"
    source = HTML.replace('rel="canonical" href="' + URL, 'rel="canonical" href="' + old).replace(
        "</head>", '<link rel="alternate" hreflang="fr" href="' + old + '" /></head>')
    rows = [{"url": URL, "status_code": 200, "content_type": "text/html", "canonical": old},
            {"url": old, "status_code": 200, "content_type": "text/html", "canonical": destination},
            {"url": destination, "status_code": 200, "content_type": "text/html", "canonical": destination}]
    if key == "canonical_points_to_redirect":
        rows[1].update(final_url=destination, redirect_statuses=[301])
    state.update(key=key, sources={"index.html": source, "control.html": source}, report={"pages": rows,
        "issues": {key: {"count": 1, "examples": [URL], "evidence": {"kind": "url_pairs", "items": [
            {"page": URL, "from": old, "to": destination}]}}}})
    result = run(mode)
    assert result["status"] == 200 and set(result["written"]) == {"index.html"}
    assert result["written"]["index.html"] == source.replace('rel="canonical" href="' + old,
                                                              'rel="canonical" href="' + destination)
    assert not result["ai_calls"] and state["charges"] == [0]


@pytest.mark.parametrize("mode", ["individual", "bulk"])
@pytest.mark.parametrize("observed", [True, False])
def test_https_canonical_upgrade_requires_observed_destination_in_both_endpoints(harness, mode, observed):
    state, run = harness
    key, old, destination = "canonical_from_https_to_http", "http://master.test/target", "https://master.test/target"
    source = HTML.replace('rel="canonical" href="' + URL, 'rel="canonical" href="' + old)
    rows = [{"url": URL, "status_code": 200, "content_type": "text/html", "canonical": old}]
    if observed:
        rows.append({"url": destination, "status_code": 200, "content_type": "text/html", "canonical": destination})
    state.update(key=key, sources={"index.html": source, "control.html": source}, report={"pages": rows,
        "issues": {key: {"count": 1, "examples": [URL], "evidence": {"kind": "url_pairs", "items": [
            {"page": URL, "from": old, "to": destination}]}}}})
    result = run(mode)
    assert not result["ai_calls"]
    if observed:
        assert result["status"] == 200 and set(result["written"]) == {"index.html"}
        assert result["written"]["index.html"] == source.replace('rel="canonical" href="' + old,
                                                                  'rel="canonical" href="' + destination)
        assert state["charges"] == [0]
    else:
        assert not result["written"] and not state["pr_bodies"] and not any(state["charges"])
        assert result["response"].get("error") or result["response"].get("results")


@pytest.mark.parametrize("mode", ["url_preview", "github_preview", "github_confirm"])
@pytest.mark.parametrize("key", ["canonical_from_https_to_http", "duplicate_pages_without_canonical",
    "duplicate_pages_without_canonical_indexable", " DUPLICATE_PAGES_WITHOUT_CANONICAL ",
    "sitemap_non_canonical_page", "sitemap_non_canonical_page_indexable", " SITEMAP_NON_CANONICAL_PAGE ",
    "sitemap_3xx_redirect", "sitemap_3xx_redirect_indexable", " SITEMAP_3XX_REDIRECT ",
    "sitemap_noindex_page", "sitemap_noindex_page_not_indexable", " SITEMAP_NOINDEX_PAGE ",
    "sitemap_4xx_page", "sitemap_4xx_page_not_indexable", " SITEMAP_4XX_PAGE ",
    "sitemap_http_urls_for_https", "sitemap_http_urls_for_https_indexable", " SITEMAP_HTTP_URLS_FOR_HTTPS ",
    "indexable_page_not_in_sitemap", "indexable_page_not_in_sitemap_indexable", " INDEXABLE_PAGE_NOT_IN_SITEMAP ",
    "viewport_not_set", "viewport_not_set_indexable", " VIEWPORT_NOT_SET ",
    "twitter_card_missing", "twitter_card_missing_not_indexable", " TWITTER_CARD_MISSING ",
    "hreflang_defined_but_html_lang_missing", "hreflang_defined_but_html_lang_missing_not_indexable",
    " HREFLANG_DEFINED_BUT_HTML_LANG_MISSING ",
    "hreflang_to_non_canonical", "hreflang_to_non_canonical_not_indexable", " HREFLANG_TO_NON_CANONICAL "])
def test_canonical_cannot_bypass_evidence_through_model_preview(harness, monkeypatch, mode, key):
    state, _ = harness

    def forbidden(*args, **kwargs):
        pytest.fail("Use the evidence-bound deep fixer, never a cached or model-generated canonical preview")

    for name in ("_github_api_get", "_github_api_post", "_github_api_put", "_openai_url_fix",
                 "_openai_generate_file_patch", "_correction_gate", "_correction_charge"):
        monkeypatch.setattr(m, name, forbidden)
    for name in ("find", "pending", "operation"):
        monkeypatch.setattr(m.correction_journal, name, forbidden)
    request = Request({"type": "http", "method": "POST", "path": "/", "headers": []})
    request.state.user = state["user"]
    if mode == "url_preview":
        response = m.api_issue_url_fix(request, "review", key, url=URL)
    else:
        response = m.api_github_fix(request, "review", key, m._GithubFixBody(url=URL,
            confirm=mode == "github_confirm", file_path="index.html", patched_content=HTML))
    assert response.status_code == 422 and json.loads(response.body)["needs_deep_fix"] is True
    assert not state["charges"] and not state["pr_bodies"]


@pytest.mark.parametrize("mode", ["individual", "bulk"])
@pytest.mark.parametrize("case", ["verified", "different_observations", "different_current_body", "partial_verified"])
def test_duplicate_canonical_endpoints_are_free_bounded_and_require_human_review(harness, monkeypatch, mode, case):
    state, run = harness
    key, other, control = "duplicate_pages_without_canonical", URL + "other", URL + "control"
    source = HTML.replace('<link rel="canonical" href="' + URL + '" />\n', '')
    state.update(key=key, sources={"index.html": source, "other.html": source, "control.html": HTML})
    def observation(url, **extra):
        return {"url": url, "final_url": url, "status_code": 200, "content_type": "text/html", "canonical": None,
            "title": "Shared title", "meta_description": DESCRIPTION, "lang": "fr", "h1": ["Shared heading"],
            "text_word_count": 80, "content_sketch": list(range(30)), "image_urls": [], "internal_links": [],
            "external_links": [], "ld_json_blocks": 0, **extra}
    impacted = [URL, other]
    rows = [observation(URL), observation(other)]
    if case == "different_observations":
        rows[1]["content_sketch"] = list(range(1, 31))
    elif case == "different_current_body":
        state["sources"]["other.html"] = source.replace("<h1>", "<h1>Changed ")
    elif case == "partial_verified":
        impacted.append(control)
        rows.append(observation(control, content_sketch=list(range(1, 31))))
    state["report"] = {"issues": {key: {"count": len(impacted), "examples": impacted}}, "pages": rows}
    monkeypatch.setattr(m, "_project_github_cfg", lambda *a, **k: {"repo": "review/site", "branch": "main", "mode": "auto"})
    verification = []
    original = m._bloc_verification
    def capture(data, *, fusion_auto):
        verification.append(fusion_auto)
        return original(data, fusion_auto=fusion_auto)
    monkeypatch.setattr(m, "_bloc_verification", capture)
    def forbidden(*a, **k):
        pytest.fail("A canonical convention must not call a model")
    for name in ("_openai_generate_file_patch", "_ai_map_urls_to_files", "_ai_pick_repo_files"):
        monkeypatch.setattr(m, name, forbidden)
    result = run(mode)
    assert not result["ai_calls"]
    if case in {"verified", "partial_verified"}:
        assert result["status"] == 200 and set(result["written"]) == {"index.html", "other.html"}
        assert all(raw == source.replace("</head>", '<link rel="canonical" href="' + URL + '" />\n</head>')
                   for raw in result["written"].values())
        assert state["charges"] == [0] and state["pr_bodies"]
        assert all("CONVENTION" in body.upper() for body in state["pr_bodies"])
        assert verification and not any(verification)
        if case == "partial_verified":
            assert any(control in body for body in state["pr_bodies"])
    else:
        assert not result["written"] and not state["pr_bodies"] and not any(state["charges"])


@pytest.mark.parametrize("mode", ["individual", "bulk"])
@pytest.mark.parametrize("case", ["verified", "unobserved", "stale_head", "partial_verified"])
def test_canonical_sitemap_cleanup_is_free_and_keeps_other_entries_and_all_html(harness, mode, case):
    state, run = harness
    key, master, control = "sitemap_non_canonical_page", URL + "other", URL + "control"
    alias = '<url><loc>' + URL + '</loc><lastmod>2001-01-01</lastmod></url>'
    master_entry = '<url><loc>' + master + '</loc><lastmod>2026-01-01</lastmod></url>'
    control_entry = '<url><loc>' + control + '</loc></url>'
    xml = '<urlset>' + alias + master_entry + control_entry + '</urlset>'
    state.update(key=key, sources={"sitemap.xml": xml, "index.html": HTML.replace(URL, master),
        "other.html": HTML.replace(URL, master), "control.html": HTML.replace(URL, control)})
    pair = {"page": URL, "from": URL, "to": master}
    rows = [{"url": u, "final_url": u, "status_code": 200, "content_type": "text/html", "canonical": master}
            for u in (URL, master)]
    pairs = [pair]
    if case == "unobserved":
        rows = rows[:1]
    elif case == "stale_head":
        state["sources"]["index.html"] = HTML
    elif case == "partial_verified":
        pairs.append({"page": control, "from": control, "to": URL + "absent"})
        rows.append({"url": control, "status_code": 200, "content_type": "text/html", "canonical": URL + "absent"})
    state["report"] = {"issues": {key: {"count": len(pairs), "examples": [p["from"] for p in pairs],
        "evidence": {"kind": "url_pairs", "items": pairs}}}, "pages": rows}
    result = run(mode)
    assert not result["ai_calls"]
    if case in {"verified", "partial_verified"}:
        assert result["status"] == 200 and result["written"] == {"sitemap.xml": xml.replace(alias, "")}
        assert state["charges"] == [0] and state["pr_bodies"]
        if case == "partial_verified":
            assert any(control in body for body in state["pr_bodies"])
    else:
        assert not result["written"] and not state["pr_bodies"] and not any(state["charges"])


@pytest.mark.parametrize("mode", ["individual", "bulk"])
@pytest.mark.parametrize("case", ["verified", "unobserved", "stale_rule", "partial_verified"])
def test_sitemap_redirect_endpoints_keep_rules_html_and_verified_master_metadata(harness, mode, case):
    state, run = harness
    key, master, dead = "sitemap_3xx_redirect", URL + "other", URL + "dead"
    alias = '<url><loc>' + URL + '</loc><lastmod>2001-01-01</lastmod></url>'
    master_entry = '<url><loc>' + master + '</loc><lastmod>2026-01-01</lastmod></url>'
    dead_entry = '<url><loc>' + dead + '</loc></url>'
    xml = '<urlset>' + alias + master_entry + dead_entry + '</urlset>'
    rules = "/ /other 301!\n/dead /absent 301\n"
    state.update(key=key, sources={"sitemap.xml": xml, "_redirects": rules,
        "index.html": HTML, "other.html": HTML.replace(URL, master)})
    rows = [{"url": URL, "final_url": master, "status_code": 200, "content_type": "text/html", "canonical": master,
             "redirect_chain": [URL], "redirect_statuses": [301]},
            {"url": master, "status_code": 200, "content_type": "text/html", "canonical": master}]
    pairs = [{"page": URL, "from": URL, "to": master}]
    if case == "unobserved":
        rows = rows[:1]
    elif case == "stale_rule":
        state["sources"]["_redirects"] = rules.replace("/ /other", "/ /absent")
    elif case == "partial_verified":
        pairs.append({"page": dead, "from": dead, "to": URL + "absent"})
    state["report"] = {"issues": {key: {"count": len(pairs), "examples": [p["from"] for p in pairs],
        "evidence": {"kind": "url_pairs", "items": pairs}}}, "pages": rows}
    result = run(mode)
    assert not result["ai_calls"]
    if case in {"verified", "partial_verified"}:
        assert result["status"] == 200 and result["written"] == {"sitemap.xml": xml.replace(alias, "")}
        assert state["charges"] == [0] and state["pr_bodies"]
        if case == "partial_verified":
            assert any(dead in body for body in state["pr_bodies"])
    else:
        assert not result["written"] and not state["pr_bodies"] and not any(state["charges"])


@pytest.mark.parametrize("mode", ["individual", "bulk"])
@pytest.mark.parametrize("case", ["verified", "unobserved", "stale_robots", "partial_verified"])
def test_sitemap_noindex_endpoints_delist_only_verified_entries_and_require_review(harness, monkeypatch, mode, case):
    state, run = harness
    key, control = "sitemap_noindex_page", URL + "control"
    entry = '<url><loc>' + URL + '</loc><priority>0.1</priority></url>'
    xml = '<urlset>' + entry + '<url><loc>' + control + '</loc></url></urlset>'
    source = HTML.replace('</head>', '<meta name="robots" content="noindex, follow" /></head>')
    state.update(key=key, sources={"sitemap.xml": xml, "index.html": source,
                                  "control.html": HTML.replace(URL, control)})
    rows = [{"url": URL, "final_url": URL, "status_code": 200, "content_type": "text/html",
             "canonical": URL, "meta_robots": "noindex, follow", "meta_robots_tag_count": 1,
             "redirect_chain": [], "redirect_statuses": []}]
    impacted = [URL]
    if case == "unobserved":
        rows = []
    elif case == "stale_robots":
        state["sources"]["index.html"] = source.replace("noindex, follow", "index, follow")
    elif case == "partial_verified":
        impacted.append(control)
        rows.append(dict(rows[0], url=control, final_url=control, meta_robots="index, follow"))
    state["report"] = {"issues": {key: {"count": len(impacted), "examples": impacted}}, "pages": rows}
    monkeypatch.setattr(m, "_project_github_cfg", lambda *a, **k: {"repo": "review/site", "branch": "main", "mode": "auto"})
    verification, original = [], m._bloc_verification
    def capture(data, *, fusion_auto):
        verification.append(fusion_auto)
        return original(data, fusion_auto=fusion_auto)
    monkeypatch.setattr(m, "_bloc_verification", capture)
    def forbidden(*a, **k):
        pytest.fail("No model or inferred target for a noindex sitemap repair")
    for name in ("_openai_generate_file_patch", "_ai_map_urls_to_files", "_ai_pick_repo_files"):
        monkeypatch.setattr(m, name, forbidden)
    result = run(mode)
    assert not result["ai_calls"]
    if case in {"verified", "partial_verified"}:
        assert result["status"] == 200 and result["written"] == {"sitemap.xml": xml.replace(entry, "")}
        assert state["charges"] == [0] and state["pr_bodies"]
        assert all("noindex" in body.lower() for body in state["pr_bodies"])
        assert verification and not any(verification)
        if case == "partial_verified":
            assert any(control in body for body in state["pr_bodies"])
    else:
        assert not result["written"] and not state["pr_bodies"] and not any(state["charges"])


@pytest.mark.parametrize("mode", ["individual", "bulk"])
@pytest.mark.parametrize("case", ["verified", "unobserved", "protected", "stale_http", "restored_source", "partial_verified"])
def test_sitemap_error_endpoints_delist_only_still_missing_entries_with_human_review(harness, monkeypatch, mode, case):
    state, run = harness
    key, control, protected = "sitemap_4xx_page", URL + "control", URL + "protected"
    entry = '<url><loc>' + URL + '</loc><priority>0.1</priority></url>'
    xml = '<urlset>' + entry + '<url><loc>' + control + '</loc></url></urlset>'
    state.update(key=key, sources={"sitemap.xml": xml, "control.html": HTML.replace(URL, control)})
    rows = [{"url": URL, "final_url": URL, "status_code": 404, "content_type": "text/html",
             "error": None, "blocked_by_host": False, "redirect_chain": [], "redirect_statuses": []}]
    impacted = [URL]
    if case == "unobserved":
        rows = []
    elif case == "protected":
        rows[0]["status_code"] = 403
    elif case == "restored_source":
        state["sources"]["index.html"] = HTML
    elif case == "partial_verified":
        impacted.append(protected)
        rows.append(dict(rows[0], url=protected, final_url=protected, status_code=403))
        state["sources"]["sitemap.xml"] = xml = xml.replace('</urlset>', '<url><loc>' + protected + '</loc></url></urlset>')
    state["report"] = {"issues": {key: {"count": len(impacted), "examples": impacted}}, "pages": rows}
    monkeypatch.setattr(m, "_sitemap_error_status", lambda url: 200 if case == "stale_http" else 404)
    monkeypatch.setattr(m, "_project_github_cfg", lambda *a, **k: {"repo": "review/site", "branch": "main", "mode": "auto"})
    verification, original = [], m._bloc_verification
    def capture(data, *, fusion_auto):
        verification.append(fusion_auto)
        return original(data, fusion_auto=fusion_auto)
    monkeypatch.setattr(m, "_bloc_verification", capture)
    def forbidden(*a, **k):
        pytest.fail("No model, inferred target or page creation for missing sitemap entries")
    for name in ("_openai_generate_file_patch", "_ai_map_urls_to_files", "_ai_pick_repo_files"):
        monkeypatch.setattr(m, name, forbidden)
    result = run(mode)
    assert not result["ai_calls"]
    if case in {"verified", "partial_verified"}:
        assert result["status"] == 200 and result["written"] == {"sitemap.xml": xml.replace(entry, "")}
        assert state["charges"] == [0] and state["pr_bodies"]
        assert all("404/410" in body for body in state["pr_bodies"])
        assert verification and not any(verification)
        if case == "partial_verified":
            assert any(protected in body for body in state["pr_bodies"])
    else:
        assert not result["written"] and not state["pr_bodies"] and not any(state["charges"])


@pytest.mark.parametrize("mode", ["individual", "bulk"])
@pytest.mark.parametrize("case", ["verified", "collision", "unobserved", "stale_head", "fresh_http_refused", "partial_verified"])
def test_sitemap_https_endpoints_only_upgrade_verified_entries_without_ai(harness, monkeypatch, mode, case):
    state, run = harness
    key, old, control = "sitemap_http_urls_for_https", "http://site.test/", "http://site.test/control"
    alias = '<url><loc>' + old + '</loc><priority>0.1</priority></url>'
    xml = '<urlset>' + alias + '<url><loc>' + control + '</loc></url></urlset>'
    if case == "collision":
        xml = xml.replace('</urlset>', '<url><loc>' + URL + '</loc><priority>0.9</priority></url></urlset>')
    state.update(key=key, sources={"sitemap.xml": xml, "index.html": HTML,
                                  "control.html": HTML.replace(URL, URL + "control")})
    rows = [{"url": URL, "final_url": URL, "status_code": 200, "content_type": "text/html", "canonical": URL}]
    impacted = [old]
    if case == "unobserved":
        rows = []
    elif case == "stale_head":
        state["sources"]["index.html"] = HTML.replace(URL, URL + "control")
    elif case == "partial_verified":
        impacted.append(control)
        rows.append(dict(rows[0], url=URL + "control", final_url=URL + "control", canonical=URL + "control", meta_robots="noindex"))
    state["report"] = {"issues": {key: {"count": len(impacted), "examples": impacted}}, "pages": rows}
    monkeypatch.setattr(m, "_sitemap_https_page", lambda *a: case != "fresh_http_refused")
    result = run(mode)
    assert not result["ai_calls"]
    if case in {"verified", "collision", "partial_verified"}:
        expected = xml.replace(alias, '') if case == "collision" else xml.replace('<loc>' + old + '</loc>', '<loc>' + URL + '</loc>')
        assert result["status"] == 200 and result["written"] == {"sitemap.xml": expected}
        assert state["charges"] == [0] and state["pr_bodies"]
        if case == "partial_verified":
            assert any(control in body for body in state["pr_bodies"])
    else:
        assert not result["written"] and not state["pr_bodies"] and not any(state["charges"])


@pytest.mark.parametrize('mode', ['individual', 'bulk'])
@pytest.mark.parametrize('case', ['verified', 'unobserved', 'stale_noindex', 'fresh_http_refused', 'partial', 'already_present'])
def test_sitemap_add_endpoints_only_add_observed_currently_indexable_pages(harness, monkeypatch, mode, case):
    state, run = harness
    key, control = 'indexable_page_not_in_sitemap', URL + 'control'
    xml = '<urlset><url><loc>' + control + '</loc><priority>0.9</priority></url></urlset>'
    state.update(key=key, sources={'sitemap.xml': xml, 'index.html': HTML})
    rows = [{'url': URL, 'final_url': URL, 'status_code': 200, 'content_type': 'text/html', 'canonical': URL}]
    impacted = [URL]
    if case == 'unobserved':
        rows = []
    elif case == 'stale_noindex':
        state['sources']['index.html'] = HTML.replace('</head>', '<meta name="robots" content="noindex" /></head>')
    elif case == 'partial':
        impacted.append(control)
        rows.append(dict(rows[0], url=control, final_url=control, canonical=control, meta_robots='noindex'))
    elif case == 'already_present':
        state['sources']['sitemap.xml'] = xml.replace('</urlset>', '<url><loc>' + URL + '</loc></url></urlset>')
    state['report'] = {'issues': {key: {'count': len(impacted), 'examples': impacted}}, 'pages': rows}
    monkeypatch.setattr(m, '_sitemap_https_page', lambda *a: case != 'fresh_http_refused')
    result = run(mode)
    assert not result['ai_calls']
    if case in {'verified', 'partial'}:
        assert result['status'] == 200 and result['written'] == {'sitemap.xml': xml.replace('</urlset>', '<url><loc>' + URL + '</loc></url>\n</urlset>')}
        assert state['charges'] == [0] and state['pr_bodies']
        if case == 'partial':
            assert any(control in body for body in state['pr_bodies'])
    else:
        assert not result['written'] and not state['pr_bodies'] and not any(state['charges'])


@pytest.mark.parametrize('mode', ['individual', 'bulk'])
@pytest.mark.parametrize('case', ['verified', 'unobserved', 'unknown_count', 'current_viewport', 'stale_noindex',
    'fresh_http_refused', 'partial', 'scripted'])
def test_viewport_endpoints_add_only_one_verified_tag_without_ai_or_charge(harness, monkeypatch, mode, case):
    state, run = harness
    key, other = 'viewport_not_set', URL + 'other'
    state.update(key=key, sources={'index.html': HTML, 'other.html': HTML.replace(URL, other)})
    rows = [{'url': URL, 'final_url': URL, 'status_code': 200, 'content_type': 'text/html', 'canonical': URL,
             'meta_viewport': None, 'meta_viewport_tag_count': 0}]
    impacted = [URL]
    if case == 'unobserved':
        rows = []
    elif case == 'unknown_count':
        del rows[0]['meta_viewport_tag_count']
    elif case == 'current_viewport':
        state['sources']['index.html'] = HTML.replace('</head>', '<meta name="viewport" content="custom" /></head>')
    elif case == 'stale_noindex':
        state['sources']['index.html'] = HTML.replace('</head>', '<meta name="robots" content="noindex" /></head>')
    elif case == 'scripted':
        state['sources']['index.html'] = HTML.replace('</body>', '<script src="/app.js"></script></body>')
    elif case == 'partial':
        impacted.append(other)
        rows.append(dict(rows[0], url=other, final_url=other, canonical=other, meta_robots='noindex'))
    state['report'] = {'issues': {key: {'count': len(impacted), 'examples': impacted}}, 'pages': rows}
    monkeypatch.setattr(m, '_sitemap_https_page', lambda *a: case != 'fresh_http_refused')
    result = run(mode)
    assert not result['ai_calls']
    if case in {'verified', 'partial'}:
        tag = '<meta name="viewport" content="width=device-width, initial-scale=1" />\n'
        assert result['status'] == 200 and result['written'] == {'index.html': HTML.replace('</head>', tag + '</head>')}
        assert state['charges'] == [0] and state['pr_bodies']
        if case == 'partial':
            assert any(other in body for body in state['pr_bodies'])
    else:
        assert not result['written'] and not state['pr_bodies'] and not any(state['charges'])


@pytest.mark.parametrize('mode', ['individual', 'bulk'])
@pytest.mark.parametrize('case', ['verified', 'unobserved', 'unknown_fields', 'no_image', 'current_card', 'stale_values',
    'stale_noindex', 'fresh_http_refused', 'partial', 'scripted'])
def test_twitter_endpoints_preserve_existing_values_and_never_invent_an_image(harness, monkeypatch, mode, case):
    from tests.test_verified_twitter_card import HTML as TW_HTML, page as tw_page, TAGS, A
    state, run = harness
    key, other = 'twitter_card_missing', URL + 'other'
    source = TW_HTML.replace(A, URL)
    rows, impacted = [tw_page(URL)], [URL]
    state.update(key=key, sources={'index.html': source, 'other.html': source.replace(URL, other)})
    if case == 'unobserved':
        rows = []
    elif case == 'unknown_fields':
        del rows[0]['twitter_card']
    elif case == 'no_image':
        rows[0].update(twitter_image=None, og_image=None)
    elif case == 'current_card':
        state['sources']['index.html'] = source.replace('</head>', TAGS[0] + '</head>')
    elif case == 'stale_values':
        state['sources']['index.html'] = source.replace('OG description', 'Other description')
    elif case == 'stale_noindex':
        state['sources']['index.html'] = source.replace('</head>', '<meta name="robots" content="noindex" /></head>')
    elif case == 'scripted':
        state['sources']['index.html'] = source.replace('</body>', '<script src="/app.js"></script></body>')
    elif case == 'partial':
        impacted.append(other)
        rows.append(tw_page(other, meta_robots='noindex'))
    state['report'] = {'issues': {key: {'count': len(impacted), 'examples': impacted}}, 'pages': rows}
    monkeypatch.setattr(m, '_twitter_missing_page', lambda *a: case != 'fresh_http_refused')
    result = run(mode)
    assert not result['ai_calls']
    if case in {'verified', 'partial'}:
        assert result['status'] == 200 and result['written'] == {'index.html': source.replace('</head>', '\n'.join(TAGS) + '\n</head>')}
        assert state['charges'] == [0] and state['pr_bodies']
        if case == 'partial':
            assert any(other in body for body in state['pr_bodies'])
    else:
        assert not result['written'] and not state['pr_bodies'] and not any(state['charges'])


@pytest.mark.parametrize('mode', ['individual', 'bulk'])
@pytest.mark.parametrize('case', ['verified', 'unobserved', 'unknown_fields', 'ambiguous_language', 'current_lang',
    'stale_values', 'stale_noindex', 'fresh_http_refused', 'partial', 'scripted'])
def test_self_hreflang_language_endpoints_add_only_proved_root_lang_without_ai(harness, monkeypatch, mode, case):
    from tests.test_verified_hreflang_lang import HTML as LANG_HTML, page as lang_page, A
    state, run = harness
    key, other = 'hreflang_defined_but_html_lang_missing', URL + 'other'
    source = LANG_HTML.replace(A, URL)
    rows, impacted = [lang_page(URL)], [URL]
    state.update(key=key, sources={'index.html': source, 'other.html': source.replace(URL, other)})
    if case == 'unobserved':
        rows = []
    elif case == 'unknown_fields':
        del rows[0]['served_lang']
    elif case == 'ambiguous_language':
        rows[0]['hreflang_raw'].append({'hreflang': 'en', 'href': URL})
        rows[0]['hreflang']['en'] = URL
    elif case == 'current_lang':
        state['sources']['index.html'] = source.replace('<html>', '<html lang="fr">')
    elif case == 'stale_values':
        state['sources']['index.html'] = source.replace('hreflang="fr"', 'hreflang="en"')
    elif case == 'stale_noindex':
        state['sources']['index.html'] = source.replace('</head>', '<meta name="robots" content="noindex" /></head>')
    elif case == 'scripted':
        state['sources']['index.html'] = source.replace('</body>', '<script src="/app.js"></script></body>')
    elif case == 'partial':
        impacted.append(other)
        rows.append(lang_page(other, meta_robots='noindex'))
    state['report'] = {'issues': {key: {'count': len(impacted), 'examples': impacted}}, 'pages': rows}
    monkeypatch.setattr(m, '_hreflang_lang_page', lambda *a: case != 'fresh_http_refused')
    result = run(mode)
    assert not result['ai_calls']
    if case in {'verified', 'partial'}:
        assert result['status'] == 200 and result['written'] == {'index.html': source.replace('<html>', '<html lang="fr">')}
        assert state['charges'] == [0] and state['pr_bodies']
        if case == 'partial':
            assert any(other in body for body in state['pr_bodies'])
    else:
        assert not result['written'] and not state['pr_bodies'] and not any(state['charges'])


@pytest.mark.parametrize('mode', ['individual', 'bulk'])
@pytest.mark.parametrize('case', ['verified', 'unobserved', 'unknown_fields', 'wrong_language', 'no_return',
    'stale_values', 'stale_noindex', 'fresh_http_refused', 'partial', 'scripted'])
def test_canonical_hreflang_endpoints_preserve_other_head_fields_without_ai(harness, monkeypatch, mode, case):
    from tests.test_verified_hreflang_canonical import source, page, S, OLD, NEW
    state, run = harness
    key, other = 'hreflang_to_non_canonical', URL + 'other'
    raw = source().replace(S, URL)
    rows = [page(URL, 'fr', URL, [('fr', URL), ('en', OLD)]),
            page(OLD, 'en', NEW, [('fr', URL), ('en', NEW)], served=None),
            page(NEW, 'en', NEW, [('fr', URL), ('en', NEW)])]
    impacted, pairs = [URL], [{'page': URL, 'from': OLD, 'to': NEW}]
    state.update(key=key, sources={'index.html': raw})
    if case == 'unobserved':
        rows = []
    elif case == 'unknown_fields':
        del rows[2]['served_lang']
    elif case == 'wrong_language':
        rows[2]['lang'] = 'fr'
    elif case == 'no_return':
        rows[2]['hreflang'] = {'en': NEW}
        rows[2]['hreflang_raw'] = [{'hreflang': 'en', 'href': NEW}]
    elif case == 'stale_values':
        state['sources']['index.html'] = raw.replace('hreflang="en"', 'hreflang="de"')
    elif case == 'stale_noindex':
        state['sources']['index.html'] = raw.replace('</head>', '<meta name="robots" content="noindex" /></head>')
    elif case == 'scripted':
        state['sources']['index.html'] = raw.replace('</body>', '<script src="/app.js"></script></body>')
    elif case == 'partial':
        impacted.append(other)
        pairs.append({'page': other, 'from': OLD, 'to': NEW})
    state['report'] = {'issues': {key: {'count': len(impacted), 'examples': impacted,
        'evidence': {'kind': 'url_pairs', 'items': pairs}}}, 'pages': rows}
    monkeypatch.setattr(m, '_hreflang_canonical_page', lambda *a: case != 'fresh_http_refused')
    result = run(mode)
    assert not result['ai_calls']
    if case in {'verified', 'partial'}:
        assert result['status'] == 200 and result['written'] == {
            'index.html': raw.replace('hreflang="en" href="' + OLD, 'hreflang="en" href="' + NEW, 1)}
        assert state['charges'] == [0] and state['pr_bodies']
        if case == 'partial':
            assert any(other in body for body in state['pr_bodies'])
    else:
        assert not result['written'] and not state['pr_bodies'] and not any(state['charges'])


def test_bulk_does_not_silently_stop_at_five_families(harness, monkeypatch):
    state, run = harness
    keys = ["missing_title", "missing_meta_description", "missing_alt_text", "viewport_not_set",
            "multiple_h1", "open_graph_tags_missing", "twitter_card_missing"]
    state["sources"] = {"index.html": HTML}
    state["report"] = {"issues": {k: {"count": 1, "examples": [URL]} for k in keys},
                       "pages": [{"url": URL, "final_url": URL, "status_code": 200, "content_type": "text/html",
                                  "canonical": URL, "meta_viewport": None, "meta_viewport_tag_count": 0,
                                  "twitter_card": None, "twitter_title": 'Own title', "twitter_description": DESCRIPTION,
                                  "twitter_image": 'https://site.test/social.png'}]}
    monkeypatch.setattr(m, "_github_fixable_issue_candidates", ORIGINAL_CANDIDATES)
    processed = []

    def patch(**kwargs):
        processed.append(kwargs["issue_key"])
        return ["index.html"], [], ["index.html"], []

    monkeypatch.setattr(m, "_deep_patch_issue_files", patch)
    result = run("bulk")
    print(json.dumps({"case": "bulk_seven_families", "status": result["status"],
                      "eligible": len(keys), "processed": len(processed)}))
    assert result["status"] == 200, result
    assert len(processed) == len(keys), "Eligible families were dropped despite sufficient file budget."


@pytest.mark.parametrize("case", ["other_page_still_broken", "target_not_crawled", "indexability_variant"])
def test_verification_never_confirms_an_unproven_resolution(case):
    from backend.models import IssueTask, Project, User
    from sqlalchemy import select

    m.DB.create_tables()
    tag = uuid.uuid4().hex
    key = "missing_h1_indexable"
    other_url = "https://site.test/other"
    with m.DB.session() as db:
        user = User(email="review-" + tag + "@example.invalid", password_hash="unused")
        db.add(user)
        db.commit()
        project = Project(owner_user_id=user.id, slug="review-" + tag,
                          site_name="site.test", base_url=URL)
        db.add(project)
        db.commit()
        slug, project_id = project.slug, project.id
        task = IssueTask(project_id=project_id, user_id=user.id, issue_key=key,
                         issue_label="Missing H1", crawl_ts="20261001-090000", url=URL,
                         status="done", severity="warning",
                         note=json.dumps({"deep": True, "pages": 2, "pr_number": 1}))
        db.add(task)
        db.commit()
        task_id = task.id

    after = {"meta": {"timestamp": "20261001-100000"},
             "issues": {key: {"count": 1, "examples": [other_url]}},
             "pages": [{"url": URL, "status_code": 200}, {"url": other_url, "status_code": 200}]}
    if case == "target_not_crawled":
        after["pages"] = [{"url": other_url, "status_code": 200}]
    elif case == "indexability_variant":
        after["issues"] = {"missing_h1_not_indexable": {"count": 1, "examples": [URL]}}
    m._verify_corrections_after_crawl(slug, after)
    with m.DB.session() as db:
        task = db.scalar(select(IssueTask).where(IssueTask.id == task_id))
        verification = json.loads(task.note).get("verify", {})
    print(json.dumps({"case": case, "verification": verification}))
    assert verification.get("result") != "resolved", "The product confirms a repair that the crawl does not prove."


@pytest.fixture
def verification_task():
    from backend.models import IssueTask, Project, User

    m.DB.create_tables()

    def create(*, key="missing_h1_indexable", scope=None, slug=None, note=None):
        tag = uuid.uuid4().hex
        scope = scope if scope is not None else [URL, "https://site.test/other"]
        with m.DB.session() as db:
            user = User(email="review-" + tag + "@example.invalid", password_hash="unused")
            db.add(user)
            db.flush()
            project = Project(owner_user_id=user.id, slug=slug or "review-" + tag,
                              site_name="site.test", base_url=URL)
            db.add(project)
            db.flush()
            task = IssueTask(project_id=project.id, user_id=user.id, issue_key=key,
                             issue_label=key, crawl_ts="20261001-090000", url=scope[0] if scope else URL,
                             status="done", severity="warning", note=json.dumps(note or {
                                 "deep": True, "pages": len(scope), "verification_urls": scope}))
            db.add(task)
            db.flush()
            result = {"slug": project.slug, "user_id": user.id, "task_id": task.id, "key": key}
            db.commit()
            return result

    return create


def _verify(task, report, **kwargs):
    from backend.models import IssueTask
    from sqlalchemy import select

    m._verify_corrections_after_crawl(task["slug"], report, **kwargs)
    with m.DB.session() as db:
        row = db.scalar(select(IssueTask).where(IssueTask.id == task["task_id"]))
        return json.loads(row.note).get("verify", {})


@pytest.mark.parametrize("case,expected", [
    ("clean", "resolved"), ("clean_empty_issues", "resolved"),
    ("real_crawler_timestamp", "resolved"), ("worker_timestamp", "resolved"),
    ("missing_target", "unverified"), ("target_error", "unverified"),
    ("target_fetch_error", "unverified"), ("stale", "unverified"),
    ("time_budget", "unverified"), ("blocked_host", "unverified"),
    ("indexability_variant", "still_present"), ("other_page", "still_present"),
    ("short_becomes_long", "still_present"), ("unknown_legacy_scope", "unverified"),
    ("different_scheme", "unverified"), ("different_path_case", "unverified"),
    ("capped_examples", "unverified"),
])
def test_verification_requires_a_fresh_successful_scope(verification_task, case, expected):
    other = "https://site.test/other"
    kwargs = {}
    key = "title_too_short_indexable" if case == "short_becomes_long" else "missing_h1_indexable"
    note = {"deep": True, "pages": 2} if case == "unknown_legacy_scope" else None
    if case == "capped_examples":
        note = {"verification_urls": [URL]}
    scope = ["https://site.test/Page", other] if case == "different_path_case" else [URL, other]
    task = verification_task(key=key, scope=scope, note=note)
    report = {"meta": {"timestamp": "20261001-100000"}, "issues": {key: {"count": 0}},
              "pages": [{"url": url, "status_code": 200} for url in scope]}
    if case == "clean_empty_issues":
        report["issues"] = {}
    elif case == "real_crawler_timestamp":
        report["meta"] = {"started_at": "2026-10-01T10:00:00+00:00"}
    elif case == "worker_timestamp":
        report["meta"] = {}
        kwargs["crawl_ts"] = "20261001-100000"
    elif case == "missing_target":
        report["pages"] = report["pages"][:1]
    elif case == "target_error":
        report["pages"][1]["status_code"] = 500
    elif case == "target_fetch_error":
        report["pages"][1]["error"] = "timeout"
    elif case == "stale":
        report["meta"]["timestamp"] = "20261001-090000"
    elif case == "time_budget":
        report["meta"]["stopped_on_time_budget"] = True
    elif case == "blocked_host":
        report["meta"]["blocked_by_host"] = {"count": 1, "urls": [other]}
    elif case == "indexability_variant":
        report["issues"] = {"missing_h1_not_indexable": {"count": 1, "examples": [URL]}}
    elif case == "other_page":
        report["issues"][key] = {"count": 1, "examples": [other]}
    elif case == "short_becomes_long":
        report["issues"] = {"title_too_long_not_indexable": {"count": 1, "examples": [other]}}
    elif case == "different_scheme":
        report["pages"][0]["url"] = "http://site.test/"
    elif case == "different_path_case":
        report["pages"][0]["url"] = "https://site.test/page"
    elif case == "capped_examples":
        report["issues"][key] = {"count": 2000, "examples": [other]}
    assert _verify(task, report, **kwargs)["result"] == expected


def test_verification_is_isolated_by_project_owner(verification_task):
    first = verification_task()
    second = verification_task(slug=first["slug"])
    report = {"meta": {"timestamp": "20261001-100000"}, "issues": {},
              "pages": [{"url": URL, "status_code": 200},
                        {"url": "https://site.test/other", "status_code": 200}]}
    assert _verify(first, report) == {}
    assert _verify(second, report, user_id=second["user_id"])["result"] == "resolved"
    assert _verify(first, report) == {}


def test_system_fetches_can_prove_a_sitemap_repair(verification_task):
    url = "https://site.test/sitemap.xml"
    task = verification_task(key="sitemap_invalid_format", scope=[url])
    report = {"meta": {"timestamp": "20261001-100000"}, "issues": {}, "pages": [],
              "system_fetches": [{"url": url, "status_code": 200, "type": "sitemap"}]}
    assert _verify(task, report)["result"] == "resolved"


def test_truncated_original_examples_cannot_prove_a_whole_family(verification_task):
    task = verification_task(scope=[URL], note={"deep": True, "verification_urls": [URL],
                                                "verification_expected_count": 501})
    report = {"meta": {"timestamp": "20261001-100000"}, "issues": {},
              "pages": [{"url": URL, "status_code": 200}]}
    assert _verify(task, report)["result"] == "unverified"


def test_different_crawl_settings_do_not_prove_a_resolution(verification_task, monkeypatch, tmp_path):
    task = verification_task(scope=[URL])
    baseline = {"meta": {"resources_checked": True}, "issues": {
        task["key"]: {"count": 1, "examples": [URL]}}}
    monkeypatch.setattr(m.dash, "load_report_json", lambda *a: baseline)
    report = {"meta": {"timestamp": "20261001-100000", "resources_checked": False},
              "issues": {}, "pages": [{"url": URL, "status_code": 200}]}
    verification = _verify(task, report, runs_dir=tmp_path)
    assert verification["result"] == "unverified" and "introduced" not in verification


def test_a_non_html_response_does_not_prove_an_html_repair(verification_task):
    task = verification_task(scope=[URL])
    report = {"meta": {"timestamp": "20261001-100000"}, "issues": {},
              "pages": [{"url": URL, "status_code": 200, "content_type": "image/png"}]}
    assert _verify(task, report)["result"] == "unverified"


def test_noindex_cannot_hide_a_reciprocal_hreflang_defect(verification_task, monkeypatch, tmp_path):
    task = verification_task(key="missing_reciprocal_hreflang", scope=[URL])
    baseline = {"meta": {}, "issues": {task["key"]: {"count": 1, "examples": [URL]}},
                "pages": [{"url": URL, "status_code": 200}]}
    monkeypatch.setattr(m.dash, "load_report_json", lambda *a: baseline)
    report = {"meta": {"timestamp": "20261001-100000"}, "issues": {},
              "pages": [{"url": URL, "status_code": 200, "x_robots_tag": "noindex"}]}
    result = _verify(task, report, runs_dir=tmp_path)
    assert result["result"] == "unverified" and "noindex" in result["reason"]


def test_noindex_with_a_missing_baseline_is_not_proof(verification_task):
    task = verification_task(key="missing_reciprocal_hreflang", scope=[URL])
    report = {"meta": {"timestamp": "20261001-100000"}, "issues": {},
              "pages": [{"url": URL, "status_code": 200, "x_robots_tag": "noindex"}]}
    assert _verify(task, report)["result"] == "unverified"


@pytest.mark.parametrize("raw_url", ["http://site.test/blog", "https://site.test/blog/",
                                     "https://site.test/Blog", "https://site.test/blog?q=1"])
def test_og_url_requires_the_exact_measured_value(raw_url):
    raw = '<meta property="og:url" content="' + raw_url + '" />'
    assert m._rewrite_og_url(raw, [{"from": "https://site.test/blog", "to": URL}]) == (raw, 0)


def test_bulk_reports_every_family_excluded_by_the_budget(harness, monkeypatch):
    state, run = harness
    keys = ["missing_title", "missing_h1", "missing_alt_text", "viewport_not_set"]
    state.update({"budget": 2, "sources": {"index.html": HTML},
                  "report": {"issues": {k: {"count": 1, "examples": [URL]} for k in keys}}})
    monkeypatch.setattr(m, "_github_fixable_issue_candidates", lambda **k: [
        {"key": key, "label": key, "url": URL} for key in keys])
    seen = []

    def patch(**kwargs):
        seen.append(kwargs["max_files"])
        return ["index.html"], [], ["index.html"], ["index.html"]

    monkeypatch.setattr(m, "_deep_patch_issue_files", patch)
    result = run("bulk")["response"]
    assert result["ok"] and result["partial"]
    assert seen == [2, 1]
    assert result["fixed_count"] == 2 and result["total_count"] == 4
    assert result["not_attempted_issues_count"] == 2
    assert state["charges"] == [2]
    assert "Plafond atteint" in state["pr_bodies"][0]


@pytest.mark.parametrize("mode", ["individual", "bulk"])
def test_postbuild_language_repair_never_writes_half_a_fix(harness, mode):
    state, run = harness
    key = "served_html_lang_mismatch"
    state.update({"key": key, "budget": 1, "sources": {
        "package.json": json.dumps({"scripts": {"build": "next build"}}),
        "app/layout.tsx": 'export default function Layout({children}) { return <html lang="fr"><body>{children}</body></html>; }',
        "app/page.tsx": 'export default function Page() { return <h1>Test</h1>; }',
    }, "report": {"issues": {key: {"count": 1, "examples": [URL], "evidence": {
        "kind": "page_values", "items": [{"page": URL, "field": "lang", "value": "en"}]}}}}})
    result = run(mode)
    assert result["status"] == 422 and not result["written"] and not state["pr_bodies"]


def test_bulk_does_not_cap_a_family_at_six_files(harness, monkeypatch):
    state, run = harness
    state["key"] = "missing_h1"
    paths = ["page-%d.html" % i for i in range(9)]
    state["sources"] = {path: HTML for path in paths}
    state["report"] = {"issues": {"missing_h1": {"count": 9, "examples": [URL]}}}
    seen = []

    def patch(**kwargs):
        seen.append(kwargs["max_files"])
        return paths, [], paths, []

    monkeypatch.setattr(m, "_deep_patch_issue_files", patch)
    result = run("bulk")["response"]
    assert seen == [40] and len(result["results"][0]["files"]) == 9


def test_bulk_forwards_the_complete_preparation_and_site_context(harness, monkeypatch):
    state, run = harness
    state["key"] = "missing_h1"
    state["sources"] = {"index.html": HTML}
    pages = [{"url": URL, "lang": "fr", "og_image": "https://site.test/social.png"}]
    state["report"] = {"issues": {"missing_h1": {"count": 1, "examples": [URL]}}, "pages": pages}
    original = m._prepare_issue_fix
    received = {}

    def prepare(**kwargs):
        assert kwargs["pages"] == pages
        prep = original(**kwargs)
        prep.update({"targets_override": ["index.html"], "page_side": True,
                     "canonical_masters": {URL: URL}})
        return prep

    def patch(**kwargs):
        received.update(kwargs)
        return ["index.html"], [], ["index.html"], []

    monkeypatch.setattr(m, "_prepare_issue_fix", prepare)
    monkeypatch.setattr(m, "_deep_patch_issue_files", patch)
    assert run("bulk")["status"] == 200
    assert received["targets_override"] == ["index.html"] and received["page_side"]
    assert received["canonical_masters"] == {URL: URL}
    assert received["site_lang"] == m._dominant_site_lang(pages)
    assert received["site_og_image"] == m._dominant_site_og_image(pages)


@pytest.mark.parametrize("mode", ["individual", "bulk"])
def test_a_new_fix_replaces_the_old_verdict_and_baseline(harness, monkeypatch, mode):
    from backend.models import IssueTask
    from sqlalchemy import select

    state, run = harness
    state["key"] = "missing_h1_indexable"
    urls = [URL, "https://site.test/other"]
    state["sources"] = {"index.html": HTML}
    state["report"] = {"issues": {state["key"]: {"count": 2, "examples": urls}}}
    m.DB.create_tables()
    with m.DB.session() as db:
        db.add(IssueTask(project_id=state["project"].id, user_id=state["user"].id,
                         issue_key=state["key"], issue_label="H1", crawl_ts="20260901-090000",
                         url=URL, status="done", severity="warning",
                         note=json.dumps({"verify": {"result": "resolved"}})))
        db.commit()
    monkeypatch.setattr(m, "_deep_patch_issue_files", lambda **k: (
        ["index.html"], [], ["index.html"], []))
    assert run(mode)["status"] == 200
    with m.DB.session() as db:
        tasks = list(db.scalars(select(IssueTask).where(
            IssueTask.project_id == state["project"].id)).all())
        assert len(tasks) == 1 and tasks[0].crawl_ts == state["crawl_ts"]
        note = json.loads(tasks[0].note)
        assert "verify" not in note and tasks[0].status == "in_progress"
        assert note["verification_urls"] == urls and note["verification_expected_count"] == 2
