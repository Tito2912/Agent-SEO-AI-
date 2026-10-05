"""Only direct observed and still-missing 404/410 entries may be delisted."""

import base64
from types import SimpleNamespace
from urllib.parse import unquote

import pytest

from backend import app as m, repo_index
from tests.test_verified_sitemap_canonical import entry, sitemap

KEY, SITE = "sitemap_4xx_page", "https://site.test"
A, B, C = (SITE + "/" + name for name in ("missing", "gone", "control"))


def row(url, **changes):
    return {"url": url, "final_url": url, "status_code": 404, "content_type": "text/html",
            "error": None, "blocked_by_host": False, "redirect_chain": [], "redirect_statuses": [], **changes}


def prepare(rows, urls=None, paths=None):
    urls = [A] if urls is None else urls
    return m._prepare_issue_fix(issue_key=KEY, issues={KEY: {"count": len(urls), "examples": urls}}, impacted=urls,
        all_paths=["sitemap.xml"] if paths is None else paths, site_name="site.test", owner="fixture",
        repo_name="fixture", branch="baseline", token="unused", pages=rows)


@pytest.mark.parametrize("code", [404, 410])
def test_observed_terminal_error_prepares_exact_xml_cleanup_without_ai(code):
    prep = prepare([row(A, status_code=code)])
    assert not prep["refusal"] and prep.get("sitemap_error_statuses") == {A: code}
    assert prep["targets_override"] == ["sitemap.xml"] and not prep["rewriter_ai_fallback"]
    assert m._fix_premise_note(KEY)


@pytest.mark.parametrize("change", [
    {"status_code": 200}, {"status_code": 301}, {"status_code": 400}, {"status_code": 401}, {"status_code": 403},
    {"status_code": 408}, {"status_code": 429}, {"status_code": 451}, {"status_code": 500}, {"status_code": 503},
    {"status_code": "404"}, {"status_code": 404.0}, {"status_code": True}, {"status_code": None},
    {"error": "timeout"}, {"blocked_by_host": True}, {"content_type": "application/json"}, {"content_type": None},
    {"final_url": B}, {"redirect_chain": [A]}, {"redirect_statuses": [302]},
])
def test_protected_transient_redirected_or_unproven_observations_refuse(change):
    prep = prepare([row(A, **change)])
    assert prep["refusal"] and prep.get("sitemap_error_statuses") == {} and not prep["rewriter_ai_fallback"]


def test_missing_or_conflicting_observations_refuse():
    assert prepare([])["refusal"]
    assert prepare([row(A), row(A, status_code=200)])["refusal"]
    assert prepare([row(A), row(A, status_code=410)])["refusal"]
    assert prepare([row(B, final_url=A)], [A])["refusal"]


def test_verified_subset_names_refused_urls_and_partner_effects():
    prep = prepare([row(A), row(C, status_code=403, hreflang={"fr": A})], [A, C])
    assert prep.get("sitemap_error_statuses") == {A: 404}
    assert C in prep["side_effects"] and "Effet de bord" in prep["side_effects"]


@pytest.mark.parametrize("paths", [["app/sitemap.ts"], ["sitemap.xml", "app/sitemap.ts"],
    ["sitemap.xml", "backup-sitemap.xml"], ["static/sitemap.xml", "hugo.toml"], ["archive/sitemap.xml"], []])
def test_generated_or_ambiguous_sitemap_refuses_without_model(paths):
    assert prepare([row(A)], paths=paths)["refusal"]


def test_exact_xml_cleanup_preserves_distinct_paths_comments_and_extensions():
    alias = entry(A, '<priority>0.1</priority>')
    source = sitemap(alias, entry(A + "/"), entry(SITE + "/MISSING"), entry(A + "?v=1"),
                     entry(C, '<image xmlns="urn:image"><loc>' + A + '</loc></image>'))
    source = source.replace('</urlset>', '<!-- ' + alias + ' -->\r\n</urlset>')
    assert prepare([row(A)])["link_rewriter"](source) == (source.replace(alias, "", 1), 1)


def test_a_second_stale_http_witness_blocks_the_entire_single_file_write(monkeypatch):
    calls, writes = [], []
    xml = sitemap(entry(A), entry(B), entry(C))
    monkeypatch.setattr(m, "_github_api_get", lambda *a, **k: {"sha": "original", "content": base64.b64encode(xml.encode()).decode()})
    monkeypatch.setattr(m, "_github_api_put", lambda *a, **k: writes.append(k))
    def status(url):
        calls.append(url)
        return 404 if url == A else 200
    monkeypatch.setattr(m, "_sitemap_error_status", status)
    rows = [row(A), row(B, status_code=410)]
    result = m._apply_prepared_issue_fix(owner="fixture", repo_name="fixture", branch="baseline", token="unused", fix_branch="qa",
        all_paths=["sitemap.xml"], issue_key=KEY, issue_label=KEY, impacted=[A, B], site_name="site.test", file_state={},
        max_files=1, prep=prepare(rows, [A, B]), pages=rows, index=repo_index.build_repo_index(["sitemap.xml"]))
    assert calls == [A, B] and not writes and not result["patched"]


@pytest.mark.parametrize("case", ["verified_404", "verified_410", "unobserved", "returned_200", "returned_403", "returned_429",
    "returned_503", "changed_terminal_code", "new_redirect", "http_failure", "restored_route", "ambiguous_route",
    "stale_sitemap", "missing_sha", "invalid_base64", "generated_package", "insufficient_budget", "changed_rules",
    "ambiguous_rules", "toml_redirects", "xml_index"])
def test_pipeline_revalidates_http_and_current_sources_before_single_xml_put(monkeypatch, case):
    block, xml = entry(A), sitemap(entry(A), entry(C))
    sources = {"sitemap.xml": xml}
    rows = [row(A, status_code=410 if case == "verified_410" else 404)]
    if case == "unobserved":
        rows = []
    elif case in {"restored_route", "ambiguous_route"}:
        sources["missing.html"] = '<html lang="fr"><head></head><body>Restored</body></html>'
        if case == "ambiguous_route":
            sources["missing/index.html"] = sources["missing.html"]
    elif case == "stale_sitemap":
        sources["sitemap.xml"] = sitemap(entry(C))
    elif case == "generated_package":
        sources["package.json"] = '{"dependencies":{"next-sitemap":"4.0.0"}}'
    elif case == "changed_rules":
        sources["_redirects"] = "/missing /control 301\n"
    elif case == "ambiguous_rules":
        sources["_redirects"] = "/* /index.html 200\n"
    elif case == "toml_redirects":
        sources["netlify.toml"] = '[[redirects]]\nfrom="/missing"\nto="/control"\nstatus=200\n'
    elif case == "xml_index":
        sources["sitemap.xml"] = '<sitemapindex><sitemap><loc>' + A + '</loc></sitemap></sitemapindex>'
    writes, requests = {}, []
    def get(path, **kwargs):
        name = unquote(path.split("/contents/", 1)[1])
        return {"sha": "" if case == "missing_sha" else "original", "content": "!!!!" if case == "invalid_base64"
                else base64.b64encode(sources[name].encode()).decode()}
    def put(path, **kwargs):
        assert requests == [A]
        assert kwargs["json_body"]["sha"] == "original" and kwargs["json_body"]["branch"] == "qa"
        writes[unquote(path.split("/contents/", 1)[1])] = base64.b64decode(kwargs["json_body"]["content"]).decode()
        return {"content": {"sha": "patched"}}
    def status(url):
        requests.append(url)
        if case == "http_failure":
            raise TimeoutError()
        return {"returned_200": 200, "returned_403": 403, "returned_429": 429, "returned_503": 503,
                "changed_terminal_code": 410, "new_redirect": 302}.get(case, rows[0]["status_code"])
    monkeypatch.setattr(m, "_github_api_get", get)
    monkeypatch.setattr(m, "_github_api_put", put)
    monkeypatch.setattr(m, "_sitemap_error_status", status, raising=False)
    def forbidden(*args, **kwargs):
        pytest.fail("No model, tarball search or guessed target for 404 sitemap cleanup")
    for name in ("_openai_generate_file_patch", "_ai_map_urls_to_files", "_ai_pick_repo_files", "_github_tarball_grep"):
        monkeypatch.setattr(m, name, forbidden)
    prep = prepare(rows, paths=list(sources))
    result = m._apply_prepared_issue_fix(owner="fixture", repo_name="fixture", branch="baseline", token="unused", fix_branch="qa",
        all_paths=list(sources), issue_key=KEY, issue_label=KEY, impacted=[A], site_name="site.test", file_state={},
        max_files=0 if case == "insufficient_budget" else 1, prep=prep, pages=rows, index=repo_index.build_repo_index(list(sources)))
    assert not result["ai_files"]
    if case in {"verified_404", "verified_410"}:
        assert writes == {"sitemap.xml": xml.replace(block, "")}
    else:
        assert not writes


@pytest.mark.parametrize("case", ["verified_404", "verified_410", "redirect", "history", "retry_after", "wrong_url", "invalid_type",
    "json", "timeout", "unsafe", "credentials"])
def test_http_probe_never_follows_redirects_reads_bodies_or_skips_public_guard(monkeypatch, case):
    if not hasattr(m, "_sitemap_error_status"):
        pytest.fail("A current direct HTTP status check is required")
    fetched, closed = [], []
    url = "https://user:password@site.test/missing" if case == "credentials" else A
    response = SimpleNamespace(status_code=410 if case == "verified_410" else 404, url=url,
        headers={"content-type": "text/html"}, history=[], close=lambda: closed.append(True))
    if case == "redirect":
        response.status_code = 302
    elif case == "history":
        response.history = [object()]
    elif case == "retry_after":
        response.headers["retry-after"] = "120"
    elif case == "wrong_url":
        response.url = B
    elif case == "invalid_type":
        response.status_code = "404"
    elif case == "json":
        response.headers["content-type"] = "application/json"
    monkeypatch.setattr(m, "_validate_public_crawl_target", lambda url: "unsafe" if case == "unsafe" else None)
    def get(value, **kwargs):
        fetched.append(value)
        assert kwargs["allow_redirects"] is False and kwargs["stream"] is True and kwargs["timeout"] == (5, 10)
        if case == "timeout":
            raise TimeoutError()
        return response
    monkeypatch.setattr(m.requests, "get", get)
    result = m._sitemap_error_status(url)
    assert result == (response.status_code if case in {"verified_404", "verified_410"} else None)
    assert fetched == ([] if case in {"unsafe", "credentials"} else [url])
    assert closed == ([] if case in {"unsafe", "credentials", "timeout"} else [True])
