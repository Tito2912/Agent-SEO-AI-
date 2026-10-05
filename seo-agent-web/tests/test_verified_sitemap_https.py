"""An HTTPS host is not proof of an indexable HTTPS destination for every path."""

import base64
from types import SimpleNamespace
from urllib.parse import unquote

import pytest

from backend import app as m, repo_index
from tests.test_verified_sitemap_canonical import entry, sitemap

KEY, SITE = "sitemap_http_urls_for_https", "https://site.test"
A, B = SITE + "/a", SITE + "/control"
OLD = "http://site.test/a"
HTML = '<html lang="fr"><head><link rel="canonical" href="' + A + '" /></head><body>Page A</body></html>'


def row(url=A, **changes):
    return {"url": url, "final_url": url, "status_code": 200, "content_type": "text/html", "canonical": url,
            "error": None, "blocked_by_host": False, "redirect_chain": [], "redirect_statuses": [], **changes}


def prepare(rows, urls=None, paths=None):
    urls = [OLD] if urls is None else urls
    return m._prepare_issue_fix(issue_key=KEY, issues={KEY: {"count": len(urls), "examples": urls}}, impacted=urls,
        all_paths=["sitemap.xml", "a.html"] if paths is None else paths, site_name="site.test", owner="fixture",
        repo_name="fixture", branch="baseline", token="unused", pages=rows)


@pytest.mark.parametrize("change", [{"status_code": 404}, {"status_code": 301}, {"status_code": 403},
    {"status_code": 429}, {"status_code": 500}, {"status_code": "200"}, {"status_code": 200.0},
    {"status_code": True}, {"error": "timeout"}, {"blocked_by_host": True}, {"content_type": "application/json"},
    {"final_url": B}, {"canonical": B}, {"canonical": OLD}, {"redirect_chain": [A]}, {"redirect_statuses": [301]},
    {"meta_robots": "noindex, follow"}, {"meta_robots": "none"}, {"x_robots_tag": "googlebot: noindex"},
    {"x_robots_tag": "none"}])
def test_unhealthy_or_noncanonical_https_destination_refuses(change):
    prep = prepare([row(**change)])
    assert prep["refusal"] and prep.get("sitemap_https_pairs") == [] and not prep["rewriter_ai_fallback"]


def test_missing_indirect_or_conflicting_https_observations_refuse():
    for rows in ([], [row(OLD, final_url=A)], [row(), row(status_code=404)], [row(), row(canonical=None)]):
        assert prepare(rows)["refusal"]


@pytest.mark.parametrize("url", ["http://other.test/a", "http://blog.site.test/a", "http://user:password@site.test/a",
    "http://site.test/a#section", "http://site.test:80/a", "https://site.test/a", "http://site.test/never-crawled"])
def test_unowned_ambiguous_or_unobserved_url_is_never_upgraded(url):
    assert prepare([row()], [url])["refusal"]


@pytest.mark.parametrize("paths", [["app/sitemap.ts"], ["sitemap.xml", "app/sitemap.ts"],
    ["sitemap.xml", "backup-sitemap.xml"], ["static/sitemap.xml", "hugo.toml"], ["archive/sitemap.xml"], []])
def test_generated_or_ambiguous_sitemap_refuses(paths):
    assert prepare([row()], paths=paths)["refusal"]


def test_verified_subset_preserves_unproven_entries_and_names_them():
    other = "http://site.test/control"
    prep = prepare([row(), row(B, meta_robots="noindex")], [OLD, other])
    assert not prep["refusal"] and prep["sitemap_https_pairs"] == [{"from": OLD, "to": A}]
    assert other in prep["side_effects"] and not prep["rewriter_ai_fallback"]
    source = sitemap(entry(OLD, '<lastmod>2001-01-01</lastmod>'), entry(other))
    assert prep["link_rewriter"](source) == (source.replace('<loc>' + OLD, '<loc>' + A), 1)


def test_collision_keeps_existing_https_entry_including_its_metadata():
    alias = entry(OLD, '<lastmod>2001-01-01</lastmod><priority>0.1</priority>')
    master = entry(A, '<lastmod>2026-10-05</lastmod><priority>0.9</priority>')
    source = sitemap(alias, master, entry(B))
    assert prepare([row()])["link_rewriter"](source) == (source.replace(alias, ""), 1)


def test_fragment_variants_remain_distinct_from_the_exact_entry():
    source = sitemap(entry(OLD), entry(OLD + '#section'), entry(A + '#section'))
    assert prepare([row()])["link_rewriter"](source) == (source.replace('<loc>' + OLD + '</loc>', '<loc>' + A + '</loc>'), 1)


def test_upgrade_preserves_exact_paths_queries_extensions_comments_and_namespace():
    old = OLD + '?x=1&y=2'
    target = A + '?x=1&y=2'
    block = entry(old, '<image xmlns="urn:image"><loc>' + OLD + '</loc></image>')
    source = sitemap(block, entry(OLD + "/"), entry("http://site.test/A"), entry(OLD + '?x=2'))
    source = source.replace('</urlset>', '<!-- ' + block + ' -->\r\n</urlset>')
    new, count = prepare([row(target)], [old])["link_rewriter"](source)
    assert (new, count) == (source.replace('http://site.test/a?x=1&amp;y=2', 'https://site.test/a?x=1&amp;y=2', 1), 1)


@pytest.mark.parametrize("source", [
    '<urlset><url><loc>' + OLD + '</loc>',
    '<!DOCTYPE urlset [<!ENTITY x "' + OLD + '">]><urlset><url><loc>&x;</loc></url></urlset>',
    '<sitemapindex><sitemap><loc>' + OLD + '</loc></sitemap></sitemapindex>',
    '<urlset><url><loc>' + OLD + '</loc><loc>' + OLD + '</loc></url></urlset>',
    '<urlset><url><loc><![CDATA[' + OLD + ']]></loc></url></urlset>',
    '<urlset xmlns="urn:wrong"><url><loc>' + OLD + '</loc></url></urlset>',
    '<urlset><url><loc kind="alias">' + OLD + '</loc></url></urlset>',
    '<urlset><url><loc>' + OLD + '</loc><x:link xmlns:x="http://www.w3.org/1999/xhtml" rel="alternate" hreflang="fr" href="' + OLD + '" /></url></urlset>',
])
def test_unverifiable_xml_or_unproved_moved_alternates_refuse(source):
    assert prepare([row()])["link_rewriter"](source) == (source, 0)


def test_second_changed_https_witness_blocks_entire_file_write(monkeypatch):
    xml = sitemap(entry(OLD), entry('http://site.test/control'))
    sources = {'sitemap.xml': xml, 'a.html': HTML, 'control.html': HTML.replace(A, B)}
    rows, writes, probes = [row(), row(B)], [], []
    monkeypatch.setattr(m, '_github_api_get', lambda api, **kw: {'sha': 'original', 'content': base64.b64encode(
        sources[unquote(api.split('/contents/', 1)[1])].encode()).decode()})
    monkeypatch.setattr(m, '_github_api_put', lambda *a, **kw: writes.append(kw))
    def probe(url, canonical):
        probes.append(url)
        return url == A
    monkeypatch.setattr(m, '_sitemap_https_page', probe)
    urls = [OLD, 'http://site.test/control']
    result = m._apply_prepared_issue_fix(owner='fixture', repo_name='fixture', branch='baseline', token='unused', fix_branch='qa',
        all_paths=list(sources), issue_key=KEY, issue_label=KEY, impacted=urls, site_name='site.test', file_state={}, max_files=1,
        prep=prepare(rows, urls, list(sources)), pages=rows, index=repo_index.build_repo_index(list(sources)))
    assert probes == [A, B] and not writes and not result['patched']


@pytest.mark.parametrize("case", ["verified", "collision", "unobserved", "stale_head", "noindex_head", "redirect_head",
    "ambiguous_route", "missing_route", "changed_rules", "ambiguous_rules", "toml_redirects", "generated_package",
    "missing_sha", "invalid_base64", "stale_sitemap", "xml_index", "insufficient_budget", "fresh_http_refused"])
def test_pipeline_revalidates_sources_and_https_before_one_xml_put(monkeypatch, case):
    xml = sitemap(entry(OLD, '<priority>0.1</priority>'), entry(B))
    sources = {"sitemap.xml": xml, "a.html": HTML}
    rows = [] if case == "unobserved" else [row()]
    if case == "collision":
        sources["sitemap.xml"] = xml = sitemap(entry(OLD), entry(A, '<priority>0.9</priority>'), entry(B))
    elif case == "stale_head":
        sources["a.html"] = HTML.replace(A, B)
    elif case == "noindex_head":
        sources["a.html"] = HTML.replace('</head>', '<meta name="robots" content="noindex" /></head>')
    elif case == "redirect_head":
        sources["a.html"] = HTML.replace('</head>', '<meta http-equiv="refresh" content="0;url=/control" /></head>')
    elif case == "ambiguous_route":
        sources["a/index.html"] = HTML
    elif case == "missing_route":
        del sources["a.html"]
    elif case == "changed_rules":
        sources["_redirects"] = '/a /control 301\n'
    elif case == "ambiguous_rules":
        sources["_redirects"] = '/* /index.html 200\n'
    elif case == "toml_redirects":
        sources["netlify.toml"] = '[[redirects]]\nfrom="/a"\nto="/control"\nstatus=200\n'
    elif case == "generated_package":
        sources["package.json"] = '{"dependencies":{"next-sitemap":"4.0.0"}}'
    elif case == "stale_sitemap":
        sources["sitemap.xml"] = sitemap(entry(B))
    elif case == "xml_index":
        sources["sitemap.xml"] = '<sitemapindex><sitemap><loc>' + OLD + '</loc></sitemap></sitemapindex>'
    writes, probes = {}, []
    def get(path, **kwargs):
        name = unquote(path.split('/contents/', 1)[1])
        return {"sha": "" if case == "missing_sha" else "original", "content": "!!!!" if case == "invalid_base64"
                else base64.b64encode(sources[name].encode()).decode()}
    def put(path, **kwargs):
        assert probes == [(A, A)] and kwargs["json_body"]["sha"] == "original"
        writes[unquote(path.split('/contents/', 1)[1])] = base64.b64decode(kwargs["json_body"]["content"]).decode()
        return {"content": {"sha": "patched"}}
    def probe(url, canonical):
        probes.append((url, canonical))
        return case != "fresh_http_refused"
    def forbidden(*a, **k):
        pytest.fail("No AI or inferred target for a guarded sitemap HTTPS upgrade")
    monkeypatch.setattr(m, "_github_api_get", get)
    monkeypatch.setattr(m, "_github_api_put", put)
    monkeypatch.setattr(m, "_sitemap_https_page", probe, raising=False)
    for name in ("_openai_generate_file_patch", "_ai_map_urls_to_files", "_ai_pick_repo_files", "_github_tarball_grep"):
        monkeypatch.setattr(m, name, forbidden)
    result = m._apply_prepared_issue_fix(owner="fixture", repo_name="fixture", branch="baseline", token="unused", fix_branch="qa",
        all_paths=list(sources), issue_key=KEY, issue_label=KEY, impacted=[OLD], site_name="site.test", file_state={},
        max_files=0 if case == "insufficient_budget" else 1, prep=prepare(rows, paths=list(sources)), pages=rows,
        index=repo_index.build_repo_index(list(sources)))
    assert not result["ai_files"]
    if case in {"verified", "collision"}:
        expected = xml.replace(entry(OLD), '') if case == "collision" else xml.replace('<loc>' + OLD, '<loc>' + A)
        assert writes == {"sitemap.xml": expected}
    else:
        assert not writes


@pytest.mark.parametrize("case", ["verified", "unsafe", "credentials", "http", "redirect", "history", "wrong_url", "noindex_header",
    "none_header", "noindex_meta", "none_meta", "changed_canonical", "missing_canonical", "json", "oversize", "timeout", "bad_utf8",
    "body_script", "head_script"])
def test_current_https_probe_requires_public_direct_indexable_html_and_closes_response(monkeypatch, case):
    if not hasattr(m, "_sitemap_https_page"):
        pytest.fail("A fresh HTTPS destination check is required")
    fetched, closed = [], []
    url = {'credentials': 'https://user:secret@site.test/a', 'http': OLD}.get(case, A)
    body = HTML
    if case in {"noindex_meta", "none_meta"}:
        body = body.replace('</head>', '<meta name="robots" content="' + ('none' if case == 'none_meta' else 'noindex') + '" /></head>')
    elif case == "changed_canonical":
        body = body.replace(A, B)
    elif case == "missing_canonical":
        body = body.replace('<link rel="canonical" href="' + A + '" />', '')
    elif case == "oversize":
        body += ' ' * 80_001
    elif case == "body_script":
        body = body.replace('</body>', '<script async src="/analytics.js"></script></body>')
    elif case == "head_script":
        body = body.replace('</head>', '<script src="/seo.js"></script></head>')
    content = b'\xff' if case == 'bad_utf8' else body.encode()
    response = SimpleNamespace(status_code=302 if case == "redirect" else 200, url=B if case == "wrong_url" else url,
        history=[object()] if case == "history" else [], headers={"content-type": "application/json" if case == "json" else "text/html"},
        close=lambda: closed.append(True), iter_content=lambda chunk_size: iter([content]))
    if case in {"noindex_header", "none_header"}:
        response.headers["x-robots-tag"] = 'none' if case == 'none_header' else 'googlebot: noindex'
    monkeypatch.setattr(m, "_validate_public_crawl_target", lambda u: "unsafe" if case == "unsafe" else None)
    def get(value, **kwargs):
        fetched.append(value)
        assert kwargs["allow_redirects"] is False and kwargs["stream"] is True and kwargs["timeout"] == (5, 10)
        if case == "timeout":
            raise TimeoutError()
        return response
    monkeypatch.setattr(m.requests, "get", get)
    assert m._sitemap_https_page(url, A) is (case in {"verified", "body_script"})
    assert fetched == ([] if case in {"unsafe", "credentials", "http"} else [url])
    assert closed == ([] if case in {"unsafe", "credentials", "http", "timeout"} else [True])
