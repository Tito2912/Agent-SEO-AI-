"""Delisting noindex pages must use current literal instructions and exact XML entries."""

import base64
from urllib.parse import unquote

import pytest

from backend import app as m, repo_index
from tests.test_verified_sitemap_canonical import entry, sitemap

KEY, SITE = "sitemap_noindex_page", "https://site.test"
A, B, C = (SITE + "/" + name for name in ("private", "redirect", "control"))


def row(url, **changes):
    return {"url": url, "final_url": url, "status_code": 200, "content_type": "text/html", "canonical": url,
            "meta_robots": "noindex, follow", "meta_robots_tag_count": 1, "redirect_chain": [], "redirect_statuses": [], **changes}


def prepare(rows, urls=None, paths=None):
    urls = [A] if urls is None else urls
    return m._prepare_issue_fix(issue_key=KEY, issues={KEY: {"count": len(urls), "examples": urls}}, impacted=urls,
        all_paths=["sitemap.xml", "private.html"] if paths is None else paths, site_name="site.test", owner="fixture",
        repo_name="fixture", branch="baseline", token="unused", pages=rows)


def test_verified_noindex_selects_only_the_sitemap_without_ai_and_keeps_human_review():
    prep = prepare([row(A)])
    assert not prep["refusal"] and not prep["rewriter_ai_fallback"]
    assert prep.get("sitemap_noindex_urls") == [A] and prep["targets_override"] == ["sitemap.xml"]
    assert m._fix_premise_note(KEY)


@pytest.mark.parametrize("change", [
    {"status_code": 404}, {"status_code": 503}, {"status_code": True}, {"status_code": "200"},
    {"content_type": "application/json"}, {"error": "timeout"}, {"blocked_by_host": True},
    {"meta_robots": "index, follow"}, {"meta_robots": "noindexable"}, {"meta_robots": None},
    {"meta_robots": "max-snippet:noindex"}, {"meta_robots": "noindex=1"},
    {"meta_robots_tag_count": 2}, {"meta_robots_tag_count": True}, {"meta_robots_tag_count": None},
    {"final_url": C}, {"redirect_statuses": [301]}, {"redirect_chain": [A]},
])
def test_unobserved_indexable_ambiguous_or_unhealthy_page_is_not_removed(change):
    assert prepare([row(A, **change)])["refusal"]


def test_missing_conflicting_or_preview_header_only_observations_refuse():
    assert prepare([])["refusal"]
    assert prepare([row(A), row(A, meta_robots="index, follow")])["refusal"]
    assert prepare([row(A, meta_robots=None, meta_robots_tag_count=0, x_robots_tag="noindex")])["refusal"]


def test_observed_redirect_to_literal_noindex_needs_an_independent_final_page():
    redirected = row(B, final_url=A, canonical=A, redirect_chain=[B], redirect_statuses=[302])
    assert prepare([redirected], [B])["refusal"]
    assert prepare([redirected, row(A)], [B], ["sitemap.xml", "_redirects", "private.html"]).get("sitemap_noindex_urls") == [B]
    assert prepare([redirected, row(A, meta_robots="index, follow")], [B])["refusal"]


@pytest.mark.parametrize("change", [
    {"redirect_chain": [B, B], "redirect_statuses": [301, 302]},
    {"redirect_chain": [B, A], "redirect_statuses": [301, 302]}, {"redirect_statuses": [304]},
    {"redirect_chain": [B, "https://external.test/hop"], "redirect_statuses": [301, 302]},
    {"redirect_chain": [B, "http://site.test/hop"], "redirect_statuses": [301, 302]},
])
def test_incomplete_looping_external_or_downgraded_redirect_refuses(change):
    redirected = row(B, **{"final_url": A, "canonical": A, "redirect_chain": [B], "redirect_statuses": [302], **change})
    assert prepare([redirected, row(A)], [B])["refusal"]


def test_only_verified_subset_is_removed_and_refusals_and_partner_effects_are_named():
    rows = [row(A), row(C, meta_robots="index, follow", hreflang={"fr": A})]
    prep = prepare(rows, [A, C])
    assert prep.get("sitemap_noindex_urls") == [A] and C in prep["side_effects"]
    assert "Effet de bord" in prep["side_effects"]


@pytest.mark.parametrize("paths", [["app/sitemap.ts"], ["sitemap.xml", "app/sitemap.ts"],
    ["sitemap.xml", "backup-sitemap.xml"], ["static/sitemap.xml", "hugo.toml"], ["archive/sitemap.xml"], []])
def test_generated_ambiguous_or_unknown_sitemap_source_refuses_without_a_model(paths):
    prep = prepare([row(A)], paths=paths)
    assert prep["refusal"] and not prep["rewriter_ai_fallback"]


def test_verified_xml_removal_preserves_comments_extensions_line_endings_and_distinct_urls():
    alias = entry(A, "<lastmod>2001-01-01</lastmod>")
    control = entry(C, '<image xmlns="urn:images"><caption>Fran\u00e7ais</caption><loc>' + A + '</loc></image>')
    source = sitemap(alias, entry(A + "/"), entry(SITE + "/PRIVATE"), entry(A + "?variant=1"), control)
    source = source.replace('</urlset>', '<!-- ' + alias + ' -->\r\n</urlset>')
    assert m._remove_sitemap_locs(source, [A], verified=True) == (source.replace(alias, "", 1), 1)


def test_prefixes_escaped_query_and_duplicate_exact_entries_are_supported():
    url = A + "?one=1&two=2"
    source = sitemap(entry(url), entry(C), entry(url))
    source = source.replace('<urlset xmlns="', '<sm:urlset xmlns:sm="').replace('</urlset>', '</sm:urlset>')
    for tag in ("url", "loc"):
        source = source.replace('<' + tag + '>', '<sm:' + tag + '>').replace('</' + tag + '>', '</sm:' + tag + '>')
    alias = '<sm:url><sm:loc>' + url.replace("&", "&amp;") + '</sm:loc></sm:url>'
    assert m._remove_sitemap_locs(source, [url], verified=True) == (source.replace(alias, ""), 2)


@pytest.mark.parametrize("source", [
    '<sitemapindex><sitemap><loc>' + A + '</loc></sitemap></sitemapindex>',
    '<urlset><url><loc>' + A, '<!DOCTYPE urlset [<!ENTITY x "data">]><urlset />',
    '<urlset xmlns="urn:other">' + entry(A) + '</urlset>',
    '<urlset><url><loc>' + A + '</loc><loc>' + C + '</loc></url></urlset>',
    '<urlset><url><loc><![CDATA[' + A + ']]></loc></url></urlset>',
])
def test_ambiguous_or_unsafe_xml_is_left_byte_identical(source):
    assert m._remove_sitemap_locs(source, [A], verified=True) == (source, 0)


@pytest.mark.parametrize("case", ["verified", "redirected", "stale_robots", "noindex_body_only", "noindex_comment_only",
    "duplicate_robots", "ambiguous_attribute", "template", "script", "native_header_only", "missing_sha", "invalid_base64",
    "generated_package", "stale_sitemap", "stale_redirect", "new_direct_redirect", "missing_current_page", "insufficient_budget",
    "inert_template", "noscript", "duplicate_attribute", "googlebot_only", "ambiguous_routes", "title_text", "textarea", "foreign_svg"])
def test_actual_pipeline_rechecks_noindex_and_writes_only_xml_with_no_model(monkeypatch, case):
    html = '<html lang="fr"><head><meta name="robots" content="noindex, follow" /><link rel="canonical" href="' + A + '" /></head><body>Private</body></html>'
    private = entry(A, "<lastmod>2001-01-01</lastmod>")
    source = sitemap(private, entry(C))
    sources = {"sitemap.xml": source, "private.html": html}
    rows, urls = [row(A)], [A]
    if case == "redirected" or case == "stale_redirect":
        rows.append(row(B, final_url=A, canonical=A, redirect_chain=[B], redirect_statuses=[302]))
        urls = [B]
        sources["sitemap.xml"] = source = sitemap(entry(B), private, entry(C))
        sources["_redirects"] = "/redirect /private 302\n" if case == "redirected" else "/redirect /other 302\n"
    elif case == "new_direct_redirect":
        sources["_redirects"] = "/private /other 301!\n"
    elif case == "stale_robots":
        sources["private.html"] = html.replace("noindex", "index")
    elif case in {"noindex_body_only", "noindex_comment_only"}:
        tag = '<meta name="robots" content="noindex, follow" />'
        base = html.replace(tag, '')
        sources["private.html"] = base.replace("Private", tag + "Private") if case == "noindex_body_only" else base.replace("</head>", '<!-- ' + tag + ' --></head>')
    elif case == "duplicate_robots":
        sources["private.html"] = html.replace("</head>", '<meta name="robots" content="index, follow" /></head>')
    elif case == "ambiguous_attribute":
        sources["private.html"] = html.replace('name="robots"', 'data-name="robots" name="other"')
    elif case == "template":
        sources["private.html"] = html.replace("Private", "{{ content }}")
    elif case == "script":
        sources["private.html"] = html.replace("</head>", '<script>window.state = {};</script></head>')
    elif case == "inert_template":
        sources["private.html"] = html.replace('<meta name="robots" content="noindex, follow" />',
            '<template><meta name="robots" content="noindex, follow" /></template>')
    elif case == "noscript":
        sources["private.html"] = html.replace('<meta name="robots" content="noindex, follow" />',
            '<noscript><meta name="robots" content="noindex, follow" /></noscript>')
    elif case in {"title_text", "textarea", "foreign_svg"}:
        tag = {"title_text": "title", "textarea": "textarea", "foreign_svg": "svg"}[case]
        sources["private.html"] = html.replace('<meta name="robots" content="noindex, follow" />',
            '<' + tag + '><meta name="robots" content="noindex, follow" /></' + tag + '>')
    elif case == "duplicate_attribute":
        sources["private.html"] = html.replace('name="robots"', 'name="robots" name="other"')
    elif case == "googlebot_only":
        sources["private.html"] = html.replace('name="robots"', 'name="googlebot"')
    elif case == "ambiguous_routes":
        sources["private/index.html"] = html
    elif case == "native_header_only":
        rows = [row(A, meta_robots=None, meta_robots_tag_count=0, x_robots_tag="noindex")]
    elif case == "generated_package":
        sources["package.json"] = '{"dependencies":{"next-sitemap":"4.0.0"}}'
    elif case == "stale_sitemap":
        sources["sitemap.xml"] = sitemap(entry(C))
    elif case == "missing_current_page":
        sources.pop("private.html")
    writes = {}
    def get(path, **kwargs):
        name = unquote(path.split("/contents/", 1)[1])
        return {"sha": "" if case == "missing_sha" else "original", "content": "!!!!" if case == "invalid_base64"
                else base64.b64encode(sources[name].encode()).decode()}
    def put(path, **kwargs):
        name = unquote(path.split("/contents/", 1)[1])
        assert kwargs["json_body"]["sha"] == "original" and kwargs["json_body"]["branch"] == "qa"
        writes[name] = base64.b64decode(kwargs["json_body"]["content"]).decode()
        return {"content": {"sha": "patched"}}
    monkeypatch.setattr(m, "_github_api_get", get)
    monkeypatch.setattr(m, "_github_api_put", put)
    def forbidden(*args, **kwargs):
        pytest.fail("No model or heuristic targeting for verified noindex delisting")
    for name in ("_openai_generate_file_patch", "_ai_map_urls_to_files", "_ai_pick_repo_files", "_github_tarball_grep"):
        monkeypatch.setattr(m, name, forbidden)
    prep = prepare(rows, urls, list(sources))
    applied = m._apply_prepared_issue_fix(owner="fixture", repo_name="fixture", branch="baseline", token="unused", fix_branch="qa",
        all_paths=list(sources), issue_key=KEY, issue_label=KEY, impacted=urls, site_name="site.test", file_state={},
        max_files=0 if case == "insufficient_budget" else 1, prep=prep, pages=rows, index=repo_index.build_repo_index(list(sources)))
    assert not applied["ai_files"]
    if case in {"verified", "redirected"}:
        removed = private if case == "verified" else entry(B)
        assert writes == {"sitemap.xml": source.replace(removed, "")}
    else:
        assert not writes
