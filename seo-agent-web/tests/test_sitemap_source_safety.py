"""Sitemap creation/repair must use measured HTML and never overwrite a now-valid file."""

import base64
import xml.etree.ElementTree as ET

import pytest

from backend import app as m


URL = "https://site.test/Page/?a=One"
PAGE = {"url": URL, "final_url": URL, "canonical": URL, "status_code": 200,
        "content_type": "text/html; charset=utf-8"}
NS = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
BROKEN = b"<urlset><url><loc>broken"


@pytest.mark.parametrize("overrides", [
    {"content_type": None}, {"content_type": ""}, {"content_type": "application/json"},
    {"content_type": "application/xml"}, {"content_type": "image/png"}, {"content_type": "text/plain"},
    {"blocked_by_host": True}, {"status_code": "200"}, {"status_code": 200.0},
    {"status_code": "invalid"}, {"status_code": True}, {"status_code": None},
    {"error": "timeout"}, {"meta_robots": "NOINDEX, follow"}, {"x_robots_tag": "googlebot: noindex"},
])
def test_unobserved_nonhtml_or_unavailable_pages_are_excluded(overrides):
    assert m._urls_indexables_du_rapport([{**PAGE, **overrides}]) == []


@pytest.mark.parametrize("canonical", [
    "http://site.test/Page/?a=One", "https://site.test/page/?a=One", "https://site.test/Page?a=One",
    "https://site.test/Page/?a=one", "https://other.test/Page/?a=One", "not-an-absolute-url",
])
def test_canonical_identity_preserves_scheme_case_query_and_slash(canonical):
    assert m._urls_indexables_du_rapport([{**PAGE, "canonical": canonical}]) == []


@pytest.mark.parametrize("canonical", [URL, "https://SITE.TEST/Page/?a=One", URL+"#fragment", None, ""])
def test_selfcanonical_or_missing_canonical_still_passes(canonical):
    assert m._urls_indexables_du_rapport([{**PAGE, "canonical": canonical}]) == [URL]


def test_sort_deduplicate_and_use_the_observed_final_url():
    other = {**PAGE, "url": "https://site.test/old", "final_url": "https://site.test/a/",
             "canonical": "https://site.test/a/", "content_type": "application/xhtml+xml"}
    assert m._urls_indexables_du_rapport([PAGE, None, other, PAGE, "invalid"]) == [URL, "https://site.test/a/"]


@pytest.fixture
def github(monkeypatch):
    calls = []
    state = {"reply": {"sha": "original", "encoding": "base64", "content": base64.b64encode(BROKEN).decode()}}
    monkeypatch.setattr(m, "_github_api_get", lambda *a, **kw: state["reply"])
    monkeypatch.setattr(m, "_github_api_put", lambda path, **kw: calls.append(kw["json_body"]) or {})
    monkeypatch.setattr(m, "_anthropic_messages_text", lambda **kw: pytest.fail("No model call is permitted"))
    return state, calls


def repair(pages):
    return m._deep_reparer_le_sitemap(owner="fixture", repo_name="fixture", token="test", fix_branch="qa",
                                    all_paths=["sitemap.xml"], pages=pages)


@pytest.mark.parametrize("mode", ["create", "repair"])
def test_actual_put_contains_only_measured_indexable_urls(github, mode):
    _, calls = github
    pages = [PAGE, {**PAGE, "url": "https://site.test/api", "final_url": "https://site.test/api",
                    "canonical": "https://site.test/api", "content_type": "application/json"},
             {**PAGE, "canonical": URL.lower()}, {**PAGE, "blocked_by_host": True}]
    if mode == "repair":
        paths, _ = repair(pages)
    else:
        paths, _ = m._deep_creer_le_sitemap(owner="fixture", repo_name="fixture", token="test", fix_branch="qa",
                                          all_paths=["robots.txt"], pages=pages)
    assert paths == ["sitemap.xml"] and len(calls) == 1
    root = ET.fromstring(base64.b64decode(calls[0]["content"]))
    assert [node.text for node in root.iter(NS+"loc")] == [URL]
    assert calls[0]["branch"] == "qa"
    assert ("sha" in calls[0]) is (mode == "repair")


@pytest.mark.parametrize("source", [
    m._sitemap_xml([URL]).encode(), b'<sitemapindex><sitemap><loc>https://site.test/child.xml</loc></sitemap></sitemapindex>',
    b'<urlset xmlns:image="urn:image"><url><loc>https://site.test/</loc><image:image /></url></urlset>',
])
def test_a_now_valid_sitemap_is_preserved_even_if_the_report_is_stale(github, source):
    state, calls = github
    state["reply"]["content"] = base64.b64encode(source).decode()
    changed, notes = repair([PAGE])
    assert not changed and not calls
    assert notes


@pytest.mark.parametrize("reply", [
    {}, {"sha": "original"}, {"sha": "", "content": base64.b64encode(BROKEN).decode()},
    {"sha": "original", "content": "!invalid-base64!"}, {"sha": "original", "content": ""},
    {"sha": "original", "content": base64.b64encode(BROKEN).decode(), "encoding": "none"},
    {"sha": "original", "content": base64.b64encode(b'<!DOCTYPE x [<!ENTITY secret "data">]><urlset>&secret;</urlset>').decode()},
    {"sha": "original", "content": base64.b64encode(b'<?xml version="1.0" encoding="unknown-encoding"?><urlset />').decode()},
])
def test_unknown_or_unsafe_current_content_cannot_be_replaced(github, reply):
    state, calls = github
    state["reply"] = reply
    changed, notes = repair([PAGE])
    assert not changed and not calls and notes


def test_a_confirmed_zero_byte_blob_is_repairable(github):
    state, calls = github
    state["reply"] = {"sha": "empty-blob", "encoding": "base64", "size": 0, "content": ""}
    changed, _ = repair([PAGE])
    assert changed == ["sitemap.xml"] and len(calls) == 1


@pytest.mark.parametrize("sha", [None, 123, {}])
def test_a_non_string_blob_identity_is_not_a_safe_write_basis(github, sha):
    state, calls = github
    state["reply"]["sha"] = sha
    changed, _ = repair([PAGE])
    assert not changed and not calls
