from __future__ import annotations

import xml.etree.ElementTree as ET

import pytest

from backend import app as m


def sitemap(*urls):
    return ('<?xml version="1.0"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
            + "\n".join(f"<url><loc>{url}</loc><lastmod>2026-01-{i + 1:02d}</lastmod></url>"
                        for i, url in enumerate(urls)) + "\n</urlset>\n")


def locs(raw):
    return [e.text for e in ET.fromstring(raw).findall("./{*}url/{*}loc")]


@pytest.mark.parametrize("urls", [
    ("https://x.fr/gauntlet/", "http://x.fr/gauntlet/"),
    ("http://x.fr/gauntlet/", "https://x.fr/gauntlet/"),
    ("http://x.fr/gauntlet/", "http://x.fr/gauntlet/", "https://x.fr/gauntlet/"),
])
def test_https_upgrade_does_not_create_duplicate_sitemap_entries(urls):
    fixed, n = m._rewrite_http_to_https(sitemap(*urls), ["x.fr"])
    assert n > 0
    assert locs(fixed) == ["https://x.fr/gauntlet/"]
    assert "2026-01-01" in fixed
    assert "2026-01-02" not in fixed
    assert 'xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"' in fixed


def test_other_preexisting_duplicates_are_not_removed_by_https_upgrade():
    raw = sitemap("http://x.fr/a", "https://x.fr/a", "https://x.fr/b", "https://x.fr/b")
    fixed, _ = m._rewrite_http_to_https(raw, ["x.fr"])
    assert locs(fixed) == ["https://x.fr/a", "https://x.fr/b", "https://x.fr/b"]


@pytest.mark.parametrize("neighbor", ["https://x.fr/A", "https://x.fr/a/", "https://x.fr/a?q=1"])
def test_upgraded_entry_does_not_delete_distinct_neighbor_urls(neighbor):
    raw = sitemap(neighbor, "http://x.fr/a", "https://x.fr/a")
    fixed, _ = m._rewrite_http_to_https(raw, ["x.fr"])
    assert locs(fixed) == [neighbor, "https://x.fr/a"]


@pytest.mark.parametrize("neighbor", ["https://x.fr/A", "https://x.fr/a/", "http://x.fr/a",
                                      "https://x.fr/a?q=1"])
def test_dedupe_itself_matches_exact_urls_not_path_case_or_scheme(neighbor):
    fixed, n = m._dedupe_sitemap_locs(sitemap(neighbor, "https://x.fr/a", "https://x.fr/a"),
                                     ["https://x.fr/a"])
    assert n == 1
    assert locs(fixed) == [neighbor, "https://x.fr/a"]


def test_xml_escaped_query_values_are_deduplicated_safely():
    raw = sitemap("https://x.fr/a?a=1&amp;b=2", "http://x.fr/a?a=1&amp;b=2")
    fixed, _ = m._rewrite_http_to_https(raw, ["x.fr"])
    assert locs(fixed) == ["https://x.fr/a?a=1&b=2"]


def test_sitemap_index_children_are_never_deleted():
    raw = ('<sitemapindex><sitemap><loc>http://x.fr/s1.xml</loc></sitemap>'
           '<sitemap><loc>https://x.fr/s1.xml</loc></sitemap></sitemapindex>')
    fixed, n = m._rewrite_http_to_https(raw, ["x.fr"])
    assert n == 1
    assert fixed.count("<sitemap>") == 2


def test_external_http_entries_are_left_alone():
    raw = sitemap("http://external.test/a", "http://x.fr/a", "https://x.fr/a")
    fixed, _ = m._rewrite_http_to_https(raw, ["x.fr"])
    assert locs(fixed) == ["http://external.test/a", "https://x.fr/a"]


def test_repeated_execution_is_idempotent():
    fixed, _ = m._rewrite_http_to_https(sitemap("http://x.fr/a", "https://x.fr/a"), ["x.fr"])
    assert m._rewrite_http_to_https(fixed, ["x.fr"]) == (fixed, 0)


def test_non_xml_source_does_not_trigger_sitemap_block_deletion():
    raw = 'const xml = `<urlset><url><loc>http://x.fr/a</loc></url></urlset>`;'
    fixed, n = m._rewrite_http_to_https(raw, ["x.fr"])
    assert n == 1
    assert fixed == raw.replace("http://x.fr", "https://x.fr")


def test_untrusted_xml_entities_are_not_expanded():
    raw = ('<!DOCTYPE urlset [<!ENTITY url "http://x.fr/a">]>'
           '<urlset><url><loc>&url;</loc></url></urlset>')
    fixed, n = m._rewrite_http_to_https(raw, ["x.fr"])
    assert n == 1
    assert "&url;" in fixed
    assert fixed == raw.replace("http://x.fr", "https://x.fr")
