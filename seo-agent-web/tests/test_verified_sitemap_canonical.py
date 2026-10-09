"""Canonical sitemap cleanup must preserve actual masters, not trust unverified evidence."""

import base64
from html import escape
from urllib.parse import unquote
from xml.etree import ElementTree as ET

import pytest

from backend import app as m, repo_index

KEY = "sitemap_non_canonical_page"
SITE = "https://site.test"
A, B, C = (SITE + "/" + n for n in ("copy", "master", "control"))
PAIR = {"page": A, "from": A, "to": B}
NS = "http://www.sitemaps.org/schemas/sitemap/0.9"


def row(url, canonical=None, **extra):
    return {"url": url, "final_url": url, "status_code": 200, "content_type": "text/html",
            "canonical": canonical or url, **extra}


def prepare(rows, pairs=None, paths=None):
    pairs = [dict(PAIR)] if pairs is None else pairs
    return m._prepare_issue_fix(issue_key=KEY, issues={KEY: {"count": len(pairs), "examples": [p["from"] for p in pairs],
        "evidence": {"kind": "url_pairs", "items": pairs}}}, impacted=[p["from"] for p in pairs],
        all_paths=paths or ["sitemap.xml", "index.html"], site_name="site.test", owner="fixture",
        repo_name="fixture", branch="baseline", token="unused", pages=rows)


def entry(url, extra=""):
    return '<url><loc>' + escape(url, quote=False) + '</loc>' + extra + '</url>'


def sitemap(*entries):
    return '<?xml version="1.0" encoding="UTF-8"?>\r\n<urlset xmlns="' + NS + '">\r\n' + "\r\n".join(entries) + '\r\n</urlset>\r\n'


def rewrite(source, pairs=None):
    return m._rewrite_sitemap_locs(source, pairs or [PAIR], canonical=True)


def test_verified_relationship_selects_only_the_literal_sitemap_and_no_ai_fallback():
    prep = prepare([row(A, B), row(B)])
    assert not prep["refusal"] and not prep["rewriter_ai_fallback"]
    assert prep.get("sitemap_canonical_pairs") == [PAIR]
    assert prep["targets_override"] == ["sitemap.xml"]


@pytest.mark.parametrize("change", [
    {"status_code": 404}, {"status_code": 503}, {"status_code": True}, {"status_code": "200"},
    {"content_type": "application/json"}, {"error": "timeout"}, {"blocked_by_host": True},
    {"meta_robots": "noindex"}, {"x_robots_tag": "googlebot: NOINDEX"},
    {"canonical": SITE + "/unobserved"}, {"final_url": SITE + "/redirected"},
])
def test_unhealthy_unknown_or_noncanonical_destination_is_not_written(change):
    assert prepare([row(A, B), row(B, **change)])["refusal"]


@pytest.mark.parametrize("change", [
    {"status_code": 404}, {"status_code": "200"}, {"content_type": "application/json"},
    {"error": "timeout"}, {"canonical": C}, {"final_url": C},
])
def test_unknown_or_contradictory_source_refuses(change):
    assert prepare([row(A, **{"canonical": B, **change}), row(B)])["refusal"]


def test_missing_destination_conflicts_external_targets_and_cycles_refuse():
    assert prepare([row(A, B)])["refusal"]
    assert prepare([row(A, B), row(B), row(B, C)])["refusal"]
    assert prepare([row(A, B), row(B)], [PAIR, {**PAIR, "to": C}])["refusal"]
    assert prepare([row(A, B), row(B, A)])["refusal"]
    external = "https://external.test/master"
    assert prepare([row(A, external), row(external)], [{**PAIR, "to": external}])["refusal"]


def test_verified_chain_resolves_to_a_healthy_self_master():
    prep = prepare([row(A, B), row(B, C), row(C)])
    assert prep.get("sitemap_canonical_pairs") == [{**PAIR, "to": C}]


def test_partial_relationships_are_visible_refusals_not_a_whole_family_success():
    dead = SITE + "/dead-copy"
    prep = prepare([row(A, B), row(B), row(dead, SITE + "/absent")],
                   [PAIR, {"page": dead, "from": dead, "to": SITE + "/absent"}])
    assert not prep["refusal"] and prep.get("sitemap_canonical_pairs") == [PAIR]
    assert dead in prep["side_effects"]


@pytest.mark.parametrize("paths", [["app/sitemap.ts"], ["dist/sitemap.xml"], ["archive/sitemap.xml"],
    ["sitemap.xml", "backup-sitemap.xml"], ["sitemap.xml", "app/sitemap.ts"], ["static/sitemap.xml", "hugo.toml"]])
def test_ambiguous_generated_or_unserved_source_paths_refuse(paths):
    assert prepare([row(A, B), row(B)], paths=paths)["refusal"]


@pytest.mark.parametrize("master_first", [True, False])
def test_existing_master_entry_wins_with_its_metadata_and_unrelated_duplicates_intact(master_first):
    alias = entry(A, "<lastmod>2001-01-01</lastmod>")
    master = entry(B, "<lastmod>2026-01-01</lastmod><priority>0.8</priority>")
    control = entry(C, "<lastmod>2025-02-02</lastmod>")
    source = sitemap(*( [master, alias] if master_first else [alias, master]), control, control)
    output, count = rewrite(source)
    assert count == 1 and output == source.replace(alias, "")
    assert master in output and output.count(control) == 2


def test_missing_master_gets_one_minimal_entry_without_inheriting_alias_metadata():
    second = SITE + "/other-copy"
    alias = entry(A, "<lastmod>2001-01-01</lastmod>")
    source = sitemap(alias, entry(second), entry(C))
    output, count = rewrite(source, [PAIR, {"page": second, "from": second, "to": B}])
    assert count == 2 and output == source.replace(alias, entry(B)).replace(entry(second), "")


def test_namespace_prefix_entities_comments_and_case_sensitive_urls_are_preserved():
    master = B + "?one=1&two=2"
    alias = entry(A)
    source = sitemap(alias, entry(master), entry(A.upper()), entry(C))
    source = source.replace('<urlset xmlns="', '<sm:urlset xmlns:sm="').replace('</urlset>', '</sm:urlset>')
    for tag in ("url", "loc"):
        source = source.replace('<' + tag + '>', '<sm:' + tag + '>').replace('</' + tag + '>', '</sm:' + tag + '>')
    source = source.replace('</sm:urlset>', '<!-- ' + alias + ' -->\r\n</sm:urlset>')
    actual_alias = '<sm:url><sm:loc>' + A + '</sm:loc></sm:url>'
    output, count = rewrite(source, [{**PAIR, "to": master}])
    assert count == 1 and output == source.replace(actual_alias, "")
    ET.fromstring(output)


@pytest.mark.parametrize("source", [
    '<sitemapindex><sitemap><loc>' + A + '</loc></sitemap></sitemapindex>',
    '<urlset><url><loc>' + A,
    '<!DOCTYPE urlset [<!ENTITY injected "data">]><urlset />',
    '<urlset xmlns="urn:other">' + entry(A) + '</urlset>',
    '<urlset><url><loc>' + A + '</loc><loc>' + B + '</loc></url></urlset>',
    '<urlset><url><loc><![CDATA[' + A + ']]></loc></url></urlset>',
    '<urlset><url><loc><!-- annotation -->' + A + '</loc></url></urlset>',
    'export default "<urlset>' + entry(A) + '</urlset>";',
])
def test_unsafe_or_ambiguous_xml_is_left_byte_identical(source):
    assert rewrite(source) == (source, 0)


def test_unicode_byte_offsets_and_escaped_source_queries_preserve_extensions():
    alias_url = A + "?one=1&two=2"
    alias = entry(alias_url)
    control = entry(C, '<image xmlns="urn:images"><caption>Fran\u00e7ais</caption><loc>' + A + '</loc></image>')
    source = sitemap(control, alias, entry(B))
    output, count = rewrite(source, [{"page": alias_url, "from": alias_url, "to": B}])
    assert count == 1 and output == source.replace(alias, "")


@pytest.mark.parametrize("case", ["verified", "missing_sha", "invalid_base64", "generated_package", "unreadable_package", "stale_sitemap",
    "current_self_canonical", "current_master_noindex", "current_computed_head"])
def test_actual_pipeline_never_targets_html_or_calls_a_model(monkeypatch, case):
    source = sitemap(entry(A), entry(B), entry(C))
    sources = {"sitemap.xml": source, "index.html": '<a href="' + A + '">Control</a>'}
    def page(canonical):
        return '<html lang="fr"><head><link rel="canonical" href="' + canonical + '" /></head><body>Control</body></html>'
    sources.update({"copy.html": page(B), "master.html": page(B)})
    if case == "current_self_canonical":
        sources["copy.html"] = page(A)
    elif case == "current_master_noindex":
        sources["master.html"] = page(B).replace("</head>", '<meta name="robots" content="noindex" /></head>')
    elif case == "current_computed_head":
        sources["copy.html"] = page(B).replace("Control", "{{ content }}")
    if case in {"generated_package", "unreadable_package"}:
        sources["package.json"] = '{"dependencies":{"next-sitemap":"4.0.0"}}'
    if case == "stale_sitemap":
        sources["sitemap.xml"] = sitemap(entry(B), entry(C))
    written = {}
    def get(path, **kwargs):
        file = unquote(path.split("/contents/", 1)[1])
        if file == "package.json" and case == "unreadable_package":
            raise OSError("unreadable")
        return {"sha": "" if case == "missing_sha" else "original", "content": "!!!!" if case == "invalid_base64"
                else base64.b64encode(sources[file].encode()).decode()}
    def put(path, **kwargs):
        file = unquote(path.split("/contents/", 1)[1])
        written[file] = base64.b64decode(kwargs["json_body"]["content"]).decode()
        return {"content": {"sha": "patched"}}
    monkeypatch.setattr(m, "_github_api_get", get)
    monkeypatch.setattr(m, "_github_api_put", put)
    def forbidden(*a, **k):
        pytest.fail("No AI or heuristic targeting for a verified sitemap")
    for name in ("_openai_generate_file_patch", "_ai_map_urls_to_files", "_ai_pick_repo_files", "_github_tarball_grep"):
        monkeypatch.setattr(m, name, forbidden)
    rows = [row(A, B), row(B)]
    prep = prepare(rows)
    applied = m._apply_prepared_issue_fix(owner="fixture", repo_name="fixture", branch="baseline", token="unused",
        fix_branch="qa", all_paths=list(sources), issue_key=KEY, issue_label=KEY, impacted=[A], site_name="site.test",
        file_state={}, max_files=1, prep=prep, pages=rows, index=repo_index.build_repo_index(list(sources)))
    assert not applied["ai_files"]
    if case == "verified":
        assert written == {"sitemap.xml": source.replace(entry(A), "")}
    else:
        assert not written
