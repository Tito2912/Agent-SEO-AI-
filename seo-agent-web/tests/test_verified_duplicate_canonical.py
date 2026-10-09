"""Duplicate metadata alone cannot authorize canonical consolidation."""

import base64
from urllib.parse import unquote

import pytest

from backend import app as m, repo_index

KEY = "duplicate_pages_without_canonical"
SITE = "https://site.test"
A, B = SITE + "/a", SITE + "/b"


def row(url, title="Shared title", description="Shared description", **extra):
    return {"url": url, "final_url": url, "status_code": 200, "content_type": "text/html",
        "canonical": None, "title": title, "meta_description": description, "lang": "fr",
        "h1": ["Shared heading"], "text_word_count": 80, "content_sketch": list(range(30)),
        "image_urls": [], "internal_links": [SITE + "/"], "external_links": [], "ld_json_blocks": 0, **extra}


def prepare(rows, impacted=None):
    impacted = [A, B] if impacted is None else impacted
    return m._prepare_issue_fix(issue_key=KEY, issues={KEY: {"count": len(impacted), "examples": impacted}},
        impacted=impacted, all_paths=["a.html", "b.html", "control.html"], site_name="site.test",
        owner="fixture", repo_name="fixture", branch="baseline", token="unused", pages=rows)


def test_verified_twins_share_one_stable_master_including_self_canonical():
    rows = [row(A), row(B)]
    assert m._duplicate_canonical_masters([A, B], rows) == {A: A, B: A}
    assert m._duplicate_canonical_masters([B, A], list(reversed(rows))) == {A: A, B: A}
    prep = prepare(rows)
    assert prep.get("canonical_masters") == {A: A, B: A} and not prep["refusal"]
    assert not prep["rewriter_ai_fallback"]


@pytest.mark.parametrize("change", [
    {"status_code": 404}, {"status_code": 503}, {"status_code": "200"}, {"status_code": True},
    {"content_type": "application/json"}, {"error": "timeout"}, {"blocked_by_host": True},
    {"meta_robots": "googlebot: noindex"}, {"x_robots_tag": "NOINDEX"}, {"final_url": SITE + "/redirected"},
    {"canonical": SITE + "/unknown-master"}, {"content_sketch": []}, {"content_sketch": [True, 1]},
    {"content_sketch": ["1", "2"]}, {"text_word_count": 0}, {"text_word_count": "80"},
    {"ld_json_blocks": 1},
])
def test_unhealthy_incomplete_or_unknown_master_group_refuses_without_model(change):
    rows = [row(A, **change), row(B, **change)]
    prep = prepare(rows)
    assert prep["refusal"] and not prep.get("canonical_masters")


@pytest.mark.parametrize("change", [
    {"h1": ["Different heading"]}, {"content_sketch": list(range(1, 31))}, {"text_word_count": 81},
    {"lang": "en"}, {"image_urls": [SITE + "/different-product.png"]},
    {"internal_links": [SITE + "/different-target"]}, {"external_links": ["https://other.test/"]},
])
def test_shared_metadata_does_not_consolidate_different_observed_content(change):
    prep = prepare([row(A), row(B, **change)])
    assert prep["refusal"] and not prep.get("canonical_masters")


def test_description_can_group_different_titles_when_content_observations_match():
    assert m._duplicate_canonical_masters([A, B], [row(A, title="First"), row(B, title="Second")]) == {A: A, B: A}


def test_an_existing_verified_self_master_is_preferred_to_the_shortest_url():
    assert m._duplicate_canonical_masters([A], [row(A), row(B, canonical=B)]) == {A: B}


def test_two_existing_self_masters_are_ambiguous():
    assert prepare([row(A), row(B, canonical=B), row(SITE + "/c", canonical=SITE + "/c")], [A])["refusal"]


def test_metadata_bridge_without_a_shared_group_key_is_ambiguous():
    c = SITE + "/c"
    assert not m._duplicate_canonical_masters([A, B, c], [row(A, "Title one", "Desc one"),
        row(B, "Title one", "Desc two"), row(c, "Title two", "Desc two")])


def test_incomplete_impacted_group_does_not_elect_a_new_master_from_a_subset():
    assert not m._duplicate_canonical_masters([A], [row(A), row(B)])
    assert not m._duplicate_canonical_masters([A, B], [row(A)])


def test_duplicate_rows_are_not_twins_and_conflicting_observations_are_refused():
    assert not m._duplicate_canonical_masters([A], [row(A), row(A)])
    assert not m._duplicate_canonical_masters([A, B], [row(A), row(B), row(A, h1=["Different"])])


def html(url, canonical=""):
    return ('<!doctype html><html lang="fr"><head>\r\n<title>Shared title</title>\r\n'
        + ('<link rel="canonical" href="' + canonical + '" />\r\n' if canonical else '')
        + '<meta property="og:url" content="' + url + '" />\r\n'
        + '<link rel="alternate" hreflang="fr" href="' + url + '" />\r\n'
        + '</head><body><h1>Shared heading</h1><a href="' + SITE + '/">Control</a></body></html>\r\n')


@pytest.mark.parametrize("case", ["verified", "unrelated_route", "computed_source", "insufficient_budget", "stale_canonical",
    "one_stale_page", "different_current_body", "scripted_page", "templated_page", "missing_sha", "ambiguous_route",
    "current_noindex", "current_language", "duplicate_attribute", "existing_master", "changed_existing_master",
    "read_failure", "write_failure"])
def test_real_pipeline_preflights_all_sources_and_is_page_bound_and_model_free(monkeypatch, case):
    sources = {"a.html": html(A), "b.html": html(B), "control.html": html(A)}
    if case == "unrelated_route":
        sources = {"component.html": html(A)}
    elif case == "computed_source":
        sources = {"a.tsx": '<Head><meta property="og:url" content={url} /></Head>'}
    elif case == "stale_canonical":
        sources = {"a.html": html(A, SITE + "/existing"), "b.html": html(B, SITE + "/existing")}
    elif case == "one_stale_page":
        sources["b.html"] = html(B, B)
    elif case == "different_current_body":
        sources["b.html"] = html(B).replace("Shared heading", "Changed heading")
    elif case == "scripted_page":
        sources["b.html"] = html(B).replace("</head>", "<script>var x = 1;</script></head>")
    elif case == "templated_page":
        sources["b.html"] = html(B).replace("Shared heading", "{{ heading }}")
    elif case == "current_noindex":
        sources["b.html"] = html(B).replace("</head>", '<meta name="robots" content="noindex" /></head>')
    elif case == "current_language":
        sources["b.html"] = html(B).replace('lang="fr"', 'lang="en"')
    elif case == "duplicate_attribute":
        sources["b.html"] = html(B).replace('property="og:url"', 'property="og:url" content="https://other.test/"')
    rows, impacted = [row(A), row(B)], [A, B]
    if case == "ambiguous_route":
        rows, impacted = [row(A), row(A + "?view=copy")], [A, A + "?view=copy"]
    if case in {"existing_master", "changed_existing_master"}:
        sources["b.html"] = html(B, B)
        rows, impacted = [row(A), row(B, canonical=B)], [A]
        if case == "changed_existing_master":
            sources["b.html"] = sources["b.html"].replace("Shared heading", "Changed heading")
    written = {}
    monkeypatch.setattr(m, "_github_tarball_grep", lambda *a, **k: list(sources))
    def get(path, **kwargs):
        source_path = unquote(path.split("/contents/", 1)[1])
        if case == "read_failure" and source_path == "b.html":
            raise OSError("unreadable")
        return {"sha": "" if case == "missing_sha" else "original", "content":
            base64.b64encode(sources[source_path].encode()).decode()}

    monkeypatch.setattr(m, "_github_api_get", get)

    def put(path, **kwargs):
        if case == "write_failure":
            raise OSError("GitHub write refused")
        written[unquote(path.split("/contents/", 1)[1])] = base64.b64decode(kwargs["json_body"]["content"]).decode()
        return {"content": {"sha": "patched"}}

    monkeypatch.setattr(m, "_github_api_put", put)
    for name in ("_openai_generate_file_patch", "_ai_map_urls_to_files", "_ai_pick_repo_files"):
        monkeypatch.setattr(m, name, lambda *a, **k: pytest.fail("No model or AI targeting for a canonical convention"))
    prep = prepare(rows, impacted)
    applied = m._apply_prepared_issue_fix(owner="fixture", repo_name="fixture", branch="baseline", token="unused",
        fix_branch="qa-correction", all_paths=list(sources), issue_key=KEY, issue_label=KEY, impacted=impacted,
        site_name="site.test", file_state={}, max_files=1 if case == "insufficient_budget" else 4,
        prep=prep, pages=rows, index=repo_index.build_repo_index(list(sources)), allow_ai_targeting=True)
    assert not applied["ai_files"]
    if case == "existing_master":
        assert set(written) == {"a.html"}
        assert 'rel="canonical" href="' + B + '"' in written["a.html"]
        assert 'property="og:url" content="' + B + '"' in written["a.html"]
    elif case != "verified":
        assert not written
    else:
        assert set(written) == {"a.html", "b.html"}
        for path, url in (("a.html", A), ("b.html", B)):
            expected = sources[path].replace('</head>', '<link rel="canonical" href="' + A + '" />\r\n</head>')
            expected = expected.replace('property="og:url" content="' + url, 'property="og:url" content="' + A)
            assert written[path] == expected
    assert m._fix_premise_note(KEY), "The conventional choice must remain human-reviewed even without a model"


@pytest.mark.parametrize("change", [
    lambda s: s.replace("</head>", '<meta property="og:url" content="https://site.test/second" /></head>'),
    lambda s: s.replace('content="https://site.test/a"', 'content=https://site.test/a'),
    lambda s: s.replace("</head>", '<base href="https://other.test/" /></head>'),
    lambda s: s.replace("</head>", ""),
    lambda s: s.replace("</head>", "</head><head></head>"),
    lambda s: s.replace('lang="fr"', ''),
    lambda s: s.replace("</body>", '<link rel="canonical" href="https://site.test/a" /></body>'),
])
def test_ambiguous_or_nonliteral_head_is_left_byte_identical(change):
    source = change(html(A))
    assert m._add_duplicate_canonical(source, A) == (source, 0)


def test_escaped_master_and_compact_html_preserve_other_attributes_and_line_endings():
    source = html(A).replace("\r\n", "")
    master = SITE + "/master?one=1&two=2"
    output, count = m._add_duplicate_canonical(source, master)
    escaped = master.replace("&", "&amp;")
    expected = source.replace('property="og:url" content="' + A, 'property="og:url" content="' + escaped)
    expected = expected.replace("</head>", '<link rel="canonical" href="' + escaped + '" /></head>')
    assert count == 1 and output == expected


@pytest.mark.parametrize("attributes", [
    'data-content="' + A + '" content="' + A + '"',
    'data-hint=\' content="' + A + '"\' content="' + A + '"',
    'content="' + A + '" data-content="' + A + '"',
])
def test_content_lookalikes_never_rewrite_an_unrelated_attribute(attributes):
    source = html(A).replace('content="' + A + '"', attributes)
    assert m._add_duplicate_canonical(source, B) == (source, 0)
