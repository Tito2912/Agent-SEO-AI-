"""Resume a protected OG alignment only after the branch has repaired its HTTP canonical."""

from __future__ import annotations

import pytest

from backend import app as m


SITE = "https://fixture.test"
CANONICAL = SITE + "/page"
INSECURE = CANONICAL.replace("https://", "http://", 1)
OG = CANONICAL + "/"


def prepare(*, og=OG, canonical=INSECURE, extra_pages=None):
    pages = [{"url": CANONICAL + "/", "final_url": CANONICAL + "/", "status_code": 200,
              "canonical": canonical, "og_url": og}, *(extra_pages or [])]
    return m._prepare_issue_fix(
        issue_key="open_graph_url_not_matching_canonical", issues={}, impacted=[CANONICAL + "/"],
        all_paths=["index.html"], site_name="fixture.test", owner="fixture", repo_name="test",
        branch="main", token="fixture-only", pages=pages,
    )


SOURCES = [
    '<link rel="canonical" href="{canonical}">\n<meta property="og:url" content="{og}">',
    "useHead({{ link: [{{ rel: 'canonical', href: '{canonical}' }}], "
    "meta: [{{ property: 'og:url', content: '{og}' }}] }});",
    "export const metadata = {{ alternates: {{ canonical: '{canonical}' }}, "
    "openGraph: {{ url: '{og}' }} }};",
]


@pytest.mark.parametrize("source", SOURCES)
def test_the_repaired_branch_resumes_the_flagged_alignment_without_ai(source):
    prep = prepare()
    assert prep["link_rewriter"] is not None
    assert prep["rewriter_ai_fallback"] is False and prep["rewriter_is_ai"] is False
    raw = source.format(canonical=CANONICAL, og=OG)
    repaired, count = prep["link_rewriter"](raw)
    assert count == 1
    assert repaired == source.format(canonical=CANONICAL, og=CANONICAL)
    assert prep["link_rewriter"](repaired) == (repaired, 0)


@pytest.mark.parametrize("source", SOURCES)
def test_an_unrepaired_http_canonical_is_never_copied(source):
    prep = prepare()
    assert prep["link_rewriter"] is not None
    raw = source.format(canonical=INSECURE, og=OG)
    assert prep["link_rewriter"](raw) == (raw, 0)
    assert prep["rewriter_ai_fallback"] is False


def test_another_pages_canonical_cannot_satisfy_the_deferred_alignment():
    prep = prepare()
    raw = SOURCES[0].format(canonical=SITE + "/other", og=OG)
    assert prep["link_rewriter"] is not None
    assert prep["link_rewriter"](raw) == (raw, 0)


def test_a_deferred_pair_does_not_use_the_crawl_as_a_fallback_without_a_literal_canonical():
    prep = prepare()
    raw = '<meta property="og:url" content="' + OG + '">'
    assert prep["link_rewriter"] is not None
    assert prep["link_rewriter"](raw) == (raw, 0)


def test_the_original_pair_api_still_refuses_an_insecure_destination():
    pages = [{"url": CANONICAL, "canonical": INSECURE, "og_url": OG}]
    assert m._og_url_pairs_from_pages([CANONICAL], pages) == []


def test_a_known_dead_http_destination_is_not_deferred():
    prep = prepare(extra_pages=[{"url": INSECURE, "status_code": 404}])
    assert prep["url_pairs"] == []


def test_query_and_path_case_are_preserved_exactly():
    canonical = SITE + "/Page?lang=fr"
    og = SITE + "/Page/?lang=fr"
    prep = prepare(og=og, canonical=canonical.replace("https://", "http://", 1))
    raw = SOURCES[0].format(canonical=canonical, og=og)
    assert prep["link_rewriter"] is not None
    repaired, count = prep["link_rewriter"](raw)
    assert count == 1 and repaired == SOURCES[0].format(canonical=canonical, og=canonical)
