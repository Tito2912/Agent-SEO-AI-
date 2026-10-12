"""Ce que l'apercu ne reproduit pas, le banc le juge sur ce qu'il SERT.

Banc complet du 12/10/2026 : une douzaine de familles restaient « unverified » sur les dix stacks.
L'apercu injecte un noindex qui sort ses pages du score (viewport, x-default, hreflang invalide,
doublons), et son robots.txt designe le sitemap de PRODUCTION, si bien que le sitemap corrige
n'etait jamais lu. Le banc juge desormais le HTML observe page par page, et LIT le sitemap servi
par chaque apercu.
"""
from __future__ import annotations

import pytest

from ops.gauntlet import live_cycle as bench

SITE = "https://fixture.test"


def page(path="/broken", **kw):
    return {"url": SITE + path, "status_code": 200, "content_type": "text/html", "x_robots_tag": "noindex", **kw}


def field_report(key, pages, count=1, examples=("/broken",)):
    return {"meta": {"base_url": SITE + "/"}, "pages": pages,
            "issues": {key: {"count": count, "examples": [SITE + e for e in examples]}}}


def verdict(key, prod_pages, old_pages, new_pages, **kw):
    prod = field_report(key, prod_pages, **kw)
    return bench.html_checks(prod, field_report(key, old_pages, count=0), field_report(key, new_pages, count=0),
                             {key})[0]["verdict"]


@pytest.mark.parametrize("after, expected", [
    (dict(meta_viewport_tag_count=1, meta_viewport="width=device-width, initial-scale=1"), "resolved_on_observed_html"),
    (dict(meta_viewport_tag_count=0, meta_viewport=None), "still_present"),
    (dict(meta_viewport_tag_count=1, meta_viewport="initial-scale=1"), "still_present"),
])
def test_viewport(after, expected) -> None:
    sans = page(meta_viewport_tag_count=0, meta_viewport=None)
    assert verdict("viewport_not_set", [sans], [sans], [page(**after)]) == expected


@pytest.mark.parametrize("after, expected", [
    ({"fr": SITE + "/broken", "x-default": SITE + "/broken"}, "resolved_on_observed_html"),
    ({"fr": SITE + "/broken"}, "still_present"),
])
def test_x_default(after, expected) -> None:
    sans = page(hreflang={"fr": SITE + "/broken"})
    assert verdict("x_default_hreflang_missing", [sans], [sans], [page(hreflang=after)]) == expected


@pytest.mark.parametrize("after, expected", [
    ({"fr-fr": SITE + "/broken"}, "resolved_on_observed_html"),
    ({"fr_fr": SITE + "/broken"}, "still_present"),
    ({}, "still_present"),          # supprimer l'annotation n'est pas la corriger
])
def test_invalid_hreflang_code(after, expected) -> None:
    fautive = page(hreflang={"fr_fr": SITE + "/broken"})
    assert verdict("hreflang_annotation_invalid", [fautive], [fautive], [page(hreflang=after)]) == expected


@pytest.mark.parametrize("key, field", [("duplicate_titles", "title"), ("duplicate_meta_descriptions", "meta_description")])
@pytest.mark.parametrize("after_a, after_b, expected", [
    ("Valeur propre a A", "Valeur propre a B", "resolved_on_observed_html"),
    # Une seule des deux reecrite suffit : la valeur de l'autre n'est plus partagee.
    ("Valeur propre a A", "Valeur commune", "resolved_on_observed_html"),
    ("Valeur commune", "Valeur commune", "still_present"),
    ("Valeur propre a A", "", "still_present"),       # vider n'est pas corriger
])
def test_duplicates_must_end_up_distinct(key, field, after_a, after_b, expected) -> None:
    jumelles = [page("/a", **{field: "Valeur commune"}), page("/b", **{field: "Valeur commune"})]
    after = [page("/a", **{field: after_a}), page("/b", **{field: after_b})]
    assert verdict(key, jumelles, jumelles, after, count=2, examples=("/a", "/b")) == expected


# --- le sitemap servi -------------------------------------------------------------------------

def sitemap(*urls):
    return "<urlset>" + "".join("<url><loc>%s</loc></url>" % u for u in urls) + "</urlset>"


def sitemap_verdict(key, flagged, before, after, after_pages=()):
    prod = {"meta": {"base_url": SITE}, "pages": [],
            "issues": {key: {"count": len(flagged), "examples": list(flagged)}}}
    baseline = {"meta": {"base_url": "https://baseline.test"}, "pages": []}
    corrected = {"meta": {"base_url": "https://corrected.test"}, "pages": list(after_pages)}
    served = {"https://baseline.test/sitemap.xml": sitemap(*before), "https://corrected.test/sitemap.xml": sitemap(*after)}
    return bench.sitemap_checks(prod, baseline, corrected, {key}, served.get)[0]["verdict"]


PAGE, ABSENT, HTTP = SITE + "/page", SITE + "/page-absente", "http://fixture.test/page"


@pytest.mark.parametrize("key, flagged, before, after, expected", [
    ("sitemap_4xx_page", [ABSENT], [PAGE, ABSENT], [PAGE], "resolved_on_served_sitemap"),
    ("sitemap_4xx_page", [ABSENT], [PAGE, ABSENT], [PAGE, ABSENT], "still_present"),
    # L'entree en clair doit etre REMPLACEE par sa version https, pas seulement retiree.
    ("sitemap_http_urls_for_https", [HTTP], [HTTP], [PAGE], "resolved_on_served_sitemap"),
    ("sitemap_http_urls_for_https", [HTTP], [HTTP], [], "still_present"),
    ("indexable_page_not_in_sitemap", [PAGE], [], [PAGE], "resolved_on_served_sitemap"),
    ("indexable_page_not_in_sitemap", [PAGE], [], [], "still_present"),
    # Le defaut absent de l'apercu de reference : rien a juger.
    ("sitemap_4xx_page", [ABSENT], [PAGE], [PAGE], "unverified"),
])
def test_the_served_sitemap_is_judged_entry_by_entry(key, flagged, before, after, expected) -> None:
    assert sitemap_verdict(key, flagged, before, after) == expected


def test_a_page_that_became_its_own_canonical_may_stay_in_the_sitemap() -> None:
    soignee = page("/page", canonical=PAGE)
    assert sitemap_verdict("sitemap_non_canonical_page", [PAGE], [PAGE], [PAGE], [soignee]) == "resolved_on_served_sitemap"
    autre = page("/page", canonical=SITE + "/maitresse")
    assert sitemap_verdict("sitemap_non_canonical_page", [PAGE], [PAGE], [PAGE], [autre]) == "still_present"


def test_without_a_fetcher_nothing_is_judged_and_nothing_is_fetched() -> None:
    prod = {"meta": {"base_url": SITE}, "pages": [], "issues": {"sitemap_4xx_page": {"count": 1, "examples": [ABSENT]}}}
    assert bench.sitemap_checks(prod, prod, prod, {"sitemap_4xx_page"}, None) == []
