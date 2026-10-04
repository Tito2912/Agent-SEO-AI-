from __future__ import annotations

import copy
from types import SimpleNamespace

import pytest

from backend import app as m
from ops.gauntlet import title_cycle as bench
from ops.gauntlet.ai_budget import ClaudeBudget

SITE = "https://fixture.test"
DESC = "Une description de validation propre a cette page du parcours, qui presente les controles effectues avant sa publication."


def page(route, title, **kw):
    return {"url": SITE + route, "final_url": SITE + route, "status_code": 200, "content_type": "text/html",
            "title": title, "title_tag_count": 1, "meta_description": DESC + route,
            "meta_description_tag_count": 1, "canonical": SITE + route, **kw}


def reports():
    a, b = sorted(bench.PAIR)
    rows = [page(a, "Le titre commun du groupe", meta_description=DESC),
            page(b, "Le titre commun du groupe", meta_description=DESC),
            page("/missing/", "", title_tag_count=0), page("/short/", "Test"),
            page("/long/", "Un titre beaucoup trop long pour etre conserve dans le parcours. " * 2, meta_robots="noindex"),
            page("/multiple/", "Le titre retenu parmi les deux", title_tag_count=2),
            page("/untouched/", "Une autre page du parcours")]
    before = {"pages": rows, "issues": {k: {"count": 2 if k in bench.DUPLICATES else 1} for k in bench.KEYS}}
    scopes = {k: {"impacted_urls": [SITE + r for r in (sorted(bench.PAIR) if k in bench.DUPLICATES
                     else ["/missing/"] if k == "missing_title" else ["/multiple/"] if k == "multiple_title_tags"
                     else ["/short/", "/long/"])]} for k in bench.KEYS}
    after = copy.deepcopy(before)
    for i, p in enumerate(after["pages"]):
        if i < 2:
            p.update(title=f"Un titre distinct pour la page {i}", meta_description=DESC + f" Edition {i}.")
        elif i < 6:
            p.update(title=f"Titre valide pour la page de controle {i}", title_tag_count=1)
    return before, after, scopes


def checks(before, after, scopes):
    return {r["family"]: r["verdict"] for r in bench.html_checks(before, after, scopes)}


def test_all_positive_controls_resolve_without_hiding_native_noindex():
    b, a, scopes = reports()
    result = checks(b, a, scopes)
    assert all(v == "resolved_on_selected_observed_html" for k, v in result.items() if k != "collateral_metadata")
    assert result["collateral_metadata"] == "unchanged_or_coherent_social_updates"


@pytest.mark.parametrize("field,value,family", [
    ("title", "Court", "duplicate_titles"),
    ("title", "x" * 71, "duplicate_titles"),
    ("title_tag_count", 2, "duplicate_titles"),
    ("title_tag_count", None, "duplicate_titles"),
    ("meta_description", "Courte", "duplicate_meta_descriptions"),
    ("meta_description", "x" * 161, "duplicate_meta_descriptions"),
    ("meta_description_tag_count", 2, "duplicate_meta_descriptions"),
    ("status_code", 202, "duplicate_titles"),
])
def test_counter_zero_never_certifies_a_bad_rendered_value(field, value, family):
    b, a, scopes = reports()
    a["issues"] = {k: {"count": 0} for k in bench.KEYS}
    a["pages"][0][field] = value
    assert checks(b, a, scopes)[family] == "unverified"


def test_duplicate_with_an_unselected_page_is_not_resolved():
    b, a, scopes = reports()
    a["pages"][-1]["title"] = a["pages"][0]["title"]
    assert checks(b, a, scopes)["duplicate_titles"] == "unverified"


@pytest.mark.parametrize("change", [{"canonical": SITE}, {"meta_robots": "noindex"},
                                    {"x_robots_tag": "noindex"}, {"h1": ["Changed"]},
                                    {"hreflang": {"fr": SITE}}, {"og_image": SITE + "/other.jpg"}])
def test_collateral_or_noindex_on_selected_pages_is_not_a_pass(change):
    b, a, scopes = reports()
    a["pages"][0].update(change)
    assert checks(b, a, scopes)["collateral_metadata"] == "unverified"


def test_an_unrequested_change_to_an_unselected_page_is_not_a_pass():
    b, a, scopes = reports()
    a["pages"][-1]["meta_description"] += " Extra."
    assert checks(b, a, scopes)["collateral_metadata"] == "unverified"


def test_missing_or_added_html_routes_are_not_certified():
    b, a, scopes = reports()
    a["pages"].pop()
    assert checks(b, a, scopes)["collateral_metadata"] == "unverified"
    a["pages"].append(page("/extra/", "Une page supplementaire"))
    assert checks(b, a, scopes)["collateral_metadata"] == "unverified"


def test_absent_family_or_incomplete_pair_has_no_positive_control():
    b, a, scopes = reports()
    b["issues"]["duplicate_titles"]["count"] = 0
    assert checks(b, a, scopes)["duplicate_titles"] == "unverified"
    scopes["duplicate_meta_descriptions"]["impacted_urls"].pop()
    assert checks(b, a, scopes)["duplicate_meta_descriptions"] == "unverified"


def test_social_title_can_follow_the_actual_changed_title_not_an_unrelated_value():
    b, a, scopes = reports()
    b["pages"][0]["og_title"] = b["pages"][0]["title"]
    a["pages"][0]["og_title"] = a["pages"][0]["title"]
    assert checks(b, a, scopes)["collateral_metadata"] == "unchanged_or_coherent_social_updates"
    a["pages"][0]["og_title"] = "Unrelated"
    assert checks(b, a, scopes)["collateral_metadata"] == "unverified"


def test_duplicate_selection_keeps_all_unselected_occurrences_visible():
    b, _, _ = reports()
    urls = [SITE + r for r in sorted(bench.PAIR)] + [SITE + "/untouched/"]
    b["issues"]["duplicate_titles"]["examples"] = urls
    assert bench.select_urls(m, b, "duplicate_titles") == (urls[:2], urls[2:])
    b["issues"]["duplicate_titles"]["examples"] = urls[1:]
    with pytest.raises(ValueError, match="Both"):
        bench.select_urls(m, b, "duplicate_titles")


def test_customer_or_changed_seed_is_refused_before_writes(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="Hugo/Nuxt"):
        bench.cycle(None, "unused", "customer", tmp_path, ClaudeBudget(0))
    module = SimpleNamespace(_github_api_get=lambda *a, **kw: {"object": {"sha": "changed"}},
                             _github_ref_api_path=lambda *a: "/".join(a))
    monkeypatch.setattr(bench.live, "close_prs", lambda *a: [])
    r = bench.cycle(module, "unused", "hugo", tmp_path, ClaudeBudget(0))
    assert r["status"] == "failed" and r["pull_requests"] == []
    assert not r["pull_requests_closed_without_merge"] and r["ai_budget"]["attempted"] == 0


def test_preview_root_and_xml_sitemap_must_both_be_ready(monkeypatch):
    replies = iter([SimpleNamespace(status_code=404, headers={}),
                    SimpleNamespace(status_code=503, headers={}),
                    SimpleNamespace(status_code=200, headers={"content-type": "text/html"}),
                    SimpleNamespace(status_code=200, headers={"content-type": "application/xml"}, content=b"<urlset />")])
    monkeypatch.setattr(bench.live.requests, "get", lambda *a, **kw: next(replies))
    monkeypatch.setattr(bench.time, "sleep", lambda n: None)
    assert bench.preview_sitemap("https://fixture.test/") == b"<urlset />"


@pytest.mark.parametrize("status,content_type", [(404, "text/html"), (301, "text/html"), (200, "application/json")])
def test_failed_redirected_or_non_html_preview_root_is_not_ready(monkeypatch, status, content_type):
    monkeypatch.setattr(bench.live.requests, "get", lambda *a, **kw: SimpleNamespace(
        status_code=status, headers={"content-type": content_type}, content=b""))
    with pytest.raises(TimeoutError):
        bench.preview_sitemap("https://fixture.test/", timeout=0)
