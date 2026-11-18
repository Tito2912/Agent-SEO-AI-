"""Evidence, consent memory and business relevance of a three-action plan."""
from __future__ import annotations

import sys
from copy import deepcopy
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

from backend.agent_strategy import project_url, recommend_actions  # noqa: E402

SITE = "https://atelier.example"
HOME, REPAIR, SERVICE = SITE + "/", SITE + "/reparation", SITE + "/atelier"


def plan(**kwargs):
    return recommend_actions(base_url=SITE, slug="atelier", **kwargs)


def page(url, title="Réparation vélo en atelier"):
    return {"url": url, "title": title, "status_code": 200, "indexable": True}


def query(url=REPAIR, **kwargs):
    return {"page": url, "keyword": "réparation vélo", "clicks": 1,
            "impressions": 500, "position": 7, "ctr": 0.002, **kwargs}


def test_indexability_of_an_important_page_precedes_more_content_and_deduplicates():
    report = {"pages": [page(HOME), page(REPAIR), page(SERVICE)], "issues": {
        "noindex_page": {"count": 1, "examples": [REPAIR]},
        "missing_title": {"count": 1, "examples": [REPAIR]},
        "missing_meta_description": {"count": 1, "examples": [SERVICE]},
    }}
    result = plan(report=report, query_rows=[query()], business_context={"priority_urls": [REPAIR]})
    assert [a["kind"] for a in result] == ["technical_indexability", "technical_metadata"]
    assert result[0]["page_url"] == REPAIR
    assert result[0]["evidence"]["issue_key"] == "noindex_page"
    assert "intentionnel" in result[0]["uncertainty"]
    assert len({a["page_url"] for a in result}) == len(result)


@pytest.mark.parametrize("status", ["refused", "dismissed", "done", "in_progress", "accepted"], ids=["refused", "dismissed", "done", "working", "accepted"])
def test_a_customer_decision_survives_report_order_and_hides_the_same_action(status):
    original = plan(query_rows=[query()])[0]
    assert plan(query_rows=[query()], decisions={original["action_key"]: status}) == []
    assert plan(query_rows=[query()], decisions=[{"action_key": original["action_key"], "status": status}]) == []
    assert plan(query_rows=[query()], decisions=[{"action_key": original["action_key"], "state": status}]) == []
    assert plan(query_rows=[query()], decisions={original["action_key"]: "proposed"})


def test_gsc_native_rows_are_supported_but_bad_measurements_cannot_create_an_action():
    valid = {"keys": ["réparation vélo", REPAIR], "clicks": 0, "impressions": 400, "position": 5}
    invalid = [query(position=float("nan")), query(clicks=None), query(impressions=float("inf")), query(clicks=600)]
    result = plan(query_rows=[*invalid, valid])
    assert len(result) == 1
    assert result[0]["evidence"]["clicks"] == 0
    assert result[0]["evidence"]["query"] == "réparation vélo"
    assert "prouve pas" in result[0]["uncertainty"]
    assert result[0]["next_href"] == "/projects/atelier/keywords/opportunities"
    assert "potential_clicks" not in result[0]["evidence"]


def test_missing_gsc_does_not_prevent_evidence_based_crawl_work():
    report = {"pages": [page(HOME)], "issues": {"missing_title": {"count": 1, "examples": [HOME]}}}
    result = plan(report=report)
    assert len(result) == 1 and result[0]["evidence"]["gsc_available"] is False
    assert "gsc_impressions_observed" not in result[0]["evidence"]
    assert plan() == []


def test_low_ctr_and_near_first_page_use_the_existing_page_not_new_article():
    result = plan(query_rows=[query(), query(SERVICE, impressions=900, clicks=50, position=13)])
    assert [a["page_url"] for a in result] == [SERVICE, REPAIR]
    assert all(a["kind"] == "improve_existing" for a in result)
    assert "cause" in result[0]["uncertainty"]


def topic(title="Réparation vélo : choisir ses freins"):
    return {"covered": False, "competitor_title": title,
            "competitor_url": "https://rival.example/freins", "terms": ["réparation", "vélo", "freins"]}


def test_a_competitor_topic_alone_is_not_a_business_case():
    assert plan(competitor_topics=[topic()]) == []
    assert plan(competitor_topics=[topic()], business_context={"services": ["installation plomberie"]}) == []
    result = plan(competitor_topics=[topic()], business_context={"services": ["réparation vélo"]})
    assert len(result) == 1 and result[0]["kind"] == "new_content"
    assert result[0]["page_url"] == ""
    assert result[0]["evidence"]["business_terms"] == ["réparation", "vélo"]
    assert "volume" in result[0]["uncertainty"]
    assert plan(competitor_topics=[topic()], business_context={"activity": "réparation vélo"})


def test_new_topics_are_rechecked_against_own_pages_and_business_exclusions():
    context = {"services": ["réparation vélo"]}
    assert plan(competitor_topics=[topic()], business_context=context,
                existing_pages=[page(REPAIR, "Réparation vélo : choisir ses freins")]) == []
    assert plan(competitor_topics=[topic()], business_context={**context, "excluded_topics": ["freins"]}) == []
    covered = {**topic(), "covered": True, "own_url": REPAIR}
    assert plan(competitor_topics=[covered], business_context=context) == []


def test_similar_competitor_topics_are_one_action_and_declared_not_exact_absence():
    result = plan(competitor_topics=[topic(), topic("Choisir des freins pour réparation vélo")],
                  business_context={"services": ["réparation vélo"]})
    assert len(result) == 1
    assert "pas été repéré" in result[0]["reason"]
    assert "échantillon" in result[0]["uncertainty"]


@pytest.mark.parametrize("url", ["https://evil.example/p", "//evil.example/p", "javascript:alert(1)",
                                  SITE + "/%2e%2e/private", SITE + "/x%0afoo", "https://u:pw@atelier.example/p"],
                         ids=["foreign", "protocol-relative", "javascript", "traversal", "control", "credentials"])
def test_external_or_unsafe_pages_never_become_actions(url):
    assert plan(query_rows=[query(url)]) == []
    assert not project_url(url, SITE)


def test_base_path_is_a_scope_and_internal_links_encode_the_project_slug():
    result = recommend_actions(base_url=SITE + "/shop", slug="a/b?c", query_rows=[query(SITE + "/shop/p")])
    assert result[0]["next_href"] == "/projects/a%2Fb%3Fc/keywords/opportunities"
    assert recommend_actions(base_url=SITE + "/shop", slug="s", query_rows=[query(SITE + "/shopping/p")]) == []


def test_no_mutation_no_more_than_three_actions_and_deterministic_ranking():
    data = [query(SITE + "/p" + str(i), impressions=500 + i) for i in range(8)]
    snapshot = deepcopy(data)
    result = plan(query_rows=data, limit=30)
    assert len(result) == 3 and result[0]["page_url"].endswith("p7")
    assert result == plan(query_rows=reversed(data), limit=30)
    assert data == snapshot
    assert plan(query_rows=data, limit=-1) == []


def test_an_issue_without_an_observed_project_page_or_important_target_is_not_prioritized():
    report = {"pages": [page(SERVICE)], "issues": {"noindex_page": {"count": 1, "examples": [SERVICE]}}}
    assert plan(report=report) == []
    assert plan(report=report, business_context={"expected_indexable_urls": [SERVICE]})
    assert plan(report=report, business_context={"important_pages": [SERVICE]})
    report["issues"]["noindex_page"]["examples"] = [SITE + "/not-observed"]
    assert plan(report=report, business_context={"expected_indexable_urls": [SITE + "/not-observed"]}) == []
