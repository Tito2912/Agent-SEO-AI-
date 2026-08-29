"""The rules that turn Search Console reporting into something to act on.

Every threshold here was moved at least once by measuring against a real customer account
(voiceoverstudioai.com, 2026-08-29), and each move is pinned below so the next reader can see
what the numbers are for rather than guess.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

from backend import keywords as kw  # noqa: E402


def row(query, impressions, clicks, position, **extra):
    ctr = extra.pop("ctr", (clicks / impressions) if impressions else 0.0)
    return {"query": query, "impressions": impressions, "clicks": clicks,
            "position": position, "ctr": ctr, **extra}


# The account that shaped the rules: nine queries, thousands of impressions, almost no clicks,
# and not one top-3 ranking to calibrate against.
REAL_ACCOUNT = [
    row("kling ai", 7196, 10, 6.82, ctr=0.00139),
    row("official kling ai pricing free credits 2026", 700, 0, 8.76),
    row("pictory test", 522, 0, 7.80),
    row("pictory ai pricing", 481, 0, 7.91),
    row("pictory pricing", 373, 0, 8.99),
    row("kling ai pricing official 2026", 367, 0, 9.65),
    row("kling ai free credits 2026", 340, 0, 10.24),
    row("kling ai pricing free credits official 2026", 302, 0, 8.63),
    row("official kling ai pricing free credits video", 234, 0, 9.79),
]


def test_the_biggest_opportunity_is_not_the_one_that_gets_dropped() -> None:
    """The row that shaped the fallback rule.

    7 196 impressions at position 6.8 for 10 clicks was invisible: it HAS clicks, so it is not
    "never clicked", and the account has no top-3 query, so there was nothing for it to be
    "less than". The single biggest opportunity on the account was the one being skipped.
    """
    opportunities = kw.find_opportunities(REAL_ACCOUNT)
    assert opportunities, "the real account produced nothing at all"
    first = opportunities[0]
    assert first["query"] == "kling ai"
    assert first["kind"] == kw.KIND_LOW_CTR
    assert first["impressions"] == 7196


def test_a_page_one_query_with_no_clicks_needs_no_baseline() -> None:
    # The rule that always works: it is a fact about the account, not a comparison.
    opportunities = {o["query"]: o for o in kw.find_opportunities(REAL_ACCOUNT)}
    assert opportunities["pictory ai pricing"]["kind"] == kw.KIND_NO_CLICKS


def test_position_eleven_is_near_page_one_not_page_one() -> None:
    opportunities = {o["query"]: o for o in kw.find_opportunities(REAL_ACCOUNT)}
    assert opportunities["kling ai free credits 2026"]["kind"] == kw.KIND_NEAR_PAGE_ONE


def test_the_estimate_is_withheld_when_there_is_nothing_to_compare_against() -> None:
    """A number a customer cannot audit is worse than no number.

    With no top-3 ranking the site has no demonstrated rate, so no gain is claimed — `None`,
    which is a different answer from zero.
    """
    assert kw.top_position_ctr(REAL_ACCOUNT) is None
    opportunities = kw.find_opportunities(REAL_ACCOUNT)
    assert all(o["potential_clicks"] is None for o in opportunities)
    assert kw.summarise(opportunities)["potential_clicks"] is None


def test_the_site_own_top_three_rate_is_preferred_over_the_flat_floor() -> None:
    """When the site HAS top-3 queries, its own rate is the comparison — not a constant."""
    rows = REAL_ACCOUNT + [row("marque exacte", 1000, 250, 1.4)]  # 25 % when it ranks well
    baseline = kw.top_position_ctr(rows)
    assert baseline and abs(baseline - 0.25) < 1e-9

    opportunities = {o["query"]: o for o in kw.find_opportunities(rows)}
    kling = opportunities["kling ai"]
    assert kling["reference_ctr"] == baseline
    assert kling["potential_clicks"] == pytest.approx(round(7196 * (0.25 - 0.00139)), abs=1)


def test_a_top_three_query_is_never_an_opportunity() -> None:
    # Nothing to promise on a query already at the top; flagging it would be noise.
    rows = [row("marque exacte", 1000, 250, 1.4)]
    assert kw.find_opportunities(rows) == []


def test_a_query_nobody_sees_is_not_a_recommendation() -> None:
    """Position and CTR on four impressions are noise, and advice built on noise is worse than
    silence."""
    rows = [row("requete confidentielle", 4, 0, 7.0)]
    assert kw.find_opportunities(rows) == []
    assert kw.find_opportunities(rows, min_impressions=1)


def test_a_healthy_page_one_query_is_left_alone() -> None:
    rows = [row("bonne requete", 1000, 120, 5.0)]  # 12 %, no top-3 row to compare with
    assert kw.find_opportunities(rows) == []


@pytest.mark.parametrize("field", ["query", "keyword", "keys"])
def test_the_query_column_is_read_under_every_name_it_arrives_with(field: str) -> None:
    """The first run against the real API returned ZERO opportunities: its rows are keyed
    `keyword`, and only `query` was read. Nine rows silently became none."""
    value = ["ma requête"] if field == "keys" else "ma requête"
    rows = [{field: value, "impressions": 500, "clicks": 0, "position": 7.0, "ctr": 0.0}]
    found = kw.find_opportunities(rows)
    assert len(found) == 1 and found[0]["query"] == "ma requête"


def test_rows_that_cannot_be_read_are_dropped_rather_than_scored() -> None:
    rows = [
        {"query": "", "impressions": 900, "clicks": 0, "position": 7.0},
        {"query": "sans position", "impressions": 900, "clicks": 0, "position": 0},
        {"query": "position illisible", "impressions": 900, "clicks": 0, "position": "n/a"},
        "pas un dictionnaire",
    ]
    assert kw.find_opportunities(rows) == []


def test_the_summary_counts_what_the_list_contains() -> None:
    opportunities = kw.find_opportunities(REAL_ACCOUNT)
    summary = kw.summarise(opportunities)
    assert summary["total"] == len(opportunities)
    assert sum(summary["by_kind"].values()) == len(opportunities)
    assert summary["impressions"] == sum(o["impressions"] for o in opportunities)


def test_the_order_is_by_impressions_a_fact_of_the_account() -> None:
    # Not by estimated gain: impressions are measured, an estimate is not.
    impressions = [o["impressions"] for o in kw.find_opportunities(REAL_ACCOUNT)]
    assert impressions == sorted(impressions, reverse=True)
