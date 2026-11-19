"""Missingness and comparability are part of a result, not zero-valued success."""
from __future__ import annotations

import sys
from copy import deepcopy
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

from backend.agent_results import compare_periods, summarise_period  # noqa: E402

URL = "https://atelier.example/reparation"


def period(start="2026-10-01", end="2026-10-07", *, clicks=5, impressions=100, position=8, **kwargs):
    return {"ok": True, "complete": True, "page_url": URL, "property": "sc-domain:atelier.example",
            "start_date": start, "end_date": end, "dimension": "page", "filters": {"country": "fra"},
            "rows": [{"page": URL, "clicks": clicks, "impressions": impressions, "position": position}], **kwargs}


def after(**kwargs):
    return period("2026-10-09", "2026-10-15", **kwargs)


def compare(before=None, later=None, **kwargs):
    return compare_periods(before if before is not None else period(), later if later is not None else after(), page_url=URL, **kwargs)


def test_equal_complete_periods_compare_observations_without_claiming_causal_or_commercial_results():
    result = compare(later=after(clicks=12, impressions=200, position=6), change_at="2026-10-08T15:00:00Z")
    assert result["comparable"]
    assert result["changes"]["clicks"] == {"absolute": 7, "relative": 1.4}
    assert result["changes"]["ctr"]["absolute"] == pytest.approx(0.01)
    assert result["changes"]["position"]["absolute"] == -2
    assert result["causality"] == "not_established" and result["commercial_results"] is None


def test_missing_data_is_not_zero_but_an_explicitly_complete_empty_response_can_be_zero():
    assert summarise_period(None, page_url=URL)["metrics"] is None
    partial = summarise_period(period(complete=False), page_url=URL)
    assert partial["status"] == "partial" and partial["metrics"]["clicks"] == 5
    assert summarise_period(period(rows=[], complete=False), page_url=URL)["metrics"] is None
    empty = summarise_period(period(rows=[]), page_url=URL)
    assert empty["status"] == "complete"
    assert empty["metrics"] == {"clicks": 0, "impressions": 0, "ctr": None, "position": None}


def test_zero_baseline_never_creates_an_infinite_percentage_gain():
    result = compare(before=period(rows=[]), later=after(clicks=7))
    assert result["comparable"]
    assert result["changes"]["clicks"] == {"absolute": 7, "relative": None}
    assert result["changes"]["ctr"]["absolute"] is None


def test_ctr_uses_counts_and_position_uses_impression_weights_over_disjoint_daily_rows():
    rows = [
        {"page": URL, "date": "2026-10-01", "clicks": 1, "impressions": 10, "position": 2},
        {"page": URL, "date": "2026-10-02", "clicks": 9, "impressions": 90, "position": 10},
    ]
    result = summarise_period(period(rows=rows), page_url=URL)
    assert result["status"] == "complete"
    assert result["metrics"]["ctr"] == pytest.approx(0.1)
    assert result["metrics"]["position"] == pytest.approx(9.2)
    rows[1]["position"] = None
    metrics = summarise_period(period(rows=rows), page_url=URL)["metrics"]
    assert metrics["clicks"] == 10 and metrics["position"] is None


@pytest.mark.parametrize("changes,reason", [
    ({"end_date": "2026-10-16"}, "duration_mismatch"),
    ({"start_date": "2026-10-05", "end_date": "2026-10-11"}, "overlapping_or_reversed_periods"),
    ({"filters": {"country": "bel"}}, "scope_mismatch"),
    ({"property": "sc-domain:other.example"}, "scope_mismatch"),
    ({"page_url": "https://other.example/p"}, "incomplete_or_invalid_period"),
    ({"complete": False}, "incomplete_or_invalid_period"),
    ({"dimension": "query_page"}, "incomplete_or_invalid_period"),
], ids=["duration", "overlap", "filters", "property", "page", "incomplete", "query-dimension"])
def test_periods_must_have_the_same_scope_and_duration(changes, reason):
    result = compare(later=after(**changes))
    assert not result["comparable"] and result["reason"] == reason
    assert result["changes"] is None


@pytest.mark.parametrize("bad", [None, float("nan"), float("inf"), -1, True, "not-a-number"],
                         ids=["missing", "nan", "infinite", "negative", "boolean", "text"])
def test_invalid_counts_cannot_be_reported_as_success(bad):
    result = summarise_period(period(clicks=bad), page_url=URL)
    assert result["status"] == "invalid" and result["metrics"] is None


def test_native_api_page_keys_are_supported_and_unknown_position_does_not_erase_counts():
    row = {"keys": [URL], "clicks": 3, "impressions": 40}
    result = summarise_period(period(rows=[row]), page_url=URL)
    assert result["status"] == "complete"
    assert result["metrics"]["clicks"] == 3 and result["metrics"]["position"] is None


@pytest.mark.parametrize("rows,reason", [
    ([{"page": URL, "clicks": 1, "impressions": 10}] * 2, "duplicate_or_invalid_bucket"),
    ([{"page": URL, "query": "vélo", "clicks": 1, "impressions": 10}], "row_scope_mismatch"),
    ([{"page": "https://evil.example/p", "clicks": 1, "impressions": 10}], "row_scope_mismatch"),
    ([{"page": URL, "date": "2025-01-01", "clicks": 1, "impressions": 10}], "duplicate_or_invalid_bucket"),
    ([{"page": URL, "clicks": 1, "impressions": 10}, {"page": URL, "date": "2026-10-02", "clicks": 1, "impressions": 10}], "overlapping_buckets"),
], ids=["duplicate", "query", "foreign", "outside-dates", "overlap-daily"])
def test_incompatible_or_overlapping_rows_cannot_be_added(rows, reason):
    result = summarise_period(period(rows=rows), page_url=URL)
    assert result["reason"] == reason and result["metrics"] is None


def test_change_day_is_excluded_and_dates_are_interpreted_in_gsc_pacific_calendar():
    assert compare(change_at="2026-10-08T15:00:00Z")["comparable"]
    assert compare(change_at="2026-10-08T01:00:00Z")["reason"] == "periods_do_not_bracket_change"
    assert compare(change_at="2026-10-08")["reason"] == "invalid_change_date"
    assert compare(change_at="2026-10-09T15:00:00Z")["reason"] == "periods_do_not_bracket_change"


def test_date_aliases_are_accepted_without_mutating_the_inputs():
    first, second = period(), after()
    first["date_start"] = first.pop("start_date")
    first["date_end"] = first.pop("end_date")
    snapshot = deepcopy((first, second))
    assert compare_periods(first, second, page_url=URL)["comparable"]
    assert (first, second) == snapshot
