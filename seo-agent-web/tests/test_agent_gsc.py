"""An exact-page finalized response, not a query row list or an invented zero."""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

from backend import agent_gsc  # noqa: E402
from backend.agent_results import compare_periods, summarise_period  # noqa: E402

URL = "https://atelier.example/reparation?mode=velo"
PROPERTY = "sc-domain:atelier.example"


class Response:
    def __init__(self, body=None, *, status=200, raw=None):
        self.status_code = status
        self.raw = raw if raw is not None else json.dumps(body).encode("utf-8")
        self.closed = False

    @property
    def text(self):
        pytest.fail("Response text must never be read or exposed")

    def iter_content(self, chunk_size):
        for i in range(0, len(self.raw), chunk_size):
            yield self.raw[i:i + chunk_size]

    def close(self):
        self.closed = True


def data(**changes):
    return {"responseAggregationType": "byPage", "rows": [{
        "keys": [URL], "clicks": 5, "impressions": 100, "ctr": 0.05, "position": 7,
    }], **changes}


@pytest.fixture(autouse=True)
def fixed_clock(monkeypatch):
    monkeypatch.setattr(agent_gsc, "_today_pacific", lambda: date(2026, 11, 17))
    monkeypatch.setattr(agent_gsc.requests, "post", lambda *a, **kw: pytest.fail("Test attempted real network"))


def fetch(*, response=None, post=None, token=None, **kwargs):
    response = response if response is not None else Response(data())
    token = token if token is not None else lambda path, scopes=None: "test-token-not-a-secret"
    return agent_gsc.fetch_page_period(
        fetch_module=SimpleNamespace(_get_access_token=token), credentials_path=Path("unused-fixture.json"),
        request_post=post if post is not None else lambda *a, **kw: response,
        **{"property_url": PROPERTY, "page_url": URL, "start_date": "2026-10-01", "end_date": "2026-10-07", **kwargs},
    )


def test_exact_page_payload_uses_existing_scope_and_fixed_endpoint_without_redirects():
    calls = []
    token_calls = []
    response = Response(data())

    def token(path, *, scopes):
        token_calls.append((path, scopes))
        return "fictional-token"

    def post(endpoint, **kwargs):
        calls.append((endpoint, kwargs))
        return response

    result = fetch(post=post, token=token)
    assert result["ok"] and result["complete"] and result["reason"] == ""
    assert token_calls == [(Path("unused-fixture.json"), None)]
    endpoint, call = calls[0]
    assert endpoint == "https://searchconsole.googleapis.com/webmasters/v3/sites/sc-domain%3Aatelier.example/searchAnalytics/query"
    assert call["allow_redirects"] is False and call["stream"] is True
    assert call["timeout"] == (5, 15)
    assert call["headers"]["Authorization"] == "Bearer fictional-token"
    body = call["json"]
    assert body == {"startDate": "2026-10-01", "endDate": "2026-10-07", "dimensions": ["page"],
                    "type": "web", "dataState": "final", "aggregationType": "auto", "rowLimit": 2,
                    "dimensionFilterGroups": [{"groupType": "and", "filters": [
                        {"dimension": "page", "operator": "equals", "expression": URL}]}]}
    assert response.closed
    assert result["property"] == PROPERTY and result["page_url"] == URL
    assert result["filters"] == {"search_type": "web"}
    assert summarise_period(result, page_url=URL)["metrics"]["clicks"] == 5


def test_url_prefix_property_is_encoded_and_the_exact_page_filter_preserves_case_and_query():
    prop = "https://atelier.example/"
    seen = []
    result = fetch(property_url=prop, post=lambda endpoint, **kw: seen.append(endpoint) or Response(data()))
    assert result["complete"] and result["property"] == prop
    assert "https%3A%2F%2Fatelier.example%2F" in seen[0]


@pytest.mark.parametrize("body", [{"responseAggregationType": "byPage"}, data(rows=[])], ids=["rows-omitted", "empty-list"])
def test_a_valid_empty_final_response_can_establish_zero_reported_clicks(body):
    result = fetch(response=Response(body))
    assert result["ok"] and result["complete"] and result["rows"] == []
    assert summarise_period(result, page_url=URL)["metrics"] == {
        "clicks": 0, "impressions": 0, "ctr": None, "position": None,
    }


@pytest.mark.parametrize("status,reason", [(301, "redirect_refused"), (302, "redirect_refused"),
    (401, "authorization_refused"), (403, "property_access_refused"), (429, "rate_limited"),
    (500, "http_error")], ids=["301", "302", "401", "403", "429", "500"])
def test_http_failures_never_read_error_bodies_or_turn_into_zero(status, reason):
    response = Response(raw=b"private upstream error must not escape", status=status)
    result = fetch(response=response)
    assert result["ok"] is False and result["complete"] is False and result["reason"] == reason
    assert response.closed and summarise_period(result, page_url=URL)["metrics"] is None
    assert "private" not in json.dumps(result)


@pytest.mark.parametrize("body,reason", [
    ({}, "aggregation_mismatch"),
    ([], "invalid_response"),
    ({"error": {"message": "private upstream message"}}, "invalid_response"),
    (data(responseAggregationType="byProperty"), "aggregation_mismatch"),
    (data(responseAggregationType=[]), "aggregation_mismatch"),
    (data(rows=None), "unexpected_rows"),
    (data(rows=[{}, {}]), "unexpected_rows"),
    (data(rows=[{"keys": ["https://elsewhere.example/p"]}]), "page_mismatch"),
    (data(metadata={"first_incomplete_date": "2026-10-04"}), "period_not_final"),
], ids=["empty-object", "array", "error-object", "property-aggregation", "bad-aggregation", "null-rows", "two-rows", "other-page", "incomplete-metadata"])
def test_incompatible_responses_are_unavailable_without_raw_body_content(body, reason):
    result = fetch(response=Response(body))
    assert not result["ok"] and not result["complete"] and result["reason"] == reason
    assert result["rows"] == [] and "private" not in json.dumps(result)


@pytest.mark.parametrize("field,value", [
    ("clicks", None), ("clicks", float("nan")), ("impressions", -1), ("clicks", 101),
    ("clicks", 1.5), ("ctr", 0.7), ("position", 0), ("position", float("inf")),
], ids=["missing", "nan", "negative", "over-one", "fractional-count", "wrong-ctr", "zero-position", "infinite-position"])
def test_metrics_must_be_present_finite_and_coherent(field, value):
    body = data()
    body["rows"][0][field] = value
    result = fetch(response=Response(body))
    assert not result["complete"] and result["reason"] == "invalid_metrics"


@pytest.mark.parametrize("raw,reason", [(b"not-json", "invalid_json"), (b"\xff", "invalid_json"),
    (b"x" * (agent_gsc.MAX_RESPONSE_BYTES + 1), "response_too_large")], ids=["not-json", "invalid-utf8", "oversized"])
def test_response_body_is_strictly_bounded_and_bad_json_never_looks_like_no_data(raw, reason):
    response = Response(raw=raw)
    result = fetch(response=response)
    assert not result["complete"] and result["reason"] == reason and response.closed


def test_network_and_authorization_errors_are_stable_and_do_not_expose_exception_details():
    def broken_post(*a, **kw):
        raise requests.Timeout("token-private-upstream")
    result = fetch(post=broken_post)
    assert result["reason"] == "network_error" and "token-private" not in json.dumps(result)
    def broken_token(*a, **kw):
        raise RuntimeError("refresh-token-private")
    result = fetch(token=broken_token)
    assert result["reason"] == "authorization_unavailable" and "private" not in json.dumps(result)


@pytest.mark.parametrize("dates,reason", [
    (("2026-10-07", "2026-10-01"), "invalid_dates"),
    (("bad", "2026-10-01"), "invalid_dates"),
    (("2026-11-10", "2026-11-15"), "period_not_final"),
    (("2026-11-10", "2027-01-01"), "period_not_final"),
    (("2024-11-10", "2024-11-15"), "period_out_of_retention"),
], ids=["reversed", "invalid", "fresh", "future", "outside-retention"])
def test_unusable_dates_are_refused_before_refreshing_tokens_or_contacting_google(dates, reason):
    def forbidden(*a, **kw):
        pytest.fail("No auth or HTTP for dates which cannot be finalized")
    result = fetch(start_date=dates[0], end_date=dates[1], token=forbidden, post=forbidden)
    assert not result["complete"] and result["reason"] == reason


@pytest.mark.parametrize("prop,page", [
    ("sc-domain:other.example", URL), ("https://atelier.example/shop/", URL),
    (PROPERTY, "javascript:alert(1)"), ("https://u:secret@atelier.example/", URL),
    (PROPERTY, "https://atelier.example.evil.test/reparation"),
], ids=["other-domain", "other-prefix", "bad-scheme", "credentials", "suffix-trap"])
def test_properties_and_pages_must_belong_to_the_same_search_console_scope(prop, page):
    result = fetch(property_url=prop, page_url=page, token=lambda *a, **kw: pytest.fail("No auth for invalid scope"))
    assert result["reason"] == "invalid_scope" and result["property"] == ""
    assert "secret" not in json.dumps(result)


def test_date_objects_and_the_finalization_boundary_are_supported():
    result = fetch(start_date=date(2026, 11, 8), end_date=date(2026, 11, 14))
    assert result["complete"] and result["end_date"] == "2026-11-14"


def test_invalid_search_type_is_unavailable_without_authentication():
    result = fetch(search_type=["web"], token=lambda *a, **kw: pytest.fail("No auth for invalid search type"))
    assert result["reason"] == "invalid_search_type"


def test_fetched_periods_feed_comparison_without_query_totals_or_causal_attribution():
    before = fetch()
    after = fetch(start_date="2026-10-09", end_date="2026-10-15")
    result = compare_periods(before, after, page_url=URL, change_at="2026-10-08T12:00:00Z")
    assert result["comparable"] and result["causality"] == "not_established"
    assert result["changes"]["clicks"]["absolute"] == 0
