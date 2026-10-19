"""A missing authorization, provider failure and successful empty answer differ."""
from urllib.parse import parse_qs, urlencode, urlsplit

import pytest

from backend import app as m
from test_overview_search_connections import connections as _connections


@pytest.fixture()
def connections(tmp_path, monkeypatch):
    return _connections.__wrapped__(tmp_path, monkeypatch)


@pytest.mark.parametrize("source", ["gsc", "bing"])
def test_successful_empty_answer_never_borrows_old_dates_or_csv(connections, monkeypatch, source):
    data = connections
    monkeypatch.setattr(m, "_crawl_items_fallback", lambda *a: pytest.fail("Old rows must not replace this period"))
    payload = {"ok": True, "source": source, "items": [], "days": 7,
               "start_date": "2026-10-10", "end_date": "2026-10-16"}
    monkeypatch.setattr(m, f"_fetch_{source}_live_items", lambda **kw: payload)
    root = f'/projects/{data["slug"]}'
    page = data["client"].get(f"{root}/performance?source={source}&days=7")
    assert page.status_code == 200
    assert page.context["rows"] == []
    assert "Aucune recherche ou page reçue" in page.text
    assert "2026-10-10" in page.text
    assert "Connectez" not in page.text
    api = data["client"].get(f"/api{root}/search-items?source={source}&dim=query&days=7")
    assert api.json() == payload
    csv = data["client"].get(f"/api{root}/search-items?source={source}&dim=query&days=7&format=csv")
    assert len(csv.text.splitlines()) == 1
    assert "2026-10-10-2026-10-16" in csv.headers["content-disposition"]


@pytest.mark.parametrize("source", ["gsc", "bing"])
@pytest.mark.parametrize("reason,kind", [("disabled", "connect"), ("missing_credentials", "connect"),
                                         ("oauth_invalid_grant", "connect"), ("request_failed", "unavailable"),
                                         ("oauth_not_configured", "unavailable")])
def test_provider_states_keep_filters_and_the_project_destination(connections, monkeypatch, source, reason, kind):
    data = connections
    monkeypatch.setattr(m, f"_fetch_{source}_live_items", lambda **kw: {
        "ok": False, "source": source, "reason": reason, "error": "Provider unavailable"})
    path = f'/projects/{data["slug"]}/performance'
    query = urlencode({"source": source, "days": 90, "dim": "page", "q": "services & été", "page": 2})
    page = data["client"].get(f"{path}?{query}")
    assert page.status_code == 200
    assert page.context["availability"]["kind"] == kind
    assert 'name="days"' in page.text and 'name="source"' in page.text
    target = urlsplit(page.context["connections_url"])
    assert target.path == f'/projects/{data["slug"]}/settings/search'
    assert parse_qs(target.query)["next"] == [f"{path}?{query}"]
    assert target.fragment == f"{source}-connection"
    assert '/settings/accounts#' not in page.text


@pytest.mark.parametrize("source", ["gsc", "bing"])
def test_oauth_returns_to_site_connections_then_original_filtered_result(connections, source):
    data = connections
    result = f'/projects/{data["slug"]}/performance?source={source}&days=90&q=service%20%C3%A9t%C3%A9'
    path = f'/projects/{data["slug"]}/settings/search?{urlencode({"next": result})}'
    page = data["client"].get(path)
    assert page.status_code == 200
    assert page.context["return_to"] == result
    connect = data["client"].get(page.context["search_connections"][source]["connect_url"], follow_redirects=False)
    state = parse_qs(urlsplit(connect.headers["location"]).query)["state"][0]
    provider = "google" if source == "gsc" else "bing"
    callback = data["client"].get(f"/oauth/{provider}/callback?{urlencode({'code': 'test', 'state': state})}", follow_redirects=False)
    returned = data["client"].get(callback.headers["location"])
    assert returned.status_code == 200
    assert returned.context["return_to"] == result
    assert returned.context["search_connections"][source]["connected"]
    assert data["other"].get(path).status_code == 404


@pytest.mark.parametrize("target", ["//evil.example/", "/projects/another/performance", "/settings/accounts"])
def test_connection_page_return_stays_in_current_project(connections, target):
    data = connections
    page = data["client"].get(f'/projects/{data["slug"]}/settings/search?{urlencode({"next": target})}')
    assert page.context["return_to"] == f'/projects/{data["slug"]}'


def test_keyword_failure_is_not_a_disconnection_or_a_conversion_claim(connections, monkeypatch):
    data = connections
    path = f'/projects/{data["slug"]}/keywords/opportunities?days=7'
    monkeypatch.setattr(m, "_fetch_gsc_live_items", lambda **kw: {"ok": False, "reason": "request_failed"})
    failed = data["client"].get(path)
    assert "Les données Google sont indisponibles" in failed.text
    assert "Search Console n'est pas connecté" not in failed.text
    monkeypatch.setattr(m, "_fetch_gsc_live_items", lambda **kw: {"ok": True, "items": []})
    empty = data["client"].get(path)
    assert "Aucune recherche reçue ne remplit les critères" in empty.text
    assert "Cela ne mesure ni les conversions ni les ventes" in empty.text
    assert "le site convertit correctement" not in empty.text


def test_table_filter_does_not_change_source_totals_or_invent_position(connections, monkeypatch):
    data = connections
    monkeypatch.setattr(m, "_fetch_gsc_live_items", lambda **kw: {"ok": True, "items": [
        {"keyword": "atelier", "clicks": 3, "impressions": 100, "ctr": .03, "position": 0},
        {"keyword": "service", "clicks": 2, "impressions": 200, "ctr": .01, "position": 0}]})
    page = data["client"].get(f'/projects/{data["slug"]}/performance?q=atelier')
    assert page.context["totals"]["clicks"] == 5
    assert page.context["source_rows"] == 2
    assert page.context["total_rows"] == 1
    assert 'stat-strip-num">—</div>' in page.text


def test_disconnected_overview_does_not_request_empty_live_widgets(connections):
    page = connections["client"].get(f'/projects/{connections["slug"]}')
    assert 'data-live-url="/api/projects/' not in page.text
    assert 'data-search-mini' not in page.text
