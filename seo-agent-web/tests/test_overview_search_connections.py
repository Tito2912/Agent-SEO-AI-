"""The overview badges manage the current project's connections, including shared accounts."""
from __future__ import annotations

import json
import os
import re
import time
import uuid
from urllib.parse import parse_qs, urlencode, urlsplit

import pytest
from fastapi.testclient import TestClient

from backend import app as m, auth
from backend.models import AccountMember, Project, User


@pytest.fixture()
def connections(tmp_path, monkeypatch):
    m.DB.create_tables()
    monkeypatch.setenv("SEO_AGENT_ENCRYPTION_KEY", "test-overview-encryption")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    monkeypatch.setenv("BING_WEBMASTER_API_KEY", "")
    monkeypatch.setattr(m, "_google_oauth_client", lambda: ("google-id", "google-secret"))
    monkeypatch.setattr(m, "_bing_oauth_client", lambda: ("bing-id", "bing-secret"))
    monkeypatch.setattr(m, "_bing_oauth_refresh_token_data", lambda **kw: {"access_token": "test-access", "expires_in": 3600})
    monkeypatch.setattr(m, "_google_oauth_exchange_code", lambda **kw: {"refresh_token": "test-google-refresh"})
    monkeypatch.setattr(m, "_bing_oauth_exchange_code", lambda **kw: {
        "refresh_token": "test-bing-refresh", "access_token": "test-access", "expires_in": 3600})
    monkeypatch.setattr(m, "_runs_dir_pour_slug", lambda *args: tmp_path)
    tag = uuid.uuid4().hex
    with m.DB.session() as db:
        users = [User(email=f"badge-{kind}-{tag}@example.invalid", password_hash="test", is_admin=False)
                 for kind in ("owner", "member", "other")]
        db.add_all(users)
        db.commit()
        owner, member, other = [str(user.id) for user in users]
        slugs = [f"badge-{tag}-{i}" for i in range(2)]
        db.add_all([Project(owner_user_id=owner, slug=slug, site_name="Atelier", base_url="https://atelier.example/",
                            settings={"crawl": {"max_pages": 123}, "gsc_api": {"enabled": False, "days": 90},
                                      "bing": {"enabled": False, "site_url": "https://atelier.example/"}}) for slug in slugs])
        db.add(AccountMember(owner_user_id=owner, member_user_id=member))
        db.commit()
    ts = "20261017-100000"
    for slug in slugs:
        audit = tmp_path / slug / ts / "audit"
        audit.mkdir(parents=True)
        (audit / "report.json").write_text(json.dumps({
            "meta": {"base_url": "https://atelier.example/", "gsc_api": {"enabled": True, "ok": False},
                     "bing": {"enabled": True, "ok": False}},
            "pages": [{"url": "https://atelier.example/", "status_code": 200}], "issues": {}}))

    def client(uid):
        result = TestClient(m.app)
        result.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
        return result

    return {"client": client(member), "other": client(other), "owner": owner, "member": member,
            "slug": slugs[0], "second": slugs[1], "ts": ts}


def _csrf(client, slug):
    response = client.get(f"/projects/{slug}")
    assert response.status_code == 200
    return re.search(r'name="_csrf"\s+value="([^"]+)"', response.text).group(1)


def _post(data, source, action, **kwargs):
    return data["client"].post(f'/projects/{data["slug"]}/search-connections/{source}',
                               data={"_csrf": _csrf(data["client"], data["slug"]), "action": action, **kwargs},
                               follow_redirects=False)


def _bing_token(owner, key):
    m._upsert_user_connection(user_id=owner, key=key, value="test-refresh", meta={
        "access_token": "test-access", "expires_at": time.time() + 3600})


@pytest.mark.parametrize("source", ["gsc", "bing"])
def test_badge_oauth_returns_to_overview_and_enables_only_this_project(connections, source):
    data = connections
    client, slug = data["client"], data["slug"]
    overview = client.get(f'/projects/{slug}?crawl={data["ts"]}&compare=20261001-100000')
    badge = overview.context["search_connections"][source]
    assert not badge["connected"]  # The old crawl says enabled, but credentials do not exist.
    assert badge["oauth_ready"]
    response = client.get(badge["connect_url"], follow_redirects=False)
    state = parse_qs(urlsplit(response.headers["location"]).query)["state"][0]
    provider = "google" if source == "gsc" else "bing"
    callback = client.get(f"/oauth/{provider}/callback?" + urlencode({"code": "test", "state": state}), follow_redirects=False)
    assert callback.status_code == 303
    target = urlsplit(callback.headers["location"])
    assert target.path == f"/projects/{slug}"
    assert target.fragment == f"{source}-connection"
    assert parse_qs(target.query)["crawl"] == [data["ts"]]
    assert parse_qs(target.query)["compare"] == ["20261001-100000"]
    refreshed = client.get(callback.headers["location"])
    assert refreshed.context["search_connections"][source]["connected"]
    assert "connecté" in refreshed.text
    settings = m._db_project(data["member"], slug).settings
    key = "gsc_api" if source == "gsc" else "bing"
    assert settings[key]["enabled"] is True
    assert settings["crawl"] == {"max_pages": 123}
    assert m._db_project(data["member"], data["second"]).settings[key]["enabled"] is False
    if source == "gsc":
        assert m._gsc_oauth_connected(data["owner"], slug)
        assert not m._gsc_oauth_connected(data["member"], slug)
        assert settings[key]["days"] == 90
    else:
        assert m._user_connection_row(user_id=data["owner"], key=m._bing_oauth_connection_key(slug))
        assert not m._user_connection_row(user_id=data["member"], key=m._bing_oauth_connection_key(slug))
        assert settings[key]["site_url"] == "https://atelier.example/"


@pytest.mark.parametrize("source", ["gsc", "bing"])
def test_disconnect_removes_only_project_credentials_and_stops_live_requests(connections, monkeypatch, source):
    data = connections
    owner, slug, second = data["owner"], data["slug"], data["second"]
    if source == "gsc":
        for project in (slug, second):
            m._gsc_oauth_save(owner, project, refresh_token="test-google", scope="test")
        monkeypatch.setattr(m, "_google_oauth_revoke_token", lambda *args: pytest.fail("Must preserve other projects' Google grants"))
    else:
        for key in (m._bing_oauth_connection_key(slug), m._bing_oauth_connection_key(second), m._BING_OAUTH_CONNECTION_KEY):
            _bing_token(owner, key)
    for project in (slug, second):
        m._set_project_search_enabled(m._db_project(owner, project), source, True)
    assert data["client"].get(f"/projects/{slug}").context["search_connections"][source]["connected"]
    response = _post(data, source, "disconnect", next=f"/projects/{slug}?connection={source}#{source}-connection")
    assert response.status_code == 303
    overview = data["client"].get(response.headers["location"])
    assert not overview.context["search_connections"][source]["connected"]
    assert "déconnecté pour ce projet." in overview.text
    live = data["client"].get(f"/api/projects/{slug}/search-series?source={source}")
    assert live.json()["reason"] == "disabled"
    if source == "gsc":
        assert not m._gsc_oauth_connected(owner, slug)
        assert m._gsc_oauth_connected(owner, second)
    else:
        assert not m._user_connection_row(user_id=owner, key=m._bing_oauth_connection_key(slug))
        assert m._user_connection_row(user_id=owner, key=m._bing_oauth_connection_key(second))
        assert m._user_connection_row(user_id=owner, key=m._BING_OAUTH_CONNECTION_KEY)
        assert overview.context["search_connections"][source]["can_resume"]
        assert _post(data, source, "connect").status_code == 303
        assert data["client"].get(f"/projects/{slug}").context["search_connections"][source]["connected"]


@pytest.mark.parametrize("source", ["gsc", "bing"])
def test_connect_failure_does_not_activate_and_cannot_redirect_outside_app(connections, monkeypatch, source):
    data = connections
    response = _post(data, source, "connect", next="//evil.example/")
    assert urlsplit(response.headers["location"]).path == "/"
    assert "err=" in response.headers["location"]
    key = "gsc_api" if source == "gsc" else "bing"
    assert m._db_project(data["owner"], data["slug"]).settings[key]["enabled"] is False
    badge = data["client"].get(f'/projects/{data["slug"]}').context["search_connections"][source]
    response = data["client"].get(badge["connect_url"], follow_redirects=False)
    state = parse_qs(urlsplit(response.headers["location"]).query)["state"][0]
    provider = "google" if source == "gsc" else "bing"
    response = data["client"].get(f"/oauth/{provider}/callback?" + urlencode({"state": state, "error": "access_denied"}), follow_redirects=False)
    page = data["client"].get(response.headers["location"])
    assert "access_denied" in page.text
    assert not page.context["search_connections"][source]["connected"]


@pytest.mark.parametrize("source", ["gsc", "bing"])
def test_badges_require_csrf_and_access_to_project(connections, source):
    data = connections
    path = f'/projects/{data["slug"]}/search-connections/{source}'
    assert data["client"].post(path, data={"action": "disconnect"}).status_code == 403
    # A stranger's own page supplies a valid CSRF token without granting project access.
    page = data["other"].get("/settings/accounts")
    token = re.search(r'name="_csrf"\s+value="([^"]+)"', page.text).group(1)
    assert data["other"].post(path, data={"action": "disconnect", "_csrf": token}).status_code == 404


@pytest.mark.parametrize("source", ["gsc", "bing"])
def test_expired_project_access_cannot_complete_badge_oauth(connections, source):
    data = connections
    badge = data["client"].get(f'/projects/{data["slug"]}').context["search_connections"][source]
    response = data["client"].get(badge["connect_url"], follow_redirects=False)
    state = parse_qs(urlsplit(response.headers["location"]).query)["state"][0]
    with m.DB.session() as db:
        from sqlalchemy import delete
        db.execute(delete(AccountMember).where(AccountMember.member_user_id == data["member"]))
        db.commit()
    provider = "google" if source == "gsc" else "bing"
    response = data["client"].get(f"/oauth/{provider}/callback?" + urlencode({"state": state, "code": "test"}), follow_redirects=False)
    assert response.status_code in {303, 404}
    assert not m._gsc_oauth_connected(data["owner"], data["slug"])
    assert not m._user_connection_row(user_id=data["owner"], key=m._bing_oauth_connection_key(data["slug"]))
