"""CMS writes require audited targets, approval, a current snapshot and a durable receipt."""
from __future__ import annotations

import copy
import json
import socket
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

from backend import app as m, cms_guidance, cms_routes, first_correction, wordpress_cms as wp
from backend.models import CmsChange, IssueTask, Project
from test_first_verified_correction import URL, _choose, _verify, journey as journey


@pytest.fixture()
def cms(journey, monkeypatch):
    task = _choose(journey)
    monkeypatch.setenv("SEO_AGENT_ENCRYPTION_KEY", "test-cms-key")
    state = {"page": {"post_id": 5, "post_title": "Notre service", "permalink": URL, "post_status": "publish",
                       "post_type": "page", "seo_title": None, "seo_title_rendered": "Service — Atelier",
                       "meta_description": "Description initiale", "meta_description_rendered": "Description initiale"},
             "calls": [], "fail": "", "capability": True, "old_schema": False}

    def exchange(base, username, password, method, path, payload=None):
        state["calls"].append((method, path, copy.deepcopy(payload)))
        if path.startswith("wp/v2/users/me"):
            return {"id": 7, "name": "Atelier", "capabilities": {"wpseo_edit_advanced_metadata": state["capability"]}}
        if "/run" not in path:
            return {"name": path.rsplit("abilities/", 1)[1], "input_schema": {"properties": {} if state["old_schema"] else {key: {} for key in wp.FIELDS}}}
        if method == "GET":
            assert parse_qs(urlsplit(path).query) == {"input[permalink]": [URL]}
            return [copy.deepcopy(state["page"])]
        assert payload["input"]["post_id"] == 5 and payload["input"]["permalink"] == URL
        assert set(payload["input"]) - {"post_id", "permalink"} <= wp.FIELDS
        if state["fail"] != "before":
            state["page"].update({key: value for key, value in payload["input"].items() if key in wp.FIELDS})
        if state["fail"]:
            raise wp.CmsError("Réponse interrompue", uncertain=True)
        return copy.deepcopy(state["page"])

    monkeypatch.setattr(wp, "exchange", exchange)
    journey.update(task=task, remote=state, base=f'/projects/{journey["slug"]}/cms')
    return journey


def post(data, suffix, **payload):
    client = data["client"]
    client.get(data["base"])
    return client.post(data["base"] + suffix, data={"_csrf": client.cookies.get(m._CSRF_COOKIE_NAME), **payload}, follow_redirects=False)


def connect(data):
    response = post(data, "/wordpress/connect", site_url="https://atelier.example", username="editor", password="test-application-password")
    assert response.status_code == 303 and "err=" not in response.headers["location"]


def preview(data, value="Service de réparation de vélos à Lyon — Atelier"):
    response = post(data, "/wordpress/preview", task_id=data["task"].id, value=value)
    assert response.status_code == 303 and "err=" not in response.headers["location"]
    return response.headers["location"].split("/changes/", 1)[1]


def action(data, change, operation):
    return post(data, f"/wordpress/changes/{change}/{operation}")


def row(change):
    with m.DB.session() as db:
        return db.get(CmsChange, change)


def writes(data):
    return [call for call in data["remote"]["calls"] if call[0] == "POST"]


def test_selection_adapts_guidance_without_connecting_or_losing_project_settings(cms):
    response = post(cms, "/platform", platform="webflow", next="https://outside.example/")
    assert response.status_code == 303 and not response.headers["location"].startswith("https:")
    with m.DB.session() as db:
        project = db.get(Project, cms["project_id"])
        assert project.settings["crawl"] == {"max_pages": 50}
        assert cms_guidance.settings(project.settings)["platform"] == "webflow"
        assert not cms_routes.connection_state(project)["connected"]
    page = cms["client"].get(f'/projects/{cms["slug"]}/first-correction')
    assert page.context["platform_guide"]["platform"] == "webflow"
    assert "Title Tag" in page.text and "Préparer dans WordPress" not in page.text
    assert not cms["remote"]["calls"]


def test_connection_is_read_only_encrypted_owner_scoped_and_never_rendered(cms):
    connect(cms)
    with m.DB.session() as db:
        project = db.get(Project, cms["project_id"])
        stored = m._user_connection_row(user_id=cms["owner"], key=cms_routes.connection_key(project))
        assert stored.secret_value.startswith(m._SECRET_PREFIX)
        assert "test-application-password" not in stored.secret_value
        assert "password" not in json.dumps(stored.meta)
        assert json.loads(m._decrypt_secret(stored.secret_value))["username"] == "editor"
        assert not m._user_connection_row(user_id=cms["member"], key=cms_routes.connection_key(project))
    page = cms["client"].get(cms["base"])
    assert page.status_code == 200 and page.headers["Cache-Control"] == "no-store"
    assert "test-application-password" not in page.text
    assert all(call[0] == "GET" for call in cms["remote"]["calls"])
    assert cms["stranger"].get(cms["base"]).status_code == 404


@pytest.mark.parametrize("kind", ["host", "permission", "schema"])
def test_connection_rejects_wrong_host_insufficient_rights_and_old_yoast(cms, kind):
    cms["remote"]["capability"] = kind != "permission"
    cms["remote"]["old_schema"] = kind == "schema"
    response = post(cms, "/wordpress/connect", site_url="https://wrong.example" if kind == "host" else "https://atelier.example", username="editor", password="secret")
    assert response.status_code == 303 and "err=" in response.headers["location"]
    assert not writes(cms)
    if kind == "host":
        assert not cms["remote"]["calls"]


def test_preview_requires_separate_approval_and_apply_does_not_claim_crawl_success(cms):
    connect(cms)
    change = preview(cms)
    assert row(change).state == "preview" and not writes(cms)
    for path in (f'/projects/{cms["slug"]}/first-correction', cms["base"] + "/wordpress/edit?task_id=" + cms["task"].id,
                 cms["base"] + "/wordpress/changes/" + change):
        page = cms["client"].get(path)
        assert page.status_code == 200 and "test-application-password" not in page.text
    assert not writes(cms)
    assert action(cms, change, "apply").status_code == 303
    assert row(change).state == "applied" and len(writes(cms)) == 1
    assert set(writes(cms)[0][2]["input"]) == {"post_id", "permalink", "seo_title"}
    with m.DB.session() as db:
        task = db.get(IssueTask, cms["task"].id)
        assert task.status == "in_progress" and not first_correction.note_data(task.note).get("verify")
    assert not cms["client"].get("/").context["onboarding"]["complete"]
    action(cms, change, "apply")
    assert len(writes(cms)) == 1
    assert _verify(cms).context["state"]["verified"]


@pytest.mark.parametrize("reason", ["external_edit", "expired", "reconnected", "changed_identity"])
def test_stale_previews_cannot_write(cms, reason):
    connect(cms)
    change = preview(cms)
    if reason == "external_edit":
        cms["remote"]["page"]["meta_description"] = "Modification externe"
    elif reason == "changed_identity":
        cms["remote"]["page"]["post_id"] = 99
    elif reason == "expired":
        with m.DB.session() as db:
            db.get(CmsChange, change).created_at = datetime.now(timezone.utc) - timedelta(minutes=21)
            db.commit()
    else:
        post(cms, "/wordpress/connect", site_url="https://atelier.example", username="editor", password="different-password")
    response = action(cms, change, "apply")
    assert "err=" in response.headers["location"] and not writes(cms)
    assert row(change).state == "preview"


@pytest.mark.parametrize("failure", ["before", "after"])
def test_lost_write_response_is_fenced_and_check_never_retries(cms, failure):
    connect(cms)
    change = preview(cms)
    cms["remote"]["fail"] = failure
    action(cms, change, "apply")
    assert row(change).state == "unknown" and len(writes(cms)) == 1
    action(cms, change, "apply")
    action(cms, change, "check")
    assert len(writes(cms)) == 1
    assert row(change).state == ("applied" if failure == "after" else "unknown")


def test_database_failure_after_remote_success_can_be_reconciled(cms, monkeypatch):
    connect(cms)
    change = preview(cms)
    original = cms_routes.confirmed
    monkeypatch.setattr(cms_routes, "confirmed", lambda *args: (_ for _ in ()).throw(RuntimeError("database unavailable")))
    action(cms, change, "apply")
    assert row(change).state == "applying"
    action(cms, change, "apply")
    assert len(writes(cms)) == 1
    monkeypatch.setattr(cms_routes, "confirmed", original)
    action(cms, change, "check")
    assert row(change).state == "applied" and len(writes(cms)) == 1


def test_restore_inherited_default_and_preserve_user_notes(cms):
    connect(cms)
    with m.DB.session() as db:
        db.get(IssueTask, cms["task"].id).note = "Prévenir le prestataire"
        db.commit()
    change = preview(cms)
    action(cms, change, "apply")
    action(cms, change, "restore")
    assert row(change).state == "restored" and cms["remote"]["page"]["seo_title"] is None
    assert writes(cms)[-1][2]["input"]["seo_title"] is None
    action(cms, change, "restore")
    assert len(writes(cms)) == 2
    with m.DB.session() as db:
        note = json.loads(db.get(IssueTask, cms["task"].id).note)
        assert note["user_note"] == "Prévenir le prestataire" and not note.get("verify")


def test_restore_will_not_overwrite_a_subsequent_edit(cms):
    connect(cms)
    change = preview(cms)
    action(cms, change, "apply")
    cms["remote"]["page"]["seo_title"] = "Nouveau travail du client"
    assert "err=" in action(cms, change, "restore").headers["location"]
    assert row(change).state == "applied" and len(writes(cms)) == 1


def test_description_uses_its_own_field_and_same_target_evidence(cms):
    connect(cms)
    cms["report"](issues={"missing_meta_description": {"count": 1, "examples": [URL]}})
    with m.DB.session() as db:
        db.get(IssueTask, cms["task"].id).issue_key = "missing_meta_description"
        db.commit()
    change = preview(cms, value="Découvrez nos prestations de réparation dans notre atelier de Lyon.")
    action(cms, change, "apply")
    assert row(change).data["field"] == "meta_description"
    assert set(writes(cms)[0][2]["input"]) == {"post_id", "permalink", "meta_description"}


def test_unrelated_tasks_and_changes_csrf_and_report_evidence_are_enforced(cms):
    connect(cms)
    assert post(cms, "/wordpress/preview", task_id="unrelated", value="Titre").status_code == 404
    assert action(cms, "unrelated", "apply").status_code == 404
    assert cms["client"].post(cms["base"] + "/wordpress/disconnect").status_code == 403
    cms["report"](issues={"missing_title": {"count": 1, "examples": ["https://atelier.example/other/"]}})
    response = post(cms, "/wordpress/preview", task_id=cms["task"].id, value="Titre")
    assert "err=" in response.headers["location"] and not writes(cms)


def test_disconnect_or_platform_switch_blocks_old_previews(cms):
    connect(cms)
    change = preview(cms)
    post(cms, "/platform", platform="shopify")
    assert "err=" in action(cms, change, "apply").headers["location"]
    assert not writes(cms)
    connect(cms)
    post(cms, "/wordpress/disconnect")
    assert "err=" in action(cms, change, "apply").headers["location"]
    assert not writes(cms)


@pytest.mark.parametrize("url", ["http://atelier.example", "https://wrong.example", "https://editor:secret@atelier.example", "https://atelier.example:444", "https://atelier.example/%2e%2e/admin", "https://atelier.example/?x=y", "https://atelier.example/a%0db"])
def test_authenticated_connection_target_is_strict(url):
    with pytest.raises(wp.CmsError):
        wp.site_url(url, "https://atelier.example")


@pytest.mark.parametrize("addresses", [["127.0.0.1"], ["::1"], ["169.254.169.254"], ["10.0.0.1"], ["93.184.216.34", "192.168.1.1"], ["224.0.0.1"]])
def test_all_dns_answers_must_be_public(monkeypatch, addresses):
    monkeypatch.setattr(socket, "getaddrinfo", lambda *a, **kw: [(0, 0, 0, "", (ip, 443)) for ip in addresses])
    with pytest.raises(wp.CmsError):
        wp.public_address("atelier.example")


def test_pinned_transport_keeps_tls_hostname_validation(monkeypatch):
    calls = []
    sock = SimpleNamespace(close=lambda: None)
    monkeypatch.setattr(socket, "create_connection", lambda address, timeout: calls.append(address) or sock)
    connection = wp._PinnedHTTPS("atelier.example", "93.184.216.34")
    assert connection._context.check_hostname and connection._context.verify_mode == 2
    connection._context = SimpleNamespace(wrap_socket=lambda value, server_hostname: calls.append(server_hostname) or sock)
    connection.connect()
    assert calls == [("93.184.216.34", 443), "atelier.example"]


# Named ids: pytest otherwise copies the 262145-byte body into the test id, then into the
# PYTEST_CURRENT_TEST environment variable, which Windows caps at 32767 characters.
@pytest.mark.parametrize("status,body,content_type", [(302, b'{}', 'application/json'), (401, b'password secret', 'text/html'), (200, b'x' * 262145, 'application/json'), (200, b'invalid json', 'application/json'), (200, b'<html/>', 'text/html')],
                         ids=["redirect", "secret-401", "oversized-body", "invalid-json", "html-200"])
def test_transport_never_follows_redirects_or_echoes_secrets(monkeypatch, status, body, content_type):
    calls = []
    monkeypatch.setattr(wp, "public_address", lambda host: "93.184.216.34")
    class Connection:
        def __init__(self, host, address):
            calls.append((host, address))
        def request(self, method, path, **kw):
            calls.append((method, path))
            assert kw["headers"]["Authorization"].startswith("Basic ")
        def getresponse(self):
            return SimpleNamespace(status=status, read=lambda size: body[:size], getheader=lambda *args: content_type)
        def close(self):
            pass
    monkeypatch.setattr(wp, "_PinnedHTTPS", Connection)
    with pytest.raises(wp.CmsError) as error:
        wp.exchange("https://atelier.example", "editor", "password secret", "POST", "ability/run", {})
    assert error.value.uncertain and "password secret" not in str(error.value) and len(calls) == 2


def test_client_rejects_wrong_page_schema_and_non_metadata_writes():
    client = wp.WordPressClient({"site_url": "https://atelier.example"})
    with pytest.raises(wp.CmsError):
        client.update({}, {"noindex": True})
    with pytest.raises(wp.CmsError):
        client.validate_page({"post_id": 5, "post_status": "draft", "post_type": "page", "permalink": URL}, URL)
    with pytest.raises(wp.CmsError):
        wp.values({"seo_title": None})
    assert not client.contains("https://outside.example/a/")
    assert wp.WordPressClient({"site_url": "https://atelier.example/wordpress"}).contains(URL)
    assert cms_guidance.field_for_issue("missing_h1") == ""
    assert cms_guidance.field_for_issue("meta_description_too_long_not_indexable") == "meta_description"


def test_deleting_project_removes_its_cms_credentials_and_journal(cms):
    from backend import auth
    from fastapi.testclient import TestClient
    import os
    connect(cms)
    change = preview(cms)
    client = TestClient(m.app)
    client.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(user_id=cms['owner'], secret=os.environ['SEO_AGENT_SECRET_KEY']))
    client.get(cms['base'])
    response = client.post('/projects/delete', data={'slugs': [cms['slug']], '_csrf': client.cookies.get(m._CSRF_COOKIE_NAME)}, follow_redirects=False)
    assert response.status_code == 303 and not row(change)
    assert not m._user_connection_row(user_id=cms['owner'], key='CMS_WORDPRESS:' + cms['project_id'])
    assert not writes(cms)


def test_simultaneous_approvals_send_one_write(cms, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    connect(cms)
    change = preview(cms)
    entered, release = Event(), Event()
    original = wp.exchange
    def delayed(*args, **kwargs):
        if args[3] == 'POST':
            entered.set()
            assert release.wait(5)
        return original(*args, **kwargs)
    monkeypatch.setattr(wp, 'exchange', delayed)
    cms['client'].get(cms['base'])
    token = cms['client'].cookies.get(m._CSRF_COOKIE_NAME)
    target = cms['base'] + f'/wordpress/changes/{change}/apply'
    def submit():
        return cms['client'].post(target, data={'_csrf': token}, follow_redirects=False)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(submit)
        try:
            assert entered.wait(5)
            second = pool.submit(submit).result(timeout=3)
            assert second.status_code == 303 and 'err=' in second.headers['location']
        finally:
            release.set()
        assert first.result(timeout=3).status_code == 303
    assert len(writes(cms)) == 1 and row(change).state == 'applied'


@pytest.mark.parametrize('api_base', ['https://atelier.example', 'https://atelier.example/wordpress'])
def test_core_subdirectory_keeps_credentials_on_api_base_and_resolves_public_permalink(monkeypatch, api_base):
    requested = []
    page = {'post_id': 5, 'post_status': 'publish', 'post_type': 'page', 'permalink': URL,
            'seo_title': None, 'meta_description': None}
    monkeypatch.setattr(wp, 'public_address', lambda host: '93.184.216.34')
    class Connection:
        def __init__(self, host, address):
            assert host == 'atelier.example' and address == '93.184.216.34'
        def request(self, method, path, **kwargs):
            assert method == 'GET' and kwargs['headers']['Authorization'].startswith('Basic ')
            requested.append(path)
        def getresponse(self):
            return SimpleNamespace(status=200, read=lambda size: json.dumps([page]).encode(), getheader=lambda *args: 'application/json')
        def close(self):
            pass
    monkeypatch.setattr(wp, '_PinnedHTTPS', Connection)
    client = wp.WordPressClient({'site_url': api_base, 'username': 'editor', 'password': 'application-password'})
    assert client.read(URL)['post_id'] == 5
    assert len(requested) == 1
    expected_prefix = urlsplit(api_base).path + '/wp-json/wp-abilities/v1/abilities/' + wp.READ + '/run?'
    assert requested[0].startswith(expected_prefix)
    assert parse_qs(urlsplit(requested[0]).query) == {'input[permalink]': [URL]}


def test_tracked_corrections_link_to_the_editor_and_details_render_platform_guidance(cms):
    connect(cms)
    page = cms['client'].get(f'/projects/{cms["slug"]}/corrections')
    assert page.status_code == 200 and 'Préparer dans WordPress' in page.text
    editor = cms['client'].get(cms['base'] + '/wordpress/edit?task_id=' + cms['task'].id)
    assert editor.status_code == 200 and 'Voir l’avant/après' in editor.text
    detail = cms['client'].get(f'/projects/{cms["slug"]}/issues/missing_title')
    assert detail.status_code == 200 and 'Dans WordPress avec Yoast SEO' in detail.text
    assert not writes(cms)
