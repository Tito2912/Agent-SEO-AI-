"""Real transactions, simulated Google only: explicit recovery and expired grants."""

from datetime import timedelta
from urllib.parse import parse_qs, urlsplit

import pytest
from bs4 import BeautifulSoup
from sqlalchemy import func, select

from backend import own_support_gmail as g, own_support_recovery as recovery
from test_customer_activation import lab as lab, user, client
from test_telegram_approvals import bot as bot
from test_own_support import (
    sav as sav,
    c,
    cfg,
    ticket,
    tickets,
    NOW,
    Response,
    CALLBACK,
    m,
)


def ignored(x):
    x["remote"].incoming()
    x["remote"].messages["a1"]["labelIds"].append("SPAM")
    g.poll(x["owner"].id)
    row = tickets(x)[0]
    assert row.state == "ignored" and row.reason == "outside_label"
    assert c.reveal(row.payload_encrypted) == {}
    x["remote"].calls.clear()
    return row.id


def body_reads(x):
    return [
        p
        for _, url, p in x["remote"].calls
        if "/messages/" in url and p.get("fields") == g.BODY_FIELDS
    ]


def test_explicit_recheck_keeps_one_ticket_retention_and_waits_for_new_approval(sav):
    x = sav
    ident = ignored(x)
    before, cursor = ticket(x, ident), cfg(x).cursor
    x["remote"].messages["a1"]["labelIds"].remove("SPAM")
    recovery.recheck(x["owner"].id, ident, True)
    row = ticket(x, ident)
    assert row.id == ident and len(tickets(x)) == 1 and row.state == "received"
    assert row.thread_id == "f1" and row.revision == before.revision + 1
    assert row.created_at == before.created_at and cfg(x).cursor == cursor
    assert row.provider_calls == 0 and not row.authorized_until
    assert row.payload_encrypted.startswith(c.module()._SECRET_PREFIX)
    assert "comment lancer" in c.reveal(row.payload_encrypted)["text"]
    assert len(body_reads(x)) == 1 and not x["remote"].ai and not x["remote"].sends
    assert all(
        method == "GET" and "/history" not in url
        for method, url, _ in x["remote"].calls
    )
    with x["db"].session() as db:
        assert g.intake_reserved(db, x["owner"].id) == 2
        audit = db.scalar(
            select(c.AuditLog).where(c.AuditLog.action == "own_support.recheck")
        )
        assert audit.target_id == ident and audit.meta == {"state": "received"}
    calls = len(x["remote"].calls)
    with pytest.raises(ValueError, match="recheck_unavailable"):
        recovery.recheck(x["owner"].id, ident, True)
    assert len(x["remote"].calls) == calls


@pytest.mark.parametrize("label", ["SPAM", "TRASH", "SENT", "DRAFT", "CHAT", "missing"])
def test_recheck_still_rejects_excluded_labels_before_body_read(sav, label):
    x = sav
    ident = ignored(x)
    x["remote"].messages["a1"]["labelIds"] = (
        [label] if label == "missing" else ["Label_SAV", label]
    )
    with pytest.raises(ValueError, match="outside_label"):
        recovery.recheck(x["owner"].id, ident, True)
    assert ticket(x, ident).state == "ignored" and not body_reads(x)
    assert not x["remote"].ai and not x["remote"].sends and cfg(x).active


@pytest.mark.parametrize("case", ["recipient", "reply_to", "automatic", "self", "old"])
def test_recheck_preserves_envelope_window_and_loop_controls(sav, case):
    x = sav
    ident = ignored(x)
    value = x["remote"].messages["a1"]
    value["labelIds"].remove("SPAM")
    changes = {
        "recipient": ("To", "private@example.invalid"),
        "reply_to": ("Reply-To", "other@example.invalid"),
        "automatic": ("Auto-Submitted", "auto-replied"),
        "self": ("From", c.CONTACT),
    }
    if case == "old":
        value["internalDate"] = str(int((NOW - timedelta(days=1)).timestamp() * 1000))
    else:
        key, content = changes[case]
        value["payload"]["headers"] = [
            h for h in value["payload"]["headers"] if h["name"] != key
        ]
        value["payload"]["headers"].append({"name": key, "value": content})
    with pytest.raises(ValueError):
        recovery.recheck(x["owner"].id, ident, True)
    assert ticket(x, ident).state == "ignored" and not body_reads(x)
    assert cfg(x).active


@pytest.mark.parametrize(
    "state",
    [
        "received",
        "draft",
        "send_queued",
        "sending",
        "accepted",
        "send_uncertain",
        "human",
        "other_ignore",
    ],
)
def test_recheck_never_reopens_prepared_sent_or_manually_ignored_work(sav, state):
    x = sav
    ident = ignored(x)
    with x["db"].session() as db:
        row = db.get(c.Ticket, ident)
        row.state = "ignored" if state == "other_ignore" else state
        if state == "other_ignore":
            row.reason = "loop_blocked"
        db.commit()
    with pytest.raises(ValueError, match="recheck_unavailable"):
        recovery.recheck(x["owner"].id, ident, True)
    assert not x["remote"].calls


@pytest.mark.parametrize(
    "case",
    [
        "no_consent",
        "other_owner",
        "binding",
        "expired",
        "paused",
        "provider_attempt",
        "payload",
    ],
)
def test_recheck_requires_owner_binding_retention_consent_and_unread_state(sav, case):
    x = sav
    ident = ignored(x)
    owner = x["owner"].id
    with x["db"].session() as db:
        row = db.get(c.Ticket, ident)
        if case == "binding":
            row.binding = "b" * 32
        if case == "expired":
            row.created_at = NOW - timedelta(days=31)
        if case == "provider_attempt":
            row.provider_calls = 1
        if case == "payload":
            row.payload_encrypted = c.seal({"text": "already read"})
        db.commit()
    if case == "other_owner":
        owner = user(x["db"], admin=True).id
    if case == "paused":
        g.pause(owner)
    with pytest.raises(ValueError):
        recovery.recheck(owner, ident, case != "no_consent")
    assert not x["remote"].calls


def test_missing_message_does_not_expire_the_history_cursor_or_pause_other_mail(sav):
    x = sav
    ident = ignored(x)
    cursor = cfg(x).cursor
    del x["remote"].messages["a1"]
    with pytest.raises(ValueError, match="message_missing"):
        recovery.recheck(x["owner"].id, ident, True)
    assert cfg(x).cursor == cursor and cfg(x).active and not cfg(x).error_code
    assert ticket(x, ident).state == "ignored" and not body_reads(x)


@pytest.mark.parametrize("case", ["pause", "revision", "double_click"])
def test_recheck_revalidates_after_network_and_serializes_concurrent_actions(sav, case):
    x = sav
    ident = ignored(x)
    x["remote"].messages["a1"]["labelIds"].remove("SPAM")

    def change(method, url, params, body):
        if params.get("fields") != g.BODY_FIELDS:
            return
        if case == "pause":
            g.pause(x["owner"].id)
        elif case == "revision":
            with x["db"].session() as db:
                db.get(c.Ticket, ident).revision += 1
                db.commit()
        else:
            with pytest.raises(ValueError, match="busy"):
                recovery.recheck(x["owner"].id, ident, True)

    x["remote"].hook = change
    if case == "double_click":
        recovery.recheck(x["owner"].id, ident, True)
        assert ticket(x, ident).state == "received"
    else:
        with pytest.raises(ValueError):
            recovery.recheck(x["owner"].id, ident, True)
        assert c.reveal(ticket(x, ident).payload_encrypted) == {}
    assert not x["remote"].ai and not x["remote"].sends


@pytest.mark.parametrize("case", ["sensitive", "attachment"])
def test_recovered_requests_still_escalate_when_needed(sav, case):
    x = sav
    ident = ignored(x)
    if case == "sensitive":
        x["remote"].incoming(
            text="Bonjour, je souhaite un remboursement de ma facture."
        )
    else:
        x["remote"].messages["a1"]["labelIds"].remove("SPAM")
        x["remote"].messages["a1"]["payload"]["filename"] = "private.txt"
    recovery.recheck(x["owner"].id, ident, True)
    row = ticket(x, ident)
    assert row.state == "review_required"
    assert row.reason == (
        "sensitive_request" if case == "sensitive" else "attachments_require_human"
    )
    if case == "attachment":
        assert not body_reads(x)
    assert not x["remote"].ai and not x["remote"].sends


def test_recheck_and_normal_intake_share_daily_budget(sav, monkeypatch):
    x = sav
    ident = ignored(x)
    monkeypatch.setattr(c, "DAILY_IMPORT", 2)
    x["remote"].messages["a1"]["labelIds"].remove("SPAM")
    recovery.recheck(x["owner"].id, ident, True)
    x["remote"].incoming(ident="b1", thread="c1")
    g.poll(x["owner"].id)
    assert len(tickets(x)) == 1 and cfg(x).error_code == "import_limit"
    assert not [p for _, u, p in x["remote"].calls if u.endswith("/messages/b1")]
    with x["db"].session() as db:
        assert (
            db.scalar(
                select(func.count())
                .select_from(c.AuditLog)
                .where(c.AuditLog.action == "own_support.recheck")
            )
            == 1
        )


@pytest.mark.parametrize(
    "error,status,expected,paused",
    [
        ("invalid_grant", 400, "gmail_reconnect_required", True),
        ("invalid_client", 401, "oauth_client_rejected", True),
        ("unauthorized_client", 400, "oauth_client_rejected", True),
        ("temporarily_unavailable", 400, "provider_refused", False),
        ("server_error", 503, "provider_refused", False),
    ],
)
def test_expired_grant_pauses_once_preserves_cursor_and_hides_provider_details(
    sav, monkeypatch, error, status, expected, paused
):
    x = sav
    before = cfg(x)
    with x["db"].session() as db:
        row = db.get(c.Channel, c.KEY)
        credentials = c.reveal(row.gmail_credentials)
        credentials["expires_at"] = (NOW - timedelta(seconds=1)).isoformat()
        row.gmail_credentials = c.seal(credentials)
        db.commit()
    original = x["remote"].request

    def response(method, url, **kwargs):
        if url == g.TOKEN:
            x["remote"].calls.append((method, url, {}))
            return Response(
                {"error": error, "error_description": "private-token-details"}, status
            )
        return original(method, url, **kwargs)

    monkeypatch.setattr(c.requests, "request", response)
    g.poll(x["owner"].id)
    row = cfg(x)
    assert row.error_code == expected and row.active is not paused
    assert row.cursor == before.cursor and row.activated_at == before.activated_at
    assert row.binding == before.binding and row.pending == before.pending
    assert "private-token-details" not in row.error_code
    assert not x["remote"].ai and not x["remote"].sends
    if paused:
        calls = len(x["remote"].calls)
        g.poll(x["owner"].id)
        assert len(x["remote"].calls) == calls


def test_rotated_refresh_token_is_kept_without_recreating_the_binding(sav):
    x = sav
    before = cfg(x)
    with x["db"].session() as db:
        row = db.get(c.Channel, c.KEY)
        value = c.reveal(row.gmail_credentials)
        value.update(
            expires_at=(NOW - timedelta(seconds=1)).isoformat(),
            refresh_token="previous-simulated-refresh",
        )
        row.gmail_credentials = c.seal(value)
        db.commit()
    g.poll(x["owner"].id)
    row = cfg(x)
    assert row.active and row.binding == before.binding and row.cursor == before.cursor
    assert c.reveal(row.gmail_credentials)["refresh_token"] == "simulated-refresh"


def test_connection_pause_notifies_verified_owner_once_without_mail_or_token_data(
    sav, bot, monkeypatch
):
    from backend import telegram, telegram_approvals

    x = sav
    telegram_approvals.activate(x["owner"].id, "fr", True)
    notices = []
    monkeypatch.setattr(telegram, "notifier", notices.append)
    with x["db"].session() as db:
        row = db.get(c.Channel, c.KEY)
        value = c.reveal(row.gmail_credentials)
        value["expires_at"] = (NOW - timedelta(seconds=1)).isoformat()
        row.gmail_credentials = c.seal(value)
        db.commit()
    original = x["remote"].request

    def expired(method, url, **kwargs):
        if url == g.TOKEN:
            return Response(
                {"error": "invalid_grant", "error_description": "secret-detail"}, 400
            )
        return original(method, url, **kwargs)

    monkeypatch.setattr(c.requests, "request", expired)
    g.poll(x["owner"].id)
    g.poll(x["owner"].id)
    assert len(notices) == 1 and "réception SAV est en pause" in notices[0]
    assert 'href="https://noyaru.com/settings/system/support"' in notices[0]
    assert all(
        value not in notices[0]
        for value in ("secret-detail", "simulated-refresh", cfg(x).mailbox)
    )
    with x["db"].session() as db:
        assert (
            db.scalar(
                select(func.count())
                .select_from(c.AuditLog)
                .where(c.AuditLog.action == "own_support.connection_paused")
            )
            == 1
        )
    assert not x["remote"].ai and not x["remote"].sends


def test_reauthorization_then_resume_keeps_known_requests_and_history(sav):
    x = sav
    ident = ignored(x)
    before = cfg(x)
    g.pause(x["owner"].id)
    with x["db"].session() as db:
        db.get(c.Channel, c.KEY).error_code = "gmail_reconnect_required"
        db.commit()
    url, nonce = g.start(x["owner"].id, CALLBACK)
    state = parse_qs(urlsplit(url).query)["state"][0]
    g.finish(x["owner"].id, "new-simulated-code", state, nonce, CALLBACK)
    assert not cfg(x).active and cfg(x).cursor == before.cursor
    g.activate(x["owner"].id, True)
    assert cfg(x).active and cfg(x).binding == before.binding
    assert cfg(x).activated_at == before.activated_at and len(tickets(x)) == 1
    assert ticket(x, ident).state == "ignored" and not cfg(x).error_code
    assert not x["remote"].ai and not x["remote"].sends


@pytest.mark.parametrize("lang", ["fr", "en"])
def test_http_recovery_form_requires_csrf_and_consent_then_waits_for_telegram(
    sav, lang
):
    x = sav
    ident = ignored(x)
    http = client(x["owner"])
    response = http.get(c.PATH + "?lang=" + lang)
    assert response.status_code == 200 and not x["remote"].calls
    assert response.headers["cache-control"] == "no-store"
    action = c.PATH + "/tickets/" + ident + "/recheck"
    soup = BeautifulSoup(response.text, "html.parser")
    form = soup.find("form", action=action)
    assert form and form.find("input", {"name": "confirm"}).has_attr("required")
    token = form.find("input", {"name": "_csrf"})["value"]
    assert http.get(action).status_code == 405
    assert http.post(action, data={"confirm": "true"}).status_code == 403
    refused = http.post(action, data={"_csrf": token}, follow_redirects=False)
    assert (
        refused.status_code == 303 and "confirm_required" in refused.headers["location"]
    )
    assert not x["remote"].calls
    x["remote"].messages["a1"]["labelIds"].remove("SPAM")
    accepted = http.post(
        action, data={"_csrf": token, "confirm": "true"}, follow_redirects=False
    )
    assert accepted.status_code == 303 and ticket(x, ident).state == "received"
    assert not x["remote"].ai and not x["remote"].sends
    assert all(
        value not in response.text
        for value in (
            "simulated-refresh",
            "simulated-access",
            "simulated-client-secret",
        )
    )


def test_http_recovery_rejects_other_admin_before_any_gmail_read(sav):
    x = sav
    ident = ignored(x)
    other = client(user(x["db"], admin=True))
    other.get("/auth/login")
    token = other.cookies.get(m._CSRF_COOKIE_NAME)
    assert token
    result = other.post(
        c.PATH + "/tickets/" + ident + "/recheck",
        data={"_csrf": token, "confirm": "true"},
        follow_redirects=False,
    )
    assert result.status_code == 403 and not x["remote"].calls


@pytest.mark.parametrize("lang", ["fr", "en"])
def test_http_expired_connection_opens_reconnect_without_false_ready_button(sav, lang):
    x = sav
    g.pause(x["owner"].id)
    with x["db"].session() as db:
        db.get(c.Channel, c.KEY).error_code = "gmail_reconnect_required"
        db.commit()
    x["remote"].calls.clear()
    response = client(x["owner"]).get(c.PATH + "?lang=" + lang)
    soup = BeautifulSoup(response.text, "html.parser")
    form = soup.find("form", action=c.PATH + "/oauth/start")
    assert form and form.find_parent("details").has_attr("open")
    assert not soup.find("form", action=c.PATH + "/channel/activate")
    assert soup.select_one('[role="alert"]') and "7" in soup.get_text()
    assert not x["remote"].calls
