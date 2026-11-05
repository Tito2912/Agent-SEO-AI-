"""Simulated Telegram transport, real database transactions, never real messages or AI."""

from datetime import timedelta
import copy
from types import SimpleNamespace
import uuid

import pytest
from bs4 import BeautifulSoup
from sqlalchemy import func, select
from backend import telegram_approvals as a, own_writing as w, app as m
from backend.models import OwnSeoBrief, OwnSeoDraft, SeoKnowledgeSource, UsageEvent
from test_own_writing import writing as writing, authorize as authorize_draft
from test_own_seo import own as own, change_report
from test_customer_activation import lab as lab, client, user

REAL_API = a.api


@pytest.fixture()
def bot(lab, monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123456:" + ("x" * 35))
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "101")
    monkeypatch.setattr(m, "_rate_limit_retry_after", lambda **kw: None)
    calls = []
    messages = {}
    updates = []

    def api(method, body=None):
        body = copy.deepcopy(body or {})
        calls.append((method, body))
        if method == "getMe":
            return {"id": 202, "is_bot": True}
        if method == "getChat":
            return {"id": 101, "type": "private"}
        if method == "getWebhookInfo":
            return {"url": ""}
        if method == "sendMessage":
            ident = len(messages) + 1
            messages[ident] = body
            return {"message_id": ident}
        if method == "editMessageText":
            messages[body["message_id"]] = body
            return True
        if method == "getUpdates":
            return updates[:]
        if method == "answerCallbackQuery":
            return True
        raise AssertionError(method)

    monkeypatch.setattr(a, "api", api)
    return {
        **lab,
        "calls": calls,
        "messages": messages,
        "updates": updates,
        "api": api,
        "owner": lab["admin"],
    }


def activate(x):
    a.activate(x["owner"].id, "fr", True)


def rows(x):
    with x["db"].session() as db:
        return list(
            db.scalars(
                select(a.Approval).order_by(a.Approval.created_at, a.Approval.id)
            )
        )


def row(x, ident):
    with x["db"].session() as db:
        return db.get(a.Approval, ident)


def query(row, verb, nonce=None):
    return {
        "id": uuid.uuid4().hex,
        "from": {"id": 101, "is_bot": False},
        "data": a.callback_data(row, verb, nonce),
        "message": {
            "message_id": int(row.message_id),
            "from": {"id": 202, "is_bot": True},
            "chat": {"id": 101, "type": "private"},
        },
    }


def click(x, ident, verb):
    a.handle_callback(query(row(x, ident), verb))


def decision(x, ident, yes=True):
    r = row(x, ident)
    for page in range(len(r.pages)):
        click(x, ident, "p" + str(page))
    click(x, ident, "y" if yes else "n")
    r = row(x, ident)
    assert r.confirmation
    a.handle_callback(query(r, "c", r.confirmation))


def test_inactive_get_has_no_remote_calls_and_other_admin_is_forbidden(bot):
    x = bot
    c = client(x["owner"])
    response = c.get(a.PATH)
    assert response.status_code == 200 and "Aucune demande" in response.text
    assert not x["calls"] and not rows(x)
    assert client(user(x["db"], admin=True)).get(a.PATH).status_code == 403
    assert client(user(x["db"])).get(a.PATH).status_code == 403
    assert client().get(a.PATH, follow_redirects=False).status_code in {303, 307}


def test_activation_requires_owner_and_confirmation_and_preserves_duplicate_binding(
    bot,
):
    x = bot
    with pytest.raises(ValueError, match="confirm_required"):
        a.activate(x["owner"].id, "fr", False)
    activate(x)
    with x["db"].session() as db:
        first = db.get(a.Channel, a.KEY).binding
    activate(x)
    with x["db"].session() as db:
        assert db.get(a.Channel, a.KEY).binding == first
    assert {call[0] for call in x["calls"]} == {"getMe", "getChat", "getWebhookInfo"}


@pytest.mark.parametrize("chat", ["-101", "0", "@channel", "101:bad"])
def test_group_or_ambiguous_chat_cannot_activate(bot, monkeypatch, chat):
    monkeypatch.setenv("TELEGRAM_CHAT_ID", chat)
    with pytest.raises(ValueError, match="private_required"):
        activate(bot)
    assert not bot["calls"]


def test_webhook_is_never_replaced(bot, monkeypatch):
    original = bot["api"]
    monkeypatch.setattr(
        a,
        "api",
        lambda method, body=None: (
            {"url": "https://other-service.example/"}
            if method == "getWebhookInfo"
            else original(method, body)
        ),
    )
    with pytest.raises(ValueError, match="webhook_exists"):
        activate(bot)
    assert not rows(bot)


@pytest.mark.parametrize("yes", [True, False])
def test_real_decision_is_persisted_only_after_confirmation_and_duplicate_is_harmless(
    bot, yes
):
    x = bot
    activate(x)
    ident = a.test_request(x["owner"].id)
    a.deliver_one()
    r = row(x, ident)
    click(x, ident, "y" if yes else "n")
    confirmed = row(x, ident)
    assert confirmed.state == "pending" and confirmed.confirmation
    q = query(confirmed, "c", confirmed.confirmation)
    a.handle_callback(q)
    a.handle_callback(q)
    assert row(x, ident).state == ("approved" if yes else "rejected")
    with x["db"].session() as db:
        assert db.scalar(select(func.count()).select_from(UsageEvent)) == 0
    assert not any(
        method
        not in {
            "getMe",
            "getChat",
            "getWebhookInfo",
            "sendMessage",
            "editMessageText",
            "answerCallbackQuery",
        }
        for method, _ in x["calls"]
    )
    assert (
        a.callback_data(r, "c", confirmed.confirmation).isascii()
        and len(a.callback_data(r, "c", confirmed.confirmation).encode()) <= 64
    )


@pytest.mark.parametrize(
    "change", ["sender", "group", "bot", "message", "nonce", "inline", "forward"]
)
def test_foreign_or_forged_callback_cannot_change_state(bot, change):
    x = bot
    activate(x)
    ident = a.test_request(x["owner"].id)
    a.deliver_one()
    q = query(row(x, ident), "y")
    if change == "sender":
        q["from"]["id"] = 999
    elif change == "group":
        q["message"]["chat"]["type"] = "group"
    elif change == "bot":
        q["message"]["from"]["id"] = 999
    elif change == "message":
        q["message"]["message_id"] = 999
    elif change == "nonce":
        q["data"] = q["data"][:-12] + "0" * 12
    elif change == "inline":
        q["inline_message_id"] = "arbitrary"
    elif change == "forward":
        q["message"]["forward_origin"] = {"type": "user"}
    a.handle_callback(q)
    assert row(x, ident).state == "pending" and row(x, ident).confirmation is None


@pytest.mark.parametrize("change", ["token", "chat", "owner", "pause", "expired"])
def test_existing_buttons_cannot_act_after_authority_or_expiry_changes(
    bot, monkeypatch, change
):
    x = bot
    activate(x)
    ident = a.test_request(x["owner"].id)
    a.deliver_one()
    click(x, ident, "y")
    r = row(x, ident)
    q = query(r, "c", r.confirmation)
    if change == "token":
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "987654:" + ("y" * 35))
    elif change == "chat":
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "102")
    elif change == "owner":
        monkeypatch.setenv("SYSTEM_SETTINGS_OWNER_EMAIL", "other@example.invalid")
    elif change == "pause":
        a.pause(x["owner"].id)
    elif change == "expired":
        with x["db"].session() as db:
            db.get(a.Approval, ident).expires_at = a.now() - timedelta(seconds=1)
            db.commit()
    try:
        a.handle_callback(q)
    except ValueError as exc:
        assert str(exc) in {"binding_changed", "owner_required", "inactive"}
    assert row(x, ident).state != "approved"


def test_all_pages_must_be_successfully_shown_before_approval(bot, monkeypatch):
    x = bot
    activate(x)
    ident = a.test_request(x["owner"].id)
    with x["db"].session() as db:
        db.get(a.Approval, ident).pages = a.split_pages("🧭" * 4000)
        db.commit()
    a.deliver_one()
    click(x, ident, "y")
    assert row(x, ident).confirmation is None
    original = bot["api"]

    def fail(method, body=None):
        if method == "editMessageText":
            raise ValueError("transport_uncertain")
        return original(method, body)

    monkeypatch.setattr(a, "api", fail)
    click(x, ident, "p1")
    assert row(x, ident).seen == 1
    monkeypatch.setattr(a, "api", original)
    decision(x, ident)
    assert row(x, ident).state == "approved"
    assert "".join(a.split_pages("🧭" * 4000)) == "🧭" * 4000


def test_uncertain_delivery_is_not_retried_without_owner_resend(bot, monkeypatch):
    x = bot
    activate(x)
    ident = a.test_request(x["owner"].id)
    original = x["api"]
    attempts = []

    def fail(method, body=None):
        if method == "sendMessage":
            attempts.append(body)
            raise ValueError("transport_uncertain")
        return original(method, body)

    monkeypatch.setattr(a, "api", fail)
    a.deliver_one()
    a.deliver_one()
    assert len(attempts) == 1 and row(x, ident).delivery == "uncertain"
    a.resend(x["owner"].id, ident)
    monkeypatch.setattr(a, "api", original)
    a.deliver_one()
    assert row(x, ident).delivery == "sent"


def test_receiver_lease_and_durable_cursor_handle_replay_once(bot):
    x = bot
    activate(x)
    ident = a.test_request(x["owner"].id)
    a.deliver_one()
    decision(x, ident, False)
    token = a.claim()
    assert token and a.claim() is None
    a.process_update(
        {
            "update_id": 999,
            "message": {"text": "arbitrary untrusted text", "chat": {"id": 909}},
        },
        token,
    )
    with x["db"].session() as db:
        assert db.get(a.Channel, a.KEY).offset == "1000"
    with pytest.raises(ValueError, match="lease_lost"):
        a.process_update({"update_id": 1000}, "wrong")
    assert row(x, ident).state == "rejected"


def test_transport_errors_never_contain_bot_token_or_remote_description(
    bot, monkeypatch
):
    # Use the real API wrapper with a fully simulated HTTP boundary.
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123456:" + ("x" * 35))
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "101")

    def fail(*args, **kw):
        raise requests_exception()

    def requests_exception():
        return RuntimeError("https://api.telegram.org/bot123456:SECRET/raw")

    monkeypatch.setattr(a.requests, "post", fail)
    with pytest.raises(ValueError, match="^transport_uncertain$"):
        REAL_API("getMe")
    monkeypatch.setattr(
        a.requests,
        "post",
        lambda *args, **kw: SimpleNamespace(
            status_code=403, json=lambda: {"ok": False, "description": "SECRET"}
        ),
    )
    with pytest.raises(ValueError, match="^chat_denied$"):
        REAL_API("getMe")


def test_native_csrf_and_conversation_finder_do_not_compete_with_receiver(
    bot, monkeypatch
):
    x = bot
    c = client(x["owner"])
    c.get(a.PATH)
    assert (
        c.post(
            a.PATH + "/activate", data={"confirm": "true"}, follow_redirects=False
        ).status_code
        == 403
    )
    response = c.post(
        a.PATH + "/activate",
        data={"confirm": "true", "_csrf": c.cookies.get(m._CSRF_COOKIE_NAME)},
        follow_redirects=False,
    )
    assert response.status_code == 303
    monkeypatch.setattr(
        a.telegram, "conversations_recentes", lambda: pytest.fail("competing poll")
    )
    response = c.post(
        "/settings/system/telegram/conversation",
        data={"_csrf": c.cookies.get(m._CSRF_COOKIE_NAME)},
        follow_redirects=False,
    )
    assert response.headers["location"] == a.PATH
    for lang in ("fr", "en"):
        page = BeautifulSoup(c.get(a.PATH + "?lang=" + lang).text, "html.parser")
        assert page.select_one('link[href^="/static/telegram-approvals.css"]')
        assert all(
            form.select_one('input[name="_csrf"]')
            for form in page.select('form[method="post"]')
        )


def test_source_exact_version_can_be_approved_or_rejected_without_inventing_guidance(
    bot, writing
):
    x = writing
    bot["owner"] = x["owner"]
    activate(bot)
    with x["db"].session() as db:
        source = db.get(SeoKnowledgeSource, "google_essentials")
        source.approved_hash = None
        db.commit()
    a.sync()
    r = next(
        r
        for r in rows(x)
        if r.kind == "knowledge" and r.target_id == "google_essentials"
    )
    a.deliver_one()
    decision(x, r.id)
    with x["db"].session() as db:
        source = db.get(SeoKnowledgeSource, "google_essentials")
        assert source.latest_hash == source.approved_hash


def test_brief_review_and_discard_call_existing_rules(bot, writing):
    x = writing
    bot["owner"] = x["owner"]
    activate(bot)
    with x["db"].session() as db:
        db.get(OwnSeoBrief, x["brief"]).status = "prepared"
        db.commit()
    a.sync()
    r = next(r for r in rows(x) if r.kind == "brief")
    a.deliver_one()
    decision(x, r.id)
    with x["db"].session() as db:
        assert db.get(OwnSeoBrief, x["brief"]).status == "reviewed"
    assert not x["calls"]


def test_writing_authorization_is_atomic_one_call_and_draft_review_never_publishes(
    bot, writing, monkeypatch
):
    x = writing
    bot["owner"] = x["owner"]
    activate(bot)
    a.sync()
    r = next(r for r in rows(x) if r.kind == "writing")
    a.deliver_one()
    monkeypatch.setattr(
        a.threading,
        "Thread",
        lambda target, args, **kw: SimpleNamespace(start=lambda: target(*args)),
    )
    decision(x, r.id)
    assert row(x, r.id).state == "approved" and len(x["calls"]) == 1
    draft_id = row(x, r.id).result_id
    with x["db"].session() as db:
        assert db.get(OwnSeoDraft, draft_id).status == "draft"
    a.sync()
    draft = next(r for r in rows(x) if r.kind == "draft")
    a.deliver_one()
    decision(x, draft.id)
    with x["db"].session() as db:
        assert db.get(OwnSeoDraft, draft_id).status == "reviewed"
    assert len(x["calls"]) == 1


@pytest.mark.parametrize("change", ["source", "report", "paused", "content"])
def test_changed_draft_or_evidence_never_approves(bot, writing, change):
    x = writing
    bot["owner"] = x["owner"]
    activate(bot)
    ident = authorize_draft(x)
    w.run(ident)
    a.sync()
    r = next(r for r in rows(x) if r.kind == "draft")
    a.deliver_one()
    for p in range(len(r.pages)):
        click(x, r.id, "p" + str(p))
    click(x, r.id, "y")
    r = row(x, r.id)
    q = query(r, "c", r.confirmation)
    with x["db"].session() as db:
        if change == "source":
            db.get(
                SeoKnowledgeSource, "google_essentials"
            ).guidance_fr = "Changed reference text that has not been reapproved."
        elif change == "paused":
            db.get(a.s.OwnSeoWorkspace, a.s.KEY).paused = True
        elif change == "content":
            db.get(OwnSeoDraft, ident).content_hash = "0" * 64
        db.commit()
    if change == "report":
        change_report(
            x,
            lambda d: d["meta"]["gsc_api"]["queries"]["top"][0].update(impressions=999),
        )
    a.handle_callback(q)
    assert row(x, r.id).state == "pending"
    with x["db"].session() as db:
        assert db.get(OwnSeoDraft, ident).status == "draft"


def test_refused_writing_is_not_recreated_and_never_calls_provider(bot, writing):
    x = writing
    bot["owner"] = x["owner"]
    activate(bot)
    a.sync()
    r = next(r for r in rows(x) if r.kind == "writing")
    a.deliver_one()
    decision(x, r.id, False)
    a.sync()
    assert len([r for r in rows(x) if r.kind == "writing"]) == 1 and not x["calls"]
    with x["db"].session() as db:
        assert db.scalar(select(func.count()).select_from(OwnSeoDraft)) == 0


def test_failed_business_action_rolls_back_both_decision_and_object(
    bot, writing, monkeypatch
):
    x = writing
    bot["owner"] = x["owner"]
    activate(bot)
    with x["db"].session() as db:
        db.get(OwnSeoBrief, x["brief"]).status = "prepared"
        db.commit()
    a.sync()
    r = next(r for r in rows(x) if r.kind == "brief")
    a.deliver_one()
    for page in range(len(r.pages)):
        click(x, r.id, "p" + str(page))
    click(x, r.id, "y")
    r = row(x, r.id)

    def broken(db, *args):
        db.get(OwnSeoBrief, x["brief"]).status = "reviewed"
        raise ValueError("changed")

    monkeypatch.setattr(a.adapters, "apply", broken)
    a.handle_callback(query(r, "c", r.confirmation))
    assert row(x, r.id).state == "pending"
    with x["db"].session() as db:
        assert db.get(OwnSeoBrief, x["brief"]).status == "prepared"
    assert not x["calls"]


def test_interrupted_send_becomes_uncertain_without_automatic_duplicate(bot):
    x = bot
    activate(x)
    ident = a.test_request(x["owner"].id)
    with x["db"].session() as db:
        r = db.get(a.Approval, ident)
        r.delivery = "sending"
        r.delivery_until = a.now() - timedelta(seconds=1)
        db.commit()
    a.sync()
    assert row(x, ident).delivery == "uncertain"
    assert not a.deliver_one()


def test_competing_receiver_pauses_without_overwriting_telegram_configuration(
    bot, monkeypatch
):
    x = bot
    activate(x)
    original = x["api"]

    def api(method, body=None):
        if method == "getUpdates":
            raise ValueError("receiver_conflict")
        return original(method, body)

    monkeypatch.setattr(a, "api", api)
    a.cycle()
    with x["db"].session() as db:
        channel = db.get(a.Channel, a.KEY)
        assert not channel.enabled and channel.error_code == "receiver_conflict"
    assert not any(
        method in {"setWebhook", "deleteWebhook"} for method, _ in x["calls"]
    )


def test_explicit_resend_invalidates_old_nonce_and_message(bot):
    x = bot
    activate(x)
    ident = a.test_request(x["owner"].id)
    a.deliver_one()
    old = row(x, ident)
    a.resend(x["owner"].id, ident)
    a.deliver_one()
    a.handle_callback(query(old, "y"))
    assert row(x, ident).confirmation is None
    decision(x, ident, False)
    assert row(x, ident).state == "rejected"


def test_binding_to_a_different_bot_resets_only_its_cursor(bot, monkeypatch):
    x = bot
    activate(x)
    with x["db"].session() as db:
        db.get(a.Channel, a.KEY).offset = "999999"
        db.commit()
    a.pause(x["owner"].id)
    original = x["api"]
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "654321:" + ("z" * 35))
    monkeypatch.setattr(
        a,
        "api",
        lambda method, body=None: (
            {"id": 303, "is_bot": True} if method == "getMe" else original(method, body)
        ),
    )
    activate(x)
    with x["db"].session() as db:
        assert db.get(a.Channel, a.KEY).offset == "0"


def test_writing_cannot_authorize_new_evidence_that_changes_between_read_and_apply(
    bot, writing, monkeypatch
):
    x = writing
    bot["owner"] = x["owner"]
    activate(bot)
    a.sync()
    r = next(r for r in rows(x) if r.kind == "writing")
    a.deliver_one()
    for page in range(len(r.pages)):
        click(x, r.id, "p" + str(page))
    click(x, r.id, "y")
    r = row(x, r.id)
    original = a.adapters.apply

    def changed(db, *args):
        change_report(
            x,
            lambda d: d["meta"]["gsc_api"]["queries"]["top"][0].update(impressions=777),
        )
        return original(db, *args)

    monkeypatch.setattr(a.adapters, "apply", changed)
    a.handle_callback(query(r, "c", r.confirmation))
    assert row(x, r.id).state == "pending" and not x["calls"]
    with x["db"].session() as db:
        assert db.scalar(select(func.count()).select_from(OwnSeoDraft)) == 0


def test_receiver_resumes_an_accepted_writing_request_after_a_process_restart(
    bot, writing, monkeypatch
):
    x = writing
    bot["owner"] = x["owner"]
    activate(bot)
    a.sync()
    r = next(r for r in rows(x) if r.kind == "writing")
    a.deliver_one()
    monkeypatch.setattr(
        a.threading, "Thread", lambda **kw: SimpleNamespace(start=lambda: None)
    )
    decision(x, r.id)
    assert row(x, r.id).state == "approved" and not x["calls"]
    monkeypatch.setattr(
        a.threading,
        "Thread",
        lambda target, args, **kw: SimpleNamespace(start=lambda: target(*args)),
    )
    a.resume_writing()
    a.resume_writing()
    assert len(x["calls"]) == 1
    with x["db"].session() as db:
        assert db.get(OwnSeoDraft, row(x, r.id).result_id).status == "draft"
