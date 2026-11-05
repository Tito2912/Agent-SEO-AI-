"""Simulated Gmail/AI/mail transport; real storage, policy and Telegram decisions."""

import base64
import copy
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
from bs4 import BeautifulSoup
from sqlalchemy import select, func
from requests.structures import CaseInsensitiveDict

from backend import (
    app as m,
    own_seo as s,
    own_support as support,
    own_support_common as c,
)
from backend import own_support_gmail as g
from backend import own_support_message as msg, telegram_approvals as bot_service
from backend.models import UsageEvent, UserConnection
from test_customer_activation import lab as lab, client, user
from test_telegram_approvals import bot as bot

NOW = datetime.now(timezone.utc).replace(microsecond=0)
MAILBOX = "noyaru-support-test@gmail.com"
CLIENT = "123456-supporttest.apps.googleusercontent.com"
CALLBACK = "https://noyaru.com" + g.CALLBACK


class Response:
    def __init__(self, value=None, status=200, headers=None):
        self.value = value
        self.status_code = status
        self.headers = CaseInsensitiveDict(headers or {})

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def iter_content(self, size):
        yield json.dumps(self.value).encode() if self.value is not None else b""


class Remote:
    def __init__(self):
        self.calls = []
        self.ai = []
        self.sends = []
        self.mailbox = MAILBOX
        self.label = "Noyaru-SAV"
        self.labels_id = "Label_SAV"
        self.messages = {}
        self.history = []
        self.current = 100
        self.page_size = 10
        self.scope = c.SCOPE
        self.failed = None
        self.token_expiry = 3600
        self.ai_result = None
        self.hook = None

    def incoming(
        self,
        ident="a1",
        thread="f1",
        text="Bonjour, comment lancer une analyse et comprendre les résultats dans Noyaru ?",
        headers=None,
        kind="text/plain",
        parts=None,
        at=None,
    ):
        self.current += 1
        payload = {
            "mimeType": kind,
            "filename": "",
            "headers": [
                {"name": k, "value": v}
                for k, v in {
                    "From": "Customer <customer@example.invalid>",
                    "To": c.CONTACT,
                    "Subject": "Question analyse SEO",
                    "Message-ID": "<question-" + ident + "@example.invalid>",
                    **(headers or {}),
                }.items()
            ],
            "body": {
                "size": len(text.encode()),
                "data": base64.urlsafe_b64encode(text.encode()).decode().rstrip("="),
            },
        }
        if parts is not None:
            payload["parts"] = parts
        self.messages[ident] = {
            "id": ident,
            "threadId": thread,
            "labelIds": [self.labels_id, "INBOX"],
            "internalDate": str(
                int(
                    (at or NOW + timedelta(seconds=self.current - 100)).timestamp()
                    * 1000
                )
            ),
            "sizeEstimate": 2000,
            "payload": payload,
        }
        self.history.append(
            {
                "cursor": self.current,
                "messagesAdded": [{"message": {"id": ident, "threadId": thread}}],
            }
        )

    def request(self, method, url, **kw):
        assert kw["stream"] is True and kw["allow_redirects"] is False
        params = kw.get("params") or {}
        body = kw.get("json") or {}
        self.calls.append((method, url, copy.deepcopy(params)))
        if self.hook:
            self.hook(method, url, params, body)
        if self.failed == url:
            raise TimeoutError("provider response with a secret must not be logged")
        if url == g.TOKEN:
            return Response(
                {
                    "access_token": "simulated-access",
                    "refresh_token": "simulated-refresh",
                    "token_type": "Bearer",
                    "scope": self.scope,
                    "expires_in": self.token_expiry,
                }
            )
        assert (kw.get("headers") or {}).get(
            "Authorization"
        ) == "Bearer simulated-access" or not url.startswith(g.API)
        if url == g.API + "/profile":
            return Response(
                {"emailAddress": self.mailbox, "historyId": str(self.current)}
            )
        if url == g.API + "/labels":
            return Response(
                {
                    "labels": [
                        {"id": self.labels_id, "name": self.label, "type": "user"},
                        {"id": "INBOX", "name": "INBOX", "type": "system"},
                    ]
                }
            )
        if url == g.API + "/history":
            assert params["labelId"] == self.labels_id and set(
                params["historyTypes"]
            ) == {"messageAdded", "labelAdded"}
            after = int(params["startHistoryId"])
            values = [v for v in self.history if v["cursor"] > after]
            offset = int(params.get("pageToken", 0))
            page = values[offset : offset + self.page_size]
            result = {
                "history": [
                    {k: v for k, v in h.items() if k != "cursor"} for h in page
                ],
                "historyId": str(self.current),
            }
            if offset + len(page) < len(values):
                result["nextPageToken"] = str(offset + len(page))
            return Response(result)
        if url.startswith(g.API + "/messages/"):
            assert method == "GET" and "/attachments/" not in url
            ident = url.split("/")[-1]
            if ident not in self.messages:
                return Response({}, 404)
            value = copy.deepcopy(self.messages[ident])
            if params.get("fields") == g.META_FIELDS:

                def strip(part):
                    (part.get("body") or {}).pop("data", None)
                    for sub in part.get("parts", []):
                        strip(sub)

                strip(value["payload"])
            else:
                assert params.get("fields") == g.BODY_FIELDS
            return Response(value)
        if url in {
            "https://api.brevo.com/v3/smtp/email",
            "https://api.resend.com/emails",
            "https://api.sendgrid.com/v3/mail/send",
        }:
            self.sends.append((url, copy.deepcopy(body), copy.deepcopy(kw["headers"])))
            if "brevo" in url:
                return Response({"messageId": "<simulated-receipt@brevo.invalid>"}, 201)
            if "sendgrid" in url:
                return Response(
                    None, 202, {"X-Message-Id": "simulated-sendgrid-receipt"}
                )
            return Response({"id": "aabbccdd-0000-4000-8000-112233445566"})
        raise AssertionError((method, url, params))

    def ai_post(self, url, **kw):
        assert url == "https://api.anthropic.com/v1/messages"
        self.ai.append(kw["json"])
        output = self.ai_result or {
            "needs_human": False,
            "body": "Bonjour, ouvrez votre projet dans Noyaru puis lancez une analyse. Consultez ensuite les anomalies relevées pour choisir la prochaine action, en tenant compte des limites du rapport.",
            "sources": ["lancer-un-crawl"],
        }
        return SimpleNamespace(
            status_code=200,
            text="",
            json=lambda: {"content": [{"type": "text", "text": json.dumps(output)}]},
        )


@pytest.fixture()
def sav(lab, monkeypatch):
    x = lab
    owner = x["admin"]
    remote = Remote()
    monkeypatch.setattr(s, "clock", lambda: NOW)
    for key, value in {
        "SEO_AGENT_ENCRYPTION_KEY": "support-fixture-encryption-" + "e" * 50,
        "SEO_AGENT_ENCRYPTION_KEYS": "",
        "ANTHROPIC_API_KEY": "simulated-ai",
        "OPENAI_API_KEY": "",
        "SMTP_HOST": "",
        "MAIL_API_PROVIDER": "resend",
        "MAIL_API_KEY": "simulated-sender",
        "MAIL_FROM": c.CONTACT,
        "PUBLIC_BASE_URL": "https://noyaru.com",
        "SEO_AGENT_DISABLE_SCHEDULERS": "true",
        "SEO_AGENT_DISABLE_TELEGRAM_RECEIVER": "true",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(m, "_rate_limit_retry_after", lambda **kw: None)
    monkeypatch.setattr(c.requests, "request", remote.request)
    monkeypatch.setattr(m.requests, "post", remote.ai_post)
    g.configure(
        owner.id, MAILBOX, "Noyaru-SAV", CLIENT, "simulated-client-secret", True
    )
    url, nonce = g.start(owner.id, CALLBACK)
    state = parse_qs(urlsplit(url).query)["state"][0]
    g.finish(owner.id, "once-code", state, nonce, CALLBACK)
    g.activate(owner.id, True)
    return {**x, "owner": owner, "remote": remote}


def tickets(x):
    with x["db"].session() as db:
        return list(db.scalars(select(c.Ticket).order_by(c.Ticket.created_at)))


def ticket(x, ident):
    with x["db"].session() as db:
        return db.get(c.Ticket, ident)


def cfg(x):
    with x["db"].session() as db:
        return db.get(c.Channel, c.KEY)


def receive(x, **kw):
    x["remote"].incoming(**kw)
    g.poll(x["owner"].id)
    return tickets(x)[-1].id


def decide(x, ident, kind, approve=True):
    with x["db"].session() as db:
        c.s.locked(db)
        doc = support.approval_document(db, x["owner"].id, ident, kind, "fr")
        result = support.decide(db, x["owner"].id, ident, kind, approve, doc["version"])
        db.commit()
        return result


def draft(x):
    ident = receive(x)
    decide(x, ident, "support_write")
    support.run(ident)
    assert ticket(x, ident).state == "draft"
    return ident


def test_activation_starts_now_and_empty_get_is_read_only_and_no_client_authority(sav):
    x = sav
    assert cfg(x).cursor == "100" and cfg(x).active
    before = len(x["remote"].calls)
    response = client(x["owner"]).get(c.PATH)
    assert response.status_code == 200 and "SAV email de Noyaru" in response.text
    assert (
        len(x["remote"].calls) == before
        and not x["remote"].ai
        and not x["remote"].sends
    )
    assert client(user(x["db"])).get(c.PATH).status_code == 403
    assert client(user(x["db"], admin=True)).get(c.PATH).status_code == 403
    assert client(x["owner"]).post(c.PATH + "/channel/poll").status_code == 403
    with x["db"].session() as db:
        assert db.scalar(select(func.count()).select_from(UserConnection)) == 0


def test_receive_draft_two_approvals_single_send_with_acceptance_not_delivery(sav):
    x = sav
    ident = receive(x)
    assert (
        ticket(x, ident).state == "received"
        and not x["remote"].ai
        and not x["remote"].sends
    )
    with pytest.raises(ValueError):
        decide(x, ident, "support_send")
    decide(x, ident, "support_write")
    support.run(ident)
    row = ticket(x, ident)
    assert row.state == "draft" and row.provider_calls == 1 and len(x["remote"].ai) == 1
    assert "customer@example.invalid" not in json.dumps(x["remote"].ai)
    assert "Bonjour" not in json.dumps(row.dossier)
    support.run(ident)
    assert not x["remote"].sends
    decide(x, ident, "support_send")
    support.run(ident)
    row = ticket(x, ident)
    assert (
        row.state == "accepted"
        and row.receipt["delivered"] is False
        and row.receipt["id"]
    )
    assert (
        len(x["remote"].sends) == 1
        and c.reveal(row.outbox_encrypted)["to"] == "customer@example.invalid"
    )
    for _ in range(2):
        support.run(ident)
    assert len(x["remote"].sends) == 1 and len(x["remote"].ai) == 1
    with x["db"].session() as db:
        usage = list(db.scalars(select(UsageEvent)))
        assert usage and all(
            e.user_id == x["owner"].id and e.metric == "ia_cout_microdollars"
            for e in usage
        )


def test_duplicate_message_and_cursor_pages_never_skip_or_duplicate_imports(sav):
    x = sav
    x["remote"].page_size = 2
    for n in range(4):
        x["remote"].incoming(ident="a" + str(n), thread="f" + str(n))
    g.poll(x["owner"].id)
    assert len(tickets(x)) == 2 and cfg(x).cursor == "100"
    g.poll(x["owner"].id)
    assert len(tickets(x)) == 4 and cfg(x).cursor == "104"
    g.poll(x["owner"].id)
    assert len(tickets(x)) == 4
    x["remote"].current += 1
    x["remote"].history.append(
        {
            "cursor": 105,
            "labelsAdded": [
                {"message": {"id": "a0", "threadId": "f0"}, "labelIds": ["Label_SAV"]}
            ],
        }
    )
    g.poll(x["owner"].id)
    assert len(tickets(x)) == 4


@pytest.mark.parametrize(
    "change", ["private", "old", "self", "automatic", "replyto", "duplicate_from"]
)
def test_ineligible_headers_never_trigger_a_body_fetch_or_ai(sav, change):
    x = sav
    headers = {}
    at = None
    if change == "private":
        headers = {"To": "personal@example.invalid"}
    if change == "old":
        at = NOW - timedelta(days=1)
    if change == "self":
        headers = {"From": MAILBOX}
    if change == "automatic":
        headers = {"Auto-Submitted": "auto-replied"}
    if change == "replyto":
        headers = {"Reply-To": "other@example.invalid"}
    x["remote"].incoming(headers=headers, at=at)
    if change == "duplicate_from":
        x["remote"].messages["a1"]["payload"]["headers"].append(
            {"name": "From", "value": "other@example.invalid"}
        )
    g.poll(x["owner"].id)
    assert (
        tickets(x)[0].state == "ignored"
        and c.reveal(tickets(x)[0].payload_encrypted) == {}
    )
    assert not [
        p
        for _, url, p in x["remote"].calls
        if url.endswith("/messages/a1") and p.get("fields") == g.BODY_FIELDS
    ]
    assert not x["remote"].ai and not x["remote"].sends


def test_attachments_are_detected_without_fetching_attachment_data(sav):
    x = sav
    part = {
        "mimeType": "image/png",
        "filename": "private.png",
        "body": {"size": 50, "data": "PRIVATE_ATTACHMENT"},
    }
    ident = receive(x, kind="multipart/mixed", parts=[part])
    row = ticket(x, ident)
    assert row.state == "review_required" and row.reason == "attachments_require_human"
    assert not [
        p
        for _, url, p in x["remote"].calls
        if url.endswith("/messages/a1") and p.get("fields") == g.BODY_FIELDS
    ]
    assert "PRIVATE_ATTACHMENT" not in row.payload_encrypted
    decide(x, ident, "support_review")
    assert ticket(x, ident).state == "human" and not x["remote"].ai


def test_sensitive_question_is_not_automatically_sent_to_ai(sav):
    x = sav
    ident = receive(
        x,
        text="Bonjour, je demande un remboursement de ma dernière facture et la suppression de mon compte.",
    )
    assert ticket(x, ident).reason == "sensitive_request"
    with pytest.raises(ValueError):
        decide(x, ident, "support_write")
    assert not x["remote"].ai and not x["remote"].sends


def test_reply_rejection_keeps_draft_for_explicit_revision_and_old_version_stays_refused(
    sav,
):
    x = sav
    ident = draft(x)
    before = ticket(x, ident).draft_hash
    decide(x, ident, "support_send", False)
    assert ticket(x, ident).state == "draft_refused" and not x["remote"].sends
    text = "Bonjour, ouvrez votre projet puis consultez les réglages avant de lancer votre analyse. La documentation ci-dessous vous aide à préparer cette étape."
    support.manual_draft(x["owner"].id, ident, text, ["lancer-un-crawl"], True)
    assert (
        ticket(x, ident).state == "draft"
        and ticket(x, ident).draft_hash != before
        and len(x["remote"].ai) == 1
    )
    decide(x, ident, "support_send")
    support.run(ident)
    assert len(x["remote"].sends) == 1


@pytest.mark.parametrize(
    "change", ["pause", "sender", "source", "new_thread", "expiry"]
)
def test_changes_after_send_approval_prevent_any_email(sav, monkeypatch, change):
    x = sav
    ident = draft(x)
    decide(x, ident, "support_send")
    if change == "pause":
        g.pause(x["owner"].id)
    if change == "sender":
        monkeypatch.setenv("MAIL_API_KEY", "changed-sender")
    if change == "source":
        original = m.content_library.get_doc
        monkeypatch.setattr(
            m.content_library,
            "get_doc",
            lambda slug, lang="fr": {
                **original(slug, lang),
                "description": "New release description",
            },
        )
    if change == "new_thread":
        x["remote"].incoming(
            ident="a2",
            thread="f1",
            text="Autre question, merci de tenir compte de cette précision avant votre réponse.",
        )
    if change == "expiry":
        with x["db"].session() as db:
            db.get(c.Ticket, ident).authorized_until = NOW - timedelta(seconds=1)
            db.commit()
    support.run(ident)
    assert not x["remote"].sends


def test_uncertain_send_never_retries_or_falls_back_and_creates_a_human_decision(sav):
    x = sav
    ident = draft(x)
    decide(x, ident, "support_send")
    x["remote"].failed = "https://api.resend.com/emails"
    support.run(ident)
    assert (
        ticket(x, ident).state == "send_uncertain" and ticket(x, ident).send_intent_at
    )
    x["remote"].failed = None
    support.run(ident)
    support.run(ident)
    assert (
        len(
            [
                v
                for v in x["remote"].calls
                if v[0] == "POST" and v[1] == "https://api.resend.com/emails"
            ]
        )
        == 1
    )
    decide(x, ident, "support_review")
    assert (
        ticket(x, ident).state == "send_uncertain"
        and ticket(x, ident).receipt["review_ack"]
    )
    with pytest.raises(ValueError):
        decide(x, ident, "support_send")


@pytest.mark.parametrize("provider", ["brevo", "resend", "sendgrid"])
def test_fixed_sender_recipient_receipt_and_no_cc_bcc_for_each_transport(
    sav, monkeypatch, provider
):
    x = sav
    monkeypatch.setenv("MAIL_API_PROVIDER", provider)
    ident = draft(x)
    decide(x, ident, "support_send")
    support.run(ident)
    assert ticket(x, ident).state == "accepted" and len(x["remote"].sends) == 1
    url, body, headers = x["remote"].sends[0]
    raw = json.dumps(body)
    assert (
        c.CONTACT in raw
        and "customer@example.invalid" in raw
        and "cc" not in body
        and "bcc" not in body
    )
    assert ticket(x, ident).receipt["provider"] == provider
    if provider == "resend":
        assert headers["Idempotency-Key"].startswith("noyaru-support/")


def test_google_grant_is_separate_pkce_one_use_and_scope_mailbox_verified(sav):
    x = sav
    g.pause(x["owner"].id)
    url, nonce = g.start(x["owner"].id, CALLBACK)
    args = parse_qs(urlsplit(url).query)
    state = args["state"][0]
    assert (
        args["scope"] == [c.SCOPE]
        and args["code_challenge_method"] == ["S256"]
        and args["include_granted_scopes"] == ["false"]
    )
    with pytest.raises(ValueError):
        g.finish(x["owner"].id, "code", state, "wrong-cookie", CALLBACK)
    before = len(x["remote"].calls)
    g.finish(x["owner"].id, "code", state, nonce, CALLBACK)
    with pytest.raises(ValueError):
        g.finish(x["owner"].id, "code", state, nonce, CALLBACK)
    assert len(x["remote"].calls) == before + 2


def test_pii_is_encrypted_not_in_dossier_or_private_download_credentials(sav):
    x = sav
    ident = draft(x)
    row = ticket(x, ident)
    assert (
        "customer@example.invalid" not in row.payload_encrypted
        and "Bonjour" not in row.draft_encrypted
    )
    result = client(x["owner"]).get(c.PATH + "/tickets/" + ident + "/download")
    assert result.status_code == 200 and "customer@example.invalid" in result.text
    for secret in (
        "simulated-refresh",
        "simulated-client-secret",
        "simulated-sender",
        "simulated-access",
    ):
        assert secret not in result.text
    assert (
        client(user(x["db"]))
        .get(c.PATH + "/tickets/" + ident + "/download")
        .status_code
        == 403
    )


def test_sav_telegram_pages_are_encrypted_and_retention_removes_their_copy(sav, bot):
    x = sav
    ident = receive(x)
    bot_service.activate(x["owner"].id, "fr", True)
    bot_service.sync()
    with x["db"].session() as db:
        request = db.scalar(
            select(bot_service.Approval).where(
                bot_service.Approval.kind == "support_write",
                bot_service.Approval.target_id == ident,
            )
        )
        rid = request.id
        assert all(p.startswith(m._SECRET_PREFIX) for p in request.pages)
        assert "customer@example.invalid" not in json.dumps(request.pages)
        assert "customer@example.invalid" in "\n".join(bot_service.pages_for(request))
        cfg = db.get(c.Channel, c.KEY)
        cfg.active = False
        db.get(c.Ticket, ident).created_at = NOW - timedelta(days=31)
        db.commit()
    support.tick()
    with x["db"].session() as db:
        assert db.get(c.Ticket, ident) is None
        request = db.get(bot_service.Approval, rid)
        assert request.pages == [] and request.state == "expired"
        assert "customer@example.invalid" not in "\n".join(
            bot_service.pages_for(request)
        )


def test_reduction_removes_quoted_history_links_and_common_secrets():
    raw = "Bonjour voici ma question technique.\npassword: very-private\nBearer abcdefghijklmnop\nme@example.invalid https://customer.example/private?token=x\n> ancient private message\nOn yesterday wrote:\nAnother private thread"
    text = msg.reduce(raw)
    for value in (
        "very-private",
        "abcdefghijklmnop",
        "me@example.invalid",
        "https://",
        "ancient",
        "Another private",
    ):
        assert value not in text


@pytest.mark.parametrize("phase", ["write", "send"])
@pytest.mark.parametrize("approve", [True, False])
def test_real_telegram_callback_dispatches_only_confirmed_support_action(
    sav, bot, monkeypatch, phase, approve
):
    from test_telegram_approvals import decision, rows, query, row as botrow

    x = sav
    ident = draft(x) if phase == "send" else receive(x)
    bot_service.activate(x["owner"].id, "en", True)
    bot_service.sync()
    card = next(
        r for r in rows(x) if r.kind == "support_" + phase and r.target_id == ident
    )
    bot_service.deliver_one()
    dispatched = []
    monkeypatch.setattr(
        bot_service, "run_result", lambda kind, rid: dispatched.append((kind, rid))
    )
    decision(x, card.id, approve)
    for _ in range(100):
        if dispatched or not approve:
            break
        import time

        time.sleep(0.01)
    assert (
        ticket(x, ident).state
        == ("send_queued" if phase == "send" else "writing_queued")
        if approve
        else ticket(x, ident).state == ("draft_refused" if phase == "send" else "human")
    )
    assert dispatched == ([("support_" + phase, ident)] if approve else [])
    r = botrow(x, card.id)
    bot_service.handle_callback(query(r, "c", r.confirmation))
    assert len(dispatched) == int(approve) and not x["remote"].sends


@pytest.mark.parametrize("change", ["scope", "mailbox", "expired", "binding"])
def test_oauth_mismatch_never_replaces_existing_credentials_or_enables_reader(
    sav, change
):
    x = sav
    g.pause(x["owner"].id)
    before = c.reveal(cfg(x).gmail_credentials)["refresh_token"]
    url, nonce = g.start(x["owner"].id, CALLBACK)
    state = parse_qs(urlsplit(url).query)["state"][0]
    if change == "scope":
        x["remote"].scope += " https://www.googleapis.com/auth/gmail.send"
    if change == "mailbox":
        x["remote"].mailbox = "personal@example.invalid"
    if change in {"expired", "binding"}:
        with x["db"].session() as db:
            conf = db.get(c.Channel, c.KEY)
            if change == "expired":
                conf.oauth_until = NOW - timedelta(seconds=1)
            else:
                conf.binding = "b" * 32
            db.commit()
    with pytest.raises(ValueError):
        g.finish(x["owner"].id, "code", state, nonce, CALLBACK)
    assert (
        not cfg(x).active
        and c.reveal(cfg(x).gmail_credentials)["refresh_token"] == before
    )


@pytest.mark.parametrize("change", ["label", "mailbox", "history404"])
def test_reader_stops_instead_of_rebinding_or_importing_old_mail(sav, change):
    x = sav
    x["remote"].incoming()
    if change == "label":
        x["remote"].label = "Unrelated"
    if change == "mailbox":
        x["remote"].mailbox = "wrong@gmail.com"
    if change == "history404":
        original = x["remote"].request
        x["remote"].request = lambda method, url, **kw: (
            Response({}, 404)
            if url == g.API + "/history"
            else original(method, url, **kw)
        )
        # The installed bound method must also be updated.
        c.requests.request = x["remote"].request
    g.poll(x["owner"].id)
    assert not cfg(x).active and not tickets(x) and not x["remote"].ai


@pytest.mark.parametrize(
    "change", ["unknown_source", "html", "link", "extra", "human", "provider_timeout"]
)
def test_unsupported_ai_output_and_unknown_provider_result_never_send_or_retry(
    sav, monkeypatch, change
):
    x = sav
    ident = receive(x)
    decide(x, ident, "support_write")
    result = {
        "needs_human": False,
        "body": "Bonjour, ouvrez votre projet et consultez les réglages de votre analyse avant de démarrer. La documentation publique vous aide à comprendre cette étape.",
        "sources": ["lancer-un-crawl"],
    }
    if change == "unknown_source":
        result["sources"] = ["private-account-data"]
    if change == "html":
        result["body"] += "<script>alert(1)</script>"
    if change == "link":
        result["body"] += " https://outside.example/action"
    if change == "extra":
        result["send_now"] = True
    if change == "human":
        result = {"needs_human": True, "reason": "account"}
    if change == "provider_timeout":

        def timeout(*a, **kw):
            x["remote"].ai.append(kw["json"])
            raise TimeoutError("private data")

        monkeypatch.setattr(m.requests, "post", timeout)
        monkeypatch.setenv("OPENAI_API_KEY", "must-not-fallback")
    x["remote"].ai_result = result
    support.run(ident)
    support.run(ident)
    row = ticket(x, ident)
    assert (
        row.state == "review_required"
        and row.provider_calls == 1
        and len(x["remote"].ai) == 1
    )
    assert (
        not row.draft_encrypted
        and not x["remote"].sends
        and "private data" not in (row.error_code or "")
    )


@pytest.mark.parametrize("phase", ["writing", "sending"])
def test_live_lease_and_crashed_attempt_never_duplicate_external_effect(sav, phase):
    x = sav
    ident = draft(x) if phase == "sending" else receive(x)
    decide(x, ident, "support_send" if phase == "sending" else "support_write")
    support.claim(ident)
    with pytest.raises(ValueError, match="busy"):
        support.claim(ident)
    with x["db"].session() as db:
        row = db.get(c.Ticket, ident)
        row.lease_until = NOW - timedelta(seconds=1)
        if phase == "sending":
            row.send_intent_at = NOW
        else:
            row.provider_calls = 1
            row.provider_started_at = NOW
        db.commit()
    support.run(ident)
    assert ticket(x, ident).state == (
        "send_uncertain" if phase == "sending" else "review_required"
    )
    assert not x["remote"].sends and len(x["remote"].ai) == int(phase == "sending")


def test_daily_limits_include_approved_work_waiting_to_run(sav, monkeypatch):
    x = sav
    monkeypatch.setattr(c, "DAILY_AI", 1)
    first = receive(x)
    decide(x, first, "support_write")
    second = receive(x, ident="a2", thread="f2")
    with pytest.raises(ValueError):
        decide(x, second, "support_write")
    assert not x["remote"].ai
    support.run(first)
    monkeypatch.setattr(c, "DAILY_SENDS", 1)
    decide(x, first, "support_send")
    body = "Bonjour, ouvrez votre projet puis consultez les réglages de votre analyse. La documentation ci-dessous vous aide à préparer cette étape."
    support.manual_draft(x["owner"].id, second, body, ["lancer-un-crawl"], True)
    with pytest.raises(ValueError):
        decide(x, second, "support_send")
    assert not x["remote"].sends


def test_expired_private_copies_are_unavailable_before_cleanup_and_secrets_never_render(
    sav,
):
    x = sav
    ident = receive(x)
    g.pause(x["owner"].id)
    response = client(x["owner"]).get(c.PATH + "?lang=en")
    soup = BeautifulSoup(response.text, "html.parser")
    assert all(not inp.get("value") for inp in soup.select("input[type=password]"))
    for secret in (
        "simulated-refresh",
        "simulated-access",
        "simulated-client-secret",
        "simulated-sender",
    ):
        assert secret not in response.text
    with x["db"].session() as db:
        db.get(c.Ticket, ident).created_at = NOW - timedelta(days=31)
        db.commit()
    assert (
        client(x["owner"]).get(c.PATH + "/tickets/" + ident + "/download").status_code
        == 404
    )
    assert "customer@example.invalid" not in client(x["owner"]).get(c.PATH).text


def test_stale_binding_cannot_starve_new_support_cards(sav, bot):
    from backend import telegram_approval_adapters as adapters

    x = sav
    for index in range(20):
        x["remote"].incoming(ident=hex(0xA0 + index)[2:], thread=hex(0xF0 + index)[2:])
    for _ in range(4):
        g.poll(x["owner"].id)
    with x["db"].session() as db:
        for row in db.scalars(select(c.Ticket)):
            row.binding = "b" * 32
            row.created_at = NOW - timedelta(days=1, seconds=1)
        db.commit()
    ident = receive(x, ident="c1", thread="d1")
    with x["db"].session() as db:
        candidates = [
            v
            for v in adapters.candidates(db, x["owner"].id, "fr")
            if v[0].startswith("support_")
        ]
    assert candidates == [("support_write", ident)]


def test_recipient_local_part_is_preserved_and_self_addresses_ignore_case(sav):
    x = sav
    ident = receive(
        x, headers={"From": "Customer.Name@Example.Invalid", "To": "CONTACT@NOYARU.COM"}
    )
    row = ticket(x, ident)
    assert (
        row.state == "received"
        and c.reveal(row.payload_encrypted)["from"] == "Customer.Name@example.invalid"
    )
    body = "Bonjour, ouvrez votre projet puis consultez les réglages de votre analyse. La documentation ci-dessous vous aide à préparer cette étape."
    support.manual_draft(x["owner"].id, ident, body, ["lancer-un-crawl"], True)
    decide(x, ident, "support_send")
    support.run(ident)
    assert (
        c.reveal(ticket(x, ident).outbox_encrypted)["to"]
        == "Customer.Name@example.invalid"
    )
    other = receive(x, ident="b1", thread="c1", headers={"From": "CONTACT@NOYARU.COM"})
    assert ticket(x, other).state == "ignored"
