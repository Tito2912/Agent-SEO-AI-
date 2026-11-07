"""Readable owner decisions retain exact data, full page gating and private SAV storage."""

from datetime import datetime, timezone
from types import SimpleNamespace

from bs4 import BeautifulSoup
import pytest

from backend import (
    telegram_approval_format as f,
    telegram_approvals as a,
    own_support as support,
)
from test_customer_activation import lab as lab
from test_telegram_approvals import bot as bot, row, click, query
from test_own_support import sav as sav, receive, draft, c, ticket


def plain(payload):
    return BeautifulSoup(payload["text"], "html.parser").get_text()


@pytest.mark.parametrize("lang", ["fr", "en"])
@pytest.mark.parametrize(
    "kind",
    [
        "test",
        "knowledge",
        "brief",
        "writing",
        "draft",
        "blog_pr",
        "blog_publish",
        "support_write",
        "support_send",
        "support_review",
    ],
)
def test_action_cards_escape_data_keep_context_and_do_not_claim_completion(
    lang, kind, monkeypatch
):
    monkeypatch.setattr(a, "pages_for", lambda row: row.pages)
    channel = SimpleNamespace(lang=lang)
    row = SimpleNamespace(
        kind=kind,
        state="pending",
        id="a" * 32,
        nonce="b" * 12,
        confirmation="c" * 12,
        choice="yes",
        pages=["Client: <b>not markup</b> & 🧭"],
        seen=1,
        expires_at=datetime(2026, 11, 8, 10, tzinfo=timezone.utc),
    )
    doc = {
        "title": "Subject </b><script>bad()</script>",
        "effect": "One exact action: <b>data</b> & context.",
        "path": "https://foreign.example/action",
        "ready": True,
        "decision_summary": "To: Customer.Name@example.invalid",
    }
    payload = a.card(channel, row, doc=doc)
    assert payload["parse_mode"] == "HTML" and "<script>" not in payload["text"]
    assert "Client: <b>not markup</b> & 🧭" in plain(payload)
    assert f.units(plain(payload)) <= 4096
    buttons = [b for line in payload["reply_markup"]["inline_keyboard"] for b in line]
    assert any(b.get("callback_data") == a.callback_data(row, "y") for b in buttons)
    assert all(
        b["url"].startswith("https://noyaru.com/settings/system/")
        for b in buttons
        if "url" in b
    )
    confirmation = a.card(channel, row, doc=doc, confirm=True)
    assert doc["decision_summary"] in plain(confirmation) and doc["effect"] in plain(
        confirmation
    )
    assert any(
        b.get("callback_data") == a.callback_data(row, "c", row.confirmation)
        for line in confirmation["reply_markup"]["inline_keyboard"]
        for b in line
    )
    row.state = "approved"
    final = a.card(channel, row)
    assert ("Approval recorded" if lang == "en" else "Accord enregistré") in plain(
        final
    )
    assert all(
        "callback_data" not in b
        for line in final["reply_markup"]["inline_keyboard"]
        for b in line
    )
    assert (
        "email sent" not in plain(final).lower()
        and "email envoyé" not in plain(final).lower()
    )


@pytest.mark.parametrize(
    "text",
    ["🧭" * 4000, ("Un paragraphe avec des mots.\n\n" * 200), "x" * 4000, "<&>" * 1500],
)
def test_soft_pagination_preserves_all_characters_and_utf16_budget(text):
    pieces = a.split_pages(text)
    assert "".join(pieces) == text and all(f.units(p) <= 2800 for p in pieces)
    if text.startswith("Un paragraphe"):
        assert pieces[0].endswith("\n\n")


def test_sections_keep_source_texts_and_do_not_generate_raw_json():
    sources = [
        {
            "title": "Source A",
            "url": "https://noyaru.com/docs/a",
            "text": "First paragraph.\n\n" + "detail " * 250,
            "fingerprint": "a" * 64,
        },
        {
            "title": "Source B",
            "url": "https://noyaru.com/docs/b",
            "text": "Last fact.",
            "excerpt_limited": True,
        },
    ]
    doc = f.document(
        {"kind": "support_write"},
        [("💬 Message", "A concrete question.")] + f.source_sections(sources),
    )
    pieces = f.pages(doc)
    combined = "\n".join(pieces)
    assert sources[0]["text"] in combined and sources[1]["text"] in combined
    assert all(f.units(p) <= 2800 for p in pieces)
    assert '"fingerprint"' not in combined and '"sources"' not in combined
    assert "source complète" in combined
    with pytest.raises(ValueError, match="document_too_long"):
        f.pages(f.document({}, [("Source", "x" * 50000)]))


def test_editorial_evidence_uses_labels_and_draft_keeps_every_paragraph_and_reference():
    row = SimpleNamespace(
        created_at=datetime(2026, 11, 7, 10, tzinfo=timezone.utc),
        bundle={
            "sources": [
                {
                    "id": "official",
                    "title": "Official source",
                    "url": "https://example.invalid/source",
                    "text": "Full source evidence.",
                    "fingerprint": "a" * 64,
                }
            ]
        },
        content={
            "title": "A sourced title",
            "description": "A concrete description.",
            "intro": {"text": "Original introduction.", "sources": ["official"]},
            "sections": [
                {
                    "heading": "A useful section",
                    "paragraphs": [
                        {"text": "Complete paragraph text.", "sources": ["official"]}
                    ],
                }
            ],
        },
    )
    doc = f.document({}, f.draft_sections(row, en=True))
    text = "\n".join(f.pages(doc))
    assert "Original introduction." in text and "Complete paragraph text." in text
    assert row.created_at.isoformat() in text and "Historical snapshot" in text
    assert (
        "[1]" in text
        and "Source 1 · Official source" in text
        and "Full source evidence." in text
    )
    labels = f.evidence(
        {
            "source": "gsc_api",
            "kind": "guide",
            "lang": "fr",
            "impressions": 123,
            "query": "Exact query",
        }
    )
    assert (
        "Google Search Console" in labels
        and "Guide pratique" in labels
        and "Français" in labels
    )
    assert "123" in labels and "Exact query" in labels and '"query"' not in labels


@pytest.mark.parametrize("lang", ["fr", "en"])
def test_support_dossier_and_confirmation_show_exact_email_without_json(
    sav, bot, monkeypatch, lang
):
    x = sav
    ident = draft(x)
    with x["db"].session() as db:
        doc = support.approval_document(db, x["owner"].id, ident, "support_send", lang)
    manifest = support.mail.manifest(ticket(x, ident))
    assert manifest["text"] in doc["text"] and manifest["to"] in doc["decision_summary"]
    assert (
        manifest["from"] in doc["decision_summary"]
        and manifest["reply_to"] in doc["decision_summary"]
    )
    assert '"subject":' not in doc["text"] and "\\n" not in doc["text"]
    a.activate(x["owner"].id, lang, True)
    a.sync()
    with x["db"].session() as db:
        card = db.scalar(
            a.select(a.Approval).where(
                a.Approval.target_id == ident, a.Approval.kind == "support_send"
            )
        )
        rid = card.id
        assert all(p.startswith(support.c.module()._SECRET_PREFIX) for p in card.pages)
    a.deliver_one()
    record = row(x, rid)
    assert record.state == "pending" and not x["remote"].sends
    for page in range(len(record.pages)):
        click(x, rid, "p" + str(page))
    click(x, rid, "y")
    record = row(x, rid)
    assert record.confirmation and not x["remote"].sends
    shown = bot["messages"][int(record.message_id)]
    assert manifest["to"] in plain(shown) and manifest["from"] in plain(shown)
    dispatched = []
    monkeypatch.setattr(
        a, "run_result", lambda kind, rid: dispatched.append((kind, rid))
    )
    a.handle_callback(query(record, "c", record.confirmation))
    import time

    for _ in range(100):
        if dispatched:
            break
        time.sleep(0.01)
    assert row(x, rid).state == "approved" and ticket(x, ident).state == "send_queued"
    assert dispatched == [("support_send", ident)] and not x["remote"].sends
    final = bot["messages"][int(record.message_id)]
    assert any(
        b.get("url") == "https://noyaru.com" + doc["path"]
        for line in final["reply_markup"]["inline_keyboard"]
        for b in line
    )


def test_existing_cached_card_is_only_reformatted_by_explicit_resend(sav, bot):
    x = sav
    ident = receive(x)
    a.activate(x["owner"].id, "fr", True)
    a.sync()
    with x["db"].session() as db:
        card = db.scalar(a.select(a.Approval).where(a.Approval.target_id == ident))
        rid = card.id
        card.pages = [c.seal({"text": "LEGACY PRIVATE DOSSIER"})]
        db.commit()
    a.deliver_one()
    before = row(x, rid)
    a.sync()
    assert a.pages_for(row(x, rid)) == ["LEGACY PRIVATE DOSSIER"]
    a.resend(x["owner"].id, rid)
    after = row(x, rid)
    assert after.version == before.version and after.nonce != before.nonce
    assert after.seen == 0 and after.confirmation is None and after.message_id is None
    assert "LEGACY" not in "\n".join(a.pages_for(after))
    assert all(p.startswith(support.c.module()._SECRET_PREFIX) for p in after.pages)
    assert not x["remote"].ai and not x["remote"].sends


def test_approval_button_waits_for_all_pages_and_failed_edit_does_not_unlock_it(
    bot, monkeypatch
):
    x = bot
    a.activate(x["owner"].id, "fr", True)
    rid = a.test_request(x["owner"].id)
    with x["db"].session() as db:
        db.get(a.Approval, rid).pages = ["First page", "Second page"]
        db.commit()
    a.deliver_one()
    record = row(x, rid)
    shown = bot["messages"][int(record.message_id)]
    assert not any(
        b.get("callback_data") == a.callback_data(record, "y")
        for line in shown["reply_markup"]["inline_keyboard"]
        for b in line
    )
    original = bot["api"]

    def fail(method, body=None):
        if method == "editMessageText":
            raise ValueError("transport_uncertain")
        return original(method, body)

    monkeypatch.setattr(a, "api", fail)
    click(x, rid, "p1")
    click(x, rid, "y")
    assert row(x, rid).seen == 1 and row(x, rid).confirmation is None
    monkeypatch.setattr(a, "api", original)
    click(x, rid, "p1")
    record = row(x, rid)
    assert record.seen == 3
    shown = bot["messages"][int(record.message_id)]
    assert any(
        b.get("callback_data") == a.callback_data(record, "y")
        for line in shown["reply_markup"]["inline_keyboard"]
        for b in line
    )
