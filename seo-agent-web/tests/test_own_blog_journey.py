"""Two real decisions, simulated transports; never AI, Telegram or GitHub writes."""

from datetime import timedelta
from types import SimpleNamespace
import uuid

import pytest
from sqlalchemy import select
from bs4 import BeautifulSoup

from backend import own_blog as b, own_writing as w, telegram_approvals as a
from backend import telegram_approval_adapters as adapters
from backend.models import OwnSeoBrief, OwnSeoDraft
from test_own_writing import writing as writing
from test_own_seo import own as own
from test_customer_activation import lab as lab, client
from test_telegram_approvals import (
    bot as bot,
    activate,
    rows,
    row as approval,
    decision,
)
from test_own_blog import Remote, Response, served_article, row


@pytest.fixture()
def journey(writing, monkeypatch):
    x = writing
    with x["db"].session() as db:
        db.get(OwnSeoBrief, x["brief"]).status = "prepared"
        db.commit()
    remote = Remote()
    monkeypatch.setattr(
        b,
        "connection",
        lambda owner, **kw: ("local-github", b.s.digest(["own-blog", "local-github"])),
    )
    monkeypatch.setattr(b.requests, "request", remote.request)
    return {**x, "remote": remote}


def write(x):
    with x["db"].session() as db:
        b.s.locked(db)
        doc = adapters.inspect(db, x["owner"].id, "writing", x["brief"], "fr")
        ident = adapters.apply(
            db,
            x["owner"].id,
            "writing",
            x["brief"],
            True,
            uuid.uuid4().hex,
            doc["version"],
        )
        db.commit()
    w.run(ident)
    with x["db"].session() as db:
        return ident, db.scalar(
            select(b.Publication.id).where(b.Publication.fr_id == ident)
        )


def approve_article(x, ident, yes=True):
    with x["db"].session() as db:
        b.s.locked(db)
        doc = adapters.inspect(db, x["owner"].id, "blog_pr", ident, "fr")
        adapters.apply(
            db, x["owner"].id, "blog_pr", ident, yes, uuid.uuid4().hex, doc["version"]
        )
        db.commit()


def test_only_two_cards_reach_exact_publication(journey, bot, monkeypatch, tmp_path):
    x = journey
    bot["owner"] = x["owner"]
    activate(bot)
    monkeypatch.setattr(
        a.threading,
        "Thread",
        lambda target, args, **kw: SimpleNamespace(start=lambda: target(*args)),
    )
    a.sync()
    first = next(r for r in rows(x) if r.kind == "writing")
    assert not any(r.kind in {"brief", "draft", "blog_pr"} for r in rows(x))
    a.deliver_one()
    decision(x, first.id)
    draft_id = approval(x, first.id).result_id
    assert len(x["calls"]) == 1 and not x["remote"].writes
    with x["db"].session() as db:
        assert db.get(OwnSeoBrief, x["brief"]).status == "reviewed"
        assert db.get(OwnSeoDraft, draft_id).status == "draft"
        ident = db.scalar(
            select(b.Publication.id).where(b.Publication.fr_id == draft_id)
        )
    a.sync()
    final = next(r for r in rows(x) if r.kind == "blog_pr")
    assert len(rows(x)) == 2
    a.deliver_one()
    message = bot["messages"][int(approval(x, final.id).message_id)]
    assert "Décision 2/2" in BeautifulSoup(message["text"], "html.parser").get_text()
    assert any(
        "?publication=" + ident + "#publication-" + ident in button.get("url", "")
        for line in message["reply_markup"]["inline_keyboard"]
        for button in line
    )
    decision(x, final.id)
    assert row(x, ident).state == "reviewing"  # PR exists; no merge before CI.
    x["remote"].pending = True
    b.run(ident)
    assert not x["remote"].pr["merged"]
    x["remote"].pending = False
    b.run(ident)
    assert row(x, ident).state == "queued_publish"
    a.sync()
    assert len(rows(x)) == 2
    b.run(ident)
    result = row(x, ident)
    assert result.state == "observing"
    file = result.proposal["files"][0]
    public = served_article(file, result.receipt["merge"], monkeypatch, tmp_path)
    monkeypatch.setattr(
        b.requests,
        "get",
        lambda *args, **kw: Response(headers=dict(public.headers), raw=public.content),
    )
    b.run(ident)
    assert row(x, ident).state == "published" and len(x["calls"]) == 1


@pytest.mark.parametrize(
    "change",
    ["content", "source", "expiry", "connection", "base", "failed-ci"],
    ids=["text", "source", "expired", "access", "base", "ci"],
)
def test_changed_or_failed_operation_never_merges(journey, monkeypatch, change):
    x = journey
    draft, ident = write(x)
    approve_article(x, ident)
    b.run(ident)
    with x["db"].session() as db:
        if change == "content":
            db.get(OwnSeoDraft, draft).content_hash = "0" * 64
        elif change == "expiry":
            db.get(b.Publication, ident).expires_at = b.now() - timedelta(seconds=1)
        elif change == "source":
            db.get(OwnSeoBrief, x["brief"]).status = "discarded"
        db.commit()
    if change == "connection":
        monkeypatch.setattr(b, "connection", lambda owner, **kw: ("other", "other"))
    if change == "base":
        x["remote"].branches["main"] = x["remote"].commit(x["remote"].base, {})
    if change == "failed-ci":
        x["remote"].failure = True
    b.run(ident)
    b.run(ident)
    assert not x["remote"].pr["merged"]
    assert len(x["calls"]) == 1


def test_refusal_does_not_mark_reviewed_write_or_recreate(journey):
    x = journey
    draft, ident = write(x)
    approve_article(x, ident, False)
    b.run(ident)
    assert row(x, ident).state == "cancelled" and not x["remote"].writes
    with x["db"].session() as db:
        assert db.get(OwnSeoDraft, draft).status == "draft"
    w.prepare_article(draft)
    assert row(x, ident).state == "cancelled" and not x["remote"].writes


def test_duplicate_handoff_and_resume_never_repeat_ai_or_make_another_proposal(journey):
    x = journey
    draft, ident = write(x)
    w.prepare_article(draft)
    w.run(draft)
    w.prepare_article(draft)
    with x["db"].session() as db:
        assert list(db.scalars(select(b.Publication.id))) == [ident]
    assert len(x["calls"]) == 1 and not x["remote"].writes


def test_refresh_invalidates_the_exact_publication_mandate(journey):
    x = journey
    _, ident = write(x)
    approve_article(x, ident)
    b.run(ident)
    b.refresh(x["owner"].id, ident)
    assert row(x, ident).state == "proposal" and not row(x, ident).receipt.get(
        "article_consent"
    )
    for _ in range(3):
        b.run(ident)
    assert not x["remote"].pr["merged"]


def test_automatic_preparation_never_extends_expiry(journey):
    x = journey
    _, ident = write(x)
    with x["db"].session() as db:
        publication = db.get(b.Publication, ident)
        publication.state = "checking"
        publication.next_check_at = None
        publication.expires_at = b.now() - timedelta(seconds=1)
        expired = publication.expires_at
        db.commit()
    b.tick()
    result = row(x, ident)
    assert result.state == "needs_review" and result.error_code == "expired"
    assert w.utc(result.expires_at) == w.utc(expired) and not x["remote"].writes


def test_navigation_points_to_this_article_and_keeps_selection(journey):
    x = journey
    draft, ident = write(x)
    c = client(x["owner"])
    page = BeautifulSoup(c.get(w.PATH + "?brief=" + x["brief"]).text, "html.parser")
    assert page.select_one(
        'a[href="' + b.PATH + "?publication=" + ident + "#publication-" + ident + '"]'
    )
    assert not page.select('form[action$="/review"]')
    publication = BeautifulSoup(
        c.get(b.PATH + "?publication=" + ident).text, "html.parser"
    )
    assert publication.select_one(
        'a[href="' + w.PATH + "?brief=" + x["brief"] + "#draft-" + draft + '"]'
    )
    assert publication.select_one(
        'a[href="' + b.PATH + "?publication=" + ident + "#publication-" + ident + '"]'
    )
    assert (
        publication.select_one("details.own-proposal-choice").has_attr("open") is False
    )
    selected = BeautifulSoup(c.get(b.PATH + "?draft=" + draft).text, "html.parser")
    assert selected.select_one("#blog-fr option[selected]")["value"] == draft


@pytest.mark.parametrize(
    "state",
    ["approved", "rejected", "expired"],
    ids=["old-accepted", "old-refused", "old-expired"],
)
def test_new_flow_does_not_revive_a_legacy_writing_decision(journey, bot, state):
    x = journey
    bot["owner"] = x["owner"]
    activate(bot)
    with x["db"].session() as db:
        channel = a.active(db)
        fingerprint = w.bundle(db, x["owner"].id, x["brief"], allow_prepared=True)[1]
        doc = adapters.document(
            "writing",
            x["brief"],
            fingerprint,
            "Legacy writing",
            "Private draft only",
            "One private draft",
            w.PATH,
        )
        old = a.register(db, channel, doc)
        old.state = state
        db.commit()
    a.sync()
    assert len(rows(x)) == 1 and rows(x)[0].state == state
    assert not x["calls"] and not x["remote"].writes
