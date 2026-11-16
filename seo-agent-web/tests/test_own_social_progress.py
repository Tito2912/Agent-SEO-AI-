"""Progress reads existing milestones, never runs or retries a production."""

from datetime import timedelta
from types import SimpleNamespace

import pytest
from bs4 import BeautifulSoup

from backend import own_social_media as m
from test_customer_activation import lab as lab, client, user
from test_telegram_approvals import bot as bot
from test_own_social_media import media as media, prepare, approve, read


@pytest.mark.parametrize(
    "state,attempts,started,lease,expected",
    [
        ("proposal", 0, False, False, "proposal"),
        ("queued", 0, False, False, "queued"),
        ("queued", 1, True, False, "montage_queued"),
        ("running", 0, False, True, "starting"),
        ("running", 0, True, True, "voice"),
        ("running", 1, True, True, "montage"),
        ("running", 1, True, False, "unconfirmed"),
        ("review", 1, True, False, "review"),
        ("approved", 1, True, False, "approved"),
        ("stopped", 0, True, False, "stopped"),
        ("uncertain", 0, True, False, "uncertain"),
        ("cancelled", 1, True, False, "cancelled"),
    ],
    ids=[
        "approval",
        "queue",
        "saved-voice",
        "start",
        "voice",
        "montage",
        "lost-activity",
        "review",
        "ready",
        "stopped",
        "unknown-result",
        "removed",
    ],
)
def test_phase_follows_observed_milestones(state, attempts, started, lease, expected):
    now = m.s.clock()
    row = SimpleNamespace(
        state=state,
        expires_at=now + timedelta(hours=1),
        lease_until=now + timedelta(minutes=1) if lease else None,
        render_attempts=attempts,
        provider_started_at=now if started else None,
    )
    assert m.progress_phase(row) == expected


def test_expired_activity_never_looks_like_live_work():
    now = m.s.clock()
    row = SimpleNamespace(
        state="running",
        expires_at=now + timedelta(hours=1),
        lease_until=now - timedelta(seconds=1),
        render_attempts=0,
        provider_started_at=now,
    )
    assert m.progress_phase(row) == "unconfirmed"
    row.expires_at = now - timedelta(seconds=1)
    assert m.progress_phase(row) == "expired"
    row.state = "approved"
    assert m.progress_phase(row) == "approved"


def test_status_detects_voice_to_montage_without_a_state_change(media):
    x = media
    ident = prepare(x)
    approve(x, ident)
    c = client(x["owner"])
    url = m.PATH + "/" + ident + "/status"
    with x["db"].session() as db:
        row = db.get(m.Media, ident)
        row.state = "running"
        row.provider_started_at = m.s.clock()
        row.lease_until = m.s.clock() + timedelta(minutes=5)
        db.commit()
    first = c.get(url)
    assert first.json() == {"state": "running", "error": None, "phase": "voice"}
    assert first.headers["cache-control"] == "no-store"
    with x["db"].session() as db:
        row = db.get(m.Media, ident)
        row.render_attempts = 1
        row.result = {"audio": "fictional-private-audio", "transcript": "not in status"}
        db.commit()
    second = c.get(url)
    assert second.json() == {"state": "running", "error": None, "phase": "montage"}
    assert "fictional-private" not in second.text and "transcript" not in second.text
    assert not x["voice_calls"] and read(x, ident).provider_calls == 0
    for actor in (user(x["db"]), user(x["db"], admin=True)):
        assert client(actor).get(url).status_code == 403
    assert c.get(m.PATH + "/foreign/status").status_code == 404
    assert client().get(url, follow_redirects=False).status_code == 303


@pytest.mark.parametrize("lang", ["fr", "en"], ids=["fr", "en"])
def test_active_progress_precedes_settings_and_has_no_pointless_approval(media, lang):
    x = media
    ident = prepare(x)
    approve(x, ident)
    with x["db"].session() as db:
        row = db.get(m.Media, ident)
        row.state = "running"
        row.provider_started_at = m.s.clock()
        row.lease_until = m.s.clock() + timedelta(minutes=5)
        db.commit()
    page = client(x["owner"]).get(m.PATH + "?media=" + ident + "&lang=" + lang)
    soup = BeautifulSoup(page.text, "html.parser")
    progress = soup.select_one("#media-progress")
    assert progress["data-media-phase"] == "voice" and "is-active" in progress["class"]
    assert (
        page.text.index('id="media-progress"')
        < page.text.index('id="media-availability"')
        < page.text.index('id="media-limits"')
    )
    assert len(progress.select(".media-steps li")) == 4
    assert progress.select_one('[aria-current="step"]').text.strip().startswith("2")
    assert (
        progress.select_one("noscript")
        and progress.select_one("[data-media-monitor]")["hidden"] == ""
    )
    assert not soup.select(
        '.social-studio a[href="/settings/system/telegram/approvals"]'
    )
    assert soup.select_one('a[href="#media-progress"]')
    assert not x["voice_calls"]


def test_unconfirmed_activity_stops_animation_without_restarting_work(media):
    x = media
    ident = prepare(x)
    approve(x, ident)
    with x["db"].session() as db:
        row = db.get(m.Media, ident)
        row.state = "running"
        row.lease_until = m.s.clock() - timedelta(seconds=1)
        db.commit()
    c = client(x["owner"])
    page = c.get(m.PATH + "?media=" + ident)
    progress = BeautifulSoup(page.text, "html.parser").select_one("#media-progress")
    assert progress["data-media-phase"] == "unconfirmed"
    assert (
        "is-active" not in progress["class"] and "needs-attention" in progress["class"]
    )
    assert c.get(m.PATH + "/" + ident + "/status").json()["phase"] == "unconfirmed"
    assert read(x, ident).state == "running" and not x["voice_calls"]


def test_ready_video_has_a_direct_link_and_the_existing_review_decision(media):
    x = media
    ident = prepare(x)
    approve(x, ident)
    m.run(ident)
    page = client(x["owner"]).get(m.PATH + "?media=" + ident)
    soup = BeautifulSoup(page.text, "html.parser")
    assert soup.select_one('#media-progress a[href="#media-preview"]')
    assert soup.select_one("video#media-preview")
    assert (
        len(soup.select('.social-studio a[href="/settings/system/telegram/approvals"]'))
        == 1
    )
    assert len(x["voice_calls"]) == 1 and read(x, ident).state == "review"
