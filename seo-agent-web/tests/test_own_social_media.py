"""Real owner decisions and durable workers; no real provider or Telegram request."""

import base64
from datetime import timedelta
import io
import json
import threading
import uuid
import wave

import pytest
from sqlalchemy import func, select

from backend import own_social_media as m, app
from backend.models import UsageEvent
from test_customer_activation import lab as lab, client, user
from test_telegram_approvals import bot as bot, activate, decision, rows


REAL_NARRATION = m.narration


def audio():
    stream = io.BytesIO()
    with wave.open(stream, "wb") as output:
        output.setnchannels(1)
        output.setsampwidth(2)
        output.setframerate(24000)
        output.writeframes(b"\x00\x00" * 24000 * 24)
    return stream.getvalue()


def result():
    value = b"\x00\x00\x00\x18ftypmp42" + b"0" * 2000
    return {
        "video": base64.b64encode(value).decode(),
        "sha256": m.render.sha(value),
        "bytes": len(value),
        "duration": 24,
        "width": 720,
        "height": 1280,
        "captions": ["fixture"],
    }


@pytest.fixture()
def media(bot, monkeypatch):
    x = bot
    activate(x)
    monkeypatch.setenv("OPENAI_API_KEY", "fictional-voice-key")
    monkeypatch.setattr(m.render, "executable", lambda: "/fixture/ffmpeg")
    monkeypatch.setattr(m.approvals(), "run_result", lambda *args: None)
    calls = []
    monkeypatch.setattr(
        m, "narration", lambda packet, key: calls.append(packet) or audio()
    )
    monkeypatch.setattr(m.render, "render", lambda packet, value: result())
    return {**x, "voice_calls": calls}


def prepare(x, key=None, **kwargs):
    return m.prepare(
        x["owner"].id,
        "fr",
        kwargs.get("goal", "discovery"),
        "freelance",
        key or uuid.uuid4().hex,
        True,
    )


def read(x, ident):
    with x["db"].session() as db:
        return db.get(m.Media, ident)


def approve(x, ident, kind="social_render", yes=True):
    m.approvals().sync()
    m.approvals().deliver_one()
    card = next(
        r
        for r in rows(x)
        if r.target_id == ident and r.kind == kind and r.state == "pending"
    )
    decision(x, card.id, yes=yes)
    return card.id


def test_private_page_frames_status_and_video_are_owner_only(media):
    x = media
    ident = prepare(x)
    paths = [
        m.PATH,
        m.PATH + "/frame/0",
        m.PATH + "/" + ident + "/status",
        m.PATH + "/" + ident + "/video",
    ]
    for who in (user(x["db"]), user(x["db"], admin=True)):
        for path in paths:
            assert client(who).get(path).status_code == 403
    for path in paths:
        assert client().get(path, follow_redirects=False).status_code == 303
    c = client(x["owner"])
    assert c.get(m.PATH).status_code == 200
    assert c.get(m.PATH + "/frame/0").headers["content-type"] == "image/png"
    assert c.get(m.PATH + "/frame/3").status_code == 404
    assert c.get(m.PATH + "/foreign/status").status_code == 404
    assert not x["voice_calls"]


def test_preparation_is_idempotent_free_and_csrf_protected(media):
    x = media
    c = client(x["owner"])
    key = uuid.uuid4().hex
    assert (
        c.post(m.PATH + "/prepare", data={"key": key, "confirm": "true"}).status_code
        == 403
    )
    page = c.get(m.PATH)
    assert page.status_code == 200 and "storyboard" in page.text
    r = c.post(
        m.PATH + "/prepare",
        data={
            "_csrf": c.cookies.get(app._CSRF_COOKIE_NAME),
            "key": key,
            "confirm": "true",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303 and "err=&" in r.headers["location"]
    ident = prepare(x, key)
    assert prepare(x) == ident and not x["voice_calls"]
    with x["db"].session() as db:
        assert db.scalar(select(func.count()).select_from(m.Media)) == 1
        assert db.scalar(select(func.count()).select_from(UsageEvent)) == 0
    assert read(x, ident).state == "proposal"


def test_exact_two_decisions_produce_then_review_without_publication(media):
    x = media
    ident = prepare(x)
    approve(x, ident)
    assert read(x, ident).state == "queued"
    m.run(ident)
    m.run(ident)
    row = read(x, ident)
    assert (
        row.state == "review" and row.provider_calls == 1 and len(x["voice_calls"]) == 1
    )
    approve(x, ident, "social_review")
    assert read(x, ident).state == "approved" and len(x["voice_calls"]) == 1
    response = client(x["owner"]).get(m.PATH + "/" + ident + "/video")
    assert (
        response.status_code == 200
        and m.render.sha(response.content) == row.result["sha256"]
    )
    assert response.headers["cache-control"] == "no-store"
    assert client(x["owner"]).post(m.PATH + "/" + ident + "/publish").status_code in {
        403,
        404,
        405,
    }
    with x["db"].session() as db:
        assert db.scalar(select(func.count()).select_from(UsageEvent)) == 0


@pytest.mark.parametrize("stage", ["proposal", "review"])
def test_rejection_removes_asset_and_cannot_be_replayed(media, stage):
    x = media
    ident = prepare(x)
    if stage == "review":
        approve(x, ident)
        m.run(ident)
    count = len(x["voice_calls"])
    approve(x, ident, "social_review" if stage == "review" else "social_render", False)
    assert read(x, ident).state == "cancelled" and not read(x, ident).result
    m.run(ident)
    assert len(x["voice_calls"]) == count


@pytest.mark.parametrize(
    "change",
    ["key", "binding", "packet", "expiry", "capture"],
    ids=["key", "binding", "packet", "expiry", "capture"],
)
def test_changed_mandate_stops_before_a_provider_call(media, monkeypatch, change):
    x = media
    ident = prepare(x)
    approve(x, ident)
    if change == "key":
        monkeypatch.setenv("OPENAI_API_KEY", "changed-key")
    elif change == "capture":
        monkeypatch.setattr(m.render, "captures", lambda: "changed")
    else:
        with x["db"].session() as db:
            row = db.get(m.Media, ident)
            if change == "binding":
                row.binding = "changed"
            if change == "packet":
                row.packet = {**row.packet, "title": "changed"}
            if change == "expiry":
                row.expires_at = m.s.clock() - timedelta(seconds=1)
            db.commit()
    m.run(ident)
    assert read(x, ident).state == "stopped" and not x["voice_calls"]


def test_spoofed_queued_row_without_telegram_receipt_cannot_run(media):
    x = media
    ident = prepare(x)
    with x["db"].session() as db:
        db.get(m.Media, ident).state = "queued"
        db.commit()
    m.run(ident)
    assert read(x, ident).error_code == "mandate_required" and not x["voice_calls"]


def test_unknown_provider_outcome_never_retries_even_after_resume(media, monkeypatch):
    x = media
    ident = prepare(x)
    approve(x, ident)

    def timeout(*args):
        x["voice_calls"].append("timeout")
        raise TimeoutError("no response")

    monkeypatch.setattr(m, "narration", timeout)
    m.run(ident)
    m.run(ident)
    assert read(x, ident).state == "uncertain" and len(x["voice_calls"]) == 1


@pytest.mark.parametrize("saved", [False, True], ids=["missing-result", "saved-voice"])
def test_restart_uses_saved_voice_or_stops_uncertainty(media, saved):
    x = media
    ident = prepare(x)
    approve(x, ident)
    with x["db"].session() as db:
        row = db.get(m.Media, ident)
        row.state = "running"
        row.provider_calls = 1
        row.lease_token = "previous"
        row.lease_until = m.s.clock() - timedelta(seconds=1)
        if saved:
            row.result = {"audio": base64.b64encode(audio()).decode()}
        db.commit()
    m.run(ident)
    assert (
        read(x, ident).state == ("review" if saved else "uncertain")
        and not x["voice_calls"]
    )


def test_local_montage_retries_once_without_repeating_narration(media, monkeypatch):
    x = media
    ident = prepare(x)
    approve(x, ident)
    monkeypatch.setattr(
        m.render,
        "render",
        lambda *args: (_ for _ in ()).throw(ValueError("render_failed")),
    )
    m.run(ident)
    assert read(x, ident).state == "queued" and read(x, ident).render_attempts == 1
    m.run(ident)
    m.run(ident)
    assert (
        read(x, ident).state == "stopped"
        and read(x, ident).render_attempts == 2
        and len(x["voice_calls"]) == 1
    )


def test_concurrent_worker_does_not_duplicate_the_paid_call(media, monkeypatch):
    x = media
    ident = prepare(x)
    approve(x, ident)
    entered, release = threading.Event(), threading.Event()

    def voice(*args):
        x["voice_calls"].append("one")
        entered.set()
        assert release.wait(10)
        return audio()

    monkeypatch.setattr(m, "narration", voice)
    thread = threading.Thread(target=m.run, args=(ident,))
    thread.start()
    try:
        assert entered.wait(10)
        m.run(ident)
    finally:
        release.set()
        thread.join(timeout=15)
    assert len(x["voice_calls"]) == 1 and read(x, ident).state == "review"


def test_daily_cap_counts_uncertain_and_cancelled_provider_attempts(media):
    x = media
    for i in range(2):
        ident = prepare(x)
        approve(x, ident)
        with x["db"].session() as db:
            row = db.get(m.Media, ident)
            row.state = "cancelled"
            row.provider_calls = 1
            row.provider_started_at = m.s.clock()
            db.commit()
    ident = prepare(x)
    approve(x, ident)
    m.run(ident)
    assert read(x, ident).error_code == "budget" and not x["voice_calls"]


def test_tampered_video_cannot_be_approved_or_downloaded(media):
    x = media
    ident = prepare(x)
    approve(x, ident)
    m.run(ident)
    with x["db"].session() as db:
        row = db.get(m.Media, ident)
        row.result = {**row.result, "video": base64.b64encode(b"tampered").decode()}
        db.commit()
        with pytest.raises(ValueError, match="changed"):
            m.approval_document(db, x["owner"].id, ident, "social_review", "fr")
    assert client(x["owner"]).get(m.PATH + "/" + ident + "/video").status_code == 409


def test_cancel_during_provider_request_discards_late_result(media, monkeypatch):
    x = media
    ident = prepare(x)
    approve(x, ident)

    def voice(*args):
        with x["db"].session() as db:
            row = db.get(m.Media, ident)
            row.state = "cancelled"
            row.lease_token = None
            db.commit()
        return audio()

    monkeypatch.setattr(m, "narration", voice)
    m.run(ident)
    assert read(x, ident).state == "cancelled" and not read(x, ident).result


def test_provider_request_is_fixed_bounded_and_checks_exact_transcript(
    media, monkeypatch
):
    # Exercise the actual HTTP adapter, not a network service.
    narration = REAL_NARRATION
    with media["db"].session() as db:
        packet = m.packet(db, "fr", "discovery", "freelance")
    script = " ".join(s["text"] for s in packet["scenes"])
    seen = []

    class Response:
        status_code = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def iter_content(self, size):
            yield json.dumps(
                {
                    "choices": [
                        {
                            "finish_reason": "stop",
                            "message": {
                                "audio": {
                                    "transcript": script,
                                    "data": base64.b64encode(audio()).decode(),
                                }
                            },
                        }
                    ]
                }
            ).encode()

    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def post(self, url, **kwargs):
            seen.append((url, kwargs, self.trust_env))
            return Response()

    monkeypatch.setattr(m.requests, "Session", Session)
    assert narration(packet, "fixture-key") == audio()
    url, body, proxy = seen[0]
    assert url == "https://api.openai.com/v1/chat/completions" and not proxy
    assert body["allow_redirects"] is False and body["json"]["store"] is False
    assert (
        body["json"]["model"] == m.MODEL
        and body["json"]["max_completion_tokens"] == 2048
    )
    assert body["json"]["messages"][1]["content"] == script
    script += " Changed."
    with pytest.raises(ValueError, match="narration_changed"):
        narration(packet, "fixture-key")


def test_missing_capture_reports_a_controlled_error(media, monkeypatch, tmp_path):
    monkeypatch.setattr(m.render, "CAPTURES", tmp_path)
    with pytest.raises(ValueError, match="captures_changed"):
        m.render.captures()
    response = client(media["owner"]).get(m.PATH)
    assert response.status_code == 200 and "captures" in response.text


def test_expired_proposal_needs_a_new_key_and_no_old_receipt_is_reused(media):
    x = media
    key = uuid.uuid4().hex
    ident = prepare(x, key)
    with x["db"].session() as db:
        row = db.get(m.Media, ident)
        row.expires_at = m.s.clock() - timedelta(seconds=1)
        db.commit()
    assert prepare(x, key) == ident
    assert prepare(x) != ident
    page = client(x["owner"]).get(m.PATH + "?media=" + ident)
    assert (
        "La demande a expiré" in page.text
        and "Préparer une nouvelle fabrication" in page.text
    )
    assert not x["voice_calls"]


def test_retained_outputs_are_bounded_until_owner_removes_one(media):
    x = media
    with x["db"].session() as db:
        for _ in range(10):
            db.add(
                m.Media(
                    owner_id=x["owner"].id,
                    authorization_key=uuid.uuid4().hex,
                    binding="a" * 64,
                    connection_hash="b" * 64,
                    input_hash=uuid.uuid4().hex,
                    packet={},
                    result={},
                    state="approved",
                    created_at=m.s.clock() - timedelta(days=2),
                    expires_at=m.s.clock(),
                )
            )
        db.commit()
    with pytest.raises(ValueError, match="storage_limit"):
        prepare(x)
    assert not x["voice_calls"]


def test_queued_job_rechecks_storage_before_paid_call(media):
    x = media
    ident = prepare(x)
    approve(x, ident)
    with x["db"].session() as db:
        for _ in range(10):
            db.add(
                m.Media(
                    owner_id=x["owner"].id,
                    authorization_key=uuid.uuid4().hex,
                    binding="a" * 64,
                    connection_hash="b" * 64,
                    input_hash=uuid.uuid4().hex,
                    packet={},
                    result={},
                    state="approved",
                    created_at=m.s.clock() - timedelta(days=2),
                    expires_at=m.s.clock(),
                )
            )
        db.commit()
    m.run(ident)
    assert (
        read(x, ident).state == "stopped"
        and read(x, ident).error_code == "storage_limit"
    )
    assert not x["voice_calls"] and read(x, ident).provider_calls == 0


def test_old_expired_candidates_do_not_hide_new_decisions(media):
    x = media
    with x["db"].session() as db:
        for _ in range(10):
            db.add(
                m.Media(
                    owner_id=x["owner"].id,
                    authorization_key=uuid.uuid4().hex,
                    binding="a" * 64,
                    connection_hash="b" * 64,
                    input_hash=uuid.uuid4().hex,
                    packet={},
                    result={},
                    state="proposal",
                    created_at=m.s.clock() - timedelta(days=2),
                    expires_at=m.s.clock() - timedelta(days=1),
                )
            )
        db.commit()
    ident = prepare(x)
    with x["db"].session() as db:
        assert list(m.candidates(db, x["owner"].id)) == [("social_render", ident)]


def test_running_paid_job_reserves_an_output_slot(media):
    x = media
    with x["db"].session() as db:
        row = m.Media(
            owner_id=x["owner"].id,
            authorization_key=uuid.uuid4().hex,
            binding="a" * 64,
            connection_hash="b" * 64,
            input_hash="c" * 64,
            packet={},
            result={},
            state="running",
            provider_calls=1,
            expires_at=m.s.clock(),
        )
        db.add(row)
        db.flush()
        assert m.occupied(db, x["owner"].id) == 1
        assert m.occupied(db, x["owner"].id, row.id) == 0
        row.state = "cancelled"
        db.flush()
        assert m.occupied(db, x["owner"].id) == 0
