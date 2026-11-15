"""Real owner decisions and durable workers; no real provider or Telegram request."""

import base64
from datetime import timedelta
import io
import json
import struct
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
    m.set_policy(x["owner"].id, "2", "")
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


def spent(x, at, *, state="stopped", owner=None):
    """Historical attempt metadata only; no request or authority is fabricated."""
    with x["db"].session() as db:
        row = m.Media(
            owner_id=owner or x["owner"].id,
            authorization_key=uuid.uuid4().hex,
            binding="fixture-history",
            connection_hash="fixture-history",
            input_hash="fixture-history",
            packet={},
            result={},
            state=state,
            provider_calls=1,
            provider_started_at=at,
            expires_at=at + timedelta(hours=24),
        )
        db.add(row)
        db.commit()
        return row.id


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
    # Even a request approved before other attempts must pass the final gate.
    ident = prepare(x)
    approve(x, ident)
    for state in ("uncertain", "cancelled"):
        spent(x, m.s.clock(), state=state)
    with pytest.raises(ValueError, match="^budget$"):
        prepare(x, goal="product")  # A new pack, not the already authorized duplicate.
    m.run(ident)
    assert read(x, ident).error_code == "budget" and not x["voice_calls"]
    assert read(x, ident).provider_started_at is None


def test_budget_returns_exact_reopening_and_excludes_other_owners(media, monkeypatch):
    x = media
    now = m.s.clock().replace(microsecond=0)
    monkeypatch.setattr(m.s, "clock", lambda: now)
    spent(x, now - timedelta(hours=20), state="cancelled")
    spent(x, now - timedelta(hours=2), state="uncertain")
    other = user(x["db"])
    spent(x, now, owner=other.id)
    with x["db"].session() as db:
        budget = m.audio_budget(db, x["owner"].id)
    assert budget == {"limit": 2, "remaining": 0, "retry_at": now + timedelta(hours=4)}


def test_budget_reopens_at_24_hours_without_changing_history(media, monkeypatch):
    x = media
    now = m.s.clock().replace(microsecond=0)
    monkeypatch.setattr(m.s, "clock", lambda: now)
    first = spent(x, now - timedelta(hours=24))
    second = spent(x, now - timedelta(hours=23))
    with x["db"].session() as db:
        assert m.audio_budget(db, x["owner"].id) == {
            "limit": 2,
            "remaining": 1,
            "retry_at": None,
        }
    monkeypatch.setattr(m.s, "clock", lambda: now + timedelta(hours=1))
    with x["db"].session() as db:
        assert m.audio_budget(db, x["owner"].id)["remaining"] == 2
    assert read(x, first).provider_calls == read(x, second).provider_calls == 1
    assert read(x, first).state == read(x, second).state == "stopped"
    assert prepare(x)
    assert not x["voice_calls"]


def test_excess_history_waits_until_enough_attempts_have_expired(media, monkeypatch):
    x = media
    now = m.s.clock().replace(microsecond=0)
    monkeypatch.setattr(m.s, "clock", lambda: now)
    for hours in (22, 20, 2):
        spent(x, now - timedelta(hours=hours))
    with x["db"].session() as db:
        assert m.audio_budget(db, x["owner"].id)["retry_at"] == now + timedelta(hours=4)


@pytest.mark.parametrize("lang", ["fr", "en"], ids=["fr", "en"])
def test_full_budget_is_visible_and_post_creates_no_row_or_telegram_card(media, lang):
    x = media
    for hours in (20, 2):
        spent(x, m.s.clock() - timedelta(hours=hours))
    c = client(x["owner"])
    count, calls = len(rows(x)), len(x["calls"])
    page = c.get(m.PATH + "?lang=" + lang)
    assert page.status_code == 200 and "data-media-local-time" in page.text
    assert "0 / 2" in page.text and "UTC" in page.text
    assert 'action="' + m.PATH + "/prepare" not in page.text
    response = c.post(
        m.PATH + "/prepare?lang=" + lang,
        data={
            "_csrf": c.cookies.get(app._CSRF_COOKIE_NAME),
            "key": uuid.uuid4().hex,
            "confirm": "true",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303 and "err=budget" in response.headers["location"]
    assert len(rows(x)) == count and len(x["calls"]) == calls and not x["voice_calls"]
    with x["db"].session() as db:
        assert db.scalar(select(func.count()).select_from(m.Media)) == 2


def test_blocked_old_attempt_does_not_offer_a_new_fabrication_until_slot_reopens(
    media, monkeypatch
):
    x = media
    now = m.s.clock()
    ident = prepare(x)
    with x["db"].session() as db:
        row = db.get(m.Media, ident)
        row.state, row.error_code = "stopped", "budget"
        db.commit()
    for hours in (20, 2):
        spent(x, now - timedelta(hours=hours))
    page = client(x["owner"]).get(m.PATH + "?media=" + ident)
    assert (
        "Vérifier la disponibilité" in page.text
        and "Préparer une nouvelle fabrication" not in page.text
    )
    monkeypatch.setattr(m.s, "clock", lambda: now + timedelta(hours=4, seconds=1))
    page = client(x["owner"]).get(m.PATH + "?media=" + ident)
    assert "Préparer une nouvelle fabrication" in page.text
    assert read(x, ident).provider_started_at is None


def test_saved_audio_montage_and_exact_review_remain_available_with_no_new_slot(media):
    x = media
    ident = prepare(x)
    approve(x, ident)
    with x["db"].session() as db:
        row = db.get(m.Media, ident)
        row.provider_calls, row.provider_started_at = 1, m.s.clock()
        row.result = {"audio": base64.b64encode(audio()).decode()}
        db.commit()
    spent(x, m.s.clock())
    m.run(ident)
    assert read(x, ident).state == "review" and not x["voice_calls"]
    page = client(x["owner"]).get(m.PATH + "?media=" + ident)
    assert "0 / 2" in page.text and "<video " in page.text
    approve(x, ident, "social_review")
    assert read(x, ident).state == "approved"


def test_duplicate_submission_returns_existing_request_without_new_authority_at_cap(
    media,
):
    x = media
    key = uuid.uuid4().hex
    ident = prepare(x, key)
    for _ in range(2):
        spent(x, m.s.clock())
    assert prepare(x, key) == ident
    assert read(x, ident).state == "proposal" and not x["voice_calls"]
    m.approvals().sync()
    assert not rows(x)  # Even delayed first delivery waits while the voice cap is full.


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


@pytest.mark.parametrize("pending", [False, True], ids=["finite-wav", "streaming-wav"])
def test_provider_request_is_fixed_bounded_and_checks_exact_transcript(
    media, monkeypatch, pending
):
    # Exercise the actual HTTP adapter, not a network service.
    narration = REAL_NARRATION
    with media["db"].session() as db:
        packet = m.packet(db, "fr", "discovery", "freelance")
    script = " ".join(s["text"] for s in packet["scenes"])
    seen = []
    received = bytearray(audio())
    if pending:
        struct.pack_into("<I", received, 4, 0xFFFFFFFF)
        struct.pack_into("<I", received, 40, 0xFFFFFFFF)

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
                                    "data": base64.b64encode(received).decode(),
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
        and body["json"]["max_completion_tokens"] == 4096
    )
    assert body["json"]["messages"][1]["content"] == script
    script += " Changed."
    with pytest.raises(ValueError, match="narration_changed"):
        narration(packet, "fixture-key")


@pytest.mark.parametrize(
    "code",
    [
        "audio_missing",
        "audio_encoding",
        "audio_format",
        "audio_incomplete",
        "audio_duration",
    ],
    ids=["missing", "encoding", "format", "incomplete", "duration"],
)
def test_audio_failure_is_specific_terminal_and_keeps_the_paid_attempt(
    media, monkeypatch, code
):
    x = media
    ident = prepare(x)
    approve(x, ident)

    def rejected_audio(*args):
        x["voice_calls"].append("attempt")
        raise ValueError(code)

    monkeypatch.setattr(m, "narration", rejected_audio)
    m.run(ident)
    m.run(ident)
    row = read(x, ident)
    assert row.state == "stopped" and row.error_code == code
    assert row.provider_calls == 1 and row.provider_started_at and row.result == {}
    assert x["voice_calls"] == ["attempt"]
    for lang in ("fr", "en"):
        response = client(x["owner"]).get(m.PATH + "?media=" + ident + "&lang=" + lang)
        assert response.status_code == 200
        assert (
            "automatic retry" in response.text
            if lang == "en"
            else "appel automatique" in response.text
        )


@pytest.mark.parametrize(
    "fault,expected",
    [
        ("missing", "audio_missing"),
        ("encoding", "audio_encoding"),
        ("format", "audio_format"),
        ("incomplete", "audio_incomplete"),
        ("duration", "audio_duration"),
        ("json", "audio_invalid"),
        ("text", "narration_changed"),
        ("limit", "narration_truncated"),
    ],
    ids=[
        "missing",
        "encoding",
        "format",
        "incomplete",
        "duration",
        "json",
        "text",
        "limit",
    ],
)
def test_real_http_adapter_rejects_bad_audio_once_with_no_external_detail(
    media, monkeypatch, fault, expected
):
    with media["db"].session() as db:
        packet = m.packet(db, "fr", "discovery", "freelance")
    script = " ".join(s["text"] for s in packet["scenes"])
    received = bytearray(audio())
    if fault == "format":
        received[:4] = b"nope"
    if fault == "incomplete":
        received = received[:-100]
    if fault == "duration":
        received = received[: 44 + 24000 * 2 * 3]
        struct.pack_into("<I", received, 4, len(received) - 8)
        struct.pack_into("<I", received, 40, len(received) - 44)
    payload = {
        "choices": [
            {
                "finish_reason": "length" if fault == "limit" else "stop",
                "message": {
                    "audio": None
                    if fault == "missing"
                    else {
                        "transcript": "different words" if fault == "text" else script,
                        "data": "invalid!"
                        if fault == "encoding"
                        else base64.b64encode(received).decode(),
                    }
                },
            }
        ]
    }
    body = b"invalid-json" if fault == "json" else json.dumps(payload).encode()
    calls = []

    class Response:
        status_code = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def iter_content(self, size):
            yield body

    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def post(self, *args, **kwargs):
            calls.append("one request")
            return Response()

    monkeypatch.setattr(m.requests, "Session", Session)
    with pytest.raises(ValueError, match="^" + expected + "$"):
        REAL_NARRATION(packet, "fixture-key")
    assert calls == ["one request"]


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


def test_distinct_productions_wait_for_the_active_montage(media, monkeypatch):
    x = media
    first = prepare(x)
    second = m.prepare(
        x["owner"].id, "en", "discovery", "freelance", uuid.uuid4().hex, True
    )
    approve(x, first)
    approve(x, second)
    entered, release = threading.Event(), threading.Event()

    def voice(*args):
        x["voice_calls"].append("one")
        entered.set()
        assert release.wait(10)
        return audio()

    monkeypatch.setattr(m, "narration", voice)
    thread = threading.Thread(target=m.run, args=(first,))
    thread.start()
    try:
        assert entered.wait(10)
        m.run(second)
        assert read(x, second).state == "queued" and read(x, second).provider_calls == 0
        assert len(x["voice_calls"]) == 1
    finally:
        release.set()
        thread.join(timeout=15)
    assert read(x, first).state == "review"
    m.run(second)
    assert read(x, second).state == "review" and len(x["voice_calls"]) == 2
