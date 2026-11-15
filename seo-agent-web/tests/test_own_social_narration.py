"""Complete differing narration is private evidence, never an automatic approval."""

import base64
import json

import pytest
from backend import own_social_media as m
from test_customer_activation import lab as lab, client
from test_telegram_approvals import bot as bot, rows, row as approval_row, click
from test_own_social_media import (
    media as media,
    audio,
    prepare,
    approve,
    read,
    REAL_NARRATION,
)


def http_reply(monkeypatch, packet, *, transcript=None, finish="stop", data=None):
    calls = []
    payload = {
        "choices": [
            {
                "finish_reason": finish,
                "message": {
                    "audio": {
                        "transcript": transcript
                        if transcript is not None
                        else m.script(packet),
                        "data": base64.b64encode(audio()).decode()
                        if data is None
                        else data,
                    }
                },
            }
        ]
    }

    class Response:
        status_code = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def iter_content(self, size):
            yield json.dumps(payload).encode()

    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def post(self, url, **kwargs):
            calls.append((url, kwargs, self.trust_env))
            return Response()

    monkeypatch.setattr(m.requests, "Session", Session)
    return calls


def differing(x, monkeypatch, received=None):
    ident = prepare(x)
    approve(x, ident)
    packet = read(x, ident).packet
    received = received or m.script(packet).replace("ne prouve pas", "garantit")

    def voice(packet, key):
        x["voice_calls"].append(packet)
        raise m.NarrationReview(audio(), received)

    monkeypatch.setattr(m, "narration", voice)
    m.run(ident)
    return ident, received


@pytest.mark.parametrize(
    "finish,code",
    [
        ("length", "narration_truncated"),
        ("content_filter", "narration_incomplete"),
        ("tool_calls", "narration_incomplete"),
    ],
    ids=["truncated", "filtered", "tools"],
)
def test_incomplete_output_has_its_own_reason_and_never_retries(
    media, monkeypatch, finish, code
):
    x = media
    ident = prepare(x)
    approve(x, ident)
    calls = http_reply(monkeypatch, read(x, ident).packet, finish=finish)
    monkeypatch.setattr(m, "narration", REAL_NARRATION)
    m.run(ident)
    m.run(ident)
    row = read(x, ident)
    assert row.state == "stopped" and row.error_code == code and row.result == {}
    assert row.provider_calls == 1 and row.provider_started_at
    assert len(calls) == 1 and not calls[0][2]


def test_exact_script_and_formatting_still_return_bytes(media, monkeypatch):
    packet = read(media, prepare(media)).packet
    http_reply(monkeypatch, packet, transcript=m.script(packet).upper() + " !")
    assert REAL_NARRATION(packet, "fictional") == audio()


def test_http_mismatch_carries_valid_audio_for_explicit_private_review(
    media, monkeypatch
):
    packet = read(media, prepare(media)).packet
    received = m.script(packet).replace("ne prouve pas", "garantit")
    calls = http_reply(monkeypatch, packet, transcript=received)
    with pytest.raises(m.NarrationReview, match="^narration_changed$") as exc:
        REAL_NARRATION(packet, "fictional")
    assert exc.value.audio == audio() and exc.value.transcript == received
    assert len(calls) == 1 and calls[0][1]["json"]["max_completion_tokens"] == 4096


@pytest.mark.parametrize(
    "transcript",
    ["", " \n\t", "...!", "x" * 4001, "bad\x00text", "bad\u200btext", 42, [], True],
    ids=["empty", "blank", "punctuation", "long", "nul", "invisible", "number", "array", "bool"],
)
def test_unusable_transcript_cannot_become_a_review(media, monkeypatch, transcript):
    packet = read(media, prepare(media)).packet
    http_reply(monkeypatch, packet, transcript=transcript)
    with pytest.raises(ValueError, match="^audio_invalid$"):
        REAL_NARRATION(packet, "fictional")


def test_bad_wav_is_not_saved_just_because_transcript_differs(media, monkeypatch):
    packet = read(media, prepare(media)).packet
    http_reply(
        monkeypatch,
        packet,
        transcript="a different script",
        data=base64.b64encode(b"bad wav").decode(),
    )
    with pytest.raises(ValueError, match="^audio_invalid$"):
        REAL_NARRATION(packet, "fictional")


@pytest.mark.parametrize("accept", [False, True], ids=["reject", "explicit-accept"])
def test_changed_claim_stays_private_until_exact_second_decision(
    media, monkeypatch, accept
):
    x = media
    ident, received = differing(x, monkeypatch)
    row = read(x, ident)
    assert row.state == "review" and row.error_code == "narration_review"
    assert row.result["narration_check"]["transcript"] == received
    assert "audio" not in row.result and m.video(row)
    m.run(ident)
    assert len(x["voice_calls"]) == 1 and read(x, ident).state == "review"
    with x["db"].session() as db:
        doc = m.approval_document(db, x["owner"].id, ident, "social_review", "en")
        assert "explicitly accept this received narration" in doc["effect"]
        assert received in doc["text"] and m.script(row.packet) in doc["text"]
        assert "difference" in doc["decision_summary"]
    approve(x, ident, "social_review", yes=accept)
    row = read(x, ident)
    assert row.state == ("approved" if accept else "cancelled")
    if accept:
        assert row.error_code is None and row.reviewed_at and m.video(row)
    else:
        assert row.result == {}
    assert len(x["voice_calls"]) == 1


def test_local_retry_keeps_narration_comparison_without_another_voice_call(
    media, monkeypatch
):
    x = media
    original_render = m.render.render
    attempts = []

    def once(packet, data):
        attempts.append(1)
        if len(attempts) == 1:
            raise ValueError("render_failed")
        return original_render(packet, data)

    monkeypatch.setattr(m.render, "render", once)
    ident, received = differing(x, monkeypatch)
    assert (
        read(x, ident).state == "queued"
        and read(x, ident).result["narration_check"]["transcript"] == received
    )
    m.run(ident)
    assert (
        read(x, ident).state == "review"
        and read(x, ident).error_code == "narration_review"
    )
    assert len(x["voice_calls"]) == 1 and len(attempts) == 2


def test_changed_transcript_invalidates_review_even_with_unchanged_video(
    media, monkeypatch
):
    x = media
    ident, _ = differing(x, monkeypatch)
    m.approvals().sync()
    m.approvals().deliver_one()
    card = next(
        r for r in rows(x) if r.target_id == ident and r.kind == "social_review"
    )
    video_sha = read(x, ident).result["sha256"]
    with x["db"].session() as db:
        row = db.get(m.Media, ident)
        check = {"transcript": "a different received narration"}
        check["sha256"] = m.s.digest([m.script(row.packet), check["transcript"]])
        row.result = {**row.result, "narration_check": check}
        db.commit()
    for page in range(len(card.pages)):
        click(x, card.id, "p" + str(page))
    click(x, card.id, "y")
    assert not approval_row(x, card.id).confirmation
    assert (
        read(x, ident).state == "review"
        and read(x, ident).result["sha256"] == video_sha
    )
    assert len(x["voice_calls"]) == 1


def test_tampered_evidence_is_not_downloadable_or_approvable(media, monkeypatch):
    x = media
    ident, _ = differing(x, monkeypatch)
    with x["db"].session() as db:
        row = db.get(m.Media, ident)
        row.result = {
            **row.result,
            "narration_check": {"transcript": "tampered", "sha256": "bad"},
        }
        db.commit()
    assert client(x["owner"]).get(m.PATH + "/" + ident + "/video").status_code == 409
    with x["db"].session() as db:
        with pytest.raises(ValueError, match="^changed$"):
            m.approval_document(db, x["owner"].id, ident, "social_review", "fr")


@pytest.mark.parametrize("lang", ["fr", "en"], ids=["fr", "en"])
def test_private_page_shows_safe_word_diff_and_original_text(media, monkeypatch, lang):
    x = media
    ident, received = differing(
        x, monkeypatch, "Received <script>alert('fictional')</script> text"
    )
    page = client(x["owner"]).get(m.PATH + "?media=" + ident + "&lang=" + lang)
    assert (
        page.status_code == 200
        and 'class="narration-diff"' in page.text
        and "<del>" in page.text
        and "<ins>" in page.text
    )
    assert (
        "<script>alert('fictional')</script>" not in page.text
        and "&lt;script&gt;" in page.text
    )
    assert "<video " in page.text and m.script(read(x, ident).packet) in page.text
    assert 'action="' + m.PATH + "/prepare" not in page.text
    assert page.headers["cache-control"] == "no-store" and len(x["voice_calls"]) == 1


def test_old_authorization_cannot_use_new_output_budget(media, monkeypatch):
    x = media
    with monkeypatch.context() as old:
        old.setattr(m, "PROFILE", "noyaru-commented-captures-v1")
        old.setattr(m, "MAX_TOKENS", 2048)
        ident = prepare(x)
        approve(x, ident)
    m.run(ident)
    assert (
        read(x, ident).state == "stopped"
        and read(x, ident).error_code == "connection_changed"
    )
    assert not x["voice_calls"] and read(x, ident).provider_started_at is None
