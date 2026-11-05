"""No real DSN, bot credentials or provider calls: diagnostic hooks keep tokens private."""

import copy
import json
import sys
from types import SimpleNamespace
from backend import telegram, app as m, sentry_filtre


def test_http_urls_nested_locals_and_rotated_tokens_are_masked_without_mutating_event(
    monkeypatch,
):
    current = "123456:" + "a" * 35
    previous = "654321:" + "b" * 35
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", current)
    event = {
        "request": {"url": "https://api.telegram.org/bot" + current + "/getUpdates"},
        "breadcrumbs": {
            "values": [
                {
                    "data": {
                        "url": "https://api.telegram.org/bot"
                        + previous.replace(":", "%3A")
                        + "/sendMessage"
                    }
                }
            ]
        },
        "exception": {
            "values": [{"stacktrace": {"frames": [{"vars": {"token": current}}]}}]
        },
        "spans": [
            {
                "description": "POST https://api.telegram.org/bot"
                + previous
                + "/getMe",
                "data": {"status": 200},
            }
        ],
        "message": "A useful diagnostic",
        "extra": {"count": 3, "enabled": True, "optional": None},
    }
    original = copy.deepcopy(event)
    clean = telegram.redact_diagnostics(event, {})
    encoded = json.dumps(clean)
    assert (
        current not in encoded
        and previous not in encoded
        and previous.replace(":", "%3A") not in encoded
    )
    assert (
        "[telegram-token]" in encoded
        and "/getUpdates" in encoded
        and "/sendMessage" in encoded
    )
    assert (
        clean["extra"] == event["extra"]
        and clean["message"] == event["message"]
        and event == original
    )


def test_diagnostics_mask_real_format_tokens_even_when_bot_is_unconfigured(monkeypatch):
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    token = "123456:" + "z" * 35
    assert token not in telegram.redact_diagnostics(
        "POST https://api.telegram.org/bot" + token + "/getMe"
    )
    assert telegram.redact_diagnostics(None) is None


def test_all_three_sentry_delivery_paths_use_the_telegram_filter(monkeypatch):
    captured = {}
    monkeypatch.setattr(m, "_SENTRY_READY", False)
    monkeypatch.setenv("SENTRY_DSN", "https://public@example.invalid/1")
    monkeypatch.setitem(
        sys.modules,
        "sentry_sdk",
        SimpleNamespace(init=lambda **kw: captured.update(kw)),
    )
    m._init_sentry()
    for key in ("before_send", "before_send_transaction", "before_breadcrumb"):
        assert captured[key] is sentry_filtre.nettoyer_evenement
        token = "123456:" + "w" * 35
        event = {
            "request": {
                "data": "private-form",
                "cookies": "private-cookie",
                "query_string": "private-query",
            },
            "message": "https://api.telegram.org/bot"
            + token.replace(":", "%3A")
            + "/getUpdates contact@example.invalid",
        }
        cleaned = json.dumps(captured[key](event, {}))
        assert (
            token.replace(":", "%3A") not in cleaned
            and "example.invalid" not in cleaned
        )
        assert not any(
            value in cleaned
            for value in ("private-form", "private-cookie", "private-query")
        )
    assert (
        captured["send_default_pii"] is False
        and captured["include_local_variables"] is False
    )
    assert captured["max_request_body_size"] == "never"
    assert m._SENTRY_READY
