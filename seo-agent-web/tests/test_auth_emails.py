"""The two emails that let someone get into their account, and the line that broke both.

Signup on production returned "Impossible d'envoyer l'email de vérification" with NO [MAIL]
line anywhere in the logs — proof the failure happened before any mail code ran. It did:

    exp = _dt_as_naive_utc(expires_at) or datetime.now(timezone.utc)
    ttl_s = max(60, int((exp - datetime.now(timezone.utc)).total_seconds()))

_dt_as_naive_utc strips the tzinfo, so that is NAIVE minus AWARE — a TypeError in Python,
raised on the first line of composing the message. The same two lines existed, copy-pasted,
in the password reset email. Both recovery paths had therefore never worked, and the only
symptom either produced was a generic "try again later".

The arithmetic now lives in one helper used by both, so they cannot drift apart again.
"""

from __future__ import annotations

import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-mail-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402

AWARE = datetime.now(timezone.utc) + timedelta(hours=24)
NAIVE = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=24)


# --- the arithmetic ---------------------------------------------------------------------

@pytest.mark.parametrize("expires_at", [AWARE, NAIVE], ids=["aware", "naive"])
def test_a_ttl_is_computed_whichever_kind_of_datetime_arrives(expires_at: datetime) -> None:
    # The DB hands back naive values and the code creates aware ones; both must work, because
    # mixing them is exactly what broke this.
    seconds = app_module._seconds_until(expires_at)
    assert 23 * 3600 < seconds <= 24 * 3600


def test_an_expiry_already_past_does_not_produce_a_negative_ttl() -> None:
    seconds = app_module._seconds_until(datetime.now(timezone.utc) - timedelta(hours=5))
    assert seconds == 60


def test_a_missing_expiry_falls_back_instead_of_raising() -> None:
    assert app_module._seconds_until(None) == 60


# --- the two emails ---------------------------------------------------------------------

@pytest.fixture()
def captured_send(monkeypatch: pytest.MonkeyPatch) -> list[dict]:
    sent: list[dict] = []
    monkeypatch.setattr(app_module, "_send_email", lambda **kw: sent.append(kw))
    return sent


@pytest.mark.parametrize("expires_at", [AWARE, NAIVE, None], ids=["aware", "naive", "none"])
def test_the_verification_email_reaches_the_sender(expires_at, captured_send) -> None:
    app_module._send_email_verification_email(
        to_email="prospect@exemple.fr",
        verify_url="https://noyaru.com/auth/verify?token=abc",
        expires_at=expires_at,
    )
    assert len(captured_send) == 1
    assert captured_send[0]["to_addr"] == "prospect@exemple.fr"
    assert "https://noyaru.com/auth/verify?token=abc" in captured_send[0]["body"]


@pytest.mark.parametrize("expires_at", [AWARE, NAIVE, None], ids=["aware", "naive", "none"])
def test_the_password_reset_email_reaches_the_sender(expires_at, captured_send) -> None:
    app_module._send_password_reset_email(
        to_email="prospect@exemple.fr",
        reset_url="https://noyaru.com/auth/reset?token=abc",
        expires_at=expires_at,
    )
    assert len(captured_send) == 1
    assert "https://noyaru.com/auth/reset?token=abc" in captured_send[0]["body"]


def test_the_verification_email_states_a_believable_validity(captured_send) -> None:
    app_module._send_email_verification_email(
        to_email="p@exemple.fr", verify_url="https://noyaru.com/v", expires_at=AWARE
    )
    body = captured_send[0]["body"] + captured_send[0].get("html_body", "")
    assert "24" in body, "the email must tell the reader how long the link lasts"


def test_the_link_is_escaped_in_the_html_part(captured_send) -> None:
    # The URL carries a token from the request; an unescaped quote would break the anchor.
    app_module._send_email_verification_email(
        to_email="p@exemple.fr",
        verify_url='https://noyaru.com/v?t=a"onmouseover="x',
        expires_at=AWARE,
    )
    html_part = captured_send[0].get("html_body") or ""
    assert 'onmouseover="x' not in html_part
    assert "&quot;" in html_part
