"""Clicking the link in the verification email — the last step nobody had ever reached.

The email finally arrived, and the button returned 500. Same defect as the two fixed the day
before, third occurrence, in the shape I had not swept for:

    now = datetime.now(timezone.utc)                          # aware
    exp = _dt_as_naive_utc(getattr(row, "expires_at", None))  # naive
    if exp and exp <= now:                                    # TypeError

I fixed the SUBTRACTIONS and never looked for the COMPARISONS. The same three lines existed in
the password reset validator, so that path was broken twice over: its email could not be
composed, and its token could not have been validated either.

These tests walk the whole path — signup, email, click, session — because every previous test
stopped at the step before the one that was broken.
"""

from __future__ import annotations

import os
import re
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-verify-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as app_module  # noqa: E402
from backend.app import app  # noqa: E402

PASSWORD = "un-mot-de-passe-assez-long"


# --- the comparison itself ----------------------------------------------------------------

@pytest.mark.parametrize(
    "value,expected",
    [
        (datetime.now(timezone.utc) + timedelta(hours=1), False),                      # aware, future
        (datetime.now(timezone.utc) - timedelta(hours=1), True),                       # aware, past
        (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=1), False), # naive, future
        (datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=1), True),  # naive, past
        (None, False),                                                                 # no expiry
    ],
)
def test_expiry_is_judged_whichever_kind_of_datetime_arrives(value, expected: bool) -> None:
    assert app_module._dt_is_past(value) is expected


def test_a_missing_expiry_means_no_expiry_not_expired() -> None:
    # Getting this backwards would reject every token whose column happened to be NULL.
    assert app_module._dt_is_past(None) is False


# --- the whole path -----------------------------------------------------------------------

@pytest.fixture()
def captured_link(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Verification enabled, mail captured instead of sent."""
    links: list[str] = []
    monkeypatch.setattr(app_module, "_email_verification_enabled", lambda: True)
    monkeypatch.setattr(
        app_module, "_send_email_verification_email",
        lambda *, to_email, verify_url, expires_at: links.append(verify_url),
    )
    return links


def _signup(client: TestClient, email: str):
    page = client.get("/auth/signup")
    token = re.search(r'name="_csrf"\s+value="([^"]*)"', page.text)
    assert token, "the signup form no longer carries a CSRF field"
    return client.post(
        "/auth/signup",
        data={"email": email, "password": PASSWORD, "next": "/", "_csrf": token.group(1)},
        follow_redirects=False,
    )


def test_signing_up_then_clicking_the_link_signs_you_in(captured_link) -> None:
    with TestClient(app) as client:
        assert _signup(client, "nouveau@exemple.fr").status_code == 303
        assert captured_link, "no verification link was produced"

        path = captured_link[0].replace("http://testserver", "")
        response = client.get(path, follow_redirects=False)

        assert response.status_code == 303, f"the verification link returned {response.status_code}"
        assert app_module.auth.SESSION_COOKIE_NAME in response.cookies, (
            "the link was accepted but no session was opened"
        )


def test_the_same_link_cannot_be_used_twice(captured_link) -> None:
    with TestClient(app) as client:
        _signup(client, "double@exemple.fr")
        path = captured_link[0].replace("http://testserver", "")
        assert client.get(path, follow_redirects=False).status_code == 303
        second = client.get(path, follow_redirects=False)
    assert second.status_code == 400, "a verification link stayed replayable"


def test_an_expired_link_is_refused_and_not_a_500(captured_link, monkeypatch) -> None:
    # The whole point: an expired token must produce a polite refusal. It used to raise, and a
    # raise in this handler is a 500 page for someone who did nothing wrong.
    with TestClient(app) as client:
        _signup(client, "expire@exemple.fr")
        path = captured_link[0].replace("http://testserver", "")
        monkeypatch.setattr(app_module, "_dt_is_past", lambda value: True)
        response = client.get(path, follow_redirects=False)
    assert response.status_code == 400
    assert "expir" in response.text.lower()


def test_a_forged_token_is_refused(captured_link) -> None:
    with TestClient(app) as client:
        _signup(client, "faux@exemple.fr")
        response = client.get("/auth/verify?token=pas-un-vrai-jeton&next=/", follow_redirects=False)
    assert response.status_code == 400


def test_a_link_with_no_token_is_refused(captured_link) -> None:
    with TestClient(app) as client:
        assert client.get("/auth/verify?next=/", follow_redirects=False).status_code == 400


# --- where a brand-new account lands ------------------------------------------------------

def test_a_new_account_is_not_dropped_on_someone_elses_page(captured_link) -> None:
    """Observed on a real signup: verification succeeded and the first screen said
    "Job introuvable."

    The visitor had started signing up from a job page belonging to another account, so `next`
    carried a deep link the new account could never open. The ownership check was right; the
    welcome was not.
    """
    with TestClient(app) as client:
        page = client.get("/auth/signup?next=/jobs/ae025623-7a2c-470d-910c-74c8f6da9df3")
        token = re.search(r'name="_csrf"\s+value="([^"]*)"', page.text)
        assert token
        client.post(
            "/auth/signup",
            data={
                "email": "arrivant@exemple.fr",
                "password": PASSWORD,
                "next": "/jobs/ae025623-7a2c-470d-910c-74c8f6da9df3",
                "_csrf": token.group(1),
            },
            follow_redirects=False,
        )
        assert captured_link
        response = client.get(captured_link[0].replace("http://testserver", ""), follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/", (
        f"a new account was sent to {response.headers['location']}, which it cannot open"
    )
