"""What happens to a new customer when the verification email cannot be sent.

Found while walking the signup path by hand: SMTP failed, and the form came back with
"Impossible d'envoyer l'email de vérification. Réessaie plus tard." — but the account had
already been committed several lines earlier. Retrying the same address then answers "Ce
compte existe déjà", and nothing anywhere mentions that /auth/verify/resend exists. The
prospect is permanently stuck at the first screen of the product.

The SMTP outage is a configuration problem. Being unable to recover from it is a product one,
and it is the one pinned here.
"""

from __future__ import annotations

import os
import re
import sys
import tempfile
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-signup-"))
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

EMAIL = "prospect@exemple.fr"
PASSWORD = "un-mot-de-passe-assez-long"


@pytest.fixture()
def smtp_down(monkeypatch: pytest.MonkeyPatch):
    """Verification enabled (SMTP configured) but every send fails, as it did in production."""
    monkeypatch.setattr(app_module, "_email_verification_enabled", lambda: True)

    def _boom(**_kw):
        raise OSError("[Errno 111] Connection refused")

    monkeypatch.setattr(app_module, "_send_email_verification_email", _boom)


def _signup(client: TestClient, email: str = EMAIL):
    """Post the form the way a browser does, CSRF token included."""
    page = client.get("/auth/signup")
    assert page.status_code == 200
    match = re.search(r'name="_csrf"\s+value="([^"]*)"', page.text)
    assert match, "the signup form no longer carries a CSRF field"
    return client.post(
        "/auth/signup",
        data={"email": email, "password": PASSWORD, "next": "/", "_csrf": match.group(1)},
        follow_redirects=False,
    )


def test_a_failed_verification_email_does_not_strand_the_new_account(smtp_down) -> None:
    with TestClient(app) as client:
        response = _signup(client)

    assert response.status_code == 303
    location = response.headers["location"]
    assert location.startswith("/auth/verify/resend"), (
        f"a stranded signup was sent to {location}, where the account it just created is refused"
    )
    assert "prospect%40exemple.fr" in location or "prospect@exemple.fr" in location, (
        "the resend page must arrive with the address already filled in"
    )


def test_the_message_says_the_account_exists(smtp_down) -> None:
    # Without this, the "Ce compte existe déjà" they meet on any retry looks like a bug.
    with TestClient(app) as client:
        response = _signup(client, "autre@exemple.fr")
    assert "cr%C3%A9%C3%A9" in response.headers["location"] or "créé" in response.headers["location"]


def test_the_resend_page_accepts_what_signup_sends_it(smtp_down) -> None:
    with TestClient(app) as client:
        location = _signup(client, "troisieme@exemple.fr").headers["location"]
        page = client.get(location)
    assert page.status_code == 200
    assert "troisieme@exemple.fr" in page.text


def test_the_failure_is_recorded_for_whoever_has_to_debug_it(
    smtp_down, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The audit trail is the only place the real SMTP error survives; a 503 page tells the
    # operator nothing.
    logged: list[dict] = []
    original = app_module._audit_log

    def _capture(request, **kw):
        logged.append(kw)
        return original(request, **kw)

    monkeypatch.setattr(app_module, "_audit_log", _capture)
    with TestClient(app) as client:
        _signup(client, "quatrieme@exemple.fr")

    errors = [e for e in logged if e.get("status") == "send_error"]
    assert errors, "the SMTP error was not recorded anywhere"
    assert "Connection refused" in str(errors[0].get("meta") or {})
    assert any(e.get("status") == "created_unverified" for e in logged), (
        "nothing records that an unverified account was left behind"
    )
