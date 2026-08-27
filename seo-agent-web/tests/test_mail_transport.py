"""Choosing how an email leaves, now that it is not hard-wired to one vendor.

SendGrid stopped accepting our mail ("Maximum credits exceeded", HTTP 401) and replacing it
turned out to be more than an environment change: _send_email had a SendGrid-shaped special
case, and every other provider fell through to raw outbound SMTP — the port PaaS hosts
commonly filter, and the reason the HTTP path existed in the first place.

Switching vendor is now an environment change, and these tests pin the part that is easy to
get wrong: which transport a given configuration actually selects. Picking the wrong one does
not raise; it silently sends over a port that may be blocked, or authenticates with a key that
means something else to that provider.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-transport-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402


def _cfg(**over):
    base = {
        "host": "smtp.sendgrid.net", "port": 587, "username": "apikey", "password": "SG.secret",
        "from": "contact@noyaru.com", "from_name": "Noyaru",
        "ssl": False, "starttls": True, "timeout_s": 10.0,
    }
    base.update(over)
    return base


@pytest.fixture(autouse=True)
def _no_explicit_override(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("MAIL_API_PROVIDER", raising=False)
    monkeypatch.delenv("MAIL_API_KEY", raising=False)


# --- which transport ------------------------------------------------------------------------

def test_sendgrid_credentials_select_the_sendgrid_api() -> None:
    assert app_module._mail_api_transport(_cfg()) == ("sendgrid", "SG.secret")


def test_a_sendgrid_host_with_the_wrong_username_does_not_pretend_to_have_a_key() -> None:
    # SendGrid's SMTP username is the literal word "apikey"; anything else means the password
    # is not an API key, and using it over HTTP would 401.
    assert app_module._mail_api_transport(_cfg(username="contact@noyaru.com")) == ("", "")


def test_resend_smtp_credentials_select_the_resend_api() -> None:
    cfg = _cfg(host="smtp.resend.com", username="resend", password="re_secret")
    assert app_module._mail_api_transport(cfg) == ("resend", "re_secret")


@pytest.mark.parametrize("host", ["smtp-relay.brevo.com", "smtp-relay.sendinblue.com"])
def test_brevo_smtp_credentials_do_not_select_the_http_api(host: str) -> None:
    # Brevo's SMTP password is an SMTP key, NOT the transactional API key. Reusing it over
    # HTTP would fail with an authentication error that looks like a quota problem.
    assert app_module._mail_api_transport(_cfg(host=host, username="user", password="smtp-key")) == ("", "")


def test_an_unknown_host_falls_back_to_plain_smtp() -> None:
    assert app_module._mail_api_transport(_cfg(host="smtp.ovh.net", username="u", password="p")) == ("", "")


def test_an_explicit_provider_wins_over_whatever_the_host_suggests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The point of the override: use a provider's HTTP API without configuring it as an SMTP
    # server at all, which is how Brevo has to be driven.
    monkeypatch.setenv("MAIL_API_PROVIDER", "brevo")
    monkeypatch.setenv("MAIL_API_KEY", "xkeysib-secret")
    assert app_module._mail_api_transport(_cfg()) == ("brevo", "xkeysib-secret")


def test_a_provider_without_a_key_is_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MAIL_API_PROVIDER", "brevo")
    assert app_module._mail_api_transport(_cfg(host="smtp.ovh.net", username="u", password="p")) == ("", "")


# --- what actually gets called ----------------------------------------------------------------

@pytest.fixture()
def posted(monkeypatch: pytest.MonkeyPatch) -> list[dict]:
    calls: list[dict] = []

    class _Resp:
        status_code = 202
        text = ""

    def _post(url, headers=None, json=None, timeout=None):
        calls.append({"url": url, "headers": headers or {}, "json": json or {}})
        return _Resp()

    monkeypatch.setattr(app_module.requests, "post", _post)
    monkeypatch.setattr(app_module, "_smtp_config", lambda: _cfg())
    return calls


@pytest.mark.parametrize(
    "provider,key,expected_host",
    [
        ("sendgrid", "SG.k", "api.sendgrid.com"),
        ("brevo", "xkeysib-k", "api.brevo.com"),
        ("resend", "re_k", "api.resend.com"),
    ],
)
def test_each_provider_is_called_on_its_own_endpoint(
    provider: str, key: str, expected_host: str, posted, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MAIL_API_PROVIDER", provider)
    monkeypatch.setenv("MAIL_API_KEY", key)
    app_module._send_email(to_addr="p@exemple.fr", subject="S", body="B", html_body="<p>B</p>")
    assert len(posted) == 1
    assert expected_host in posted[0]["url"]
    assert key in str(posted[0]["headers"])


@pytest.mark.parametrize("provider,key", [("brevo", "k"), ("resend", "k"), ("sendgrid", "k")])
def test_every_provider_carries_the_sender_the_recipient_and_both_bodies(
    provider: str, key: str, posted, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MAIL_API_PROVIDER", provider)
    monkeypatch.setenv("MAIL_API_KEY", key)
    app_module._send_email(
        to_addr="p@exemple.fr", subject="Sujet", body="texte brut", html_body="<p>riche</p>"
    )
    payload = str(posted[0]["json"])
    for needed in ("p@exemple.fr", "contact@noyaru.com", "Sujet", "texte brut", "<p>riche</p>"):
        assert needed in payload, f"{provider}: {needed!r} absent du corps de requête"


def test_a_refusal_reports_the_providers_own_words(monkeypatch: pytest.MonkeyPatch) -> None:
    # "Maximum credits exceeded" was the sentence that ended a day of guessing; a status code
    # alone would not have.
    class _Resp:
        status_code = 401
        text = '{"errors":[{"message":"Maximum credits exceeded"}]}'

    printed: list[str] = []
    monkeypatch.setattr(app_module.requests, "post", lambda *a, **k: _Resp())
    monkeypatch.setattr("builtins.print", lambda *a, **k: printed.append(" ".join(str(x) for x in a)))
    monkeypatch.setattr(app_module, "_smtp_config", lambda: _cfg())

    with pytest.raises(RuntimeError, match="sendgrid_api_http_401"):
        app_module._send_email(to_addr="p@exemple.fr", subject="S", body="B")

    assert any("Maximum credits exceeded" in line for line in printed)


def test_an_unknown_explicit_provider_fails_loudly_instead_of_silently_using_smtp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Silently falling back would send over a port that may be blocked, and the operator would
    # be debugging the wrong layer.
    monkeypatch.setenv("MAIL_API_PROVIDER", "mailchimp")
    monkeypatch.setenv("MAIL_API_KEY", "k")
    monkeypatch.setattr(app_module, "_smtp_config", lambda: _cfg())
    with pytest.raises(RuntimeError, match="mail_api_provider_unknown_mailchimp"):
        app_module._send_email(to_addr="p@exemple.fr", subject="S", body="B")
