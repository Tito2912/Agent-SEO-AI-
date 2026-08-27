"""Where the browser is allowed to send a form — the directive that silently broke checkout.

"Choisir Pro" did nothing. The audit log said `billing.checkout | ok`: the server had created
the Stripe session and answered 303 to checkout.stripe.com. Chrome and Safari enforce
`form-action` across the REDIRECT that follows a form POST, and the policy said `'self'`, so
the browser refused the hand-off and stayed put.

That failure mode is the reason these tests exist: it leaves NO server-side trace. Everything
logs success, and only a console warning in the visitor's browser says otherwise. The same
directive blocked every other hand-off this app makes, including "Continuer avec Google" on
the signup page — the first button a new customer sees.

These pin the two halves that matter: every host the app 303s a form POST to is allowed, and
nothing else is.
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

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-csp-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402


class _Request:
    """The two attributes _content_security_policy actually reads."""

    url = type("U", (), {"scheme": "https"})()
    headers: dict[str, str] = {}


def _form_action() -> str:
    policy = app_module._content_security_policy(_Request())
    directive = [d for d in policy.split(";") if d.strip().startswith("form-action")]
    assert directive, f"no form-action directive at all in: {policy}"
    return directive[0].strip()


# Every host the app 303s a form POST to, and why it must be reachable.
HANDOFFS = [
    ("https://checkout.stripe.com", "subscribing — the only path that takes money"),
    ("https://billing.stripe.com", "the billing portal"),
    ("https://accounts.google.com", "'Continuer avec Google' on signup and login"),
    ("https://github.com", "connecting the repository the corrector edits"),
    ("https://app.netlify.com", "connecting Netlify"),
    ("https://www.bing.com", "connecting Bing Webmaster"),
]


@pytest.mark.parametrize("host,why", HANDOFFS, ids=[h.split("//")[1] for h, _ in HANDOFFS])
def test_a_hand_off_the_app_performs_is_allowed(host: str, why: str) -> None:
    assert host in _form_action(), f"{why} would be blocked by the browser with no server-side trace"


def test_self_is_still_allowed() -> None:
    assert "'self'" in _form_action(), "ordinary forms would stop working"


def test_no_wildcard_crept_in() -> None:
    # Fixing this by allowing everything would trade a broken checkout for a phishing vector:
    # form-action is what stops injected markup posting credentials off-site.
    directive = _form_action()
    assert "*" not in directive
    assert "'unsafe-inline'" not in directive
    for host in re.findall(r"https://\S+", directive):
        assert host.count("/") == 2, f"{host} is not a bare origin"


def test_an_operator_override_still_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    # SEO_AGENT_CSP replaces the whole policy; someone tightening it must not be overruled.
    monkeypatch.setenv("SEO_AGENT_CSP", "default-src 'self'; form-action 'self'")
    assert _form_action() == "form-action 'self'"


# --- the env reader this test uncovered ---------------------------------------------------

@pytest.mark.parametrize(
    "stored,expected",
    [
        ('"Vérifie ton email"', "Vérifie ton email"),   # a wrapped value is unwrapped
        ("'wrapped'", "wrapped"),
        ("  spaced  ", "spaced"),
        ("plain", "plain"),
        # The bug: .strip("'") eats a trailing quote that is part of the VALUE.
        ("default-src 'self'; form-action 'self'", "default-src 'self'; form-action 'self'"),
        ("secret-ending-in-quote'", "secret-ending-in-quote'"),
        ("'unbalanced", "'unbalanced"),
        ('"', '"'),
    ],
)
def test_only_a_matched_pair_of_quotes_is_removed(
    stored: str, expected: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Every secret in this app is read through _safe_env: an API key or SMTP password ending in
    # a quote used to come back truncated, with no error anywhere.
    monkeypatch.setenv("SEO_AGENT_TEST_VALUE", stored)
    assert app_module._safe_env("SEO_AGENT_TEST_VALUE") == expected


def test_a_missing_variable_is_an_empty_string(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SEO_AGENT_TEST_VALUE", raising=False)
    assert app_module._safe_env("SEO_AGENT_TEST_VALUE") == ""
