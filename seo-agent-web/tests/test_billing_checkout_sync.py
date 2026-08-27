"""Turning a completed Stripe payment into an active plan, without depending on the webhook.

A real test purchase went through — `?success=1&session_id=cs_test_...` — and the account
stayed on Free. The database explained it exactly:

    customers: ['cus_V9QLFMYsk0YHPj']
    subs: []

The customer mapping was written, so the session carried the user id. But
`session.subscription` was still null: Stripe attaches it ASYNCHRONOUSLY, and the redirect
back from payment arrives first. The webhook normally fills it in moments later, so this only
becomes permanent when the webhook is missing or its signing secret does not match — and then
the customer has paid, sees "Free", and nothing anywhere records a failure.

Reconciliation now asks Stripe for the customer's subscriptions instead of giving up.
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

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-billing-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))

from backend import billing  # noqa: E402

CUSTOMER = "cus_V9QLFMYsk0YHPj"


@pytest.fixture()
def stripe_listing(monkeypatch: pytest.MonkeyPatch):
    """Control what Stripe reports for a customer's subscriptions."""
    state: dict = {"rows": [], "calls": []}

    class _Subscription:
        @staticmethod
        def list(**kwargs):
            state["calls"].append(kwargs)
            return {"data": state["rows"]}

    monkeypatch.setattr(billing, "stripe", type("S", (), {"Subscription": _Subscription}))
    return state


def test_the_newest_active_subscription_is_chosen(stripe_listing) -> None:
    stripe_listing["rows"] = [
        {"id": "sub_ancien", "status": "canceled", "created": 100},
        {"id": "sub_actif", "status": "active", "created": 200},
        {"id": "sub_encore_plus_vieux", "status": "active", "created": 50},
    ]
    assert billing._latest_subscription_id_for_customer(CUSTOMER) == "sub_actif"


def test_a_subscription_still_settling_is_accepted(stripe_listing) -> None:
    # For a few seconds after payment a subscription can be `incomplete`. Returning nothing in
    # that window is precisely what leaves a paying customer on Free.
    stripe_listing["rows"] = [{"id": "sub_neuf", "status": "incomplete", "created": 300}]
    assert billing._latest_subscription_id_for_customer(CUSTOMER) == "sub_neuf"


def test_every_status_is_requested(stripe_listing) -> None:
    stripe_listing["rows"] = [{"id": "sub_x", "status": "active", "created": 1}]
    billing._latest_subscription_id_for_customer(CUSTOMER)
    assert stripe_listing["calls"][0].get("status") == "all", (
        "filtering to active-only reintroduces the settling window this exists to cover"
    )
    assert stripe_listing["calls"][0].get("customer") == CUSTOMER


def test_a_customer_with_no_subscription_yields_nothing(stripe_listing) -> None:
    stripe_listing["rows"] = []
    assert billing._latest_subscription_id_for_customer(CUSTOMER) == ""


def test_a_stripe_outage_does_not_raise_into_the_billing_page(monkeypatch: pytest.MonkeyPatch) -> None:
    # This runs while rendering /billing. An exception here would replace the page a paying
    # customer just landed on with an error.
    class _Boom:
        class Subscription:
            @staticmethod
            def list(**_kw):
                raise RuntimeError("stripe down")

    monkeypatch.setattr(billing, "stripe", _Boom)
    assert billing._latest_subscription_id_for_customer(CUSTOMER) == ""


@pytest.mark.parametrize("customer_id", ["", "   ", None])
def test_no_customer_means_no_lookup(customer_id, stripe_listing) -> None:
    assert billing._latest_subscription_id_for_customer(customer_id) == ""
    assert stripe_listing["calls"] == [], "Stripe was called without a customer to ask about"


# --- recovering without a session id --------------------------------------------------------

def test_a_paid_customer_can_be_reconciled_from_the_stored_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The state a real purchase left behind: customer mapped, no subscription, no session id.

    The session-based sync needs a `session_id` that only exists on the redirect back from
    payment. Close that tab and there was no way back — the account had paid and stayed on
    Free permanently. Everything needed was already in our own database.
    """
    calls: list[str] = []

    monkeypatch.setattr(billing, "stripe_init", lambda: None)
    monkeypatch.setattr(billing, "stripe_enabled", lambda: True)
    monkeypatch.setattr(billing, "stripe_customer_id", lambda db, *, user_id: CUSTOMER)
    monkeypatch.setattr(billing, "_latest_subscription_id_for_customer", lambda cid: "sub_retrouve")

    def _sync(db, *, stripe_subscription_id):
        calls.append(stripe_subscription_id)
        return object()

    monkeypatch.setattr(billing, "sync_subscription_from_stripe", _sync)

    assert billing.sync_subscription_from_customer(None, user_id="u1") is not None
    assert calls == ["sub_retrouve"]


def test_an_account_that_never_paid_is_left_alone(monkeypatch: pytest.MonkeyPatch) -> None:
    # No Stripe customer means nothing to reconcile; this must not call Stripe on every page
    # view for every free account.
    called: list[str] = []
    monkeypatch.setattr(billing, "stripe_init", lambda: None)
    monkeypatch.setattr(billing, "stripe_enabled", lambda: True)
    monkeypatch.setattr(billing, "stripe_customer_id", lambda db, *, user_id: "")
    monkeypatch.setattr(
        billing, "_latest_subscription_id_for_customer", lambda cid: called.append(cid) or ""
    )
    assert billing.sync_subscription_from_customer(None, user_id="u1") is None
    assert called == []


def test_nothing_happens_when_stripe_is_not_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(billing, "stripe_init", lambda: None)
    monkeypatch.setattr(billing, "stripe_enabled", lambda: False)
    assert billing.sync_subscription_from_customer(None, user_id="u1") is None
