"""An upgrade must collect the difference before it grants the plan.

`change_plan_now` used `proration_behavior="create_prorations"`, which switches the subscription
immediately and parks the money on the NEXT invoice. Walking it as the customer
(2026-08-28) produced a Pro→Business upgrade with no payment and no invoice — two pending
proration items (-96.19 / +193.34 EUR) due four weeks later. The owner could not tell why the
account was on Business; a customer would open a dispute. It also granted the higher plan
against a card last validated a month earlier.

Charging on the spot introduces the failure this file exists for: an immediate charge can be
refused, or need 3-D Secure it cannot obtain off-session. Stripe then leaves the subscription
`past_due`, `effective_plan_key` reads anything outside {active, trialing} as `free`, and the
customer would lose the plan they were ALREADY paying for merely for trying to upgrade —
strictly worse than deferring. So a refused upgrade must roll back to the previous price.

Objects are built with the INSTALLED SDK, the convention from test_stripe_object_reading.py: a
version bump that changes the response shape fails here rather than in production.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pytest
from stripe import StripeObject

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-upgrade-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))

from backend import billing  # noqa: E402

SUB_ID = "sub_test"
ITEM_ID = "si_test"
PRO_PRICE = "price_pro"
BUSINESS_PRICE = "price_business"
UID = "user-1"


def _obj(payload: dict) -> StripeObject:
    return StripeObject.construct_from(payload, "sk_test_x")


def _subscription(price_id: str, plan_key: str, *, latest_invoice: str = "in_test") -> dict:
    return {
        "id": SUB_ID,
        "status": "active",
        "customer": "cus_test",
        "latest_invoice": latest_invoice,
        "metadata": {"user_id": UID, "plan_key": plan_key},
        "items": {"data": [{"id": ITEM_ID, "price": {"id": price_id}}]},
    }


class _Recorder:
    """Stands in for the Stripe SDK and records what the code asked it to do."""

    def __init__(self, invoice: dict | None) -> None:
        self.invoice = invoice
        self.modifies: list[dict] = []
        self.voided: list[str] = []

    # --- stripe.Subscription ---
    def retrieve(self, sub_id, **_kw):
        return _obj(_subscription(PRO_PRICE, "pro"))

    def modify(self, sub_id, **kwargs):
        self.modifies.append(kwargs)
        price = kwargs["items"][0]["price"]
        plan = "business" if price == BUSINESS_PRICE else "pro"
        return _obj(_subscription(price, plan))

    # --- stripe.Invoice ---
    def invoice_retrieve(self, invoice_id, **_kw):
        if self.invoice is None:
            raise RuntimeError("stripe is down")
        return _obj(self.invoice)

    def void_invoice(self, invoice_id, **_kw):
        self.voided.append(invoice_id)
        return _obj({"id": invoice_id, "status": "void"})


@pytest.fixture()
def stripe_that(monkeypatch):
    """Install a recorder in place of the SDK and neutralise the DB writes."""

    def _install(invoice: dict | None):
        rec = _Recorder(invoice)
        monkeypatch.setattr(billing, "stripe_init", lambda: None)
        monkeypatch.setattr(billing, "stripe_enabled", lambda: True)
        monkeypatch.setattr(billing, "price_id_for_plan", lambda k: BUSINESS_PRICE if k == "business" else PRO_PRICE)
        monkeypatch.setattr(billing, "plan_for_price_id", lambda p: "business" if p == BUSINESS_PRICE else "pro")
        monkeypatch.setattr(billing, "_release_schedule_if_any", lambda *_a, **_k: None)
        monkeypatch.setattr(
            billing, "subscription_for_user",
            lambda db, *, user_id: type("S", (), {"status": "active", "stripe_subscription_id": SUB_ID})(),
        )
        monkeypatch.setattr(billing, "upsert_subscription", lambda db, *, stripe_subscription: stripe_subscription)
        monkeypatch.setattr(billing, "sync_subscription_from_stripe", lambda db, *, stripe_subscription_id: None)
        monkeypatch.setattr(billing.stripe.Subscription, "retrieve", rec.retrieve)
        monkeypatch.setattr(billing.stripe.Subscription, "modify", rec.modify)
        monkeypatch.setattr(billing.stripe.Invoice, "retrieve", rec.invoice_retrieve)
        monkeypatch.setattr(billing.stripe.Invoice, "void_invoice", rec.void_invoice)
        return rec

    return _install


def test_the_upgrade_bills_the_difference_immediately(stripe_that) -> None:
    rec = stripe_that({"id": "in_test", "status": "paid", "amount_remaining": 0, "paid": True})

    billing.change_plan_now(None, user_id=UID, target_plan_key="business")

    assert len(rec.modifies) == 1, "the upgrade should be a single subscription update"
    assert rec.modifies[0]["proration_behavior"] == "always_invoice", (
        "'create_prorations' defers the money to the next invoice: the customer gets the plan "
        "now and the charge four weeks later, with no invoice explaining the change"
    )
    assert rec.modifies[0]["items"][0]["price"] == BUSINESS_PRICE


def test_a_refused_card_leaves_the_customer_on_the_plan_they_were_paying_for(stripe_that) -> None:
    rec = stripe_that({"id": "in_test", "status": "open", "amount_remaining": 9715, "paid": False})

    with pytest.raises(billing.UpgradePaymentFailed):
        billing.change_plan_now(None, user_id=UID, target_plan_key="business")

    assert len(rec.modifies) == 2, "the failed upgrade was not rolled back"
    rollback = rec.modifies[1]
    assert rollback["items"][0]["price"] == PRO_PRICE, (
        "the customer was left on Business without paying, or dropped below their previous plan"
    )
    assert rollback["proration_behavior"] == "none", (
        "the rollback billed something of its own"
    )
    assert rollback["metadata"]["plan_key"] == "pro", "the rollback left the wrong plan in metadata"


def test_the_failed_invoice_does_not_stay_in_dunning(stripe_that) -> None:
    # Otherwise Stripe keeps retrying a charge for a plan the customer never received.
    rec = stripe_that({"id": "in_test", "status": "open", "amount_remaining": 9715, "paid": False})
    with pytest.raises(billing.UpgradePaymentFailed):
        billing.change_plan_now(None, user_id=UID, target_plan_key="business")
    assert rec.voided == ["in_test"]


def test_an_unreadable_invoice_is_treated_as_unpaid(stripe_that) -> None:
    """Silence from Stripe must not read as success.

    `_stripe_to_dict` returning {} for a whole SDK major is exactly how billing went inert
    before; the safe direction is to refuse an upgrade that was paid, never to grant one
    that was not.
    """
    rec = stripe_that(None)
    with pytest.raises(billing.UpgradePaymentFailed):
        billing.change_plan_now(None, user_id=UID, target_plan_key="business")
    assert rec.modifies[1]["items"][0]["price"] == PRO_PRICE


def test_a_zero_amount_change_needs_no_payment(stripe_that) -> None:
    # Nothing to collect (credit balance covers it) must not be mistaken for a refusal.
    rec = stripe_that({"id": "in_test", "status": "open", "amount_remaining": 0, "paid": False})
    billing.change_plan_now(None, user_id=UID, target_plan_key="business")
    assert len(rec.modifies) == 1, "a zero-amount upgrade was rolled back"


def test_settlement_is_not_read_from_payment_intent() -> None:
    """`invoice.payment_intent` was removed from the API in 2025.

    Reading it would make the check silently wrong on the next version bump — the shape of the
    bug that kept the whole integration inert for a major.
    """
    src = (WEB_ROOT / "backend" / "billing.py").read_text(encoding="utf-8")
    fn = src.split("def _invoice_is_settled", 1)[1].split("\ndef ", 1)[0]
    # Drop the docstring: it NAMES the field precisely to explain why it is not read.
    body = fn.split('"""', 2)[-1]
    for access in ('"payment_intent"', "'payment_intent'", ".payment_intent"):
        assert access not in body, f"settlement is being read from {access}"


def test_a_past_due_subscription_is_not_entitled() -> None:
    # The premise the rollback exists for: anything outside {active, trialing} reads as free,
    # so leaving a customer past_due after a refused upgrade would strip the plan they had.
    assert "past_due" not in billing.ACTIVE_SUB_STATUSES
    assert billing.ACTIVE_SUB_STATUSES == {"active", "trialing"}
