"""A scheduled downgrade must be visible, and must not be requestable twice.

After asking for a Pro downgrade from Business, /billing looked exactly as it had before: the
page still read "Business · actif" with Business quotas, and every plan button stayed live.
Both halves were true — the customer keeps Business until the period they paid for ends — but
nothing on the page said a change was already booked, or when it would happen. The customer's
only options were to assume it had failed and click again, or to contact support.

Nothing records the pending change locally: it lives in a Stripe `subscription_schedule`, and
the stored subscription object carries only its id. `pending_plan_change` therefore reads Stripe,
but only when that id is present, so an account with no scheduled change costs no API call.

These render the real page through the real handler for a non-admin account.
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
from stripe import StripeObject

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-sched-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as app_module  # noqa: E402
from backend import auth, billing  # noqa: E402
from backend.app import app  # noqa: E402
from backend.models import BillingSubscription, User  # noqa: E402

BUSINESS_PRICE = "price_business"
PRO_PRICE = "price_pro"
SCHEDULE_ID = "sub_sched_test"
EFFECTIVE = datetime(2026, 9, 27, 18, 6, tzinfo=UTC)


@pytest.fixture(autouse=True)
def stripe_prices(monkeypatch):
    monkeypatch.setattr(billing, "stripe_enabled", lambda: True)
    monkeypatch.setattr(billing, "stripe_init", lambda: None)
    monkeypatch.setattr(billing, "list_invoices", lambda db, *, user_id, **_k: [])
    monkeypatch.setattr(
        billing, "price_id_for_plan",
        lambda k: {"business": BUSINESS_PRICE, "pro": PRO_PRICE, "solo": "price_solo"}.get(k, ""),
    )
    monkeypatch.setattr(
        billing, "plan_for_price_id",
        lambda p: {BUSINESS_PRICE: "business", PRO_PRICE: "pro", "price_solo": "solo"}.get(p, ""),
    )


def _customer(*, schedule_id: str | None):
    """A non-admin on Business, optionally with a downgrade already scheduled."""
    app_module.DB.create_tables()
    tag = uuid.uuid4().hex[:8]
    with app_module.DB.session() as db:
        user = User(
            email=f"client-{tag}@exemple.fr",
            password_hash=auth.hash_password("x" * 12),
            is_admin=False,
        )
        db.add(user)
        db.commit()
        db.refresh(user)
        uid = str(user.id)
        data = {"id": f"sub_{tag}", "status": "active"}
        if schedule_id:
            data["schedule"] = schedule_id
        db.add(BillingSubscription(
            user_id=uid,
            stripe_customer_id=f"cus_{tag}",
            stripe_subscription_id=f"sub_{tag}",
            stripe_price_id=BUSINESS_PRICE,
            plan_key="business",
            status="active",
            stripe_data=data,
        ))
        db.commit()
    client = TestClient(app)
    client.cookies.set(
        auth.SESSION_COOKIE_NAME,
        auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]),
    )
    return client


def _card(body: str, label: str) -> str:
    """The rendered card for one plan.

    Sliced on the <h2>, not on the label: the banner also names the target plan in a <strong>,
    and slicing on that silently returned a different card — the first version of this test
    passed while reading the Solo button.
    """
    head = f"<h2>{label}</h2>"
    assert head in body, f"no card rendered for {label}"
    return body.split(head, 1)[1].split("</form>", 1)[0]


def _install_schedule(monkeypatch, *, status: str = "active", raises: bool = False):
    calls: list[str] = []

    def _retrieve(schedule_id, **_kw):
        calls.append(schedule_id)
        if raises:
            raise RuntimeError("stripe is down")
        return StripeObject.construct_from({
            "id": schedule_id,
            "status": status,
            "phases": [
                {"start_date": 1756317960, "items": [{"price": BUSINESS_PRICE}]},
                {"start_date": int(EFFECTIVE.timestamp()), "items": [{"price": PRO_PRICE}]},
            ],
        }, "sk_test_x")

    monkeypatch.setattr(billing.stripe.SubscriptionSchedule, "retrieve", _retrieve)
    return calls


def test_the_page_says_a_downgrade_is_booked_and_when(monkeypatch) -> None:
    _install_schedule(monkeypatch)
    body = _customer(schedule_id=SCHEDULE_ID).get("/billing").text

    assert "Changement de plan déjà demandé" in body, (
        "the page gave no sign that a downgrade had been requested"
    )
    assert "27/09/2026" in body, "the customer cannot tell when the change takes effect"
    assert "Pro" in body


def test_the_plan_already_scheduled_cannot_be_requested_again(monkeypatch) -> None:
    _install_schedule(monkeypatch)
    body = _customer(schedule_id=SCHEDULE_ID).get("/billing").text

    # The Pro card must be inert, and say why rather than just look dead.
    pro_card = _card(body, "Pro")
    assert "disabled" in pro_card, "the customer could book the same downgrade a second time"
    assert "Déjà programmé" in pro_card


def test_the_other_plans_stay_available(monkeypatch) -> None:
    """Greying everything would trap the customer.

    Re-upgrading is the only way out of a scheduled downgrade — `change_plan_now` releases the
    schedule — so those buttons must keep working.
    """
    _install_schedule(monkeypatch)
    body = _customer(schedule_id=SCHEDULE_ID).get("/billing").text
    solo_card = _card(body, "Solo")

    assert "disabled" not in solo_card, "a customer who changed their mind had no way out"
    assert "Downgrade fin de période" in solo_card


def test_an_account_with_no_scheduled_change_never_calls_stripe(monkeypatch) -> None:
    # The ordinary case. A billing page that costs an API call per view is a page that breaks
    # when Stripe is slow.
    calls = _install_schedule(monkeypatch)
    body = _customer(schedule_id=None).get("/billing").text

    assert calls == [], "the page queried Stripe for a schedule that cannot exist"
    assert "Changement de plan déjà demandé" not in body


def test_a_released_schedule_is_not_reported(monkeypatch) -> None:
    # A schedule the customer already escaped (by re-upgrading) must stop showing.
    _install_schedule(monkeypatch, status="released")
    body = _customer(schedule_id=SCHEDULE_ID).get("/billing").text
    assert "Changement de plan déjà demandé" not in body


def test_an_unreadable_schedule_still_warns_and_never_500s(monkeypatch) -> None:
    """Better a vague warning than a page inviting the same request twice — or no page at all."""
    _install_schedule(monkeypatch, raises=True)
    response = _customer(schedule_id=SCHEDULE_ID).get("/billing")

    assert response.status_code == 200, "a Stripe outage took the billing page down"
    assert "Un changement de plan est déjà demandé" in response.text
