from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

try:
    import stripe  # type: ignore
except Exception:  # pragma: no cover
    stripe = None  # type: ignore

try:
    from .models import BillingCustomer, BillingSubscription, UsageEvent  # type: ignore
except ImportError:  # pragma: no cover
    from models import BillingCustomer, BillingSubscription, UsageEvent  # type: ignore


ACTIVE_SUB_STATUSES: set[str] = {"active", "trialing"}


def _env(name: str) -> str:
    return str(os.environ.get(name) or "").strip()


def stripe_enabled() -> bool:
    return bool(_env("STRIPE_SECRET_KEY")) and stripe is not None


def stripe_init() -> None:
    if not stripe_enabled():
        return
    stripe.api_key = _env("STRIPE_SECRET_KEY")


def public_base_url() -> str:
    return _env("PUBLIC_BASE_URL").rstrip("/")


def plan_catalog() -> dict[str, dict[str, Any]]:
    # Defaults (modifiable later). For owners-first V1:
    # - "free" is intentionally small (beta/trial + invite)
    return {
        "free": {
            "label": "Free",
            "price_label": "0€",
            "limits": {"projects": 1, "pages_crawled_month": 800, "assistant_messages_month": 30},
            "features": ["Audit", "Suggestions IA (limitées)", "Exports"],
        },
        "solo": {
            "label": "Solo",
            "price_label": "49€/mois",
            "limits": {"projects": 3, "pages_crawled_month": 20_000, "assistant_messages_month": 400},
            "features": ["Audit", "Suggestions IA", "Exports PDF/CSV", "Monitoring"],
        },
        "pro": {
            "label": "Pro",
            "price_label": "99€/mois",
            "limits": {"projects": 10, "pages_crawled_month": 100_000, "assistant_messages_month": 2_000},
            "features": ["Audit", "Suggestions IA avancées", "Exports", "Monitoring + alertes"],
        },
        "business": {
            "label": "Business",
            "price_label": "199€/mois",
            "limits": {"projects": 30, "pages_crawled_month": 300_000, "assistant_messages_month": 6_000},
            "features": ["Audit", "Suggestions IA avancées", "Exports", "Monitoring + alertes"],
        },
    }


def price_id_for_plan(plan_key: str) -> str:
    k = (plan_key or "").strip().lower()
    if k == "solo":
        return _env("STRIPE_PRICE_ID_SOLO")
    if k == "pro":
        return _env("STRIPE_PRICE_ID_PRO")
    if k == "business":
        return _env("STRIPE_PRICE_ID_BUSINESS")
    return ""


def plan_for_price_id(price_id: str) -> str:
    pid = (price_id or "").strip()
    if not pid:
        return ""
    if pid == _env("STRIPE_PRICE_ID_SOLO"):
        return "solo"
    if pid == _env("STRIPE_PRICE_ID_PRO"):
        return "pro"
    if pid == _env("STRIPE_PRICE_ID_BUSINESS"):
        return "business"
    return ""


def _period_key(dt: datetime | None = None) -> str:
    now = dt or datetime.now(UTC)
    return f"{now.year:04d}-{now.month:02d}"


def usage_add(db: Session, *, user_id: str, metric: str, amount: int, meta: dict[str, Any] | None = None) -> None:
    uid = (user_id or "").strip()
    m = (metric or "").strip()
    if not uid or not m or not isinstance(amount, int) or amount == 0:
        return
    ev = UsageEvent(user_id=uid, period=_period_key(), metric=m, amount=int(amount), meta=meta or {})
    db.add(ev)
    db.commit()


def usage_sum(db: Session, *, user_id: str, metric: str, period: str | None = None) -> int:
    uid = (user_id or "").strip()
    m = (metric or "").strip()
    p = (period or "").strip() or _period_key()
    if not uid or not m:
        return 0
    total = db.scalar(
        select(func.coalesce(func.sum(UsageEvent.amount), 0)).where(
            UsageEvent.user_id == uid, UsageEvent.period == p, UsageEvent.metric == m
        )
    )
    try:
        return int(total or 0)
    except Exception:
        return 0


def _billing_customer(db: Session, *, user_id: str) -> BillingCustomer | None:
    uid = (user_id or "").strip()
    if not uid:
        return None
    return db.scalar(select(BillingCustomer).where(BillingCustomer.user_id == uid))


def stripe_customer_id(db: Session, *, user_id: str) -> str:
    row = _billing_customer(db, user_id=user_id)
    return str(row.stripe_customer_id or "").strip() if row else ""


def get_or_create_stripe_customer(db: Session, *, user_id: str, email: str) -> str:
    stripe_init()
    if not stripe_enabled():
        raise RuntimeError("stripe_not_configured")

    uid = (user_id or "").strip()
    if not uid:
        raise RuntimeError("missing_user_id")

    existing = _billing_customer(db, user_id=uid)
    if existing and str(existing.stripe_customer_id or "").strip():
        return str(existing.stripe_customer_id).strip()

    customer = stripe.Customer.create(email=(email or "").strip(), metadata={"user_id": uid})  # type: ignore[attr-defined]
    cust_id = str(getattr(customer, "id", "") or "").strip()
    if not cust_id:
        raise RuntimeError("stripe_customer_create_failed")

    row = BillingCustomer(user_id=uid, stripe_customer_id=cust_id)
    db.add(row)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        row2 = _billing_customer(db, user_id=uid)
        if row2 and str(row2.stripe_customer_id or "").strip():
            return str(row2.stripe_customer_id).strip()
        raise
    return cust_id


def upsert_customer_mapping(db: Session, *, user_id: str, stripe_customer_id: str) -> None:
    uid = (user_id or "").strip()
    cid = (stripe_customer_id or "").strip()
    if not uid or not cid:
        return
    existing = _billing_customer(db, user_id=uid)
    if existing:
        if str(existing.stripe_customer_id or "").strip() != cid:
            existing.stripe_customer_id = cid
            db.add(existing)
            db.commit()
        return
    row = BillingCustomer(user_id=uid, stripe_customer_id=cid)
    db.add(row)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()


def _user_id_for_customer(db: Session, *, stripe_customer_id: str) -> str:
    cid = (stripe_customer_id or "").strip()
    if not cid:
        return ""
    row = db.scalar(select(BillingCustomer).where(BillingCustomer.stripe_customer_id == cid))
    return str(row.user_id or "").strip() if row else ""


def subscription_for_user(db: Session, *, user_id: str) -> BillingSubscription | None:
    uid = (user_id or "").strip()
    if not uid:
        return None
    # Prefer an active subscription when present.
    sub = db.scalar(
        select(BillingSubscription)
        .where(BillingSubscription.user_id == uid, BillingSubscription.status.in_(sorted(ACTIVE_SUB_STATUSES)))
        .order_by(BillingSubscription.updated_at.desc())
    )
    if sub:
        return sub
    return db.scalar(
        select(BillingSubscription).where(BillingSubscription.user_id == uid).order_by(BillingSubscription.updated_at.desc())
    )


def effective_plan_key(db: Session, *, user_id: str) -> str:
    sub = subscription_for_user(db, user_id=user_id)
    if not sub:
        return "free"
    status = str(getattr(sub, "status", "") or "").strip().lower()
    if status not in ACTIVE_SUB_STATUSES:
        return "free"
    plan_key = str(getattr(sub, "plan_key", "") or "").strip().lower()
    if plan_key in plan_catalog():
        return plan_key
    # Fallback: derive from price id.
    derived = plan_for_price_id(str(getattr(sub, "stripe_price_id", "") or ""))
    return derived if derived in plan_catalog() else "free"


def plan_limits(db: Session, *, user_id: str) -> dict[str, int]:
    plan_key = effective_plan_key(db, user_id=user_id)
    cat = plan_catalog()
    limits = (cat.get(plan_key, {}) or {}).get("limits")
    return dict(limits) if isinstance(limits, dict) else {}


def remaining_quota(db: Session, *, user_id: str, metric: str) -> int | None:
    limits = plan_limits(db, user_id=user_id)
    m = (metric or "").strip()
    if not m:
        return None
    limit = limits.get(m)
    if not isinstance(limit, int) or limit <= 0:
        return None
    used = usage_sum(db, user_id=user_id, metric=m)
    return max(0, int(limit) - int(used))


def ensure_within_quota(db: Session, *, user_id: str, metric: str, planned_amount: int) -> tuple[bool, int | None]:
    remaining = remaining_quota(db, user_id=user_id, metric=metric)
    if remaining is None:
        return True, None
    need = max(0, int(planned_amount))
    return remaining >= need, remaining


def sync_subscription_from_stripe(db: Session, *, stripe_subscription_id: str) -> BillingSubscription | None:
    stripe_init()
    if not stripe_enabled():
        return None
    sid = (stripe_subscription_id or "").strip()
    if not sid:
        return None
    sub = stripe.Subscription.retrieve(sid)  # type: ignore[attr-defined]
    if not isinstance(sub, dict):
        try:
            sub = sub.to_dict_recursive()
        except Exception:
            sub = {}
    return upsert_subscription(db, stripe_subscription=sub)


def upsert_subscription(db: Session, *, stripe_subscription: dict[str, Any]) -> BillingSubscription | None:
    sid = str(stripe_subscription.get("id") or "").strip()
    cid = str(stripe_subscription.get("customer") or "").strip()
    if not sid or not cid:
        return None

    meta = stripe_subscription.get("metadata") if isinstance(stripe_subscription.get("metadata"), dict) else {}
    uid = str(meta.get("user_id") or "").strip() or _user_id_for_customer(db, stripe_customer_id=cid)
    if not uid:
        return None

    price_id = ""
    items = stripe_subscription.get("items") if isinstance(stripe_subscription.get("items"), dict) else {}
    data = items.get("data") if isinstance(items.get("data"), list) else []
    if data and isinstance(data[0], dict):
        price = data[0].get("price") if isinstance(data[0].get("price"), dict) else {}
        price_id = str(price.get("id") or "").strip()

    plan_key = plan_for_price_id(price_id) or str(meta.get("plan_key") or "").strip().lower()
    if plan_key not in plan_catalog():
        plan_key = "free"

    status = str(stripe_subscription.get("status") or "").strip().lower() or "unknown"
    cancel_at_period_end = bool(stripe_subscription.get("cancel_at_period_end") or False)

    def _ts_to_dt(ts: Any) -> datetime | None:
        try:
            if ts is None:
                return None
            v = int(ts)
            if v <= 0:
                return None
            return datetime.fromtimestamp(v, tz=UTC)
        except Exception:
            return None

    cps = _ts_to_dt(stripe_subscription.get("current_period_start"))
    cpe = _ts_to_dt(stripe_subscription.get("current_period_end"))
    trial_end = _ts_to_dt(stripe_subscription.get("trial_end"))

    existing = db.scalar(select(BillingSubscription).where(BillingSubscription.stripe_subscription_id == sid))
    if existing:
        existing.user_id = uid
        existing.stripe_customer_id = cid
        existing.stripe_price_id = price_id or existing.stripe_price_id
        existing.plan_key = plan_key
        existing.status = status
        existing.cancel_at_period_end = cancel_at_period_end
        existing.current_period_start = cps
        existing.current_period_end = cpe
        existing.trial_end = trial_end
        existing.stripe_data = stripe_subscription
        db.add(existing)
        db.commit()
        return existing

    row = BillingSubscription(
        user_id=uid,
        stripe_customer_id=cid,
        stripe_subscription_id=sid,
        stripe_price_id=price_id or "unknown",
        plan_key=plan_key,
        status=status,
        cancel_at_period_end=cancel_at_period_end,
        current_period_start=cps,
        current_period_end=cpe,
        trial_end=trial_end,
        stripe_data=stripe_subscription,
    )
    db.add(row)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        existing2 = db.scalar(select(BillingSubscription).where(BillingSubscription.stripe_subscription_id == sid))
        return existing2
    return row


def create_checkout_session_url(db: Session, *, user_id: str, email: str, plan_key: str) -> str:
    stripe_init()
    if not stripe_enabled():
        raise RuntimeError("stripe_not_configured")

    pk = (plan_key or "").strip().lower()
    price_id = price_id_for_plan(pk)
    if not price_id:
        raise RuntimeError("stripe_price_missing")

    base = public_base_url()
    if not base:
        raise RuntimeError("PUBLIC_BASE_URL missing")

    uid = (user_id or "").strip()
    if not uid:
        raise RuntimeError("missing_user_id")

    customer_id = get_or_create_stripe_customer(db, user_id=uid, email=email)
    upsert_customer_mapping(db, user_id=uid, stripe_customer_id=customer_id)

    session = stripe.checkout.Session.create(  # type: ignore[attr-defined]
        mode="subscription",
        customer=customer_id,
        line_items=[{"price": price_id, "quantity": 1}],
        allow_promotion_codes=True,
        success_url=f"{base}/billing?success=1&session_id={{CHECKOUT_SESSION_ID}}",
        cancel_url=f"{base}/billing?canceled=1",
        client_reference_id=uid,
        metadata={"user_id": uid, "plan_key": pk},
        subscription_data={"metadata": {"user_id": uid, "plan_key": pk}},
    )
    url = str(getattr(session, "url", "") or "").strip()
    if not url:
        raise RuntimeError("stripe_checkout_url_missing")
    return url


def create_billing_portal_url(db: Session, *, user_id: str, email: str) -> str:
    stripe_init()
    if not stripe_enabled():
        raise RuntimeError("stripe_not_configured")

    base = public_base_url()
    if not base:
        raise RuntimeError("PUBLIC_BASE_URL missing")

    uid = (user_id or "").strip()
    if not uid:
        raise RuntimeError("missing_user_id")

    customer_id = stripe_customer_id(db, user_id=uid)
    if not customer_id:
        customer_id = get_or_create_stripe_customer(db, user_id=uid, email=email)
        upsert_customer_mapping(db, user_id=uid, stripe_customer_id=customer_id)

    session = stripe.billing_portal.Session.create(  # type: ignore[attr-defined]
        customer=customer_id,
        return_url=f"{base}/billing",
    )
    url = str(getattr(session, "url", "") or "").strip()
    if not url:
        raise RuntimeError("stripe_portal_url_missing")
    return url


def sync_from_checkout_session(db: Session, *, session_id: str) -> BillingSubscription | None:
    stripe_init()
    if not stripe_enabled():
        return None
    sid = (session_id or "").strip()
    if not sid:
        return None
    sess = stripe.checkout.Session.retrieve(sid, expand=["subscription"])  # type: ignore[attr-defined]
    if not isinstance(sess, dict):
        try:
            sess = sess.to_dict_recursive()
        except Exception:
            sess = {}
    sub_id = str(sess.get("subscription") or "").strip()
    customer_id = str(sess.get("customer") or "").strip()
    meta = sess.get("metadata") if isinstance(sess.get("metadata"), dict) else {}
    uid = str(meta.get("user_id") or "").strip() or str(sess.get("client_reference_id") or "").strip()
    if uid and customer_id:
        upsert_customer_mapping(db, user_id=uid, stripe_customer_id=customer_id)
    if not sub_id:
        return None
    return sync_subscription_from_stripe(db, stripe_subscription_id=sub_id)


def construct_webhook_event(*, payload: bytes, sig_header: str) -> dict[str, Any]:
    stripe_init()
    if not stripe_enabled():
        raise RuntimeError("stripe_not_configured")
    secret = _env("STRIPE_WEBHOOK_SECRET")
    if not secret:
        raise RuntimeError("stripe_webhook_secret_missing")
    event = stripe.Webhook.construct_event(payload, sig_header, secret)  # type: ignore[attr-defined]
    if isinstance(event, dict):
        return event
    try:
        return event.to_dict_recursive()
    except Exception:
        return {}


def handle_stripe_event(db: Session, *, event: dict[str, Any]) -> None:
    etype = str(event.get("type") or "").strip()
    data = event.get("data") if isinstance(event.get("data"), dict) else {}
    obj = data.get("object") if isinstance(data.get("object"), dict) else {}
    if not etype:
        return

    if etype == "checkout.session.completed":
        customer_id = str(obj.get("customer") or "").strip()
        sub_id = str(obj.get("subscription") or "").strip()
        meta = obj.get("metadata") if isinstance(obj.get("metadata"), dict) else {}
        uid = str(meta.get("user_id") or "").strip() or str(obj.get("client_reference_id") or "").strip()
        if uid and customer_id:
            upsert_customer_mapping(db, user_id=uid, stripe_customer_id=customer_id)
        if sub_id:
            sync_subscription_from_stripe(db, stripe_subscription_id=sub_id)
        return

    if etype.startswith("customer.subscription."):
        # object is already a subscription
        upsert_subscription(db, stripe_subscription=obj)
        return
