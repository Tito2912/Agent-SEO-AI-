"""Current customer accounts by their own effective plan, read from one database snapshot."""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from sqlalchemy import case, exists, func, or_, select

try:
    from . import billing
    from .models import AccountMember, BillingCustomer, BillingSubscription, Project, User
except ImportError:
    import billing
    from models import AccountMember, BillingCustomer, BillingSubscription, Project, User

REFRESH_SECONDS = 10


def account_query(*columns):
    """Same eligible accounts and subscription precedence for counters, details and exports."""
    ranked = select(BillingSubscription.user_id, BillingSubscription.plan_key,
                    BillingSubscription.status, BillingSubscription.stripe_price_id,
                    BillingSubscription.cancel_at_period_end, BillingSubscription.current_period_end,
                    func.row_number().over(partition_by=BillingSubscription.user_id, order_by=(
                        case((BillingSubscription.status.in_(sorted(billing.ACTIVE_SUB_STATUSES)), 0), else_=1),
                        *billing.subscription_order())).label('priority')).subquery()
    member = exists(select(AccountMember.id).where(AccountMember.member_user_id == User.id))
    own_project = exists(select(Project.id).where(Project.owner_user_id == User.id))
    own_customer = exists(select(BillingCustomer.id).where(BillingCustomer.user_id == User.id))
    return select(*columns, ranked.c.plan_key, ranked.c.status, ranked.c.stripe_price_id,
                  ranked.c.cancel_at_period_end, ranked.c.current_period_end).select_from(User).outerjoin(
        ranked, (ranked.c.user_id == User.id) & (ranked.c.priority == 1)).where(
        User.is_admin.is_(False), or_(~member, own_project, own_customer, ranked.c.user_id.is_not(None)))


def snapshot(database):
    catalog = billing.plan_catalog()
    plans = {key: {'key': key, 'label': str(value.get('label') or key), 'count': 0, 'active': 0, 'trialing': 0}
             for key, value in catalog.items()}
    query = account_query()
    with database.session() as db:
        rows = db.execute(query).all()
    for row in rows:
        key = billing.plan_key_for_subscription(SimpleNamespace(**row._mapping))
        plan = plans[key]
        plan['count'] += 1
        status = str(row.status or '').strip().lower()
        if key != 'free' and status in billing.ACTIVE_SUB_STATUSES:
            plan[status] += 1
    now = datetime.now(timezone.utc)
    return {'plans': list(plans.values()), 'total': len(rows), 'generated_at': now.isoformat(),
            'generated_at_label': now.astimezone(ZoneInfo('Europe/Paris')).strftime('%d/%m/%Y à %H:%M:%S'),
            'refresh_interval_s': REFRESH_SECONDS}
