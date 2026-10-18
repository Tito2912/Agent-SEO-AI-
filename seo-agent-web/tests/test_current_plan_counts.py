"""Live plan stock reflects actual access, one customer account at a time."""
from __future__ import annotations

import json
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone

import pytest
from bs4 import BeautifulSoup
from sqlalchemy import event, select

from backend import billing, plan_counts
from backend.models import AccountActivation, AccountMember, BillingCustomer, BillingSubscription, User
from test_customer_activation import client, lab as lab, project, user


def subscription(database, owner, plan='pro', status='active', *, updated=None, created=None, **values):
    with database.session() as db:
        customer = db.scalar(select(BillingCustomer).where(BillingCustomer.user_id == owner.id))
        if customer is None:
            customer = BillingCustomer(user_id=owner.id, stripe_customer_id='cus_' + uuid.uuid4().hex)
            db.add(customer)
        row = BillingSubscription(user_id=owner.id, stripe_customer_id=customer.stripe_customer_id,
                                  stripe_subscription_id='sub_' + uuid.uuid4().hex, stripe_price_id='price_' + plan,
                                  plan_key=plan, status=status, updated_at=updated or datetime.now(timezone.utc),
                                  created_at=created or datetime.now(timezone.utc), **values)
        db.add(row)
        db.commit()
        return row


def counts(database):
    result = plan_counts.snapshot(database)
    return {row['key']: row['count'] for row in result['plans']}


def test_existing_free_accounts_are_counted_without_requiring_new_activation_tracking(lab):
    owner = user(lab['db'])
    with lab['db'].session() as db:
        db.delete(db.get(AccountActivation, owner.id))
        db.commit()
    assert counts(lab['db']) == {'free': 1, 'solo': 0, 'pro': 0, 'business': 0}
    assert plan_counts.snapshot(lab['db'])['total'] == 1


@pytest.mark.parametrize('status,expected', [('active', 'pro'), ('trialing', 'pro'), ('past_due', 'free'),
                                           ('canceled', 'free'), ('unpaid', 'free'), ('incomplete', 'free'),
                                           ('incomplete_expired', 'free'), ('paused', 'free')])
def test_stock_agrees_with_the_effective_access_status(lab, status, expected):
    owner = user(lab['db'])
    subscription(lab['db'], owner, status=status)
    with lab['db'].session() as db:
        assert billing.effective_plan_key(db, user_id=owner.id) == expected
    assert counts(lab['db'])[expected] == 1
    record = next(row for row in plan_counts.snapshot(lab['db'])['plans'] if row['key'] == expected)
    assert record['trialing'] == int(status == 'trialing')
    assert record['active'] == int(status == 'active')


def test_each_plan_counts_unique_accounts_and_distinguishes_trials(lab):
    for plan in ['solo', 'pro', 'business']:
        subscription(lab['db'], user(lab['db']), plan, 'active')
        subscription(lab['db'], user(lab['db']), plan, 'trialing')
    user(lab['db'])
    result = plan_counts.snapshot(lab['db'])
    assert result['total'] == 7
    assert {row['key']: row['count'] for row in result['plans']} == {'free': 1, 'solo': 2, 'pro': 2, 'business': 2}
    assert all(row['active'] == row['trialing'] == 1 for row in result['plans'] if row['key'] != 'free')


def test_subscription_history_does_not_duplicate_accounts_or_hide_an_active_subscription(lab):
    owner = user(lab['db'])
    now = datetime.now(timezone.utc)
    subscription(lab['db'], owner, 'pro', updated=now - timedelta(days=1))
    subscription(lab['db'], owner, 'business', 'canceled', updated=now)
    assert counts(lab['db'])['pro'] == 1 and plan_counts.snapshot(lab['db'])['total'] == 1
    subscription(lab['db'], owner, 'solo', 'active', updated=now + timedelta(seconds=1))
    assert counts(lab['db'])['solo'] == 1 and counts(lab['db'])['pro'] == 0


def test_equal_timestamps_use_the_same_deterministic_subscription_for_access_and_stock(lab):
    owner = user(lab['db'])
    now = datetime.now(timezone.utc)
    subscription(lab['db'], owner, 'pro', updated=now, created=now - timedelta(days=2))
    subscription(lab['db'], owner, 'business', updated=now, created=now - timedelta(days=1))
    with lab['db'].session() as db:
        assert billing.effective_plan_key(db, user_id=owner.id) == 'business'
    assert counts(lab['db'])['business'] == 1


def test_scheduled_cancellation_keeps_the_plan_until_access_changes(lab):
    owner = user(lab['db'])
    subscription(lab['db'], owner, cancel_at_period_end=True,
                 current_period_end=datetime.now(timezone.utc) - timedelta(minutes=1))
    assert counts(lab['db'])['pro'] == 1


def test_price_fallback_uses_the_same_resolution_as_the_account(lab, monkeypatch):
    monkeypatch.setenv('STRIPE_PRICE_ID_PRO', 'price_migrated')
    owner = user(lab['db'])
    row = subscription(lab['db'], owner, 'unknown')
    with lab['db'].session() as db:
        db.get(BillingSubscription, row.id).stripe_price_id = 'price_migrated'
        db.commit()
    assert counts(lab['db'])['pro'] == 1
    with lab['db'].session() as db:
        db.get(BillingSubscription, row.id).stripe_price_id = 'unmapped'
        db.commit()
    assert counts(lab['db'])['free'] == 1


def test_team_only_members_and_admins_are_excluded_but_personal_accounts_remain(lab):
    database = lab['db']
    host = user(database)
    subscription(database, host, 'business')
    members = [user(database) for _ in range(4)]
    with database.session() as db:
        db.add_all(AccountMember(owner_user_id=host.id, member_user_id=member.id) for member in members)
        db.commit()
    project(database, members[1])
    subscription(database, members[2], 'solo')
    subscription(database, members[3], 'pro', 'canceled')
    subscription(database, lab['admin'], 'business')
    assert counts(database) == {'free': 2, 'solo': 1, 'pro': 0, 'business': 1}


def test_customer_deleted_is_immediately_removed_from_stock(lab):
    owner = user(lab['db'])
    subscription(lab['db'], owner, 'solo')
    assert counts(lab['db'])['solo'] == 1
    with lab['db'].session() as db:
        db.delete(db.get(User, owner.id))
        db.commit()
    assert plan_counts.snapshot(lab['db'])['total'] == 0


def test_real_subscription_events_change_the_next_reading_without_a_stripe_query(lab, monkeypatch):
    owner = user(lab['db'])
    row = subscription(lab['db'], owner, 'pro')
    monkeypatch.setenv('STRIPE_PRICE_ID_BUSINESS', 'price_business')
    c = client(lab['admin'])
    assert c.get('/api/settings/activation/plans').json()['plans'][2]['count'] == 1
    with lab['db'].session() as db:
        billing.handle_stripe_event(db, event={'type': 'customer.subscription.updated', 'data': {'object': {
            'id': row.stripe_subscription_id, 'customer': row.stripe_customer_id, 'status': 'active',
            'items': {'data': [{'price': {'id': 'price_business'}}]},
        }}})
    assert next(plan for plan in c.get('/api/settings/activation/plans').json()['plans'] if plan['key'] == 'business')['count'] == 1
    with lab['db'].session() as db:
        billing.handle_stripe_event(db, event={'type': 'customer.subscription.deleted', 'data': {'object': {
            'id': row.stripe_subscription_id, 'customer': row.stripe_customer_id, 'status': 'canceled',
            'items': {'data': [{'price': {'id': 'price_business'}}]},
        }}})
    assert counts(lab['db'])['free'] == 1


def test_live_endpoint_is_owner_only_uncached_aggregate_and_ignores_cohort_dates(lab):
    owner = user(lab['db'])
    subscription(lab['db'], owner, 'solo')
    other_admin = user(lab['db'], admin=True)
    for outsider in [owner, other_admin]:
        assert client(outsider).get('/api/settings/activation/plans').status_code == 403
    c = client(lab['admin'])
    response = c.get('/api/settings/activation/plans?date_from=2020-01-01&date_to=2020-01-02')
    assert response.status_code == 200 and response.headers['cache-control'] == 'no-store'
    data = response.json()
    assert data['total'] == 1 and data['refresh_interval_s'] == 10
    for private in [owner.id, owner.email, 'stripe_customer_id', 'stripe_data', 'price_solo']:
        assert private not in response.text
    page = c.get('/settings/activation?date_from=2020-01-01&date_to=2020-01-02')
    soup = BeautifulSoup(page.text, 'html.parser')
    assert soup.select_one('[data-plan-key=solo] [data-plan-count]').text == '1'
    assert soup.select_one('[data-plan-key=free] [data-plan-count]').text == '0'


def test_failed_reading_returns_an_error_and_initial_page_has_unknown_counts(lab, monkeypatch):
    def unavailable(*args, **kwargs):
        raise RuntimeError('database issue with private details')
    monkeypatch.setattr(plan_counts, 'snapshot', unavailable)
    c = client(lab['admin'])
    response = c.get('/api/settings/activation/plans')
    assert response.status_code == 503 and 'private details' not in response.text
    page = c.get('/settings/activation')
    assert page.status_code == 200
    soup = BeautifulSoup(page.text, 'html.parser')
    assert [node.text for node in soup.select('[data-plan-count]')] == ['—'] * 4


def test_plan_stock_reads_all_accounts_in_one_query_and_never_calls_stripe(lab, monkeypatch):
    database = lab['db']
    owners = [user(database) for _ in range(30)]
    subscription(database, owners[0], 'solo')
    subscription(database, owners[1], 'business')
    expected = Counter()
    with database.session() as db:
        for owner in owners:
            expected[billing.effective_plan_key(db, user_id=owner.id)] += 1
    monkeypatch.setattr(billing, 'stripe_init', lambda: (_ for _ in ()).throw(AssertionError('no Stripe refresh here')))
    queries = []

    def observed(connection, cursor, statement, parameters, context, many):
        queries.append(statement)

    event.listen(database.engine, 'before_cursor_execute', observed)
    try:
        result = plan_counts.snapshot(database)
    finally:
        event.remove(database.engine, 'before_cursor_execute', observed)
    assert len(queries) == 1
    assert {row['key']: row['count'] for row in result['plans']} == {key: expected[key] for key in billing.plan_catalog()}
    assert result['total'] == sum(row['count'] for row in result['plans']) == 30
    assert 'email' not in json.dumps(result)
