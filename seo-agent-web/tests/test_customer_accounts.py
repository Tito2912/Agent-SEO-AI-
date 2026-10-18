"""A private inventory reconciles the plan counters without inventing recent activity."""
from __future__ import annotations

import csv
import io
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlsplit

import pytest
from bs4 import BeautifulSoup
from sqlalchemy import event, select

from backend import billing, customer_accounts, plan_counts
from backend.models import AccountActivation, AccountMember, AuditLog, BillingSubscription, User
from test_current_plan_counts import subscription
from test_customer_activation import client, lab as lab, project, user


def login(database, owner, *, action='auth.login', status='ok', at=None, actor=None, target_type='user'):
    with database.session() as db:
        db.add(AuditLog(actor_user_id=(actor or owner).id, actor_email=owner.email, action=action, status=status,
                        target_type=target_type, target_id=owner.id,
                        created_at=at or datetime.now(timezone.utc) - timedelta(hours=1), meta={'secret': 'not exported'}))
        db.commit()


def exported(response):
    assert response.status_code == 200
    assert response.content.startswith(b'\xef\xbb\xbf')
    return list(csv.DictReader(io.StringIO(response.content.decode('utf-8-sig')), delimiter=';'))


def test_inventory_reconciles_all_plan_counts_including_old_accounts_and_teams(lab):
    database = lab['db']
    owners = [user(database) for _ in range(7)]
    for owner, plan, status in zip(owners, ['solo', 'pro', 'business', 'pro'], ['active', 'trialing', 'active', 'canceled']):
        subscription(database, owner, plan, status)
    subscription(database, owners[1], 'business', 'canceled')  # active takes precedence
    project(database, owners[4])
    with database.session() as db:
        db.add_all([AccountMember(owner_user_id=owners[2].id, member_user_id=owners[4].id),
                    AccountMember(owner_user_id=owners[2].id, member_user_id=owners[5].id)])
        db.delete(db.get(AccountActivation, owners[6].id))  # legacy Free account remains visible
        db.commit()
    subscription(database, lab['admin'], 'business')
    aggregate = plan_counts.snapshot(database)
    data = customer_accounts.snapshot(database, export=True)
    assert data['total'] == data['matched'] == aggregate['total'] == 6
    assert data['counts'] == {row['key']: row['count'] for row in aggregate['plans']}
    assert {row['user_id'] for row in data['rows']} == {o.id for o in owners if o != owners[5]}
    for key in billing.plan_catalog():
        filtered = customer_accounts.snapshot(database, plan=key, export=True)
        assert filtered['matched'] == data['counts'][key]
        with database.session() as db:
            assert all(billing.effective_plan_key(db, user_id=row['user_id']) == key for row in filtered['rows'])
    legacy = next(row for row in data['rows'] if row['user_id'] == owners[6].id)
    assert legacy['milestone'] == 'Historique non suivi' and legacy['last_login_at'] == ''


def test_subscription_details_follow_price_fallback_and_canceled_trial_history(lab, monkeypatch):
    database = lab['db']
    owner = user(database)
    row = subscription(database, owner, 'legacy', 'trialing', cancel_at_period_end=True,
                       current_period_end=datetime(2026, 11, 1, 12, tzinfo=timezone.utc))
    monkeypatch.setenv('STRIPE_PRICE_ID_PRO', 'price_legacy')
    record = customer_accounts.snapshot(database)['rows'][0]
    assert record['plan_key'] == 'pro' and record['status'] == 'trialing'
    assert record['access_label'] == 'En essai · résiliation prévue'
    assert record['current_period_end'] == '01/11/2026 à 13:00'
    with database.session() as db:
        db.get(BillingSubscription, row.id).status = 'canceled'
        db.commit()
    record = customer_accounts.snapshot(database, plan='free')['rows'][0]
    assert 'terminé' in record['access_label'] and record['plan_key'] == 'free'


def test_email_google_login_and_missing_history_do_not_invent_activity(lab):
    database = lab['db']
    email_user, google_user, no_login, old_login = [user(database) for _ in range(4)]
    now = datetime.now(timezone.utc)
    login(database, email_user, at=now - timedelta(days=2))
    login(database, google_user, action='auth.google.callback', actor=email_user, at=now - timedelta(hours=1))
    login(database, old_login, at=now - timedelta(days=31))
    for action, status, at in [('auth.login', 'invalid_credentials', now), ('auth.signup', 'ok', now),
                               ('oauth.google.callback', 'ok', now), ('auth.google.start', 'ok', now),
                               ('auth.login', 'ok', now + timedelta(days=1))]:
        login(database, no_login, action=action, status=status, at=at)
    login(database, no_login, action='auth.google.callback', target_type='project')
    rows = {row['user_id']: row for row in customer_accounts.snapshot(database)['rows']}
    assert rows[email_user.id]['last_login_at'] == customer_accounts.display_date(now - timedelta(days=2))
    assert rows[google_user.id]['last_login_at'] == customer_accounts.display_date(now - timedelta(hours=1))
    assert rows[no_login.id]['last_login_at'] == ''
    assert {row['user_id'] for row in customer_accounts.snapshot(database, usage='recent_login')['rows']} == {email_user.id, google_user.id}
    assert {row['user_id'] for row in customer_accounts.snapshot(database, usage='no_recent_login')['rows']} == {no_login.id, old_login.id}
    # OAuth often has no request actor at all; the target still identifies the login.
    with database.session() as db:
        db.add(AuditLog(action='auth.google.callback', status='ok', actor_email=no_login.email, actor_user_id=None,
                        target_type='user', target_id=no_login.id, created_at=now - timedelta(minutes=10)))
        db.commit()
    assert customer_accounts.snapshot(database, q=no_login.id)['rows'][0]['recent_login']


def test_site_counts_and_observed_steps_are_separate_from_recent_logins(lab):
    database = lab['db']
    owner, empty = user(database), user(database)
    project(database, owner)
    project(database, owner)
    when = datetime(2026, 10, 18, 12, tzinfo=timezone.utc)
    with database.session() as db:
        activation = db.get(AccountActivation, empty.id)
        activation.correction_verified_at = when
        activation.audit_available_at = when
        db.commit()
    with_site = customer_accounts.snapshot(database, usage='with_site')
    assert with_site['matched'] == 1 and with_site['rows'][0]['site_count'] == 2
    without_site = customer_accounts.snapshot(database, usage='without_site')['rows'][0]
    assert without_site['user_id'] == empty.id and without_site['milestone'] == 'Correction vérifiée'
    assert without_site['correction_verified_at'] == '18/10/2026 à 14:00' and not without_site['last_login_at']


def test_search_is_literal_case_insensitive_and_combines_with_plan_and_usage(lab):
    database = lab['db']
    owner = user(database, email='Atelier+%_@example.invalid')
    project(database, owner)
    user(database, email='another@example.invalid')
    subscription(database, owner, 'solo')
    assert customer_accounts.snapshot(database, q='ATELIER+%_', plan='solo', usage='with_site')['matched'] == 1
    assert customer_accounts.snapshot(database, q=owner.id)['rows'][0]['email'] == owner.email
    assert customer_accounts.snapshot(database, q="' OR 1=1 --")['matched'] == 0
    assert customer_accounts.snapshot(database, q='atelier', plan='free')['matched'] == 0


def test_pagination_and_export_cover_exact_same_filters_across_all_pages(lab):
    database = lab['db']
    now = datetime.now(timezone.utc)
    for index in range(29):
        user(database, email=f'export-{index:02d}@example.invalid', created=now - timedelta(days=1))
    user(database, email='excluded@example.invalid')
    c = client(lab['admin'])
    first = customer_accounts.snapshot(database, plan='free', q='export-', usage='without_site')
    second = customer_accounts.snapshot(database, plan='free', q='export-', usage='without_site', page=2)
    assert first['matched'] == 29 and len(first['rows']) == 25 and len(second['rows']) == 4
    assert not ({r['user_id'] for r in first['rows']} & {r['user_id'] for r in second['rows']})
    assert parse_qs(urlsplit(second['previous_url']).query) == {
        'plan': ['free'], 'q': ['export-'], 'usage': ['without_site'], 'page': ['1']}
    response = c.get(first['export_url'])
    csv_rows = exported(response)
    assert len(csv_rows) == 29
    assert {r['Identifiant du compte'] for r in csv_rows} == {r['user_id'] for r in first['rows'] + second['rows']}
    assert response.headers['cache-control'] == 'no-store' and 'attachment' in response.headers['content-disposition']
    page = c.get(second['previous_url'])
    assert len(BeautifulSoup(page.text, 'html.parser').select('[data-account-row]')) == 25
    assert customer_accounts.snapshot(database, q='export-', page=1000)['page'] == 2


@pytest.mark.parametrize('email', ['=1+1@example.invalid', '+1@example.invalid', '-1@example.invalid',
                                 '@formula.invalid', '  =HYPERLINK("bad")@example.invalid', '\tformula@example.invalid'])
def test_csv_escapes_spreadsheet_formulas_and_preserves_utf8(lab, email):
    owner = user(lab['db'], email=email)
    rows = exported(client(lab['admin']).get('/settings/activation/accounts.csv'))
    assert rows[0]['Email'] == "'" + email
    assert rows[0]['Identifiant du compte'] == owner.id


def test_table_escapes_untrusted_text_and_exposes_no_secrets(lab):
    owner = user(lab['db'], email='<img src=x onerror=alert(1)>@example.invalid')
    subscription(lab['db'], owner, 'pro')
    login(lab['db'], owner)
    c = client(lab['admin'])
    page = c.get('/settings/activation/accounts')
    assert page.status_code == 200 and page.headers['cache-control'] == 'no-store'
    assert page.headers['referrer-policy'] == 'no-referrer'
    soup = BeautifulSoup(page.text, 'html.parser')
    assert soup.select_one('.account-email strong').text == owner.email
    assert not soup.select_one('.account-email img')
    csv_response = c.get('/settings/activation/accounts.csv')
    for content in [page.text, csv_response.text]:
        assert all(secret not in content for secret in ['password_hash', 'stripe_data', 'cus_', 'price_pro', 'not exported'])


@pytest.mark.parametrize('path', ['/settings/activation/accounts', '/settings/activation/accounts.csv'])
def test_identity_routes_are_restricted_to_system_owner(lab, path):
    owner = user(lab['db'])
    other_admin = user(lab['db'], admin=True)
    for outsider in [None, owner, other_admin]:
        response = client(outsider).get(path, follow_redirects=False)
        assert response.status_code in (303, 403)
        assert owner.email not in response.text
    assert client(lab['admin']).get(path).status_code == 200


@pytest.mark.parametrize('query', ['plan=unknown', 'usage=unknown', 'page=0', 'q=' + 'x' * 321])
def test_bad_filters_are_rejected_for_table_and_export(lab, query):
    c = client(lab['admin'])
    assert c.get('/settings/activation/accounts?' + query).status_code == 400
    if not query.startswith('page='):
        assert c.get('/settings/activation/accounts.csv?' + query).status_code == 400


def test_empty_results_and_database_failures_are_explicit(lab, monkeypatch):
    c = client(lab['admin'])
    assert 'Aucun compte ne correspond' in c.get('/settings/activation/accounts?q=absent').text
    assert exported(c.get('/settings/activation/accounts.csv?q=absent')) == []

    def unavailable(*args, **kwargs):
        raise RuntimeError('private database connection information')

    monkeypatch.setattr(customer_accounts, 'snapshot', unavailable)
    for path in ['/settings/activation/accounts', '/settings/activation/accounts.csv']:
        response = c.get(path)
        assert response.status_code == 503 and 'private database' not in response.text


def test_inventory_reads_once_and_next_request_reflects_changes_without_stripe(lab, monkeypatch):
    database = lab['db']
    owners = [user(database) for _ in range(30)]
    for owner in owners:
        project(database, owner)
        login(database, owner)
    subscription(database, owners[0], 'solo')
    queries = []

    def observed(connection, cursor, statement, parameters, context, many):
        queries.append(statement)

    monkeypatch.setattr(billing, 'stripe_init', lambda: (_ for _ in ()).throw(AssertionError('no Stripe call')))
    event.listen(database.engine, 'before_cursor_execute', observed)
    try:
        data = customer_accounts.snapshot(database, export=True)
    finally:
        event.remove(database.engine, 'before_cursor_execute', observed)
    assert len(queries) == 1 and data['matched'] == 30
    with database.session() as db:
        db.delete(db.scalar(select(User).where(User.id == owners[1].id)))
        db.scalar(select(BillingSubscription).where(BillingSubscription.user_id == owners[0].id)).status = 'canceled'
        db.commit()
    assert customer_accounts.snapshot(database, plan='solo')['matched'] == 0
    assert customer_accounts.snapshot(database, plan='free')['matched'] == 29


def test_counter_cards_link_to_corresponding_detail_and_explain_access(lab):
    c = client(lab['admin'])
    soup = BeautifulSoup(c.get('/settings/activation').text, 'html.parser')
    assert soup.select_one('#active-plans h2').text == 'Comptes par plan'
    assert 'pas l’utilisation récente' in soup.select_one('#active-plans').text
    for key in billing.plan_catalog():
        link = soup.select_one(f'[data-plan-key={key}] .live-plan-link')
        assert link['href'] == f'/settings/activation/accounts?plan={key}'
        assert c.get(link['href']).status_code == 200
