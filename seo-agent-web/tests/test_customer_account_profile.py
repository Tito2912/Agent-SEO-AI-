"""Customer dossiers show stored evidence without decrypting secrets or impersonating users."""
from __future__ import annotations

import csv
import io
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from bs4 import BeautifulSoup
from sqlalchemy import event

from backend import app as m, billing, customer_account_profile as profiles, customer_accounts, signup_attribution
from backend.models import (AccountActivation, AccountMember, EmailVerificationToken, JobRecord, OAuthIdentity,
                            Project, UsageEvent, User, UserConnection)
from test_current_plan_counts import subscription
from test_customer_accounts import login
from test_customer_activation import client, lab as lab, project, user


def dossier(database, owner):
    return profiles.snapshot(database, owner.id, google_key=m._gsc_oauth_connection_key, bing_key=m._bing_oauth_connection_key)


def job(database, owner, *, slug='atelier', status='done', kind='crawl', when=None):
    when = when or datetime.now(timezone.utc) - timedelta(hours=1)
    with database.session() as db:
        row = JobRecord(id=str(uuid.uuid4()), owner_user_id=owner.id, slug=slug, status=status, kind=kind,
                        created_at=when.timestamp(), started_at=when.timestamp() + 10,
                        finished_at=when.timestamp() + 20 if status == 'done' else None,
                        stdout='private process output', stderr='private process error', result={'secret': 'private result'})
        db.add(row)
        db.commit()
        return row


def verified(database, owner, *, used=True):
    when = datetime.now(timezone.utc) - timedelta(minutes=2)
    with database.session() as db:
        db.add(EmailVerificationToken(user_id=owner.id, token_hash=uuid.uuid4().hex,
                                      expires_at=when + timedelta(days=1), used_at=when if used else None))
        db.commit()
    return when


def identity(database, owner):
    with database.session() as db:
        db.add(OAuthIdentity(user_id=owner.id, provider='google', provider_user_id='private-provider-' + uuid.uuid4().hex,
                             email=owner.email))
        db.commit()


def connection(database, owner, key):
    with database.session() as db:
        db.add(UserConnection(user_id=owner.id, key=key, secret_value='private credential never read', meta={'secret': 'not for display'}))
        db.commit()


def test_list_and_dossier_expose_verification_google_and_real_analysis_records(lab):
    database = lab['db']
    owner, other = user(database), user(database)
    verified_at = verified(database, owner)
    identity(database, owner)
    subscription(database, owner, 'solo')
    project(database, owner)
    for status in ['queued', 'running', 'done', 'failed']:
        job(database, owner, status=status)
    job(database, other)
    job(database, owner, kind='competitor')
    job(database, owner, when=datetime.now(timezone.utc) + timedelta(days=1))
    data = dossier(database, owner)
    assert data['account']['analysis_count'] == len(data['jobs']) == 4
    assert sum(item['count'] for item in data['job_counts']) == 4
    assert data['account']['verification_label'] == 'Email vérifié'
    assert data['account']['email_verified_at'] == customer_accounts.display_date(verified_at)
    assert data['account']['google_linked'] and data['google_since']
    assert data['account']['plan_key'] == 'solo' and len(data['sites']) == 1
    assert data['account']['last_login_at'] == ''  # identity and email verification are not logins
    assert customer_accounts.snapshot(database, usage='verified_email')['matched'] == 1
    assert customer_accounts.snapshot(database, usage='unverified_email')['matched'] == 1
    assert customer_accounts.snapshot(database, usage='with_analysis')['matched'] == 2
    assert customer_accounts.snapshot(database, usage='without_analysis')['matched'] == 0


def test_old_or_google_accounts_without_verification_proof_remain_unknown(lab):
    database = lab['db']
    owner = user(database)
    identity(database, owner)
    verified(database, owner, used=False)
    with database.session() as db:
        db.delete(db.get(AccountActivation, owner.id))
        db.get(User, owner.id).updated_at = datetime.now(timezone.utc)
        db.commit()
    data = dossier(database, owner)
    assert data['account']['verification_label'] == 'Vérification non retrouvée'
    assert not data['account']['email_verified_at'] and data['account']['google_linked']
    assert not data['account']['last_login_at'] and data['login_count'] == 0
    assert all(not milestone['date'] for milestone in data['milestones'])
    assert all(not data['account'][key] for key in ['country', 'language', 'timezone'])


def test_google_and_password_login_counts_use_authenticated_user_not_request_actor(lab):
    database = lab['db']
    owner, actor = user(database), user(database)
    when = datetime.now(timezone.utc)
    login(database, owner, at=when - timedelta(days=2))
    login(database, owner, action='auth.google.callback', actor=actor, at=when - timedelta(days=1))
    login(database, owner, action='auth.login', status='invalid_credentials', at=when)
    login(database, owner, action='auth.signup', at=when)
    data = dossier(database, owner)
    assert data['login_count'] == 2 and data['password_login_count'] == data['google_login_count'] == 1
    assert data['first_login_at'] == customer_accounts.display_date(when - timedelta(days=2))
    assert data['account']['last_login_at'] == customer_accounts.display_date(when - timedelta(days=1))
    assert dossier(database, actor)['login_count'] == 0


def test_monthly_usage_matches_billing_including_refunds_and_plan_gates(lab):
    database = lab['db']
    owner, other = user(database), user(database)
    period = billing._period_key()
    with database.session() as db:
        for uid, month, metric, amount in [(owner.id, period, 'pages_crawled_month', 200),
                                           (owner.id, period, 'pages_crawled_month', -70),
                                           (owner.id, period, 'assistant_messages_month', 9),
                                           (owner.id, '2000-01', 'pages_crawled_month', 999),
                                           (other.id, period, 'pages_crawled_month', 888)]:
            db.add(UsageEvent(user_id=uid, period=month, metric=metric, amount=amount, meta={'secret': 'private usage data'}))
        db.commit()
    data = dossier(database, owner)
    quotas = {row['key']: row for row in data['quotas']}
    with database.session() as db:
        assert all(row['used'] == billing.usage_sum(db, user_id=owner.id, metric=key) for key, row in quotas.items())
    assert quotas['pages_crawled_month']['used'] == 130
    assert quotas['assistant_messages_month']['used'] == 9
    assert quotas['pages_crawled_month']['limit_label'] == str(billing.plan_catalog()['free']['limits']['pages_crawled_month'])
    assert quotas['ai_articles_month']['limit_label'] == quotas['backlink_searches_month']['limit_label'] == 'Non inclus dans cette formule'


def test_unlimited_ledger_is_not_labeled_exhausted_and_current_overrides_are_used(lab, monkeypatch):
    owner = user(lab['db'])
    monkeypatch.setenv('PLAN_CONFIG_JSON', '{"free":{"limits":{"pages_crawled_month":0,"assistant_messages_month":7}}}')
    quotas = {row['key']: row for row in dossier(lab['db'], owner)['quotas']}
    assert quotas['pages_crawled_month']['limit_label'] == 'Sans plafond de compteur'
    assert quotas['assistant_messages_month']['limit_label'] == '7'


def test_sites_integrations_and_teams_are_scoped_without_checking_credentials(lab, monkeypatch):
    database = lab['db']
    owner, other = user(database), user(database)
    first, second = project(database, owner, slug='atelier'), project(database, owner, slug='boutique')
    project(database, other, slug='atelier')
    with database.session() as db:
        db.get(Project, first.id).settings = {'cms': {'platform': 'wordpress'}, 'secret': 'private project setting'}
        db.add(AccountMember(owner_user_id=owner.id, member_user_id=other.id))
        db.commit()
    connection(database, owner, m._gsc_oauth_connection_key(first.slug))
    connection(database, owner, 'CMS_WORDPRESS:' + first.id)
    connection(database, owner, 'BING_OAUTH_REFRESH_TOKEN')
    connection(database, owner, 'NETLIFY_TOKEN')
    connection(database, other, m._gsc_oauth_connection_key(second.slug))

    def forbidden(*args, **kwargs):
        raise AssertionError('dossier must not decrypt or refresh connections')

    for name in ['_decrypt_secret', '_decrypt_secret_with_rotation', '_gsc_oauth_connected', '_effective_bing_connection']:
        monkeypatch.setattr(m, name, forbidden)
    data = dossier(database, owner)
    assert data['members'] == 1 and len(data['sites']) == 2
    sites = {row['platform']: row for row in data['sites']}
    assert sites['WordPress']['google']['saved'] and sites['WordPress']['wordpress']['saved']
    assert not sites['Non renseigné']['google']['saved']
    assert all(row['bing']['saved'] for row in data['sites'])
    assert next(row for row in data['connections'] if row['label'] == 'Netlify')['saved']


def test_campaign_requires_existing_consent_and_disappears_after_withdrawal(lab):
    database = lab['db']
    owner = user(database)
    campaign = signup_attribution.create(database, 'google', 'cpc', 'premier-audit')
    with database.session() as db:
        row = db.get(AccountActivation, owner.id)
        row.acquisition_campaign_id = campaign.id
        db.commit()
    assert dossier(database, owner)['campaign'] is None
    with database.session() as db:
        db.get(AccountActivation, owner.id).acquisition_consented_at = datetime.now(timezone.utc)
        db.commit()
    assert dossier(database, owner)['campaign'] == {'source': 'google', 'medium': 'cpc', 'name': 'premier-audit'}
    signup_attribution.withdraw(database, owner.id)
    assert dossier(database, owner)['campaign'] is None


def test_recent_jobs_are_bounded_and_have_stable_order(lab):
    owner = user(lab['db'])
    when = datetime.now(timezone.utc) - timedelta(days=1)
    values = [job(lab['db'], owner, when=when) for _ in range(12)]
    data = dossier(lab['db'], owner)
    assert data['account']['analysis_count'] == 12 and len(data['jobs']) == 10
    assert [row['id'] for row in data['jobs']] == sorted([value.id for value in values])[:10]
    assert all(row['created_at'] == customer_accounts.display_date(when) for row in data['jobs'])


@pytest.mark.parametrize('export', [False, True])
def test_dossier_and_export_require_system_owner_and_reject_excluded_accounts(lab, export):
    database = lab['db']
    owner, member, admin = user(database), user(database), user(database, admin=True)
    with database.session() as db:
        db.add(AccountMember(owner_user_id=owner.id, member_user_id=member.id))
        db.commit()
    path = '/settings/activation/accounts/' + owner.id + ('/export.csv' if export else '')
    for outsider in [None, owner, member, admin]:
        response = client(outsider).get(path, follow_redirects=False)
        assert response.status_code in (303, 403) and owner.email not in response.text
    c = client(lab['admin'])
    response = c.get(path)
    assert response.status_code == 200 and response.headers['cache-control'] == 'no-store'
    assert response.headers['referrer-policy'] == 'no-referrer'
    for excluded in [member.id, admin.id, str(uuid.uuid4())]:
        assert c.get('/settings/activation/accounts/' + excluded + ('/export.csv' if export else '')).status_code == 404
    with database.session() as db:
        db.delete(db.get(User, owner.id))
        db.commit()
    assert c.get(path).status_code == 404


def test_exports_include_enriched_summary_and_dossier_and_escape_all_text(lab):
    database = lab['db']
    owner = user(database, email='=FORMULA@example.invalid')
    identity(database, owner)
    verified(database, owner)
    site = project(database, owner)
    with database.session() as db:
        row = db.get(Project, site.id)
        row.site_name = '  =FORMULA'
        row.base_url = '<img src=x onerror=alert(1)>'
        db.get(User, owner.id).country = 'FR'
        db.commit()
    job(database, owner, slug='=JOB')
    c = client(lab['admin'])
    bulk = list(csv.DictReader(io.StringIO(c.get('/settings/activation/accounts.csv').content.decode('utf-8-sig')), delimiter=';'))
    assert bulk[0]['Email'] == "'=FORMULA@example.invalid"
    assert bulk[0]['Google associé'] == 'Oui' and bulk[0]['Analyses enregistrées'] == '1'
    assert bulk[0]['Pays renseigné'] == 'FR'
    response = c.get('/settings/activation/accounts/' + owner.id + '/export.csv')
    assert response.content.startswith(b'\xef\xbb\xbf') and 'attachment' in response.headers['content-disposition']
    rows = list(csv.DictReader(io.StringIO(response.content.decode('utf-8-sig')), delimiter=';'))
    assert any(row['Champ'] == 'Nom' and row['Valeur'] == "'  =FORMULA" for row in rows)
    assert any(row['Champ'] == 'Site' and row['Valeur'] == "'=JOB" for row in rows)
    assert any(row['Champ'] == 'Analyses enregistrées' and row['Valeur'] == '1' for row in rows)
    assert any(row['Champ'] == 'Google Search Console · mise à jour (Paris)' for row in rows)
    page = c.get('/settings/activation/accounts/' + owner.id)
    soup = BeautifulSoup(page.text, 'html.parser')
    assert soup.select_one('.profile-url').text == '<img src=x onerror=alert(1)>'
    assert not soup.select_one('.profile-url img')
    assert not soup.select_one('.profile-url a')
    for body in [response.text, page.text]:
        assert all(secret not in body for secret in ['private process', 'private result', 'private credential', 'token_hash', 'provider_user_id'])


def test_lookup_is_read_only_constant_query_count_and_never_selects_secrets(lab, monkeypatch):
    database = lab['db']
    owner = user(database)
    for _ in range(12):
        project(database, owner)
    connection(database, owner, 'GITHUB_TOKEN')
    identity(database, owner)
    verified(database, owner)
    statements = []

    def observed(connection, cursor, statement, parameters, context, many):
        statements.append(statement)

    monkeypatch.setattr(billing, 'stripe_init', lambda: (_ for _ in ()).throw(AssertionError('no Stripe call')))
    event.listen(database.engine, 'before_cursor_execute', observed)
    try:
        data = dossier(database, owner)
    finally:
        event.remove(database.engine, 'before_cursor_execute', observed)
    assert len(data['sites']) == 12 and len(statements) == 10
    assert all(statement.lstrip().upper().startswith('SELECT') for statement in statements)
    sql = '\n'.join(statements)
    assert all(column not in sql for column in ['secret_value', 'password_hash', 'token_hash', 'stripe_data', 'stdout', 'stderr', 'provider_user_id'])


def test_failures_do_not_leak_private_database_details(lab, monkeypatch):
    owner = user(lab['db'])

    def unavailable(*args, **kwargs):
        raise RuntimeError('private database location')

    monkeypatch.setattr(profiles, 'snapshot', unavailable)
    for suffix in ['', '/export.csv']:
        response = client(lab['admin']).get('/settings/activation/accounts/' + owner.id + suffix)
        assert response.status_code == 503 and 'private database' not in response.text


def test_list_links_to_fiche_and_retains_existing_csv_filter_scope(lab):
    database = lab['db']
    owner = user(database)
    verified(database, owner)
    job(database, owner)
    user(database)
    c = client(lab['admin'])
    page = c.get('/settings/activation/accounts?usage=verified_email')
    soup = BeautifulSoup(page.text, 'html.parser')
    link = soup.select_one('.account-open')
    from urllib.parse import parse_qs, urlsplit
    assert urlsplit(link['href']).path == '/settings/activation/accounts/' + owner.id
    return_to = parse_qs(urlsplit(link['href']).query)['return_to'][0]
    assert parse_qs(urlsplit(return_to).query)['usage'] == ['verified_email']
    assert c.get(link['href']).status_code == 200
    rows = list(csv.DictReader(io.StringIO(c.get('/settings/activation/accounts.csv?usage=verified_email').content.decode('utf-8-sig')), delimiter=';'))
    assert len(rows) == 1 and rows[0]['Identifiant du compte'] == owner.id
