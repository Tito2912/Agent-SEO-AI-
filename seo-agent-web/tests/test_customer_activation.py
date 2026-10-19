"""Only real first product milestones feed owner-only, unique-account cohort metrics."""
from __future__ import annotations

import csv
import io
import json
import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from backend import app as m, auth, customer_activation as activation
from backend.db import Database
from backend.models import AccountActivation, AccountInvite, Project, User
from test_first_verified_correction import _choose, _ready, _verify, journey as journey

NOW = datetime(2026, 10, 18, 12, tzinfo=timezone.utc)


@pytest.fixture()
def lab(tmp_path, monkeypatch):
    monkeypatch.setenv('DATABASE_URL', 'sqlite:///' + str(tmp_path / 'activation.db'))
    database = Database(data_dir=tmp_path / 'data')
    database.create_tables()
    monkeypatch.setattr(m, 'DB', database)
    monkeypatch.setattr(m, '_runs_dir_for_user', lambda uid: tmp_path / 'runs' / uid)
    monkeypatch.setattr(m, '_email_verification_enabled', lambda: False)
    monkeypatch.setattr(m, '_validate_public_crawl_target', lambda value: '')
    for key in ['SIGNUP_INVITE_CODE', 'SIGNUP_DISABLED', 'SIGNUP_ALLOWLIST_EMAILS', 'SIGNUP_ALLOWLIST_DOMAINS', 'BOOTSTRAP_ADMIN_EMAIL']:
        monkeypatch.setenv(key, '')
    admin = user(database, admin=True)
    monkeypatch.setenv('SYSTEM_SETTINGS_OWNER_EMAIL', admin.email)
    yield {'db': database, 'admin': admin}
    database.engine.dispose()


def user(database, *, admin=False, created=None, email=None):
    with database.session() as db:
        value = User(email=email or f'activation-{uuid.uuid4().hex}@example.invalid', password_hash='test', is_admin=admin,
                     created_at=created or datetime.now(timezone.utc) - timedelta(minutes=5))
        db.add(value)
        db.commit()
        return value


def client(value=None):
    result = TestClient(m.app)
    if value:
        result.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(user_id=str(value.id), secret=os.environ['SEO_AGENT_SECRET_KEY']))
    return result


def post_signup(client, email, password='test-password-for-activation'):
    client.get('/auth/signup')
    return client.post('/auth/signup', data={'email': email, 'password': password,
                       '_csrf': client.cookies.get(m._CSRF_COOKIE_NAME)}, follow_redirects=False)


def account(database, uid):
    with database.session() as db:
        return db.get(AccountActivation, str(uid))


def project(database, owner, *, slug=None):
    with database.session() as db:
        row = Project(owner_user_id=str(owner.id), slug=slug or uuid.uuid4().hex, site_name='Atelier', base_url='https://atelier.example/', settings={})
        db.add(row)
        db.commit()
        return row


def report(*, clean=False, status=200, content_type='text/html', partial=False, blocked=False, url='https://atelier.example/service/'):
    return {'meta': {'base_url': 'https://atelier.example/', 'stopped_on_time_budget': partial,
                     'blocked_by_host': {'count': int(blocked)}},
            'pages': [{'url': url, 'status_code': status, 'content_type': content_type}],
            'issues': {} if clean else {'missing_title': {'count': 1, 'examples': [url]}}}


def test_signup_is_recorded_once_and_rejections_do_not_count(lab):
    c = client()
    email = f'activation-http-{uuid.uuid4().hex}@example.invalid'
    assert post_signup(c, email, 'short').status_code == 303
    with lab['db'].session() as db:
        assert not db.scalar(select(User).where(User.email == email))
    assert post_signup(c, email).status_code == 303
    with lab['db'].session() as db:
        registered = db.scalar(select(User).where(User.email == email))
        uid = registered.id
    assert account(lab['db'], uid).signup_observed
    c.cookies.clear()
    post_signup(c, email)
    with lab['db'].session() as db:
        assert db.scalar(select(func.count()).select_from(AccountActivation).where(AccountActivation.user_id == uid)) == 1


def test_a_failed_verification_email_still_counts_the_account_that_was_created(lab, monkeypatch):
    monkeypatch.setattr(m, '_email_verification_enabled', lambda: True)
    monkeypatch.setattr(m, '_send_email_verification_email', lambda **kwargs: (_ for _ in ()).throw(RuntimeError('smtp down')))
    email = f'activation-mail-{uuid.uuid4().hex}@example.invalid'
    assert 'verify/resend' in post_signup(client(), email).headers['location']
    with lab['db'].session() as db:
        registered = db.scalar(select(User).where(User.email == email))
    assert account(lab['db'], registered.id).signup_observed


def test_metrics_commit_and_rollback_with_their_account_and_project(lab):
    database = lab['db']
    with database.session() as db:
        value = User(email=f'rollback-{uuid.uuid4().hex}@example.invalid', password_hash='test', is_admin=False)
        db.add(value)
        db.flush()
        uid = str(value.id)
        assert db.get(AccountActivation, uid).signup_observed
        db.rollback()
    assert account(database, uid) is None
    owner = user(database)
    with database.session() as db:
        db.add(Project(owner_user_id=owner.id, slug='rollback', base_url='https://atelier.example', site_name='Atelier', settings={}))
        db.flush()
        db.rollback()
    assert account(database, owner.id).project_added_at is None


def test_duplicate_sites_and_multiple_projects_never_duplicate_an_account(lab):
    owner = user(lab['db'])
    for base in ['https://atelier.example/', 'https://atelier.example/', 'https://autre.example/']:
        m._db_upsert_project(user_id=owner.id, base_url=base)
    result = activation.snapshot(lab['db'])
    assert [step['count'] for step in result['steps']] == [1, 1, 0, 0]


@pytest.mark.parametrize('options', [{'status': None}, {'status': 500}, {'content_type': 'application/pdf'},
                                    {'url': 'https://other.example/'}, {'status': 0}])
def test_failed_or_non_html_reports_do_not_validate_an_audit(lab, options):
    owner = user(lab['db'])
    row = project(lab['db'], owner)
    m._verify_corrections_after_crawl(row.slug, report(**options), user_id=owner.id, crawl_ts='20261018-100000')
    assert account(lab['db'], owner.id).audit_available_at is None


def test_an_audit_with_no_tasks_is_counted_and_a_clean_audit_has_its_own_outcome(lab):
    owner = user(lab['db'])
    row = project(lab['db'], owner)
    m._verify_corrections_after_crawl(row.slug, report(clean=True), user_id=owner.id)
    data = activation.snapshot(lab['db'])
    assert [step['count'] for step in data['steps']] == [1, 1, 1, 0]
    assert data['clean_count'] == data['value_count'] == 1
    assert data['waiting'][2]['count'] == 0


@pytest.mark.parametrize('options', [{'partial': True}, {'blocked': True}])
def test_a_usable_partial_audit_is_not_a_clean_completed_audit(lab, options):
    owner = user(lab['db'])
    row = project(lab['db'], owner)
    m._verify_corrections_after_crawl(row.slug, report(clean=True, **options), user_id=owner.id)
    state = account(lab['db'], owner.id)
    assert state.audit_available_at and state.clean_audit_at is None


def test_report_scope_cannot_cross_accounts_that_share_a_slug(lab):
    one, two = user(lab['db']), user(lab['db'])
    project(lab['db'], one, slug='same')
    project(lab['db'], two, slug='same')
    m._verify_corrections_after_crawl('same', report(clean=True))
    assert not account(lab['db'], one.id).audit_available_at
    m._verify_corrections_after_crawl('same', report(clean=True), user_id=one.id)
    assert account(lab['db'], one.id).audit_available_at
    assert not account(lab['db'], two.id).audit_available_at


@pytest.fixture()
def tracked_journey(lab, journey):
    return journey


def test_only_the_real_verifier_counts_a_correction_and_the_owning_account_gets_credit(lab, tracked_journey):
    data = tracked_journey
    _ready(data)
    assert not account(lab['db'], data['owner']).correction_verified_at
    _verify(data, issues={'missing_title': {'count': 1, 'examples': ['https://atelier.example/service/']}})
    assert not account(lab['db'], data['owner']).correction_verified_at
    _verify(data)
    first = account(lab['db'], data['owner']).correction_verified_at
    assert first and not account(lab['db'], data['member']).correction_verified_at
    _verify(data)
    assert account(lab['db'], data['owner']).correction_verified_at == first


def test_a_declared_done_status_and_user_notes_cannot_create_a_conversion(lab, tracked_journey):
    data = tracked_journey
    task = _choose(data)
    c = data['client']
    c.get(f'/projects/{data["slug"]}/first-correction')
    response = c.post(f'/api/projects/{data["slug"]}/issues/missing_title/task',
                      json={'status': 'done', 'url': task.url, 'note': json.dumps({'verify': {'result': 'resolved'}})},
                      headers={m._CSRF_HEADER_NAME: c.cookies.get(m._CSRF_COOKIE_NAME)})
    assert response.status_code == 200
    assert account(lab['db'], data['owner']).correction_verified_at is None


def test_invited_and_administrative_accounts_are_excluded_and_old_accounts_are_not_invented(lab):
    database = lab['db']
    with database.session() as db:
        db.add(AccountInvite(owner_user_id=lab['admin'].id, email='invited@example.invalid', token_hash=uuid.uuid4().hex,
                             expires_at=datetime.now(timezone.utc) + timedelta(days=1)))
        db.commit()
    invited = user(database, email='invited@example.invalid')
    assert account(database, invited.id).invited_signup
    old_id = str(uuid.uuid4())
    with database.engine.begin() as connection:
        connection.execute(User.__table__.insert().values(id=old_id, email='old@example.invalid', password_hash='test',
                           is_admin=False, created_at=datetime.now(timezone.utc) - timedelta(days=1)))
    data = activation.snapshot(database)
    assert data['steps'][0]['count'] == 0 and data['invited'] == data['untracked'] == 1


def test_first_milestones_survive_site_removal_and_disappear_on_account_deletion(lab, tracked_journey):
    data = tracked_journey
    _ready(data)
    _verify(data)
    with lab['db'].session() as db:
        db.delete(db.get(Project, data['project_id']))
        db.commit()
    assert account(lab['db'], data['owner']).correction_verified_at
    with lab['db'].session() as db:
        db.delete(db.get(User, data['owner']))
        db.commit()
    assert account(lab['db'], data['owner']) is None


def test_cohorts_use_signup_dates_paris_midnight_and_unique_accounts(lab):
    # 22:30 UTC on the previous day is already 00:30 on 18 October in Paris.
    inside = user(lab['db'], created=datetime(2026, 10, 17, 22, 30, tzinfo=timezone.utc))
    user(lab['db'], created=datetime(2026, 10, 17, 21, 59, tzinfo=timezone.utc))
    project(lab['db'], inside)
    data = activation.snapshot(lab['db'], start='2026-10-18', end='2026-10-18', now=NOW)
    # Projects are created at wall-clock time, later than fixed NOW: only the signup is eligible.
    assert data['steps'][0]['count'] == 1 and data['weeks'][0]['week'] == '2026-10-12'


def test_zero_denominators_are_unknown_not_fake_percentages(lab):
    data = activation.snapshot(lab['db'], start='2026-01-01', end='2026-01-02', now=NOW)
    assert data['value_rate'] is None and all(step['rate'] is None for step in data['steps'])


@pytest.mark.parametrize('start,end', [('bad', ''), ('2026-10-19', '2026-10-18'), ('2020-01-01', '2026-10-18'), ('2026-10-18', '2026-10-19')])
def test_invalid_periods_are_rejected(lab, start, end):
    with pytest.raises(ValueError):
        activation.snapshot(lab['db'], start=start, end=end, now=NOW)


def test_routes_are_owner_only_uncached_and_exports_have_no_customer_identifiers(lab):
    owner = user(lab['db'])
    project(lab['db'], owner)
    other_admin = user(lab['db'], admin=True)
    for path in ['/settings/activation', '/api/settings/activation', '/settings/activation.csv']:
        for outsider in [owner, other_admin]:
            assert client(outsider).get(path).status_code == 403
        response = client(lab['admin']).get(path)
        assert response.status_code == 200 and response.headers['cache-control'] == 'no-store'
        assert owner.email not in response.text and str(owner.id) not in response.text and 'password_hash' not in response.text
    c = client(lab['admin'])
    data = c.get('/api/settings/activation').json()
    assert data['steps'][0]['count'] == 1 and data['steps'][1]['count'] == 1
    response = c.get('/settings/activation.csv')
    rows = list(csv.reader(io.StringIO(response.content.decode('utf-8-sig')), delimiter=';'))
    assert len(rows) == 2 and rows[1][3:5] == ['1', '1']
    assert c.get('/settings/activation?date_from=bad').status_code == 400


def test_analytics_failure_never_prevents_account_or_project_creation(lab, monkeypatch):
    def fail(*args, **kwargs):
        raise RuntimeError('analytics unavailable')
    monkeypatch.setattr(activation, '_write', fail)
    owner = user(lab['db'])
    row = project(lab['db'], owner)
    with lab['db'].session() as db:
        assert db.get(User, owner.id) and db.get(Project, row.id)
    assert account(lab['db'], owner.id) is None


def test_unproven_baseline_cannot_become_a_verified_conversion(lab, tracked_journey):
    data = tracked_journey
    _ready(data)
    # Keep the task but remove proof that the reference crawl observed this defect.
    data['report'](issues={})
    _verify(data)
    assert account(lab['db'], data['owner']).correction_verified_at is None


def test_analytics_evidence_failure_does_not_interrupt_a_real_correction(lab, tracked_journey, monkeypatch):
    data = tracked_journey
    _ready(data)
    real_extract = activation.dash.extract_impacted_pages

    def unavailable_baseline(key, issue):
        if isinstance(issue, dict) and issue.get('count'):
            raise RuntimeError('metrics evidence unavailable')
        return real_extract(key, issue)

    ts = (datetime.now(timezone.utc) + timedelta(days=1)).strftime('%Y%m%d-%H%M%S')
    after = data['report'](ts, issues={})
    with monkeypatch.context() as failure:
        failure.setattr(activation.dash, 'extract_impacted_pages', unavailable_baseline)
        m._verify_corrections_after_crawl(data['slug'], after, data['runs_dir'], user_id=data['owner'], crawl_ts=ts)
    with lab['db'].session() as db:
        task = db.scalar(select(m.IssueTask).where(m.IssueTask.project_id == data['project_id']))
        assert task.status == 'done' and json.loads(task.note)['verify']['result'] == 'resolved'
    assert data['client'].get(f'/projects/{data["slug"]}/first-correction').status_code == 200
    assert account(lab['db'], data['owner']).correction_verified_at is None


def test_daylight_saving_day_uses_the_full_paris_calendar_day(lab):
    for value in [datetime(2026, 10, 24, 22, 30, tzinfo=timezone.utc),
                  datetime(2026, 10, 25, 22, 30, tzinfo=timezone.utc),
                  datetime(2026, 10, 24, 21, 59, tzinfo=timezone.utc)]:
        user(lab['db'], created=value)
    data = activation.snapshot(lab['db'], start='2026-10-25', end='2026-10-25',
                               now=datetime(2026, 10, 26, 12, tzinfo=timezone.utc))
    assert data['steps'][0]['count'] == 2


def test_cohort_rates_use_the_same_signup_population_and_clean_results_do_not_double_count(lab):
    database = lab['db']
    signups = [user(database, created=NOW - timedelta(days=5)) for _ in range(4)]
    with database.session() as db:
        for value in signups[1:]:
            db.get(AccountActivation, value.id).project_added_at = NOW - timedelta(days=4)
        for value in signups[2:]:
            db.get(AccountActivation, value.id).audit_available_at = NOW - timedelta(days=3)
        db.get(AccountActivation, signups[2].id).correction_verified_at = NOW - timedelta(days=2)
        db.get(AccountActivation, signups[2].id).clean_audit_at = NOW - timedelta(days=2)
        db.get(AccountActivation, signups[3].id).clean_audit_at = NOW - timedelta(days=2)
        db.commit()
    data = activation.snapshot(database, start='2026-10-01', end='2026-10-18', now=NOW)
    assert [step['count'] for step in data['steps']] == [4, 3, 2, 1]
    assert [step['rate'] for step in data['steps']] == [100.0, 75.0, 50.0, 25.0]
    assert data['steps'][3]['previous_rate'] == 50.0
    assert data['clean_count'] == 1 and data['value_count'] == 2 and data['value_rate'] == 50.0
    assert [stage['count'] for stage in data['waiting']] == [1, 1, 0]
