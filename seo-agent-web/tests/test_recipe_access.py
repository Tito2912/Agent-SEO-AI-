"""Only an audited, expiring grant lets its account add its exact demo origin."""
import importlib.util
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bs4 import BeautifulSoup
import pytest
from sqlalchemy import func, select

from backend import app as m, billing, recipe_access as access
from backend.models import AuditLog, BillingCustomer, BillingSubscription, Project, ProjectRecipeAccess, User
from test_customer_activation import client, lab as lab, project, user

ORIGIN = 'https://demo.example/'


def page(uid):
    return f'/settings/activation/accounts/{uid}/recipe-access'


def post(c, path, **values):
    c.get('/')
    return c.post(path, data={'_csrf': c.cookies.get(m._CSRF_COOKIE_NAME), **values}, follow_redirects=False)


def grant(lab, target, **kwargs):
    with lab['db'].session() as db:
        row = access.grant(db, actor=lab['admin'], user_id=target.id, base_url=ORIGIN, hours=24,
                           reason='Recette WordPress', **kwargs)
        db.commit()
        return row


def saved(lab, target):
    with lab['db'].session() as db:
        return db.get(ProjectRecipeAccess, target.id)


def sites(lab, target):
    with lab['db'].session() as db:
        return set(db.scalars(select(Project.base_url).where(Project.owner_user_id == target.id)))


def test_grant_and_audit_commit_together_without_changing_billing_roles_or_limits(lab):
    target = user(lab['db'])
    with lab['db'].session() as db:
        initial = billing.plan_limits(db, user_id=target.id)
        access.grant(db, actor=lab['admin'], user_id=target.id, base_url=ORIGIN, hours=24, reason='Rollback')
        db.rollback()
    assert saved(lab, target) is None
    row = grant(lab, target)
    with lab['db'].session() as db:
        audits = list(db.scalars(select(AuditLog).where(AuditLog.action == 'recipe_access.grant')))
        assert len(audits) == 1 and audits[0].actor_user_id == lab['admin'].id
        assert audits[0].target_id == target.id and audits[0].meta['grant_id'] == row.grant_id
        assert audits[0].meta['source'] == 'web' and not audits[0].meta['stripe_modified']
        assert not db.get(User, target.id).is_admin
        assert billing.effective_plan_key(db, user_id=target.id) == 'free'
        assert billing.plan_limits(db, user_id=target.id) == initial
        assert db.scalar(select(func.count()).select_from(BillingCustomer)) == 0
        assert db.scalar(select(func.count()).select_from(BillingSubscription)) == 0


@pytest.mark.parametrize('kind', ['anonymous', 'customer', 'other_admin'])
def test_reads_and_writes_require_the_system_owner_even_with_csrf(lab, kind):
    target = user(lab['db'])
    actor = None if kind == 'anonymous' else user(lab['db'], admin=kind == 'other_admin')
    c = client(actor)
    responses = [c.get(page(target.id), follow_redirects=False),
                 post(c, page(target.id), base_url=ORIGIN, hours='24', reason='Unauthorized', confirm='1'),
                 post(c, page(target.id)+'/revoke', grant_id='anything')]
    for response in responses:
        if kind == 'anonymous':
            assert response.status_code == 303 and response.headers['location'].startswith('/auth/login')
        else:
            assert response.status_code == 403
    assert saved(lab, target) is None


def test_owner_form_has_csrf_and_requires_explicit_confirmation(lab):
    target = user(lab['db']);c = client(lab['admin'])
    response = c.get(page(target.id));assert response.status_code == 200
    assert response.headers['cache-control'] == 'no-store' and response.headers['referrer-policy'] == 'no-referrer'
    soup = BeautifulSoup(response.text, 'html.parser')
    assert soup.select_one('form[action$="/recipe-access"] input[name=_csrf]')['value']
    assert soup.select_one('input[name=confirm]').has_attr('required')
    assert c.post(page(target.id), data={'base_url': ORIGIN, 'reason': 'Missing CSRF', 'confirm': '1'}).status_code == 403
    assert post(c, page(target.id), base_url=ORIGIN, reason='Missing confirmation').status_code == 303
    assert saved(lab, target) is None
    response = post(c, page(target.id), base_url=ORIGIN, reason='Recette', confirm='1')
    assert response.status_code == 303 and access.active(saved(lab, target))
    assert 'Accès de recette' in c.get('/settings/activation/accounts/'+target.id).text


@pytest.mark.parametrize('url', ['http://demo.example/', 'https://demo.example/path', 'https://demo.example/?x=1',
    'https://demo.example/#part', 'https://user:password@demo.example/', 'https://*.example/',
    'https://demo.example:8443/', 'https://demo.example/ x', 'https://'+'x'*2050+'.example/'])
def test_grants_refuse_ambiguous_or_overbroad_origins(lab, url):
    target = user(lab['db'])
    response = post(client(lab['admin']), page(target.id), base_url=url, reason='Bad origin', confirm='1')
    assert response.status_code == 303 and 'err=' in response.headers['location']
    assert saved(lab, target) is None


@pytest.mark.parametrize('hours', [0, -1, 49])
def test_duration_is_bounded(lab, hours):
    target = user(lab['db'])
    response = post(client(lab['admin']), page(target.id), base_url=ORIGIN, hours=str(hours), reason='Bad duration', confirm='1')
    assert 'err=' in response.headers['location'] and saved(lab, target) is None


def test_public_target_validation_still_runs_for_grant_and_addition(lab, monkeypatch):
    target = user(lab['db']);project(lab['db'], target)
    calls = []
    monkeypatch.setattr(m, '_validate_public_crawl_target', lambda url: calls.append(url) or 'private address')
    response = post(client(lab['admin']), page(target.id), base_url=ORIGIN, reason='Denied', confirm='1')
    assert 'err=' in response.headers['location'] and calls == [ORIGIN]
    grant(lab, target)
    post(client(target), '/projects/add', source='site-list', mode='domain', domain=ORIGIN)
    assert ORIGIN not in sites(lab, target) and calls == [ORIGIN, ORIGIN]


def test_only_the_granted_account_can_add_only_the_exact_site_over_its_limit(lab):
    target, other = user(lab['db']), user(lab['db'])
    for _ in range(10):project(lab['db'], target)
    project(lab['db'], other)
    c = client(target)
    assert post(c, '/projects/add', source='site-list', domain=ORIGIN).status_code == 409
    grant(lab, target)
    for origin in ['https://demo.example.evil/', 'https://sub.demo.example/', 'http://demo.example/']:
        assert post(c, '/projects/add', source='site-list', domain=origin).status_code == 409
    assert post(client(other), '/projects/add', source='site-list', domain=ORIGIN).status_code == 409
    assert post(c, '/projects/add', source='site-list', domain='https://DEMO.example:443/').status_code == 303
    assert any('demo.example' in url for url in sites(lab, target))
    assert ORIGIN not in sites(lab, other)


@pytest.mark.parametrize('lang,title', [('fr', 'Autorisation de recette temporaire'), ('en', 'Temporary test access')])
def test_the_actual_add_form_explains_and_enables_the_pending_permission_then_closes_it(lab, lang, title):
    target = user(lab['db']);project(lab['db'], target);grant(lab, target)
    c = client(target)
    response = c.get('/?add_site=1&lang='+lang)
    assert response.context['site_add']['limit'] == 1 and response.context['site_add']['can_add']
    soup = BeautifulSoup(response.text, 'html.parser')
    form = soup.select_one('.site-add-form')
    assert title in form.get_text() and form.select_one('input[name=domain]')['value'] == ORIGIN
    assert not form.select_one('button[type=submit]').has_attr('disabled')
    assert post(c, '/projects/add', source='site-list', domain=ORIGIN).status_code == 303
    response = c.get('/')
    assert not response.context['site_add']['recipe'] and not response.context['site_add']['can_add']


@pytest.mark.parametrize('mode', ['gsc', 'bing', 'bulk'])
def test_import_modes_do_not_extend_the_permission_to_other_sites(lab, mode):
    target = user(lab['db']);project(lab['db'], target);grant(lab, target)
    response = post(client(target), '/projects/add', mode=mode, bulk_urls=[ORIGIN, 'https://other.example/'])
    assert response.status_code == 303
    assert ORIGIN in sites(lab, target) and 'https://other.example/' not in sites(lab, target)


def test_expiration_is_checked_per_request_without_cron_and_keeps_existing_data(lab):
    target = user(lab['db']);existing = project(lab['db'], target)
    expires = datetime.now(timezone.utc) - timedelta(seconds=1)
    grant(lab, target, now=expires-timedelta(hours=24))
    assert not access.active(saved(lab, target), now=expires)
    assert post(client(target), '/projects/add', source='site-list', domain=ORIGIN).status_code == 409
    with lab['db'].session() as db:assert db.get(Project, existing.id) is not None


def test_renewal_invalidates_stale_revoke_and_current_revoke_is_idempotent(lab):
    target = user(lab['db']);old = grant(lab, target);current = grant(lab, target)
    assert old.grant_id != current.grant_id
    c = client(lab['admin'])
    response = post(c, page(target.id)+'/revoke', grant_id=old.grant_id)
    assert 'err=' in response.headers['location'] and access.active(saved(lab, target))
    for _ in range(2):assert post(c, page(target.id)+'/revoke', grant_id=current.grant_id).status_code == 303
    assert not access.active(saved(lab, target))
    with lab['db'].session() as db:
        assert db.scalar(select(func.count()).select_from(AuditLog).where(AuditLog.action == 'recipe_access.revoke')) == 1


def test_failed_audit_rolls_back_the_permission(lab, monkeypatch):
    target = user(lab['db'])
    with lab['db'].session() as db:
        add = db.add
        def save(obj):
            if isinstance(obj, AuditLog):raise RuntimeError('Audit unavailable')
            add(obj)
        monkeypatch.setattr(db, 'add', save)
        with pytest.raises(RuntimeError):
            access.grant(db, actor=lab['admin'], user_id=target.id, base_url=ORIGIN, hours=24, reason='Atomic')
        db.rollback()
    assert saved(lab, target) is None


def test_a_stale_database_session_cannot_revoke_a_concurrent_renewal(lab):
    target = user(lab['db']);old = grant(lab, target)
    with lab['db'].session() as stale_db:
        stale = stale_db.get(ProjectRecipeAccess, target.id)
        current = grant(lab, target)
        assert stale.grant_id == old.grant_id and current.grant_id != stale.grant_id
        with pytest.raises(ValueError):
            access.revoke(stale_db, actor=lab['admin'], user_id=target.id, grant_id=old.grant_id)
        stale_db.rollback()
    assert saved(lab, target).grant_id == current.grant_id and access.active(saved(lab, target))


def test_deleting_target_removes_permission_and_owner_cannot_grant_to_an_admin(lab):
    target = user(lab['db']);grant(lab, target)
    assert 'err=' in post(client(lab['admin']), page(lab['admin'].id), base_url=ORIGIN, reason='Admin', confirm='1').headers['location']
    with lab['db'].session() as db:
        db.delete(db.get(User, target.id));db.commit()
        assert db.get(ProjectRecipeAccess, target.id) is None


def operator():
    spec = importlib.util.spec_from_file_location('recipe_operator_test', Path(m.__file__).parents[1]/'ops/recipe_access.py')
    module = importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def test_operator_defaults_to_read_only_and_requires_exact_target_confirmation(lab, monkeypatch, capsys):
    target = user(lab['db']);tool = operator()
    args = ['recipe_access.py', 'grant', '--email', target.email, '--site', ORIGIN, '--reason', 'Operator recipe']
    monkeypatch.setattr(sys, 'argv', args);tool.main()
    assert not json.loads(capsys.readouterr().out)['applied'] and saved(lab, target) is None
    monkeypatch.setattr(sys, 'argv', args+['--apply', '--confirm-email', 'other@example.invalid'])
    with pytest.raises(SystemExit):tool.main()
    assert saved(lab, target) is None
    monkeypatch.setattr(sys, 'argv', args+['--apply', '--confirm-email', target.email]);tool.main()
    result = json.loads(capsys.readouterr().out);assert result['applied'] and result['active']
    with lab['db'].session() as db:
        audit = db.scalar(select(AuditLog).where(AuditLog.action == 'recipe_access.grant'))
        assert audit.meta['source'] == 'operator_console'


def test_operator_refuses_write_without_a_configured_present_system_owner(lab, monkeypatch):
    target = user(lab['db'])
    monkeypatch.setenv('SYSTEM_SETTINGS_OWNER_EMAIL', '');monkeypatch.setenv('BOOTSTRAP_ADMIN_EMAIL', '')
    monkeypatch.setattr(sys, 'argv', ['recipe_access.py', 'grant', '--email', target.email, '--site', ORIGIN,
        '--reason', 'Denied', '--apply', '--confirm-email', target.email])
    with pytest.raises(SystemExit):operator().main()
    assert saved(lab, target) is None
