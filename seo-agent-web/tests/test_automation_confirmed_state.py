"""Read back an uncertain save without repeating its schedule, consent or quota effects."""
from __future__ import annotations

import copy

import pytest
from sqlalchemy import select

from backend import app as m
from backend.models import AccountMember, AuditLog, Project, UsageEvent
from test_customer_activation import client, lab as lab, project, user


def stored(pid):
    with m.DB.session() as db:
        return copy.deepcopy(db.get(Project, pid).settings)


def receipts():
    with m.DB.session() as db:
        return [row.id for row in db.scalars(select(AuditLog).where(
            AuditLog.action == 'automation.content_publication'))]


@pytest.fixture()
def site(lab):
    owner = user(lab['db'])
    row = project(lab['db'], owner, slug='atelier')
    browser = client(owner)
    browser.get('/settings/accounts')
    return browser, row


def save(browser, **values):
    return browser.post('/api/projects/atelier/content/auto', json=values,
                        headers={m._CSRF_HEADER_NAME: browser.cookies.get(m._CSRF_COOKIE_NAME)})


def test_read_after_a_lost_save_never_repeats_its_effects(site, monkeypatch):
    browser, row = site
    monkeypatch.setattr(m, '_article_gate', lambda *args, **kw: (True, ''))
    response = save(browser, enabled=True, section='  guides/ ', publication='auto', consent=True,
                    langues=[' EN ', 'fr'], jour=3, heure=16, fuseau='Europe/Paris')
    assert response.status_code == 200
    confirmed = response.json()
    before, logs = stored(row.id), receipts()
    with m.DB.session() as db:
        usage = [(event.id, event.amount) for event in db.scalars(select(UsageEvent))]
    assert len(logs) == 1
    # The browser can discard that response and recover with GET, even after a downgrade.
    def forbidden(*args, **kwargs):
        raise AssertionError('Reading settings must not use gates, providers or the scheduler')
    for name in ('_article_gate', '_github_api_get', '_github_api_post', '_balayer_contenu_auto'):
        monkeypatch.setattr(m, name, forbidden)
    for _ in range(2):
        result = browser.get('/api/projects/atelier/content/auto')
        assert result.status_code == 200 and result.headers['cache-control'] == 'no-store'
        data = result.json()
        assert data['settings'] == confirmed['settings']
        assert data['settings']['section'] == '/guides'
        assert data['settings']['langues'] == ['en', 'fr']
        assert data['settings']['enabled'] is True and data['prochain']
        assert 'last_run' not in data['settings'] and 'depuis' not in data['settings']
    assert stored(row.id) == before and receipts() == logs
    with m.DB.session() as db:
        assert [(event.id, event.amount) for event in db.scalars(select(UsageEvent))] == usage


def test_read_observes_a_later_stop_without_rescheduling(site, monkeypatch):
    browser, row = site
    monkeypatch.setattr(m, '_article_gate', lambda *args, **kw: (True, ''))
    assert save(browser, enabled=True, section='/blog', publication='auto', consent=True).status_code == 200
    # Stopping must remain possible when the account can no longer write articles.
    monkeypatch.setattr(m, '_article_gate', lambda *args, **kw: (False, 'quota exhausted'))
    assert save(browser, enabled=False, section='/blog', publication='auto').status_code == 200
    before, logs = stored(row.id), receipts()
    result = browser.get('/api/projects/atelier/content/auto').json()
    assert result['enabled'] is False and result['settings']['enabled'] is False
    assert result['prochain'] == ''
    assert stored(row.id) == before and receipts() == logs


@pytest.mark.parametrize('actor', ['owner', 'member', 'stranger', 'anonymous'])
def test_read_is_scoped_to_accessible_accounts(lab, actor):
    owner, member, stranger = [user(lab['db']) for _ in range(3)]
    row = project(lab['db'], owner, slug='atelier')
    # A same-slug private project must never substitute for the accessible project.
    other = project(lab['db'], stranger, slug='atelier')
    with m.DB.session() as db:
        db.add(AccountMember(owner_user_id=owner.id, member_user_id=member.id))
        db.get(Project, row.id).settings = {'content_auto': {'enabled': True, 'section': '/owner-only'}}
        db.get(Project, other.id).settings = {'content_auto': {'section': '/private'}}
        db.commit()
    browser = client({'owner': owner, 'member': member, 'stranger': stranger}.get(actor))
    result = browser.get('/api/projects/atelier/content/auto', follow_redirects=False)
    if actor == 'anonymous':
        assert result.status_code == 401
    else:
        assert result.status_code == 200
        assert result.json()['settings']['section'] == ('/private' if actor == 'stranger' else '/owner-only')
    denied = browser.get('/api/projects/missing-project/content/auto', follow_redirects=False)
    assert denied.status_code == (401 if actor == 'anonymous' else 404)


def test_unrelated_account_cannot_read_a_private_automation(lab):
    owner = user(lab['db'])
    project(lab['db'], owner, slug='secret')
    assert client(user(lab['db'])).get('/api/projects/secret/content/auto').status_code == 404


def test_legacy_settings_can_be_read_without_rewriting_them(site):
    browser, row = site
    with m.DB.session() as db:
        db.get(Project, row.id).settings = {'content_auto': {'jour': 99, 'langues': 'old', 'fuseau': 'invalid'},
                                          'other_setting': 'preserved'}
        db.commit()
    before = stored(row.id)
    data = browser.get('/api/projects/atelier/content/auto').json()
    assert data['settings']['enabled'] is False
    assert data['settings']['jour'] == 0 and data['settings']['langues'] == []
    assert data['settings']['fuseau'] == 'Europe/Paris'
    assert stored(row.id) == before
