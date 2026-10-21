"""Activer une fusion ou une publication automatique demande l'accord du client, et le garde.

En cas de litige, c'est au prestataire de prouver qu'il a mis son client en garde (CGU, articles 6
et 7) : l'accord est exigé au passage en mode automatique, et chaque activation ou retrait est
gardé dans le journal d'audit avec la version de l'avertissement montré.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from sqlalchemy import select

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_le_mode_automatique_est_etroit as _auto  # noqa: E402
from backend import app as m, legal  # noqa: E402
from backend.models import AuditLog, Project  # noqa: E402
from test_customer_activation import client, lab as lab, project, user  # noqa: E402

projet, plan, github, modele, client_connecte = _auto.projet, _auto.plan, _auto.github, _auto.modele, _auto.client_connecte
WEB = Path(__file__).resolve().parents[1]


@pytest.fixture()
def depot(lab, monkeypatch):
    owner = user(lab['db'])
    site = project(lab['db'], owner)
    monkeypatch.setattr(m, '_effective_user_connection_value', lambda **kwargs: ('jeton', 'user'))
    monkeypatch.setattr(m, '_github_api_get', lambda *args, **kwargs: {
        'permissions': {'push': True}, 'default_branch': 'main', 'html_url': 'https://github.com/atelier/site'})
    browser = client(owner)
    browser.get('/settings/accounts')
    return {'client': browser, 'slug': site.slug, 'project_id': site.id, 'db': lab['db']}


def connect(depot, **body):
    token = depot['client'].cookies.get(m._CSRF_COOKIE_NAME)
    return depot['client'].post(f"/api/projects/{depot['slug']}/github/connect",
                                json={'repo': 'atelier/site', 'branch': 'main', **body},
                                headers={m._CSRF_HEADER_NAME: token})


def journal(action):
    with m.DB.session() as db:
        return [row.meta for row in db.scalars(select(AuditLog).where(AuditLog.action == action))]


def test_full_access_needs_consent_and_each_change_is_kept(depot):
    refused = connect(depot, mode='auto')
    assert refused.status_code == 400 and 'Confirmez' in refused.json()['error']
    with depot['db'].session() as db:
        assert db.get(Project, depot['project_id']).settings.get('github_mode') != 'auto'
    assert journal('automation.github_mode') == []

    assert connect(depot, mode='auto', consent=True).json()['ok']
    assert connect(depot, mode='auto').json()['ok']  # déjà accepté : rien de nouveau à prouver
    assert connect(depot, mode='review').json()['ok']
    logs = journal('automation.github_mode')
    assert sorted((log['previous'], log['mode']) for log in logs) == [('auto', 'review'), ('review', 'auto')]
    activation = next(log for log in logs if log['mode'] == 'auto')
    assert activation['consent'] is True and activation['repo'] == 'atelier/site'
    assert activation['warning_version'] == legal.AUTO_MODE_WARNING_VERSION and activation['lang'] == 'fr'


def test_detaching_a_full_access_repository_is_kept_too(depot):
    connect(depot, mode='auto', consent=True)
    assert connect(depot, repo='', disconnect=True).json()['ok']
    assert {(log['previous'], log['mode']) for log in journal('automation.github_mode')} == {
        ('review', 'auto'), ('auto', 'disconnected')}


def test_automatic_publishing_of_articles_needs_consent_and_is_kept(client_connecte, plan):
    browser, slug, _pid = client_connecte
    refused = _auto._regler(browser, slug, enabled=True, section='/blog', publication='auto')
    assert refused.status_code == 400 and 'sans relecture humaine' in refused.json()['error']
    assert journal('automation.content_publication') == []
    assert _auto._regler(browser, slug, enabled=True, section='/blog', publication='auto', consent=True).status_code == 200
    assert _auto._regler(browser, slug, enabled=True, section='/blog', publication='auto').status_code == 200
    assert _auto._regler(browser, slug, enabled=True, section='/blog', publication='verification').status_code == 200
    logs = journal('automation.content_publication')
    assert sorted(log['publication'] for log in logs) == ['auto', 'verification']
    assert next(log for log in logs if log['publication'] == 'auto')['consent'] is True


def test_every_screen_that_turns_on_an_automatic_mode_asks_for_consent() -> None:
    templates = {name: (WEB / 'templates' / name).read_text(encoding='utf-8')
                 for name in ('crawl_settings.html', 'corrections.html', 'content.html')}
    assert 'sans relecture humaine préalable' in templates['crawl_settings.html']
    assert 'id="gh-c-consent"' in templates['corrections.html'] and 'consent: consent' in templates['corrections.html']
    assert 'id="c-auto-consent"' in templates['content.html'] and 'consent: !!(consentBox' in templates['content.html']
    assert 'consent:consent.checked' in (WEB / 'static' / 'analysis-github.js').read_text(encoding='utf-8')
