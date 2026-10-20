"""Account entry offers usable methods without changing signup authority or return paths."""
from urllib.parse import parse_qs, urlsplit

import pytest
from bs4 import BeautifulSoup
from sqlalchemy import select

from backend import app as m
from backend.models import User
from test_customer_activation import client, lab as lab


@pytest.fixture(autouse=True)
def local_only(monkeypatch):
    monkeypatch.setattr(m, '_rate_limit_retry_after', lambda **kwargs: None)
    monkeypatch.setattr(m, '_send_welcome_email', lambda **kwargs: None)
    monkeypatch.setattr(m, '_send_email_verification_email', lambda **kwargs: None)


@pytest.mark.parametrize('lang', ['fr', 'en'])
@pytest.mark.parametrize('credentials,available', [
    (('', ''), False), (('client', ''), False),
    (('', 'secret'), False), (('client', 'secret'), True),
])
def test_google_visibility_matches_configuration_on_entry_and_login_error(lab, monkeypatch, lang, credentials, available):
    monkeypatch.setattr(m, '_google_oauth_client', lambda: credentials)
    c = client()
    destination = '/projects/atelier/overview?crawl=20261001-100000#next-action'
    for path in ['/auth/login', '/auth/signup']:
        response = c.get(path, params={'lang': lang, 'next': destination})
        assert response.status_code == 200
        assert response.headers['cache-control'] == 'no-store'
        soup = BeautifulSoup(response.text, 'html.parser')
        assert soup.html['lang'] == lang
        form = soup.select_one('form')
        assert form.select_one('[name=next]')['value'] == destination
        assert form.select_one('[name=_csrf]')['value']
        assert bool(form.select_one('[formaction="/auth/google/start"]')) is available
        assert form.select_one('button[type=submit]').get('formaction') is None
        assert form.select_one('[name=password]').get('value') is None
    failed = c.post('/auth/login', data={
        '_csrf': c.cookies.get(m._CSRF_COOKIE_NAME), 'email': 'unknown@example.invalid',
        'password': 'private-password-not-to-echo', 'next': destination,
    })
    assert failed.status_code == 401 and failed.headers['cache-control'] == 'no-store'
    soup = BeautifulSoup(failed.text, 'html.parser')
    assert soup.select_one('[role=alert]')
    assert bool(soup.select_one('[formaction="/auth/google/start"]')) is available
    assert soup.select_one('[name=email]')['value'] == 'unknown@example.invalid'
    assert soup.select_one('[name=next]')['value'] == destination
    assert 'private-password-not-to-echo' not in failed.text
    if not available:
        # A bookmark or stale page cannot bypass the existing POST guard.
        refused = c.post('/auth/google/start', data={
            '_csrf': c.cookies.get(m._CSRF_COOKIE_NAME), 'mode': 'login', 'next': destination,
        }, follow_redirects=False)
        assert refused.status_code == 303
        target = urlsplit(refused.headers['location'])
        assert target.path == '/auth/login' and parse_qs(target.query)['next'] == [destination]
        assert 'err' in parse_qs(target.query)


@pytest.mark.parametrize('lang', ['fr', 'en'])
def test_switching_entry_keeps_email_destination_and_localized_help(lab, monkeypatch, lang):
    monkeypatch.setattr(m, '_google_oauth_client', lambda: ('', ''))
    c = client()
    params = {'lang': lang, 'email': 'Client+atelier@example.invalid',
              'next': '/team/accept?token=local-token', 'country': 'BE'}
    for path, target in [('/auth/login', '/auth/signup'), ('/auth/signup', '/auth/login')]:
        soup = BeautifulSoup(c.get(path, params=params).text, 'html.parser')
        switch = soup.select_one(f'.auth-entry-footer a[href^="{target}"]')
        query = parse_qs(urlsplit(switch['href']).query)
        assert query['next'] == [params['next']]
        assert query['email'] == ['client+atelier@example.invalid']
        followed = BeautifulSoup(c.get(switch['href']).text, 'html.parser')
        assert followed.select_one('[name=email]')['value'] == 'client+atelier@example.invalid'
        assert soup.select_one('.auth-entry-help')['href'] == ('/en/support' if lang == 'en' else '/support')
    # The email and next values remain HTML-escaped; an external destination is rejected.
    unsafe = BeautifulSoup(c.get('/auth/login', params={
        'next': 'https://untrusted.example/', 'email': '"><script>alert(1)</script>@example.invalid',
    }).text, 'html.parser')
    assert unsafe.select_one('[name=next]')['value'] == '/'
    assert not unsafe.select('script')


def test_team_signup_fallback_does_not_grant_registration_from_a_next_parameter(lab, monkeypatch):
    monkeypatch.setenv('SIGNUP_INVITE_CODE', 'required-local-code')
    monkeypatch.setattr(m, '_google_oauth_client', lambda: ('client', 'secret'))
    c = client()
    ordinary = BeautifulSoup(c.get('/auth/signup').text, 'html.parser')
    assert ordinary.select_one('[name=invite_code]')
    assert ordinary.select_one('[formaction="/auth/google/start"]')
    team = BeautifulSoup(c.get('/auth/signup', params={'next': '/team/accept?token=untrusted'}).text, 'html.parser')
    assert not team.select_one('[name=invite_code]')
    assert not team.select_one('[formaction="/auth/google/start"]')
    assert team.select_one('form')['action'] == '/auth/signup'
    response = c.post('/auth/signup', data={
        '_csrf': c.cookies.get(m._CSRF_COOKIE_NAME), 'next': '/team/accept?token=untrusted',
        'email': 'not-invited@example.invalid', 'password': 'local-safe-password',
        'country': 'FR', 'country_requested': 'yes',
    }, follow_redirects=False)
    assert response.status_code == 303 and 'err' in parse_qs(urlsplit(response.headers['location']).query)
    with lab['db'].session() as db:
        assert db.scalar(select(User).where(User.email == 'not-invited@example.invalid')) is None


def test_closed_signup_keeps_recovery_and_server_restrictions(lab, monkeypatch):
    monkeypatch.setenv('SIGNUP_DISABLED', 'true')
    monkeypatch.setattr(m, '_google_oauth_client', lambda: ('client', 'secret'))
    c = client()
    soup = BeautifulSoup(c.get('/auth/signup').text, 'html.parser')
    assert soup.select_one('[role=status]')
    assert not soup.select('form, button[type=submit]')
    assert soup.select_one('a[href^="/auth/login"]')
    assert soup.select_one('.auth-entry-help')
    response = c.post('/auth/signup', data={
        '_csrf': c.cookies.get(m._CSRF_COOKIE_NAME), 'email': 'closed@example.invalid',
        'password': 'local-safe-password',
    }, follow_redirects=False)
    assert response.status_code == 303 and 'err' in parse_qs(urlsplit(response.headers['location']).query)
    with lab['db'].session() as db:
        assert db.scalar(select(User).where(User.email == 'closed@example.invalid')) is None
