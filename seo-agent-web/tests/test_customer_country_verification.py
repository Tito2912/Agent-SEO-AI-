"""Country is declared by the customer; email verification remains backed by actual evidence."""
from __future__ import annotations

import csv
import io
import uuid
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
from bs4 import BeautifulSoup
from sqlalchemy import select

from backend import app as m, customer_accounts, customer_country
from backend.models import OAuthIdentity, User
from test_customer_activation import client, lab as lab, user


@pytest.fixture(autouse=True)
def local_only(monkeypatch):
    monkeypatch.setattr(m, '_rate_limit_retry_after', lambda **kwargs: None)
    monkeypatch.setattr(m, '_send_welcome_email', lambda **kwargs: None)
    monkeypatch.setattr(m, '_send_email_verification_email', lambda **kwargs: None)


def post(c, path, **data):
    c.get('/auth/signup' if path.startswith('/auth/') else '/settings/accounts')
    return c.post(path, data={'_csrf': c.cookies.get(m._CSRF_COOKIE_NAME), **data}, follow_redirects=False)


def google(lab, monkeypatch, *, email=None, country='FR', claim=True, subject=None):
    from google.oauth2 import id_token
    address = email or f'country-google-{uuid.uuid4().hex}@example.invalid'
    monkeypatch.setattr(m, '_google_oauth_client', lambda: ('client-id', 'client-secret'))
    monkeypatch.setattr(m, '_google_oauth_exchange_code', lambda **kwargs: {'id_token': 'local-test-only'})
    monkeypatch.setattr(id_token, 'verify_oauth2_token', lambda *args, **kwargs: {
        'sub': subject or 'google-' + address, 'email': address, 'email_verified': claim})
    c = client()
    response = post(c, '/auth/google/start', mode='signup', country=country, country_requested='yes')
    state = parse_qs(urlsplit(response.headers['location']).query)['state'][0]
    return c, state, address


@pytest.mark.parametrize('code', ['FR', 'BE', 'CA', 'DZ', 'TN', 'CI', 'RE'])
def test_new_email_form_records_declared_country(lab, code):
    c = client()
    address = f'country-{uuid.uuid4().hex}@example.invalid'
    response = post(c, '/auth/signup', email=address, password='safe-test-password', country=code.lower(), country_requested='yes')
    assert response.status_code == 303
    with lab['db'].session() as db:
        registered = db.scalar(select(User).where(User.email == address))
        assert registered and registered.country == code
        assert not m._user_email_verified(db, user_id=registered.id)
    result = customer_accounts.snapshot(lab['db'], q=address)['rows'][0]
    assert result['country'] == code and result['country_source'] == 'Déclaré par le client'


@pytest.mark.parametrize('country', ['', 'XX', '<script>', 'FR,US'])
def test_missing_or_invalid_country_is_rejected_by_new_forms(lab, monkeypatch, country):
    c = client()
    address = f'country-bad-{uuid.uuid4().hex}@example.invalid'
    response = post(c, '/auth/signup', email=address, password='safe-test-password', country=country, country_requested='yes')
    assert response.status_code == 303 and 'err=' in response.headers['location']
    with lab['db'].session() as db:
        assert not db.scalar(select(User.id).where(User.email == address))
    monkeypatch.setattr(m, '_google_oauth_client', lambda: (_ for _ in ()).throw(AssertionError('country checked before OAuth')))
    response = post(c, '/auth/google/start', mode='signup', country=country, country_requested='yes')
    assert response.status_code == 303 and response.headers['location'].startswith('/auth/signup?')


def test_old_forms_and_countryless_accounts_are_not_assigned_a_guessed_country(lab):
    c = client()
    c.get('/auth/signup', headers={'CF-IPCountry': 'FR', 'Accept-Language': 'fr-FR'})
    address = f'country-legacy-{uuid.uuid4().hex}@example.invalid'
    response = c.post('/auth/signup', data={'email': address, 'password': 'safe-test-password',
                      '_csrf': c.cookies.get(m._CSRF_COOKIE_NAME)}, headers={'CF-IPCountry': 'FR', 'Accept-Language': 'fr-FR'}, follow_redirects=False)
    assert response.status_code == 303
    with lab['db'].session() as db:
        registered = db.scalar(select(User).where(User.email == address))
        assert registered.country is None
    page = client(registered).get('/settings/accounts')
    soup = BeautifulSoup(page.text, 'html.parser')
    assert soup.select_one('.country-prompt')
    assert soup.select_one('#account-declared-country').find('option', selected=True) is None


def test_signup_errors_keep_the_declared_choice(lab, monkeypatch):
    c = client()
    response = post(c, '/auth/signup', email='error@example.invalid', password='short', country='CA', country_requested='yes')
    assert parse_qs(urlsplit(response.headers['location']).query)['country'] == ['CA']
    soup = BeautifulSoup(c.get(response.headers['location']).text, 'html.parser')
    assert soup.select_one('#signup-country option[selected]')['value'] == 'CA'
    monkeypatch.setattr(m, '_google_oauth_client', lambda: ('', ''))
    response = post(c, '/auth/google/start', mode='signup', country='BE', country_requested='yes')
    assert parse_qs(urlsplit(response.headers['location']).query)['country'] == ['BE']


def test_google_signup_uses_country_from_signed_state_and_records_real_proof_without_mail(lab, monkeypatch):
    c, state, address = google(lab, monkeypatch, country='BE')
    assert m._oauth_state_decode(state)['country'] == 'BE'
    assert m._email_verification_enabled() is False
    response = c.get('/auth/google/callback', params={'state': state, 'code': 'local-test', 'country': 'US'}, follow_redirects=False)
    assert response.status_code == 303
    with lab['db'].session() as db:
        registered = db.scalar(select(User).where(User.email == address))
        assert registered.country == 'BE' and m._user_email_verified(db, user_id=registered.id)
    assert customer_accounts.snapshot(lab['db'], q=address)['rows'][0]['email_verified']


@pytest.mark.parametrize('verification_required', [False, True])
def test_signup_counts_a_connection_only_if_it_really_opens_a_session(lab, monkeypatch, verification_required):
    monkeypatch.setattr(m, '_email_verification_enabled', lambda: verification_required)
    c = client()
    address = f'country-session-{uuid.uuid4().hex}@example.invalid'
    response = post(c, '/auth/signup', email=address, password='safe-test-password', country='FR', country_requested='yes')
    assert response.status_code == 303
    row = customer_accounts.snapshot(lab['db'], q=address)['rows'][0]
    assert bool(row['last_login_at']) is (not verification_required)
    assert row['email_verified'] is False


@pytest.mark.parametrize('claim', [False, None, 'true'])
def test_google_identity_without_true_verified_claim_never_becomes_email_proof(lab, monkeypatch, claim):
    c, state, address = google(lab, monkeypatch, claim=claim)
    c.get('/auth/google/callback', params={'state': state, 'code': 'local-test'}, follow_redirects=False)
    with lab['db'].session() as db:
        registered = db.scalar(select(User).where(User.email == address))
        assert registered and not m._user_email_verified(db, user_id=registered.id)


@pytest.mark.parametrize('linked', [False, True])
def test_fresh_google_verification_updates_existing_account_without_changing_declared_country(lab, monkeypatch, linked):
    owner = user(lab['db'])
    with lab['db'].session() as db:
        db.get(User, owner.id).country = 'FR'
        if linked:
            db.add(OAuthIdentity(user_id=owner.id, provider='google', provider_user_id='google-' + owner.email, email=owner.email))
        db.commit()
        assert not m._user_email_verified(db, user_id=owner.id)
    c, state, _ = google(lab, monkeypatch, email=owner.email, country='CA')
    c.get('/auth/google/callback', params={'state': state, 'code': 'local-test'}, follow_redirects=False)
    with lab['db'].session() as db:
        assert db.get(User, owner.id).country == 'FR' and m._user_email_verified(db, user_id=owner.id)


def test_google_email_collision_does_not_mark_unrelated_old_email_as_verified(lab, monkeypatch):
    owner, conflicting = user(lab['db']), user(lab['db'])
    with lab['db'].session() as db:
        db.add(OAuthIdentity(user_id=owner.id, provider='google', provider_user_id='existing-subject', email=owner.email))
        db.commit()
    c, state, _ = google(lab, monkeypatch, email=conflicting.email, subject='existing-subject')
    c.get('/auth/google/callback', params={'state': state, 'code': 'local-test'}, follow_redirects=False)
    with lab['db'].session() as db:
        assert db.get(User, owner.id).email == owner.email
        assert not m._user_email_verified(db, user_id=owner.id)


def test_tampered_google_state_never_saves_country_or_verification(lab, monkeypatch):
    c, state, address = google(lab, monkeypatch)
    head, tail = state.split('.', 1)
    tampered = head + '.' + ('A' if tail[0] != 'A' else 'B') + tail[1:]
    c.get('/auth/google/callback', params={'state': tampered, 'code': 'local-test'}, follow_redirects=False)
    with lab['db'].session() as db:
        assert not db.scalar(select(User.id).where(User.email == address))


def test_existing_customer_saves_only_own_country_with_csrf_and_preserves_preferences(lab):
    owner, other = user(lab['db']), user(lab['db'])
    with lab['db'].session() as db:
        row = db.get(User, owner.id)
        row.timezone, row.language = 'Europe/Paris', 'fr'
        db.commit()
    c = client(owner)
    assert c.post('/settings/account/country', data={'country': 'FR'}).status_code == 403
    response = post(c, '/settings/account/country', country='DZ', user_id=other.id, next='https://example.invalid/evil')
    assert response.status_code == 303 and response.headers['location'].startswith('/?')
    with lab['db'].session() as db:
        saved = db.get(User, owner.id)
        assert (saved.country, saved.timezone, saved.language) == ('DZ', 'Europe/Paris', 'fr')
        assert db.get(User, other.id).country is None
    assert not BeautifulSoup(c.get('/settings/accounts').text, 'html.parser').select_one('.country-prompt')
    assert customer_accounts.snapshot(lab['db'], q=owner.id)['rows'][0]['last_login_at'] == ''
    for value in ['', 'XX', '<script>']:
        assert post(c, '/settings/account/country', country=value).status_code == 400
    anonymous = client()
    anonymous.get('/auth/signup')
    response = anonymous.post('/settings/account/country', data={'country': 'FR', '_csrf': anonymous.cookies.get(m._CSRF_COOKIE_NAME)}, follow_redirects=False)
    assert response.status_code in (303, 401)


def test_preferences_support_same_full_country_catalog_and_can_remove_declaration(lab):
    owner = user(lab['db'])
    c = client(owner)
    assert post(c, '/settings/account/preferences', country='CI', timezone='UTC', language='fr').status_code == 303
    page = BeautifulSoup(c.get('/settings/accounts').text, 'html.parser')
    assert page.select_one('#preference-country option[selected]')['value'] == 'CI'
    assert len(page.select('#preference-country option')) == 250
    assert post(c, '/settings/account/preferences', country='', timezone='UTC', language='fr').status_code == 303
    with lab['db'].session() as db:
        assert db.get(User, owner.id).country is None


@pytest.mark.parametrize('enabled,disabled,key', [(True, False, 'required'), (False, True, 'disabled'), (False, False, 'unconfigured')])
def test_verification_diagnostic_reports_current_policy_without_mail_or_secrets(enabled, disabled, key):
    module = SimpleNamespace(_email_verification_enabled=lambda: enabled, _env_bool=lambda name: disabled)
    assert customer_country.verification_mode(module)['key'] == key


def test_unavailable_diagnostic_does_not_claim_verification_is_disabled():
    def unavailable():
        raise RuntimeError('private connection information')
    result = customer_country.verification_mode(SimpleNamespace(_email_verification_enabled=unavailable))
    assert result['key'] == 'unknown' and 'private' not in str(result)


def test_owner_sees_data_coverage_country_and_exports_without_backfill(lab, monkeypatch):
    owner, empty = user(lab['db']), user(lab['db'])
    post(client(owner), '/settings/account/country', country='FR')
    monkeypatch.setenv('EMAIL_VERIFICATION_DISABLED', 'true')
    data = customer_accounts.snapshot(lab['db'], q=owner.email)
    assert data['matched'] == 1 and data['coverage'] == {'country': 1, 'verified': 0, 'with_sites': 0, 'with_analyses': 0, 'with_login': 0}
    assert customer_accounts.snapshot(lab['db'], usage='with_country')['matched'] == 1
    assert customer_accounts.snapshot(lab['db'], usage='without_country')['rows'][0]['user_id'] == empty.id
    c = client(lab['admin'])
    page = BeautifulSoup(c.get('/settings/activation/accounts').text, 'html.parser')
    assert page.select_one('[data-verification-mode=disabled]')
    assert 'France' in page.select_one('.account-results').text
    rows = list(csv.DictReader(io.StringIO(c.get('/settings/activation/accounts.csv').content.decode('utf-8-sig')), delimiter=';'))
    row = next(row for row in rows if row['Identifiant du compte'] == owner.id)
    assert (row['Pays renseigné'], row['Nom du pays'], row['Source du pays']) == ('FR', 'France', 'Déclaré par le client')
    assert 'France' in c.get('/settings/activation/accounts/' + owner.id).text
    assert 'Source du pays;Déclaré par le client' in c.get('/settings/activation/accounts/' + owner.id + '/export.csv').text
