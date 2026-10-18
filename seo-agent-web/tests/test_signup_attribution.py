"""Registered links reach signup; explicit consent attributes only the new owning account."""
from __future__ import annotations

import csv
import io
import json
import uuid
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit

import pytest
from alembic import command
from alembic.config import Config
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, select, text

from backend import app as m, customer_activation, signup_attribution as attribution
from backend.models import AcquisitionCampaign, User
from test_customer_activation import account, client, lab as lab, project, user
from test_first_verified_correction import _ready, _verify


@pytest.fixture(autouse=True)
def no_signup_rate_limit(monkeypatch):
    monkeypatch.setattr(m, '_rate_limit_retry_after', lambda **kwargs: None)


@pytest.fixture()
def campaign(lab):
    return attribution.create(lab['db'], 'google', 'cpc', 'premier-audit-tpe')


def tagged(campaign, path='/auth/signup'):
    return path + '?' + urlencode(attribution.campaign_tags(campaign))


def signup(lab, campaign=None, consent='', *, email=None, password='test-password-for-campaign'):
    c = client()
    page = c.get(tagged(campaign) if campaign else '/auth/signup')
    assert page.status_code == 200
    address = email or f'campaign-{uuid.uuid4().hex}@example.invalid'
    payload = {'email': address, 'password': password, '_csrf': c.cookies.get(m._CSRF_COOKIE_NAME),
               'acquisition_consent': consent}
    if campaign:
        payload['acquisition_campaign'] = campaign.id
    response = c.post('/auth/signup', data=payload, follow_redirects=False)
    with lab['db'].session() as db:
        registered = db.scalar(select(User).where(User.email == address))
    return registered, response


def csrf_post(c, path, **data):
    c.get('/settings/activation' if path.startswith('/settings/activation') else '/settings/accounts')
    return c.post(path, data={'_csrf': c.cookies.get(m._CSRF_COOKIE_NAME), **data}, follow_redirects=False)


def test_owner_prepares_deduplicated_safe_links_and_other_users_cannot(lab):
    c = client(lab['admin'])
    for source in ['Google', 'google']:
        response = csrf_post(c, '/settings/activation/campaigns', source=source, medium='cpc', campaign='premier-audit-tpe')
        assert response.status_code == 303
    with lab['db'].session() as db:
        rows = list(db.scalars(select(AcquisitionCampaign)))
        assert len(rows) == 1
    page = c.get('/settings/activation')
    inputs = BeautifulSoup(page.text, 'html.parser').select('.campaign-copy input')
    assert len(inputs) == 1
    assert parse_qs(urlsplit(inputs[0]['value']).query) == {
        'utm_source': ['google'], 'utm_medium': ['cpc'], 'utm_campaign': ['premier-audit-tpe']}
    for outsider in [user(lab['db']), user(lab['db'], admin=True)]:
        assert csrf_post(client(outsider), '/settings/activation/campaigns', source='bing', medium='cpc', campaign='test').status_code == 403
    assert c.post('/settings/activation/campaigns', data={'source': 'bing', 'medium': 'cpc', 'campaign': 'test'}).status_code == 403


@pytest.mark.parametrize('source,medium,name', [('=formula', 'cpc', 'test'), ('google', 'invented', 'test'),
                                               ('x' * 41, 'cpc', 'test'), ('google', 'cpc', 'x' * 65),
                                               ('google', 'cpc', '<script>'), ('email@example.invalid', 'email', 'test')])
def test_unsafe_or_unbounded_campaign_labels_are_not_registered(lab, source, medium, name):
    with pytest.raises(ValueError):
        attribution.create(lab['db'], source, medium, name)


def test_public_navigation_carries_only_campaign_labels_without_changing_canonical_urls(lab, campaign):
    c = client()
    page = c.get(tagged(campaign, '/') + '&gclid=private-click&fbclid=private-click&email=visitor@example.invalid')
    soup = BeautifulSoup(page.text, 'html.parser')
    signup_links = [a['href'] for a in soup.select('a[href]') if a['href'].startswith('/auth/signup')]
    assert len(signup_links) >= 3
    assert all(parse_qs(urlsplit(url).query) == {key: [value] for key, value in attribution.campaign_tags(campaign).items()} for url in signup_links)
    assert 'utm_' not in soup.select_one('link[rel=canonical]')['href']
    assert 'private-click' not in page.text and 'visitor@example.invalid' not in page.text
    pricing = next(a['href'] for a in soup.select('a[href]') if a['href'].startswith('/pricing?'))
    prices = BeautifulSoup(c.get(pricing).text, 'html.parser')
    signup_url = next(a['href'] for a in prices.select('a[href]') if a['href'].startswith('/auth/signup?'))
    form = BeautifulSoup(c.get(signup_url).text, 'html.parser')
    assert form.select_one('input[name=acquisition_campaign]')['value'] == campaign.id
    consent = form.select_one('input[name=acquisition_consent]')
    assert consent and 'checked' not in consent.attrs and 'required' not in consent.attrs
    docs = BeautifulSoup(c.get(tagged(campaign, '/docs')).text, 'html.parser')
    article = next(a['href'] for a in docs.select('a[href]') if a['href'].startswith('/docs/ajouter-un-projet?'))
    assert 'utm_campaign=premier-audit-tpe' in c.get(article).text
    assert all(cookie.name in {m._CSRF_COOKIE_NAME} for cookie in c.cookies.jar)


@pytest.mark.parametrize('query', ['utm_source=google&utm_medium=cpc',
                                  'utm_source=google&utm_medium=cpc&utm_campaign=unregistered',
                                  'utm_source=google&utm_source=bing&utm_medium=cpc&utm_campaign=premier-audit-tpe',
                                  'utm_source=%3Cscript%3E&utm_medium=cpc&utm_campaign=premier-audit-tpe'])
def test_unknown_incomplete_and_ambiguous_tags_do_not_offer_false_attribution(lab, campaign, query):
    page = client().get('/auth/signup?' + query)
    assert page.status_code == 200 and 'name="acquisition_campaign"' not in page.text
    assert 'name="acquisition_consent"' not in page.text


@pytest.mark.parametrize('consent', ['', 'no', 'on'])
def test_optional_consent_is_required_for_origin_but_never_for_signup(lab, campaign, consent):
    registered, response = signup(lab, campaign, consent)
    assert registered and response.status_code == 303
    state = account(lab['db'], registered.id)
    assert state.signup_observed and state.acquisition_campaign_id is None and state.acquisition_consented_at is None
    group = customer_activation.snapshot(lab['db'])['acquisition'][0]
    assert group['source'] == '' and group['counts'] == [1, 0, 0, 0]


def test_email_signup_records_consent_once_and_rejected_retry_cannot_reassign(lab, campaign):
    registered, response = signup(lab, campaign, 'yes')
    assert response.status_code == 303
    state = account(lab['db'], registered.id)
    assert state.acquisition_campaign_id == campaign.id and state.acquisition_consented_at
    second = attribution.create(lab['db'], 'linkedin', 'paid_social', 'second-link')
    signup(lab, second, 'yes', email=registered.email)
    retained = account(lab['db'], registered.id)
    assert retained.acquisition_campaign_id == campaign.id and retained.acquisition_consented_at == state.acquisition_consented_at
    data = customer_activation.snapshot(lab['db'])
    assert data['acquisition'][0]['counts'] == [1, 0, 0, 0] and data['acquisition'][0]['source'] == 'google'


def test_rejected_form_keeps_the_registered_link_and_asks_for_consent_again(lab, campaign):
    registered, response = signup(lab, campaign, 'yes', password='short')
    assert registered is None and response.status_code == 303
    assert 'utm_campaign=premier-audit-tpe' in response.headers['location']
    form = BeautifulSoup(client().get(response.headers['location']).text, 'html.parser')
    assert form.select_one('input[name=acquisition_campaign]')['value'] == campaign.id
    assert 'checked' not in form.select_one('input[name=acquisition_consent]').attrs


def test_unknown_campaign_identifier_does_not_block_signup(lab):
    ghost = AcquisitionCampaign(id=str(uuid.uuid4()), source='google', medium='cpc', name='not-registered')
    registered, response = signup(lab, ghost, 'yes')
    assert registered and response.status_code == 303
    assert account(lab['db'], registered.id).acquisition_campaign_id is None


def google_start(lab, campaign, monkeypatch, *, consent='yes', mode='signup', email=None):
    from google.oauth2 import id_token
    email = email or f'google-campaign-{uuid.uuid4().hex}@example.invalid'
    monkeypatch.setattr(m, '_google_oauth_client', lambda: ('client-id', 'client-secret'))
    monkeypatch.setattr(m, '_google_oauth_exchange_code', lambda **kwargs: {'id_token': 'test-only-id-token'})
    monkeypatch.setattr(id_token, 'verify_oauth2_token', lambda *args, **kwargs: {
        'sub': 'google-' + email, 'email': email, 'email_verified': True})
    c = client()
    c.get(tagged(campaign))
    response = c.post('/auth/google/start', data={'mode': mode, 'acquisition_campaign': campaign.id,
                      'acquisition_consent': consent, '_csrf': c.cookies.get(m._CSRF_COOKIE_NAME)}, follow_redirects=False)
    assert response.status_code == 303
    state = parse_qs(urlsplit(response.headers['location']).query)['state'][0]
    return c, state, email


@pytest.mark.parametrize('consent', ['yes', ''])
def test_google_uses_the_signed_state_and_same_signup_consent(lab, campaign, monkeypatch, consent):
    c, state, email = google_start(lab, campaign, monkeypatch, consent=consent)
    payload = m._oauth_state_decode(state)
    assert payload.get('acquisition_campaign') == (campaign.id if consent else None)
    response = c.get('/auth/google/callback', params={'state': state, 'code': 'test-code'}, follow_redirects=False)
    assert response.status_code == 303 and 'err=' not in response.headers['location']
    with lab['db'].session() as db:
        registered = db.scalar(select(User).where(User.email == email))
    assert registered
    assert account(lab['db'], registered.id).acquisition_campaign_id == (campaign.id if consent else None)


@pytest.mark.parametrize('invalid', ['tampered', 'expired', 'wrong-csrf'])
def test_invalid_google_state_never_creates_or_attributes_an_account(lab, campaign, monkeypatch, invalid):
    c, state, email = google_start(lab, campaign, monkeypatch)
    if invalid == 'tampered':
        state += 'x'
    elif invalid == 'expired':
        payload = m._oauth_state_decode(state)
        payload['ts'] -= 21 * 60
        state = m._oauth_state_encode(payload)
    else:
        c.cookies.clear()
    response = c.get('/auth/google/callback', params={'state': state, 'code': 'test-code'}, follow_redirects=False)
    assert response.status_code == 303 and 'err=' in response.headers['location']
    with lab['db'].session() as db:
        assert db.scalar(select(User).where(User.email == email)) is None


@pytest.mark.parametrize('mode', ['login', 'signup'])
def test_existing_google_identity_and_login_cannot_restore_a_withdrawn_origin(lab, campaign, monkeypatch, mode):
    c, state, email = google_start(lab, campaign, monkeypatch)
    c.get('/auth/google/callback', params={'state': state, 'code': 'test-code'})
    with lab['db'].session() as db:
        registered = db.scalar(select(User).where(User.email == email))
    attribution.withdraw(lab['db'], registered.id)
    c, state, _ = google_start(lab, campaign, monkeypatch, email=email, mode=mode)
    assert m._oauth_state_decode(state).get('acquisition_campaign') == (campaign.id if mode == 'signup' else None)
    c.get('/auth/google/callback', params={'state': state, 'code': 'test-code'})
    assert account(lab['db'], registered.id).acquisition_campaign_id is None


def test_google_refusal_returns_to_the_campaign_form_without_prechecking_consent(lab, campaign, monkeypatch):
    c, state, _ = google_start(lab, campaign, monkeypatch)
    response = c.get('/auth/google/callback', params={'state': state, 'error': 'access_denied'}, follow_redirects=False)
    assert response.status_code == 303 and 'utm_campaign=premier-audit-tpe' in response.headers['location']
    form = BeautifulSoup(c.get(response.headers['location']).text, 'html.parser')
    assert form.select_one('input[name=acquisition_campaign]')['value'] == campaign.id
    assert 'checked' not in form.select_one('input[name=acquisition_consent]').attrs


def test_unconfigured_google_keeps_the_email_signup_available_with_its_origin(lab, campaign, monkeypatch):
    monkeypatch.setattr(m, '_google_oauth_client', lambda: ('', ''))
    c = client()
    c.get(tagged(campaign))
    response = c.post('/auth/google/start', data={'mode': 'signup', 'acquisition_campaign': campaign.id,
                      'acquisition_consent': 'yes', '_csrf': c.cookies.get(m._CSRF_COOKIE_NAME)}, follow_redirects=False)
    assert response.status_code == 303 and response.headers['location'].startswith('/auth/signup?')
    assert 'utm_campaign=premier-audit-tpe' in response.headers['location']


def test_new_customer_reaches_a_verified_result_in_the_same_campaign(lab, campaign):
    owner, _ = signup(lab, campaign, 'yes')
    row = project(lab['db'], owner)
    baseline = '20261001-100000'
    url = 'https://atelier.example/service/'
    runs_dir = m._runs_dir_for_user(owner.id)

    def report(ts=baseline, *, issues=None, urls=(url,), blocked=False):
        value = {'meta': {'base_url': row.base_url, 'blocked_by_host': {'count': int(blocked)}},
                 'pages': [{'url': item, 'status_code': 200, 'content_type': 'text/html', 'title': 'Service'} for item in urls],
                 'issues': issues if issues is not None else {'missing_title': {'count': 1, 'examples': [url]}}}
        target = runs_dir / row.slug / ts / 'audit' / 'report.json'
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(value))
        return value

    data = {'owner': owner.id, 'client': client(owner), 'slug': row.slug, 'project_id': row.id,
            'report': report, 'runs_dir': runs_dir}
    _ready(data)
    _verify(data)
    state = account(lab['db'], owner.id)
    assert state.correction_verified_at and state.acquisition_campaign_id == campaign.id
    metrics = customer_activation.snapshot(lab['db'])
    assert metrics['acquisition'][0]['counts'] == [1, 1, 1, 1] and metrics['acquisition'][0]['rate'] == 100.0


def test_withdrawal_erases_only_own_origin_preserves_milestones_and_reconciles_exports(lab, campaign):
    one, _ = signup(lab, campaign, 'yes')
    two, _ = signup(lab, campaign, 'yes')
    project(lab['db'], one)
    c = client(one)
    page = c.get('/settings/accounts')
    assert 'Retirer mon accord et effacer l’origine' in page.text
    assert c.post('/settings/account/attribution/withdraw', data={'user_id': two.id}).status_code == 403
    response = csrf_post(c, '/settings/account/attribution/withdraw', user_id=two.id)
    assert response.status_code == 303
    own = account(lab['db'], one.id)
    assert own.acquisition_campaign_id is None and own.acquisition_consented_at is None and own.project_added_at
    assert account(lab['db'], two.id).acquisition_campaign_id == campaign.id
    admin = client(lab['admin'])
    data = admin.get('/api/settings/activation').json()
    assert [sum(group['counts'][i] for group in data['acquisition']) for i in range(4)] == [step['count'] for step in data['steps']]
    assert data['steps'][0]['count'] == 2 and len(data['acquisition']) == 2
    csv_response = admin.get('/settings/activation/acquisition.csv')
    assert csv_response.status_code == 200 and csv_response.headers['cache-control'] == 'no-store'
    rows = list(csv.reader(io.StringIO(csv_response.content.decode('utf-8-sig')), delimiter=';'))
    assert sum(int(row[5]) for row in rows[1:]) == 2
    for private in [one.email, two.email, one.id, two.id, campaign.id]:
        assert private not in csv_response.text and private not in json.dumps(data)
    assert client(one).get('/settings/activation/acquisition.csv').status_code == 403
    assert 'Aucune campagne d’origine' in c.get('/settings/accounts').text


def test_migration_preserves_existing_cohorts_without_inventing_origins(tmp_path, monkeypatch):
    url = 'sqlite:///' + str(tmp_path / 'migration.db')
    monkeypatch.setenv('DATABASE_URL', url)
    config = Config(str(Path(__file__).resolve().parents[1] / 'alembic.ini'))
    command.upgrade(config, '20261018_0015')
    engine = create_engine(url)
    with engine.begin() as connection:
        connection.execute(text("INSERT INTO users (id,email,password_hash,is_admin) VALUES ('old','old@example.invalid','test',0)"))
        connection.execute(text("INSERT INTO account_activations (user_id,signup_observed,invited_signup,project_added_at) VALUES ('old',1,0,'2026-10-01 10:00:00')"))
    command.upgrade(config, 'head')
    with engine.connect() as connection:
        row = connection.execute(text('SELECT * FROM account_activations WHERE user_id = :id'), {'id': 'old'}).mappings().one()
        assert row['signup_observed'] and row['project_added_at']
        assert row['acquisition_campaign_id'] is None and row['acquisition_consented_at'] is None
    command.downgrade(config, '20261018_0015')
    with engine.connect() as connection:
        assert connection.scalar(text('SELECT count(*) FROM account_activations')) == 1
    command.upgrade(config, 'head')
    engine.dispose()
