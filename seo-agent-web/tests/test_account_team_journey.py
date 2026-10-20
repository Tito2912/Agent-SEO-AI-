"""Account orientation preserves payer scope, invitation guards and private admin context."""
from datetime import timedelta
from urllib.parse import parse_qs, urlencode, urlsplit

import pytest
from bs4 import BeautifulSoup
from sqlalchemy import select
from starlette.requests import Request

from backend import app as m, auth
from backend.models import AccountInvite, AccountMember, User
from test_first_verified_correction import journey as journey
from test_content_visibility_journey import subscribe
from test_customer_activation import lab as lab, user, client


def test_shared_site_billing_keeps_the_members_personal_plan(journey, monkeypatch):
    monkeypatch.setattr(m.billing, 'stripe_enabled', lambda: False)
    subscribe(journey)
    slug = journey['slug']
    path = f'/projects/{slug}/performance?range=custom&start=2026-09-01&q=v%C3%A9lo'
    page = journey['client'].get('/billing', params={'project': slug, 'return_to': path})
    assert page.status_code == 200 and page.headers['cache-control'] == 'no-store'
    assert page.context['plan_key'] == 'free'
    assert page.context['billing_context']['plan_label'] == 'Pro'
    scope = BeautifulSoup(page.text, 'html.parser').select_one('.billing-scope')
    assert 'Votre achat personnel ne les modifiera pas.' in scope.text
    assert scope.find('a')['href'] == path
    assert page.context['subscription'] is None and page.context['invoices'] == []
    assert journey['stranger'].get('/billing', params={'project': slug}).status_code == 404


@pytest.mark.parametrize('destination', ['https://evil.invalid/', '//evil.invalid/', '/projects/elsewhere/content', '/settings/system'])
def test_billing_return_stays_in_the_authorized_site(journey, monkeypatch, destination):
    monkeypatch.setattr(m.billing, 'stripe_enabled', lambda: False)
    page = journey['client'].get('/billing', params={'project': journey['slug'], 'return_to': destination})
    assert page.context['billing_context']['return_url'] == '/projects/' + journey['slug']


def test_own_site_and_shared_account_without_site_are_explicit(journey, monkeypatch):
    monkeypatch.setattr(m.billing, 'stripe_enabled', lambda: False)
    subscribe(journey)
    with m.DB.session() as db:
        owner = db.get(User, journey['owner'])
        owner_email = owner.email
    own = client(owner).get('/billing', params={'project': journey['slug']})
    assert own.context['plan_key'] == 'pro' and own.context['billing_context']['shared'] is False
    member = journey['client'].get('/billing')
    assert member.context['billing_context'] is None and owner_email in member.text
    assert 'Votre abonnement personnel ne change pas leurs limites.' in member.text


def test_preferences_first_and_supplier_lists_wait_for_connection(journey, monkeypatch):
    monkeypatch.setattr(m, '_effective_user_connection_value', lambda **kw: ('', 'none'))
    page = journey['client'].get('/settings/accounts')
    assert page.status_code == 200 and page.headers['cache-control'] == 'no-store'
    soup = BeautifulSoup(page.text, 'html.parser')
    ids = [section.get('id') for section in soup.select('main section[id]')]
    assert ids.index('preferences-card') < ids.index('gsc-oauth-card')
    for selector in ('#github-load-repos', '#bing-load-sites', '#netlify-load-sites'):
        assert soup.select_one(selector).has_attr('disabled')
    assert soup.select_one('a[href="#security-card"]')
    team = BeautifulSoup(journey['client'].get('/settings/team').text, 'html.parser')
    assert team.find('label', attrs={'for': 'team-invite-email'}) is None  # Free account cannot invite.
    assert 'Le compte que vous avez rejoint' in team.text


def invite_for(data):
    subscribe(data)
    session = auth.parse_session_token(data['stranger'].cookies.get(auth.SESSION_COOKIE_NAME), secret=m._safe_env('SEO_AGENT_SECRET_KEY'))
    with m.DB.session() as db:
        target = db.get(User, session['uid'])
        target_id, email = str(target.id), target.email
    invitation, token, error = m._creer_invitation(owner_user_id=data['owner'], email=email, invited_by=data['owner'])
    assert invitation is not None, error
    return str(invitation.id), token, target_id, email


def test_wrong_account_can_log_out_and_resume_the_same_invitation(journey):
    invite_id, token, target_id, email = invite_for(journey)
    path = '/team/accept?' + urlencode({'token': token})
    refused = journey['client'].get(path, follow_redirects=False)
    assert refused.status_code == 303
    page = journey['client'].get(refused.headers['location'])
    assert page.headers['referrer-policy'] == 'no-referrer' and page.headers['cache-control'] == 'no-store'
    assert email not in page.text and m._masked_invitation_email(email) in page.text
    soup = BeautifulSoup(page.text, 'html.parser')
    form = soup.select_one('input[name="next"]').parent
    assert form['action'] == '/auth/logout' and form.select_one('input[name="next"]')['value'] == path
    assert journey['client'].post('/auth/logout', data={'next': path}, follow_redirects=False).status_code == 403
    csrf = journey['client'].cookies.get(m._CSRF_COOKIE_NAME)
    signed_out = journey['client'].post('/auth/logout', data={'next': path, '_csrf': csrf}, follow_redirects=False)
    assert signed_out.status_code == 303
    assert parse_qs(urlsplit(signed_out.headers['location']).query)['next'] == [path]
    with m.DB.session() as db:
        assert db.get(AccountInvite, invite_id).used_at is None
        assert db.scalar(select(AccountMember).where(AccountMember.member_user_id == target_id)) is None
    accepted = journey['stranger'].get(path, follow_redirects=False)
    assert accepted.status_code == 303
    with m.DB.session() as db:
        assert db.get(AccountInvite, invite_id).used_at is not None
        assert db.scalar(select(AccountMember).where(AccountMember.member_user_id == target_id)).owner_user_id == journey['owner']


@pytest.mark.parametrize('state,expected', [('expired', 'expiré'), ('used', 'utilisée'), ('invalid', 'invalide')])
def test_unusable_invitation_offers_no_account_switch(journey, state, expected):
    invite_id, token, _, _ = invite_for(journey)
    with m.DB.session() as db:
        invite = db.get(AccountInvite, invite_id)
        if state == 'expired':
            invite.expires_at = m._utc_now_naive() - timedelta(seconds=1)
        if state == 'used':
            invite.used_at = m._utc_now_naive()
        db.commit()
    page = journey['client'].get('/team/accept', params={'token': token if state != 'invalid' else 'bad-token'})
    assert expected in page.text
    assert 'Reprendre votre invitation' not in page.text


def test_logout_return_cannot_leave_the_application(journey):
    journey['client'].get('/settings/team')
    csrf = journey['client'].cookies.get(m._CSRF_COOKIE_NAME)
    page = journey['client'].post('/auth/logout', data={'_csrf': csrf, 'next': 'https://evil.invalid'}, follow_redirects=False)
    assert parse_qs(urlsplit(page.headers['location']).query)['next'] == ['/']


@pytest.mark.parametrize('code', [401, 403, 404, 429, 500, 503])
def test_error_recovery_is_stable_honest_and_scoped(journey, code):
    path = '/projects/' + journey['slug'] + '/missing'
    request = Request({'type': 'http', 'method': 'GET', 'scheme': 'http', 'path': path,
                       'query_string': b'', 'headers': [], 'server': ('testserver', 80)})
    with m.DB.session() as db:
        request.state.user = db.get(User, journey['member'])
    response = m.templates.TemplateResponse('error.html', m._error_ctx(request, code, 'technical_private_detail'), status_code=code)
    soup = BeautifulSoup(response.body.decode(), 'html.parser')
    assert soup.select_one('.error-actions .primary')['href'] == '/projects/' + journey['slug']
    assert not soup.select('a[href^="javascript:"]')
    assert 'notifiée' not in soup.text
    if code == 429:
        assert soup.select_one('details:not([open])')
    else:
        assert 'technical_private_detail' not in soup.text
    request = Request({**request.scope, 'path': '/projects/foreign/content'})
    assert m._error_ctx(request, code, '')['recovery_url'] == '/'


def test_dossier_returns_to_canonical_filtered_inventory(lab):
    owner = user(lab['db'])
    viewer = client(lab['admin'])
    page = viewer.get('/settings/activation/accounts', params={'q': owner.email, 'plan': 'free'})
    soup = BeautifulSoup(page.text, 'html.parser')
    href = soup.select_one('[data-account-row] .account-email a')['href']
    returned = parse_qs(urlsplit(href).query)['return_to'][0]
    profile = viewer.get(href)
    assert profile.status_code == 200
    assert profile.headers['cache-control'] == 'no-store' and profile.headers['referrer-policy'] == 'no-referrer'
    assert BeautifulSoup(profile.text, 'html.parser').select_one('.crumbs a[href^="/settings/activation/accounts"]')['href'] == returned
    for bad in ('https://evil.invalid/', '//evil.invalid/', '/settings/system'):
        refused = viewer.get('/settings/activation/accounts/' + str(owner.id), params={'return_to': bad})
        assert refused.context['return_url'] == '/settings/activation/accounts'


def test_upgrade_navigation_keeps_filters_without_echoing_an_untrusted_job(journey):
    journey['report']()
    page = journey['client'].get('/projects/' + journey['slug'] + '?range=28d&q=v%C3%A9lo&job=untrusted-job-id')
    soup = BeautifulSoup(page.text, 'html.parser')
    link = soup.select_one('.sidebar a[href^="/billing?"]')['href']
    destination = parse_qs(urlsplit(link).query)['return_to'][0]
    filters = parse_qs(urlsplit(destination).query)
    assert filters == {'range': ['28d'], 'q': ['vélo']}
    assert 'untrusted-job-id' not in link
