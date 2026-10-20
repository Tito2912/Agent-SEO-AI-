"""Recovery keeps customer context while tokens, sessions and server guards remain authoritative."""
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlsplit

import pytest
from bs4 import BeautifulSoup

from backend import app as m, auth
from backend.models import EmailVerificationToken, PasswordResetToken
from test_customer_activation import client, lab as lab, project, user

NEXT = '/settings/accounts?next=%2Fprojects%2Fatelier#preferences'


def private(response):
    assert response.headers['cache-control'] == 'no-store'
    assert response.headers['referrer-policy'] == 'no-referrer'


def local_path(url):
    parts = urlsplit(url)
    return parts.path + '?' + parts.query


def post(c, path, **payload):
    return c.post(path, data={'_csrf': c.cookies.get(m._CSRF_COOKIE_NAME), **payload}, follow_redirects=False)


@pytest.fixture()
def recovery(lab, monkeypatch):
    captured = {'reset': [], 'verify': []}
    monkeypatch.setattr(m, '_smtp_config', lambda: {'host': 'smtp.example.invalid'})
    monkeypatch.setattr(m, '_email_verification_enabled', lambda: True)
    monkeypatch.setattr(m, '_rate_limit_retry_after', lambda **kwargs: None)
    monkeypatch.setattr(m, '_send_password_reset_email', lambda **kw: captured['reset'].append(kw['reset_url']))
    monkeypatch.setattr(m, '_send_email_verification_email', lambda **kw: captured['verify'].append(kw['verify_url']))
    return captured


@pytest.mark.parametrize('lang', ['fr', 'en'])
@pytest.mark.parametrize('path', ['/auth/forgot', '/auth/verify/resend'])
def test_recovery_prefills_and_returns_to_login_without_losing_email_or_destination(lab, recovery, lang, path):
    c = client()
    response = c.get(path, params={'email': 'Client+atelier@example.invalid', 'next': NEXT, 'lang': lang})
    private(response)
    soup = BeautifulSoup(response.text, 'html.parser')
    assert soup.html['lang'] == lang
    assert soup.select_one('[name=email]')['value'] == 'client+atelier@example.invalid'
    assert soup.select_one('[name=next]')['value'] == NEXT
    assert soup.select_one('[name=_csrf]')['value']
    assert soup.select_one('.auth-entry-help')['href'] == ('/en/support' if lang == 'en' else '/support')
    back = soup.select_one('.auth-entry-footer a')['href']
    assert parse_qs(urlsplit(back).query) == {'next': [NEXT], 'email': ['client+atelier@example.invalid']}
    login = BeautifulSoup(c.get(back).text, 'html.parser')
    forgot = login.select_one('a[href^="/auth/forgot"]')['href']
    assert parse_qs(urlsplit(forgot).query) == parse_qs(urlsplit(back).query)
    unsafe = c.get(path, params={'email': '"><script>alert(1)</script>@example.invalid', 'next': '//untrusted.example/'})
    soup = BeautifulSoup(unsafe.text, 'html.parser')
    assert soup.select_one('[name=next]')['value'] == '/' and not soup.select('main script')


@pytest.mark.parametrize('path,kind', [('/auth/forgot', 'reset'), ('/auth/verify/resend', 'verify')])
def test_request_confirmation_stays_neutral_and_explicit_resend_is_optional(lab, recovery, path, kind):
    known = user(lab['db'])
    messages = []
    for address in (known.email, 'absent@example.invalid'):
        c = client()
        c.get(path)
        response = post(c, path, email=address, next=NEXT)
        assert response.status_code == 303
        private(response)
        assert auth.SESSION_COOKIE_NAME not in response.cookies
        query = parse_qs(urlsplit(response.headers['location']).query)
        assert query['email'] == [address] and query['next'] == [NEXT]
        messages.append(query['msg'])
        page = c.get(response.headers['location'])
        soup = BeautifulSoup(page.text, 'html.parser')
        assert soup.select_one('[role=status]')
        assert 'open' not in soup.select_one('details.auth-recovery-resend').attrs
        assert soup.select_one('[name=email]')['value'] == address
        assert soup.select_one('.auth-recovery-email').get_text().endswith(address)
    assert messages[0] == messages[1]
    assert len(recovery[kind]) == 1


@pytest.mark.parametrize('verified,verification_enabled', [(True, True), (False, False)])
def test_password_request_reset_error_and_success_reach_the_original_safe_destination(lab, recovery, monkeypatch, verified, verification_enabled):
    owner = user(lab['db'])
    monkeypatch.setattr(m, '_email_verification_enabled', lambda: verification_enabled)
    if verified:
        with lab['db'].session() as db:
            m._mark_user_email_verified(db, user_id=owner.id)
    c = client()
    c.get('/auth/forgot')
    request = post(c, '/auth/forgot', email=owner.email, next=NEXT)
    assert request.status_code == 303 and len(recovery['reset']) == 1
    url = local_path(recovery['reset'][0])
    page = c.get(url)
    assert page.status_code == 200
    private(page)
    soup = BeautifulSoup(page.text, 'html.parser')
    token = soup.select_one('[name=token]')['value']
    assert soup.select_one('#reset-password')['aria-describedby'] == 'reset-password-hint'
    assert soup.select_one('#reset-confirmation')['aria-describedby'] == 'reset-password-hint'
    before = owner.password_hash
    refused = c.post('/auth/reset', data={'token': token, 'password': 'new-local-password', 'password2': 'new-local-password'})
    assert refused.status_code == 403
    mismatch = post(c, '/auth/reset', token=token, password='new-local-password', password2='different-local-password', next=NEXT)
    assert mismatch.status_code == 400
    private(mismatch)
    soup = BeautifulSoup(mismatch.text, 'html.parser')
    assert soup.select_one('[role=alert]')
    assert soup.select_one('[name=token]')['value'] == token
    assert all('value' not in field.attrs for field in soup.select('[type=password]'))
    with lab['db'].session() as db:
        assert db.get(type(owner), owner.id).password_hash == before
    success = post(c, '/auth/reset', token=token, password='new-local-password', password2='new-local-password', next=NEXT)
    assert success.status_code == 303 and success.headers['location'] == NEXT
    private(success)
    assert auth.SESSION_COOKIE_NAME in success.cookies
    with lab['db'].session() as db:
        assert auth.verify_password('new-local-password', db.get(type(owner), owner.id).password_hash)
        assert m._valid_password_reset_row(db, token=token) is None
    replay = c.get(url)
    assert replay.status_code == 400 and not BeautifulSoup(replay.text, 'html.parser').select('form')


def test_verification_keeps_real_session_redirect_and_consumes_the_link(lab, recovery):
    owner = user(lab['db'])
    project(lab['db'], owner, slug='verification-local')
    destination = '/projects/verification-local'
    c = client()
    c.get('/auth/verify/resend')
    request = post(c, '/auth/verify/resend', email=owner.email, next=destination)
    assert request.status_code == 303 and len(recovery['verify']) == 1
    url = local_path(recovery['verify'][0])
    verified = c.get(url, follow_redirects=False)
    assert verified.status_code == 303 and verified.headers['location'] == destination
    private(verified)
    assert auth.SESSION_COOKIE_NAME in verified.cookies
    replay = c.get(url)
    assert replay.status_code == 400
    private(replay)
    soup = BeautifulSoup(replay.text, 'html.parser')
    assert soup.select_one('[role=alert]')
    assert urlsplit(soup.select_one('.btn.primary')['href']).path == '/auth/verify/resend'


@pytest.mark.parametrize('path,model,issue', [
    ('/auth/reset', PasswordResetToken, '_issue_password_reset_token'),
    ('/auth/verify', EmailVerificationToken, '_issue_email_verification_token'),
])
@pytest.mark.parametrize('state', ['absent', 'forged', 'expired', 'used'])
def test_unusable_links_keep_refusal_and_the_correct_next_step(lab, recovery, path, model, issue, state):
    owner = user(lab['db'])
    token = '' if state == 'absent' else 'forged-token'
    if state in ('expired', 'used'):
        with lab['db'].session() as db:
            token, _ = getattr(m, issue)(db, user_id=owner.id)
            row = db.query(model).filter(model.user_id == owner.id).one()
            if state == 'expired':
                row.expires_at = datetime.now(timezone.utc) - timedelta(hours=1)
            else:
                row.used_at = datetime.now(timezone.utc)
            db.commit()
    response = client().get(path, params={'token': token, 'next': NEXT, 'lang': 'en'})
    assert response.status_code == 400
    private(response)
    soup = BeautifulSoup(response.text, 'html.parser')
    assert not soup.select('form') and soup.select_one('[role=alert]')
    primary = soup.select_one('.btn.primary')['href']
    assert urlsplit(primary).path == ('/auth/forgot' if path == '/auth/reset' else '/auth/verify/resend')
    assert parse_qs(urlsplit(primary).query)['next'] == [NEXT]
    assert 'token' not in parse_qs(urlsplit(primary).query)


@pytest.mark.parametrize('path', ['/auth/forgot', '/auth/verify/resend'])
@pytest.mark.parametrize('failure', ['disabled', 'sender', 'rate'])
def test_unavailable_email_or_rate_limit_keeps_recovery_and_never_exposes_sender_details(lab, recovery, monkeypatch, path, failure):
    owner = user(lab['db'])
    if failure == 'disabled':
        monkeypatch.setattr(m, '_smtp_config', lambda: {})
        monkeypatch.setattr(m, '_email_verification_enabled', lambda: False)
    elif failure == 'sender':
        def unavailable(**kwargs):
            raise OSError('private-smtp-exception')
        monkeypatch.setattr(m, '_send_password_reset_email', unavailable)
        monkeypatch.setattr(m, '_send_email_verification_email', unavailable)
    else:
        monkeypatch.setattr(m, '_rate_limit_retry_after', lambda **kwargs: 60)
    c = client()
    c.get(path)
    response = post(c, path, email=owner.email, next=NEXT)
    assert response.status_code == (429 if failure == 'rate' else 503)
    private(response)
    soup = BeautifulSoup(response.text, 'html.parser')
    assert soup.select_one('[role=alert]') and soup.select_one('a[href$="/support"]')
    if failure != 'rate':
        assert soup.select_one('.btn.primary')['href'] == '/support'
    assert soup.select_one('[name=email]')['value'] == owner.email
    assert soup.select_one('[name=next]')['value'] == NEXT
    assert not soup.select_one('details.auth-recovery-resend')
    assert 'private-smtp-exception' not in response.text and 'SendGrid/SMTP' not in response.text


def test_reset_for_unverified_account_requires_confirmation_and_clears_another_session(lab, recovery):
    target, other = user(lab['db']), user(lab['db'])
    project(lab['db'], target, slug='awaiting-confirmation')
    with lab['db'].session() as db:
        m._mark_user_email_verified(db, user_id=other.id)
        token, _ = m._issue_password_reset_token(db, user_id=target.id)
    c = client(other)
    # Use a host cookie like a real browser; an httpx domainless supercookie cannot be expired by a response.
    previous_session = c.cookies.get(auth.SESSION_COOKIE_NAME)
    c.cookies.clear()
    c.cookies.set(auth.SESSION_COOKIE_NAME, previous_session, domain='testserver.local', path='/')
    c.get('/auth/reset', params={'token': token})
    response = post(c, '/auth/reset', token=token, password='updated-local-password',
                    password2='updated-local-password', next='/projects/awaiting-confirmation')
    assert response.status_code == 303
    private(response)
    assert response.cookies.get(auth.SESSION_COOKIE_NAME, '') == ''
    location = urlsplit(response.headers['location'])
    assert location.path == '/auth/verify/resend'
    query = parse_qs(location.query)
    assert query['email'] == [target.email] and query['next'] == ['/projects/awaiting-confirmation']
    assert query['msg'] == ['Mot de passe mis à jour.']
    assert not recovery['verify']  # The redirect must not pretend an email has been sent.
    page = c.get(response.headers['location'])
    soup = BeautifulSoup(page.text, 'html.parser')
    assert soup.select_one('[role=status]') and soup.select_one('form')
    assert not soup.select_one('details.auth-recovery-resend')
    assert soup.select_one('[name=email]')['value'] == target.email
    assert c.get('/settings/accounts', follow_redirects=False).status_code == 303
    with lab['db'].session() as db:
        assert auth.verify_password('updated-local-password', db.get(type(target), target.id).password_hash)
        assert not m._user_email_verified(db, user_id=target.id)
    sent = post(c, '/auth/verify/resend', email=target.email, next='/projects/awaiting-confirmation')
    assert sent.status_code == 303 and len(recovery['verify']) == 1
    confirmed = c.get(local_path(recovery['verify'][0]), follow_redirects=False)
    assert confirmed.status_code == 303 and confirmed.headers['location'] == '/projects/awaiting-confirmation'
    assert confirmed.cookies.get(auth.SESSION_COOKIE_NAME)
