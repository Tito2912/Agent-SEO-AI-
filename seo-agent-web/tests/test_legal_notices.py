"""Annoncer un changement des conditions : un e-mail par compte, dans sa langue, 30 jours avant (CGU, article 13)."""
from __future__ import annotations

from datetime import date, timedelta

import pytest
from bs4 import BeautifulSoup
from sqlalchemy import select

from backend import app as m, legal_notices
from backend.models import AuditLog, LegalNotice, LegalNoticeDelivery, User
from test_customer_activation import client, lab as lab, user

PAGE = '/settings/legal-notices'


@pytest.fixture()
def outbox(monkeypatch):
    sent: list[dict] = []

    def send(**mail):
        if mail['to_addr'] in failing:
            raise RuntimeError('smtp down')
        sent.append(mail)

    failing: set[str] = set()
    monkeypatch.setattr(m, '_send_email', send)
    monkeypatch.setattr(legal_notices, 'dispatch', lambda function, *args, **kwargs: function(*args, **kwargs))
    return {'sent': sent, 'failing': failing}


def speaker(database, language):
    created = user(database)
    with database.session() as db:
        db.get(User, created.id).language = language
        db.commit()
    return created


def post(c, path=PAGE, **data):
    c.get('/settings/accounts')
    return c.post(path, data={'_csrf': c.cookies.get(m._CSRF_COOKIE_NAME), **data}, follow_redirects=False)


def announcement(**changes):
    data = {'document': 'terms', 'effective_on': legal_notices.earliest().isoformat(),
            'summary_fr': 'Le prix du plan Pro passe de 49 € à 59 € par mois.',
            'summary_en': 'The Pro plan price goes from €49 to €59 per month.', 'confirm': 'yes'}
    data.update(changes)
    return data


def notices(database):
    with database.session() as db:
        return list(db.scalars(select(LegalNotice)))


def test_only_the_site_owner_sees_and_uses_the_screen(lab, outbox):
    page = client(lab['admin']).get(PAGE)
    assert page.status_code == 200 and page.headers['cache-control'] == 'no-store'
    soup = BeautifulSoup(page.text, 'html.parser')
    assert soup.select_one('input[name=effective_on]')['min'] == legal_notices.earliest().isoformat()
    assert soup.select_one(f'a.nav-item[href="{PAGE}"]') is not None
    for outsider in [user(lab['db']), user(lab['db'], admin=True)]:
        assert client(outsider).get(PAGE).status_code == 403
        assert post(client(outsider), **announcement()).status_code == 403
    assert notices(lab['db']) == [] and outbox['sent'] == []


@pytest.mark.parametrize('changes,message', [
    ({'effective_on': (date.today() + timedelta(days=29)).isoformat()}, 'au moins 30 jours'),
    ({'summary_en': '  '}, 'en anglais'),
    ({'document': 'contract'}, 'Choisissez ce qui change'),
    ({'new_version_url': 'javascript:alert(1)'}, 'doit commencer par'),
    ({'confirm': ''}, 'Cochez la confirmation'),
])
def test_an_incomplete_or_early_announcement_is_refused(lab, outbox, changes, message):
    response = post(client(lab['admin']), **announcement(**changes))
    assert response.status_code == 303 and 'err=' in response.headers['location']
    page = client(lab['admin']).get(response.headers['location'])
    assert message in BeautifulSoup(page.text, 'html.parser').select_one('.alert.error').get_text()
    assert notices(lab['db']) == [] and outbox['sent'] == []


def test_every_account_is_told_in_its_language_and_each_delivery_is_kept(lab, outbox):
    french, english, silent = speaker(lab['db'], 'fr'), speaker(lab['db'], 'en'), speaker(lab['db'], None)
    response = post(client(lab['admin']), **announcement())
    assert response.status_code == 303 and 'msg=' in response.headers['location']

    by_address = {mail['to_addr']: mail for mail in outbox['sent']}
    assert len(outbox['sent']) == 4  # le propriétaire reçoit aussi sa copie
    fr_mail = by_address[french.email]
    assert 'modification de nos conditions générales' in fr_mail['subject'] and 'Hello' not in fr_mail['body']
    assert '/terms' in fr_mail['body'] and '/en/terms' not in fr_mail['body']
    assert 'Le prix du plan Pro passe de 49 € à 59 € par mois.' in fr_mail['body']
    assert legal_notices.legal.format_date(legal_notices.earliest(), 'fr') in fr_mail['subject']
    en_mail = by_address[english.email]
    assert 'changes to our Terms and Conditions' in en_mail['subject'] and 'Bonjour' not in en_mail['body']
    assert '/en/terms' in en_mail['body'] and 'free of charge' in en_mail['body']
    both = by_address[silent.email]
    assert ' / ' in both['subject'] and 'Bonjour' in both['body'] and 'Hello' in both['body']
    assert '<script' not in both['html_body'] and 'lang="fr"' in both['html_body']

    with lab['db'].session() as db:
        rows = {row.email: row for row in db.scalars(select(LegalNoticeDelivery))}
        assert {row.status for row in rows.values()} == {'sent'} and all(row.sent_at for row in rows.values())
        assert (rows[french.email].lang, rows[english.email].lang, rows[silent.email].lang) == ('fr', 'en', 'fr+en')
        assert db.scalar(select(AuditLog).where(AuditLog.action == 'legal_notice.announce')) is not None
    history = BeautifulSoup(client(lab['admin']).get(PAGE).text, 'html.parser').select_one('#history').get_text(' ')
    assert '4 envoyés' in history and 'Conditions générales' in history


def test_a_failed_email_is_kept_and_sent_again_without_repeating_the_others(lab, outbox):
    unlucky = speaker(lab['db'], 'fr')
    outbox['failing'].add(unlucky.email)
    post(client(lab['admin']), **announcement(document='privacy'))
    assert [mail['to_addr'] for mail in outbox['sent']] == [lab['admin'].email]
    with lab['db'].session() as db:
        failed = db.scalar(select(LegalNoticeDelivery).where(LegalNoticeDelivery.email == unlucky.email))
        assert failed.status == 'failed' and 'smtp down' in failed.error
        notice_id = failed.notice_id
    page = BeautifulSoup(client(lab['admin']).get(PAGE).text, 'html.parser')
    assert '1 en échec' in page.select_one('#history').get_text(' ')
    assert page.select_one(f'form[action="{PAGE}/{notice_id}/resume"]') is not None

    outbox['failing'].clear()
    assert post(client(lab['admin']), f'{PAGE}/{notice_id}/resume').status_code == 303
    assert [mail['to_addr'] for mail in outbox['sent']] == [lab['admin'].email, unlucky.email]
    assert 'politique de confidentialité' in outbox['sent'][-1]['subject']


def test_a_deleted_account_takes_its_deliveries_with_it(lab, outbox):
    leaving = speaker(lab['db'], 'en')
    post(client(lab['admin']), **announcement(new_version_url='/terms'))
    assert 'The new version can be read now' in [m_ for m_ in outbox['sent'] if m_['to_addr'] == leaving.email][0]['body']
    with lab['db'].session() as db:
        db.delete(db.get(User, leaving.id))
        db.commit()
        assert db.scalar(select(LegalNoticeDelivery).where(LegalNoticeDelivery.email == leaving.email)) is None
        assert db.scalar(select(LegalNoticeDelivery).where(LegalNoticeDelivery.email == lab['admin'].email)) is not None


def test_the_privacy_policy_names_this_email_in_both_languages() -> None:
    from fastapi.testclient import TestClient
    browser = TestClient(m.app)
    assert "annonce d'un changement des conditions ou du service" in browser.get('/privacy').text
    assert 'notice of a change to the terms or the service' in browser.get('/en/privacy').text
