"""Hosted payment screens follow the language actually chosen in Noyaru."""
import os
import uuid
from types import SimpleNamespace

from fastapi.testclient import TestClient
import pytest

from backend import app as m, auth, billing
from backend.models import BillingSubscription, User


@pytest.fixture()
def customer(monkeypatch):
    m.DB.create_tables()
    monkeypatch.setenv('STRIPE_SECRET_KEY', 'sk_test_fixture')
    monkeypatch.setenv('STRIPE_PRICE_ID_SOLO', 'price_fixture_solo')
    monkeypatch.setenv('PUBLIC_BASE_URL', 'http://testserver')
    monkeypatch.setenv('EMAIL_VERIFICATION_DISABLED', 'true')
    monkeypatch.setattr(billing, 'client_stripe_valide', lambda db, **kw: 'cus_fixture')
    monkeypatch.setattr(billing, 'upsert_customer_mapping', lambda db, **kw: None)
    monkeypatch.setattr(billing, '_client_en_francais', lambda *args: None)
    with m.DB.session() as db:
        user = User(email=f'stripe-lang-{uuid.uuid4().hex}@example.invalid', password_hash='fixture',
                    is_admin=False, country='FR', language='fr')
        db.add(user)
        db.commit()
        uid = str(user.id)
    client = TestClient(m.app)
    client.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(user_id=uid, secret=os.environ['SEO_AGENT_SECRET_KEY']))

    def post(path, lang, **values):
        page = client.get('/billing', params={'lang': lang})
        assert page.status_code == 200 and page.context['request'].state.user.language == 'fr'
        response = client.post(path, data={'_csrf': client.cookies.get(m._CSRF_COOKIE_NAME), **values}, follow_redirects=False)
        assert response.status_code == 303
        return response

    return client, uid, post


@pytest.mark.parametrize('lang', ['fr', 'en'])
def test_checkout_uses_the_request_language_and_keeps_the_same_payment_parameters(customer, monkeypatch, lang):
    _client, uid, post = customer
    sent = []
    monkeypatch.setattr(billing.stripe.checkout.Session, 'create', lambda **kw: sent.append(kw) or SimpleNamespace(url='https://checkout.stripe.test/session'))
    response = post('/billing/checkout', lang, plan_key='solo', execution_immediate='1')
    assert response.headers['location'] == 'https://checkout.stripe.test/session'
    assert len(sent) == 1 and sent[0]['locale'] == lang
    assert sent[0]['line_items'] == [{'price': 'price_fixture_solo', 'quantity': 1}]
    assert sent[0]['mode'] == 'subscription' and sent[0]['client_reference_id'] == uid
    assert sent[0]['metadata'] == {'user_id': uid, 'plan_key': 'solo'}
    assert sent[0]['success_url'] == 'http://testserver/billing?success=1&session_id={CHECKOUT_SESSION_ID}'


@pytest.mark.parametrize('lang', ['fr', 'en'])
def test_portal_uses_the_request_language(customer, monkeypatch, lang):
    _client, _uid, post = customer
    sent = []
    monkeypatch.setattr(billing.stripe.billing_portal.Session, 'create', lambda **kw: sent.append(kw) or SimpleNamespace(url='https://billing.stripe.test/session'))
    response = post('/billing/portal', lang)
    assert response.headers['location'] == 'https://billing.stripe.test/session'
    assert sent == [{'customer': 'cus_fixture', 'return_url': 'http://testserver/billing', 'locale': lang}]


@pytest.mark.parametrize('lang', ['fr', 'en'])
@pytest.mark.parametrize('refuse_direct_flow', [False, True])
def test_cancellation_and_its_fallback_keep_the_chosen_language(customer, monkeypatch, lang, refuse_direct_flow):
    client, uid, post = customer
    with m.DB.session() as db:
        db.add(BillingSubscription(user_id=uid, stripe_customer_id='cus_fixture', stripe_subscription_id='sub_'+uid,
                                  stripe_price_id='price_fixture_solo', plan_key='solo', status='active'))
        db.commit()
    monkeypatch.setattr(billing, 'list_invoices', lambda *args, **kw: [])
    monkeypatch.setattr(billing, 'pending_plan_change', lambda *args, **kw: None)
    monkeypatch.setattr(billing, 'cloturer_si_fantome', lambda *args, **kw: None)
    sent = []

    def portal(**values):
        sent.append(values)
        if refuse_direct_flow and values.get('flow_data'):
            raise RuntimeError('Direct cancellation unavailable in this test configuration')
        return SimpleNamespace(url='https://billing.stripe.test/cancel')

    monkeypatch.setattr(billing.stripe.billing_portal.Session, 'create', portal)
    response = post('/billing/portal', lang, intention='resilier')
    assert response.headers['location'] == 'https://billing.stripe.test/cancel'
    assert all(values['locale'] == lang and values['customer'] == 'cus_fixture' for values in sent)
    assert sent[0]['flow_data']['subscription_cancel']['subscription'] == 'sub_'+uid
    assert len(sent) == (2 if refuse_direct_flow else 1)
    if refuse_direct_flow:
        assert 'flow_data' not in sent[-1]
