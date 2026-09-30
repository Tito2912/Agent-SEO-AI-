# -*- coding: utf-8 -*-
"""Le service s'ouvre aux consommateurs : ce que la loi leur doit est dans le produit, pas
seulement dans le texte.

Décision du propriétaire, 30/09/2026 : B2B et B2C. EComShop est assujettie à la TVA.
- Prix TTC, affichés comme tels (tarifs, abonnement, CGU).
- Rétractation de 14 jours : la DEMANDE EXPRESSE de commencer tout de suite est une case à cocher
  vérifiée par le serveur (art. L221-25 du Code de la consommation), tracée dans l'audit.
- Résiliation « en trois clics » (art. L215-1-1) : un bouton « Résilier mon abonnement » qui ouvre
  Stripe sur l'écran de résiliation, et une confirmation par e-mail avec la date de fin — envoyée
  UNE fois, même si Stripe rejoue l'événement.
- La TVA détaillée par Stripe Tax est un RÉGLAGE (`STRIPE_AUTOMATIC_TAX`) : Stripe refuse un
  paiement qui la demande tant que le tableau de bord n'est pas prêt. Un changement de plan
  programmé reporte le calcul de TVA de l'abonnement sur ses nouvelles phases.
- Le plafond de responsabilité ne vise que les professionnels ; le médiateur n'est nommé que
  déclaré (`LEGAL_MEDIATOR`).
"""
from __future__ import annotations

import html
import os
import re
import sys
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
from stripe import StripeObject

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-b2c-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as m  # noqa: E402
from backend import auth, billing  # noqa: E402
from backend.models import BillingSubscription, User  # noqa: E402

PRO = "price_pro"
FIN = datetime(2026, 10, 30, 9, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def stripe_simule(monkeypatch):
    monkeypatch.setattr(billing, "stripe_enabled", lambda: True)
    monkeypatch.setattr(billing, "stripe_init", lambda: None)
    monkeypatch.setattr(billing, "list_invoices", lambda db, *, user_id, **_k: [])
    monkeypatch.setattr(billing, "price_id_for_plan", lambda k: {"pro": PRO, "solo": "price_solo", "business": "price_business"}.get(k, ""))
    monkeypatch.setattr(billing, "plan_for_price_id", lambda p: {PRO: "pro", "price_solo": "solo", "price_business": "business"}.get(p, ""))


def _client(*, abonnement: str | None = None, resilie: bool = False) -> tuple[TestClient, str, str]:
    m.DB.create_tables()
    tag = uuid.uuid4().hex[:8]
    with m.DB.session() as db:
        user = User(email=f"conso-{tag}@exemple.fr", password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(user)
        db.commit()
        db.refresh(user)
        uid = str(user.id)
        if abonnement:
            db.add(BillingSubscription(
                user_id=uid, stripe_customer_id=f"cus_{tag}", stripe_subscription_id=f"sub_{tag}",
                stripe_price_id=PRO, plan_key="pro", status=abonnement, cancel_at_period_end=resilie,
                current_period_end=FIN, stripe_data={"id": f"sub_{tag}", "status": abonnement,
                                                     "cancel_at_period_end": resilie}))
            db.commit()
    client = TestClient(m.app)
    client.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    return client, uid, f"sub_{tag}"


def _post(client: TestClient, chemin: str, data: dict) -> object:
    client.get("/billing")
    jeton = client.cookies.get(m._CSRF_COOKIE_NAME)
    return client.post(chemin, data=data, headers={"x-csrf-token": jeton}, follow_redirects=False)


def _cgu() -> str:
    return TestClient(m.app).get("/terms").text


# ── Ce que disent les pages ─────────────────────────────────────────────────────────────────

def test_les_prix_sont_affiches_TTC() -> None:
    assert "TTC par mois" in TestClient(m.app).get("/pricing").text
    assert "toutes taxes comprises" in _cgu()
    assert "Prix TTC" in _client()[0].get("/billing").text


def test_la_TVA_n_est_dite_DETAILLEE_que_si_Stripe_la_calcule(monkeypatch) -> None:
    monkeypatch.delenv("STRIPE_AUTOMATIC_TAX", raising=False)
    assert "autoliquidation" not in _cgu()
    monkeypatch.setenv("STRIPE_AUTOMATIC_TAX", "1")
    assert "autoliquidation" in _cgu()


def test_le_mediateur_n_est_nomme_que_DECLARE(monkeypatch) -> None:
    monkeypatch.delenv("LEGAL_MEDIATOR", raising=False)
    assert "médiateur de la consommation" not in _cgu()
    monkeypatch.setenv("LEGAL_MEDIATOR", "Médiateur Exemple, 1 rue de Test, 75000 Paris, mediateur.example")
    assert "médiateur de la consommation : Médiateur Exemple, 1 rue de Test" in _cgu()


def test_la_retractation_de_14_jours_et_son_FORMULAIRE() -> None:
    page = _cgu()
    assert 'id="retractation"' in page and "<strong>14 jours</strong>" in page
    assert "au prorata des jours écoulés" in page
    assert "Je vous notifie par la présente ma rétractation" in page


def _puce(page: str, qui: str) -> str:
    return page.split("<strong>Si vous êtes un %s</strong>" % qui, 1)[1].split("</li>", 1)[0]


def test_le_plafond_de_responsabilite_ne_vise_QUE_les_professionnels() -> None:
    page = _cgu()
    assert "12 mois" in _puce(page, "professionnel")
    conso = _puce(page, "consommateur")
    assert "12 mois" not in conso and "sans limitation" in conso
    assert "garantie légale de conformité" in conso


# ── La demande expresse de commencer tout de suite ──────────────────────────────────────────

def test_la_case_est_proposee_a_la_SOUSCRIPTION_et_obligatoire() -> None:
    page = _client()[0].get("/billing").text
    assert page.count('name="execution_immediate" value="1" required') == 3, "une case par plan"
    assert 'href="/terms#retractation"' in page
    actif = _client(abonnement="active")[0].get("/billing").text
    assert "execution_immediate" not in actif, "un changement de plan n'est pas une souscription"


def test_sans_la_case_le_serveur_REFUSE_de_lancer_le_paiement(monkeypatch) -> None:
    appels = []
    monkeypatch.setattr(billing, "create_checkout_session_url",
                        lambda db, **kw: appels.append(kw) or "https://checkout.stripe.test/x")
    client, _uid, _sid = _client()
    rep = _post(client, "/billing/checkout", {"plan_key": "pro"})
    assert rep.status_code == 303 and "err=" in rep.headers["location"] and not appels
    rep = _post(client, "/billing/checkout", {"plan_key": "pro", "execution_immediate": "1"})
    assert rep.headers["location"] == "https://checkout.stripe.test/x" and len(appels) == 1


# ── La résiliation ──────────────────────────────────────────────────────────────────────────

def test_le_bouton_RESILIER_est_la_tant_que_l_abonnement_court() -> None:
    page = _client(abonnement="active")[0].get("/billing").text
    assert "Résilier mon abonnement" in page and 'name="intention" value="resilier"' in page
    deja = html.unescape(_client(abonnement="active", resilie=True)[0].get("/billing").text)
    assert "Résilier mon abonnement" not in deja
    assert "Résiliation enregistrée" in deja and "Fin de l'abonnement" in deja and "30/10/2026" in deja


def test_le_bouton_ouvre_le_portail_SUR_la_resiliation(monkeypatch) -> None:
    vu = []
    monkeypatch.setattr(billing, "create_billing_portal_url",
                        lambda db, **kw: vu.append(kw["resilier"]) or "https://portal.stripe.test/x")
    client, _uid, _sid = _client(abonnement="active")
    _post(client, "/billing/portal", {"intention": "resilier"})
    _post(client, "/billing/portal", {})
    assert vu == [True, False]


def test_le_portail_recoit_le_PARCOURS_de_resiliation_et_s_en_passe_s_il_est_refuse(monkeypatch) -> None:
    _client_, uid, sid = _client(abonnement="active")
    monkeypatch.setattr(billing, "stripe_customer_id", lambda db, *, user_id: "cus_x")
    appels = []

    def _creer(**kw):
        appels.append(kw)
        if "flow_data" in kw and len(appels) == 1 and refuser:
            raise RuntimeError("cancellation disabled in portal configuration")
        return StripeObject.construct_from({"url": "https://portal.stripe.test/%d" % len(appels)}, "sk_test")

    monkeypatch.setattr(billing.stripe.billing_portal.Session, "create", _creer)
    refuser = False
    with m.DB.session() as db:
        assert billing.create_billing_portal_url(db, user_id=uid, email="x@exemple.fr", resilier=True)
    flux = appels[0]["flow_data"]
    assert flux["type"] == "subscription_cancel" and flux["subscription_cancel"]["subscription"] == sid
    appels.clear()
    refuser = True
    with m.DB.session() as db:
        url = billing.create_billing_portal_url(db, user_id=uid, email="x@exemple.fr", resilier=True)
    assert url.endswith("/2") and "flow_data" not in appels[1], "le client doit pouvoir résilier quand même"


def _evenement(sid: str, uid: str, **champs) -> dict:
    obj = {"id": sid, "customer": "cus_x", "status": "active", "cancel_at_period_end": False,
           "current_period_end": int(FIN.timestamp()), "metadata": {"user_id": uid},
           "items": {"data": [{"price": {"id": PRO}}]}}
    obj.update(champs)
    return {"id": "evt_%s" % uuid.uuid4().hex[:6], "type": "customer.subscription.updated", "data": {"object": obj}}


def test_une_resiliation_est_signalee_UNE_fois() -> None:
    _c, uid, sid = _client(abonnement="active")
    with m.DB.session() as db:
        effet = billing.handle_stripe_event(db, event=_evenement(sid, uid, cancel_at_period_end=True))
        assert effet["resiliation"]["user_id"] == uid and effet["resiliation"]["fin"] == FIN
        assert effet["resiliation"]["immediate"] is False
        assert billing.handle_stripe_event(db, event=_evenement(sid, uid, cancel_at_period_end=True)) is None, "rejoué"


def test_un_abonnement_arrete_pour_IMPAYE_n_est_pas_une_resiliation() -> None:
    _c, uid, sid = _client(abonnement="active")
    with m.DB.session() as db:
        assert billing.handle_stripe_event(db, event=_evenement(
            sid, uid, status="canceled", cancellation_details={"reason": "payment_failed"})) is None


def test_une_fin_IMMEDIATE_est_dite_immediate() -> None:
    _c, uid, sid = _client(abonnement="active")
    with m.DB.session() as db:
        effet = billing.handle_stripe_event(db, event=_evenement(
            sid, uid, status="canceled", ended_at=int(FIN.timestamp()),
            cancellation_details={"reason": "cancellation_requested"}))
    assert effet["resiliation"]["immediate"] is True


def test_le_webhook_envoie_la_CONFIRMATION_avec_la_date_de_fin_une_seule_fois(monkeypatch) -> None:
    _c, uid, sid = _client(abonnement="active")
    envois = []
    monkeypatch.setattr(m, "_send_email", lambda **kw: envois.append(kw))
    evenement = _evenement(sid, uid, cancel_at_period_end=True)
    monkeypatch.setattr(billing, "construct_webhook_event", lambda **_kw: evenement)
    client = TestClient(m.app)
    for _ in range(2):
        assert client.post("/stripe/webhook", content=b"{}", headers={"stripe-signature": "t=1,v1=x"}).status_code == 200
    assert len(envois) == 1, "Stripe rejoue : une seule confirmation"
    assert envois[0]["to_addr"].startswith("conso-")
    assert "30/10/2026" in envois[0]["body"] and "résiliation" in envois[0]["subject"]


def test_un_e_mail_en_echec_ne_fait_pas_REJOUER_le_webhook(monkeypatch) -> None:
    _c, uid, sid = _client(abonnement="active")

    def _panne(**_kw):
        raise RuntimeError("smtp down")

    monkeypatch.setattr(m, "_send_email", _panne)
    monkeypatch.setattr(billing, "construct_webhook_event", lambda **_kw: _evenement(sid, uid, cancel_at_period_end=True))
    rep = TestClient(m.app).post("/stripe/webhook", content=b"{}", headers={"stripe-signature": "t=1,v1=x"})
    assert rep.status_code == 200


# ── La TVA calculée par Stripe ──────────────────────────────────────────────────────────────

def _session_de_paiement(monkeypatch) -> dict:
    vu = {}
    monkeypatch.setattr(billing, "get_or_create_stripe_customer", lambda db, **kw: "cus_x")
    monkeypatch.setattr(billing, "upsert_customer_mapping", lambda db, **kw: None)
    monkeypatch.setattr(billing, "public_base_url", lambda: "http://testserver")
    monkeypatch.setattr(billing.stripe.checkout.Session, "create",
                        lambda **kw: vu.update(kw) or StripeObject.construct_from({"url": "https://checkout.stripe.test/s"}, "sk_test"))
    with m.DB.session() as db:
        billing.create_checkout_session_url(db, user_id="u1", email="x@exemple.fr", plan_key="pro")
    return vu


def test_la_TVA_automatique_est_un_REGLAGE(monkeypatch) -> None:
    monkeypatch.delenv("STRIPE_AUTOMATIC_TAX", raising=False)
    assert "automatic_tax" not in _session_de_paiement(monkeypatch)
    monkeypatch.setenv("STRIPE_AUTOMATIC_TAX", "1")
    vu = _session_de_paiement(monkeypatch)
    assert vu["automatic_tax"] == {"enabled": True} and vu["tax_id_collection"] == {"enabled": True}
    assert vu["customer_update"] == {"address": "auto", "name": "auto"}


@pytest.mark.parametrize("taxe", [True, False])
def test_un_changement_programme_GARDE_la_TVA_de_l_abonnement(monkeypatch, taxe) -> None:
    _c, uid, sid = _client(abonnement="active")
    debut, fin = 1756317960, int(FIN.timestamp())
    monkeypatch.setattr(billing.stripe.Subscription, "retrieve", lambda _id, **_k: StripeObject.construct_from({
        "id": sid, "status": "active", "cancel_at_period_end": False, "automatic_tax": {"enabled": taxe},
        "current_period_start": debut, "current_period_end": fin,
        "items": {"data": [{"id": "si_1", "price": {"id": PRO}}]}}, "sk_test"))
    monkeypatch.setattr(billing.stripe.SubscriptionSchedule, "create", lambda **_k: StripeObject.construct_from({
        "id": "sub_sched_1", "current_phase": {"start_date": debut, "end_date": fin},
        "phases": [{"start_date": debut, "end_date": fin}]}, "sk_test"))
    phases = {}
    monkeypatch.setattr(billing.stripe.SubscriptionSchedule, "modify", lambda _id, **kw: phases.update(kw))
    monkeypatch.setattr(billing, "sync_subscription_from_stripe", lambda db, **kw: None)
    with m.DB.session() as db:
        billing.schedule_plan_change_at_period_end(db, user_id=uid, target_plan_key="solo")
    for phase in phases["phases"]:
        assert (phase.get("automatic_tax") == {"enabled": True}) is taxe, phase


def test_temoin_la_page_rend_la_date_STOCKEE() -> None:
    """Témoin de lecture : la page rend bien la date stockée (sans quoi l'assertion « 30/10/2026 »
    du test du bouton pourrait passer par hasard ailleurs)."""
    page = _client(abonnement="active")[0].get("/billing").text
    assert re.search(r"Prochain renouvellement : <span class=\"mono\">30/10/2026</span>", page)
