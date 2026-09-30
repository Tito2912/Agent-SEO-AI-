# -*- coding: utf-8 -*-
"""Un abonnement « actif » dont la période est finie depuis des jours, sans nouvelle de Stripe.

Vu le 30/09/2026 après le passage des clés Stripe en réel : le compte administrateur affichait
« Pro · actif, prochain renouvellement le 11/06/2026 ». C'était un abonnement du mode TEST, que les
clés réelles ne voient plus et qu'aucun webhook ne mettra jamais à jour : actif pour toujours, et
son plan comptait partout. Noyaru relit désormais un tel abonnement chez Stripe : connu, il est
remis à jour ; inconnu, il est clos. Une panne, elle, ne clôt rien.
"""
from __future__ import annotations

import os
import sys
import tempfile
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from stripe import StripeObject

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-fantome-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as m  # noqa: E402
from backend import auth, billing  # noqa: E402
from backend.models import BillingSubscription, User  # noqa: E402

JUIN = datetime(2026, 6, 11, 9, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def stripe_simule(monkeypatch):
    monkeypatch.setattr(billing, "stripe_enabled", lambda: True)
    monkeypatch.setattr(billing, "stripe_init", lambda: None)
    monkeypatch.setattr(billing, "list_invoices", lambda db, *, user_id, **_k: [])
    monkeypatch.setattr(billing, "price_id_for_plan", lambda k: {"pro": "price_pro"}.get(k, ""))
    monkeypatch.setattr(billing, "plan_for_price_id", lambda p: {"price_pro": "pro"}.get(p, ""))


def _abonne(fin: datetime) -> tuple[str, str]:
    m.DB.create_tables()
    tag = uuid.uuid4().hex[:8]
    sid = "sub_test_" + tag
    with m.DB.session() as db:
        u = User(email="fantome-%s@exemple.fr" % tag, password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(u)
        db.commit()
        db.refresh(u)
        db.add(BillingSubscription(
            user_id=str(u.id), stripe_customer_id="cus_" + tag, stripe_subscription_id=sid,
            stripe_price_id="price_pro", plan_key="pro", status="active",
            stripe_data={"id": sid, "status": "active",
                         "items": {"data": [{"price": {"id": "price_pro"}, "current_period_end": int(fin.timestamp())}]}}))
        db.commit()
        return str(u.id), sid


def _introuvable(_id, **_k):
    raise billing.stripe.InvalidRequestError("No such subscription: %s" % _id, "id", code="resource_missing")


def _ligne(sid: str) -> BillingSubscription:
    with m.DB.session() as db:
        return db.scalar(billing.select(BillingSubscription).where(BillingSubscription.stripe_subscription_id == sid))


def test_un_abonnement_INCONNU_de_Stripe_et_perime_est_CLOS(monkeypatch) -> None:
    uid, sid = _abonne(JUIN)
    monkeypatch.setattr(billing.stripe.Subscription, "retrieve", _introuvable)
    with m.DB.session() as db:
        assert billing.cloturer_si_fantome(db, row=billing.subscription_for_user(db, user_id=uid)) == "clos"
        assert billing.effective_plan_key(db, user_id=uid) == "free"
    assert _ligne(sid).status == "canceled"


def test_un_abonnement_CONNU_de_Stripe_est_remis_a_jour_pas_clos(monkeypatch) -> None:
    uid, sid = _abonne(JUIN)
    futur = int((datetime.now(UTC) + timedelta(days=20)).timestamp())
    monkeypatch.setattr(billing.stripe.Subscription, "retrieve", lambda _id, **_k: StripeObject.construct_from({
        "id": sid, "customer": "cus_x", "status": "active", "metadata": {"user_id": uid},
        "items": {"data": [{"price": {"id": "price_pro"}, "current_period_end": futur}]}}, "sk_live"))
    with m.DB.session() as db:
        assert billing.cloturer_si_fantome(db, row=billing.subscription_for_user(db, user_id=uid)) == "resynchronise"
    assert _ligne(sid).status == "active"


def test_une_PANNE_ne_clot_rien(monkeypatch) -> None:
    uid, sid = _abonne(JUIN)

    def _panne(_id, **_k):
        raise billing.stripe.APIConnectionError("réseau coupé")

    monkeypatch.setattr(billing.stripe.Subscription, "retrieve", _panne)
    with m.DB.session() as db, pytest.raises(billing.stripe.APIConnectionError):
        billing.cloturer_si_fantome(db, row=billing.subscription_for_user(db, user_id=uid))
    assert _ligne(sid).status == "active"


@pytest.mark.parametrize("jours", [-10, 1])
def test_une_periode_EN_COURS_ou_a_peine_finie_ne_coute_aucun_appel(monkeypatch, jours) -> None:
    """Moins de deux jours de retard : le webhook de renouvellement peut encore arriver."""
    uid, sid = _abonne(datetime.now(UTC) - timedelta(days=jours))
    monkeypatch.setattr(billing.stripe.Subscription, "retrieve", lambda *_a, **_k: pytest.fail("appel inutile"))
    with m.DB.session() as db:
        assert billing.cloturer_si_fantome(db, row=billing.subscription_for_user(db, user_id=uid)) == "a_jour"


def test_la_page_Abonnement_clot_le_fantome_AVANT_de_l_afficher(monkeypatch) -> None:
    uid, sid = _abonne(JUIN)
    monkeypatch.setattr(billing.stripe.Subscription, "retrieve", _introuvable)
    monkeypatch.setattr(billing, "stripe_customer_id", lambda db, *, user_id: "")
    client = TestClient(m.app)
    client.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    page = client.get("/billing").text
    assert "11/06/2026" not in page and "Résilier mon abonnement" not in page
    assert _ligne(sid).status == "canceled"


def test_la_page_Abonnement_survit_a_une_PANNE(monkeypatch) -> None:
    uid, sid = _abonne(JUIN)

    def _panne(_id, **_k):
        raise billing.stripe.APIConnectionError("réseau coupé")

    monkeypatch.setattr(billing.stripe.Subscription, "retrieve", _panne)
    client = TestClient(m.app)
    client.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    assert client.get("/billing").status_code == 200
    assert _ligne(sid).status == "active"


def test_la_tache_quotidienne_balaye_TOUS_les_fantomes(monkeypatch) -> None:
    _u1, s1 = _abonne(JUIN)
    _u2, s2 = _abonne(JUIN)
    monkeypatch.setattr(billing.stripe.Subscription, "retrieve", _introuvable)
    monkeypatch.setenv("CRON_SECRET", "secret-cron")
    monkeypatch.setattr(m, "_balayer_verifications_pr", lambda **_k: None)
    monkeypatch.setattr(m, "_balayer_contenu_auto", lambda **_k: None)
    rep = TestClient(m.app).get("/cron/autopilot", headers={"Authorization": "Bearer secret-cron"})
    assert rep.status_code == 200 and rep.json()["balayages"]["abonnements"] == "ok"
    assert _ligne(s1).status == _ligne(s2).status == "canceled"
