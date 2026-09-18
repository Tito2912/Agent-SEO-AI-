# -*- coding: utf-8 -*-
"""Le webhook Stripe change-t-il vraiment le QUOTA, et que fait-il d'un prix inconnu ?

Le 27/09/2026, un `subscription_schedule` doit faire basculer le compte de test de Business vers
Pro. Le proprietaire a accepte quatre semaines sans facturer pour OBSERVER ce moment, parce qu'un
quota Business reste ouvert sur un abonnement Pro serait invisible. Ces tests jouent l'evenement
d'avance : si le chemin est casse, on l'apprend maintenant et non le jour dit.

CE QUI N'ETAIT PROUVE PAR RIEN. Les deux moities existaient separement — des tests sur
`plan_for_price_id`, d'autres sur l'affichage de /billing — mais aucun n'allait de l'evenement
jusqu'au quota. `handle_stripe_event` -> `upsert_subscription` -> `effective_plan_key` ->
`plan_limits` est la chaine que le client subit, et c'est la seule qui compte.

`customer.subscription.deleted` est note dans l'historique du projet comme « the one link never
exercised ». Il l'est ici.

LE PRIX INCONNU EST LE CAS QUI MERITAIT CE FICHIER. `plan_for_price_id` compare l'identifiant a
trois variables d'environnement et rend "" pour tout le reste ; `upsert_subscription` retombe
alors sur `free`. Un prix inconnu n'est pourtant JAMAIS une intention du client : c'est un tarif
migre, un identifiant renomme, ou une variable absente au passage en live — la meme classe que
les `MAIL_*` et `ANTHROPIC_API_KEY` non declares, ou l'absence d'une variable ETEINT une
fonctionnalite au lieu d'echouer. Degrader un client payant sur cette base est une DEVINETTE aux
consequences reelles, et le webhook repond 200 sans que rien n'apparaisse nulle part.
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-webhook-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402
from backend import auth, billing  # noqa: E402
from backend.models import BillingCustomer, BillingSubscription, User  # noqa: E402

BUSINESS_PRICE = "price_business_test"
PRO_PRICE = "price_pro_test"
INCONNU = "price_migre_jamais_declare"


@pytest.fixture(autouse=True)
def prix_connus(monkeypatch):
    """Seuls les trois prix declares sont reconnus, comme en production."""
    monkeypatch.setattr(
        billing, "plan_for_price_id",
        lambda p: {BUSINESS_PRICE: "business", PRO_PRICE: "pro"}.get((p or "").strip(), ""),
    )


def _abonne_business() -> tuple[str, str, str]:
    """Un client sur Business, tel que le compte de test l'est aujourd'hui."""
    app_module.DB.create_tables()
    tag = uuid.uuid4().hex[:8]
    with app_module.DB.session() as db:
        user = User(email="client-%s@exemple.fr" % tag,
                    password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(user)
        db.commit()
        db.refresh(user)
        uid = str(user.id)
        # Le mapping client->utilisateur est INDISPENSABLE : `upsert_subscription` retrouve
        # l'utilisateur par la table `billing_customers`, jamais par l'abonnement lui-meme. Sans
        # lui, la fonction sort en rendant None et le webhook repond 200 sans rien faire — c'est
        # ce qui a fait echouer quatre de ces tests a leur premiere ecriture, et j'ai failli
        # l'annoncer comme un defaut du produit. En production, `checkout.session.completed` pose
        # ce mapping avant tout evenement d'abonnement.
        db.add(BillingCustomer(user_id=uid, stripe_customer_id="cus_%s" % tag))
        db.add(BillingSubscription(
            user_id=uid, stripe_customer_id="cus_%s" % tag,
            stripe_subscription_id="sub_%s" % tag, stripe_price_id=BUSINESS_PRICE,
            plan_key="business", status="active",
            stripe_data={"id": "sub_%s" % tag, "status": "active"},
        ))
        db.commit()
    return uid, "cus_%s" % tag, "sub_%s" % tag


def _evenement(etype: str, *, cid: str, sid: str, price: str, status: str = "active") -> dict:
    """La forme que Stripe envoie : l'objet EST l'abonnement pour `customer.subscription.*`."""
    return {
        "type": etype,
        "data": {"object": {
            "id": sid, "customer": cid, "status": status,
            "items": {"data": [{"id": "si_x", "price": {"id": price}}]},
            "metadata": {},
        }},
    }


def _plan_et_quota(uid: str) -> tuple[str, dict]:
    with app_module.DB.session() as db:
        return (billing.effective_plan_key(db, user_id=uid),
                billing.plan_limits(db, user_id=uid))


def _envoyer(evenement: dict) -> None:
    with app_module.DB.session() as db:
        billing.handle_stripe_event(db, event=evenement)


# --- le checkpoint du 27/09, joue d'avance --------------------------------------------------

def test_le_downgrade_business_vers_pro_change_REELLEMENT_le_quota() -> None:
    """C'est l'evenement que le 27/09 doit produire, et la seule chose qui prouve qu'il a servi.

    Un plan qui change sans que le quota suive serait exactement l'etat invisible qu'on cherche a
    ecarter : le client paierait Pro en gardant les droits Business.
    """
    uid, cid, sid = _abonne_business()
    avant, quota_avant = _plan_et_quota(uid)
    assert avant == "business"

    _envoyer(_evenement("customer.subscription.updated", cid=cid, sid=sid, price=PRO_PRICE))

    apres, quota_apres = _plan_et_quota(uid)
    assert apres == "pro", "le downgrade n'a pas ete porte jusqu'au plan effectif"
    assert quota_apres != quota_avant, (
        "le plan a change mais les limites non : c'est l'etat invisible qu'on redoute")


def test_un_abonnement_SUPPRIME_retire_les_droits() -> None:
    """`customer.subscription.deleted` : le maillon que l'historique du projet dit jamais exerce."""
    uid, cid, sid = _abonne_business()
    _envoyer(_evenement("customer.subscription.deleted", cid=cid, sid=sid,
                        price=BUSINESS_PRICE, status="canceled"))
    plan, _ = _plan_et_quota(uid)
    assert plan == "free", "un abonnement supprime laisse les droits ouverts"


def test_une_montee_en_gamme_ouvre_les_droits_tout_de_suite() -> None:
    """Le sens inverse doit marcher aussi, sinon le client paie sans recevoir."""
    uid, cid, sid = _abonne_business()
    _envoyer(_evenement("customer.subscription.updated", cid=cid, sid=sid, price=PRO_PRICE))
    assert _plan_et_quota(uid)[0] == "pro"
    _envoyer(_evenement("customer.subscription.updated", cid=cid, sid=sid, price=BUSINESS_PRICE))
    assert _plan_et_quota(uid)[0] == "business"


# --- le prix inconnu ------------------------------------------------------------------------

def test_un_prix_INCONNU_ne_degrade_pas_un_client_payant() -> None:
    """Un tarif migre, un identifiant renomme, une variable absente au passage en live.

    Aucun de ces cas n'est une decision du client, et aucun ne doit lui retirer ce qu'il paie. Le
    webhook repond 200 : si la degradation passe, rien nulle part ne la signale.
    """
    uid, cid, sid = _abonne_business()
    _envoyer(_evenement("customer.subscription.updated", cid=cid, sid=sid, price=INCONNU))
    plan, _ = _plan_et_quota(uid)
    assert plan == "business", (
        "un prix inconnu a fait tomber un client payant en free : c'est une devinette, "
        "pas une intention")


def test_un_abonnement_JAMAIS_VU_n_accorde_aucun_droit() -> None:
    """L'autre bord de la garde, et le plus dangereux des deux.

    Conserver le plan existant n'a de sens que s'il en existe un. Sur un abonnement inconnu, il
    n'y a rien a conserver, et choisir un plan reviendrait a offrir des droits que personne n'a
    payes. Une mutation a survecu a la premiere version de ce fichier — elle accordait `business`
    a tout abonnement neuf, et aucun test ne s'en apercevait.
    """
    uid, cid, _ = _abonne_business()
    _envoyer(_evenement("customer.subscription.updated", cid=cid, sid="sub_jamais_vu",
                        price=INCONNU))
    with app_module.DB.session() as db:
        from backend.models import BillingSubscription as _BS
        from sqlalchemy import select as _select
        neuf = db.scalar(_select(_BS).where(_BS.stripe_subscription_id == "sub_jamais_vu"))
    assert neuf is not None, "l'abonnement aurait du etre enregistre"
    assert neuf.plan_key == "free", (
        "un abonnement inconnu a recu %r : des droits que personne n'a payes" % neuf.plan_key)


def test_un_prix_inconnu_sur_un_abonnement_ANNULE_laisse_bien_tomber_les_droits() -> None:
    """La garde ci-dessus ne doit pas devenir un moyen de garder ses droits apres resiliation :
    c'est le STATUT qui tranche alors, pas le prix."""
    uid, cid, sid = _abonne_business()
    _envoyer(_evenement("customer.subscription.deleted", cid=cid, sid=sid,
                        price=INCONNU, status="canceled"))
    assert _plan_et_quota(uid)[0] == "free"
