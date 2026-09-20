# -*- coding: utf-8 -*-
"""Chaque webhook Stripe laisse une ligne, y compris — surtout — ceux qu'on refuse.

POURQUOI MAINTENANT, ET PAS APRES. Le 27/09/2026, un `subscription_schedule` doit faire basculer
le compte de test de Business vers Pro. Le proprietaire a accepte quatre semaines sans facturer
pour OBSERVER ce moment, parce qu'un quota Business reste ouvert sur un abonnement Pro serait
invisible. Tout le benefice du rendez-vous tient donc a une chose : pouvoir dire, le jour dit,
ce qui s'est passe.

CE QUI NE LE PERMETTAIT PAS. Les deux refus de signature repartaient en 400 SANS ecrire une
ligne :

    if not sig:
        return JSONResponse(..., status_code=400)        # aucun log
    except Exception as e:
        return JSONResponse(..., status_code=400)        # aucun log

Stripe voit l'echec dans son tableau de bord ; nous, rien. Et l'effet visible cote client est
seulement un plan qui n'a pas change. Le cas est REEL et documente : l'application lit UN seul
`STRIPE_WEBHOOK_SECRET`, elle est donc entierement en test ou entierement en live. Une bascule
partielle des cles produirait exactement ca — un evenement signe par l'autre secret, rejete en
silence.

LES TROIS ETATS QU'ON POUVAIT CONFONDRE, et qui se distinguent maintenant :

    « webhook REFUSE »                 il n'est jamais entre
    « plan INCHANGE »                  il est entre et n'a rien change
    « plan business -> pro »           il a fait son travail

Le deuxieme est celui qui manquait, et c'est le diagnostic du 27/09 : un evenement traite sans
effet ressemble a un evenement jamais arrive. Meme famille que le worker muet et la troncature
silencieuse corriges la veille — le systeme sait quelque chose qu'il ne dit pas.

CE QU'ON NE JOURNALISE PAS : la signature elle-meme. C'est un secret partage ; son VERDICT
suffit au diagnostic.
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-wh-log-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as m  # noqa: E402
from backend import auth, billing  # noqa: E402
from backend.app import app  # noqa: E402
from backend.models import BillingCustomer, BillingSubscription, User  # noqa: E402

BUSINESS_PRICE = "price_business_test"
PRO_PRICE = "price_pro_test"


@pytest.fixture(autouse=True)
def prix_connus(monkeypatch):
    monkeypatch.setattr(
        billing, "plan_for_price_id",
        lambda p: {BUSINESS_PRICE: "business", PRO_PRICE: "pro"}.get((p or "").strip(), ""))


def _abonne_business() -> tuple[str, str, str]:
    """Le compte de test tel qu'il est aujourd'hui : Business, actif."""
    m.DB.create_tables()
    tag = uuid.uuid4().hex[:8]
    with m.DB.session() as db:
        u = User(email="c-%s@exemple.fr" % tag,
                 password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(u)
        db.commit()
        db.refresh(u)
        uid = str(u.id)
        # Sans ce mapping, `upsert_subscription` ne retrouve pas l'utilisateur et sort en
        # rendant None — le webhook repond 200 sans rien faire.
        db.add(BillingCustomer(user_id=uid, stripe_customer_id="cus_%s" % tag))
        db.add(BillingSubscription(
            user_id=uid, stripe_customer_id="cus_%s" % tag,
            stripe_subscription_id="sub_%s" % tag, stripe_price_id=BUSINESS_PRICE,
            plan_key="business", status="active",
            stripe_data={"id": "sub_%s" % tag, "status": "active"}))
        db.commit()
    return uid, "cus_%s" % tag, "sub_%s" % tag


def _evenement(etype: str, *, cid: str, sid: str, price: str, status: str = "active") -> dict:
    return {
        "type": etype, "id": "evt_%s" % uuid.uuid4().hex[:8],
        "data": {"object": {
            "id": sid, "customer": cid, "status": status,
            "items": {"data": [{"id": "si_x", "price": {"id": price}}]},
            "metadata": {}}},
    }


def _poster(monkeypatch, caplog, *, signature: str = "t=1,v1=abc",
            construit=None) -> tuple[int, str]:
    """Poste sur /stripe/webhook et rend (statut, journal)."""
    if construit is not None:
        monkeypatch.setattr(billing, "construct_webhook_event", construit)
    entetes = {"stripe-signature": signature} if signature else {}
    with caplog.at_level(logging.INFO):
        rep = TestClient(app).post("/stripe/webhook", content=b'{"x":1}', headers=entetes)
    return rep.status_code, caplog.text


# --- les refus, qui ne disaient rien ----------------------------------------------------------

def test_une_signature_ABSENTE_laisse_une_ligne(monkeypatch, caplog) -> None:
    code, journal = _poster(monkeypatch, caplog, signature="")
    assert code == 400
    assert "webhook REFUSE" in journal and "signature absent" in journal, journal


def test_une_signature_INVALIDE_laisse_une_ligne_et_nomme_le_reglage(monkeypatch, caplog) -> None:
    """LE CAS DU 27/09 : un evenement signe pour l'autre secret. Dire « signature invalide »
    sans nommer `STRIPE_WEBHOOK_SECRET` laisserait chercher du cote de Stripe."""

    def _refuse(**_kw):
        raise ValueError("signature mismatch")

    code, journal = _poster(monkeypatch, caplog, construit=_refuse)
    assert code == 400
    assert "webhook REFUSE" in journal and "signature invalide" in journal, journal
    assert "STRIPE_WEBHOOK_SECRET" in journal, journal


def test_la_SIGNATURE_elle_meme_n_est_jamais_journalisee(monkeypatch, caplog) -> None:
    """C'est un secret partage : son verdict suffit au diagnostic."""
    secret = "v1=ce-secret-ne-doit-pas-fuir"

    def _refuse(**_kw):
        raise ValueError("bad")

    _code, journal = _poster(monkeypatch, caplog, signature="t=1,%s" % secret, construit=_refuse)
    assert "ce-secret-ne-doit-pas-fuir" not in journal, journal


# --- ce qui passe ------------------------------------------------------------------------------

def _ligne(journal: str, marque: str) -> str:
    """LA ligne qui porte `marque`, et pas le journal entier.

    MES DEUX PREMIERS TESTS CHERCHAIENT DANS TOUT LE TEXTE : l'identifiant de l'evenement
    apparaissait deja sur la ligne « recu », donc retirer celui de la ligne « traite » ou de
    la ligne d'erreur ne cassait rien. Deux mutations y ont survecu. Chercher une sous-chaine
    dans un journal exige de dire SUR QUELLE LIGNE on l'attend — c'est la troisieme fois que
    ce piege se referme sur moi.
    """
    lignes = [ligne for ligne in journal.splitlines() if marque in ligne]
    assert len(lignes) == 1, "%d ligne(s) portant %r :\n%s" % (len(lignes), marque, journal)
    return lignes[0]


def test_un_webhook_ACCEPTE_dit_son_type_et_son_identifiant(monkeypatch, caplog) -> None:
    uid, cid, sid = _abonne_business()
    ev = _evenement("customer.subscription.updated", cid=cid, sid=sid, price=PRO_PRICE)
    code, journal = _poster(monkeypatch, caplog, construit=lambda **_kw: ev)
    assert code == 200

    recu = _ligne(journal, "webhook recu")
    assert ev["type"] in recu and ev["id"] in recu, recu
    traite = _ligne(journal, "traite")
    assert ev["type"] in traite and ev["id"] in traite, traite
    assert uid  # l'abonne existe bien


def test_un_handler_EN_ERREUR_nomme_l_evenement(monkeypatch, caplog) -> None:
    """Sans le type ni l'identifiant SUR SA LIGNE, un 500 ne se rattache a rien : les journaux
    de production entrelacent les requetes, la ligne « recu » peut etre loin au-dessus."""
    ev = _evenement("customer.subscription.updated", cid="cus_x", sid="sub_x", price=PRO_PRICE)

    def _boum(_db, *, event):
        raise RuntimeError("base injoignable")

    monkeypatch.setattr(billing, "handle_stripe_event", _boum)
    code, journal = _poster(monkeypatch, caplog, construit=lambda **_kw: ev)
    assert code == 500

    erreur = _ligne(journal, "EN ERREUR")
    assert ev["type"] in erreur and ev["id"] in erreur, erreur


# --- le discriminant du 27/09 -------------------------------------------------------------------

def test_un_downgrade_ECRIT_la_transition(caplog) -> None:
    """Le rendez-vous lui-meme : Business -> Pro doit se lire dans les journaux."""
    uid, cid, sid = _abonne_business()
    with caplog.at_level(logging.INFO):
        with m.DB.session() as db:
            billing.handle_stripe_event(db, event=_evenement(
                "customer.subscription.updated", cid=cid, sid=sid, price=PRO_PRICE))
    assert "plan business -> pro" in caplog.text, caplog.text
    assert uid in caplog.text and sid in caplog.text, caplog.text


def test_un_evenement_SANS_EFFET_le_dit_aussi(caplog) -> None:
    """LE DIAGNOSTIC QUI MANQUAIT. « Arrive et sans effet » ressemblait a « jamais arrive ».

    Le 27/09, si le plan ne bouge pas, cette ligne est la seule chose qui distinguera un
    webhook rejete d'un webhook traite dont l'effet est nul.
    """
    _uid, cid, sid = _abonne_business()
    with caplog.at_level(logging.INFO):
        with m.DB.session() as db:
            billing.handle_stripe_event(db, event=_evenement(
                "customer.subscription.updated", cid=cid, sid=sid, price=BUSINESS_PRICE))
    assert "plan INCHANGE" in caplog.text, caplog.text
    assert "business" in caplog.text


def test_la_trace_ne_fait_pas_tomber_le_webhook(monkeypatch, caplog) -> None:
    """Journaliser ne doit jamais casser ce qu'on journalise : un plan illisible se tait."""
    _uid, cid, sid = _abonne_business()
    appels = {"n": 0}
    vrai = billing.effective_plan_key

    def _casse_la_seconde_fois(db, *, user_id):
        appels["n"] += 1
        if appels["n"] >= 2:
            raise RuntimeError("lecture impossible")
        return vrai(db, user_id=user_id)

    monkeypatch.setattr(billing, "effective_plan_key", _casse_la_seconde_fois)
    with m.DB.session() as db:
        billing.handle_stripe_event(db, event=_evenement(
            "customer.subscription.updated", cid=cid, sid=sid, price=PRO_PRICE))
    # L'abonnement a bien ete ecrit malgre l'echec de la trace.
    with m.DB.session() as db:
        assert vrai(db, user_id=_uid) == "pro"


# --- l'enumeration -------------------------------------------------------------------------------

def test_AUCUNE_sortie_du_webhook_n_est_muette() -> None:
    """Le garde-fou qui couvre le refus qu'on ajoutera demain.

    Les tests ci-dessus verrouillent les quatre issues d'aujourd'hui. Celui-ci relit le source
    et exige que chaque `return` de `stripe_webhook` soit precede d'une ligne de journal —
    c'est la forme du defaut qu'on corrige, et elle se reintroduit en une ligne.
    """
    import ast
    import inspect
    import textwrap

    arbre = ast.parse(textwrap.dedent(inspect.getsource(m.stripe_webhook)))
    fn = next(n for n in ast.walk(arbre)
              if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)))

    muets: list[int] = []
    for bloc in ast.walk(fn):
        corps = getattr(bloc, "body", None)
        if not isinstance(corps, list):
            continue
        for i, noeud in enumerate(corps):
            if not isinstance(noeud, ast.Return):
                continue
            avant = ast.unparse(ast.Module(body=corps[:i], type_ignores=[]))
            if "logger." not in avant:
                muets.append(noeud.lineno)
    assert not muets, (
        "des sorties de stripe_webhook ne journalisent rien (lignes relatives %r) : "
        "un refus silencieux rend le rendez-vous du 27/09 inobservable" % muets)
