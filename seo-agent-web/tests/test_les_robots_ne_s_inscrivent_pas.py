# -*- coding: utf-8 -*-
"""Les robots ne s'inscrivent pas, et ne font envoyer aucun email.

Relevé du 02/11/2026 : 39 des 44 comptes de la plateforme étaient des inscriptions de robots
faites avec les adresses réelles de personnes qui n'avaient rien demandé — chacune a reçu un
email de Noyaru. Ce que ces tests défendent : un robot (champ piège rempli, formulaire non servi
par Noyaru, envoi plus rapide qu'une personne) ne crée AUCUN compte et ne fait partir AUCUN
email ; le journal garde la vraie raison ; une personne, elle, s'inscrit comme avant.
"""
from __future__ import annotations

import os
import re
import sys
import tempfile
import time
import uuid
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-robots-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")

from bs4 import BeautifulSoup  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import select  # noqa: E402

from backend import anti_robots  # noqa: E402
from backend import app as m  # noqa: E402
from backend.models import AuditLog, User  # noqa: E402
from _inscription_humaine import jeton_humain  # noqa: E402

CLE = "une-cle-de-test"


# ── La décision ──────────────────────────────────────────────────────────────────────────────

def test_une_PERSONNE_passe() -> None:
    jeton = anti_robots.jeton_d_ouverture(CLE, maintenant=1000)
    assert anti_robots.raison_de_refus(CLE, piege="", jeton=jeton, maintenant=1030) == ""


@pytest.mark.parametrize("piege, jeton, maintenant, raison", [
    ("https://spam.example", None, 1030, "champ piège rempli"),
    ("", "", 1030, "sans heure d'ouverture"),
    ("", "1000.0123456789abcdef0123456789abcdef", 1030, "falsifiée"),
    ("", None, 1001, "envoyé 1.0 s après l'ouverture"),
    ("", None, 1000 + 3 * 3600, "plus de deux heures"),
], ids=["piege", "sans-jeton", "falsifie", "trop-rapide", "trop-vieux"])
def test_un_ROBOT_est_reconnu(piege, jeton, maintenant, raison) -> None:
    jeton = anti_robots.jeton_d_ouverture(CLE, maintenant=1000) if jeton is None else jeton
    assert raison in anti_robots.raison_de_refus(CLE, piege=piege, jeton=jeton, maintenant=maintenant)


def test_un_jeton_signe_avec_une_AUTRE_cle_ne_passe_pas() -> None:
    jeton = anti_robots.jeton_d_ouverture("autre-cle", maintenant=1000)
    assert "falsifiée" in anti_robots.raison_de_refus(CLE, piege="", jeton=jeton, maintenant=1030)


def test_les_bornes_du_delai() -> None:
    jeton = anti_robots.jeton_d_ouverture(CLE, maintenant=1000)
    assert anti_robots.raison_de_refus(CLE, piege="", jeton=jeton, maintenant=1000 + anti_robots.DELAI_MIN_S) == ""
    assert anti_robots.raison_de_refus(CLE, piege="", jeton=jeton, maintenant=1000 + anti_robots.AGE_MAX_S) == ""


# ── Par la vraie route ───────────────────────────────────────────────────────────────────────

@pytest.fixture()
def envois(monkeypatch):
    m.DB.create_tables()
    partis: list[str] = []
    monkeypatch.setattr(m, "_email_verification_enabled", lambda: True)
    monkeypatch.setattr(m, "_send_email_verification_email",
                        lambda **kw: partis.append(kw.get("to_email")))
    return partis


def _inscrire(client, email, **en_plus):
    page = client.get("/auth/signup")
    csrf = re.search(r'name="_csrf"\s+value="([^"]*)"', page.text).group(1)
    donnees = {"email": email, "password": "un-mot-de-passe-assez-long", "next": "/",
               "country": "FR", "country_requested": "yes", "_csrf": csrf}
    donnees.update(en_plus)
    return client.post("/auth/signup", data=donnees, follow_redirects=False)


def _compte(email):
    with m.DB.session() as db:
        return db.scalar(select(User).where(User.email == email))


def _journal(email):
    with m.DB.session() as db:
        return [(r.status, (r.meta or {}).get("reason")) for r in db.scalars(
            select(AuditLog).where(AuditLog.action == "auth.signup", AuditLog.actor_email == email))]


@pytest.mark.parametrize("en_plus, raison", [
    ({"site_web": "https://promo.example", "formulaire_ouvert": "JETON"}, "champ piège rempli"),
    ({}, "formulaire sans heure d'ouverture"),
    ({"formulaire_ouvert": "PAGE"}, "envoyé"),
], ids=["piege", "sans-jeton", "envoi-immediat"])
def test_un_robot_ne_cree_RIEN_et_ne_fait_partir_AUCUN_email(envois, en_plus, raison) -> None:
    email = "robot-%s@exemple.fr" % uuid.uuid4().hex[:8]
    with TestClient(m.app) as client:
        if en_plus.get("formulaire_ouvert") == "JETON":
            en_plus["formulaire_ouvert"] = jeton_humain()
        if en_plus.get("formulaire_ouvert") == "PAGE":
            # Le jeton que la page vient de servir, renvoyé aussitôt : comme un robot.
            page = client.get("/auth/signup").text
            en_plus["formulaire_ouvert"] = re.search(r'name="formulaire_ouvert" value="([^"]+)"', page).group(1)
        reponse = _inscrire(client, email, **en_plus)
    assert reponse.status_code == 303 and "err=" in reponse.headers["location"]
    assert _compte(email) is None and envois == []
    ((statut, consignee),) = _journal(email)
    assert statut == "robot" and raison in consignee


def test_une_personne_s_inscrit_et_recoit_son_email(envois) -> None:
    email = "personne-%s@exemple.fr" % uuid.uuid4().hex[:8]
    with TestClient(m.app) as client:
        reponse = _inscrire(client, email, formulaire_ouvert=jeton_humain())
    assert reponse.status_code == 303 and _compte(email) is not None
    assert envois == [email]


def test_la_personne_trop_RAPIDE_peut_reessayer(envois) -> None:
    """Un gestionnaire de mots de passe peut tout remplir en une seconde : le refus dit de
    réessayer, et le second envoi passe — un clic de plus, pas un compte perdu."""
    email = "rapide-%s@exemple.fr" % uuid.uuid4().hex[:8]
    with TestClient(m.app) as client:
        page = client.get("/auth/signup").text
        aussitot = re.search(r'name="formulaire_ouvert" value="([^"]+)"', page).group(1)
        refus = _inscrire(client, email, formulaire_ouvert=aussitot)
        assert "r%C3%A9essayez" in refus.headers["location"] or "réessayez" in refus.headers["location"]
        assert _inscrire(client, email, formulaire_ouvert=jeton_humain()).status_code == 303
    assert _compte(email) is not None


def test_le_formulaire_porte_le_jeton_et_un_piege_INVISIBLE() -> None:
    with TestClient(m.app) as client:
        soupe = BeautifulSoup(client.get("/auth/signup").text, "html.parser")
    jeton = soupe.select_one('form[action="/auth/signup"] input[name="formulaire_ouvert"]')
    assert jeton and jeton["type"] == "hidden" and re.fullmatch(r"\d+\.[0-9a-f]{32}", jeton["value"])
    assert int(jeton["value"].split(".")[0]) <= time.time()
    piege = soupe.select_one('form[action="/auth/signup"] input[name="site_web"]')
    boite = piege.find_parent("div")
    assert piege["tabindex"] == "-1" and piege["autocomplete"] == "off" and not piege.get("value")
    assert boite["aria-hidden"] == "true" and "left:-10000px" in boite["style"].replace(" ", "")
