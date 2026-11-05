# -*- coding: utf-8 -*-
"""Le captcha d'inscription (Cloudflare Turnstile, 05/11/2026), en plus du piège et du délai.

Ce que ces tests défendent :
  * sans ses DEUX clés, il n'existe pas : ni case, ni script, ni appel à Cloudflare, ni CSP ouverte ;
  * avec elles, un jeton absent, faux, périmé ou déjà servi ne crée AUCUN compte et ne fait
    partir AUCUN email, et le journal garde la réponse de Cloudflare ;
  * une panne de Cloudflare, ou notre clé secrète refusée, ne ferme PAS l'inscription : le piège
    et le délai gardent la porte, et l'incident est journalisé ;
  * la CSP ne s'ouvre à challenges.cloudflare.com que sur la page qui affiche la case.

Le vrai Cloudflare a été interrogé le 05/11/2026 avec ses clés de test (essai local, Chromium) :
jeton accepté, refusé (invalid-input-response), déjà servi (timeout-or-duplicate), case en échec,
clé secrète fausse (invalid-input-secret) — les réponses simulées ici sont celles-là.
"""
from __future__ import annotations

import os
import re
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-captcha-"))
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

SITE = "0x4AAAAAAAtestSiteKeyNoyaru"
SECRETE = "0x4AAAAAAAtestSecretKeyNoyaruXXXXXXXX"
CLOUDFLARE = "https://challenges.cloudflare.com"


@pytest.fixture()
def cloudflare(monkeypatch):
    """Ce que Cloudflare reçoit, et ce qu'il répond (`reponse`, ou une exception)."""
    etat = {"recu": [], "reponse": {"success": True, "error-codes": []}}

    def repondre(donnees):
        etat["recu"].append(dict(donnees))
        if isinstance(etat["reponse"], Exception):
            raise etat["reponse"]
        return etat["reponse"]

    monkeypatch.setattr(anti_robots, "_demander_a_cloudflare", repondre)
    return etat


# ── La décision ──────────────────────────────────────────────────────────────────────────────

def test_un_jeton_ACCEPTE_par_Cloudflare_passe(cloudflare) -> None:
    assert anti_robots.verifier_captcha(SECRETE, "jeton-de-la-case", "203.0.113.7") == ("", "")
    assert cloudflare["recu"] == [{"secret": SECRETE, "response": "jeton-de-la-case", "remoteip": "203.0.113.7"}]


def test_une_adresse_IP_inconnue_n_est_pas_envoyee(cloudflare) -> None:
    anti_robots.verifier_captcha(SECRETE, "jeton", "unknown")
    assert "remoteip" not in cloudflare["recu"][0]


@pytest.mark.parametrize("jeton, raison", [
    ("", "captcha non résolu"),
    ("   ", "captcha non résolu"),
    ("x" * (anti_robots.JETON_MAX + 1), "trop long"),
], ids=["vide", "blanc", "trop-long"])
def test_sans_jeton_valable_REFUS_sans_meme_appeler_Cloudflare(cloudflare, jeton, raison) -> None:
    refus, incident = anti_robots.verifier_captcha(SECRETE, jeton, "203.0.113.7")
    assert raison in refus and incident == ""
    assert cloudflare["recu"] == []


@pytest.mark.parametrize("codes", [["invalid-input-response"], ["timeout-or-duplicate"],
                                   ["missing-input-response"], []])
def test_un_jeton_que_Cloudflare_DECLARE_faux_est_REFUSE(cloudflare, codes) -> None:
    cloudflare["reponse"] = {"success": False, "error-codes": codes}
    refus, incident = anti_robots.verifier_captcha(SECRETE, "jeton", "203.0.113.7")
    assert refus.startswith("captcha refusé par Cloudflare") and incident == ""
    assert all(c in refus for c in codes)


@pytest.mark.parametrize("reponse", [
    {"success": False, "error-codes": ["invalid-input-secret"]},
    {"success": False, "error-codes": ["internal-error"]},
    {"success": False, "error-codes": ["bad-request"]},
    ["pas", "un", "objet"],
], ids=["cle-secrete-refusee", "panne", "requete", "illisible"])
def test_NOTRE_erreur_ou_celle_de_Cloudflare_n_est_pas_un_refus(cloudflare, reponse) -> None:
    cloudflare["reponse"] = reponse
    refus, incident = anti_robots.verifier_captcha(SECRETE, "jeton", "203.0.113.7")
    assert refus == "" and incident


def test_Cloudflare_INJOIGNABLE_est_un_incident_qui_ne_cite_jamais_la_cle(cloudflare) -> None:
    cloudflare["reponse"] = ConnectionError("échec vers siteverify avec secret=" + SECRETE)
    refus, incident = anti_robots.verifier_captcha(SECRETE, "jeton", "203.0.113.7")
    assert refus == "" and incident == "Cloudflare injoignable (ConnectionError)"
    assert SECRETE not in incident


@pytest.mark.parametrize("site, secrete, actif", [
    (SITE, SECRETE, True),
    ("1x00000000000000000000AA", "1x0000000000000000000000000000000AA", True),
    (SITE, "", False),
    ("", SECRETE, False),
    ("G-TEST1234", SECRETE, False),
    (SITE, "cle secrete avec espaces", False),
], ids=["production", "cles-de-test", "sans-secrete", "sans-site", "site-mal-forme", "secrete-mal-formee"])
def test_les_DEUX_cles_bien_formees_ou_rien(site, secrete, actif) -> None:
    assert anti_robots.cles_captcha(site, secrete) == ((site, secrete) if actif else ("", ""))


# ── Par la vraie route ───────────────────────────────────────────────────────────────────────

@pytest.fixture()
def envois(monkeypatch):
    m.DB.create_tables()
    partis: list[str] = []
    monkeypatch.setattr(m, "_email_verification_enabled", lambda: True)
    monkeypatch.setattr(m, "_send_email_verification_email",
                        lambda **kw: partis.append(kw.get("to_email")))
    # Le limiteur garde ses compteurs EN BASE, partagés par toute la suite (20 inscriptions par
    # IP en 15 min) : la première barrière de ce fichier les a épuisés, et les tests d'inscription
    # joués après lui répondaient « Trop de tentatives ». Il n'est pas ce qu'on teste ici.
    monkeypatch.setattr(m, "_rate_limit_retry_after", lambda **kwargs: None)
    for v in ("TURNSTILE_SITE_KEY", "TURNSTILE_SECRET_KEY", "SIGNUP_DISABLED", "SIGNUP_INVITE_CODE"):
        monkeypatch.delenv(v, raising=False)
    return partis


@pytest.fixture()
def avec_captcha(monkeypatch):
    monkeypatch.setenv("TURNSTILE_SITE_KEY", SITE)
    monkeypatch.setenv("TURNSTILE_SECRET_KEY", SECRETE)


def _inscrire(client, email, **en_plus):
    page = client.get("/auth/signup")
    csrf = re.search(r'name="_csrf"\s+value="([^"]*)"', page.text).group(1)
    donnees = {"email": email, "password": "un-mot-de-passe-assez-long", "next": "/",
               "country": "FR", "country_requested": "yes", "_csrf": csrf,
               "formulaire_ouvert": jeton_humain()}
    donnees.update(en_plus)
    return client.post("/auth/signup", data=donnees, follow_redirects=False)


def _compte(email):
    with m.DB.session() as db:
        return db.scalar(select(User).where(User.email == email))


def _journal(email, action="auth.signup"):
    with m.DB.session() as db:
        return [(r.status, (r.meta or {}).get("reason")) for r in db.scalars(
            select(AuditLog).where(AuditLog.action == action, AuditLog.actor_email == email))]


def _csp(reponse) -> dict[str, str]:
    return {d.split(" ", 1)[0]: d for d in reponse.headers["content-security-policy"].split("; ")}


def test_SANS_cles_ni_case_ni_script_ni_Cloudflare(envois, cloudflare) -> None:
    email = "sans-cles-%s@exemple.fr" % uuid.uuid4().hex[:8]
    with TestClient(m.app) as client:
        page = client.get("/auth/signup")
        reponse = _inscrire(client, email)
    assert "cf-turnstile" not in page.text and CLOUDFLARE not in page.text
    assert CLOUDFLARE not in page.headers["content-security-policy"]
    assert _csp(page)["frame-src"] == "frame-src 'none'"
    assert reponse.status_code == 303 and _compte(email) is not None and envois == [email]
    assert cloudflare["recu"] == []


@pytest.mark.parametrize("variable", ["TURNSTILE_SITE_KEY", "TURNSTILE_SECRET_KEY"])
def test_UNE_seule_cle_ne_fait_rien(envois, cloudflare, monkeypatch, variable) -> None:
    """Une case sans clé secrète ne vérifierait rien ; une clé secrète sans case refuserait
    toutes les inscriptions."""
    monkeypatch.setenv(variable, SITE if variable == "TURNSTILE_SITE_KEY" else SECRETE)
    email = "une-cle-%s@exemple.fr" % uuid.uuid4().hex[:8]
    with TestClient(m.app) as client:
        page = client.get("/auth/signup")
        _inscrire(client, email)
    assert "cf-turnstile" not in page.text and CLOUDFLARE not in page.headers["content-security-policy"]
    assert _compte(email) is not None and cloudflare["recu"] == []


def test_AVEC_cles_la_case_son_script_et_la_CSP_de_CETTE_page(envois, avec_captcha) -> None:
    with TestClient(m.app) as client:
        page = client.get("/auth/signup")
        autres = [client.get(p) for p in ("/auth/login", "/", "/privacy")]
    soupe = BeautifulSoup(page.text, "html.parser")
    case = soupe.select_one('form[action="/auth/signup"] div.cf-turnstile')
    assert case and case["data-sitekey"] == SITE and case["data-language"] == "fr"
    # Avant le bouton « Créer le compte » : le jeton fait partie du formulaire envoyé.
    assert case.find_next("button")["type"] == "submit"
    assert soupe.select_one('script[src="%s/turnstile/v0/api.js"]' % CLOUDFLARE)
    assert _csp(page)["script-src"].endswith(" " + CLOUDFLARE)
    assert _csp(page)["frame-src"] == "frame-src " + CLOUDFLARE
    for autre in autres:
        assert CLOUDFLARE not in autre.headers["content-security-policy"], autre.url
        assert "cf-turnstile" not in autre.text


def test_la_case_parle_la_langue_de_la_page(envois, avec_captcha) -> None:
    with TestClient(m.app) as client:
        page = client.get("/auth/signup", headers={"Cookie": "noyaru_lang=en"})
    case = BeautifulSoup(page.text, "html.parser").select_one("div.cf-turnstile")
    assert case["data-language"] == "en"


def test_inscriptions_FERMEES_pas_de_case(envois, avec_captcha, monkeypatch) -> None:
    monkeypatch.setenv("SIGNUP_DISABLED", "true")
    with TestClient(m.app) as client:
        page = client.get("/auth/signup")
    assert "cf-turnstile" not in page.text and CLOUDFLARE not in page.headers["content-security-policy"]


def test_une_personne_dont_la_case_est_validee_s_inscrit(envois, avec_captcha, cloudflare) -> None:
    email = "personne-%s@exemple.fr" % uuid.uuid4().hex[:8]
    with TestClient(m.app) as client:
        reponse = _inscrire(client, email, **{anti_robots.CHAMP_CAPTCHA: "jeton-de-la-case"})
    assert reponse.status_code == 303 and _compte(email) is not None and envois == [email]
    ((recu),) = cloudflare["recu"]
    assert recu["secret"] == SECRETE and recu["response"] == "jeton-de-la-case"


@pytest.mark.parametrize("jeton, reponse, raison", [
    ("", None, "captcha non résolu"),
    ("jeton-falsifie", {"success": False, "error-codes": ["invalid-input-response"]}, "invalid-input-response"),
    ("jeton-deja-servi", {"success": False, "error-codes": ["timeout-or-duplicate"]}, "timeout-or-duplicate"),
], ids=["sans-jeton", "refuse", "deja-servi"])
def test_un_robot_ne_cree_RIEN_et_ne_fait_partir_AUCUN_email(envois, avec_captcha, cloudflare,
                                                             jeton, reponse, raison) -> None:
    if reponse:
        cloudflare["reponse"] = reponse
    email = "robot-%s@exemple.fr" % uuid.uuid4().hex[:8]
    with TestClient(m.app) as client:
        refus = _inscrire(client, email, **{anti_robots.CHAMP_CAPTCHA: jeton})
    assert refus.status_code == 303 and "err=" in refus.headers["location"]
    assert "r%C3%A9essayez" in refus.headers["location"]
    assert _compte(email) is None and envois == []
    ((statut, consignee),) = _journal(email)
    assert statut == "robot" and raison in consignee


@pytest.mark.parametrize("reponse", [ConnectionError("hors ligne"),
                                     {"success": False, "error-codes": ["invalid-input-secret"]}],
                         ids=["injoignable", "cle-secrete-refusee"])
def test_une_PANNE_ne_ferme_pas_l_inscription_et_se_journalise(envois, avec_captcha, cloudflare, reponse) -> None:
    cloudflare["reponse"] = reponse
    email = "pendant-la-panne-%s@exemple.fr" % uuid.uuid4().hex[:8]
    with TestClient(m.app) as client:
        _inscrire(client, email, **{anti_robots.CHAMP_CAPTCHA: "jeton-de-la-case"})
    assert _compte(email) is not None and envois == [email]
    ((statut, incident),) = _journal(email, "auth.captcha")
    assert statut == "error" and "Cloudflare" in incident


def test_pendant_une_panne_le_PIEGE_garde_toujours_la_porte(envois, avec_captcha, cloudflare) -> None:
    cloudflare["reponse"] = ConnectionError("hors ligne")
    email = "robot-panne-%s@exemple.fr" % uuid.uuid4().hex[:8]
    with TestClient(m.app) as client:
        _inscrire(client, email, site_web="https://promo.example",
                  **{anti_robots.CHAMP_CAPTCHA: "jeton-de-la-case"})
    assert _compte(email) is None and envois == []


def test_le_piege_et_le_delai_PASSENT_AVANT_et_epargnent_l_appel(envois, avec_captcha, cloudflare) -> None:
    email = "robot-rapide-%s@exemple.fr" % uuid.uuid4().hex[:8]
    with TestClient(m.app) as client:
        _inscrire(client, email, formulaire_ouvert="", **{anti_robots.CHAMP_CAPTCHA: "jeton"})
    assert cloudflare["recu"] == []
    ((statut, consignee),) = _journal(email)
    assert statut == "robot" and "heure d'ouverture" in consignee


# ── Le réglage, dans Système ─────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("cle", ["TURNSTILE_SITE_KEY", "TURNSTILE_SECRET_KEY"])
def test_Systeme_refuse_une_cle_qui_n_a_pas_la_forme_de_Cloudflare(cle) -> None:
    assert m._validate_settings_env_value(cle, SITE if cle == "TURNSTILE_SITE_KEY" else SECRETE) is None
    for faux in ("G-TEST1234", "ma cle", "0x"):
        assert "0x" in (m._validate_settings_env_value(cle, faux) or "")
    assert m._SETTINGS_ENV_KEYS[cle]["editable"]
