# -*- coding: utf-8 -*-
"""Sentry reçoit où et pourquoi ça casse, jamais les données des clients (05/11/2026).

Avant ce jour, le SDK envoyait par défaut les variables locales de la pile, le corps des
formulaires, l'adresse complète des requêtes et un fil des appels sortants avec leurs paramètres
(clé SerpAPI, jeton Telegram). Ces tests font tourner le VRAI SDK, dans un processus à part (ses
intégrations modifient FastAPI, la journalisation et http.client pour tout le processus), avec
un transport qui garde ce qui serait parti chez Sentry au lieu de l'envoyer.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-sentry-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402
from backend import sentry_filtre  # noqa: E402

JETON_TELEGRAM = "1234567890:AAH5yQkzSecretSecretSecretSecret0123"
SCENARIO = r'''
import json, logging, sys
sys.path.insert(0, sys.argv[1])
import sentry_sdk
from sentry_sdk.transport import Transport
from backend import sentry_filtre

envois = []

class Capture(Transport):
    def capture_envelope(self, envelope):
        for item in envelope.items:
            if item.payload.json is not None:
                envois.append(item.payload.json)
    def flush(self, timeout=None, callback=None):
        pass
    def kill(self):
        pass

sentry_sdk.init(dsn="https://cle@o1.ingest.de.sentry.io/2", transport=Capture(), traces_sample_rate=1.0,
                **sentry_filtre.options())

# 1. Une erreur du code : données d'un client en variables locales, dans le message, dans le fil.
def corriger(page):
    contenu_client = "Jean Dupont, jean.dupont@client.fr, 06 12 34 56 78"
    raise ValueError("échec de la correction pour jean.dupont@client.fr")

logging.getLogger("seo_agent").warning("[MAIL] envoi impossible pour marie@client.fr")
sentry_sdk.add_breadcrumb(category="httplib", message="appel sortant",
                          data={"url": "https://api.telegram.org/bot%s/sendMessage" % sys.argv[2],
                                "http.query": "q=seo&api_key=CLE-SERPAPI-SECRETE", "method": "GET"})
try:
    corriger("<html>Jean Dupont</html>")
except ValueError as e:
    sentry_sdk.capture_exception(e)

# 2. Une requête reçue qui échoue : un formulaire, des paramètres d'adresse.
from fastapi import FastAPI, Form
from fastapi.testclient import TestClient

app = FastAPI()

@app.post("/auth/signup")
async def inscription(email: str = Form(...), password: str = Form(...)):
    raise RuntimeError("inscription impossible")

TestClient(app, raise_server_exceptions=False).post(
    "/auth/signup?email=paul@client.fr&token=jeton-de-verification",
    data={"email": "paul@client.fr", "password": "mot-de-passe-secret"})
sentry_sdk.flush()
# En ASCII : la console Windows relirait des accents écrits en UTF-8 dans son propre encodage.
print("ENVOIS=" + json.dumps(envois, ensure_ascii=True, default=str))
'''
# Ce qui ne doit JAMAIS partir chez Sentry.
DONNEES_CLIENTS = ["client.fr", "Jean Dupont", "06 12 34", "mot-de-passe-secret", "jeton-de-verification",
                   "CLE-SERPAPI", "SecretSecret"]


@pytest.fixture(scope="module")
def envois() -> list[dict]:
    r = subprocess.run([sys.executable, "-c", SCENARIO, str(WEB_ROOT), JETON_TELEGRAM], capture_output=True,
                       text=True, encoding="utf-8", errors="replace", timeout=120)
    ligne = next((l for l in r.stdout.splitlines() if l.startswith("ENVOIS=")), None)
    assert ligne, r.stdout[-2000:] + r.stderr[-2000:]
    return json.loads(ligne[len("ENVOIS="):])


def _erreurs(envois: list[dict]) -> dict[str, dict]:
    return {e["exception"]["values"][-1]["type"]: e for e in envois if e.get("exception")}


def test_AUCUNE_donnee_de_client_ne_part(envois) -> None:
    tout = json.dumps(envois, ensure_ascii=False)
    assert envois, "le scénario n'a rien produit : le test passerait pour rien"
    fuites = [d for d in DONNEES_CLIENTS if d in tout]
    assert not fuites, fuites


def test_ce_qui_sert_a_corriger_part_TOUJOURS(envois) -> None:
    erreurs = _erreurs(envois)
    assert set(erreurs) >= {"ValueError", "RuntimeError"}
    valeur = erreurs["ValueError"]["exception"]["values"][-1]
    assert valeur["value"] == "échec de la correction pour [e-mail]"
    cadres = valeur["stacktrace"]["frames"]
    assert any(c.get("function") == "corriger" for c in cadres), "la pile reste lisible"
    assert not any(c.get("vars") for c in cadres), "sans les variables locales"


def test_la_requete_garde_son_CHEMIN_et_perd_formulaire_et_parametres(envois) -> None:
    requete = _erreurs(envois)["RuntimeError"]["request"]
    assert requete["url"] == "http://testserver/auth/signup" and requete.get("method") == "POST"
    assert not {"data", "query_string", "cookies"} & set(requete)


def test_le_fil_garde_les_appels_sans_parametres_ni_jeton(envois) -> None:
    fil = _erreurs(envois)["ValueError"]["breadcrumbs"]["values"]
    journal = next(b for b in fil if b.get("category") == "seo_agent")
    assert journal["message"] == "[MAIL] envoi impossible pour [e-mail]"
    appel = next(b for b in fil if b.get("category") == "httplib")
    assert appel["data"] == {"url": "https://api.telegram.org/bot[jeton]/sendMessage", "method": "GET"}


@pytest.mark.parametrize("texte, attendu", [
    ("Écrire à contact@noyaru.com", "Écrire à [e-mail]"),
    ("https://noyaru.com/auth/verify?token=abc&next=%2F", "https://noyaru.com/auth/verify"),
    ("bot123456789:AAH5yQkzSecretSecretSecretSecret0123/getUpdates", "bot[jeton]/getUpdates"),
    ("rien à masquer ici", "rien à masquer ici"),
])
def test_le_nettoyage(texte, attendu) -> None:
    assert sentry_filtre.nettoyer({"a": [texte]}) == {"a": [attendu]}


def test_le_service_initialise_Sentry_AVEC_le_filtre(monkeypatch) -> None:
    import sentry_sdk

    recu: dict = {}
    monkeypatch.setenv("SENTRY_DSN", "https://cle@o1.ingest.de.sentry.io/2")
    monkeypatch.setattr(m, "_SENTRY_READY", False)
    monkeypatch.setattr(sentry_sdk, "init", lambda **kw: recu.update(kw))
    m._init_sentry()
    assert recu["dsn"] and {cle: recu.get(cle) for cle in sentry_filtre.options()} == sentry_filtre.options()
