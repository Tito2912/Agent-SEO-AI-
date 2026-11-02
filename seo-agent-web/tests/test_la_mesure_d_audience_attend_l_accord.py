# -*- coding: utf-8 -*-
"""Google Analytics sur le site public, seulement après « Accepter ».

Décisions du propriétaire, 02/11/2026 : bandeau Accepter / Refuser (refuser aussi simple
qu'accepter, CNIL), site public seulement (pas les écrans du compte, dont les adresses citent les
sites des clients), et les promesses visibles réécrites pour dire exactement ce que fait GA4.
Ce que ces tests défendent : sans ID, rien ne change et la promesse « aucun cookie de mesure »
reste vraie ; avec ID, rien de Google ne part avant l'accord ; la CSP n'ouvre Google que sur les
pages publiques ; la politique de confidentialité nomme Google Analytics et ses cookies.
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
os.environ.setdefault("SEO_AGENT_DATA_DIR", tempfile.mkdtemp(prefix="seo-agent-ga4-"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", tempfile.mkdtemp(prefix="seo-agent-ga4-runs-"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from bs4 import BeautifulSoup  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from backend import app as m  # noqa: E402
from backend import auth  # noqa: E402
from backend.models import User  # noqa: E402

ID = "G-TEST1234"


@pytest.fixture()
def ga4(monkeypatch):
    monkeypatch.setenv("GA4_MEASUREMENT_ID", ID)
    monkeypatch.setenv("SEO_AGENT_CSP_ENABLED", "true")
    monkeypatch.delenv("SEO_AGENT_CSP", raising=False)


@pytest.fixture()
def sans_ga4(monkeypatch):
    monkeypatch.delenv("GA4_MEASUREMENT_ID", raising=False)
    monkeypatch.setenv("SEO_AGENT_CSP_ENABLED", "true")
    monkeypatch.delenv("SEO_AGENT_CSP", raising=False)


def _csp(reponse) -> str:
    return reponse.headers.get("content-security-policy") or reponse.headers.get(
        "content-security-policy-report-only") or ""


# ── Sans ID : rien ne change ─────────────────────────────────────────────────────────────────

def test_SANS_ID_le_site_garde_sa_promesse(sans_ga4) -> None:
    client = TestClient(m.app)
    accueil = client.get("/")
    assert 'id="cookie-banner"' in accueil.text and 'id="consentement-mesure"' not in accueil.text
    assert "googletagmanager" not in accueil.text and "googletagmanager" not in _csp(accueil)
    assert "Aucun cookie publicitaire ni de mesure d'audience" in client.get("/privacy").text


# ── Avec ID : l'accord d'abord ───────────────────────────────────────────────────────────────

def test_le_bandeau_propose_REFUSER_aussi_simplement_qu_ACCEPTER(ga4) -> None:
    page = TestClient(m.app).get("/")
    soupe = BeautifulSoup(page.text, "html.parser")
    bandeau = soupe.select_one("#consentement-mesure")
    assert bandeau is not None and not bandeau.has_attr("hidden"), "sans choix, le bandeau est visible"
    boutons = bandeau.select("button[data-consentement]")
    assert sorted(b["data-consentement"] for b in boutons) == ["accepte", "refuse"]
    assert len({(b.name, tuple(sorted(b.get("class") or []))) for b in boutons}) == 1, "même poids"
    assert soupe.select_one("#cookie-banner") is None, "un seul bandeau"


def test_RIEN_de_Google_n_est_charge_avant_l_accord(ga4) -> None:
    """Le HTML ne cite Google que dans le script local, qui attend « Accepter »."""
    page = TestClient(m.app).get("/").text
    assert "googletagmanager" not in page
    assert re.search(r'<script src="/static/mesure-audience\.js\?v=\d+" defer></script>', page)
    js = (WEB_ROOT / "static" / "mesure-audience.js").read_text(encoding="utf-8")
    charger = js.split("function charger()")[1].split("\n  function ")[0]
    assert js.count("googletagmanager") == 1 and "googletagmanager" in charger
    assert "if (choix() === 'accepte') charger();" in js
    assert "cookie_expires: TREIZE_MOIS" in js and "395 * 24 * 3600" in js


def test_un_choix_deja_fait_cache_le_bandeau(ga4) -> None:
    for choix in ("accepte", "refuse"):
        client = TestClient(m.app)
        client.cookies.set("noyaru_consentement", choix)
        bandeau = BeautifulSoup(client.get("/").text, "html.parser").select_one("#consentement-mesure")
        assert bandeau.has_attr("hidden"), choix


def test_le_pied_de_page_permet_de_CHANGER_d_avis(ga4) -> None:
    lien = BeautifulSoup(TestClient(m.app).get("/").text, "html.parser").select_one("footer [data-gerer-cookies]")
    assert lien is not None and lien.get_text(strip=True) == "Gérer les cookies"


def test_un_ID_mal_forme_n_active_rien(monkeypatch) -> None:
    monkeypatch.setenv("GA4_MEASUREMENT_ID", "UA-1234-5")
    assert 'id="consentement-mesure"' not in TestClient(m.app).get("/").text
    assert m._validate_settings_env_value("GA4_MEASUREMENT_ID", "UA-1234-5")
    assert m._validate_settings_env_value("GA4_MEASUREMENT_ID", ID) is None


# ── La CSP : Google sur les pages publiques, et elles seules ─────────────────────────────────

@pytest.mark.parametrize("chemin", ["/", "/pricing", "/privacy", "/auth/signup", "/auth/login"])
def test_une_page_PUBLIQUE_autorise_Google(ga4, chemin) -> None:
    reponse = TestClient(m.app).get(chemin)
    politique = _csp(reponse)
    script = next(d for d in politique.split(";") if d.strip().startswith("script-src"))
    connect = next(d for d in politique.split(";") if d.strip().startswith("connect-src"))
    assert "https://www.googletagmanager.com" in script, chemin
    assert "https://*.google-analytics.com" in connect, chemin
    assert 'id="consentement-mesure"' in reponse.text, chemin


def test_un_ecran_du_COMPTE_n_autorise_jamais_Google(ga4) -> None:
    m.DB.create_tables()
    with m.DB.session() as db:
        u = User(email="ga4-%s@exemple.fr" % uuid.uuid4().hex[:8], password_hash="x", is_admin=False)
        db.add(u)
        db.commit()
        db.refresh(u)
        uid = str(u.id)
    client = TestClient(m.app)
    client.cookies.set(auth.SESSION_COOKIE_NAME,
                       auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    client.cookies.set("noyaru_consentement", "accepte")
    for chemin in ("/settings/accounts", "/billing"):
        reponse = client.get(chemin)
        assert reponse.status_code == 200, chemin
        assert "googletagmanager" not in _csp(reponse) and "consentement-mesure" not in reponse.text, chemin


# ── Ce que disent les pages juridiques ───────────────────────────────────────────────────────

@pytest.mark.parametrize("chemin, mots", [
    ("/privacy", ["Google (Analytics)", "_ga, _ga_…", "noyaru_consentement", "13 mois",
                  "seulement si vous l'acceptez", "Mesurer l'audience du site public"]),
    ("/en/privacy", ["Google (Analytics)", "_ga, _ga_…", "noyaru_consentement", "13 months",
                     "only if you accept it", "Measuring the audience of the public website"]),
])
def test_la_confidentialite_DIT_ce_que_fait_GA4(ga4, chemin, mots) -> None:
    texte = TestClient(m.app).get(chemin).text.replace("&#39;", "'")
    for mot in mots:
        assert mot in texte, (chemin, mot)
    assert "Aucun cookie publicitaire ni de mesure d'audience" not in texte
    assert "No advertising or audience measurement cookie" not in texte
