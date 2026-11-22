# -*- coding: utf-8 -*-
"""Ce qu'on envoie a un tiers sans contrat de traitement reçu (analyse du 22/11/2026).

Trois sorties, trois gardes :
  * SerpApi (recherches Google, Etats-Unis) : une adresse e-mail TAPEE dans une recherche est refusee
    avant tout appel, et les deux champs de saisie avertissent le client ;
  * PageSpeed (Google) : l'adresse d'une page part SANS identifiants, ancre, secrets ni adresse e-mail,
    et RIEN d'autre n'est touche : la page mesuree doit rester la meme ;
  * Telegram : l'adresse masquee est testee avec les notifications (test_les_notifications_telegram).
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import uuid
from pathlib import Path

import pytest
import requests

WEB_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = WEB_ROOT.parent
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-minimisation-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as app_module  # noqa: E402
from backend import auth, billing  # noqa: E402
from backend.app import app  # noqa: E402
from backend.models import Project, User  # noqa: E402

_SPEC = importlib.util.spec_from_file_location(
    "seo_audit_for_minimisation_tests",
    REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py",
)
assert _SPEC and _SPEC.loader
seo_audit = importlib.util.module_from_spec(_SPEC)
sys.modules["seo_audit_for_minimisation_tests"] = seo_audit
_SPEC.loader.exec_module(seo_audit)

AVERTISSEMENT = ("Ne saisissez ni nom, ni adresse e-mail, ni autre donnée personnelle : la recherche est "
                 "transmise à notre prestataire de recherche Google (SerpApi, États-Unis).")


# ── SerpApi : les routes ─────────────────────────────────────────────────────────────────────

def _client_solo() -> tuple[TestClient, str]:
    app_module.DB.create_tables()
    tag = uuid.uuid4().hex[:8]
    with app_module.DB.session() as db:
        user = User(email="min-%s@exemple.fr" % tag, password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(user)
        db.commit()
        db.refresh(user)
        uid = str(user.id)
        proj = Project(owner_user_id=uid, slug="site-%s" % tag, site_name="site.fr", base_url="https://site.fr/")
        db.add(proj)
        db.commit()
        db.refresh(proj)
        slug = proj.slug
    client = TestClient(app)
    client.cookies.set(auth.SESSION_COOKIE_NAME,
                       auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    return client, slug


def _poster(client: TestClient, chemin: str, corps: dict):
    client.get("/")
    jeton = client.cookies.get(app_module._CSRF_COOKIE_NAME, "") or ""
    return client.post(chemin, json=corps, headers={app_module._CSRF_HEADER_NAME: jeton}, follow_redirects=False)


@pytest.fixture()
def acces_solo(monkeypatch):
    """Le plan et le quota sont accordes ici : ce fichier mesure ce qui PART, pas la facturation."""
    monkeypatch.setattr(app_module, "_opp_has_access", lambda db, *, user_id: True)
    monkeypatch.setattr(billing, "ensure_within_quota", lambda *a, **k: (True, 99))
    monkeypatch.delenv("SERPAPI_API_KEY", raising=False)
    monkeypatch.delenv("SERPAPI_KEY", raising=False)

    def _aucun_appel(*_a, **_k):
        pytest.fail("une recherche avec une adresse e-mail ne doit JAMAIS partir")

    monkeypatch.setattr(app_module.requests, "get", _aucun_appel)


@pytest.mark.parametrize("source", ["google", "reddit"])
@pytest.mark.parametrize("requete", ["jean.dupont@exemple.fr", "avis de marie@societe.com sur ce logiciel"])
def test_une_adresse_e_mail_n_est_PAS_envoyee_a_SerpApi(acces_solo, source, requete) -> None:
    client, slug = _client_solo()
    rep = _poster(client, "/api/projects/%s/backlinks/search" % slug, {"query": requete, "source": source})
    assert rep.status_code == 400, rep.text[:200]
    assert "adresse e-mail" in rep.json()["error"]


def test_une_recherche_ordinaire_passe_la_garde(acces_solo) -> None:
    """Sans clé SerpApi la route répond 500 ; ce qui compte ici : le refus n'est pas celui de l'adresse."""
    client, slug = _client_solo()
    rep = _poster(client, "/api/projects/%s/backlinks/search" % slug,
                  {"query": "meilleur outil SEO gratuit", "source": "google"})
    assert rep.status_code == 500 and "SERPAPI_API_KEY" in rep.json()["error"], rep.text[:200]


def test_les_mots_cles_automatiques_refusent_une_adresse_e_mail(acces_solo) -> None:
    client, slug = _client_solo()
    chemin = "/api/projects/%s/backlinks/auto-settings" % slug
    refuse = _poster(client, chemin, {"enabled": True, "keywords": "meilleur outil SEO\nmarie@societe.com"})
    assert refuse.status_code == 400 and "adresse e-mail" in refuse.json()["error"], refuse.text[:200]
    accepte = _poster(client, chemin, {"enabled": False, "keywords": "meilleur outil SEO\nalternative à Ahrefs"})
    assert accepte.status_code == 200, accepte.text[:200]


# ── SerpApi : les deux champs avertissent ────────────────────────────────────────────────────

def test_les_deux_champs_de_saisie_avertissent_en_francais_et_en_anglais() -> None:
    page = (WEB_ROOT / "templates" / "backlinks_opportunities.html").read_text(encoding="utf-8")
    assert page.count(AVERTISSEMENT) == 2, "la recherche manuelle ET les mots-clés automatiques"
    en = json.loads((WEB_ROOT / "locales" / "en.json").read_text(encoding="utf-8"))
    assert "email address" in en[AVERTISSEMENT] and "SerpApi" in en[AVERTISSEMENT]


# ── PageSpeed ────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("avant, apres", [
    ("https://ex.fr/a?utm_source=x&token=abc&keyword=chaussures", "https://ex.fr/a?utm_source=x&keyword=chaussures"),
    ("https://jean:secret@ex.fr/p", "https://ex.fr/p"),
    ("https://ex.fr/p#section", "https://ex.fr/p"),
    ("https://ex.fr/p?contact=jean%40ex.fr&page=2", "https://ex.fr/p?page=2"),
    ("https://ex.fr/p?contact=jean@ex.fr&page=2", "https://ex.fr/p?page=2"),
    ("https://ex.fr/p?api_key=1&sessionId=2&monkey=3", "https://ex.fr/p?monkey=3"),
    ("https://ex.fr/p?e-mail=a&Token=b", "https://ex.fr/p"),
    ("https://ex.fr/p?access_token=zz", "https://ex.fr/p"),
])
def test_l_adresse_envoyee_a_PageSpeed_perd_ce_qui_identifie(avant, apres) -> None:
    assert seo_audit._pagespeed_url_expurgee(avant) == apres


@pytest.mark.parametrize("url", [
    "https://ex.fr", "https://ex.fr/", "https://ex.fr/?", "https://ex.fr/p?a=1&b=%20x&c=%C3%A9",
    "https://ex.fr/produits?page=2&sort=prix&keyword=sac+a+dos", "https://ex.fr:8443/p?lang=fr",
])
def test_une_adresse_sans_rien_a_retirer_part_TELLE_QUELLE(url) -> None:
    assert seo_audit._pagespeed_url_expurgee(url) == url


def test_PageSpeed_recoit_l_adresse_expurgee(monkeypatch) -> None:
    vus: dict = {}

    class _Session:
        def get(self, _url, params, timeout):
            vus.update(params)
            raise requests.RequestException("on s'arrête ici : seul ce qui PART compte")

    monkeypatch.setattr(seo_audit, "_get_pagespeed_session", lambda _ua: _Session())
    seo_audit._pagespeed_fetch("https://ex.fr/p?token=abc&page=2#haut", api_key="cle", strategy="mobile",
                               timeout_s=1, user_agent="test", attempts=1)
    assert vus["url"] == "https://ex.fr/p?page=2"
    assert "abc" not in json.dumps(vus, default=str).replace("cle", "")
