# -*- coding: utf-8 -*-
"""Le pack « Noyaru — logo & favicons » (28/09/2026) est branché partout, et rien ne pointe dans le vide.

Une icône déclarée mais absente ne casse rien de visible : le navigateur garde l'ancienne, ou
n'en affiche aucune, sans erreur. Ces tests lisent donc les DÉCLARATIONS et vérifient chaque
fichier qu'elles nomment, avec ses vraies dimensions.
"""
from __future__ import annotations

import json
import os
import re
import sys
import tempfile
from pathlib import Path

from PIL import Image

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-icones-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as m  # noqa: E402

STATIC = WEB_ROOT / "static"
TEMPLATES = WEB_ROOT / "templates"
FAVICONS = (TEMPLATES / "_favicons.html").read_text(encoding="utf-8")
MANIFESTE = json.loads((STATIC / "site.webmanifest").read_text(encoding="utf-8"))


def _fichier(url: str) -> Path:
    assert url.startswith("/static/"), url
    return STATIC / url.split("?", 1)[0][len("/static/"):]


def _declares() -> list[str]:
    """Toutes les adresses /static/ citees par les declarations et le logo."""
    textes = [FAVICONS] + [(TEMPLATES / t).read_text(encoding="utf-8")
                           for t in ("layout.html", "public_layout.html")]
    return sorted({u for t in textes for u in re.findall(r'(?:href|src|srcset)="(/static/[^"]+)"', t)})


def test_chaque_fichier_DECLARE_existe() -> None:
    manquants = [u for u in _declares() + [i["src"] for i in MANIFESTE["icons"]] if not _fichier(u).exists()]
    assert manquants == [], manquants


def test_les_tailles_annoncees_sont_les_VRAIES() -> None:
    for u, taille in re.findall(r'href="(/static/[^"]+\.png[^"]*)" sizes="(\d+x\d+)"', FAVICONS):
        assert "%dx%d" % Image.open(_fichier(u)).size == taille, u
    for icone in MANIFESTE["icons"]:
        assert "%dx%d" % Image.open(_fichier(icone["src"])).size == icone["sizes"], icone["src"]


def test_le_manifeste_est_celui_de_NOYARU() -> None:
    assert MANIFESTE["name"] == "Noyaru" and MANIFESTE["theme_color"] == "#111113"
    assert {i["purpose"] for i in MANIFESTE["icons"]} == {"any", "maskable"}


def test_plus_AUCUNE_trace_de_l_ancienne_icone() -> None:
    """Un favicon SVG declare serait PREFERE aux PNG par le navigateur : l'ancienne loupe
    survivrait au nouveau logo."""
    assert "svg" not in FAVICONS.lower().split("#}", 1)[-1]
    for ancien in ("favicon.svg", "android-chrome-192x192.png", "android-chrome-512x512.png"):
        assert not (STATIC / ancien).exists(), ancien


def test_les_adresses_RACINE_menent_aux_nouveaux_fichiers_dans_la_meme_version() -> None:
    client = TestClient(m.app)
    for racine in ("/favicon.ico", "/apple-touch-icon.png", "/site.webmanifest"):
        r = client.get(racine, follow_redirects=False)
        cible = r.headers["location"]
        assert r.status_code == 308 and _fichier(cible).exists(), (racine, cible)
        assert cible.endswith("?v=" + m._VERSION_ICONES), cible
        assert cible in FAVICONS or racine == "/favicon.ico", "version divergente : " + cible
    assert "favicon.ico?v=%s" % m._VERSION_ICONES in FAVICONS


def test_le_logo_remplace_la_loupe_et_SEO_Agent_dans_la_barre_laterale() -> None:
    layout = (TEMPLATES / "layout.html").read_text(encoding="utf-8")
    assert 'src="/static/brand/noyaru-logo.png?v=3"' in layout and 'alt="Noyaru"' in layout
    assert ">SEO Agent</a>" not in layout and "brand-logo" not in layout


def test_les_pages_PUBLIQUES_affichent_le_logo_et_les_nouvelles_icones() -> None:
    client = TestClient(m.app)
    for chemin in ("/pricing", "/docs"):
        page = client.get(chemin).text
        assert 'class="pub-nav-brand"' in page and "/static/brand/noyaru-logo.png?v=3" in page, chemin
        assert "/static/favicon.ico?v=3" in page and "favicon.svg" not in page, chemin
