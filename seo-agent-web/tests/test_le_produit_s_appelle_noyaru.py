# -*- coding: utf-8 -*-
"""Le produit s'appelle Noyaru — partout où un client ou un tiers peut lire son nom.

Demande du propriétaire, 28/09/2026 : « le nom SEO Agent doit disparaître ». Il subsistait dans
les titres d'onglet, les en-têtes des pages de connexion, les e-mails, les corps et messages de
commit des pull requests ouvertes CHEZ les clients, un commentaire écrit dans leurs fichiers,
et les signatures envoyées aux sites tiers. Trois noms de repli coexistaient dans le code.

Le dossier `seo-agent-web` et les variables `SEO_AGENT_*` restent : ce sont des noms internes,
qu'aucun client ne lit, et les renommer casserait le déploiement.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend import app as m  # noqa: E402

RACINE = Path(__file__).resolve().parents[1]
# L'ancien nom sous toutes ses graphies — mais pas `seo-agent-web` ni `SEO_AGENT_` (internes).
# « SEO Audit » aussi : un autre nom d'avant, reste dans dix titres d'onglet et le rapport PDF
# apres le premier renommage (vu le 29/09/2026 sur la page Abonnement).
ANCIEN = re.compile(r"SEO[ \-]?Agent(?!-web)(?!_)|SEOAgent|SEO Audit", re.I)


def _lignes_fautives(fichier: Path) -> list[str]:
    fautives = []
    for n, ligne in enumerate(fichier.read_text(encoding="utf-8").splitlines(), 1):
        if ANCIEN.search(ligne.replace("seo-agent-web", "").replace("SEO_AGENT_", "")):
            fautives.append("%s:%d: %s" % (fichier.name, n, ligne.strip()[:100]))
    return fautives


def test_aucun_gabarit_ne_porte_l_ancien_nom() -> None:
    fautives = []
    for f in sorted((RACINE / "templates").glob("*.html")):
        # Un commentaire Jinja ({# … #}) n'est jamais rendu : il peut raconter l'histoire.
        texte = re.sub(r"\{#.*?#\}", "", f.read_text(encoding="utf-8"), flags=re.S)
        fautives += ["%s: %s" % (f.name, l.strip()[:100]) for l in texte.splitlines()
                     if ANCIEN.search(l.replace("seo-agent-web", "").replace("SEO_AGENT_", ""))]
    assert not fautives, fautives


def test_le_serveur_n_ecrit_l_ancien_nom_nulle_part() -> None:
    """E-mails, PR, commits, fichiers poses chez le client, signatures envoyees aux tiers."""
    assert not _lignes_fautives(RACINE / "backend" / "app.py")


def test_le_nom_de_repli_est_noyaru(monkeypatch) -> None:
    monkeypatch.delenv("APP_NAME", raising=False)
    assert m._app_name() == "Noyaru"
    assert m.app.title == "Noyaru"


def test_les_pages_de_connexion_montrent_le_LOGO(monkeypatch) -> None:
    from fastapi.testclient import TestClient
    page = TestClient(m.app).get("/auth/login").text
    assert "/static/brand/noyaru-logo.png" in page and "<title>Connexion - Noyaru</title>" in page
    assert "styles.css?v=" in page, "une feuille non versionnee reste en cache apres un changement"
