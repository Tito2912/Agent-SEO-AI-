# -*- coding: utf-8 -*-
"""Un sitemap qui propose aux moteurs une page qui n'existe pas.

Quatrieme famille reprise dans la liste d'attente. Elle a ete choisie sur une MESURE, pas sur
une impression : des 100 familles montrees au client sans correcteur, NEUF seulement se
declenchent sur le parcours des neuf stacks, et celle-ci etait la seule a la fois vue et
mecaniquement reparable. Les huit autres sont soit editoriales (`links_with_no_anchor_text`,
`page_has_only_one_dofollow_incoming_internal_link`), soit des proprietes de l'hebergeur
(`http_to_https_redirect`, `redirect_chain`), soit volontaires (`noindex_page`).

Le geste est exactement celui de `sitemap_noindex_page`, sa soeur : retirer l'entree. Le sitemap
est ce qu'on SOUMET aux moteurs — une URL qui repond 4xx y est une promesse vide, tout comme une
page qui porte noindex. Les deux clefs partagent donc le meme reecriveur, et seul le motif
enonce au modele change.

Ce qu'on NE fait pas, et la consigne le dit : creer la page manquante. C'est l'autre moitie du
probleme, et elle n'est pas mecanique.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "x" * 20)

from backend import app as m  # noqa: E402

ABSENTE = "https://x.fr/page-absente"
SITEMAP = ('<?xml version="1.0" encoding="UTF-8"?>\n<urlset>\n'
           "  <url><loc>%s</loc></url>\n"
           "  <url><loc>https://x.fr/bien-vivante</loc></url>\n</urlset>\n" % ABSENTE)


def _prep(cle: str = "sitemap_4xx_page") -> dict:
    return m._prepare_issue_fix(
        issue_key=cle, issues={cle: {"count": 1, "examples": [ABSENTE]}}, impacted=[ABSENTE],
        all_paths=["public/sitemap.xml"], site_name="x.fr", owner="o", repo_name="r",
        branch="main", token="t", model_override="", pages=[])


def test_l_entree_fautive_est_retiree():
    rendu, n = _prep()["link_rewriter"](SITEMAP)
    assert n == 1
    assert ABSENTE not in rendu


def test_les_entrees_saines_restent():
    rendu, _ = _prep()["link_rewriter"](SITEMAP)
    assert "https://x.fr/bien-vivante" in rendu
    assert rendu.count("<url>") == 1


def test_le_document_reste_un_sitemap_valide():
    rendu, _ = _prep()["link_rewriter"](SITEMAP)
    assert rendu.lstrip().startswith("<?xml") and rendu.rstrip().endswith("</urlset>")


def test_la_famille_est_declaree_corrigeable_et_proposee():
    assert "sitemap_4xx_page" in m._handled_issue_keys()
    assert m._github_issue_auto_fixable("sitemap_4xx_page")


def test_la_consigne_enonce_le_bon_motif():
    """Le geste est partage avec `sitemap_noindex_page` ; la RAISON ne l'est pas."""
    assert "ERREUR (4xx)" in _prep()["extra_hint"]
    assert "noindex" not in _prep()["extra_hint"].split("Retire EXACTEMENT")[0]


def test_la_soeur_garde_son_propre_motif():
    assert "noindex" in _prep("sitemap_noindex_page")["extra_hint"]


def test_la_consigne_interdit_de_CREER_la_page_manquante():
    """C'est l'autre moitie du probleme, et elle n'est pas mecanique."""
    assert "CREER" in _prep()["extra_hint"]


def test_la_cle_vise_le_fichier_du_sitemap():
    groupes = [nom for nom, cles, _ in m._issue_file_families() if "sitemap_4xx_page" in cles]
    assert groupes == ["sitemap"]
