# -*- coding: utf-8 -*-
"""Une aiguille qui figure partout ne renseigne rien.

Le 11/09/2026, `image_redirects` rendait « aucun patch » sur les NEUF stacks alors que son
reecriveur fonctionnait parfaitement. Cause : `_evidence_needles` produit deux aiguilles par
terme — le chemin `/og.png` et le nom nu `og.png` — et le nom nu de l'image Open Graph figure
sur TOUTES les pages du site. La recherche remplissait donc son plafond de fichiers avec des
pages quelconques, et celle qui charge l'image qui redirige n'y entrait jamais. Les familles CSS
et JavaScript y echappaient par pur hasard : `style.css` est plus rare que `og.png`.

Le classement par rarete corrige cela sans rien deviner : il COMPTE.
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


def test_le_fichier_trouve_par_l_aiguille_rare_passe_devant():
    """Le cas reel : `og.png` sur toutes les pages, `/img/ancienne.png` sur une seule."""
    trouves = ["a-propos.html", "blog.html", "gauntlet/duplicate-a.html",
               "gauntlet/redirected-image.html"]
    hits = {
        "a-propos.html": ["og.png"],
        "blog.html": ["og.png"],
        "gauntlet/duplicate-a.html": ["og.png"],
        "gauntlet/redirected-image.html": ["/img/ancienne.png", "ancienne.png"],
    }
    assert m._rank_by_needle_rarity(trouves, hits)[0] == "gauntlet/redirected-image.html"


def test_a_rarete_egale_l_ordre_de_decouverte_est_conserve():
    """Le classement ne doit pas reordonner ce qu'il ne sait pas departager."""
    trouves = ["un.html", "deux.html", "trois.html"]
    hits = {p: ["commun"] for p in trouves}
    assert m._rank_by_needle_rarity(trouves, hits) == trouves


def test_un_fichier_sans_aiguille_connue_part_en_dernier():
    """Les trouvailles « approchantes » (un <img> dans le bon dossier) ne doivent pas doubler
    une correspondance exacte."""
    trouves = ["approchant.html", "exact.html"]
    hits = {"exact.html": ["/img/ancienne.png"]}
    assert m._rank_by_needle_rarity(trouves, hits) == ["exact.html", "approchant.html"]


def test_sans_aucune_correspondance_la_liste_est_rendue_telle_quelle():
    trouves = ["un.html", "deux.html"]
    assert m._rank_by_needle_rarity(trouves, {}) == trouves
