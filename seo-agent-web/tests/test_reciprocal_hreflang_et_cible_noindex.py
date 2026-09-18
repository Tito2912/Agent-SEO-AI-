# -*- coding: utf-8 -*-
"""Une cible NOINDEX absente du sitemap n'est pas une balise de retour manquante.

Sur-report trouve le 18/09/2026 en recrawlant les sites de reference apres un changement sans
rapport : creativeai-tools.com montrait `missing_reciprocal_hreflang` = 1 la ou Ahrefs affiche 0.

La cause (A) de cette famille dit qu'une page DANS le sitemap qui designe un alternate HORS
sitemap a une balise de retour cassee au niveau du sitemap. C'est vrai, et valide : elevenlabs en
tire ses 8, qu'Ahrefs compte aussi. Mais l'appartenance au sitemap ne suffit pas a trancher, et
deux sites de reference portent la MEME forme avec des verdicts Ahrefs opposes :

    elevenlabs  /mentions-legales   -> /de|/es/legal-notice       `index, follow`    Ahrefs 8
    creativeai  /blog/category/...  -> /de|/es|/fr/blog/...       `noindex, follow`  Ahrefs 0

Ce qui les separe est l'INDEXABILITE de la cible. Une page noindex n'a pas sa place dans un
sitemap : exiger qu'elle y soit reviendrait a demander la creation de `sitemap_noindex_page`, une
anomalie qu'Ahrefs classe en ERREUR. On echangerait un signalement contre un autre, plus grave.

CE QUE LE CORRECTEUR FAISAIT DE CE SUR-REPORT. La famille est corrigee : elle aurait ouvert une
pull request sur le site d'un client pour une anomalie qu'Ahrefs ne voit pas. Un sur-report sur
une famille consultative se discute ; sur une famille qui ECRIT, il modifie un site sans raison.

Une cible jamais crawlee continue d'etre signalee : on ne sait pas ce qu'elle vaut, et c'est
justement le cas ou une balise de retour manque vraiment.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_SRC = (Path(__file__).resolve().parents[2]
        / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py")
_spec = importlib.util.spec_from_file_location("seo_audit_recip_noindex", _SRC)
assert _spec and _spec.loader
audit = importlib.util.module_from_spec(_spec)
sys.modules["seo_audit_recip_noindex"] = audit
_spec.loader.exec_module(audit)

S = "https://exemple.fr"
CLE = "missing_reciprocal_hreflang"


def _page(chemin: str, alternates: dict[str, str], *, noindex: bool = False) -> object:
    p = audit.PageData(url=S + chemin)
    p.final_url = S + chemin
    p.status_code = 200
    p.content_type = "text/html; charset=utf-8"
    p.canonical = S + chemin
    p.hreflang = dict(alternates)
    p.lang = "fr"
    if noindex:
        p.meta_robots = "noindex, follow"
    return p


# La page source est au sitemap ; ses trois alternates n'y sont pas. Seule leur INDEXABILITE
# changera d'un cas a l'autre.
GROUPE = {"en": S + "/cat", "de": S + "/de/cat", "fr": S + "/fr/cat"}
SOURCE = _page("/cat", GROUPE)
SITEMAP = [S + "/cat"]


def _compte(pages: list[object]) -> int:
    return audit._score_issues(pages, base_url=S, sitemap_urls=list(SITEMAP))[CLE]["count"]


def test_une_cible_NOINDEX_hors_sitemap_ne_declenche_rien() -> None:
    """Le cas creativeai-tools : Ahrefs 0, et Noyaru doit dire 0."""
    cibles = [_page("/de/cat", GROUPE, noindex=True), _page("/fr/cat", GROUPE, noindex=True)]
    assert _compte([SOURCE, *cibles]) == 0


def test_une_cible_INDEXABLE_hors_sitemap_reste_signalee() -> None:
    """Le cas elevenlabs : Ahrefs 8. Corriger creativeai ne doit pas l'eteindre."""
    cibles = [_page("/de/cat", GROUPE), _page("/fr/cat", GROUPE)]
    assert _compte([SOURCE, *cibles]) > 0


def test_une_cible_JAMAIS_CRAWLEE_reste_signalee() -> None:
    """Sans mesure, on ne presume pas qu'elle est noindex : c'est le cas le plus souvent fautif."""
    assert _compte([SOURCE]) > 0


def test_le_discriminant_est_bien_l_indexabilite_et_rien_d_autre() -> None:
    """Meme groupe, meme sitemap, meme forme : seul `noindex` change, et le verdict bascule.

    Ecrit ainsi parce que les deux sites reels ne different QUE par la : tout autre discriminant
    qu'on aurait choisi (la langue, le chemin, le nombre d'alternates) aurait separe les deux cas
    par accident, et se serait tu au premier site qui ne lui ressemble pas.
    """
    indexables = [_page("/de/cat", GROUPE), _page("/fr/cat", GROUPE)]
    noindex = [_page("/de/cat", GROUPE, noindex=True), _page("/fr/cat", GROUPE, noindex=True)]
    assert _compte([SOURCE, *indexables]) > 0
    assert _compte([SOURCE, *noindex]) == 0
