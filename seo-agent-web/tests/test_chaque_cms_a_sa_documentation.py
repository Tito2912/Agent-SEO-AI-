"""Chaque CMS connecté a sa page de documentation, et les pages publiques nomment chaque plateforme.

Étape 8 des intégrations CMS (03/11/2026). Une connexion livrée sans page laisse le client
deviner les prérequis (version, droits de la clé) ; une plateforme absente de la page des CMS
compatibles ou de la carte de l'accueil n'existe pas pour celui qui se demande si son site l'est.
Et un CMS connecté ne doit plus figurer parmi les plateformes « guidées » de l'accueil."""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from backend import cms_guidance, cms_routes, help_navigation
from backend import content_library as cl

TEMPLATES = Path(__file__).resolve().parents[1] / "templates"
SANS_CMS = {"", "code", "other"}
PLATEFORMES = sorted(label for cle, label in cms_guidance.PLATFORMS.items() if cle not in SANS_CMS)
CONNECTES = sorted(cx.libelle for cx in cms_routes.CONNECTEURS.values())


@pytest.mark.parametrize("lang", ["fr", "en"])
@pytest.mark.parametrize("nom", sorted(cms_routes.CONNECTEURS))
def test_chaque_cms_connecte_a_sa_page_reliee_a_l_ecran_cms(nom, lang):
    page = cl.get_doc("corriger-dans-%s" % nom, lang)
    assert page is not None and page["lang"] == lang
    assert help_navigation._DOC_SCREENS[page["slug"]] == "/cms"
    assert 'href="%s"' % page["path"] in cl.get_doc("cms-compatibles", lang)["body_html"]


@pytest.mark.parametrize("lang", ["fr", "en"])
def test_le_tableau_des_cms_compatibles_nomme_chaque_plateforme(lang):
    """Le tableau, pas la page : c'est lui que le client parcourt pour trouver son outil."""
    corps = cl.get_doc("cms-compatibles", lang)["body_html"]
    tableau = corps[corps.index("<table"):corps.index("</table>")]
    assert PLATEFORMES and [label for label in PLATEFORMES if label not in tableau] == []


def _carte_compatibilite():
    accueil = (TEMPLATES / "home_public.html").read_text(encoding="utf-8")
    carte = accueil[accueil.index('id="compatibilite"'):]
    return carte[:carte.index('<div class="stacks">')]


def test_la_carte_de_l_accueil_nomme_chaque_plateforme_et_mene_aux_cms_compatibles():
    carte = _carte_compatibilite()
    assert "'/docs/cms-compatibles'" in carte
    assert [label for label in PLATEFORMES if label not in carte] == []


def test_un_cms_connecte_n_est_pas_annonce_comme_guide_sur_l_accueil():
    phrases = [p for t in re.findall(r'tr\("([^"]+)"', _carte_compatibilite()) for p in re.split(r"(?<=\.) ", t)]
    guidees = [p for p in phrases if "instructions pas à pas" in p]
    assert len(guidees) == 1, "la phrase des plateformes guidées est introuvable"
    assert [label for label in CONNECTES if label in guidees[0]] == []
