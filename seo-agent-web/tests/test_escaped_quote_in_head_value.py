# -*- coding: utf-8 -*-
r"""Un litteral se termine au guillemet NON ECHAPPE — et en francais l'apostrophe est partout.

Mesure du 14/09/2026, next-app. La ligne

    description: 'Page du parcours d\'obstacles : <110 caracteres au total>'

etait lue « Page du parcours d\ », 19 caracteres, parce que le motif s'arretait au premier
guillemet venu. Deux consequences, la seconde bien pire que la premiere :

  * toute mesure de longueur portait sur un fragment — un texte de 110 caracteres passait pour
    en faire 19, donc « trop court » ;
  * le LITTERAL rendu pour un remplacement etait tronque de la meme facon, si bien qu'ecrire
    par-dessus laissait la fin de la phrase en vrac dans le fichier. C'est ce qui a produit, sur
    le banc, des descriptions de 185 caracteres faites de deux morceaux colles.

Ce relecteur sert au reecriveur des familles de LONGUEUR, celles qui tournent chez les clients.
`l'entreprise`, `d'obstacles`, `aujourd'hui` : le defaut visait d'abord les pages francaises.

L'antislash appartient au fichier, pas a la page : la longueur RENDUE ne le compte donc pas.
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

APOSTROPHE = chr(39)
ANTISLASH = chr(92)
TEXTE = ("Page du parcours d" + ANTISLASH + APOSTROPHE + "obstacles : un texte assez long "
         "pour depasser largement le plancher de cent caracteres sans aucun probleme.")
PAGE = ("export const metadata = {\n  description: " + APOSTROPHE + TEXTE + APOSTROPHE
        + ",\n};\n")


def test_la_valeur_lue_va_jusqu_au_bout():
    litteral, valeur = m._find_head_text_value(PAGE, "description")
    assert valeur == TEXTE
    assert litteral.endswith(APOSTROPHE)


def test_le_litteral_couvre_toute_la_phrase():
    """C'est lui qu'on remplace : tronque, il laisse la fin du texte en vrac dans le fichier."""
    litteral, _ = m._find_head_text_value(PAGE, "description")
    assert TEXTE in litteral
    assert PAGE.replace(litteral, "description: 'Autre'").count("obstacles") == 0


def test_l_antislash_ne_compte_pas_dans_la_longueur_rendue():
    _, valeur = m._find_head_text_value(PAGE, "description")
    assert m._rendered_len(valeur) == len(valeur) - 1


def test_une_valeur_sans_echappement_est_inchangee():
    page = "export const metadata = {\n  description: 'Un texte tout simple',\n};\n"
    assert m._find_head_text_value(page, "description")[1] == "Un texte tout simple"


def test_un_guillemet_double_echappe_se_lit_pareil():
    guillemet = chr(34)
    texte = "Il a dit " + ANTISLASH + guillemet + "bonjour" + ANTISLASH + guillemet + " hier"
    page = "export const metadata = {\n  title: " + guillemet + texte + guillemet + ",\n};\n"
    assert m._find_head_text_value(page, "title")[1] == texte


def test_les_entites_html_restent_resolues():
    """Le comportement d'origine ne bouge pas : `&amp;` vaut un caractere."""
    assert m._rendered_len("Marque &amp; Fils") == len("Marque & Fils")


def test_la_valeur_ne_deborde_pas_sur_la_ligne_suivante():
    page = ("export const metadata = {\n  description: 'Premiere',\n  title: 'Seconde',\n};\n")
    assert m._find_head_text_value(page, "description")[1] == "Premiere"
