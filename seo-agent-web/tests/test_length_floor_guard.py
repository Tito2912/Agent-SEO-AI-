# -*- coding: utf-8 -*-
"""Trop long se coupe ; trop court ne se rallonge pas — on peut seulement refuser d'aggraver.

Mesure du 14/09/2026, next-app, famille `duplicate_meta_descriptions` jouee seule sur le banc.
Sur six descriptions reecrites, une sortait a 74 caracteres pour un plancher a 100, alors que
la valeur d'origine en faisait 104. L'anomalie visee etait reparee et une autre creee a sa
place : exactement le troc que ce banc existe pour interdire.

Aucun garde-fou ne peut inventer du texte. Celui-ci rend donc la valeur d'origine, ce qui ne
ruine pas la correction des doublons — il suffit qu'UNE page du groupe change pour que le
doublon cesse.
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

LONGUE = ("Deux pages qui portent exactement la meme meta description, afin de declencher "
          "la famille des doublons du parcours.")
COURTE = "Page du parcours : elle sert a tester le correcteur."


def _page(valeur: str) -> str:
    return "export const metadata = {\n  description: '%s',\n};\n" % valeur


def test_le_cas_mesure_sur_next_app():
    assert len(LONGUE) >= m._LENGTH_FLOORS["description"]
    assert len(COURTE) < m._LENGTH_FLOORS["description"]
    rendu, notes = m._keep_length_above_floor(_page(COURTE), _page(LONGUE))
    assert LONGUE in rendu and COURTE not in rendu
    assert len(notes) == 1 and "plancher" in notes[0]


def test_une_valeur_conforme_passe():
    autre = LONGUE.replace("Deux pages", "Cette page unique")
    apres = _page(autre)
    assert m._keep_length_above_floor(apres, _page(LONGUE)) == (apres, [])


def test_une_valeur_deja_courte_avant_peut_etre_rallongee_partiellement():
    """Famille « trop courte » : un progres qui n'atteint pas encore le seuil reste un progres."""
    avant = _page("Trop court.")
    apres = _page(COURTE)
    assert m._keep_length_above_floor(apres, avant) == (apres, [])


def test_le_titre_a_son_propre_plancher():
    avant = "<title>Un titre parfaitement convenable pour cette page</title>"
    apres = "<title>Court</title>"
    rendu, notes = m._keep_length_above_floor(apres, avant)
    assert "Un titre parfaitement convenable" in rendu and len(notes) == 1


def test_une_valeur_absente_ne_declenche_rien():
    assert m._keep_length_above_floor("<html></html>", _page(LONGUE)) == ("<html></html>", [])


def test_la_longueur_comptee_est_celle_du_RENDU():
    """`&amp;` compte pour un caractere a l'ecran, pour cinq dans le fichier."""
    avant = _page(LONGUE)
    # Rendu : 98 lettres + l'esperluette = 99 caracteres, sous le plancher. Ecrit : 103.
    sous_le_seuil = "a" * (m._LENGTH_FLOORS["description"] - 2) + "&amp;"
    assert len(sous_le_seuil) > m._LENGTH_FLOORS["description"], "un comptage naif le croirait bon"
    rendu, notes = m._keep_length_above_floor(_page(sous_le_seuil), avant)
    assert notes, "c'est la longueur RENDUE qui decide, pas celle du fichier"
    assert LONGUE in rendu


def test_il_passe_APRES_le_reparateur_d_echappement(monkeypatch):
    """L'ordre de la chaine fait tout, et ce controle etait mal place.

    Mesure du 14/09/2026 : le modele ecrit `'Page du parcours d'obstacles : ...'`, apostrophe
    NON echappee. Le litteral s'arrete alors pour de bon a 18 caracteres — le relecteur a
    raison. Place AVANT `_escape_quotes_in_written_values`, ce controle y voyait une valeur sous
    le plancher, remplacait ce fragment par l'ancienne valeur complete, et laissait la fin de la
    phrase pendante : des descriptions de 238 caracteres faites de deux morceaux colles.

    Le test verifie l'ordre par son effet : sur un contenu deja echappe — ce que le controle
    recoit desormais — la valeur est lue entiere et rien n'est touche.
    """
    apostrophe, antislash = chr(39), chr(92)
    long_texte = ("Page du parcours d" + antislash + apostrophe + "obstacles : un texte "
                  "assez long pour depasser le plancher de cent caracteres sans probleme.")
    echappe = "export const metadata = {\n  description: '%s',\n};\n" % long_texte
    assert m._rendered_len(m._find_head_text_value(echappe, "description")[1]) >= 100
    assert m._keep_length_above_floor(echappe, _page(LONGUE)) == (echappe, [])


def test_il_s_efface_quand_l_ancienne_valeur_EST_l_anomalie():
    """Doublons : restituer le texte partage remettrait exactement ce qu'on vient de corriger.

    Mesure du 14/09/2026 : `duplicate-a` et `duplicate-b` repartaient toutes deux avec leur
    description commune d'origine — plus aucune trop courte, et le doublon intact. Un remede qui
    remet la maladie n'en est pas un : ces familles obtiennent une relance, pas une restitution.
    """
    apres = _page(COURTE)
    assert m._keep_length_above_floor(apres, _page(LONGUE),
                                      valeur_ancienne_fautive=True) == (apres, [])
