# -*- coding: utf-8 -*-
"""Le DPA français et le DPA anglais doivent donner les mêmes droits, délais et exceptions (06/11/2026).

Une comparaison mécanique, pas une relecture : même plan, mêmes lignes de tableau, mêmes délais chiffrés, autant de
points à confirmer. Et les choix faits dans les clauses de la Commission : aucune option laissée en suspens, les
deux écarts annoncés dans la présentation et rappelés à leur place.
"""
from __future__ import annotations

import re
from collections import Counter
from pathlib import Path

import pytest

DOSSIER = Path(__file__).resolve().parents[1] / "docs" / "legal"
FR = (DOSSIER / "DPA-fr.md").read_text(encoding="utf-8")
EN = (DOSSIER / "DPA-en.md").read_text(encoding="utf-8")
DELAI = re.compile(r"(\d+)[\s -]*(jours?|days?|heures?|hours?|mois|months?)\b", re.I)
UNITES = {"jour": "j", "jours": "j", "day": "j", "days": "j", "heure": "h", "heures": "h", "hour": "h", "hours": "h",
          "mois": "m", "month": "m", "months": "m"}


def _delais(texte: str) -> Counter:
    return Counter((n, UNITES[u.lower()]) for n, u in DELAI.findall(texte))


def _plan(texte: str) -> list[int]:
    return [len(m.group(1)) for m in re.finditer(r"^(#{1,5}) ", texte, re.M)]


def test_meme_plan() -> None:
    assert _plan(FR) == _plan(EN)


def test_memes_lignes_de_tableau() -> None:
    assert FR.count("\n| ") == EN.count("\n| ")


def test_memes_delais_chiffres() -> None:
    assert _delais(FR) == _delais(EN)
    assert _delais(FR)[("30", "j")] >= 5 and _delais(FR)[("48", "h")] == 1


def test_autant_de_points_a_confirmer() -> None:
    assert FR.count("À CONFIRMER") == EN.count("TO BE CONFIRMED") >= 1
    assert FR.count("À METTRE EN ŒUVRE") == EN.count("TO BE IMPLEMENTED")


@pytest.mark.parametrize("texte", [FR, EN], ids=["fr", "en"])
def test_aucune_option_en_suspens_ni_gabarit(texte: str) -> None:
    for reste in ("OPTION 1", "OPTION 2", "[PRÉCISER", "[SPECIFY", "{{", "choisir l", "choose relevant"):
        assert reste not in texte


@pytest.mark.parametrize("texte, tiers, note_e, note_a", [
    (FR, "tiers bénéficiaire", "e) *Clause non reprise", "*Voir aussi l’écart 2"),
    (EN, "third party beneficiary", "(e) *Clause not adopted", "*See also deviation 2"),
], ids=["fr", "en"])
def test_les_deux_ecarts_sont_annonces_et_rappeles(texte: str, tiers: str, note_e: str, note_a: str) -> None:
    assert note_e in texte and note_a in texte
    # La clause 7.7 e) n'est décrite que dans le tableau des écarts : elle n'est pas promise en partie A.
    partie_a = re.search(r"\n## Part(?:ie)? A.*?\n## Annex", texte, re.S).group(0)
    assert tiers not in partie_a
