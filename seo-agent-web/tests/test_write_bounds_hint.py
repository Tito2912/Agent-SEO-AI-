# -*- coding: utf-8 -*-
"""Reparer une anomalie en en creant une autre n'est pas reparer.

Mesure du 13/09/2026, next-app, cycle complet des neuf stacks puis recrawl des previews :
`duplicate_meta_descriptions` est tombee de 25 a 0 — un vrai succes — mais SEPT pages sont
ressorties avec une description TROP COURTE, 83 a 96 caracteres pour un plancher a 100. Le
modele avait pour seule consigne de rendre ces descriptions uniques. Personne ne lui avait dit
jusqu'ou : les bornes n'etaient enoncees qu'aux familles de LONGUEUR, pas a celles qui font
ECRIRE une valeur neuve.

Les nombres viennent des memes tables que ces familles-la, elles-memes alignees sur les seuils du
crawler. Une seule source — la lecon de la divergence de comparaison d'URL du meme jour.
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


def test_la_consigne_porte_le_plancher_ET_le_plafond():
    indice = m._build_write_bounds_hint("description")
    assert str(m._LENGTH_FLOORS["description"]) in indice
    assert str(m._LENGTH_CEILINGS["description"]) in indice
    bas, haut = m._LENGTH_WINDOWS["description"]
    assert f"{bas} et {haut}" in indice


def test_le_titre_a_ses_propres_bornes():
    indice = m._build_write_bounds_hint("title")
    assert str(m._LENGTH_CEILINGS["title"]) in indice
    assert "titre" in indice


def test_les_nombres_ne_sont_pas_recopies_a_la_main():
    """Si une table bouge, la consigne bouge — sinon les deux moities du produit divergent."""
    ancien = m._LENGTH_FLOORS["description"]
    m._LENGTH_FLOORS["description"] = 123
    try:
        assert "123" in m._build_write_bounds_hint("description")
    finally:
        m._LENGTH_FLOORS["description"] = ancien


def test_les_familles_qui_ecrivent_une_valeur_recoivent_la_consigne():
    for cle in ("duplicate_meta_descriptions", "duplicate_titles"):
        prep = m._prepare_issue_fix(
            issue_key=cle, issues={}, impacted=["https://x.fr/a"], all_paths=["index.html"],
            site_name="x.fr", owner="o", repo_name="r", branch="main", token="t",
            model_override="", pages=[])
        assert "caracteres" in prep["extra_hint"], cle


def test_une_famille_de_longueur_garde_sa_propre_consigne():
    """Elle a deja les bornes, avec en plus le RENDU par page. On ne la double pas."""
    prep = m._prepare_issue_fix(
        issue_key="meta_description_too_long", issues={}, impacted=["https://x.fr/a"],
        all_paths=["index.html"], site_name="x.fr", owner="o", repo_name="r", branch="main",
        token="t", model_override="", pages=[])
    assert "Une valeur unique mais trop" not in (prep["extra_hint"] or "")


def test_les_bornes_atteignent_le_MESSAGE_envoye_au_modele():
    """Ce qui depend de nous s'arrete ici, et se verifie sans depenser un centime.

    Une consigne peut etre construite correctement et ne jamais quitter le processus : elle
    traverse `_prepare_issue_fix` -> `extra_hint` -> `occurrences_hint` -> le champ `contexte`
    du message. Ce test suit la chaine jusqu'au bout. Reste hors de portee d'un test, et
    seulement cela : savoir si le modele OBEIT.
    """
    prep = m._prepare_issue_fix(
        issue_key="duplicate_meta_descriptions", issues={}, impacted=["https://x.fr/a"],
        all_paths=["index.html"], site_name="x.fr", owner="o", repo_name="r", branch="main",
        token="t", model_override="", pages=[])
    message = m._patch_user_msg(
        "index.html", "<html><head></head></html>", "duplicate_meta_descriptions",
        "Descriptions dupliquees", "https://x.fr/a", "x.fr", prep["extra_hint"])
    assert str(m._LENGTH_FLOORS["description"]) in message
    assert str(m._LENGTH_CEILINGS["description"]) in message
