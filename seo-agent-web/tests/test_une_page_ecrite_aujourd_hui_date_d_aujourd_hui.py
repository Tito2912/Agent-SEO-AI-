# -*- coding: utf-8 -*-
"""Une valeur clonee, plausible, que personne ne recompte.

MESURE DU 27/09/2026, page livree EN PRODUCTION chez un client. Le front matter portait

    updatedAt: "2026-03-22"

soit, au caractere pres, la date de la page soeur dont la forme avait ete transposee. La page
etait creee le 27 septembre et s'annoncait vieille de six mois — « Mis a jour : 22/03/2026 »
s'affichait en tete de l'article servi.

MEME FAMILLE QUE LE RESUME RECOPIE, et meme raison de survivre : le build est vert, la page est
belle, et rien dans le diff ne crie. La difference avec le resume est qu'ICI ON DETIENT la bonne
valeur. On ne devine pas, on pose.

LA BORNE EST LE FRONT MATTER. Une date dans le corps peut etre une citation, un exemple chiffre,
un historique : la reecrire abimerait le texte du client.
"""
from __future__ import annotations

import pytest

from backend import app as m

JOUR = "2026-09-27"


def test_la_date_clonee_sur_la_soeur_est_remise_au_jour() -> None:
    """Le cas mesure, au caractere pres."""
    page = '---\ntitle: "Comment calculer"\nupdatedAt: "2026-03-22"\n---\n\nCorps.\n'
    sortie, notes = m._dater_du_jour(page, JOUR)
    assert 'updatedAt: "2026-09-27"' in sortie, sortie
    assert "2026-03-22" not in sortie, sortie
    assert notes and "2026-09-27" in notes[0], notes


def test_une_date_du_CORPS_n_est_pas_touchee() -> None:
    """Une date dans le texte peut etre une citation ou un exemple. La reecrire abimerait la
    page — on echangerait une date fausse en tete contre un contresens dans le texte."""
    page = ('---\ntitle: "T"\nupdatedAt: "2026-03-22"\n---\n\n'
            "Le krach du 2008-09-15 a change la reglementation.\n")
    sortie, _ = m._dater_du_jour(page, JOUR)
    assert "2008-09-15" in sortie, sortie
    assert 'updatedAt: "2026-09-27"' in sortie, sortie


def test_plusieurs_dates_de_tete_sont_toutes_remises() -> None:
    page = ('---\npublishedAt: "2026-01-02"\nupdatedAt: "2026-03-22"\n---\n\nCorps.\n')
    sortie, notes = m._dater_du_jour(page, JOUR)
    assert sortie.count("2026-09-27") == 2, sortie
    assert notes and notes[0].startswith("2 date"), notes


def test_le_front_matter_TOML_est_couvert_comme_le_YAML() -> None:
    """Hugo date ses pages entre `+++`. Ne couvrir que `---` laisserait une stack entiere."""
    page = '+++\ntitle = "T"\ndate = 2026-01-05\n+++\n\nCorps.\n'
    sortie, _ = m._dater_du_jour(page, JOUR)
    assert "date = 2026-09-27" in sortie, sortie


@pytest.mark.parametrize("page", [
    'export const metadata = { title: "T", date: "2026-03-22" };\n',
    '---\ntitle: "T"\n---\n\nCorps sans date.\n',
    "",
])
def test_ce_qui_n_a_pas_de_date_de_tete_est_rendu_tel_quel(page: str) -> None:
    """Une page JS n'a pas de bloc de tete : on ne va pas y chercher une date au jugé.
    C'est un trou assumé, pas un oubli — les dates de ces stacks vivent dans un objet, et
    les y réécrire demanderait d'en analyser la structure."""
    sortie, notes = m._dater_du_jour(page, JOUR)
    assert sortie == page and notes == [], (sortie, notes)


def test_une_page_deja_datee_du_jour_ne_produit_aucun_diff() -> None:
    """Sans ce temoin, « toujours reecrire » passerait les autres tests — et ferait d'une
    correction sans objet un fichier modifie, donc une unite facturee pour rien."""
    page = '---\ntitle: "T"\nupdatedAt: "2026-09-27"\n---\n\nCorps.\n'
    sortie, notes = m._dater_du_jour(page, JOUR)
    assert sortie == page and notes == [], (sortie, notes)


def test_le_redacteur_applique_la_garde_lui_meme(monkeypatch) -> None:
    """Comme les garde-fous d'adresse et d'antislash : dans `rediger_une_page`, pas chez
    l'appelant. Un garde-fou qu'il faut penser a appeler finit par ne pas l'etre."""
    soeur = '---\ntitle: "Soeur"\nupdatedAt: "2026-03-22"\n---\n\nCorps de la soeur.\n'
    ecrite = '---\ntitle: "Neuve"\nupdatedAt: "2026-03-22"\n---\n\nCorps neuf.\n'
    monkeypatch.setattr(m, "_correction_ai_json", lambda **kw: {"contenu": ecrite})
    notes: list[str] = []
    contenu, refus = m.rediger_une_page(
        sujet="Mon sujet", chemin="content/fr/guides/ma-page.mdx",
        soeur_chemin="content/fr/guides/soeur.mdx", soeur_contenu=soeur,
        url_de_la_page="https://site.fr/fr/guides/ma-page", notes=notes)
    assert refus == "", refus
    assert "2026-03-22" not in contenu, contenu
    assert any("date" in n for n in notes), notes
