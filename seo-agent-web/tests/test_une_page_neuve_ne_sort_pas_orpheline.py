# -*- coding: utf-8 -*-
"""Refuser une page parce qu'on ne sait pas lire l'index, c'est faire payer notre limite.

MESURE DU 26/09/2026, premier essai sur un vrai site client. L'index `content/fr/guides.mdx`
cite la page soeur TROIS fois : dans un parcours recommande numerote, dans un encart
« demarrer », dans une rubrique thematique. La regle d'alors refusait — « pas UNE forme a
cloner mais trois » — et la page sortait ORPHELINE.

Le raisonnement etait sain (choisir au hasard casserait une des formes) mais la conclusion
etait mauvaise : une page orpheline ne se voit pas dans un build vert, et le client n'a pas a
perdre sa page parce que son index est editorialise.

LE DISCRIMINANT N'EST PAS EDITORIAL, IL EST COMPTABLE. Parmi ces trois listes, une seule
ENUMERE la section : celle qui cite le plus de pages voisines distinctes. Les autres
mentionnent la soeur en passant. Ajouter une page au catalogue de sa propre section n'est pas
une prise de position — c'est l'en omettre qui en serait une.
"""
from __future__ import annotations

import re

import pytest

from backend import app as m


# L'index reel, reduit a sa structure. Les trois citations de `comment-choisir-une-plateforme`
# sont a la ligne 4 (parcours numerote, 5 voisines), 11 (encart, 2 voisines) et 15 (rubrique
# thematique, 1 voisine).
INDEX_REEL = """## Parcours recommande (debutant)

1. [Debuter l'investissement](/fr/guides/debuter-investissement/) — objectifs, horizon.
2. [Choisir une plateforme](/fr/guides/comment-choisir-une-plateforme/) — frais, retraits.
3. [Lire un graphique](/fr/guides/comment-lire-un-graphique/) — tendance, zones.
4. [Outils d'analyse technique](/fr/guides/meilleurs-outils-analyse-technique/) — workflow.
5. Option crypto : [DCA crypto](/fr/guides/dca-crypto/) — regles, frais.

## Demarrer

- [Debuter l'investissement](/fr/guides/debuter-investissement/) — la base.
- [Choisir une plateforme](/fr/guides/comment-choisir-une-plateforme/) — moins de surprises.

## Choisir sa plateforme

- [Choisir une plateforme](/fr/guides/comment-choisir-une-plateforme/) — *frais, retraits*.
"""


def _poser(index: str, section: str = "/fr/guides") -> tuple[str, str]:
    return m.ajouter_le_lien(
        index, "content/fr/guides.mdx",
        soeur_slug="comment-choisir-une-plateforme",
        slug_neuf="comment-calculer-les-interets-composes",
        titre_soeur="Choisir une plateforme",
        titre_neuf="Calculer les interets composes",
        section=section)


def test_l_index_reel_qui_rendait_la_page_orpheline() -> None:
    """Le cas mesure. Trois citations, un lien pose, aucune orpheline."""
    sortie, refus = _poser(INDEX_REEL)
    assert not refus, refus
    assert "comment-calculer-les-interets-composes" in sortie


def test_le_lien_va_dans_le_CATALOGUE_pas_dans_une_mention() -> None:
    """La liste qui enumere le plus de pages de la section, pas celle qui cite en passant."""
    sortie, _ = _poser(INDEX_REEL)
    lignes = sortie.split("\n")
    i = next(k for k, l in enumerate(lignes)
             if "comment-calculer-les-interets-composes" in l)
    # La ligne posee doit voisiner les quatre autres guides du parcours.
    bloc = "\n".join(lignes[max(0, i - 6):i + 6])
    for voisine in ("debuter-investissement", "comment-lire-un-graphique",
                    "meilleurs-outils-analyse-technique", "dca-crypto"):
        assert voisine in bloc, "posee hors du catalogue : %s absente du voisinage" % voisine


def test_la_liste_numerotee_garde_ses_numeros() -> None:
    """Cloner `2.` produirait un second « 2. ». Un diff qu'on n'ose pas lire est un diff
    qu'on approuve sans le lire."""
    # MA PREMIERE VERSION NE MESURAIT RIEN : son extraction des numeros etait cassee, et la
    # mutation qui supprime la renumerotation y a SURVECU. Une regex ancree, et rien d'autre.
    sortie, _ = _poser(INDEX_REEL)
    numeros = [int(t.group(1)) for t in
               (re.match(r"^\s*(\d+)\.\s", l) for l in sortie.split("\n")) if t]
    assert len(numeros) == 6, "le parcours doit compter six entrees : %s" % numeros
    assert numeros == list(range(1, 7)), numeros


def test_les_mentions_de_contexte_ne_sont_PAS_touchees() -> None:
    """On ajoute au catalogue, on ne recopie pas la page dans chaque paragraphe qui la cite."""
    sortie, _ = _poser(INDEX_REEL)
    assert sortie.count("comment-calculer-les-interets-composes") == 1, (
        "le lien a ete pose %d fois" % sortie.count("comment-calculer-les-interets-composes"))


def test_une_seule_citation_garde_le_comportement_d_avant() -> None:
    """Le chemin simple n'est pas touche : une citation, un clone, pas de comptage."""
    index = ("## Guides\n\n"
             "- [Choisir une plateforme](/fr/guides/comment-choisir-une-plateforme/) — frais.\n"
             "- [Lire un graphique](/fr/guides/comment-lire-un-graphique/) — tendance.\n")
    sortie, refus = _poser(index)
    assert not refus, refus
    assert sortie.count("comment-calculer-les-interets-composes") == 1


def test_sans_aucune_liste_qui_enumere_on_refuse_encore() -> None:
    """LE TEMOIN DE L'AUTRE BORD. Sans lui, « poser n'importe ou » passerait tous les tests.

    Ici la soeur n'est citee que dans des phrases — aucune entree de liste, donc aucune forme
    a cloner. On refuse, et c'est juste : inventer une entree de liste dans un paragraphe
    abimerait la page du client.
    """
    # DEUX LIENS DE SECTION DANS LA MEME PHRASE. Ma premiere version n'en mettait qu'un par
    # ligne : le garde-fou « une liste a une page n'est pas un catalogue » suffisait alors a
    # refuser, et la mutation qui supprime le controle de marqueur y a SURVECU. Il faut une
    # phrase qui, si on la prenait pour une liste, SERAIT un catalogue.
    index = ("## Guides\n\n"
             "Commencez par [choisir une plateforme]"
             "(/fr/guides/comment-choisir-une-plateforme/) puis "
             "[lire un graphique](/fr/guides/comment-lire-un-graphique/).\n\n"
             "Relisez ensuite [le meme guide]"
             "(/fr/guides/comment-choisir-une-plateforme/) et "
             "[les outils](/fr/guides/meilleurs-outils-analyse-technique/).\n")
    sortie, refus = _poser(index)
    assert not sortie
    assert "enumere" in refus, refus


def test_une_liste_qui_ne_cite_qu_UNE_page_n_est_pas_un_catalogue() -> None:
    """Deux mentions, chacune seule dans sa liste : rien n'enumere la section."""
    index = ("## A\n\n- [Choisir](/fr/guides/comment-choisir-une-plateforme/) — a.\n\n"
             "## B\n\n- [Choisir](/fr/guides/comment-choisir-une-plateforme/) — b.\n")
    sortie, refus = _poser(index)
    assert not sortie and "enumere" in refus, (sortie[:80], refus)


def test_un_menu_global_ne_passe_pas_pour_le_catalogue_de_la_section() -> None:
    """La section sert a ca. Un menu riche en liens HORS section ne compte pas.

    Sans le filtre de section, le menu — 4 liens, contre 2 dans la vraie liste — gagnerait le
    comptage et l'entree irait dans la navigation du site.
    """
    index = ("- [Accueil](/fr/) — a\n"
             "- [Brokers](/fr/brokers/etoro/) — b\n"
             "- [Crypto](/fr/crypto/bitpanda/) — c\n"
             "- [Choisir](/fr/guides/comment-choisir-une-plateforme/) — menu\n"
             "\n## Guides\n\n"
             "- [Choisir](/fr/guides/comment-choisir-une-plateforme/) — catalogue\n"
             "- [Lire un graphique](/fr/guides/comment-lire-un-graphique/) — catalogue\n")
    sortie, refus = _poser(index)
    assert not refus, refus
    lignes = sortie.split("\n")
    i = next(k for k, l in enumerate(lignes) if "interets-composes" in l)
    # LE REPERE NE PEUT PAS ETRE LE RESUME : il est retire au clonage. Ma premiere version
    # cherchait le mot « catalogue » dans la ligne posee, c'est-a-dire dans la zone que le
    # correctif supprime. On regarde donc le VOISINAGE, qui ne bouge pas.
    voisinage = "\n".join(lignes[max(0, i - 2):i + 3])
    assert "comment-lire-un-graphique" in voisinage, "posee hors du catalogue : %r" % voisinage
    assert "/fr/brokers/" not in voisinage, "posee dans le menu : %r" % voisinage


@pytest.mark.parametrize("marqueur", ["-", "*", "+", "1."])
def test_les_marqueurs_de_liste_usuels_sont_reconnus(marqueur: str) -> None:
    index = ("## Guides\n\n"
             "%s [Choisir](/fr/guides/comment-choisir-une-plateforme/) — a\n"
             "%s [Lire](/fr/guides/comment-lire-un-graphique/) — b\n" % (marqueur, marqueur))
    sortie, refus = _poser(index)
    assert not refus, (marqueur, refus)
    assert sortie.count("interets-composes") == 1


def test_l_index_modifie_repasse_le_controle_de_format() -> None:
    """La garantie qui existait deja doit tenir apres le nouveau chemin."""
    sortie, _ = _poser(INDEX_REEL)
    assert m._refus_de_format("content/fr/guides.mdx", sortie) is None


# --- le resume de la soeur ne decrit pas la page neuve ------------------------------------------
# Trouve en jouant le correctif sur l'index REEL du client : le lien etait juste, la phrase
# fausse. « Calculer les interets composes — produit (actif vs CFD), frais, retraits » se lit
# parfaitement bien, et c'est exactement pour ca qu'un relecteur peut le laisser passer.

def test_le_resume_de_la_soeur_n_est_pas_recopie() -> None:
    sortie, _ = _poser(INDEX_REEL)
    ligne = next(l for l in sortie.split("\n") if "interets-composes" in l)
    assert "frais, retraits" not in ligne, ligne


def test_le_lien_lui_meme_survit_au_retrait() -> None:
    """On enleve la prose, pas l'entree. Sans ce temoin, « tout effacer » passerait au-dessus."""
    sortie, _ = _poser(INDEX_REEL)
    ligne = next(l for l in sortie.split("\n") if "interets-composes" in l)
    assert "[Calculer les interets composes]" in ligne, ligne
    assert "(/fr/guides/comment-calculer-les-interets-composes/)" in ligne, ligne


@pytest.mark.parametrize("separateur", ["—", "–", "-", ":"])
def test_les_quatre_separateurs_de_resume_sont_reconnus(separateur: str) -> None:
    entree = "- [Titre](/fr/guides/a/) %s le resume de la soeur." % separateur
    assert m._sans_le_resume_de_la_soeur(entree) == "- [Titre](/fr/guides/a/)"


@pytest.mark.parametrize("entree", [
    '<li><a href="/blog/x">Titre</a></li>',
    '  { slug: "mon-article", titre: "Mon article" },',
    "- [Sans resume](/fr/guides/e/)",
])
def test_ce_qui_suit_le_lien_et_n_est_PAS_de_la_prose_reste_intact(entree: str) -> None:
    """`</li>`, une virgule d'objet, une balise fermante : de la STRUCTURE. L'enlever
    casserait l'entree — l'echange d'un defaut lisible contre un defaut muet."""
    assert m._sans_le_resume_de_la_soeur(entree) == entree
