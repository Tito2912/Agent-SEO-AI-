# -*- coding: utf-8 -*-
"""Ou creer une page qui n'existe pas encore, dans le depot d'un client qu'on ne connait pas.

PREMIERE ETAPE DE LA CREATION DE CONTENU, et delibrement la seule : aucun modele, aucune
ecriture GitHub, aucune generation. Juste la question la plus risquee du chantier — *ou va le
fichier, et que faut-il lier* — repondue par une fonction pure qu'on peut eprouver stack par
stack.

LA METHODE : TRANSPOSER UNE SOEUR, pas deduire de la stack. `repo_index` sait lire sept
conventions fichier -> route ; les inverser reviendrait a les reecrire a l'envers et a les
tenir a jour deux fois. On prend donc une page EXISTANTE de la meme section et on y remplace
son slug. C'est la methode que ce projet applique partout ailleurs — `_inserer_og_complet`
recopie les champs de la mise en page plutot que d'inventer un bloc, `_complete_open_graph`
clone la ligne que le modele vient d'ecrire : **decrire une grammaire bat decrire une forme**.

Elle a une propriete qu'aucune reconstruction n'aurait : **elle refuse toute seule** quand il
n'y a pas de soeur — le tout premier article d'un blog, ou la convention du site n'existe pas
encore. Inventer une arborescence chez un client est pire que de lui dire qu'on ne sait pas.

CE QUE CETTE ETAPE NE TRANCHE PAS. Savoir s'il faut modifier l'index de section demande de le
LIRE : un gabarit Hugo qui parcourt son dossier n'a besoin de rien, une liste ecrite a la main
si. Cette fonction ne lit aucun fichier — elle DESIGNE le candidat, l'etape suivante decidera.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

from backend import repo_index as ri  # noqa: E402

NEXT = [
    "package.json", "next.config.js",
    "app/layout.tsx", "app/page.tsx",
    "app/blog/page.tsx",
    "app/blog/premier-article/page.tsx",
    "app/blog/second-article/page.tsx",
    "app/contact/page.tsx",
]

HUGO = [
    "hugo.toml", "layouts/_default/single.html", "layouts/_default/list.html",
    "content/_index.md",
    "content/blog/_index.md",
    "content/blog/premier-article.md",
    "content/blog/second-article.md",
]

JEKYLL = [
    "_config.yml", "_layouts/default.html",
    "index.html",
    "_posts/2026-01-15-premier-article.md",
    "_posts/2026-02-20-second-article.md",
]


# --- ou va le fichier -------------------------------------------------------------------------

def test_next_transpose_le_dossier_de_la_soeur() -> None:
    p = ri.placement_pour_route(NEXT, "/blog/mon-sujet")
    assert p["refus"] == "", p["refus"]
    assert p["fichier"] == "app/blog/mon-sujet/page.tsx"
    assert p["section"] == "/blog"
    assert p["index"] == "app/blog/page.tsx"


def test_hugo_transpose_le_nom_de_fichier() -> None:
    p = ri.placement_pour_route(HUGO, "/blog/mon-sujet")
    assert p["refus"] == "", p["refus"]
    assert p["fichier"] == "content/blog/mon-sujet.md"
    assert p["index"] == "content/blog/_index.md"


def test_la_soeur_choisie_sert_de_MODELE_et_est_nommee() -> None:
    """L'etape suivante clonera la structure d'une soeur : il faut savoir laquelle."""
    p = ri.placement_pour_route(HUGO, "/blog/mon-sujet")
    assert "content/blog/premier-article.md" in p["soeurs"]


def test_la_stack_est_rendue_pour_l_etape_suivante() -> None:
    assert ri.placement_pour_route(NEXT, "/blog/x")["stack"] == ri.STACK_NEXT_APP
    assert ri.placement_pour_route(HUGO, "/blog/x")["stack"] == ri.STACK_HUGO


# --- le cas Jekyll, ou la date est dans le nom -------------------------------------------------

def test_jekyll_date_l_article_AUJOURD_HUI_et_pas_comme_son_ainee() -> None:
    """Jekyll met la date dans le NOM DE FICHIER et la route en decoule. Transposer tel quel
    publierait l'article neuf a la date de son ainee — dans le passe, a la mauvaise URL."""
    p = ri.placement_pour_route(JEKYLL, "/2026/09/20/mon-sujet", date="2026-09-20")
    assert p["refus"] == "", p["refus"]
    assert p["fichier"] == "_posts/2026-09-20-mon-sujet.md"


def test_jekyll_la_date_ne_touche_QUE_le_nom_de_fichier() -> None:
    """Un `_posts/` a sous-dossiers : remplacer la date sur la mauvaise moitié du chemin
    déplacerait l'article. Ma première fixture n'avait qu'un seul niveau, donc la mutation
    qui inversait la découpe du chemin ne changeait rien et a survécu."""
    imbrique = ["_config.yml", "_layouts/default.html", "index.html",
                "_posts/tech/2026-01-15-premier-article.md"]
    p = ri.placement_pour_route(imbrique, "/2026/09/20/mon-sujet", date="2026-09-20")
    assert p["fichier"] == "_posts/tech/2026-09-20-mon-sujet.md", p


def test_jekyll_sans_date_fournie_garde_le_nom_transpose() -> None:
    """Sans date, on ne fabrique pas : le chemin reste celui de la transposition brute, et
    l'appelant verra que la date n'est pas celle du jour."""
    p = ri.placement_pour_route(JEKYLL, "/2026/09/20/mon-sujet")
    assert p["fichier"] == "_posts/2026-01-15-mon-sujet.md"


# --- ce qu'on REFUSE de deviner -----------------------------------------------------------------

def test_sans_page_soeur_on_S_ABSTIENT() -> None:
    """Le tout premier article d'un blog : la convention du site n'existe pas encore.

    C'est le cas que j'avais annonce au proprietaire avant d'ecrire une ligne. Inventer
    `content/blog/x.md` sur un site qui n'a pas de dossier `blog` creerait une arborescence
    chez un client sur une supposition.
    """
    # MA PREMIÈRE VERSION UTILISAIT `NEXT`, qui a déjà deux articles : le test demandait donc
    # une abstention sur un site qui a justement la convention recherchée, et il accusait le
    # code de bien faire son travail. Une fixture doit porter le cas qu'on annonce.
    sans_blog = ["package.json", "next.config.js", "app/layout.tsx", "app/page.tsx",
                 "app/contact/page.tsx"]
    p = ri.placement_pour_route(sans_blog, "/blog/premier-de-tous")
    assert p["fichier"] == ""
    assert "aucune page soeur" in p["refus"], p["refus"]


def test_une_page_qui_EXISTE_DEJA_est_refusee() -> None:
    """Le refus doit NOMMER le fichier occupant, pas seulement dire « existe déjà ».

    Une mutation qui supprimait ce contrôle a survécu à ma première version : la transposition
    aboutissait au même fichier, que le garde-fou suivant rejetait avec un message contenant
    lui aussi « existe deja ». Deux refus corrects, mais j'en mesurais un pour l'autre.
    """
    p = ri.placement_pour_route(NEXT, "/blog/premier-article")
    assert p["fichier"] == ""
    assert "cette page existe deja" in p["refus"], p["refus"]
    assert "app/blog/premier-article/page.tsx" in p["refus"], p["refus"]


def test_une_route_NON_NORMALISEE_est_reconnue_comme_existante() -> None:
    """La route arrive d'un crawl, d'un formulaire ou d'un sujet de concurrent. Sans
    normalisation, `/blog/premier-article/` ne correspond à rien dans la carte et on
    écraserait une page existante."""
    p = ri.placement_pour_route(NEXT, "/blog/premier-article/")
    assert p["fichier"] == ""
    # Le message PRÉCIS, pas « existe deja » : le garde-fou de fin rejette lui aussi avec ces
    # deux mots, et l'assertion large mesurait donc l'un pour l'autre.
    assert "cette page existe deja" in p["refus"], p["refus"]


def test_la_racine_n_est_pas_une_page_a_creer() -> None:
    assert "racine" in ri.placement_pour_route(NEXT, "/")["refus"]


def test_un_depot_VIDE_ne_produit_aucun_chemin() -> None:
    p = ri.placement_pour_route([], "/blog/x")
    assert p["fichier"] == "" and p["refus"]


# --- les pieges de la transposition ---------------------------------------------------------------

def test_seules_les_soeurs_DIRECTES_servent_de_modele() -> None:
    """`/blog/2026/vieux` est une petite-fille, pas une soeur : sa convention est autre."""
    arbre = HUGO + ["content/blog/2026/archive.md"]
    p = ri.placement_pour_route(arbre, "/blog/mon-sujet")
    assert p["fichier"] == "content/blog/mon-sujet.md"
    assert all("2026" not in s for s in p["soeurs"]), p["soeurs"]


def test_la_forme_MAJORITAIRE_l_emporte_sur_une_page_bricolee() -> None:
    """Un site a dix articles reguliers et une page posee a la main : on suit les dix."""
    arbre = HUGO + ["content/blog/troisieme.md", "content/blog/quatrieme.md",
                    "static/blog/bricolee.html"]
    p = ri.placement_pour_route(arbre, "/blog/mon-sujet")
    assert p["fichier"] == "content/blog/mon-sujet.md", p


def test_un_slug_repete_dans_le_chemin_n_est_remplace_QU_UNE_FOIS() -> None:
    """`content/blog/blog.md` : remplacer partout renommerait le DOSSIER de tout le site."""
    arbre = ["hugo.toml", "layouts/_default/single.html",
             "content/blog/_index.md", "content/blog/blog.md"]
    p = ri.placement_pour_route(arbre, "/blog/mon-sujet")
    assert p["fichier"] == "content/blog/mon-sujet.md", p


def test_une_section_de_PREMIER_niveau_fonctionne_aussi() -> None:
    """Toutes les pages ne sont pas dans un blog : `/tarifs` est une soeur de `/contact`."""
    p = ri.placement_pour_route(NEXT, "/tarifs")
    assert p["refus"] == "", p["refus"]
    assert p["fichier"] == "app/tarifs/page.tsx"
    assert p["section"] == "/"


@pytest.mark.parametrize("route", ["/blog/mon-sujet/", "blog/mon-sujet",
                                   "https://site.fr/blog/mon-sujet?a=1#b"])
def test_la_route_est_normalisee_avant_tout(route) -> None:
    """Une URL arrive d'un crawl, d'un formulaire ou d'un sujet de concurrent : trois formes."""
    assert ri.placement_pour_route(HUGO, route)["fichier"] == "content/blog/mon-sujet.md"
