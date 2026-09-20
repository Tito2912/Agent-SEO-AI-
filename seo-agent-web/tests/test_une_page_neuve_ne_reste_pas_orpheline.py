# -*- coding: utf-8 -*-
"""Lier la page neuve, ou savoir qu'elle se liera toute seule.

LA CONTRAINTE QUI STRUCTURE TOUT LE CHANTIER, posee par le proprietaire : *« il faudrait que
l'agent soit capable de faire en sorte que la page soit coherente avec le site et linkee pour
ne pas etre orpheline »*. Publier une page que rien ne pointe serait absurde — le crawler la
signalerait lui-meme au passage suivant.

L'ETAPE 1 AVAIT LAISSE CETTE QUESTION OUVERTE, et pour une raison : y repondre demande de LIRE
l'index de section, ce qu'une fonction sur des chemins ne peut pas faire. Le signal, une fois
le fichier en main, est direct :

    l'index CITE le slug d'une soeur   -> liste ecrite a la main -> il faut y ajouter la page
    l'index ne cite personne           -> liste engendree (gabarit Hugo, lecture du dossier)

ET ON CLONE ENCORE. Une entree de liste peut etre un `<li><a href>`, un objet dans un tableau,
une ligne de Markdown, une carte JSX. Les enumerer serait sans fin ; le client en a deja une
sous la main. On recopie la sienne en y remplacant le slug, puis **on verifie que l'index se
relit** avec `_refus_de_format` — le meme jeu que les deux chemins qui commitent deja. Une
entree etalee sur plusieurs lignes ne se clone pas en copiant une ligne, et c'est le controle
qui l'attrape plutot qu'une regle qui devinerait.

CE QUE `requis=False` NE PROUVE PAS. La liste peut vivre dans un fichier de donnees voisin
(`lib/posts.ts`) qu'on ne regarde pas ici. On traite donc ce cas comme « cet index-la n'a rien
a nous apprendre », pas comme une preuve d'absence — et une page orpheline se verrait de toute
facon au crawl suivant. C'est un filet assume pour cette etape, pas une garantie.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-lien-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402

INDEX_MAIN = (
    "export default function Blog() {\n"
    "  return (\n"
    "    <ul>\n"
    '      <li><a href="/blog/premier-article">Premier article</a></li>\n'
    '      <li><a href="/blog/second-article">Second article</a></li>\n'
    "    </ul>\n"
    "  );\n"
    "}\n"
)

INDEX_ENGENDRE = (
    "---\n"
    "title: Le blog\n"
    "---\n\n"
    "Tous nos articles ci-dessous.\n"
)


# --- faut-il lier ? -----------------------------------------------------------------------------

def test_un_index_qui_CITE_une_soeur_demande_un_ajout() -> None:
    etat = m.lien_a_poser(INDEX_MAIN, "premier-article")
    assert etat["requis"] is True
    assert "premier-article" in etat["fragment"]


def test_un_index_qui_ne_cite_personne_n_a_rien_a_apprendre() -> None:
    """Gabarit Hugo qui parcourt son dossier : créer le fichier suffit."""
    assert m.lien_a_poser(INDEX_ENGENDRE, "premier-article")["requis"] is False


def test_une_soeur_citee_DEUX_FOIS_rend_le_clonage_ambigu() -> None:
    """Une carte en haut de page ET une entrée de menu : deux formes, pas une. En choisir une
    au hasard en casserait l'autre — on refuse plutôt que de trancher à pile ou face."""
    deux = INDEX_MAIN.replace(
        "    </ul>",
        '      <nav><a href="/blog/premier-article">encore</a></nav>\n    </ul>')
    etat = m.lien_a_poser(deux, "premier-article")
    assert etat["requis"] is True and etat.get("ambigu"), etat


# --- poser le lien ------------------------------------------------------------------------------

def test_l_entree_est_CLONEE_sur_celle_de_la_soeur() -> None:
    sortie, refus = m.ajouter_le_lien(
        INDEX_MAIN, "app/blog/page.tsx", soeur_slug="premier-article",
        slug_neuf="mon-sujet", titre_soeur="Premier article", titre_neuf="Mon sujet")
    assert refus == "", refus
    assert '<li><a href="/blog/mon-sujet">Mon sujet</a></li>' in sortie


def test_l_entree_neuve_se_place_JUSTE_APRES_la_soeur() -> None:
    """Ni en tête ni en queue : à côté de celle qu'on a copiée, là où sa syntaxe est valide.
    Une entrée posée hors de la liste casserait la page."""
    sortie, _r = m.ajouter_le_lien(
        INDEX_MAIN, "app/blog/page.tsx", soeur_slug="premier-article",
        slug_neuf="mon-sujet", titre_soeur="Premier article", titre_neuf="Mon sujet")
    lignes = sortie.split("\n")
    i_soeur = next(i for i, l in enumerate(lignes) if "premier-article" in l)
    assert "mon-sujet" in lignes[i_soeur + 1], sortie


def test_le_RESTE_de_l_index_n_est_pas_touche() -> None:
    sortie, _r = m.ajouter_le_lien(
        INDEX_MAIN, "app/blog/page.tsx", soeur_slug="premier-article",
        slug_neuf="mon-sujet", titre_soeur="Premier article", titre_neuf="Mon sujet")
    sans_neuf = "\n".join(l for l in sortie.split("\n") if "mon-sujet" not in l)
    assert sans_neuf == INDEX_MAIN.rstrip("\n") + "\n" or sans_neuf == INDEX_MAIN, sans_neuf


def test_un_titre_ABSENT_de_l_entree_n_est_pas_fabrique() -> None:
    """Quand le libellé vient du front matter de la page, l'entrée ne porte qu'une URL.
    Y injecter un titre inventerait du balisage que le site ne rend pas."""
    index = 'const articles = [\n  { slug: "premier-article" },\n];\n'
    sortie, refus = m.ajouter_le_lien(
        index, "lib/articles.ts", soeur_slug="premier-article",
        slug_neuf="mon-sujet", titre_soeur="Premier article", titre_neuf="Mon sujet")
    assert refus == "", refus
    assert '{ slug: "mon-sujet" },' in sortie
    assert "Mon sujet" not in sortie


# --- ce qu'on REFUSE ------------------------------------------------------------------------------

def test_un_titre_de_soeur_VIDE_ne_mutile_pas_la_ligne() -> None:
    """Le garde-fou qui empêche `replace("", x)` — lequel insère entre CHAQUE caractère.

    Une mutation relâchant la condition a survécu à ma première version parce que mon titre
    absent ne se lisait simplement pas dans la ligne : le remplacement était un no-op. Avec un
    titre VIDE, il devient destructeur. C'est le cas réel quand la sœur n'a pas de titre lisible.
    """
    sortie, refus = m.ajouter_le_lien(
        INDEX_MAIN, "app/blog/page.tsx", soeur_slug="premier-article",
        slug_neuf="mon-sujet", titre_soeur="", titre_neuf="Mon sujet")
    assert refus == "", refus
    assert '<li><a href="/blog/mon-sujet">Premier article</a></li>' in sortie, sortie
    assert "MMon sujeto" not in sortie and sortie.count("Mon sujet") == 0, sortie


def test_un_slug_IDENTIQUE_a_celui_de_la_soeur_est_refuse() -> None:
    """Sinon on duplique la ligne à l'identique : deux entrées vers la même page, et le
    crawler signalerait des liens dupliqués sur un index que personne n'a touché à la main."""
    sortie, refus = m.ajouter_le_lien(
        INDEX_MAIN, "app/blog/page.tsx", soeur_slug="premier-article",
        slug_neuf="premier-article")
    assert sortie == "" and "ne se lit pas" in refus, refus


def test_un_index_engendre_ne_se_modifie_PAS() -> None:
    sortie, refus = m.ajouter_le_lien(
        INDEX_ENGENDRE, "content/blog/_index.md", soeur_slug="premier-article",
        slug_neuf="mon-sujet")
    assert sortie == "" and "engendree" in refus, refus


def test_un_index_qui_ne_se_RELIT_PLUS_est_refuse() -> None:
    """Le contrôle d'après-coup. Une entrée dont la syntaxe s'étale sur plusieurs lignes ne se
    clone pas en copiant une ligne : le fichier sort déséquilibré, et c'est `_refus_de_format`
    qui l'attrape — pas une règle qui aurait tenté de deviner la forme."""
    casse = ('const articles = [\n'
             '  { slug: "premier-article",\n'
             '    titre: "Premier" },\n'
             '];\n')
    sortie, refus = m.ajouter_le_lien(
        casse, "lib/articles.ts", soeur_slug="premier-article", slug_neuf="mon-sujet")
    assert sortie == "" and "ne se relit plus" in refus, refus


def test_un_slug_absent_de_l_entree_est_refuse() -> None:
    sortie, refus = m.ajouter_le_lien(
        INDEX_MAIN, "app/blog/page.tsx", soeur_slug="", slug_neuf="mon-sujet")
    assert sortie == "" and refus, refus
