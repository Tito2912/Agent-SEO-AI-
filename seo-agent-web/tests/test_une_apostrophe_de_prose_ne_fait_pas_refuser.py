# -*- coding: utf-8 -*-
"""Refuser une page valide coute une correction au client. La prose francaise n'est pas du code.

MESURE DU 25/09/2026, banc des neuf idiomes. La page next-pages a ete refusee DEUX fois de
suite, avec deux modeles differents, sur un fichier parfaitement equilibre. Le motif etait
identique a chaque fois : `{ non referme (+1), ( non referme (+1)`. Deux modeles qui echouent
au meme endroit de la meme facon ne se trompent pas ensemble — c'est la mesure qui se trompe.

La cause : `_unbalanced_delimiters` suivait les guillemets PARTOUT. Dans `<p>L'agent lit.</p>`,
l'apostrophe de `L'agent` ouvrait une chaine que rien ne refermait, et le reste du fichier
etait lu comme du texte — les accolades et parentheses du composant n'etaient jamais comptees.

Ce fichier interdit la FORME du defaut, pas les trois cas mesures : il fabrique la page de
prose pour CHAQUE extension que `_refus_de_format` soumet au comptage, lue dans la source. Une
extension ajoutee demain est couverte sans que personne y pense.
"""
from __future__ import annotations

import ast
import pathlib

import pytest

from backend import app


APP_SOURCE = pathlib.Path(app.__file__).read_text(encoding="utf-8", errors="replace")


def _extensions_soumises_au_comptage() -> tuple[str, ...]:
    """Les extensions que `_refus_de_format` fait passer par le comptage de delimiteurs.

    On les LIT dans la source plutot que de les recopier : une liste recopiee ici mesurerait
    ce que je crois que le produit fait, et resterait verte le jour ou le produit change.
    """
    arbre = ast.parse(APP_SOURCE)
    for noeud in ast.walk(arbre):
        if not (isinstance(noeud, ast.FunctionDef) and noeud.name == "_refus_de_format"):
            continue
        for interne in ast.walk(noeud):
            if (isinstance(interne, ast.Call)
                    and isinstance(interne.func, ast.Attribute)
                    and interne.func.attr == "endswith"
                    and interne.args
                    and isinstance(interne.args[0], ast.Tuple)):
                exts = tuple(e.value for e in interne.args[0].elts
                             if isinstance(e, ast.Constant) and isinstance(e.value, str))
                if exts:
                    return exts
    raise AssertionError("les extensions comptees ne se lisent plus dans _refus_de_format")


EXTENSIONS = _extensions_soumises_au_comptage()


def _page_de_prose(corps: str) -> str:
    """Un composant equilibre dont le seul contenu variable est de la prose entre balises."""
    return ("export default function Page() {\n"
            "  return (\n"
            "    <main>\n"
            "%s\n"
            "    </main>\n"
            "  )\n"
            "}\n" % corps)


# Trois formes de prose francaise ordinaire. Chacune a ete mesuree comme faisant refuser.
PROSES = {
    "apostrophe en nombre impair": "      <p>L'agent lit la page avant de corriger.</p>",
    "citation a guillemet droit": '      <p>Il a repondu " et la conversation s\'est arretee.</p>',
    "enumeration numerotee": "      <p>Trois etapes : 1) lire, 2) comparer, 3) corriger.</p>",
    "parenthese seule dans une phrase": "      <p>Voir la doc (section Sitemap.</p>",
    "adresse web dans le texte": "      <p>Voir https://noyaru.com (et la doc) pour la suite.</p>",
}


@pytest.mark.parametrize("extension", EXTENSIONS)
@pytest.mark.parametrize("forme", sorted(PROSES))
def test_la_prose_francaise_ne_fait_refuser_aucune_extension(extension: str, forme: str) -> None:
    contenu = _page_de_prose(PROSES[forme])
    refus = app._refus_de_format("src/pages/exemple" + extension, contenu)
    assert refus is None, (
        "prose refusee sur %s (%s) : %s" % (extension, forme, refus))


# Ce qui doit continuer de partir en refus. Sans ces cas, le correctif ci-dessus se resumerait
# a « ne rien compter », ce qui passerait tous les tests precedents.
CASSES = {
    "accolade JSX ouverte dans du texte": "      <p>{maVariable</p>",
    "parenthese de code non refermee": "      {liste.map((x) => <b>{x}</b>}",
    "accolade de composant non refermee": None,   # traite a part : hors gabarit
}


@pytest.mark.parametrize("forme", ["accolade JSX ouverte dans du texte",
                                   "parenthese de code non refermee"])
def test_un_vrai_desequilibre_reste_refuse(forme: str) -> None:
    refus = app._refus_de_format("src/pages/exemple.jsx", _page_de_prose(CASSES[forme]))
    assert refus and "delimiteur" in refus, "desequilibre reel non vu (%s) : %r" % (forme, refus)


def test_une_accolade_de_composant_non_refermee_reste_refusee() -> None:
    """Le desequilibre est HORS de toute region de prose : le chemin normal doit le voir."""
    casse = ("export default function Page() {\n"
             "  return (\n"
             "    <main>\n"
             "      <p>L'agent lit la page.</p>\n"
             "    </main>\n"
             "  )\n")
    refus = app._refus_de_format("src/pages/exemple.jsx", casse)
    assert refus and "delimiteur" in refus, refus


def test_un_generique_typescript_ne_devient_pas_de_la_prose() -> None:
    """`Array<string>` ouvre une pseudo-balise. Ce qui suit est du CODE, pas du texte.

    Si on y traitait les guillemets comme de la prose, l'interieur des chaines serait compte —
    et on aurait remplace un faux refus par un autre.
    """
    ts = ("const noms: Array<string> = ['a ( b', 'c']\n"
          "export function lire(x: Map<string, number>): void {\n"
          "  console.log(noms, x\n"
          "}\n")
    assert app._unbalanced_delimiters(ts), "parenthese non refermee manquee apres un generique"

    sain = ("const noms: Array<string> = ['a ( b )', 'c']\n"
            "export function lire(x: Map<string, number>): void {\n"
            "  console.log(noms, x)\n"
            "}\n")
    assert not app._unbalanced_delimiters(sain), "chaine comptee comme de la prose"


def test_la_prose_ne_desactive_pas_le_comptage_des_accolades() -> None:
    """Dans du texte, `{` et `}` restent des delimiteurs : c'est une expression JSX."""
    assert app._unbalanced_delimiters(_page_de_prose("      <p>L'etat vaut {valeur}.</p>")) == ""
    assert app._unbalanced_delimiters(_page_de_prose("      <p>L'etat vaut {valeur.</p>"))


# --- Les cinq trous que les mutations ont trouves ---------------------------------------------
# Chacun de ces tests existe parce qu'une mutation a SURVECU sans lui. Ils ne decrivent pas des
# cas imagines : ils decrivent les cinq facons dont la selection des vraies balises pouvait etre
# cassee sans qu'aucun test ne bronche.

def test_la_prose_qui_suit_une_balise_auto_fermante_reste_de_la_prose() -> None:
    """`<br />` n'a pas de `</br>` : sans la branche auto-fermante, le texte suivant redevient
    du code et l'apostrophe rouvre une chaine."""
    contenu = ("export default function Page() {\n"
               "  return (\n"
               "    <main>\n"
               "      <br />L'agent lit la page avant d'agir. Trois etapes : 1) lire, 2) agir.\n"
               "    </main>\n"
               "  )\n"
               "}\n")
    assert app._refus_de_format("src/pages/x.jsx", contenu) is None


def test_une_balise_html_en_majuscules_est_reconnue_par_sa_fermeture() -> None:
    """HTML ne distingue pas la casse : `<SECTION>` se ferme par `</section>`, et un gabarit
    Astro peut porter les deux. Une recherche sensible a la casse y perdrait la prose."""
    # La prose est bornee PAR la balise en majuscules. Ma premiere version la mettait dans un
    # `<p>` interieur : c'est `p` qui bornait la region, la casse de `SECTION` ne servait a
    # rien, et la mutation survivait. Un test doit toucher ce qu'il nomme.
    # DEUX apostrophes, pas une : avec une seule, la chaine ouverte avale la parenthese et le
    # defaut s'annule lui-meme. Le bug qu'on corrige savait masquer sa propre mesure.
    contenu = ("---\nconst titre = 'x'\n---\n"
               "<SECTION>L'agent lit la page d'accueil (section Sitemap.</section>\n")
    assert app._refus_de_format("src/pages/x.astro", contenu) is None


def test_un_fragment_jsx_borne_de_la_prose() -> None:
    """`<>` n'a pas de nom : c'est `</>` qui prouve que la region entre les deux est du texte."""
    contenu = ("export default function Page() {\n"
               "  return (\n"
               "    <>L'agent lit la page avant d'agir. Etapes : 1) lire, 2) agir.</>\n"
               "  )\n"
               "}\n")
    assert app._refus_de_format("src/pages/x.jsx", contenu) is None


def test_une_accolade_dans_un_commentaire_n_est_pas_un_delimiteur() -> None:
    """Un commentaire peut contenir du balisage d'exemple. Ce qu'il contient ne compte pas."""
    contenu = ("// <p>{ceci n'est qu'un exemple</p>\n"
               "export default function Page() {\n"
               "  return (\n"
               "    <main><p>L'agent lit la page.</p></main>\n"
               "  )\n"
               "}\n")
    assert app._refus_de_format("src/pages/x.jsx", contenu) is None


def test_une_accolade_dans_une_chaine_n_est_pas_un_delimiteur() -> None:
    """Une chaine peut contenir du balisage. Le JSON-LD en est plein."""
    contenu = ('const gabarit = "<p>{ceci reste une chaine</p>"\n'
               "export default function Page() {\n"
               "  return (\n"
               "    <main><p>L'agent lit la page.</p><span>{gabarit}</span></main>\n"
               "  )\n"
               "}\n")
    assert app._refus_de_format("src/pages/x.jsx", contenu) is None
