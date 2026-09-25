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


# --- Ce que 413 fichiers REELS ont trouve -------------------------------------------------------
# Premier correctif ecrit, la sonde a relu tous les fichiers des neuf depots du banc. Un seul
# refus, et il etait faux. La preuve est de construction : un fichier deja dans le depot, que
# Netlify construit et sert, est valide. Ces trois tests figent les deux formes trouvees.

LIGNE_REELLE = ("{alternates.map((a) => (<link rel=\"alternate\" hreflang={a.lang} "
                "href={a.href} />))}")


def test_la_ligne_de_base_astro_qui_avait_ete_refusee_a_tort() -> None:
    """`src/layouts/Base.astro`, textuellement. Apres le `/>` on est dans l'expression qui se
    referme, pas dans du texte : ses deux parentheses doivent etre comptees."""
    contenu = "---\nconst alternates = []\n---\n<html>\n<head>\n%s\n</head>\n</html>\n" % LIGNE_REELLE
    assert app._refus_de_format("src/layouts/Base.astro", contenu) is None


@pytest.mark.parametrize("element", ["(<p>{x}</p>)", "(<hr key={x} />)"])
def test_un_element_dans_une_expression_ne_mange_pas_ses_parentheses(element: str) -> None:
    """Fermante ou auto-fermante, meme exigence : ce qui suit l'element appartient au code.

    Ma deuxieme version comptait les elements ouverts et croyait ce cas resolu : apres `</p>`
    on est bien toujours dans `<main>`, mais AUSSI dans l'accolade ouverte avant lui. Il faut
    suivre les deux.
    """
    contenu = ("export default function P() {\n  return (\n    <main>\n"
               "      {items.map((x) => %s)}\n    </main>\n  )\n}\n" % element)
    assert app._refus_de_format("src/pages/x.jsx", contenu) is None


def test_une_expression_vraiment_non_refermee_dans_un_element_reste_vue() -> None:
    """Le pendant du test precedent : sans lui, « ne rien compter » passerait les deux."""
    contenu = ("export default function P() {\n  return (\n    <main>\n"
               "      {items.map((x) => (<p>{x}</p>)}\n    </main>\n  )\n}\n")
    assert app._refus_de_format("src/pages/x.jsx", contenu)


# --- Cinq autres trous, trouves par la deuxieme serie de mutations ------------------------------

def test_du_code_apres_un_generique_reste_du_code() -> None:
    """Sans le controle de fermeture, `Array<string>` empilerait un element et tout ce qui suit
    deviendrait de la prose. Ma premiere version du test ne le montrait pas : le desequilibre
    que j'y avais mis tombait DANS une accolade, donc hors prose de toute facon.

    Le desequilibre doit aussi etre BORNE par une balise plus loin : une region de prose qui
    court jusqu'a la fin du fichier n'est jamais emise, et masquait la mutation sans rien
    prouver. Deux fois de suite, ce test a mesure autre chose que ce qu'il annonce.
    """
    ts = ("const noms: Array<string> = ['a'\n"
          "export default function P() { return <p>texte</p> }\n")
    assert app._unbalanced_delimiters(ts), "crochet non referme manque apres un generique"


def test_une_balise_auto_fermante_n_ouvre_pas_un_element() -> None:
    """`<Encart />` ne se ferme pas : ce qui suit appartient a l'englobant, pas a l'encart.

    Le composant est utilise des DEUX facons dans le fichier — c'est la seule situation ou le
    controle de fermeture, a lui seul, ne suffit pas a distinguer les deux.
    """
    contenu = ("export default function P() {\n"
               "  return (\n"
               "    <main>\n"
               "      {items.map((x) => (<Encart key={x} />))}\n"
               "      <Encart>L'agent lit la page.</Encart>\n"
               "    </main>\n"
               "  )\n"
               "}\n")
    assert app._refus_de_format("src/pages/x.jsx", contenu) is None


def test_la_prose_reprend_apres_une_expression() -> None:
    """Ce qui suit `{valeur}` est encore du texte. Sinon l'apostrophe d'apres rouvre une chaine."""
    contenu = ("export default function P() {\n"
               "  return (\n"
               "    <main>\n"
               "      <p>L'etat vaut {valeur} et c'est tout.</p>\n"
               "    </main>\n"
               "  )\n"
               "}\n")
    assert app._refus_de_format("src/pages/x.jsx", contenu) is None


def test_le_corps_d_un_script_reste_du_code() -> None:
    """Un `<script>` casse doit partir en refus : son corps n'est pas de la prose."""
    contenu = ("<html>\n<body>\n  <p>L'agent lit la page.</p>\n"
               "  <script>\n    const f = (a\n  </script>\n</body>\n</html>\n")
    assert app._refus_de_format("src/pages/x.astro", contenu)


def test_une_fermeture_de_casse_differente_est_reconnue() -> None:
    """HTML ne distingue pas la casse : `<section>` peut se fermer par `</SECTION>`.

    Ma premiere version mettait la majuscule sur l'OUVRANTE — or c'est le nom de l'ouvrante
    qu'on met en minuscules avant de chercher, donc la recherche tombait juste de toute facon.
    C'est la FERMANTE qui doit differer pour que le test touche ce qu'il nomme.
    """
    contenu = ("---\nconst titre = 'x'\n---\n"
               "<section>L'agent lit la page d'accueil (section Sitemap.</SECTION>\n")
    assert app._refus_de_format("src/pages/x.astro", contenu) is None


def test_une_chaine_qui_porte_une_balise_ouvrante_garde_son_guillemet() -> None:
    """Construire du HTML par concatenation est courant : `const ouvre = "<li>"`.

    Le scanner de prose ne suit les chaines qu'a l'INTERIEUR des balises — cette `<li>` est donc
    vue comme une vraie balise, et la region de texte qu'elle ouvre recouvre le guillemet
    fermant de la chaine. Si l'appelant sautait cette region, il ne verrait jamais ce guillemet
    et lirait tout le reste du fichier comme une chaine ouverte. C'est ce que garantit le
    `not quote` de l'appelant : les deux balayages ne voient pas la meme chose, et c'est celui
    qui suit les chaines qui doit gagner.
    """
    contenu = ('const ouvre = "<li>"\n'
               "export default function P() {\n"
               "  return (\n"
               "    <main><li>L'agent lit la page.</li></main>\n"
               "  )\n"
               "}\n")
    assert app._refus_de_format("src/pages/x.jsx", contenu) is None

    # ET le fichier CASSE doit encore partir en refus. Sans cette moitie, le test ne voit rien :
    # perdre le guillemet fermant fait tout avaler, donc ne produit AUCUN refus — le meme
    # resultat qu'un fichier sain. Un defaut qui n'ajoute pas de bruit se mesure par ce qu'il
    # fait DISPARAITRE.
    casse = contenu.replace("  )\n", "")
    assert app._refus_de_format("src/pages/x.jsx", casse), (
        "parenthese non refermee manquee : le guillemet fermant de la chaine a ete saute")


def test_un_chevron_dans_un_attribut_ne_coupe_pas_la_balise() -> None:
    """`title="a > b"` : le chevron appartient a la valeur, pas a la fin de la balise.

    POURQUOI CE TEST REGARDE LA FONCTION ET PAS LE VERDICT. J'ai cherche une entree ou retirer
    le suivi des chaines d'attribut change le refus final : sur douze candidats, aucune. Le
    garde `not quote` de l'appelant masque le defaut — les deux protegent la meme chose par
    deux cotes.

    Le laisser non teste pour autant serait s'en remettre a ce masque. Sans ce suivi,
    `_spans_de_prose` rend une region qui COMMENCE au milieu d'un attribut (` b">L'agent...`) :
    son contrat est faux meme si son unique appelant d'aujourd'hui n'en souffre pas. C'est le
    genre de ligne qu'un deuxieme appelant paierait, et il n'y aurait alors plus rien pour dire
    d'ou vient le defaut.
    """
    texte = '<main><p title="a > b">L\'agent lit la page.</p></main>'
    assert [texte[d:f] for d, f in app._spans_de_prose(texte)] == ["L'agent lit la page."]
