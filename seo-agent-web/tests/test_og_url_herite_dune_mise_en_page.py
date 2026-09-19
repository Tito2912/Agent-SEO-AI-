# -*- coding: utf-8 -*-
"""Ecrire un og:url la ou il n'est PAS ecrit, en recopiant le canonical de la meme page.

MESURE DU 19/09/2026, SUR UN SITE CLIENT REEL : neuf pages signalees en
`open_graph_url_not_matching_canonical`, neuf fichiers essayes, AUCUN patche. Le correcteur
n'avait pas tort — il n'avait simplement qu'un seul geste.

LA CAUSE, lue dans le depot :

    app/layout.tsx    openGraph: { url: getSiteUrl() }      -> https://oryvalo.com/
    app/about/page.tsx    alternates: { canonical: '/about' }    (aucun openGraph)

Next.js HERITE l'openGraph de la mise en page racine. Chaque page emet donc l'og:url de la
racine tandis que son canonical differe. La valeur fautive n'est ecrite dans AUCUN fichier de
page : le reecriveur cherchait une chaine a remplacer, n'en trouvait aucune, et s'abstenait.

ON RECOPIE LE LITTERAL, ET C'EST LE POINT DELICAT. Le canonical de ces pages est RELATIF
(`/about`), et `_canonical_ecrit_dans` — qui n'accepte que l'absolu — rend une chaine vide sur
ces fichiers. Reconstruire une URL absolue aurait demande de connaitre le domaine et de deviner
la resolution de `metadataBase`. Recopier la chaine telle quelle rend les deux valeurs egales
PAR CONSTRUCTION : Next resout un og:url relatif exactement comme il resout un canonical
relatif. La coherence recherchee est interne au fichier, donc elle se regle dans le fichier.

UNE ABSTENTION ANTERIEURE EST LEVEE ICI, sciemment, et il faut dire pourquoi. Le code portait :
« les formes objet demandent de savoir ou s'inserer dans une structure ; on s'y abstient, comme
pour l'Open Graph ». Cette prudence valait tant qu'une insertion ratee partait droit chez le
client. Depuis que les corrections attendent le verdict du build avant d'etre proposees, une
erreur de structure fait echouer la CI et la pull request reste en brouillon. Le risque a change
de nature, donc la reponse aussi — mais la portee reste etroite, et les tests d'abstention
ci-dessous sont la moitie la plus importante de ce fichier.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")

from backend import app as m  # noqa: E402

# La forme EXACTE du depot client, recopiee depuis `app/about/page.tsx`.
PAGE_CLIENT = """import type { Metadata } from 'next';
import Link from 'next/link';

export const metadata: Metadata = {
  title: 'About',
  description: 'Learn more about Oryvalo, our editorial approach.',
  alternates: { canonical: '/about' },
};

export default function AboutPage() {
  return <article><h1>About Oryvalo</h1></article>;
}
"""


# --- ce qu'on ecrit ----------------------------------------------------------------------

def test_l_og_url_est_POSE_a_partir_du_canonical() -> None:
    sortie, notes = m._inserer_og_url_depuis_le_canonical(PAGE_CLIENT)
    assert "openGraph: { url: '/about' }," in sortie, sortie
    assert notes and "/about" in notes[0]
    # Pose APRES le canonical, dans le meme objet, a la meme indentation.
    lignes = sortie.splitlines()
    i = next(n for n, l in enumerate(lignes) if "alternates:" in l)
    assert "openGraph:" in lignes[i + 1], "la propriété n'est pas posée juste après le canonical"
    assert lignes[i + 1].startswith("  "), "l'indentation ne suit pas celle du voisin"


def test_le_reste_du_fichier_n_est_PAS_touche() -> None:
    """Une insertion qui deplace ou reecrit autre chose serait un diff qu'on ne peut pas relire."""
    sortie, _notes = m._inserer_og_url_depuis_le_canonical(PAGE_CLIENT)
    retire = sortie.replace("  openGraph: { url: '/about' },\n", "")
    assert retire == PAGE_CLIENT, "le fichier a changé ailleurs que sur la ligne ajoutée"


def test_le_resultat_reste_du_TYPESCRIPT_valide() -> None:
    """Le fichier doit se relire : accolades equilibrees, virgules a leur place.

    On ne lance pas `tsc` ici — c'est le build du client qui juge, et c'est justement pour ca
    que la pull request part en brouillon. Mais un desequilibre grossier doit etre vu tout de
    suite plutot que decouvert par une CI rouge chez quelqu'un d'autre.
    """
    sortie, _notes = m._inserer_og_url_depuis_le_canonical(PAGE_CLIENT)
    assert sortie.count("{") == sortie.count("}"), "accolades déséquilibrées"
    assert sortie.count("'") % 2 == 0, "apostrophes déséquilibrées"
    bloc = sortie.split("export const metadata: Metadata = {", 1)[1].split("};", 1)[0]
    for ligne in [x.strip() for x in bloc.splitlines() if x.strip()]:
        assert ligne.endswith(","), "propriété sans virgule finale : %r" % ligne


@pytest.mark.parametrize("guillemet", ["'", '"'])
def test_le_guillemet_du_voisin_est_REPRIS(guillemet) -> None:
    """Un fichier en guillemets doubles ne doit pas recevoir une ligne en simples : le diff
    passerait sous le formateur du client au commit suivant, et brouillerait l'historique."""
    src = PAGE_CLIENT.replace("'/about'", "%s/about%s" % (guillemet, guillemet))
    sortie, _notes = m._inserer_og_url_depuis_le_canonical(src)
    assert "openGraph: { url: %s/about%s }," % (guillemet, guillemet) in sortie, sortie


def test_un_canonical_ABSOLU_est_recopie_tel_quel() -> None:
    """La regle ne depend pas de la forme : on copie, on ne reconstruit pas."""
    src = PAGE_CLIENT.replace("'/about'", "'https://oryvalo.com/about'")
    sortie, _notes = m._inserer_og_url_depuis_le_canonical(src)
    assert "openGraph: { url: 'https://oryvalo.com/about' }," in sortie


# --- ce qu'on REFUSE d'ecrire, et c'est la moitie qui compte --------------------------------

def test_un_openGraph_DEJA_PRESENT_est_laisse_tranquille() -> None:
    """On pose une propriete absente ; on ne s'insere pas dans une structure existante.

    C'est la frontiere exacte de l'abstention qu'on leve. Poser une cle a cote d'un canonical
    est une operation qu'on sait faire ; entrer dans un objet deja ecrit pour y ajouter ou y
    remplacer une cle en est une autre, et elle reste hors de portee.
    """
    src = PAGE_CLIENT.replace(
        "  alternates: { canonical: '/about' },",
        "  alternates: { canonical: '/about' },\n  openGraph: { title: 'About' },")
    sortie, notes = m._inserer_og_url_depuis_le_canonical(src)
    assert sortie == src and not notes


def test_un_fichier_qui_porte_PLUSIEURS_pages_est_refuse() -> None:
    """Deux canonicals : on ne sait pas de quelle page on parle. Refuser vaut mieux que deviner."""
    src = PAGE_CLIENT.replace(
        "  alternates: { canonical: '/about' },",
        "  alternates: { canonical: '/about' },\n  alternates: { canonical: '/contact' },")
    sortie, notes = m._inserer_og_url_depuis_le_canonical(src)
    assert sortie == src and not notes


def test_un_canonical_MULTI_LIGNE_est_refuse() -> None:
    """La forme n'est pas celle qu'on sait cloner : l'indentation et la virgule ne se devinent
    pas de façon fiable dans une structure repartie sur plusieurs lignes."""
    src = PAGE_CLIENT.replace(
        "  alternates: { canonical: '/about' },",
        "  alternates: {\n    canonical: '/about',\n  },")
    sortie, notes = m._inserer_og_url_depuis_le_canonical(src)
    assert sortie == src and not notes


def test_une_valeur_en_BACKTICKS_est_refusee() -> None:
    """Un gabarit de chaine est une valeur calculee : la recopier ecrirait du code mort."""
    src = PAGE_CLIENT.replace("'/about'", "`${base}/about`")
    sortie, notes = m._inserer_og_url_depuis_le_canonical(src)
    assert sortie == src and not notes


def test_une_valeur_ASSEMBLEE_entre_guillemets_est_refusee() -> None:
    """Le refus qui a failli n'etre prouve par rien, et la lecon vaut le test.

    Ma premiere version n'exercait ce garde-fou qu'avec des backticks — or le motif n'accepte
    que `'` ou `"`, donc il s'arretait AVANT de l'atteindre. Une mutation qui supprimait le
    garde-fou survivait : le test passait pour une autre raison que celle qu'il annoncait.

    La forme qui l'atteint vraiment est une valeur entre guillemets qui contient malgre tout
    une marque d'assemblage. On s'abstient : recopier `${BASE}/about` dans un og:url ecrirait
    une adresse qui n'existe pas.
    """
    src = PAGE_CLIENT.replace("'/about'", '"${BASE}/about"')
    sortie, notes = m._inserer_og_url_depuis_le_canonical(src)
    assert sortie == src and not notes, sortie


def test_un_fichier_SANS_canonical_est_refuse() -> None:
    """Rien a recopier : cette famille ne fabrique pas de valeur, elle en aligne deux."""
    src = PAGE_CLIENT.replace("  alternates: { canonical: '/about' },\n", "")
    sortie, notes = m._inserer_og_url_depuis_le_canonical(src)
    assert sortie == src and not notes


def test_une_page_HTML_ordinaire_n_est_pas_concernee() -> None:
    """Le geste vise une forme objet precise ; du balisage se corrige par remplacement, et ce
    chemin-la existait deja."""
    src = '<html><head><link rel="canonical" href="https://x.fr/a"></head></html>'
    sortie, notes = m._inserer_og_url_depuis_le_canonical(src)
    assert sortie == src and not notes


# --- la portee, dans la boucle de correction --------------------------------------------------

def test_le_geste_est_RESTREINT_a_la_famille_og_url() -> None:
    """Poser un openGraph dans la pull request d'une AUTRE anomalie elargirait un diff que le
    client a accepte de relire pour autre chose.

    Le test lit la boucle de correction la ou elle est ecrite : l'appel doit etre garde par la
    famille, et pas applique a tout patch qui passe.
    """
    import ast as _ast

    arbre = _ast.parse(Path(m.__file__).read_text(encoding="utf-8"))
    appels = [n for n in _ast.walk(arbre)
              if isinstance(n, _ast.Call)
              and _ast.unparse(n.func) == "_inserer_og_url_depuis_le_canonical"]
    assert appels, "le geste n'est appelé nulle part : il ne sert à rien"

    gardes = []
    for n in _ast.walk(arbre):
        if not isinstance(n, _ast.If):
            continue
        if not any(isinstance(c, _ast.Call)
                   and _ast.unparse(c.func) == "_inserer_og_url_depuis_le_canonical"
                   for c in _ast.walk(n)):
            continue
        gardes.append(_ast.unparse(n.test))
    assert gardes, "l'insertion n'est gardée par aucune condition"
    assert any("_OG_URL_KEYS" in g for g in gardes), (
        "l'insertion n'est pas restreinte à la famille og:url : %s" % gardes)
