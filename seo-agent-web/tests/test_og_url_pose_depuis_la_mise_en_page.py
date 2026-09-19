# -*- coding: utf-8 -*-
"""Poser un openGraph COMPLET sur une page qui herite le sien d'une mise en page.

TROISIEME TENTATIVE SUR CETTE FAMILLE, et les deux precedentes expliquent la forme de ce
fichier. Elles ont echoue le 19/09/2026 en croyant decrire une regle alors qu'elles decrivaient
une apparence.

LE CAS. En Next.js App Router, `app/layout.tsx` declare un openGraph et les pages n'en
declarent aucun : elles en HERITENT. Quand cet openGraph porte une `url` fixe, toutes les pages
annoncent l'adresse de la racine alors que leur canonical differe. La valeur fautive n'est
ecrite dans AUCUN fichier de page — il n'y a rien a remplacer, il faut POSER.

POURQUOI LE BLOC EST COMPLET. La premiere tentative ne posait que `url`. Mesure sur une
reproduction Next.js reelle : les metadonnees sont fusionnees SUPERFICIELLEMENT, donc un
openGraph de page REMPLACE celui de la mise en page.

    page SANS openGraph          9 balises og:, toutes heritees
    page AVEC { url } seul       3 balises. og:image, og:site_name, og:type DISPARAISSENT.
    page AVEC le bloc complet    9 balises — et title/description/url deviennent ceux de la
                                 PAGE au lieu d'etre ceux de la racine.

Perdre `og:image`, c'est perdre le visuel d'apercu sur les reseaux. Et le crawler l'aurait
signale au passage suivant en `open_graph_tags_incomplete`, dont la regle exige cinq balises :
on aurait echange une anomalie contre une autre.

CE QUE LE BUILD NE VOIT PAS. La premiere tentative a produit un diff propre, un build Vercel
vert, et une pull request sortie du brouillon, prete a merger. UN BUILD VALIDE LA SYNTAXE, PAS
LE SENS. C'est pour cela que le dernier test de ce fichier mesure le RENDU et non le diff.
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

# La mise en page du client, recopiee telle quelle.
LAYOUT = """import type { Metadata } from 'next';

export const metadata: Metadata = {
  metadataBase: new URL(getSiteUrl()),
  title: { default: "Oryvalo", template: "%s | Oryvalo" },
  description: "Learn how to start an AI video service.",
  alternates: { canonical: "/" },
  openGraph: {
    type: "website",
    title: "Oryvalo — AI Video Service",
    description: "Beginner-friendly tutorials.",
    url: getSiteUrl(),
    siteName: "Oryvalo",
    images: [{ url: "/opengraph-image", width: 1200, height: 630, alt: "Oryvalo" }],
  },
};
"""

PAGE = """import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'About',
  description: 'Learn more about Oryvalo.',
  alternates: { canonical: '/about' },
};

export default function AboutPage() {
  return <article><h1>About</h1></article>;
}
"""


# --- lire la mise en page ----------------------------------------------------------------

def test_les_champs_A_REPORTER_sont_lus_dans_la_mise_en_page() -> None:
    """`images` porte des accolades IMBRIQUEES : une expression reguliere s'y casserait.

    Le scanner equilibre les accolades et saute ce qui est dans une chaine. C'est la raison
    d'etre de cette fonction, et le test le montre sur la vraie forme.
    """
    rep = m._og_a_reporter_depuis_layout(LAYOUT)
    assert rep["type"] == '"website"'
    assert rep["siteName"] == '"Oryvalo"'
    assert rep["images"] == '[{ url: "/opengraph-image", width: 1200, height: 630, alt: "Oryvalo" }]'


def test_ni_title_ni_description_ne_sont_reportes() -> None:
    """Volontaire : sans eux, Next retombe sur ceux de la PAGE.

    C'est meilleur que l'etat actuel, ou les neuf pages du site partagent le titre et la
    description de la racine. Les reporter figerait ce defaut au lieu de le lever.
    """
    rep = m._og_a_reporter_depuis_layout(LAYOUT)
    assert "title" not in rep and "description" not in rep


def test_une_accolade_DANS_UNE_CHAINE_ne_ferme_pas_le_bloc() -> None:
    """Le piege exact que le scanner existe pour eviter.

    L'accolade est FERMANTE et NON APPARIEE — c'est le seul cas qui distingue. Une paire
    equilibree comme `{valo}` traverse aussi bien un scanner naif : une mutation qui retirait
    la gestion des chaines a survecu a ma premiere version de ce test.
    """
    piege = LAYOUT.replace('alt: "Oryvalo"', 'alt: "Oryvalo}"')
    rep = m._og_a_reporter_depuis_layout(piege)
    assert "images" in rep, "le bloc a été fermé trop tôt par une accolade dans un texte : %r" % rep
    assert rep["images"].endswith("]"), rep["images"]
    assert rep.get("siteName") == '"Oryvalo"', "le bloc a été tronqué : %r" % rep


def test_une_mise_en_page_SANS_openGraph_ne_reporte_rien() -> None:
    assert m._og_a_reporter_depuis_layout("export const metadata = { title: 'x' };") == {}


def test_la_mise_en_page_la_PLUS_PROCHE_est_cherchee_en_premier() -> None:
    """Next applique les mises en page par segment : celle du dossier l'emporte sur la racine."""
    ordre = m._layouts_au_dessus("app/blog/mon-article/page.tsx")
    assert ordre.index("app/blog/mon-article/layout.tsx") < ordre.index("app/blog/layout.tsx")
    assert ordre.index("app/blog/layout.tsx") < ordre.index("app/layout.tsx")


# --- ce qu'on ecrit ----------------------------------------------------------------------

def test_le_bloc_pose_est_COMPLET() -> None:
    sortie, notes = m._inserer_og_complet(PAGE, m._og_a_reporter_depuis_layout(LAYOUT))
    assert notes and "/about" in notes[0]
    for attendu in ('type: "website",', 'siteName: "Oryvalo",',
                    'images: [{ url: "/opengraph-image"', "url: '/about',"):
        assert attendu in sortie, "%r manque :\n%s" % (attendu, sortie)
    # Le guillemet de l'url suit celui du canonical voisin, pas celui du layout.
    assert "url: '/about'," in sortie and 'url: "/about",' not in sortie


def test_le_reste_du_fichier_n_est_PAS_touche() -> None:
    sortie, _notes = m._inserer_og_complet(PAGE, m._og_a_reporter_depuis_layout(LAYOUT))
    debut, fin = sortie.split("  openGraph: {", 1)
    reconstruit = debut + fin.split("  },\n", 1)[1]
    assert reconstruit == PAGE, "le fichier a changé ailleurs que sur le bloc ajouté"


def test_le_resultat_reste_equilibre() -> None:
    sortie, _notes = m._inserer_og_complet(PAGE, m._og_a_reporter_depuis_layout(LAYOUT))
    assert sortie.count("{") == sortie.count("}")
    assert sortie.count("[") == sortie.count("]")


# --- ce qu'on REFUSE d'ecrire --------------------------------------------------------------

def test_sans_image_ou_sans_type_on_S_ABSTIENT() -> None:
    """Le refus qui empeche d'echanger une anomalie contre une autre.

    La regle `open_graph_tags_incomplete` du crawler exige cinq balises. Poser un bloc sans
    image ni type les ferait disparaitre : la page quitterait une famille pour entrer dans une
    autre, en perdant son visuel de partage au passage.
    """
    for manquant in ("images", "type"):
        rep = m._og_a_reporter_depuis_layout(LAYOUT)
        rep.pop(manquant)
        sortie, notes = m._inserer_og_complet(PAGE, rep)
        assert sortie == PAGE and not notes, "posé sans %s" % manquant


@pytest.mark.parametrize("nom, page", [
    ("openGraph deja present",
     PAGE.replace("  alternates: { canonical: '/about' },",
                  "  alternates: { canonical: '/about' },\n  openGraph: { title: 'x' },")),
    ("plusieurs pages dans un fichier",
     PAGE.replace("  alternates: { canonical: '/about' },",
                  "  alternates: { canonical: '/about' },\n  alternates: { canonical: '/x' },")),
    ("canonical sur plusieurs lignes",
     PAGE.replace("  alternates: { canonical: '/about' },",
                  "  alternates: {\n    canonical: '/about',\n  },")),
    ("valeur assemblee", PAGE.replace("'/about'", '"${BASE}/about"')),
    ("aucun canonical", PAGE.replace("  alternates: { canonical: '/about' },\n", "")),
])
def test_les_abstentions(nom, page) -> None:
    sortie, notes = m._inserer_og_complet(page, m._og_a_reporter_depuis_layout(LAYOUT))
    assert sortie == page and not notes, nom


def test_le_geste_est_RESTREINT_a_la_famille_og_url() -> None:
    """Poser un openGraph dans la pull request d'une AUTRE anomalie elargirait un diff que le
    client a accepte de relire pour autre chose."""
    import ast as _ast

    arbre = _ast.parse(Path(m.__file__).read_text(encoding="utf-8"))
    gardes = []
    for n in _ast.walk(arbre):
        if not isinstance(n, _ast.If):
            continue
        if any(isinstance(c, _ast.Call) and _ast.unparse(c.func) == "_inserer_og_complet"
               for c in _ast.walk(n)):
            gardes.append(_ast.unparse(n.test))
    assert gardes, "l'insertion n'est gardée par aucune condition"
    assert any("_OG_URL_KEYS" in g for g in gardes), gardes


def test_la_mise_en_page_n_est_lue_QU_UNE_FOIS_pour_tout_le_lot(monkeypatch) -> None:
    """Un projet de vingt pages ne doit pas declencher vingt lectures pour une seule reponse.

    MON PREMIER TEST MESURAIT DE TRAVERS : il interdisait l'appel dans toute boucle, alors que
    parcourir les mises en page candidates de la plus proche a la racine est exactement la
    bonne methode. Ce n'est pas la forme du code qui compte, c'est le nombre de lectures — et
    ca se compte.
    """
    import base64

    lectures: list[str] = []

    def _faux_get(chemin, *, token, params=None, timeout_s=30.0):
        lectures.append(chemin)
        if chemin.endswith("layout.tsx"):
            return {"content": base64.b64encode(LAYOUT.encode("utf-8")).decode("ascii")}
        raise AssertionError("lecture inattendue : %s" % chemin)

    monkeypatch.setattr(m, "_github_api_get", _faux_get)
    monkeypatch.setattr(
        m, "_github_api_put",
        lambda chemin, *, token, json_body: {"content": {"sha": "n"},
                                             "commit": {"sha": "a", "html_url": ""}})

    pages = ["app/about/page.tsx", "app/seo/page.tsx", "app/contact/page.tsx"]
    etat = {p: {"sha": "v", "content": PAGE.replace("/about", "/" + p.split("/")[1])}
            for p in pages}
    patched, _skipped, _targets, _ai = m._deep_patch_issue_files(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="fix",
        all_paths=pages + ["app/layout.tsx"],
        issue_key="open_graph_url_not_matching_canonical", issue_label="OG",
        impacted_urls=["https://exemple.fr/about"], site_name="exemple",
        file_state=etat, max_files=8, link_rewriter=lambda raw: (raw, 0),
        rewriter_ai_fallback=False, targets_override=pages, allow_ai_targeting=False,
    )
    assert len(patched) == 3, "les trois pages devaient être corrigées : %r" % patched
    lectures_layout = [c for c in lectures if c.endswith("layout.tsx")]
    assert len(lectures_layout) == 1, (
        "la mise en page a été lue %d fois pour 3 pages : %r"
        % (len(lectures_layout), lectures_layout))
