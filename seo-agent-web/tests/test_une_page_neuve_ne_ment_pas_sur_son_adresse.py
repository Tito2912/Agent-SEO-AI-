# -*- coding: utf-8 -*-
"""Le canonical d'une page qu'on vient d'écrire ne peut pas être faux : on l'a choisi.

CE FICHIER EST NÉ D'UNE MESURE, pas d'une crainte. Le 20/09/2026, le banc des neuf idiomes a
produit deux pages — les seules que le modèle ait su écrire. **Les deux portaient le même
canonical faux** : il perdait le segment de section, `/comment-…` là où la page vit sous
`/gauntlet/comment-…`, pendant qu'`og:url` gardait le bon chemin. Deux sur deux n'est pas du
bruit.

LA CAUSE EST INSTRUCTIVE. La page sœur clonée était `canonical-404`, celle dont le défaut *est*
un canonical vers une 404. On demande au modèle d'imiter la FORME d'une sœur ; il en a imité le
défaut avec. Sur un vrai site les sœurs sont saines, mais « probablement saines » n'est pas une
garantie, et le rédacteur ne possédait aucun des garde-fous d'URL du correcteur.

ET LES DEUX BUILDS NETLIFY ÉTAIENT VERTS — quatre vérifications réussies chacun, la boucle de
vérification disait « promouvoir ». **Un build ne voit pas une adresse qui ment.** C'est la
leçon de la régression og:url du 19/09, revenue ailleurs : valider sur le sens, pas sur la
compilation.

LA DIFFÉRENCE AVEC LE CORRECTEUR, et c'est elle qui autorise à réparer plutôt qu'à s'abstenir :
partout ailleurs, deviner quelle page une balise devrait désigner est une interprétation, et
`_canonical_ecrit_dans` s'abstient dès qu'il hésite. Ici l'adresse a été **choisie** quelques
lignes plus haut. Il n'y a rien à interpréter.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-url-page-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402

BONNE = "https://site.fr/gauntlet/ma-page"

# Les deux fichiers sont ceux que le banc a réellement produits le 20/09/2026, réduits à leur
# tête. Une fixture inventée aurait mesuré ma supposition sur la forme du défaut.
NEXT_REEL = """export const metadata = {
  title: 'Ma page',
  alternates: {
    canonical: 'https://site.fr/ma-page',
  },
  openGraph: {
    type: 'article',
    url: 'https://site.fr/gauntlet/ma-page',
    images: ['https://site.fr/og.png'],
  },
};

export default function Page() { return <main><h1>Ma page</h1></main>; }
"""

HTML_REEL = """<!doctype html>
<html lang="fr">
  <head>
    <title>Ma page</title>
    <link rel="canonical" href="https://site.fr/ma-page" />
    <meta property="og:url" content="https://site.fr/gauntlet/ma-page" />
    <meta property="og:image" content="https://site.fr/og.png" />
  </head>
  <body><h1>Ma page</h1></body>
</html>
"""

# Nuxt déclare sa tête comme des objets dans `useHead()` : aucun `<link>` à trouver, et `rel`
# porte « canonical » comme VALEUR. C'est la quatrième écriture, mesurée le 15/09/2026.
NUXT = """<script setup>
useHead({
  link: [{ rel: 'canonical', href: 'https://site.fr/ma-page' }],
  meta: [{ property: 'og:url', content: 'https://site.fr/ailleurs' }],
})
</script>
<template><h1>Ma page</h1></template>
"""


def _canon(contenu: str) -> list[str]:
    return [contenu[d:f] for d, f, role in m._spans_url_de_page(contenu) if role == "canonical"]


def _ogurl(contenu: str) -> list[str]:
    return [contenu[d:f] for d, f, role in m._spans_url_de_page(contenu) if role == "og:url"]


# ── le défaut mesuré, dans les deux fichiers qui l'ont porté ─────────────────────────────────

@pytest.mark.parametrize("source", [NEXT_REEL, HTML_REEL], ids=["next-app", "static-html"])
def test_le_canonical_faux_du_banc_est_REMIS_a_la_bonne_adresse(source) -> None:
    sortie, notes = m._urls_de_la_page_neuve(source, url_de_la_page=BONNE)
    assert _canon(sortie) == [BONNE], sortie
    assert "https://site.fr/ma-page" not in sortie, "l'ancienne adresse survit quelque part"
    assert notes and "canonical" in notes[0], notes


@pytest.mark.parametrize("source", [NEXT_REEL, HTML_REEL], ids=["next-app", "static-html"])
def test_l_og_url_deja_juste_n_est_pas_touche(source) -> None:
    """Le témoin. Un garde-fou qui réécrit tout ne prouve rien : celui-ci doit laisser
    tranquille ce qui va bien, sinon son diff devient illisible."""
    sortie, notes = m._urls_de_la_page_neuve(source, url_de_la_page=BONNE)
    assert _ogurl(sortie) == [BONNE], sortie
    assert len(notes) == 1, notes


def test_une_page_DEJA_juste_ne_produit_aucun_diff() -> None:
    juste = HTML_REEL.replace("https://site.fr/ma-page", BONNE)
    sortie, notes = m._urls_de_la_page_neuve(juste, url_de_la_page=BONNE)
    assert sortie == juste and notes == [], notes


# ── les écritures que ce projet a rencontrées sur les neuf idiomes ───────────────────────────

def test_l_ecriture_par_OBJETS_de_nuxt_est_lue_et_reprise() -> None:
    """`{ rel: 'canonical', href }` dans `useHead()` : aucun `<link>`, et `rel` porte la valeur.
    Les deux motifs habituels passaient à côté — mesuré le 15/09/2026 sur le correcteur."""
    sortie, notes = m._urls_de_la_page_neuve(NUXT, url_de_la_page=BONNE)
    assert _canon(sortie) == [BONNE], sortie
    assert _ogurl(sortie) == [BONNE], sortie
    assert len(notes) == 2, notes


def test_une_valeur_ASSEMBLEE_n_est_jamais_touchee() -> None:
    """Elle est juste par construction, et c'est la forme que ce projet préfère : une URL
    écrite en dur n'est égale à la base du site que par coïncidence, et diverge le jour d'un
    changement de domaine ou sur un déploiement d'aperçu.

    LA FIXTURE EST UN GABARIT JEKYLL, pas un littéral gabarité JS, et c'est une correction.
    Ma première version mettait `` `${base}/x` `` : un littéral à accent grave, que le scanner
    ne voit même pas — il exige un guillemet simple ou double. Le test passait donc sans que
    `_VALEUR_ASSEMBLEE_RE` n'ait rien à faire, et la mutation qui la retire y a SURVÉCU.
    `{{ site.url }}` est lu par le scanner, et c'est bien la garde qui l'épargne.
    """
    source = HTML_REEL.replace('href="https://site.fr/ma-page"',
                               'href="{{ site.url }}/gauntlet/ma-page"')
    sortie, notes = m._urls_de_la_page_neuve(source, url_de_la_page=BONNE)
    assert "{{ site.url }}/gauntlet/ma-page" in sortie, sortie
    assert not any("canonical" in n for n in notes), notes


def test_ce_que_le_scanner_NE_VOIT_PAS_est_dit_ici() -> None:
    """La limite assumée, écrite pour qu'elle ne se découvre pas en production.

    Un littéral à accent grave (`` canonical: `${base}/x` ``) n'est pas lu : les motifs de ce
    projet exigent un guillemet simple ou double. Conséquence pratique : on n'y touche pas —
    ce qui est le bon geste, puisqu'une valeur calculée est juste par construction. Mais on ne
    la VÉRIFIE pas non plus, et une expression fausse à accent grave passerait.
    """
    source = NEXT_REEL.replace("'https://site.fr/ma-page'", "`${base}/ailleurs`")
    assert _canon(source) == [], "le scanner lit désormais les accents graves : mettre à jour"
    sortie, notes = m._urls_de_la_page_neuve(source, url_de_la_page=BONNE)
    assert sortie == source and not any("canonical" in n for n in notes), notes


def test_une_valeur_RELATIVE_reste_relative() -> None:
    """Imposer un style réécrirait la convention du client pour un gain nul : les deux
    résolvent vers la même adresse."""
    source = HTML_REEL.replace('href="https://site.fr/ma-page"', 'href="/ma-page"')
    sortie, _notes = m._urls_de_la_page_neuve(source, url_de_la_page=BONNE)
    assert _canon(sortie) == ["/gauntlet/ma-page"], sortie


def test_un_canonical_vers_un_AUTRE_HOTE_est_repris() -> None:
    """Le cas le plus grave et le plus silencieux : la page neuve déclarerait qu'elle est la
    copie d'une page d'un autre site."""
    source = HTML_REEL.replace("https://site.fr/ma-page", "https://concurrent.fr/ma-page")
    sortie, notes = m._urls_de_la_page_neuve(source, url_de_la_page=BONNE)
    assert "concurrent.fr" not in sortie, sortie
    assert notes, notes


def test_sans_adresse_connue_on_ne_touche_a_RIEN() -> None:
    """Le garde-fou ne tient que parce qu'on connaît la bonne réponse. Sans elle, il
    réécrirait à l'aveugle — ce que le correcteur se refuse à faire partout ailleurs."""
    sortie, notes = m._urls_de_la_page_neuve(HTML_REEL, url_de_la_page="")
    assert sortie == HTML_REEL and notes == []


# ── la garde est DANS le rédacteur, pas chez l'appelant ──────────────────────────────────────

def test_le_REDACTEUR_applique_la_garde_lui_meme(monkeypatch) -> None:
    """Un garde-fou qu'on peut oublier de brancher ne protège que les appelants dont on se
    souvient. Il vit donc dans `rediger_une_page`, comme `_refus_de_format`."""
    # La sœur est en Next, pas en HTML brut, et c'est une contrainte du jour : `_forme_dune_soeur`
    # ne sait lire que le front matter et un objet `metadata`. Une sœur en HTML rend « structure
    # illisible » avant même d'atteindre la garde — c'est le second défaut du banc du 20/09,
    # traité à part. Ce test-ci mesure la garde, pas le lecteur de forme.
    monkeypatch.setattr(m, "_correction_ai_json", lambda **kw: {"contenu": NEXT_REEL})
    notes: list[str] = []
    contenu, refus = m.rediger_une_page(
        sujet="Ma page", chemin="app/gauntlet/ma-page/page.tsx",
        soeur_chemin="app/gauntlet/soeur/page.tsx", soeur_contenu=NEXT_REEL,
        url_de_la_page=BONNE, notes=notes)
    assert refus == "", refus
    assert _canon(contenu) == [BONNE], contenu
    assert notes, "la correction n'est pas remontée à l'appelant"


def test_la_ROUTE_de_production_passe_bien_l_adresse() -> None:
    """La garde est inerte sans `url_de_la_page`, et rien dans le code ne le crierait : un
    appel qui l'oublie rend une page dont le canonical n'est plus vérifié du tout."""
    import ast

    source = Path(m.__file__).read_text(encoding="utf-8")
    arbre = ast.parse(source)
    sans_adresse = []
    for n in ast.walk(arbre):
        if isinstance(n, ast.Call) and ast.unparse(n.func) == "rediger_une_page":
            if not any(k.arg == "url_de_la_page" for k in n.keywords):
                sans_adresse.append(n.lineno)
    assert not sans_adresse, (
        "ces appels rédigent une page sans lui dire son adresse, donc sans garde-fou : %s"
        % ", ".join("app.py:%d" % ligne for ligne in sans_adresse))
