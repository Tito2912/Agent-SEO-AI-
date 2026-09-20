# -*- coding: utf-8 -*-
"""Lire la tête d'une page sœur, quelle que soit la façon dont elle la déclare.

MESURE DU 20/09/2026, banc des neuf idiomes. La rédaction de contenu a refusé **six stacks sur
neuf** avec « structure illisible dans … : on ne sait pas quelle forme doit avoir la page
neuve ». Le refus était faux : les six pages étaient parfaitement lisibles. Personne n'avait
regardé comment elles écrivaient leur tête.

Le lecteur ne connaissait que deux écritures — un front matter, et un objet `metadata` de Next
App Router. Or les neuf dépôts n'en utilisent que **quatre** en tout :

    front matter        hugo (`+++`), jekyll (`---`)
    objet `metadata`    next-app
    BALISES             static-html, astro, next-pages, gatsby, sveltekit
    OBJETS `useHead`    nuxt

Deux lectures manquaient, pas six. Et c'est la faute de méthode que ce projet répète : j'ai
écrit un **nouveau** lecteur d'après les idiomes que j'avais sous les yeux, alors que le
correcteur en possède un, élargi le 15/09 pour `useHead` et le 16/09 pour Nuxt, précisément
parce qu'il s'était déjà fait prendre de la même manière.

LE CAS ASTRO EST À PART, et c'est le plus sournois : ses bornes `---` encadrent du JavaScript,
pas du YAML. Le lecteur entrait dans la branche « front matter », n'en tirait aucune clé, et
rendait la main — alors que la page portait sa tête en balises quinze lignes plus bas. Un
lecteur qui s'arrête à la première branche qui *ressemble* ne lit pas, il devine.

Les fixtures ci-dessous sont les têtes réelles des sœurs du banc, réduites.
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

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-tete-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402

_TETE_BALISES = """    <meta name="viewport" content="width=device-width" />
    <title>Page de test</title>
    <meta name="description" content="Une page du parcours." />
    <link rel="canonical" href="https://exemple.fr/page-absente" />
    <meta property="og:title" content="Page de test" />
    <meta property="og:url" content="https://exemple.fr/gauntlet/page" />
"""

STATIC_HTML = ('<!doctype html>\n<html lang="fr">\n  <head>\n' + _TETE_BALISES
               + "  </head>\n  <body><h1>Page</h1></body>\n</html>\n")

ASTRO = ("---\n// FAMILLE VISEE : canonical_points_to_4xx\n---\n"
         '<!doctype html>\n<html lang="fr">\n  <head>\n' + _TETE_BALISES
         + "  </head>\n  <body><h1>Page</h1></body>\n</html>\n")

NEXT_PAGES = ("import Head from 'next/head';\n\nexport default function Page() {\n"
              "  return (\n    <main>\n      <Head>\n" + _TETE_BALISES
              + "      </Head>\n      <h1>Page</h1>\n    </main>\n  );\n}\n")

GATSBY = ("import * as React from 'react';\n\nexport default function Page() {\n"
          "  return <main><h1>Page</h1></main>;\n}\n\n"
          "export function Head() {\n  return (\n    <>\n" + _TETE_BALISES
          + "    </>\n  );\n}\n")

SVELTEKIT = "<script></script>\n\n<svelte:head>\n" + _TETE_BALISES + "</svelte:head>\n<h1>Page</h1>\n"

NUXT = """<script setup>
useHead({
    title: 'Page de test',
    htmlAttrs: { lang: 'fr' },
    link: [{ rel: 'canonical', href: 'https://exemple.fr/page-absente' }],
    meta: [
      { name: 'viewport', content: 'width=device-width' },
      { name: 'description', content: 'Une page du parcours.' },
      { property: 'og:title', content: 'Page de test' },
      { property: 'og:url', content: 'https://exemple.fr/gauntlet/page' },
    ],
})
</script>
<template><h1>Page</h1></template>
"""

JEKYLL = """---
layout: gauntlet
permalink: /gauntlet/page
raw_head: |
  <title>Page de test</title>
---
"""

NEXT_APP = """export const metadata = {
  title: 'Page de test',
  description: 'Une page du parcours.',
};

export default function Page() { return <main><h1>Page</h1></main>; }
"""

BALISEES = [
    ("static-html", STATIC_HTML, "gauntlet/page.html"),
    ("astro", ASTRO, "src/pages/gauntlet/page.astro"),
    ("next-pages", NEXT_PAGES, "pages/gauntlet/page.js"),
    ("gatsby", GATSBY, "src/pages/gauntlet/page.js"),
    ("sveltekit", SVELTEKIT, "src/routes/gauntlet/page/+page.svelte"),
    ("nuxt", NUXT, "pages/gauntlet/page.vue"),
]


# ── les six idiomes qui étaient refusés ───────────────────────────────────────────────────────

@pytest.mark.parametrize("nom, source, chemin", BALISEES, ids=[n for n, _s, _c in BALISEES])
def test_les_six_idiomes_REFUSES_sont_desormais_lus(nom, source, chemin) -> None:
    forme = m._forme_dune_soeur(source, chemin)
    assert forme["cles"], "« structure illisible » sur une page parfaitement lisible"
    assert forme["bornes"] == "balises", forme


@pytest.mark.parametrize("nom, source, chemin", BALISEES, ids=[n for n, _s, _c in BALISEES])
def test_le_CANONICAL_de_la_soeur_est_reclame_a_la_page_neuve(nom, source, chemin) -> None:
    """La clé qui compte le plus : une sœur qui porte un canonical impose à sa cadette d'en
    porter un. C'est ce qui fait entrer la page neuve dans le garde-fou d'adresse."""
    assert "link:canonical" in m._forme_dune_soeur(source, chemin)["cles"]


@pytest.mark.parametrize("nom, source, chemin", BALISEES, ids=[n for n, _s, _c in BALISEES])
def test_le_TITRE_de_la_soeur_est_reclame_lui_aussi(nom, source, chemin) -> None:
    """Rien ne le vérifiait, et la mutation qui retirait la lecture du `<title>` a SURVÉCU :
    la porte d'entrée accepte aussi `description` et `link:canonical`, donc la tête restait
    « lisible » sans son titre. Une page neuve sans titre serait alors acceptée — et
    `missing_title` est une famille que le crawler signale."""
    assert "title" in m._forme_dune_soeur(source, chemin)["cles"]


@pytest.mark.parametrize("nom, source, chemin", BALISEES, ids=[n for n, _s, _c in BALISEES])
def test_les_balises_OUVERTES_AU_PARTAGE_sont_reclamees_aussi(nom, source, chemin) -> None:
    """Une sœur qui porte des balises Open Graph et une cadette qui n'en porte pas, c'est une
    régression que le crawler signalerait au passage suivant."""
    cles = m._forme_dune_soeur(source, chemin)["cles"]
    assert "og:title" in cles and "og:url" in cles, cles


# ── le cas Astro, celui qui s'arrêtait à la première branche ─────────────────────────────────

def test_un_front_matter_qui_ne_donne_AUCUNE_cle_ne_clot_pas_la_lecture() -> None:
    """Astro borne du JavaScript avec `---`. Le lecteur entrait dans la branche « front
    matter », n'en tirait rien, et rendait la main sur une page dont la tête était quinze
    lignes plus bas. Un lecteur qui s'arrête à la première branche qui RESSEMBLE ne lit pas."""
    assert m._front_matter_span(ASTRO.split("\n")), "la fixture ne pose plus le piège"
    assert m._forme_dune_soeur(ASTRO, "x.astro")["bornes"] == "balises"


# ── ce qui ne doit PAS changer ────────────────────────────────────────────────────────────────

def test_un_vrai_front_matter_garde_la_priorite() -> None:
    """Chez Jekyll et Hugo, les clés de tête sont STRUCTURELLES : une `layout` manquante casse
    le build. Les remplacer par les balises trouvées dans `raw_head` perdrait la seule
    vérification qui protège vraiment ces deux stacks."""
    forme = m._forme_dune_soeur(JEKYLL, "gauntlet/page.html")
    assert forme["bornes"] == "---"
    assert "layout" in forme["cles"] and "permalink" in forme["cles"], forme


def test_l_objet_metadata_garde_la_priorite() -> None:
    forme = m._forme_dune_soeur(NEXT_APP, "app/gauntlet/page/page.tsx")
    assert forme["bornes"] == "metadata" and "title" in forme["cles"], forme


def test_un_fragment_SANS_tete_reste_illisible() -> None:
    """Le témoin, et il porte un `viewport` exprès : n'importe quel bout de gabarit en a un.
    Sans signal éditorial — titre, description, canonical — on ne lit pas une tête de page, et
    dire le contraire ferait écrire un fichier dont la structure ne ressemble à rien."""
    fragment = ('<div class="carte">\n  <meta name="viewport" content="width=device-width" />\n'
                "  <p>Un bout de gabarit.</p>\n</div>\n")
    assert m._forme_dune_soeur(fragment, "src/components/Carte.astro")["cles"] == []


def test_une_page_VIDE_reste_illisible() -> None:
    assert m._forme_dune_soeur("", "x.html")["cles"] == []
    assert m._forme_dune_soeur("   \n\n", "x.html")["cles"] == []


# ── bout en bout : la page neuve doit reproduire ce que la sœur portait ──────────────────────

def test_une_page_neuve_qui_OUBLIE_le_canonical_de_sa_soeur_est_refusee(monkeypatch) -> None:
    """La vérification sert à quelque chose sur ces six idiomes aussi, pas seulement à les
    laisser passer. Sinon élargir le lecteur reviendrait à retirer un contrôle."""
    sans_canonical = STATIC_HTML.replace(
        '    <link rel="canonical" href="https://exemple.fr/page-absente" />\n', "")
    monkeypatch.setattr(m, "_correction_ai_json", lambda **kw: {"contenu": sans_canonical})
    contenu, refus = m.rediger_une_page(
        sujet="Ma page", chemin="gauntlet/ma-page.html",
        soeur_chemin="gauntlet/soeur.html", soeur_contenu=STATIC_HTML,
        url_de_la_page="https://exemple.fr/gauntlet/ma-page")
    assert contenu == "" and "link:canonical" in refus, refus


def test_une_page_neuve_SANS_TITRE_est_refusee(monkeypatch) -> None:
    """Le pendant comportemental : `missing_title` est une famille du crawler, et écrire nous-
    mêmes une page qui la déclenche serait le pire des services."""
    sans_titre = STATIC_HTML.replace("<title>Page de test</title>", "")
    monkeypatch.setattr(m, "_correction_ai_json", lambda **kw: {"contenu": sans_titre})
    contenu, refus = m.rediger_une_page(
        sujet="Ma page", chemin="gauntlet/ma-page.html",
        soeur_chemin="gauntlet/soeur.html", soeur_contenu=STATIC_HTML,
        url_de_la_page="https://exemple.fr/gauntlet/ma-page")
    assert contenu == "" and "title" in refus, refus


def test_une_page_neuve_COMPLETE_passe_et_voit_son_adresse_reprise(monkeypatch) -> None:
    """Les deux gardes s'enchaînent : la sœur est lue, la page neuve est acceptée, et son
    canonical — recopié du défaut de la sœur — est remis à la bonne adresse."""
    monkeypatch.setattr(m, "_correction_ai_json", lambda **kw: {"contenu": STATIC_HTML})
    notes: list[str] = []
    contenu, refus = m.rediger_une_page(
        sujet="Ma page", chemin="gauntlet/ma-page.html",
        soeur_chemin="gauntlet/soeur.html", soeur_contenu=STATIC_HTML,
        url_de_la_page="https://exemple.fr/gauntlet/ma-page", notes=notes)
    assert refus == "", refus
    assert "https://exemple.fr/page-absente" not in contenu, contenu
    assert 'href="https://exemple.fr/gauntlet/ma-page"' in contenu, contenu
    assert notes, notes
