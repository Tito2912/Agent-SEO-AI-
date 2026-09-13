# -*- coding: utf-8 -*-
"""`og:url` ne s'ecrit pas qu'en balise : deux stacks sur neuf en font une clef d'objet.

Mesure du 13/09/2026, les neuf stacks, chaque famille isolee sur un etat vierge. La famille
`open_graph_url_not_matching_canonical` ne produisait AUCUN patch sur next-app et nuxt, et sur
elles seules. La cause n'est pas le ciblage — les six bons fichiers etaient trouves — mais
l'ecriture visee : `_OG_URL_TAG_RE` ne reconnait qu'un `<meta property="og:url">`. Or

    next-app :  openGraph: { ..., url: 'https://.../canonical-http', ... }
    nuxt     :  meta: [ { property: 'og:url', content: 'https://.../canonical-http' } ]

ne contiennent pas une seule balise. Le reecriveur rendait 0, et au-dela d'une paire le repli IA
est interdit (`_og_fallback_allowed` : la bonne valeur est PAR PAGE) — donc il ne se passait
rien du tout. Ni patch, ni refus, ni trace : le genre de silence qui se lit « corrige » dans un
tableau de verdicts.

Ce qui reste interdit ici vaut d'etre teste autant que ce qui est ajoute : `twitter: { url }` et
`alternates: { canonical }` ne sont pas cette famille.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "x" * 20)

from backend import app as m  # noqa: E402

FAUX = "https://exemple.test/gauntlet/canonical-http"
VRAI = "https://exemple.test/gauntlet/reference"
PAIRES = [{"page": FAUX, "from": FAUX, "to": VRAI}]

NUXT = (
    "useHead({\n"
    "  title: 'Canonical http',\n"
    "  meta: [\n"
    "      { property: 'og:url', content: '" + FAUX + "' },\n"
    "      { property: 'og:image', content: 'https://exemple.test/og.png' }\n"
    "  ],\n"
    "  link: [\n"
    "      { rel: 'canonical', href: '" + VRAI + "' }\n"
    "  ],\n"
    "});\n"
)

NEXT_APP = (
    "export const metadata = {\n"
    "  alternates: {\n"
    "    canonical: '" + VRAI + "',\n"
    "  },\n"
    "  openGraph: {\n"
    "    type: 'article',\n"
    "    url: '" + FAUX + "',\n"
    "    images: ['https://exemple.test/og.png'],\n"
    "  },\n"
    "  twitter: {\n"
    "    card: 'summary_large_image',\n"
    "    url: '" + FAUX + "',\n"
    "  },\n"
    "};\n"
)


def test_la_forme_nuxt_est_reecrite():
    rendu, n = m._rewrite_og_url(NUXT, PAIRES)
    assert n == 1
    assert "{ property: 'og:url', content: '" + VRAI + "' }" in rendu


def test_la_forme_next_app_est_reecrite():
    rendu, n = m._rewrite_og_url(NEXT_APP, PAIRES)
    assert n == 1
    assert "    url: '" + VRAI + "',\n    images:" in rendu


def test_le_bloc_twitter_n_est_pas_touche():
    """La famille parle d'UNE balise. `twitter:url` est une autre balise."""
    rendu, _ = m._rewrite_og_url(NEXT_APP, PAIRES)
    apres_twitter = rendu.split("twitter: {", 1)[1]
    assert "url: '" + FAUX + "'" in apres_twitter


def test_le_canonical_n_est_pas_touche():
    rendu, _ = m._rewrite_og_url(NEXT_APP, PAIRES)
    assert "canonical: '" + VRAI + "'" in rendu


def test_un_openGraph_en_ligne_est_traite_sans_ouvrir_le_scanner():
    """Un bloc referme sur sa propre ligne ne doit pas laisser le scanner courir jusqu'au bas
    du fichier : il y trouverait le `url:` du bloc twitter."""
    source = (
        "export const metadata = {\n"
        "  openGraph: { type: 'article', url: '" + FAUX + "' },\n"
        "  twitter: {\n"
        "    url: '" + FAUX + "',\n"
        "  },\n"
        "};\n"
    )
    rendu, n = m._rewrite_og_url(source, PAIRES)
    assert n == 1
    assert "openGraph: { type: 'article', url: '" + VRAI + "' }" in rendu
    assert "    url: '" + FAUX + "',\n" in rendu


def test_une_valeur_non_signalee_reste_en_place():
    """Rien n'est reecrit sur la foi de l'emplacement : seule une valeur mesuree l'est."""
    autre = NEXT_APP.replace(FAUX, "https://exemple.test/une-autre-page")
    assert m._rewrite_og_url(autre, PAIRES) == (autre, 0)


def test_la_balise_html_marche_toujours():
    source = '<meta property="og:url" content="' + FAUX + '" />\n'
    rendu, n = m._rewrite_og_url(source, PAIRES)
    assert n == 1 and VRAI in rendu


def test_sans_paire_le_contenu_ne_bouge_pas():
    assert m._rewrite_og_url(NEXT_APP, []) == (NEXT_APP, 0)


def test_une_valeur_fraichement_ecrite_n_est_pas_relue():
    """Le defaut mesure sur la preview #17 de next-app, le 13/09/2026.

    Deux pages du parcours forment une chaine : le og:url fautif de `canonical-other` doit
    devenir `canonical-relay`, qui est LUI-MEME le og:url fautif de la page `canonical-relay`.
    Tant que les passes se succedaient sur un contenu deja reecrit, la seconde relisait la
    valeur que la premiere venait d'ecrire et la remplacait a son tour : `canonical-other`
    repartait avec `.../missing-h1`, l'URL d'une page sans aucun rapport.
    """
    autre = "https://exemple.test/gauntlet/canonical-other"
    relais = "https://exemple.test/gauntlet/canonical-relay"
    cible = "https://exemple.test/gauntlet/missing-h1"
    chaine = [{"page": autre, "from": autre, "to": relais},
              {"page": relais, "from": relais, "to": cible}]
    source = (
        "export const metadata = {\n"
        "  openGraph: {\n"
        "    type: 'article',\n"
        "    url: '" + autre + "',\n"
        "  },\n"
        "};\n"
    )
    rendu, n = m._rewrite_og_url(source, chaine)
    assert n == 1
    assert "url: '" + relais + "'" in rendu
    assert cible not in rendu
