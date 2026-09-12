# -*- coding: utf-8 -*-
"""Dans un `images:` d'objet social, la clef de l'URL est `url`, jamais `src`.

Mesure du 12/09/2026, next-app, cycle complet puis recrawl des previews. Chargee d'ajouter un
texte alternatif, la correction a transforme :

    images: ['https://…/og.png']
    images: [{ src: 'https://…/og.png', alt: '…' }]

L'intention est bonne — un alt est un vrai progres — mais l'API de metadonnees de Next attend
`url`. Avec `src`, elle ne resout pas l'image et n'emet AUCUN `og:image` ni `twitter:image`. Le
site se construit, la page est servie, et les images sociales ont disparu : `twitter_card_incomplete`
est passee de 3 a 5 et `open_graph_tags_incomplete` de 4 a 5, sur des pages qui allaient tres
bien avant qu'on les corrige.

Le piege de mesure vaut d'etre retenu : le fichier SOURCE gardait ses trois clefs. La perte
n'etait visible qu'a la construction, donc seulement sur la page servie.
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

AVANT = """export const metadata = {
  openGraph: {
    title: 'T',
    images: ['https://exemple.test/og.png'],
  },
  twitter: {
    card: 'summary_large_image',
    images: ['https://exemple.test/og.png'],
  },
};
"""


def _avec(images: str) -> str:
    return AVANT.replace("images: ['https://exemple.test/og.png'],", images)


def test_le_cas_mesure_sur_next_app():
    apres = _avec("images: [{ src: 'https://exemple.test/og.png', alt: 'Une image' }],")
    rendu, notes = m._repair_social_image_key(apres, AVANT)
    assert "src:" not in rendu
    assert rendu.count("url: 'https://exemple.test/og.png'") == 2
    assert "alt: 'Une image'" in rendu
    assert len(notes) == 2


def test_une_clef_url_correcte_n_est_pas_touchee():
    apres = _avec("images: [{ url: 'https://exemple.test/og.png', alt: 'Une image' }],")
    assert m._repair_social_image_key(apres, AVANT) == (apres, [])


def test_la_forme_chaine_simple_reste_intacte():
    assert m._repair_social_image_key(AVANT, AVANT) == (AVANT, [])


def test_un_src_hors_bloc_social_est_laisse_tranquille():
    page = '<main>\n  <img src="/photo.png" alt="x" />\n</main>\n'
    assert m._repair_social_image_key(page, AVANT) == (page, [])


def test_un_src_ailleurs_dans_le_bloc_social_est_laisse_tranquille():
    """On ne corrige que la ligne `images:` — ailleurs, `src` peut vouloir dire autre chose."""
    apres = AVANT.replace("    title: 'T',", "    title: 'T',\n    autre: { src: 'x' },")
    assert m._repair_social_image_key(apres, AVANT) == (apres, [])
