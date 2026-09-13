# -*- coding: utf-8 -*-
"""Le correcteur doit appeler « different » ce que le crawler a appele « different ».

Mesure du 13/09/2026, next-app : la famille `open_graph_url_not_matching_canonical` signalait
QUATRE pages et le correcteur n'en tirait que DEUX paires. Les deux manquantes etaient
precisement celles dont l'ecart tenait au slash final (`/blog` face a `/blog/`) et au schema
(`https://` face a `http://`) — les deux choses que `_norm_url_for_match` efface, puisqu'elle
sert a RECONNAITRE une page d'un rapport a l'autre, pas a decider si deux valeurs different.

Consequence pour un client : deux pages affichees en anomalie qu'aucune correction ne pouvait
atteindre, sans le moindre message.
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

S = "https://exemple.test"


def _page(url: str, og: str, canonical: str) -> dict[str, str]:
    return {"url": url, "og_url": og, "canonical": canonical}


def test_le_slash_final_est_une_vraie_difference():
    pages = [_page(S + "/blog", S + "/blog", S + "/blog/")]
    paires = m._og_url_pairs_from_pages([S + "/blog"], pages)
    assert paires == [{"page": S + "/blog", "from": S + "/blog", "to": S + "/blog/"}]


def test_un_canonical_en_clair_ne_se_recopie_pas_dans_og_url():
    """Le defaut de cette page est son canonical. L'aligner dessus ecrirait du http.

    `canonical_from_https_to_http` s'en charge, et la famille se resout ensuite d'elle-meme.
    """
    p = S + "/gauntlet/canonical-http"
    pages = [_page(p, p, p.replace("https://", "http://"))]
    assert m._og_url_pairs_from_pages([p], pages) == []


def test_une_page_vraiment_conforme_ne_donne_pas_de_paire():
    pages = [_page(S + "/a", S + "/a", S + "/a")]
    assert m._og_url_pairs_from_pages([S + "/a"], pages) == []


def test_les_variantes_d_une_meme_page_ne_comptent_qu_une_fois():
    """Un rapport liste `/blog` ET `/blog/` : deux entrees, un seul defaut a corriger.

    Le compte de paires decide du repli IA (`_og_fallback_allowed`), donc un doublon ne serait
    pas anodin : il ferait passer une page seule pour deux.
    """
    pages = [_page(S + "/blog", S + "/blog", S + "/blog/"),
             _page(S + "/blog/", S + "/blog", S + "/blog/")]
    assert len(m._og_url_pairs_from_pages([S + "/blog"], pages)) == 1


def test_une_page_non_signalee_reste_hors_du_lot():
    pages = [_page(S + "/blog", S + "/blog", S + "/blog/"),
             _page(S + "/autre", S + "/autre", S + "/autre/")]
    paires = m._og_url_pairs_from_pages([S + "/blog"], pages)
    assert [p["page"] for p in paires] == [S + "/blog"]


def test_une_page_signalee_ecrite_avec_un_slash_est_bien_reconnue():
    """La reconnaissance de la page, elle, doit rester insensible au slash final."""
    pages = [_page(S + "/blog/", S + "/blog", S + "/blog/")]
    assert len(m._og_url_pairs_from_pages([S + "/blog"], pages)) == 1
