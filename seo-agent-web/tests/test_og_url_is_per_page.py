"""Le og:url d'une page ne doit jamais atterrir sur une autre.

Mesure sur la verification par previsualisation des neuf stacks (10/09/2026) :
`open_graph_url_not_matching_canonical` AUGMENTAIT apres correction — 2 -> 4 sur jekyll,
2 -> 5 sur sveltekit et next-pages, 2 -> 6 sur gatsby. Des pages sans aucun rapport ressortaient
toutes avec le meme og:url :

    /gauntlet/                            og:url -> .../gauntlet/canonical-http
    /gauntlet/hreflang-to-non-canonical   og:url -> .../gauntlet/canonical-http
    /gauntlet/mixed-image                 og:url -> .../gauntlet/canonical-http

y compris l'index du parcours, qui doit rester irreprochable.

Le reecriveur deterministe n'y est pour rien : il remplace une valeur qu'il a trouvee, donc il
ne touche que la bonne page. C'est le REPLI IA qui derape. Son intention est legitime — un
framework construit og:url dans du code, ou aucun litteral n'existe a remplacer — mais on lui
passe la paire d'UNE page, et rien dans le fichier qu'il edite ne lui dit a quelle page il
correspond. Il ecrit donc la valeur qu'on lui a montree, quelle que soit la page.

La bonne valeur est PAR PAGE. Quand il y en a plusieurs, le modele ne peut pas choisir : le
repli est coupe. Avec une seule paire il n'y a pas d'ambiguite, et le cas du framework reste
couvert.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")

from backend import app as app_module  # noqa: E402

PAIR_A = {"page": "https://s.fr/a", "from": "https://s.fr/a", "to": "https://s.fr/canonique"}
PAIR_B = {"page": "https://s.fr/b", "from": "https://s.fr/b", "to": "https://s.fr/autre"}


def test_a_single_pair_still_allows_the_fallback() -> None:
    """Le cas du framework : une page, une valeur, aucune ambiguite possible."""
    assert app_module._og_fallback_allowed([PAIR_A]) is True


def test_several_pairs_cut_the_fallback() -> None:
    """Le modele ne peut pas savoir a quelle page correspond le fichier qu'il edite."""
    assert app_module._og_fallback_allowed([PAIR_A, PAIR_B]) is False


def test_no_pair_cuts_the_fallback() -> None:
    assert app_module._og_fallback_allowed([]) is False


def test_the_deterministic_rewriter_still_handles_several_pages() -> None:
    """Couper le repli ne doit rien retirer au reecriveur : lui trouve la valeur dans le
    fichier, donc il ne peut pas se tromper de page."""
    content = ('<meta property="og:url" content="https://s.fr/a" />\n'
               '<meta property="og:url" content="https://s.fr/b" />\n')
    out, n = app_module._rewrite_og_url(content, [PAIR_A, PAIR_B])
    assert n == 2
    assert "https://s.fr/canonique" in out and "https://s.fr/autre" in out


def test_a_page_absent_from_the_pairs_is_never_touched() -> None:
    """Le coeur du defaut : une page etrangere aux paires ressortait avec la valeur d'une autre."""
    content = '<meta property="og:url" content="https://s.fr/etrangere" />\n'
    out, n = app_module._rewrite_og_url(content, [PAIR_A, PAIR_B])
    assert n == 0 and out == content


def test_the_family_uses_the_guard() -> None:
    import inspect
    src = inspect.getsource(app_module._prepare_issue_fix)
    assert "_og_fallback_allowed(_og_pairs)" in src
