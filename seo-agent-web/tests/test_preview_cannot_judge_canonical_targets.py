"""Une preview ne peut pas juger une famille dont la detection RESOUT une cible.

Le canonical et le hreflang d'une page du banc restent en ABSOLU vers l'hote de production — c'est
voulu, changer ces valeurs changerait l'anomalie. Sur une preview, `page_by_any.get(canon)` ne
trouve donc jamais la page cible, et toute famille qui en depend tombe a zero.

Mesure du 17/09/2026 sur les neuf stacks, production contre preview :

    canonical_points_to_4xx                         9  ->  0
    canonical_points_to_redirect                   18  ->  0
    non_canonical_page_specified_as_canonical_one   9  ->  0
    hreflang_to_non_canonical                      16  ->  0
    hreflang_to_redirect_or_broken_page             1  ->  0
                                                   53  ->  0

Pas une seule survivante. `verify_correction` lisait ces zeros comme des corrections et creditait
ces familles a chaque cycle.

C'est le defaut du 15/09 sous une forme plus sournoise. Alors, la preview repondait 404 partout et
le garde-fou de comparabilite a pu l'attraper. ICI LA PREVIEW EST SAINE : elle sert toutes ses
pages, elle passe la comparabilite — et elle ment quand meme, sur ces familles-la seulement.

Un compteur a zero ne veut rien dire tant qu'on n'a pas etabli qu'il y avait quelque chose a
compter ; et un instrument qui sait dire « je ne sais pas » vaut mieux qu'un instrument toujours
affirmatif.
"""

from __future__ import annotations

import importlib.util
import json
import types
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = WEB_ROOT / "ops" / "gauntlet" / "verify_correction.py"


def _module() -> types.ModuleType:
    spec = importlib.util.spec_from_file_location("verif_cibles", SCRIPT)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


verif = _module()

_compteur = iter(range(10_000))


def _rapport(tmp_path: Path, issues: dict[str, int], pages: int = 40) -> str:
    p = tmp_path / ("report-%d.json" % next(_compteur))
    p.write_text(json.dumps({
        "pages": [{"url": "u%d" % i} for i in range(pages)],
        "issues": {k: {"count": v} for k, v in issues.items()},
    }), encoding="utf-8")
    return str(p)


def test_les_familles_a_cible_resolue_sont_nommees() -> None:
    """Les cinq qui se sont tues sur les neuf previews, plus leurs voisines de meme mecanique."""
    for k in ("canonical_points_to_4xx", "canonical_points_to_redirect",
              "non_canonical_page_specified_as_canonical_one", "hreflang_to_non_canonical",
              "hreflang_to_redirect_or_broken_page"):
        assert k in verif._FAMILLES_A_CIBLE_RESOLUE, k


def test_une_famille_ORDINAIRE_reste_jugee() -> None:
    """Le garde-fou doit rester etroit : hors de cette liste, rien ne change."""
    assert "missing_alt_text" not in verif._FAMILLES_A_CIBLE_RESOLUE
    assert "open_graph_url_not_matching_canonical" not in verif._FAMILLES_A_CIBLE_RESOLUE


def test_le_repli_d_indexabilite_ne_fait_pas_echapper_une_famille_muette() -> None:
    """`canonical_points_to_4xx_indexable` doit etre reconnue comme la meme famille.

    Sans cela, la variante d'indexabilite passerait a travers la liste et continuerait d'etre
    creditee — le trou serait simplement deplace.
    """
    assert verif.base("canonical_points_to_4xx_indexable") in verif._FAMILLES_A_CIBLE_RESOLUE
    assert verif.base("hreflang_to_non_canonical_not_indexable") in verif._FAMILLES_A_CIBLE_RESOLUE


def test_le_crawl_de_verification_passe_TOUJOURS_l_alias_d_hote() -> None:
    """C'est ce qui rend ces familles jugeables — sans lui, le faux succes revient.

    L'alias est lu dans le source plutot que par un appel : la commande de crawl lance un
    sous-processus, et ce qui compte ici est qu'elle NE PUISSE PAS etre construite sans lui.
    """
    source = SCRIPT.read_text(encoding="utf-8")
    debut = source.index("cmd = [sys.executable")
    commande = source[debut:source.index("]", debut) + 1]
    assert "--canonical-host-alias" in commande, commande
    assert "noyaru-stack-%s.netlify.app" in commande, (
        "l'alias doit nommer l'hote de production de la stack crawlee")
