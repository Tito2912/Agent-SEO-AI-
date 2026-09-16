"""Le verificateur du banc ne doit pas compter deux fois la meme page.

Le crawler emet, pour UNE page, la famille generique ET sa variante d'indexabilite :
`meta_description_too_short` = 1 et `meta_description_too_short_indexable` = 1 designent la meme
page. `comptes()` les additionnait.

Mesure du 16/09/2026, nuxt : le controle annoncait la famille des descriptions en regression de
5 a 9. En pages distinctes elle allait de 3 a 5. La regression etait REELLE — l'instrument
l'affichait deux fois trop grande. Un instrument qui exagere fait chercher un defaut la ou il
n'y en a pas, exactement comme un instrument qui minimise en cache un.

Ces tests existent aussi parce que `verify_correction` n'en avait aucun : il executait `main()`
des l'import, donc rien ne pouvait le charger.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
_SRC = WEB_ROOT / "ops" / "gauntlet" / "verify_correction.py"
_spec = importlib.util.spec_from_file_location("verif_banc", _SRC)
assert _spec and _spec.loader
verif = importlib.util.module_from_spec(_spec)
sys.modules["verif_banc"] = verif
_spec.loader.exec_module(verif)


_compteur = iter(range(10_000))


def _rapport(tmp_path: Path, issues: dict[str, int]) -> str:
    p = tmp_path / ("report-%d.json" % next(_compteur))
    p.write_text(json.dumps({"pages": [], "issues": {k: {"count": v} for k, v in issues.items()}}),
                 encoding="utf-8")
    return str(p)


def test_une_famille_et_sa_variante_designent_les_memes_pages(tmp_path: Path) -> None:
    c = verif.comptes(_rapport(tmp_path, {
        "meta_description_too_short": 1,
        "meta_description_too_short_indexable": 1,
    }))
    assert c["description"] == 1, c


def test_les_deux_variantes_d_indexabilite_ne_s_additionnent_pas(tmp_path: Path) -> None:
    """3 indexables + 2 non indexables = 5 pages, pas 10."""
    c = verif.comptes(_rapport(tmp_path, {
        "meta_description_too_short": 5,
        "meta_description_too_short_indexable": 3,
        "meta_description_too_short_not_indexable": 2,
    }))
    assert c["description"] == 5, c


def test_des_familles_DIFFERENTES_repliees_ensemble_s_additionnent(tmp_path: Path) -> None:
    """« Trop courte » et « absente » parlent de pages distinctes : la somme est juste ici.

    C'est ce qui distingue les deux niveaux de repli, et c'est pour l'avoir confondu que le
    compte etait faux.
    """
    c = verif.comptes(_rapport(tmp_path, {
        "meta_description_too_short": 4,
        "meta_description_too_short_not_indexable": 4,
        "missing_meta_description": 1,
    }))
    assert c["description"] == 5, c


def test_le_cas_nuxt_du_16_09_rend_le_bon_ecart(tmp_path: Path) -> None:
    """Les rapports reels qui m'avaient fait annoncer 5 -> 9."""
    avant = verif.comptes(_rapport(tmp_path, {
        "meta_description_too_long": 1, "meta_description_too_long_not_indexable": 1,
        "meta_description_too_short": 1, "meta_description_too_short_indexable": 1,
        "missing_meta_description": 1,
    }))
    apres = verif.comptes(_rapport(tmp_path, {
        "meta_description_too_short": 4, "meta_description_too_short_not_indexable": 4,
        "missing_meta_description": 1,
    }))
    assert (avant["description"], apres["description"]) == (3, 5), (avant, apres)


def test_une_famille_sans_variante_reste_intacte(tmp_path: Path) -> None:
    c = verif.comptes(_rapport(tmp_path, {"open_graph_url_not_matching_canonical": 5}))
    assert c["open_graph_url_not_matching_canonical"] == 5, c
