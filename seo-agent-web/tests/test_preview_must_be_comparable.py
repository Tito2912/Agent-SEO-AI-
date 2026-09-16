# -*- coding: utf-8 -*-
"""Un compteur a zero ne prouve rien tant qu'on n'a pas etabli qu'il y avait quoi compter.

Mesure du 15/09/2026, et c'est le defaut le plus grave trouve ce jour-la — dans l'INSTRUMENT,
pas dans le correcteur.

Une correction defectueuse avait casse cinq stacks sur neuf. Le deploiement de next-app a
ECHOUE : sa preview repondait 404 sur tout, `robots.txt` compris. Le crawl n'a ramene que 4 pages
au lieu de 43, la famille visee ne pouvait donc pas se declencher, son compteur valait zero — et
`verify_correction` a annonce « 1 -> 0 », c'est-a-dire REUSSITE, sur un site qui n'existait pas.
Sur hugo, le sitemap corrompu par la meme correction empechait la decouverte : 7 pages sur 52,
meme verdict triomphant.

Neuf stacks annoncees corrigees, cinq reellement abimees.

C'est la suite de la lecon du 11/09 — « un site qui REPOND n'est pas un site A JOUR » — dans une
forme plus sournoise : une preview qui repond 404 partout repond quand meme.
"""

from __future__ import annotations

import importlib.util
import json
import types
from pathlib import Path

SCRIPT = (Path(__file__).resolve().parents[1] / "ops" / "gauntlet" / "verify_correction.py")


def _module() -> types.ModuleType:
    """Charge le script sans executer son `main()`.

    Il a longtemps fallu RETIRER la ligne `raise SystemExit(main())` du source avant de
    l'executer, parce que le script la lancait au niveau module. Le 16/09/2026 il a recu une
    garde `if __name__ == "__main__":` — et cette chirurgie de chaine est alors devenue un piege :
    oter la ligne laissait un `if` suivi de ses seuls commentaires, donc une `IndentationError`.
    Un chargement ordinaire ne peut pas se casser de cette facon.
    """
    spec = importlib.util.spec_from_file_location("verif_correction", SCRIPT)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _rapport(tmp_path: Path, nom: str, pages: int) -> str:
    chemin = tmp_path / nom
    chemin.write_text(json.dumps({"pages": [{"url": "u%d" % i} for i in range(pages)],
                                  "issues": {}}), encoding="utf-8")
    return str(chemin)


def test_le_cas_mesure_une_preview_qui_ne_sert_rien(tmp_path):
    """next-app : 4 pages servies contre 43 a la reference, apres un build rouge."""
    mod = _module()
    ref = _rapport(tmp_path, "ref.json", 43)
    apres = _rapport(tmp_path, "apres.json", 4)
    assert mod._preview_comparable("next-app", ref, apres) is False


def test_un_sitemap_casse_qui_ampute_la_decouverte_est_vu_aussi(tmp_path):
    """hugo : 7 pages sur 52. L'ancien controle y annoncait « 1 -> 0 »."""
    mod = _module()
    assert mod._preview_comparable(
        "hugo", _rapport(tmp_path, "r.json", 52), _rapport(tmp_path, "a.json", 7)) is False


def test_une_preview_complete_passe(tmp_path):
    mod = _module()
    assert mod._preview_comparable(
        "static-html", _rapport(tmp_path, "r.json", 51), _rapport(tmp_path, "a.json", 47)) is True


def test_la_variation_normale_d_un_crawl_n_est_pas_une_alerte(tmp_path):
    """Une page lente ou une limite atteinte ne doit pas invalider un passage honnete."""
    mod = _module()
    assert mod._preview_comparable(
        "astro", _rapport(tmp_path, "r.json", 50), _rapport(tmp_path, "a.json", 34)) is True


def test_un_site_minuscule_reste_verifiable(tmp_path):
    """Le plancher absolu evite de bloquer un depot de deux pages."""
    mod = _module()
    assert mod._preview_comparable(
        "x", _rapport(tmp_path, "r.json", 2), _rapport(tmp_path, "a.json", 2)) is True


def test_une_preview_totalement_vide_est_refusee(tmp_path):
    mod = _module()
    assert mod._preview_comparable(
        "x", _rapport(tmp_path, "r.json", 40), _rapport(tmp_path, "a.json", 0)) is False
