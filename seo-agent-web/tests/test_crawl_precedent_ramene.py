# -*- coding: utf-8 -*-
"""Le crawl precedent est ramene sur le disque du worker avant chaque crawl.

14/10/2026, easyshopbuilder. Crawl de 10:01, puis deux deploiements (le worker redemarre a vide :
il n'a pas de disque durable), puis fusion de 11 descriptions et crawl de 14:40. Ahrefs :
« Meta description changed » 11, « Changed pages not submitted to IndexNow » 11. Noyaru : 0,
sans un mot. Le crawler ne cherche son crawl precedent que dans les dossiers VOISINS du sien ;
le worker ne ramenait du stockage objet que `run.json`, jamais `report.json`. Tout le suivi
des changements s'eteignait apres chaque redemarrage.
"""
from __future__ import annotations

import inspect

from backend import app as m

CRAWLS = ["20261013-230401", "20261014-080000", "20261014-100108", "20261014-120000",
          "20261014-144057", "20261015-090000"]


def _ramener(monkeypatch, *, absents: set[str] = frozenset(), avant: str = "20261014-144057") -> tuple[list, list]:
    demandes: list[str] = []

    def ramener(chemin):
        ts = chemin.parts[-3]
        demandes.append(ts)
        assert chemin.parts[-2:] == ("audit", "report.json")
        return ts not in absents

    monkeypatch.setattr(m.dash, "list_project_crawls", lambda runs_dir, slug: list(CRAWLS))
    monkeypatch.setattr(m, "_ensure_runs_file_local", ramener)
    return m._ramener_les_crawls_precedents(m.Path("/runs"), "site", avant), demandes


def test_les_trois_derniers_rapports_anterieurs_sont_ramenes(monkeypatch) -> None:
    ramenes, demandes = _ramener(monkeypatch)

    assert ramenes == ["20261014-120000", "20261014-100108", "20261014-080000"]
    assert demandes == ramenes      # ni le crawl en cours, ni un plus recent, ni un quatrieme


def test_un_rapport_introuvable_n_arrete_pas_la_recherche(monkeypatch) -> None:
    ramenes, _ = _ramener(monkeypatch, absents={"20261014-100108"})

    assert ramenes == ["20261014-120000", "20261014-080000", "20261013-230401"]


def test_un_premier_crawl_n_a_rien_a_ramener(monkeypatch) -> None:
    assert _ramener(monkeypatch, avant="20261013-230401") == ([], [])


def test_une_liste_illisible_ne_bloque_pas_le_crawl(monkeypatch) -> None:
    def panne(*a):
        raise OSError("stockage objet injoignable")
    monkeypatch.setattr(m.dash, "list_project_crawls", panne)

    assert m._ramener_les_crawls_precedents(m.Path("/runs"), "site", "20261014-144057") == []


def test_le_job_de_crawl_ramene_AVANT_de_lancer_le_crawler() -> None:
    src = inspect.getsource(m._run_crawl_job)
    assert "_ramener_les_crawls_precedents(runs_dir, slug, timestamp)" in src
    assert src.index("_ramener_les_crawls_precedents(") < src.index('"--profile"')
