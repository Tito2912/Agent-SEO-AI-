# -*- coding: utf-8 -*-
"""Un crawl borné dans le TEMPS rend ce qu'il a lu, au lieu d'être tué et de tout perdre.

Relevé le 28/09/2026 sur educatorsfinancialgroup.ca, analysé comme concurrent : ~18 s par page
(un hôte qui laisse pendre des requêtes), 100 pages frôlaient la borne de 30 minutes à laquelle
l'application TUE le processus. Un processus tué n'écrit pas `report.json` : les pages lues
étaient perdues, et le concurrent était déclaré « illisible, il bloque peut-être les robots ».

`--max-duration` arrête de LANCER des pages et laisse le rapport s'écrire. Un vrai crawl exige
Chromium, que la CI n'installe pas : la lecture d'une page est remplacée par une page lente,
tout le reste — file, rapport, métadonnées — est le vrai.
"""
from __future__ import annotations

import importlib.util
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
_SPEC = importlib.util.spec_from_file_location(
    "seo_audit_for_time_budget_tests",
    REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py",
)
assert _SPEC and _SPEC.loader
seo_audit = importlib.util.module_from_spec(_SPEC)
sys.modules["seo_audit_for_time_budget_tests"] = seo_audit
_SPEC.loader.exec_module(seo_audit)

BASE = "https://concurrent-lent.test/"


def _page_lente(url, config, rp, base_parts):
    """Chaque page met 0,2 s et mene a deux autres : une file qui ne s'epuise jamais."""
    time.sleep(0.2)
    n = int(url.rstrip("/").rsplit("p", 1)[-1]) if "/p" in url else 0
    return seo_audit.PageData(
        url=url, final_url=url, status_code=200, content_type="text/html",
        title=f"Page {n}", h1=[f"Page {n}"],
        internal_links=[f"{BASE}p{2 * n + 1}", f"{BASE}p{2 * n + 2}"])


def _crawler(monkeypatch, tmp_path, *extra):
    monkeypatch.setattr(seo_audit, "_extract_page", _page_lente)
    monkeypatch.setattr(seo_audit, "_shutdown_browser_sessions", lambda executor, workers: None)
    # robots.txt et sitemap echouent vite et sans consequence : `.test` ne se resout jamais.
    out = tmp_path / "out"
    debut = time.monotonic()
    seo_audit.main([BASE, "--max-pages", "500", "--workers", "3", "--timeout", "1",
                    "--output-dir", str(out), *extra])
    duree = time.monotonic() - debut
    return json.loads((out / "report.json").read_text(encoding="utf-8")), duree


def test_la_borne_ARRETE_le_crawl_et_le_rapport_s_ecrit(monkeypatch, tmp_path) -> None:
    report, duree = _crawler(monkeypatch, tmp_path, "--max-duration", "1")
    meta = report["meta"]
    assert meta["stopped_on_time_budget"] is True
    # 3 pages par lot de 0,2 s pendant ~1 s : une poignee, loin des 500 demandees, mais PAS zero.
    assert 3 <= meta["pages_crawled"] < 60, meta["pages_crawled"]
    titres = {p.get("title") for p in report["pages"]}
    assert "Page 0" in titres, "les pages lues sont DANS le rapport"
    assert duree < 30, duree


def test_sans_borne_rien_ne_change(monkeypatch, tmp_path) -> None:
    report, _ = _crawler(monkeypatch, tmp_path, "--max-pages", "9")
    assert report["meta"]["stopped_on_time_budget"] is False
    assert report["meta"]["pages_crawled"] == 9
