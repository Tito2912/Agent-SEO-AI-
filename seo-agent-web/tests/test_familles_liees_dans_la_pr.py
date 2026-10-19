# -*- coding: utf-8 -*-
"""Une famille sans correctif propre, mais dont les URL sont celles d'une famille corrigee dans la
MEME PR, ne doit pas s'annoncer « resteront signalees ».

PR #1 de cityscootlab, 15/10/2026 : la PR affichait « ❌ Sitemap : URL en 3XX » et « ❌ Hreflang
vers redirect … Ces pages resteront signalees au prochain crawl ». Le canonical de /de /es /fr,
repare dans la meme PR (`/de/` -> `/de`), les a regles au crawl suivant : Next construit le
sitemap et les hreflang a partir du canonical. Les trois familles portaient les MEMES paires
« de -> vers ». Le correcteur ne sait pas si un site derive l'un de l'autre ; il sait que les URL
sont les memes, et il le dit au lieu de promettre un echec.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-familles-liees-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402

B = "https://cityscootlab.com"
PAIRES = [[f"{B}/{l}/", f"{B}/{l}"] for l in ("de", "es", "fr")]


def _canonical(ok: bool = True, paires=None) -> dict:
    return {"issue_key": "canonical_points_to_redirect", "issue_label": "Canonical → redirection",
            "ok": ok, "files": ["content/de/index.mdx"] if ok else [], "paires": paires or PAIRES,
            "error": "" if ok else "Aucun patch appliqué"}


def _sitemap(paires=None) -> dict:
    return {"issue_key": "sitemap_3xx_redirect", "issue_label": "Sitemap : URL en 3XX", "ok": False,
            "error": "Aucune destination canonical HTML 200 indexable verifiee pour ces entrees du sitemap.",
            "paires": PAIRES if paires is None else paires}


def _hreflang() -> dict:
    return {"issue_key": "hreflang_to_redirect_or_broken_page", "issue_label": "Hreflang vers redirect / URL cassée",
            "ok": False, "error": "Aucun patch appliqué", "files": [], "paires": PAIRES[:1],
            "skipped": ["app/(en)/page.tsx", "content/index.mdx"],
            "targets": ["app/(en)/page.tsx", "content/index.mdx"], "not_attempted_files": ["content/de/index.mdx"]}


def _corps(results: list[dict]) -> str:
    m._lier_les_familles(results, None)
    return m._corps_pr_groupee(site_name="cityscootlab.com", ts="t", results=results, files_total=1,
                               ai_written=False, premise_key="")


def test_le_cas_cityscootlab_ne_promet_plus_d_echec() -> None:
    corps = _corps([_canonical(), _sitemap(), _hreflang()])

    assert "↪ **Sitemap : URL en 3XX** — aucun fichier propre à cette famille ; mêmes URL que « Canonical → redirection »" in corps
    assert "↪ **Hreflang vers redirect / URL cassée**" in corps
    assert "❌" not in corps
    assert "resteront signalées" not in corps
    assert corps.count("Le prochain crawl dira si ces pages tombent avec la correction « Canonical → redirection »") == 2
    assert "⏭️ **Non corrigés" in corps and "📄 **Non traités" in corps
    assert corps.count("> ↪ **Peut tomber avec « Canonical → redirection ».**") == 2


def test_une_paire_non_couverte_garde_l_echec_annonce() -> None:
    corps = _corps([_canonical(), _sitemap(paires=PAIRES + [[f"{B}/vieux", f"{B}/neuf"]])])

    assert "❌ **Sitemap : URL en 3XX** — Aucune destination canonical" in corps
    assert "Peut tomber avec" not in corps


def test_une_famille_couvrante_en_echec_ne_couvre_rien() -> None:
    corps = _corps([_canonical(ok=False), _sitemap()])

    assert "↪" not in corps and corps.count("❌") == 2


def test_sans_paires_rien_n_est_lie() -> None:
    corps = _corps([_canonical(), _sitemap(paires=[])])

    assert "❌ **Sitemap : URL en 3XX**" in corps and "↪" not in corps


def test_une_famille_corrigee_reste_corrigee() -> None:
    results = [_canonical(), {**_sitemap(), "ok": True, "files": ["public/sitemap.xml"]}]
    m._lier_les_familles(results, None)

    assert not any(r.get("lie_a") for r in results)


def test_deux_familles_couvrantes_sont_nommees_ensemble() -> None:
    moitie_a, moitie_b = PAIRES[:2], PAIRES[2:]
    results = [{**_canonical(paires=moitie_a)},
               {**_canonical(paires=moitie_b), "issue_key": "x", "issue_label": "Autre famille"},
               _sitemap()]
    m._lier_les_familles(results, None)

    assert results[2]["lie_a"] == "« Canonical → redirection » et « Autre famille »"


def test_les_paires_se_lisent_dans_la_preuve_du_crawl() -> None:
    issues = {"sitemap_3xx_redirect": {"count": 1, "examples": [f"{B}/de/"], "evidence": {
        "kind": "url_pairs", "items": [{"from": f"{B}/de/", "to": f"{B}/de"},
                                       {"from": f"{B}/de/", "to": f"{B}/de"}, {"from": "", "to": "x"}]}}}

    assert m._paires_de_preuve(issues, "sitemap_3xx_redirect") == [[f"{B}/de/", f"{B}/de"]]
    autre_preuve = {"kind": "page_values", "items": [{"from": f"{B}/de/", "to": f"{B}/de"}]}
    assert m._paires_de_preuve({"sitemap_3xx_redirect": {"evidence": autre_preuve}}, "sitemap_3xx_redirect") == []
