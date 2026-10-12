# -*- coding: utf-8 -*-
"""Parité Ahrefs mesurée sur noyaru.com, le 12/10/2026 — premier crawl des deux outils.

Ahrefs : 41 URL internes + 14 ressources, 0 erreur / 42 avertissements / 16 infos. Noyaru
affichait 52 + 14, 42 / 45, et quatre familles en désaccord. Chaque écart a été vérifié sur les
pages servies :
- 11 captures de la documentation, intégrées (<img>) ET liées (clic pour agrandir), comptées
  comme pages ET comme ressources ;
- « X (Twitter) card missing » = 7 : exactement les 7 pages sans twitter:card — 5 sans aucun
  Open Graph, plus / et /pricing dont l'Open Graph est INCOMPLET (pas d'og:image). Noyaru
  exemptait toute page portant de l'Open Graph : 5 ;
- « X (Twitter) card incomplete » = 0 sur 31 pages à carte `summary` sans image : l'image n'est
  requise que par `summary_large_image`. Noyaru : 31 ;
- « Meta description tag missing or empty » 2 et « too short » 1, sous INDEXABLE. Noyaru
  repliait l'absente dans « trop courte » : 0 et 3.
Après correction, le crawl local de noyaru.com donne 41 / 55 et 0 / 42 / 16, famille par famille.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

from backend import audit_dashboard as dash

REPO_ROOT = Path(__file__).resolve().parents[2]
_SPEC = importlib.util.spec_from_file_location(
    "seo_audit_parite_noyaru", REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py")
assert _SPEC and _SPEC.loader
seo_audit = importlib.util.module_from_spec(_SPEC)
sys.modules["seo_audit_parite_noyaru"] = seo_audit
_SPEC.loader.exec_module(seo_audit)

BASE = "https://site.test"
OG_SANS_IMAGE = dict(og_title="Titre OG", og_description="Description OG", og_url=None, og_type="website")


def _page(path: str, **kw: Any) -> Any:
    url = BASE + path
    data: dict[str, Any] = {
        "url": url, "final_url": url, "status_code": 200, "content_type": "text/html; charset=utf-8",
        "title": f"Titre propre de la page {path}",
        "meta_description": f"Description unique et de longueur raisonnable pour la page {path}, sans declencher de regle.",
        "canonical": url, "lang": "fr", "h1": ["Titre"], "h1_tag_count": 1, "title_tag_count": 1,
    }
    data.update(kw)
    if data.get("og_url", "") is None:
        data["og_url"] = url
    return seo_audit.PageData(**data)


def _urls(issues: dict[str, Any], key: str) -> set[str]:
    return {u.replace(BASE, "") for u in (issues.get(key) or {}).get("examples", [])} if (issues.get(key) or {}).get("count") else set()


def _site() -> dict[str, Any]:
    pages = [
        _page("/", **OG_SANS_IMAGE),                                       # OG incomplet, aucune balise X
        _page("/pricing", **OG_SANS_IMAGE),
        _page("/docs/demarrer", **OG_SANS_IMAGE, twitter_card="summary"),  # carte summary, aucune image
        _page("/status", meta_description=None),                           # ni OG, ni X, ni description
        _page("/mentions-legales", meta_description="Éditeur, directeur de la publication et hébergeur."),
        _page("/complet", og_title="T", og_description="D", og_url=None, og_type="website",
              og_image="https://site.test/og.png"),                          # OG complet, aucune balise X
        _page("/grande", **OG_SANS_IMAGE, twitter_card="summary_large_image"),
    ]
    return seo_audit._score_issues(pages, base_url=BASE)


def test_a_missing_card_is_flagged_unless_a_complete_open_graph_covers_it() -> None:
    assert _urls(_site(), "twitter_card_missing") == {"/", "/pricing", "/status", "/mentions-legales"}


def test_only_a_large_image_card_needs_an_image() -> None:
    assert _urls(_site(), "twitter_card_incomplete") == {"/grande"}


def test_an_absent_description_is_missing_not_too_short_even_when_indexable() -> None:
    issues = _site()
    assert "/status" in _urls(issues, "missing_meta_description")
    assert "/status" not in _urls(issues, "meta_description_too_short_indexable")
    assert "/mentions-legales" in _urls(issues, "meta_description_too_short_indexable")
    assert "/mentions-legales" not in _urls(issues, "missing_meta_description")


def test_a_linked_and_embedded_image_counts_once_as_a_resource() -> None:
    image = BASE + "/static/captures/anomalies.webp"
    report = {
        "meta": {"pages_crawled": 3},
        "issues": {},
        "pages": [
            {"url": BASE + "/", "final_url": BASE + "/", "status_code": 200, "content_type": "text/html"},
            {"url": BASE + "/docs", "final_url": BASE + "/docs", "status_code": 200, "content_type": "text/html"},
            {"url": image, "final_url": image, "status_code": 200, "content_type": "image/webp"},
        ],
        "resources": [
            {"url": image, "type": "image", "status_code": 200},
            {"url": BASE + "/static/styles.css", "type": "css", "status_code": 200},
        ],
    }
    summary = dash.summarize_report(report)
    assert summary["pages_crawled"] == 2
    assert summary["urls_crawled_distribution"] == 4


def test_a_linked_file_that_is_not_a_resource_still_counts_as_internal() -> None:
    pdf = BASE + "/guide.pdf"
    report = {"meta": {"pages_crawled": 2}, "issues": {}, "resources": [],
              "pages": [{"url": BASE + "/", "status_code": 200, "content_type": "text/html"},
                        {"url": pdf, "status_code": 200, "content_type": "application/pdf"}]}
    assert dash.summarize_report(report)["pages_crawled"] == 2
