"""Aligner og:url sur un canonical CASSE propage le defaut au lieu de le corriger.

Mesure du 16/09/2026 sur le banc, static-html, page `/gauntlet/canonical-404`. Avant correction :

    canonical  -> /page-absente              (404 — c'est le defaut injecte)
    og:url     -> /gauntlet/canonical-404    (juste)

Apres un passage du correcteur, les deux valeurs avaient ete ECHANGEES : `canonical_points_to_4xx`
avait bien repare le canonical, puis `open_graph_url_not_matching_canonical` avait aligne og:url
sur la valeur fautive lue au crawl. Une balise correcte s'est mise a designer une 404.

Ce que l'incident apprend sur la LECTURE du bilan : la famille est passee de 5 a 1. Quatre pages
vraiment corrigees, une regression — et la ligne ressemble a un succes partiel. Une baisse n'est
pas une correction tant qu'on n'a pas regarde ce qui reste.

`_og_url_pairs_from_pages` refusait deja le cas jumeau (canonical en http) avec exactement ce
raisonnement : le defaut de la page est son canonical, pas son og:url. Il manquait la meme clause
pour une destination definitivement absente.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-og-dead-canonical-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402

SITE = "https://exemple.fr"
MORTE = f"{SITE}/page-absente"
VIVANTE = f"{SITE}/mentions-legales"


def _pages(canonical: str) -> list[dict[str, object]]:
    """Le site du banc, reduit : la page signalee, la destination morte, une destination vivante."""
    return [
        {"url": f"{SITE}/canonical-404", "status_code": 200,
         "canonical": canonical, "og_url": f"{SITE}/canonical-404"},
        {"url": MORTE, "status_code": 404, "canonical": "", "og_url": ""},
        {"url": VIVANTE, "status_code": 200, "canonical": VIVANTE, "og_url": VIVANTE},
    ]


def test_un_canonical_vers_une_404_ne_devient_jamais_l_og_url() -> None:
    paires = app_module._og_url_pairs_from_pages([f"{SITE}/canonical-404"], _pages(MORTE))
    assert paires == [], f"le correcteur allait recopier une 404 dans og:url : {paires}"


def test_un_canonical_VIVANT_reste_bien_corrige() -> None:
    """Sans cette moitie, le test precedent passerait aussi si la famille ne faisait plus rien.

    C'est la mesure qui manquait le jour de l'incident : on avait verifie que la famille
    CORRIGEAIT, jamais ce qu'elle corrigeait EN.
    """
    paires = app_module._og_url_pairs_from_pages([f"{SITE}/canonical-404"], _pages(VIVANTE))
    assert len(paires) == 1, paires
    assert paires[0]["from"] == f"{SITE}/canonical-404"
    assert paires[0]["to"] == VIVANTE


def test_une_410_compte_comme_une_404() -> None:
    """410 dit « parti pour de bon » — plus definitif encore qu'une 404, jamais moins."""
    pages = _pages(MORTE)
    pages[1]["status_code"] = 410
    assert app_module._og_url_pairs_from_pages([f"{SITE}/canonical-404"], pages) == []


def test_une_destination_que_le_crawl_n_a_pas_vue_reste_corrigeable() -> None:
    """Le refus s'appuie sur une MESURE, pas sur une absence de mesure.

    Une page hors du crawl (hors plafond, hors sitemap) n'est pas une page morte. La traiter
    comme telle bloquerait la famille sur tout site dont le canonical sort du perimetre crawle.
    """
    pages = [p for p in _pages(MORTE) if p["url"] != MORTE]
    paires = app_module._og_url_pairs_from_pages([f"{SITE}/canonical-404"], pages)
    assert len(paires) == 1, paires
    assert paires[0]["to"] == MORTE
