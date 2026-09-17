"""Deuxieme des familles sans correcteur : une URL listee plusieurs fois au sitemap.

`page_in_multiple_sitemaps` couvre DEUX cas que son nom ne distingue pas :

  - la meme URL dans DEUX fichiers de sitemap differents. Lequel garde l'entree ? La mesure ne le
    dit pas, et se tromper retire une page d'un plan de site qui la portait a bon droit.
  - la meme URL DEUX FOIS dans le MEME fichier. Aucune direction a choisir : un doublon n'apporte
    rien, et la premiere occurrence fait foi.

Le correctif ne traite que le second, et c'est ce qui lui permet d'etre deterministe. Le premier
continuera d'etre explique au client sans etre corrige — un correctif qui devine lequel des deux
plans de site a raison serait exactement le genre de supposition que ce projet refuse ailleurs.

La PREMIERE occurrence est gardee : elle porte souvent les `<lastmod>` et `<priority>` d'origine,
et l'ordre du fichier est celui que son auteur a voulu.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-sitemap-dedupe-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402

SITEMAP = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<url><loc>https://x.fr/a</loc><lastmod>2026-01-01</lastmod></url>
<url><loc>https://x.fr/b</loc></url>
<url><loc>https://x.fr/a</loc><lastmod>2026-09-09</lastmod></url>
<url><loc>https://x.fr/c</loc></url>
</urlset>
"""


def _locs(contenu: str) -> list[str]:
    return [m.group(2).strip() for m in app_module._SITEMAP_LOC_RE.finditer(contenu)]


def test_la_copie_part_et_une_seule_entree_reste() -> None:
    sortie, n = app_module._dedupe_sitemap_locs(SITEMAP, ["https://x.fr/a"])
    assert n == 1, n
    assert _locs(sortie).count("https://x.fr/a") == 1


def test_la_PREMIERE_occurrence_est_celle_qui_reste() -> None:
    """Elle porte le `<lastmod>` d'origine, et l'ordre du fichier est voulu par son auteur."""
    sortie, _ = app_module._dedupe_sitemap_locs(SITEMAP, ["https://x.fr/a"])
    assert "2026-01-01" in sortie, sortie
    assert "2026-09-09" not in sortie, sortie


def test_les_autres_entrees_ne_bougent_pas() -> None:
    sortie, _ = app_module._dedupe_sitemap_locs(SITEMAP, ["https://x.fr/a"])
    assert _locs(sortie) == ["https://x.fr/a", "https://x.fr/b", "https://x.fr/c"], _locs(sortie)


def test_une_URL_presente_UNE_SEULE_fois_n_est_pas_touchee() -> None:
    """Le cas des deux fichiers : ici il n'y a qu'une occurrence, donc rien a faire ICI.

    C'est l'abstention qui permet au correctif d'etre deterministe — il ne choisit jamais lequel
    de deux plans de site garde la page.
    """
    sortie, n = app_module._dedupe_sitemap_locs(SITEMAP, ["https://x.fr/b"])
    assert n == 0 and sortie == SITEMAP


def test_rejouer_le_correctif_n_enleve_rien_de_plus() -> None:
    sortie, _ = app_module._dedupe_sitemap_locs(SITEMAP, ["https://x.fr/a"])
    encore, n = app_module._dedupe_sitemap_locs(sortie, ["https://x.fr/a"])
    assert n == 0 and encore == sortie


def test_un_sitemap_INDEX_n_est_pas_ampute() -> None:
    """Un index liste ses enfants dans des blocs `<sitemap>`, pas `<url>`. Supprimer l'un d'eux
    retirerait un plan de site entier — la meme propriete que celle sur laquelle repose
    `_remove_sitemap_locs`."""
    index = ('<?xml version="1.0"?>\n<sitemapindex>\n'
             '<sitemap><loc>https://x.fr/s1.xml</loc></sitemap>\n'
             '<sitemap><loc>https://x.fr/s1.xml</loc></sitemap>\n</sitemapindex>\n')
    sortie, n = app_module._dedupe_sitemap_locs(index, ["https://x.fr/s1.xml"])
    assert n == 0 and sortie == index


def test_sans_url_visee_on_ne_touche_a_rien() -> None:
    sortie, n = app_module._dedupe_sitemap_locs(SITEMAP, [])
    assert n == 0 and sortie == SITEMAP


def test_la_famille_est_declaree_corrigeable() -> None:
    assert "page_in_multiple_sitemaps" in set(app_module._handled_issue_keys())
