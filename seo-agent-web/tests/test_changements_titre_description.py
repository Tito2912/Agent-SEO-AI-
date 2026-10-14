"""Titre et description modifies entre deux crawls : ce qu'Ahrefs montre, on le montre.

oryvalo.com, 13/10/2026. La PR #16 change le titre de /seo et la description de /about ; au crawl
suivant, Ahrefs affiche « Title tag changed » 1, « Meta description changed » 1 et « Changed pages
not submitted to IndexNow » 2. Le crawler detectait les deux changements mais ne les emettait plus
(7b184f4 : « Ahrefs ne les suit pas » — faux), ne regardait les titres que sur les racines, et ne
proposait a IndexNow que l'accueil et les racines de langue.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
_SPEC = importlib.util.spec_from_file_location(
    "seo_audit_changements", REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py"
)
assert _SPEC and _SPEC.loader
seo_audit = importlib.util.module_from_spec(_SPEC)
sys.modules["seo_audit_changements"] = seo_audit
_SPEC.loader.exec_module(seo_audit)

BASE = "https://site.test"


def _page(chemin: str, **kw: Any) -> Any:
    url = f"{BASE}{chemin}"
    defaults: dict[str, Any] = {
        "url": url,
        "final_url": url,
        "status_code": 200,
        "content_type": "text/html; charset=utf-8",
        "title": f"Titre propre de la page {chemin}",
        "meta_description": f"Description unique et de longueur raisonnable pour la page {chemin}, sans regle.",
        "canonical": url,
        "lang": "fr",
        "h1": [f"Titre principal {chemin}"],
        "h1_tag_count": 1,
        "title_tag_count": 1,
    }
    defaults.update(kw)
    return seo_audit.PageData(**defaults)


def _site(**modifs: dict[str, Any]) -> list[Any]:
    return [_page(c, **modifs.get(c, {})) for c in ("/", "/seo", "/about", "/prive")]


def _urls(issues: dict[str, Any], cle: str) -> list[str]:
    return sorted(e if isinstance(e, str) else e.get("url") for e in issues[cle]["examples"])


def _apres_la_pr(**en_plus: dict[str, Any]) -> dict[str, Any]:
    avant = _site()
    apres = _site(**{
        "/seo": {"title": "SEO Guides: Keyword Research & Internal Linking Tips"},
        "/about": {"meta_description": "Learn more about Oryvalo, our editorial approach and our review process."},
        **en_plus,
    })
    # strict_link_counts : le profil Ahrefs, celui qui limitait les titres aux racines.
    return seo_audit._score_issues(apres, previous_pages=avant, base_url=BASE, strict_link_counts=True)


def test_le_cas_oryvalo_tombe_comme_chez_ahrefs() -> None:
    issues = _apres_la_pr()

    assert _urls(issues, "title_tag_changed") == [f"{BASE}/seo"]
    assert _urls(issues, "meta_description_changed") == [f"{BASE}/about"]
    assert _urls(issues, "pages_to_submit_to_indexnow") == [f"{BASE}/about", f"{BASE}/seo"]


def test_une_page_non_indexable_ne_compte_pas() -> None:
    issues = _apres_la_pr(**{"/prive": {"title": "Nouveau titre prive", "meta_robots": "noindex",
                                        "meta_description": "Nouvelle description de la page privee, assez longue."}})

    assert f"{BASE}/prive" not in _urls(issues, "title_tag_changed")
    assert f"{BASE}/prive" not in _urls(issues, "meta_description_changed")
    assert f"{BASE}/prive" not in _urls(issues, "pages_to_submit_to_indexnow")


def test_un_h1_qui_change_compte_et_part_a_indexnow() -> None:
    issues = _apres_la_pr(**{"/prive": {"h1": ["Un autre h1"]}})

    assert _urls(issues, "h1_tag_changed") == [f"{BASE}/prive"]
    assert f"{BASE}/prive" in _urls(issues, "pages_to_submit_to_indexnow")


def test_un_h1_qui_apparait_est_un_changement() -> None:
    """make-avis, 14/10/2026 : 16 pages SANS H1 en recoivent un ; Ahrefs « H1 tag changed » 16."""
    avant = _site(**{"/prive": {"h1": []}})
    apres = _site()
    issues = seo_audit._score_issues(apres, previous_pages=avant, base_url=BASE, strict_link_counts=True)

    assert _urls(issues, "h1_tag_changed") == [f"{BASE}/prive"]
    assert _urls(issues, "pages_to_submit_to_indexnow") == [f"{BASE}/prive"]


def test_un_h1_absent_des_deux_cotes_ne_change_rien_et_le_noindex_reste_dehors() -> None:
    sans = {"/prive": {"h1": []}}
    assert seo_audit._score_issues(_site(**sans), previous_pages=_site(**sans), base_url=BASE)[
        "h1_tag_changed"]["count"] == 0
    noindex = _apres_la_pr(**{"/prive": {"h1": ["Autre"], "meta_robots": "noindex"}})
    assert f"{BASE}/prive" not in _urls(noindex, "h1_tag_changed")


def test_sans_crawl_precedent_rien_ne_change() -> None:
    issues = seo_audit._score_issues(_site(), base_url=BASE)

    assert issues["pages_to_submit_to_indexnow"]["count"] == 0
    assert issues.get("title_tag_changed", {"count": 0})["count"] == 0
