"""Les quatre ecarts de prosperfactory.com avec Ahrefs, 13/10/2026.

Ahrefs : 109 adresses internes, 51 avertissements, 17 remarques. Le crawler : 112, 43, 20. Chaque
ecart venait d'une regle, pas du site :
- ses quatre adresses de paiement repondent 302 vers Stripe avec « X-Robots-Tag: noindex, follow » ;
  Ahrefs les compte en « Noindex page » ET « Noindex follow page », le crawler ne lisait que les
  en-tetes de la page d'arrivee (Stripe) ;
- « 302 redirect » est un avertissement chez Ahrefs, une remarque chez nous ;
- « ressources externes refusees » est une famille Semrush ;
- /de, /es, /fr (sans barre) etaient essayees par le crawler lui-meme, alors que rien ne les lie.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
_SPEC = importlib.util.spec_from_file_location(
    "seo_audit_parite_pf", REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py"
)
assert _SPEC and _SPEC.loader
seo_audit = importlib.util.module_from_spec(_SPEC)
sys.modules["seo_audit_parite_pf"] = seo_audit
_SPEC.loader.exec_module(seo_audit)

BASE = "https://site.test"
FAMILLES = ("noindex_page", "noindex_follow_page", "noindex_and_nofollow_page", "nofollow_page")


def _page(url: str, **kw: Any) -> Any:
    defaults: dict[str, Any] = {
        "url": url,
        "final_url": url,
        "status_code": 200,
        "content_type": "text/html; charset=utf-8",
        "title": f"Titre propre de la page {url}",
        "meta_description": f"Description unique et de longueur raisonnable pour la page {url}, sans regle.",
        "canonical": url,
        "lang": "fr",
        "h1": ["Titre principal"],
        "h1_tag_count": 1,
        "title_tag_count": 1,
    }
    defaults.update(kw)
    return seo_audit.PageData(**defaults)


def _paiement(url: str, robots: str | None) -> Any:
    """Une adresse de paiement : 302 vers Stripe, la page d'arrivee n'a aucune directive."""
    return _page(url, final_url="https://buy.stripe.com/abc", redirect_chain=[url], redirect_statuses=[302],
                 redirect_x_robots_tag=robots)


def _comptes(pages: list[Any]) -> dict[str, list[str]]:
    issues = seo_audit._score_issues(pages, base_url=BASE)
    return {k: [e if isinstance(e, str) else e.get("url") for e in issues[k]["examples"]] for k in FAMILLES}


def test_une_redirection_noindex_follow_est_comptee_comme_ahrefs() -> None:
    c = _comptes([_page(f"{BASE}/"), _paiement(f"{BASE}/payment", "noindex, follow, noarchive")])

    assert c["noindex_page"] == [f"{BASE}/payment"]
    assert c["noindex_follow_page"] == [f"{BASE}/payment"]
    assert c["noindex_and_nofollow_page"] == []
    assert c["nofollow_page"] == []


def test_une_redirection_noindex_nofollow_va_dans_l_autre_famille() -> None:
    c = _comptes([_page(f"{BASE}/"), _paiement(f"{BASE}/go/x", "noindex, nofollow")])

    assert c["noindex_page"] == [f"{BASE}/go/x"]
    assert c["noindex_and_nofollow_page"] == [f"{BASE}/go/x"]
    assert c["nofollow_page"] == [f"{BASE}/go/x"]
    assert c["noindex_follow_page"] == []


def test_une_redirection_sans_directive_ne_compte_nulle_part() -> None:
    c = _comptes([_page(f"{BASE}/"), _paiement(f"{BASE}/payment", None)])

    assert all(v == [] for v in c.values())


def test_une_url_deja_jugee_sur_son_document_n_est_pas_rejugee() -> None:
    # Redirection interne vers une page servie 200 en « noindex, follow » : le crawler l'a deja
    # classee sur ce document. Un en-tete contraire sur le 301 ne doit pas la mettre AUSSI en nofollow.
    p = _page(f"{BASE}/ancien", final_url=f"{BASE}/neuf", redirect_chain=[f"{BASE}/ancien"],
              redirect_statuses=[301], x_robots_tag="noindex, follow", redirect_x_robots_tag="noindex, nofollow")
    c = _comptes([_page(f"{BASE}/"), p])

    assert c["noindex_follow_page"].count(f"{BASE}/ancien") == 1
    assert f"{BASE}/ancien" not in c["noindex_and_nofollow_page"]
    assert f"{BASE}/ancien" not in c["nofollow_page"]


def test_le_crawl_n_essaie_plus_les_racines_de_langue_sans_barre() -> None:
    depart = seo_audit._urls_de_depart(f"{BASE}/", [f"{BASE}/", f"{BASE}/fr/", f"{BASE}/de/"])

    assert f"{BASE}/fr" not in depart and f"{BASE}/de" not in depart
    # Les variantes http/www de l'accueil restent : Ahrefs les essaie aussi.
    assert depart == [f"{BASE}/", "http://site.test/", "http://www.site.test/", "https://www.site.test/",
                      f"{BASE}/fr/", f"{BASE}/de/"]


def test_une_302_est_un_avertissement() -> None:
    from backend import audit_dashboard

    assert audit_dashboard.ISSUE_CATALOG["redirect_302"].severity == "warning"


def test_une_ressource_externe_refusee_n_est_plus_signalee_ni_comptee_cassee(monkeypatch) -> None:
    class _ToutRefuse:
        def can_fetch(self, *_a: Any) -> bool:
            return False

    monkeypatch.setattr(seo_audit, "_load_robots", lambda *a, **k: (_ToutRefuse(), [], None))
    page = _page(f"{BASE}/", image_urls=["https://cdn.tiers.test/logo.png"])
    refusee = {"url": "https://cdn.tiers.test/logo.png", "type": "image", "status_code": 403}

    issues = seo_audit._score_external_resource_issues([page], [refusee], timeout_s=5, user_agent="t")

    assert issues["disallowed_external_resources"]["count"] == 0
    # La detection sert toujours : une 403 refusee n'est pas une image cassee.
    assert issues["broken_external_images"]["count"] == 0
