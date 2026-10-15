"""IndexNow, pages ajoutees aux sitemaps, pages orphelines du sitemap : la parite d'apres la PR #1
de cityscootlab (15/10/2026).

La PR repare le canonical de /de /es /fr (`/de/`, une redirection, devient `/de`), 13 titres, 4
descriptions et 4 liens `/xx/#ancre`. Au crawl suivant, Ahrefs montre IndexNow 23, « Pages added to
sitemaps » 3 et aucune page orpheline. Noyaru montrait IndexNow 17, aucune famille « ajoutees », et
2 « Orphaned sitemap pages », une famille Semrush. Les 6 d'ecart d'IndexNow, lus dans l'export
Ahrefs : /de /es /fr DEVENUES indexables, et trois pages dont un seul lien interne a change.
"""

from __future__ import annotations

import dataclasses
import importlib.util
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
WEB_ROOT = REPO_ROOT / "seo-agent-web"
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
_SPEC = importlib.util.spec_from_file_location(
    "seo_audit_parite_cityscootlab", REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py"
)
assert _SPEC and _SPEC.loader
seo_audit = importlib.util.module_from_spec(_SPEC)
sys.modules["seo_audit_parite_cityscootlab"] = seo_audit
_SPEC.loader.exec_module(seo_audit)

from backend import audit_dashboard as dash  # noqa: E402

BASE = "https://site.test"
SITEMAP = f"{BASE}/sitemap.xml"


def _u(chemin: str) -> str:
    return f"{BASE}{chemin}"


def _page(chemin: str, **kw: Any) -> Any:
    url = _u(chemin)
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
        "internal_links": [_u("/"), _u("/de"), _u("/fr/blog"), _u("/guide")],
    }
    defaults.update(kw)
    return seo_audit.PageData(**defaults)


CHEMINS = ("/", "/de", "/fr/blog", "/guide", "/m2")


def _site(**modifs: dict[str, Any]) -> list[Any]:
    return [_page(c, **modifs.get(c, {})) for c in CHEMINS]


def _crawl(pages: list[Any], locs: list[str] | None) -> list[Any]:
    """Un crawl tel que le suivant le relira : score avec son sitemap, PUIS aller-retour par
    report.json — c'est la que la memoire du sitemap doit survivre."""
    seo_audit._score_issues(
        pages, sitemap_urls=set(locs) if locs is not None else None,
        sitemap_urlsets={SITEMAP: list(locs)} if locs else None, base_url=BASE,
    )
    return seo_audit._pages_from_report({"pages": [dataclasses.asdict(p) for p in pages]})


def _score(pages: list[Any], locs: list[str] | None, previous: list[Any] | None) -> dict[str, Any]:
    return seo_audit._score_issues(
        pages, sitemap_urls=set(locs) if locs is not None else None,
        sitemap_urlsets={SITEMAP: list(locs)} if locs else None,
        previous_pages=previous, base_url=BASE,
    )


def _urls(issues: dict[str, Any], cle: str) -> list[str]:
    bloc = issues.get(cle) or {"examples": []}
    return sorted(e if isinstance(e, str) else e.get("url") for e in bloc["examples"])


LOCS_AVANT = [_u("/"), _u("/de/"), _u("/fr/blog"), _u("/guide"), _u("/m2")]
LOCS_APRES = [_u("/"), _u("/de"), _u("/fr/blog"), _u("/guide"), _u("/m2")]


def _avant_la_pr() -> list[Any]:
    # /de declare `/de/` (une redirection) : non canonique, donc non indexable, comme chez Ahrefs.
    # /fr/blog lie `/de/` ET `/de` : deux cibles distinctes avant, une seule apres.
    pages = _site(**{
        "/de": {"canonical": _u("/de/")},
        "/fr/blog": {"internal_links": [_u("/"), _u("/de/"), _u("/de"), _u("/fr/blog"), _u("/guide")]},
    })
    # Et `/de/` elle-meme, crawlee depuis le sitemap : le crawler l'enregistre en 200 avec la page
    # d'arrivee pour `final_url` (forme relevee dans un vrai rapport). Elle est AU sitemap ; /de non.
    pages.append(_page("/de/", final_url=_u("/de"), redirect_statuses=[301], redirect_chain=[_u("/de/")],
                       canonical=_u("/de/")))
    return _crawl(pages, LOCS_AVANT)


def test_le_cas_cityscootlab_tombe_comme_chez_ahrefs() -> None:
    issues = _score(_site(), LOCS_APRES, _avant_la_pr())

    assert _urls(issues, "pages_to_submit_to_indexnow") == [_u("/de"), _u("/fr/blog")]
    assert _urls(issues, "pages_added_to_sitemaps") == [_u("/de")]


def test_le_lien_vers_soi_ne_compte_pas() -> None:
    """/fr/blog fait 16 liens chez Ahrefs, 17 avec son lien de menu vers elle-meme."""
    avant = _crawl(_site(), LOCS_APRES)
    apres = _site(**{"/guide": {"internal_links": [_u("/"), _u("/de"), _u("/fr/blog"), _u("/guide"),
                                                   _u("/guide")]}})
    sans_soi = _site(**{"/guide": {"internal_links": [_u("/"), _u("/de"), _u("/fr/blog")]}})

    assert _score(apres, LOCS_APRES, avant)["pages_to_submit_to_indexnow"]["count"] == 0
    assert _score(sans_soi, LOCS_APRES, avant)["pages_to_submit_to_indexnow"]["count"] == 0


def test_l_ordre_et_les_doublons_des_liens_ne_changent_rien() -> None:
    avant = _crawl(_site(), LOCS_APRES)
    apres = _site(**{"/m2": {"internal_links": [_u("/guide"), _u("/fr/blog"), _u("/de"), _u("/"), _u("/de")]}})

    assert _score(apres, LOCS_APRES, avant)["pages_to_submit_to_indexnow"]["count"] == 0


def test_un_lien_en_plus_ou_en_moins_part_a_indexnow() -> None:
    avant = _crawl(_site(), LOCS_APRES)
    en_plus = _site(**{"/m2": {"internal_links": [_u("/"), _u("/de"), _u("/fr/blog"), _u("/guide"), _u("/m2-bis")]}})
    en_moins = _site(**{"/m2": {"internal_links": [_u("/"), _u("/de"), _u("/fr/blog")]}})

    assert _urls(_score(en_plus, LOCS_APRES, avant), "pages_to_submit_to_indexnow") == [_u("/m2")]
    assert _urls(_score(en_moins, LOCS_APRES, avant), "pages_to_submit_to_indexnow") == [_u("/m2")]


def test_une_page_non_indexable_reste_hors_d_indexnow() -> None:
    avant = _crawl(_site(**{"/m2": {"meta_robots": "noindex"}}), LOCS_APRES)
    apres = _site(**{"/m2": {"meta_robots": "noindex", "internal_links": [_u("/")]}})

    assert _score(apres, LOCS_APRES, avant)["pages_to_submit_to_indexnow"]["count"] == 0


def test_une_page_devenue_non_indexable_n_est_pas_devenue_indexable() -> None:
    avant = _crawl(_site(), LOCS_APRES)
    apres = _site(**{"/de": {"canonical": _u("/de/")}})

    assert _u("/de") not in _urls(_score(apres, LOCS_APRES, avant), "pages_to_submit_to_indexnow")


def test_la_page_redirigee_retiree_du_sitemap_n_est_pas_comptee() -> None:
    """Ahrefs ne compte pas `/de/` dans « Pages removed » : ce n'est pas une page. Ici, rien
    d'autre que /de n'est ajoute."""
    issues = _score(_site(), LOCS_APRES, _avant_la_pr())

    assert _urls(issues, "pages_added_to_sitemaps") == [_u("/de")]
    assert "pages_removed_from_sitemaps" not in issues


def test_une_page_connue_par_sa_seule_redirection_est_bien_ajoutee() -> None:
    """Rien ne liait /de directement : le crawl d'avant ne l'a vue qu'au bout de `/de/`, l'URL du
    sitemap. C'est l'URL FINALE qui dit si la page y etait — /de n'y etait pas."""
    pages = [p for p in _site(**{"/de": {"canonical": _u("/de/")}}) if p.url != _u("/de")]
    pages.append(_page("/de/", final_url=_u("/de"), redirect_statuses=[301], redirect_chain=[_u("/de/")],
                       canonical=_u("/de/")))

    issues = _score(_site(), LOCS_APRES, _crawl(pages, LOCS_AVANT))

    assert _urls(issues, "pages_added_to_sitemaps") == [_u("/de")]


def test_un_rapport_sans_memoire_du_sitemap_n_ajoute_rien() -> None:
    """Les rapports d'avant le 15/10/2026 n'ont pas `in_sitemap` : inconnu n'est pas absent."""
    ancien = _avant_la_pr()
    for p in ancien:
        p.in_sitemap = None

    assert _score(_site(), LOCS_APRES, ancien)["pages_added_to_sitemaps"]["count"] == 0


def test_un_crawl_sans_sitemap_lu_n_ajoute_ni_ne_retire_rien() -> None:
    sans_sitemap = _crawl(_site(), None)
    assert all(p.in_sitemap is None for p in sans_sitemap)

    assert _score(_site(), LOCS_APRES, sans_sitemap)["pages_added_to_sitemaps"]["count"] == 0
    assert _score(_site(), None, _crawl(_site(), LOCS_AVANT))["pages_added_to_sitemaps"]["count"] == 0


def test_la_memoire_du_sitemap_survit_au_rapport() -> None:
    pages = _avant_la_pr()
    par_url = {p.url: p.in_sitemap for p in pages}

    assert par_url[_u("/de")] is False
    assert par_url[_u("/guide")] is True


def test_les_pages_orphelines_du_sitemap_se_taisent() -> None:
    """/m2 est au sitemap et rien ne la lie : Semrush la signale, Ahrefs non."""
    issues = _score(_site(), LOCS_APRES, None)

    assert issues["orphaned_sitemap_pages"]["count"] == 0


def test_la_nouvelle_famille_est_un_changement_et_parle_francais() -> None:
    assert dash.is_delta_issue_key("pages_added_to_sitemaps")
    meta = dash.ISSUE_CATALOG["pages_added_to_sitemaps"]
    assert meta.label == "Pages ajoutées aux sitemaps"
    assert meta.category == "Sitemaps"
