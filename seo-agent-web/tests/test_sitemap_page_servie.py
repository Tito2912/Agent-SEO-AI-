"""Sur un framework, le sitemap se corrige d'apres la page SERVIE, et un refus dit sa raison.

Banc complet du 12/10/2026, lu dans le sitemap servi par chaque apercu : `sitemap_noindex_page`,
`sitemap_non_canonical_page` et `indexable_page_not_in_sitemap` n'etaient corrigees sur AUCUN
framework. Avant d'ecrire le sitemap, le plan relit chaque page source et exige du HTML brut ;
un `.js`, un `.astro`, un gabarit levaient une exception, avalee sans un mot — et le banc
affichait « DEJA CORRIGE ».

La page SERVIE, relue au moment de corriger, est le temoin : elle doit encore dire ce que le
crawl a observe. Sur un site en HTML pur, la source EST le document : la regle stricte reste.
"""
from __future__ import annotations

import base64
from types import SimpleNamespace
from urllib.parse import unquote

import pytest

from backend import app as m
from backend import repo_index
from tests.test_verified_sitemap_canonical import entry, sitemap
from tests.test_verified_sitemap_noindex import row as noindex_row

SITE = "https://site.test"
A, C = SITE + "/private", SITE + "/control"
GATSBY = ["gatsby-config.js", "package.json", "static/sitemap.xml", "src/pages/private.js", "src/pages/control.js"]


# --- la page servie -----------------------------------------------------------------------

def servir(monkeypatch, html, *, status=200, headers=None, url=A):
    reponse = SimpleNamespace(status_code=status, url=url, history=[], close=lambda: None,
                              headers={"content-type": "text/html; charset=utf-8", **(headers or {})},
                              iter_content=lambda chunk_size: iter([html.encode()]))
    monkeypatch.setattr(m, "_validate_public_crawl_target", lambda u: None)
    monkeypatch.setattr(m.requests, "get", lambda u, **kw: reponse)


NEXT = ('<!DOCTYPE html><html><head><script src="/_next/static/chunks/main.js" defer></script>'
        '<link rel="canonical" href="' + A + '"/><meta name="robots" content="noindex, follow"/></head>'
        '<body><link rel="canonical" href="' + C + '"/><div id="__next"></div></body></html>')


def test_a_framework_page_is_read_scripts_and_all_head_only(monkeypatch) -> None:
    servir(monkeypatch, NEXT)
    assert m._page_servie(A) == {"canonicals": [A], "robots": ["noindex, follow"], "x_robots": ""}


@pytest.mark.parametrize("change", [dict(status=301), dict(status=404), dict(url=C),
                                    dict(headers={"content-type": "application/json"}),
                                    dict(headers={"retry-after": "30"})])
def test_anything_but_a_direct_html_200_is_no_witness(monkeypatch, change) -> None:
    servir(monkeypatch, NEXT, **change)
    assert m._page_servie(A) is None


def test_the_witness_must_repeat_the_observation(monkeypatch) -> None:
    servir(monkeypatch, NEXT)
    assert m._page_servie_confirme(A, noindex="noindex, follow")
    assert not m._page_servie_confirme(A, noindex="noindex, nofollow")
    assert m._page_servie_confirme(A, canonical=A)
    assert not m._page_servie_confirme(A, canonical=C)
    assert not m._page_servie_confirme(A, canonical=A, indexable=True)   # elle est noindex


def test_two_robots_directives_make_no_witness(monkeypatch) -> None:
    # Laquelle retient le moteur ? On ne sait pas : on ne retire rien sur ce doute.
    servir(monkeypatch, NEXT.replace("</head>", '<meta name="robots" content="index, follow"/></head>'))
    assert not m._page_servie_confirme(A, noindex="noindex, follow")


# --- le pipeline ----------------------------------------------------------------------------

def run(monkeypatch, paths, sources, *, temoin=None, key="sitemap_noindex_page", rows=None, urls=(A,)):
    writes = {}

    def get(path, **kw):
        name = unquote(path.split("/contents/", 1)[1])
        return {"sha": "original", "content": base64.b64encode(sources[name].encode()).decode()}

    def put(path, **kw):
        writes[unquote(path.split("/contents/", 1)[1])] = base64.b64decode(kw["json_body"]["content"]).decode()
        return {"content": {"sha": "patched"}}

    monkeypatch.setattr(m, "_github_api_get", get)
    monkeypatch.setattr(m, "_github_api_put", put)
    monkeypatch.setattr(m, "_page_servie", temoin or (lambda url: pytest.fail("la source litterale suffisait")))
    for name in ("_openai_generate_file_patch", "_ai_map_urls_to_files", "_ai_pick_repo_files", "_github_tarball_grep"):
        monkeypatch.setattr(m, name, lambda *a, **kw: pytest.fail("aucun modele pour un sitemap"))
    rows = rows or [noindex_row(A)]
    prep = m._prepare_issue_fix(issue_key=key, issues={key: {"count": len(urls), "examples": list(urls)}},
                                impacted=list(urls), all_paths=paths, site_name="site.test", owner="o", repo_name="r",
                                branch="main", token="t", pages=rows)
    applied = m._apply_prepared_issue_fix(owner="o", repo_name="r", branch="main", token="t", fix_branch="qa",
                                          all_paths=paths, issue_key=key, issue_label=key, impacted=list(urls),
                                          site_name="site.test", file_state={}, max_files=1, prep=prep, pages=rows,
                                          index=repo_index.build_repo_index(paths))
    return applied, writes


SOURCES = {"package.json": '{"dependencies": {"gatsby": "5.0.0"}}', "gatsby-config.js": "module.exports = {}",
           "static/sitemap.xml": sitemap(entry(A), entry(C)), "src/pages/private.js": "export default () => null",
           "src/pages/control.js": "export default () => null"}


def test_a_framework_noindex_page_leaves_the_sitemap_when_the_served_page_confirms(monkeypatch) -> None:
    servie = {"canonicals": [A], "robots": ["noindex, follow"], "x_robots": ""}
    applied, writes = run(monkeypatch, GATSBY, SOURCES, temoin=lambda url: servie)
    assert writes == {"static/sitemap.xml": sitemap(entry(A), entry(C)).replace(entry(A), "", 1)}


def test_a_served_page_that_contradicts_the_crawl_refuses_and_says_why(monkeypatch) -> None:
    servie = {"canonicals": [A], "robots": ["index, follow"], "x_robots": ""}
    applied, writes = run(monkeypatch, GATSBY, SOURCES, temoin=lambda url: servie)
    assert writes == {} and any(s.startswith("Sitemap refuse : page servie contradictoire") for s in applied["skipped"])


def test_a_pure_html_site_keeps_the_strict_literal_rule_and_never_asks_the_network(monkeypatch) -> None:
    # Une page HTML que l'analyseur litteral refuse (script dans l'en-tete) : refus, sans temoin.
    html = ('<html lang="fr"><head><script>window.s = 1;</script><meta name="robots" content="noindex, follow" />'
            '<link rel="canonical" href="' + A + '" /></head><body>Prive</body></html>')
    sources = {"sitemap.xml": sitemap(entry(A), entry(C)), "private.html": html, "control.html": html}
    applied, writes = run(monkeypatch, list(sources), sources)
    assert writes == {} and any("non litterale" in s for s in applied["skipped"])


JEKYLL = ["_config.yml", "sitemap.xml", "private.html", "control.html", "_layouts/default.html"]
ENTETE = "---\npermalink: /sitemap.xml\n---\n"
PAGE_JEKYLL = "---\nlayout: default\ntitle: Prive\n---\nTexte\n"


def test_a_jekyll_sitemap_keeps_its_front_matter_and_loses_the_entry(monkeypatch) -> None:
    # Le sitemap Jekyll est une page : front matter puis XML. Avant le 12/10/2026, l'analyseur
    # XML n'y lisait rien, et le correcteur concluait « rien a changer ».
    sources = {"_config.yml": "title: Site", "sitemap.xml": ENTETE + sitemap(entry(A), entry(C)),
               "private.html": PAGE_JEKYLL, "control.html": PAGE_JEKYLL, "_layouts/default.html": "{{ content }}"}
    servie = {"canonicals": [A], "robots": ["noindex, follow"], "x_robots": ""}
    applied, writes = run(monkeypatch, JEKYLL, sources, temoin=lambda url: servie)
    assert writes == {"sitemap.xml": ENTETE + sitemap(entry(A), entry(C)).replace(entry(A), "", 1)}, applied


def test_a_sitemap_built_by_liquid_is_refused_with_its_reason(monkeypatch) -> None:
    liquid = ENTETE + sitemap(entry(A), "{% for p in site.pages %}<url><loc>{{ p.url }}</loc></url>{% endfor %}")
    sources = {"_config.yml": "title: Site", "sitemap.xml": liquid, "private.html": PAGE_JEKYLL,
               "control.html": PAGE_JEKYLL, "_layouts/default.html": "{{ content }}"}
    servie = {"canonicals": [A], "robots": ["noindex, follow"], "x_robots": ""}
    applied, writes = run(monkeypatch, JEKYLL, sources, temoin=lambda url: servie)
    assert writes == {} and any("Liquid" in s for s in applied["skipped"])


def test_a_framework_page_missing_from_the_sitemap_is_added_when_served_indexable(monkeypatch) -> None:
    rows = [{"url": A, "final_url": A, "status_code": 200, "content_type": "text/html", "canonical": A,
             "meta_robots": None, "redirect_chain": [], "redirect_statuses": [], "x_robots_tag": None}]
    sources = dict(SOURCES, **{"static/sitemap.xml": sitemap(entry(C))})
    monkeypatch.setattr(m, "_sitemap_https_page", lambda url, canonical, **kw: False)   # scripts dans le <head>
    servie = {"canonicals": [A], "robots": [], "x_robots": ""}
    applied, writes = run(monkeypatch, GATSBY, sources, temoin=lambda url: servie,
                          key="indexable_page_not_in_sitemap", rows=rows)
    assert list(writes) == ["static/sitemap.xml"] and A in writes["static/sitemap.xml"], applied
