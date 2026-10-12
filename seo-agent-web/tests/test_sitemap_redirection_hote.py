"""Une entree de sitemap qui redirige par le fait de l'HOTE se reecrit, si c'est pour de bon.

Decision du proprietaire, 12/10/2026. `sitemap_3xx_redirect` n'admettait que les redirections
declarees dans le `_redirects` du depot ; il refusait donc partout celles de l'hote — http vers
https, ajout de la barre finale (`/blog` -> `/blog/` sur gatsby et nuxt) — que le banc mesure sur
les dix stacks. Admises desormais quand : chaque saut observe est PERMANENT (301, 308), aucune
regle du depot n'en couvre un, l'hote redirige TOUJOURS ainsi quand on le relit, et la
destination relue est saine (200, indexable, canonique d'elle-meme ou sans canonical).
"""
from __future__ import annotations

import base64
from types import SimpleNamespace
from urllib.parse import unquote

import pytest

from backend import app as m
from backend import repo_index
from backend import sitemap_redirects as sr
from tests.test_verified_sitemap_canonical import entry, sitemap
from tests.test_verified_sitemap_redirect import KEY, PAIR, A, B, prepare, redirected, row


def ident(url):
    return m._verification_url(url)


# --- la regle -------------------------------------------------------------------------------

@pytest.mark.parametrize("rules, statuses, attendu", [
    ("", [301], True),
    ("/ancienne /nouvelle 301\n", [308], True),      # une regle du depot, mais pour une autre adresse
    ("", [302], False),                              # temporaire : un etat de passage
    ("", [301, 307], False),
    ("/old /master 301\n", [301], False),            # le depot la declare : chemin strict
    ("/* /index.html 200\n", [301], False),          # un _redirects illisible ne prouve rien
])
def test_only_permanent_redirects_no_repository_rule_covers_are_the_hosts(rules, statuses, attendu) -> None:
    rows = [redirected(redirect_chain=[A] * len(statuses), redirect_statuses=statuses), row(B)]
    assert sr.host_redirects(rules, [PAIR], rows, ident) is attendu


def test_an_unobserved_source_is_never_the_hosts() -> None:
    assert sr.host_redirects("", [PAIR], [row(B)], ident) is False


@pytest.mark.parametrize("codes, final, attendu", [
    ([301], B, True), ([301, 308], B, True),
    ([302], B, False), ([301], A + "/autre", False), ([], B, False),
])
def test_the_host_must_still_redirect_permanently_when_reread(monkeypatch, codes, final, attendu) -> None:
    reponse = SimpleNamespace(history=[SimpleNamespace(status_code=c) for c in codes], status_code=200,
                              url=final, close=lambda: None)
    monkeypatch.setattr(m, "_validate_public_crawl_target", lambda u: None)
    monkeypatch.setattr(m.requests, "get", lambda u, **kw: reponse)
    assert m._redirection_permanente_vers(A, B) is attendu


# --- le pipeline ----------------------------------------------------------------------------

ALIAS = entry(A, "<lastmod>2001-01-01</lastmod>")
MASTER = entry(B)
HTML = '<html lang="fr"><head><link rel="canonical" href="' + B + '" /></head><body>Master</body></html>'


def run(monkeypatch, *, sources=None, rows=None, encore=True, saine=True):
    sources = sources or {"sitemap.xml": sitemap(ALIAS, MASTER), "_redirects": "/ancienne /nouvelle 301\n",
                          "master.html": HTML}
    rows = rows or [redirected(), row(B)]
    writes = {}

    def get(path, **kw):
        return {"sha": "original", "content": base64.b64encode(sources[unquote(path.split("/contents/", 1)[1])].encode()).decode()}

    def put(path, **kw):
        writes[unquote(path.split("/contents/", 1)[1])] = base64.b64decode(kw["json_body"]["content"]).decode()
        return {"content": {"sha": "patched"}}

    monkeypatch.setattr(m, "_github_api_get", get)
    monkeypatch.setattr(m, "_github_api_put", put)
    monkeypatch.setattr(m, "_redirection_permanente_vers", lambda source, cible: encore)
    monkeypatch.setattr(m, "_destination_saine", lambda url: saine)
    prep = prepare(rows, paths=list(sources))
    applied = m._apply_prepared_issue_fix(owner="o", repo_name="r", branch="main", token="t", fix_branch="qa",
                                          all_paths=list(sources), issue_key=KEY, issue_label=KEY, impacted=[A],
                                          site_name="site.test", file_state={}, max_files=1, prep=prep, pages=rows,
                                          index=repo_index.build_repo_index(list(sources)))
    return applied, writes


def test_a_permanent_host_redirect_leaves_the_sitemap(monkeypatch) -> None:
    applied, writes = run(monkeypatch)
    assert writes == {"sitemap.xml": sitemap(ALIAS, MASTER).replace(ALIAS, "")}, applied


def test_a_repository_without_any_redirect_file_is_not_ambiguous_for_a_host_redirect(monkeypatch) -> None:
    sources = {"sitemap.xml": sitemap(ALIAS, MASTER), "master.html": HTML}
    applied, writes = run(monkeypatch, sources=sources)
    assert list(writes) == ["sitemap.xml"], applied


@pytest.mark.parametrize("kw", [dict(encore=False), dict(saine=False),
                                dict(rows=[redirected(redirect_statuses=[302]), row(B)])])
def test_a_vanished_temporary_or_unhealthy_redirect_is_refused_with_its_reason(monkeypatch, kw) -> None:
    applied, writes = run(monkeypatch, **kw)
    assert writes == {} and any(s.startswith("Sitemap refuse") for s in applied["skipped"])


def test_an_unreadable_server_config_still_refuses(monkeypatch) -> None:
    sources = {"sitemap.xml": sitemap(ALIAS, MASTER), "master.html": HTML, ".htaccess": "Redirect 301 /old /master"}
    applied, writes = run(monkeypatch, sources=sources)
    assert writes == {} and any("ambigue" in s for s in applied["skipped"])


def test_without_live_proof_a_repository_without_redirect_file_writes_nothing(monkeypatch) -> None:
    sources = {"sitemap.xml": sitemap(ALIAS, MASTER), "master.html": HTML}
    applied, writes = run(monkeypatch, sources=sources, encore=False)
    assert writes == {} and any("ambigue" in s for s in applied["skipped"])
