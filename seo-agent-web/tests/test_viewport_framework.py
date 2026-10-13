# -*- coding: utf-8 -*-
"""Le viewport d'un framework : le modele ECRIT la balise, le code en FIXE la valeur.

Banc complet du 12/10/2026 : `viewport_not_set` rendait « AUCUNE CIBLE » sur astro et hugo — le
plan protege ne lit qu'une page HTML litterale. Meme chemin que la carte Twitter (c288573) : sur
un framework, le modele ecrit la balise dans l'idiome du fichier, avec LA valeur du plan protege,
et un fichier qui en ecrit une autre est refuse.
"""
from __future__ import annotations

import base64
from urllib.parse import unquote

import pytest

from backend import app as m
from backend import repo_index
from backend import viewport
from tests.test_verified_sitemap_https import row

A = "https://site.test/gauntlet/viewport"
VALEUR = "width=device-width, initial-scale=1"
ASTRO = ["astro.config.mjs", "package.json", "src/pages/gauntlet/viewport.astro", "src/layouts/Base.astro"]
PAGE = "---\n---\n<html lang=\"fr\">\n  <head>\n    <title>Page</title>\n  </head>\n</html>\n"
AJOUT = "    <meta name=\"viewport\" content=\"" + VALEUR + "\" />\n"


def sans_viewport(url=A):
    return row(url, meta_viewport=None, meta_viewport_tag_count=0)


def prepare(paths, rows, urls=(A,)):
    key = "viewport_not_set"
    return m._prepare_issue_fix(issue_key=key, issues={key: {"count": len(urls), "examples": list(urls)}},
                                impacted=list(urls), all_paths=paths, site_name="site.test", owner="o",
                                repo_name="r", branch="main", token="t", pages=rows)


def test_the_fixed_value_is_the_protected_plans_own() -> None:
    assert 'content="' + VALEUR + '"' in viewport.TAG


def test_a_framework_page_gets_the_fixed_value_to_write() -> None:
    plan = prepare(ASTRO, [sans_viewport()])
    assert not plan["refusal"] and plan["viewport_urls"] is None
    assert plan["valeurs_imposees"] == {"src/pages/gauntlet/viewport.astro": {"viewport": VALEUR}}
    assert plan["targets_override"] == ["src/pages/gauntlet/viewport.astro"]
    assert 'viewport = "' + VALEUR + '"' in plan["extra_hint"]


def test_literal_stacks_keep_the_protected_plan() -> None:
    plan = prepare(["index.html", "gauntlet/viewport.html"], [sans_viewport()])
    assert plan["viewport_urls"] == [A] and "valeurs_imposees" not in plan


def test_without_an_own_file_the_protected_plan_stays_and_refuses() -> None:
    plan = prepare([p for p in ASTRO if "viewport.astro" not in p], [sans_viewport()])
    assert plan["refusal"] and plan["viewport_urls"] == [A] and "valeurs_imposees" not in plan


def run(monkeypatch, ecrite):
    written = {}
    monkeypatch.setattr(m, "_github_api_get", lambda api, **kw: {
        "sha": "s", "encoding": "base64", "content": base64.b64encode(PAGE.encode()).decode()})

    def put(api, **kw):
        written[unquote(api.split("/contents/", 1)[1])] = base64.b64decode(kw["json_body"]["content"]).decode()
        return {"content": {"sha": "n"}}
    monkeypatch.setattr(m, "_github_api_put", put)
    monkeypatch.setattr(m, "_openai_generate_file_patch", lambda **kw: {"patched_content": ecrite})
    for name in ("_ai_map_urls_to_files", "_ai_pick_repo_files", "_github_tarball_grep"):
        monkeypatch.setattr(m, name, lambda *a, **kw: pytest.fail("la cible est connue"))
    rows = [sans_viewport()]
    applied = m._apply_prepared_issue_fix(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="qa", all_paths=list(ASTRO),
        issue_key="viewport_not_set", issue_label="Viewport", impacted=[A], site_name="site.test",
        file_state={}, max_files=3, prep=prepare(ASTRO, rows), pages=rows, index=repo_index.build_repo_index(ASTRO))
    return applied, written


def test_the_fixed_value_written_in_the_page_is_committed(monkeypatch) -> None:
    corrige = PAGE.replace("  </head>", AJOUT + "  </head>")
    _, written = run(monkeypatch, corrige)
    assert written == {"src/pages/gauntlet/viewport.astro": corrige}


@pytest.mark.parametrize("valeur", ["width=device-width", "width=device-width, initial-scale=1, maximum-scale=1"])
def test_another_value_refuses_the_file(monkeypatch, valeur) -> None:
    applied, written = run(monkeypatch, PAGE.replace("  </head>", AJOUT.replace(VALEUR, valeur) + "  </head>"))
    assert written == {} and applied["patched"] == []
