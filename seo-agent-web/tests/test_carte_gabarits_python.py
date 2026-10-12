# -*- coding: utf-8 -*-
"""Un dépôt à gabarits serveur Python a sa carte des routes, et elle ne devine pas.

12/10/2026, banc du dixième stack (Flask) : le dépôt était lu comme du HTML statique.
`templates/gauntlet/missing-alt.html` était rangé sous l'URL `/templates/gauntlet/missing-alt`,
que le site ne sert pas ; la vraie URL n'avait aucun fichier, et le correcteur DEVINAIT — la mise
en page commune `base.html` ou l'index statique du parcours, à la place du gabarit de la page.
Sur sept familles, aucune correction.

Deux garde-fous tiennent la convention de nom (`/x` -> `templates/x.html`) :
- les fixtures n'entrent pas dans la carte (dans le dépôt du produit, `/` pointait aussi vers
  `tests/fixtures/flask/templates/index.html`) ;
- un gabarit dont le titre ÉCRIT EN DUR n'est pas celui de la page servie n'est pas cette page :
  `templates/index.html` du produit est l'application interne (« Noyaru (local) »), pas l'accueil
  public — le fichier que la PR #1 avait visé.
"""
from __future__ import annotations

import base64

import pytest

from backend import app as m
from backend import repo_index as ri

FLASK = ["app.py", "freeze.py", "requirements.txt", "README.md",
         "templates/base.html", "templates/index.html", "templates/a_propos.html", "templates/blog.html",
         "templates/gauntlet/_layout.html", "templates/gauntlet/missing-alt.html",
         "templates/partials/nav.html", "templates/emails/welcome.html",
         "public/gauntlet/index.html", "public/robots.txt", "static/favicon.png"]


def test_a_flask_repository_is_read_as_python_templates_not_static_html() -> None:
    assert ri.detect_stack(FLASK) == ri.STACK_PY_TEMPLATES
    # Sans dependances Python, des .html restent du HTML statique.
    assert ri.detect_stack([p for p in FLASK if p != "requirements.txt"]) == ri.STACK_STATIC
    assert ri.detect_stack(["index.html", "a-propos.html"]) == ri.STACK_STATIC


@pytest.mark.parametrize("url, attendu", [
    ("/", ["templates/index.html"]),
    ("/a-propos", ["templates/a_propos.html"]),
    ("/a_propos", ["templates/a_propos.html"]),
    ("/blog", ["templates/blog.html"]),
    ("https://site.test/gauntlet/missing-alt", ["templates/gauntlet/missing-alt.html"]),
    ("/gauntlet", ["public/gauntlet/index.html"]),
    ("/templates/gauntlet/missing-alt", []),
    ("/partials/nav", []), ("/emails/welcome", []),
])
def test_each_url_maps_to_its_own_template(url, attendu) -> None:
    assert ri.route_files(ri.build_repo_index(FLASK), url) == attendu


def test_layouts_are_shared_never_a_page_target() -> None:
    index = ri.build_repo_index(FLASK)
    assert ri.is_shared_path(index, "templates/base.html")
    assert ri.is_shared_path(index, "templates/gauntlet/_layout.html")


def test_fixtures_never_enter_the_map() -> None:
    produit = ["seo-agent-web/backend/app.py", "seo-agent-web/requirements.txt",
               "seo-agent-web/templates/index.html", "seo-agent-web/templates/home_public.html",
               "seo-agent-web/tests/fixtures/flask/templates/index.html",
               "seo-agent-web/tests/fixtures/next-app/app/layout.tsx"]
    index = ri.build_repo_index(produit)
    assert index["stack"] == ri.STACK_PY_TEMPLATES, "a fixture's layout.tsx must not make it Next.js"
    assert ri.route_files(index, "/") == ["seo-agent-web/templates/index.html"]


@pytest.mark.parametrize("raw, servi, contredit", [
    ("<html><head><title>Noyaru (local)</title></head></html>", "Noyaru - L'audit SEO qui livre le correctif", True),
    ("<title>Noyaru  (local)</title>", "noyaru (local)", False),
    ('{% extends "base.html" %}{% block title %}Accueil du site{% endblock %}', "Accueil du site", False),
    ('{% block title %}Tarifs - {{ app_name }}{% endblock %}', "Tarifs - Noyaru", False),
    ("<title>{{ page.title }}</title>", "Autre chose", False),
    ("<p>pas de titre</p>", "Accueil", False),
    ("<title>Ecrit en dur</title>", "", False),
    ("<title>L&#39;audit</title>", "L'audit", False),
])
def test_a_hardcoded_title_that_is_not_the_served_one_disproves_the_template(raw, servi, contredit) -> None:
    assert bool(m._gabarit_d_une_autre_page(raw, servi)) is contredit


def test_the_write_step_refuses_the_internal_app_page_for_the_public_home(monkeypatch) -> None:
    raw = "<!doctype html><html><head><title>Noyaru (local)</title></head><body></body></html>"
    paths = ["app.py", "requirements.txt", "templates/index.html", "templates/base.html"]
    puts = []
    monkeypatch.setattr(m, "_github_api_get", lambda *a, **kw: {
        "sha": "s", "content": base64.b64encode(raw.encode()).decode()})
    monkeypatch.setattr(m, "_github_api_put", lambda *a, **kw: puts.append(a) or {"content": {"sha": "n"}})
    monkeypatch.setattr(m, "_openai_generate_file_patch", lambda **kw: pytest.fail("never ask the model for the wrong page"))
    patched, skipped, targets, _ai = m._deep_patch_issue_files(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="qa", all_paths=paths,
        issue_key="open_graph_tags_incomplete", issue_label="Open Graph", impacted_urls=["https://site.test/"],
        site_name="site.test", file_state={}, max_files=2, index=ri.build_repo_index(paths),
        targets_override=["templates/index.html"],
        pages=[{"url": "https://site.test/", "title": "Noyaru - L'audit SEO qui livre le correctif"}])
    assert patched == [] and puts == [] and "templates/index.html" in skipped
