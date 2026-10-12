# -*- coding: utf-8 -*-
"""La PR #1 de noyaru.com, rejouée ligne par ligne contre les garde-fous du 12/10/2026.

Premier passage du correcteur sur le site du produit lui-même (gabarits Jinja servis par FastAPI,
un idiome qu'aucun des neuf sites du banc ne porte). « Open Graph incomplet — 8 fichiers ». Sa
propre CI l'a rejetée, et la relecture a trouvé quatre défauts :
- /pricing et /ressources-seo : `url_for('static', filename=..., _external=True)`, syntaxe Flask ;
  Starlette lève NoMatchFound -> erreur 500 (c'est ce qu'a vu la CI) ;
- six pages pointées vers /static/og-image.png, qui n'existe pas (404) ;
- og:image et og:type posés dans `public_layout.html` EN PLUS des pages qui l'étendent ;
- des fixtures de tests (`tests/fixtures/next-app/app/layout.tsx`) parmi les cibles.
Chaque ligne ajoutée ci-dessous est celle de la PR, au caractère près.
"""
from __future__ import annotations

import pytest

from backend import app as m

REPO = ["seo-agent-web/templates/pricing_public.html", "seo-agent-web/templates/public_layout.html",
        "seo-agent-web/static/brand/noyaru-logo.png", "seo-agent-web/static/styles.css"]

PRICING = """{% extends "public_layout.html" %}

{% block title %}Tarifs - {{ app_name }}{% endblock %}

{% block head_extra %}
<meta property="og:title" content="Tarifs - {{ app_name }}" />
<meta property="og:type" content="website" />
<meta property="og:url" content="{{ canonical_url }}" />
{% endblock %}
"""
DOCS = PRICING.replace("Tarifs - {{ app_name }}", "{{ page.title }}")
LAYOUT = """<!doctype html>
<html lang="fr">
  <head>
    <title>{% block title %}{{ app_name }}{% endblock %}</title>
    <link rel="stylesheet" href="/static/styles.css?v=16" />
    {% block head_extra %}{% endblock %}
  </head>
  <body></body>
</html>
"""
INDEX = """<!doctype html>
<html lang="fr">
  <head>
    <title>Noyaru (local)</title>
  </head>
  <body></body>
</html>
"""
OG_URL = '<meta property="og:url" content="{{ canonical_url }}" />\n'


def _apres_og_url(raw: str, ajout: str) -> str:
    return raw.replace(OG_URL, OG_URL + ajout + "\n", 1)


def _garde(new: str, raw: str, path: str, *, repo=REPO, pages=None):
    return m._retirer_ajouts_non_prouves(new, raw, path, all_paths=repo, pages=pages or [])


@pytest.mark.parametrize("ajout", [
    """<meta property="og:image" content="{{ url_for('static', filename='og-image.png', _external=True) if url_for is defined else '/static/og-image.png' }}" />""",
])
def test_a_flask_url_for_never_reaches_a_starlette_template(ajout) -> None:
    out, notes = _garde(_apres_og_url(PRICING, ajout), PRICING, "seo-agent-web/templates/pricing_public.html")
    assert out == PRICING
    assert notes and "url_for(" in notes[0]


@pytest.mark.parametrize("raw, ajout", [
    (DOCS, """<meta property="og:image" content="{{ og_image | default('https://noyaru.com/static/og-image.png', true) }}" />"""),
    (PRICING, '<meta property="og:image" content="{{ canonical_url }}static/og-image.png" />'),
    (PRICING, '<meta property="og:image" content="{{ request.base_url }}static/og-image.png" />'),
])
def test_an_image_that_exists_nowhere_is_removed(raw, ajout) -> None:
    out, notes = _garde(_apres_og_url(raw, ajout), raw, "seo-agent-web/templates/page.html")
    assert out == raw and notes


def test_the_same_line_is_kept_once_the_image_exists_in_the_repository() -> None:
    ajout = '<meta property="og:image" content="{{ canonical_url }}static/og-image.png" />'
    new = _apres_og_url(PRICING, ajout)
    # Un litteral entre guillemets dans l'expression designe le fichier : ici aucun, donc refus.
    assert _garde(new, PRICING, "seo-agent-web/templates/page.html",
                  repo=REPO + ["seo-agent-web/static/og-image.png"])[0] == PRICING
    literal = '<meta property="og:image" content="https://noyaru.com/static/og-image.png" />'
    new = _apres_og_url(PRICING, literal)
    assert _garde(new, PRICING, "seo-agent-web/templates/page.html",
                  repo=REPO + ["seo-agent-web/static/og-image.png"])[0] == new


def test_an_image_already_used_by_the_site_is_kept() -> None:
    literal = '<meta property="og:image" content="https://cdn.site.test/partage.png" />'
    new = _apres_og_url(PRICING, literal)
    pages = [{"url": "https://site.test/", "og_image": "https://cdn.site.test/partage.png"}]
    assert _garde(new, PRICING, "seo-agent-web/templates/page.html", pages=pages)[0] == new


def test_a_known_template_call_with_known_arguments_is_kept() -> None:
    raw = PRICING.replace(OG_URL, OG_URL + "<link rel=\"icon\" href=\"{{ url_for('static', path='brand/noyaru-logo.png') }}\" />\n")
    ajout = """<meta property="og:image" content="{{ url_for('static', path='brand/noyaru-logo.png') }}" />"""
    new = _apres_og_url(raw, ajout)
    assert _garde(new, raw, "seo-agent-web/templates/page.html")[0] == new


def test_a_known_call_with_an_unknown_keyword_is_removed() -> None:
    """Le cas exact Flask/Starlette : le fichier sait `url_for('static', path=...)`, le patch
    ecrit `url_for('static', filename=...)` vers une image qui EXISTE — seul l'argument trahit."""
    raw = PRICING.replace(OG_URL, OG_URL + "<link rel=\"icon\" href=\"{{ url_for('static', path='brand/noyaru-logo.png') }}\" />\n")
    ajout = """<meta property="og:image" content="{{ url_for('static', filename='brand/noyaru-logo.png') }}" />"""
    out, notes = _garde(_apres_og_url(raw, ajout), raw, "seo-agent-web/templates/page.html")
    assert out == raw and "filename=" in notes[0] and "url_for(" not in notes[0]


def test_a_shared_layout_whose_head_opens_a_block_gets_no_social_tag() -> None:
    ajout = ('    <meta property="og:image" content="https://noyaru.com/static/brand/noyaru-logo.png?v=3" />\n'
             '    <meta property="og:type" content="website" />\n')
    new = LAYOUT.replace("    {% block head_extra %}", ajout + "    {% block head_extra %}", 1)
    out, notes = _garde(new, LAYOUT, "seo-agent-web/templates/public_layout.html")
    assert out == LAYOUT
    assert len(notes) == 2 and all("bloc" in n for n in notes)


def test_a_standalone_page_keeps_its_proved_tags_and_loses_the_invented_image() -> None:
    ajout = ('    <meta property="og:image" content="https://noyaru.com/static/og-image.png" />\n'
             '    <meta property="og:title" content="Noyaru (local)" />\n')
    new = INDEX.replace("  </head>", ajout + "  </head>", 1)
    out, notes = _garde(new, INDEX, "seo-agent-web/templates/index.html")
    assert "og:image" not in out and 'og:title" content="Noyaru (local)"' in out
    assert len(notes) == 1


def test_a_jsx_expression_is_not_judged_by_this_guard() -> None:
    raw = "<head>\n  <title>x</title>\n</head>\n"
    new = raw.replace("</head>", '  <meta property="og:image" content={ogImage} />\n</head>')
    assert _garde(new, raw, "src/layouts/Base.astro")[0] == new


@pytest.mark.parametrize("path, hors_site", [
    ("seo-agent-web/tests/fixtures/next-app/app/layout.tsx", True),
    ("tests/test_home.py", True), ("src/components/Button.test.tsx", True),
    ("src/__mocks__/head.ts", True), ("e2e/home.spec.ts", True),
    ("app/layout.tsx", False), ("seo-agent-web/templates/pricing_public.html", False),
    ("content/blog/testing-en-production.md", False),
])
def test_tests_fixtures_and_mocks_are_never_targets(path, hors_site) -> None:
    assert m._chemin_hors_site(path) is hors_site


def test_the_write_step_drops_a_fixture_even_when_named(monkeypatch) -> None:
    monkeypatch.setattr(m, "_github_api_get", lambda *a, **kw: pytest.fail("a fixture must never be read"))
    _patched, _skipped, targets, _ai = m._deep_patch_issue_files(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="qa",
        all_paths=["seo-agent-web/tests/fixtures/next-app/app/layout.tsx"], issue_key="open_graph_tags_incomplete",
        issue_label="Open Graph", impacted_urls=["https://site.test/"], site_name="site.test", file_state={},
        max_files=2, targets_override=["seo-agent-web/tests/fixtures/next-app/app/layout.tsx"])
    assert targets == []
