# -*- coding: utf-8 -*-
"""Une page a gabarits Jinja se corrige dans la PAGE, d'apres le document qu'elle REND.

12/10/2026, banc du dixieme stack (Flask) : la carte des routes posee, deux familles restaient
intactes, et pour la meme raison — la page n'est pas un document.
- `twitter_card_missing` : le plan protege lit la source comme le HTML servi et refuse tout `{%`,
  or `templates/gauntlet/twitter-missing.html` n'est qu'un `{% extends %}` et des blocs.
- `served_html_lang_mismatch` : `<html lang>` est ecrit par `base.html`, commun a tout le site ;
  la seule correction par page est de surcharger le bloc de langue que la mise en page expose.

Ce qui ne bouge pas : un plan protege ne lit que du litteral. Une expression n'est toleree que
dans une valeur d'attribut (`href="{{ url_for(...) }}"`) ou dans le corps ; partout ailleurs
dans l'en-tete, refus. Et la mise en page commune n'est JAMAIS ecrite.
"""
from __future__ import annotations

import base64
from pathlib import Path
from urllib.parse import unquote

import pytest

from backend import app as m
from backend import jinja_heritage as jh
from backend import repo_index
from tests.test_verified_sitemap_https import row

A = "https://site.test/gauntlet/a"
IMAGE = "https://site.test/og.png"
LAYOUT_PATH, PAGE_PATH = "templates/gauntlet/_layout.html", "templates/gauntlet/a.html"
LAYOUT = ("<!doctype html>\n<html{% block html_attrs %}{% endblock %}>\n  <head>\n    <meta charset=\"utf-8\" />\n"
          "{% block head %}{% endblock %}\n  </head>\n  <body>\n{% block body %}{% endblock %}\n  </body>\n</html>\n")
HEAD = ("    {# la page de test #}\n"
        "    <title>Page A</title>\n"
        "    <meta name=\"description\" content=\"Description A\" />\n"
        "    <link rel=\"canonical\" href=\"" + A + "\" />\n"
        "    <meta property=\"og:title\" content=\"Page A\" />\n"
        "    <meta property=\"og:description\" content=\"Description A\" />\n"
        "    <meta property=\"og:image\" content=\"" + IMAGE + "\" />\n"
        "    <meta name=\"twitter:title\" content=\"Page A\" />\n"
        "    <meta name=\"twitter:image\" content=\"" + IMAGE + "\" />\n")
PAGE = ("{% extends \"gauntlet/_layout.html\" %}\n{% block html_attrs %} lang=\"fr\"{% endblock %}\n"
        "{% block head %}\n" + HEAD + "{% endblock %}\n{% block body %}\n    <h1>A</h1>\n"
        "    <a href=\"{{ url_for('index') }}\">Accueil</a>\n{% endblock %}\n")
SOURCES = {"app.py": "from flask import Flask\n", "requirements.txt": "flask\n", LAYOUT_PATH: LAYOUT, PAGE_PATH: PAGE}
TAGS = ("    <meta name=\"twitter:card\" content=\"summary_large_image\" />\n"
        "    <meta name=\"twitter:description\" content=\"Description A\" />\n")
TWITTER = dict(twitter_card=None, twitter_title="Page A", twitter_description=None, twitter_image=IMAGE,
               og_title="Page A", og_description="Description A", og_image=IMAGE, title="Page A",
               meta_description="Description A")


def forbidden(*a, **kw):
    pytest.fail("ni modele ni ciblage heuristique pour une page a gabarits prouvee")


def run(monkeypatch, sources, key, rows, urls=(A,)):
    written = {}

    def get(api, **kw):
        path = unquote(api.split("/contents/", 1)[1])
        return {"sha": "original", "encoding": "base64", "content": base64.b64encode(sources[path].encode()).decode()}

    def put(api, **kw):
        path = unquote(api.split("/contents/", 1)[1])
        written[path] = base64.b64decode(kw["json_body"]["content"]).decode()
        return {"content": {"sha": "new"}}

    monkeypatch.setattr(m, "_github_api_get", get)
    monkeypatch.setattr(m, "_github_api_put", put)
    monkeypatch.setattr(m, "_twitter_missing_page", lambda url, canonical, observed: True, raising=False)
    monkeypatch.setattr(m, "_sitemap_https_page", lambda url, canonical, **kw: True)
    for name in ("_openai_generate_file_patch", "_ai_map_urls_to_files", "_ai_pick_repo_files", "_github_tarball_grep"):
        monkeypatch.setattr(m, name, forbidden)
    urls = list(urls)
    prep = m._prepare_issue_fix(issue_key=key, issues={key: {"count": len(urls), "examples": urls}}, impacted=urls,
        all_paths=list(sources), site_name="site.test", owner="o", repo_name="r", branch="main", token="t", pages=rows)
    applied = m._apply_prepared_issue_fix(owner="o", repo_name="r", branch="main", token="t", fix_branch="qa",
        all_paths=list(sources), issue_key=key, issue_label=key, impacted=urls, site_name="site.test", file_state={},
        max_files=3, prep=prep, pages=rows, index=repo_index.build_repo_index(list(sources)), allow_ai_targeting=True)
    return applied, written


def test_the_card_lands_at_the_end_of_the_page_head_block_and_nowhere_else(monkeypatch) -> None:
    applied, written = run(monkeypatch, SOURCES, "twitter_card_missing", [row(A, **TWITTER)])
    assert written == {PAGE_PATH: PAGE.replace(HEAD, HEAD + TAGS)}
    assert applied["patched"] == [PAGE_PATH] and not applied["ai_files"]


def test_a_url_for_in_an_attribute_of_the_layout_head_is_a_value_not_markup(monkeypatch) -> None:
    layout = LAYOUT.replace("    <meta charset", "    <link rel=\"icon\" href=\"{{ url_for('static', filename='f.png') }}\" />\n    <meta charset")
    _, written = run(monkeypatch, dict(SOURCES, **{LAYOUT_PATH: layout}), "twitter_card_missing", [row(A, **TWITTER)])
    assert written == {PAGE_PATH: PAGE.replace(HEAD, HEAD + TAGS)}


@pytest.mark.parametrize("change", [
    # Du balisage calcule dans l'en-tete : le document rendu n'est plus connu.
    (LAYOUT_PATH, "    <meta charset=\"utf-8\" />\n", "    <meta charset=\"utf-8\" />\n    {{ extra_head|safe }}\n"),
    (PAGE_PATH, "    <title>", "    {% if draft %}<meta name=\"robots\" content=\"noindex\" />{% endif %}\n    <title>"),
    (PAGE_PATH, "    <title>", "    {{ super() }}\n    <title>"),
    (PAGE_PATH, "{% block head %}", "{%- block head %}"),
    (LAYOUT_PATH, "{% block head %}{% endblock %}", "{% block head %}{% block meta %}{% endblock %}{% endblock %}"),
    # Du texte hors bloc dans une page qui etend : un gabarit qu'on comprend mal.
    (PAGE_PATH, "{% block body %}", "<p>perdu</p>\n{% block body %}"),
    # Une valeur que le plan compare au crawl, mais calculee : elle ne prouve rien.
    (PAGE_PATH, "content=\"" + IMAGE + "\" />\n    <meta name=\"twitter:title\"",
     "content=\"{{ og_image }}\" />\n    <meta name=\"twitter:title\""),
    # La source ne dit plus ce que le crawl a vu.
    (PAGE_PATH, "og:description\" content=\"Description A\"", "og:description\" content=\"Autre\""),
    # Mise en page absente : rien a composer.
    (LAYOUT_PATH, None, None),
])
def test_anything_not_literal_where_the_plan_reads_refuses_without_writing(monkeypatch, change) -> None:
    path, old, new = change
    sources = dict(SOURCES)
    if old is None:
        del sources[path]
    else:
        assert sources[path].count(old) == 1
        sources[path] = sources[path].replace(old, new)
    _, written = run(monkeypatch, sources, "twitter_card_missing", [row(A, **TWITTER)])
    assert written == {}


def test_the_shared_layout_is_never_written_even_when_it_carries_the_head(monkeypatch) -> None:
    layout = LAYOUT.replace("{% block head %}{% endblock %}", "{% block head %}\n" + HEAD + "{% endblock %}")
    page = PAGE.replace("{% block head %}\n" + HEAD + "{% endblock %}\n", "")
    _, written = run(monkeypatch, dict(SOURCES, **{LAYOUT_PATH: layout, PAGE_PATH: page}),
                     "twitter_card_missing", [row(A, **TWITTER)])
    assert written == {}


def test_the_last_head_block_the_page_owns_is_the_one_written(monkeypatch) -> None:
    # La mise en page rend apres celui de la page un bloc qu'elle remplit seule : y ecrire
    # voudrait dire ecrire dans la mise en page commune.
    layout = LAYOUT.replace("{% block head %}{% endblock %}\n",
                            "{% block head %}{% endblock %}\n{% block extra %}    <meta name=\"theme-color\" content=\"#fff\" />\n{% endblock %}\n")
    _, written = run(monkeypatch, dict(SOURCES, **{LAYOUT_PATH: layout}), "twitter_card_missing", [row(A, **TWITTER)])
    assert written == {PAGE_PATH: PAGE.replace(HEAD, HEAD + TAGS)}


def test_a_value_that_jinja_would_read_as_code_is_never_written_into_a_template(monkeypatch) -> None:
    # `&#123;&#123;` est du texte dans la page servie ; recopie en clair dans un gabarit, ce
    # serait une expression Jinja — ou une erreur de syntaxe qui casse le build du client.
    page = PAGE.replace("og:description\" content=\"Description A\"", "og:description\" content=\"Code &#123;&#123; promo &#125;&#125;\"")
    observed = dict(TWITTER, og_description="Code {{ promo }}")
    _, written = run(monkeypatch, dict(SOURCES, **{PAGE_PATH: page}), "twitter_card_missing", [row(A, **observed)])
    assert written == {}


@pytest.mark.parametrize("ouvre, ferme", [
    # Dans un commentaire, les balises n'existent pas : le plan relu voudrait encore les ajouter.
    ("    <!--", "-->"),
    # Dans un element, l'en-tete devient ambigu : le plan relu ne le lit plus du tout.
    ("    <div hidden>", "</div>"),
])
def test_tags_that_would_land_inside_something_the_layout_closes_are_refused(monkeypatch, ouvre, ferme) -> None:
    layout = LAYOUT.replace("{% block head %}{% endblock %}\n", "{% block head %}{% endblock %}" + ferme + "\n")
    page = PAGE.replace(HEAD + "{% endblock %}", HEAD + ouvre + "{% endblock %}")
    _, written = run(monkeypatch, dict(SOURCES, **{LAYOUT_PATH: layout, PAGE_PATH: page}),
                     "twitter_card_missing", [row(A, **TWITTER)])
    assert written == {}


def test_a_plan_that_changes_anything_else_cannot_be_carried_into_the_page() -> None:
    def lecteur(path):
        return SOURCES.get(path)
    view = jh.literal_view(PAGE_PATH, lecteur)
    tag = "<meta name=\"twitter:card\" content=\"summary\" />"
    ajout = view.replace("  </head>", "    " + tag + "\n  </head>")
    assert jh.add_to_head(PAGE, PAGE_PATH, lecteur, view, ajout) == PAGE.replace(HEAD, HEAD + "    " + tag + "\n")
    for autre in (ajout.replace("<title>Page A</title>", "<title>Autre</title>"),       # pas une meta
                  ajout.replace("content=\"Page A\"", "content=\"Autre\"", 1)):          # une meta existante
        assert jh.add_to_head(PAGE, PAGE_PATH, lecteur, view, autre) is None


def test_a_missing_viewport_takes_the_same_road(monkeypatch) -> None:
    rows = [row(A, meta_viewport=None, meta_viewport_tag_count=0)]
    _, written = run(monkeypatch, SOURCES, "viewport_not_set", rows)
    tag = "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n"
    assert written == {PAGE_PATH: PAGE.replace(HEAD, HEAD + tag)}


# --- La langue servie --------------------------------------------------------------------------

HOME = "https://site.test/"
BASE = ("<!doctype html>\n<html lang=\"{% block lang %}fr{% endblock %}\">\n  <head>\n"
        "    <title>{% block title %}{% endblock %}</title>\n"
        "    <link rel=\"icon\" href=\"{{ url_for('static', filename='f.png') }}\" />\n"
        "    {% block head %}{% endblock %}\n  </head>\n  <body>{% block body %}{% endblock %}</body>\n</html>\n")
INDEX = ("{% extends \"base.html\" %}\n{% block title %}Accueil{% endblock %}\n{% block head %}\n"
         "    <link rel=\"canonical\" href=\"" + HOME + "\" />\n"
         "    <link rel=\"alternate\" hreflang=\"en\" href=\"" + HOME + "\" />\n"
         "    <link rel=\"alternate\" hreflang=\"fr\" href=\"https://site.test/a-propos\" />\n"
         "{% endblock %}\n{% block body %}<h1>Home</h1>{% endblock %}\n")
LANG_SOURCES = {"app.py": "from flask import Flask\n", "requirements.txt": "flask\n",
                "templates/base.html": BASE, "templates/index.html": INDEX}


def lire(sources):
    return lambda path: sources.get(path)


@pytest.mark.parametrize("base, index, expected", [
    # Le bloc EST la valeur : la page le declare juste apres son extends.
    (BASE, INDEX, INDEX.replace("{% extends \"base.html\" %}\n", "{% extends \"base.html\" %}\n{% block lang %}en{% endblock %}\n")),
    # La page le surcharge deja, avec la mauvaise langue : on remplace sa valeur.
    (BASE, INDEX.replace("{% block title %}", "{% block lang %}fr{% endblock %}\n{% block title %}"),
     INDEX.replace("{% block title %}", "{% block lang %}en{% endblock %}\n{% block title %}")),
    # Le bloc porte les attributs : on change la valeur de lang, et rien d'autre.
    (BASE.replace("<html lang=\"{% block lang %}fr{% endblock %}\">", "<html{% block html_attrs %} lang=\"fr\"{% endblock %}>"),
     INDEX.replace("{% block title %}", "{% block html_attrs %} lang=\"fr\" dir=\"ltr\"{% endblock %}\n{% block title %}"),
     INDEX.replace("{% block title %}", "{% block html_attrs %} lang=\"en\" dir=\"ltr\"{% endblock %}\n{% block title %}")),
    # Bloc d'attributs vide dans la page : on y ajoute la langue.
    (BASE.replace("<html lang=\"{% block lang %}fr{% endblock %}\">", "<html{% block html_attrs %}{% endblock %}>"),
     INDEX.replace("{% block title %}", "{% block html_attrs %}{% endblock %}\n{% block title %}"),
     INDEX.replace("{% block title %}", "{% block html_attrs %} lang=\"en\"{% endblock %}\n{% block title %}")),
])
def test_the_page_overrides_the_language_block_of_its_layout(base, index, expected) -> None:
    sources = dict(LANG_SOURCES, **{"templates/base.html": base, "templates/index.html": index})
    assert m._rewrite_served_lang_gabarit(index, "templates/index.html", lire(sources)) == (expected, 1)
    assert jh.rendered_lang("templates/index.html", lire(dict(sources, **{"templates/index.html": expected}))) == "en"


@pytest.mark.parametrize("base, index", [
    # Langue fixe dans la mise en page commune : pas de prise par page, rien n'est ecrit.
    (BASE.replace("{% block lang %}fr{% endblock %}", "fr"), INDEX),
    # Langue calculee par la mise en page : elle ne se lit pas.
    (BASE.replace("{% block lang %}fr{% endblock %}", "{{ lang }}"), INDEX),
    # Le bloc par defaut porte autre chose qu'une langue.
    (BASE.replace("{% block lang %}fr{% endblock %}", "{% block lang %}{{ g.lang }}{% endblock %}"), INDEX),
    # La page ne dit pas sa langue (pas de hreflang auto-reference).
    (BASE, INDEX.replace("hreflang=\"en\" href=\"" + HOME + "\"", "hreflang=\"en\" href=\"https://site.test/en/\"")),
    # Elle la dit deja juste : rien a faire.
    (BASE, INDEX.replace("hreflang=\"en\"", "hreflang=\"fr-FR\"").replace("hreflang=\"fr\" href=\"https://site.test/a-propos\"", "hreflang=\"de\" href=\"https://site.test/de\"")),
    # Une page racine sans extends n'a pas de mise en page a surcharger.
    (BASE, INDEX.replace("{% extends \"base.html\" %}\n", "")),
    # Le seul bloc de la balise <html> n'est pas la langue.
    (BASE.replace("<html lang=\"{% block lang %}fr{% endblock %}\">", "<html class=\"{% block cls %}{% endblock %}\" lang=\"fr\">"), INDEX),
    # Deux langues dans les attributs de la page : le rendu ne dirait toujours pas une seule langue.
    (BASE.replace("<html lang=\"{% block lang %}fr{% endblock %}\">", "<html{% block html_attrs %}{% endblock %}>"),
     INDEX.replace("{% block title %}", "{% block html_attrs %} lang=\"fr\" xml:lang=\"de\" lang=\"de\"{% endblock %}\n{% block title %}")),
])
def test_no_per_page_handle_means_nothing_is_written(base, index) -> None:
    sources = dict(LANG_SOURCES, **{"templates/base.html": base, "templates/index.html": index})
    assert m._rewrite_served_lang_gabarit(index, "templates/index.html", lire(sources)) == (index, 0)


def test_the_page_wins_over_an_intermediate_layout_as_jinja_resolves_it() -> None:
    # base -> section -> page : la section fixe sa langue, la page la surcharge, et c'est la page
    # que Jinja retient — l'enfant le plus derive l'emporte.
    section = "{% extends \"base.html\" %}\n{% block lang %}de{% endblock %}\n"
    index = INDEX.replace("{% extends \"base.html\" %}", "{% extends \"section.html\" %}")
    sources = dict(LANG_SOURCES, **{"templates/section.html": section, "templates/index.html": index})
    fixed, n = m._rewrite_served_lang_gabarit(index, "templates/index.html", lire(sources))
    assert n == 1 and "{% block lang %}en{% endblock %}" in fixed
    assert jh.rendered_lang("templates/index.html", lire(dict(sources, **{"templates/index.html": fixed}))) == "en"


def test_the_served_lang_family_is_deterministic_and_targets_the_page_on_a_template_site(monkeypatch) -> None:
    assert m._served_lang_strategy(list(LANG_SOURCES)) == "deterministic"
    rows = [row(HOME, title="Accueil", served_lang="fr", lang="fr",
                hreflang={"en": HOME, "fr": "https://site.test/a-propos"})]
    applied, written = run(monkeypatch, LANG_SOURCES, "served_html_lang_mismatch", rows, urls=(HOME,))
    assert list(written) == ["templates/index.html"] and "{% block lang %}en{% endblock %}" in written["templates/index.html"]
    assert not applied["ai_files"]


def test_the_flask_fixture_itself_is_repaired_both_ways() -> None:
    root = Path(__file__).resolve().parent / "fixtures" / "flask"

    def lecteur(path):
        f = root / path
        return f.read_text(encoding="utf-8") if f.is_file() else None

    twitter = "templates/gauntlet/twitter-missing.html"
    view = jh.literal_view(twitter, lecteur)
    assert view and "{" not in view.split("</head>")[0]
    index = lecteur("templates/index.html")
    fixed, n = m._rewrite_served_lang_gabarit(index, "templates/index.html", lecteur)
    assert n == 1 and jh.rendered_lang("templates/index.html", lambda p: fixed if p == "templates/index.html" else lecteur(p)) == "en"
