# -*- coding: utf-8 -*-
"""Une page a gabarits Python sans langue se corrige sans modele — et jamais en `<htmllang`.

Banc complet du 12/10/2026, Flask : `html_lang_attribute_missing` annoncee « ok », la page
servie toujours sans langue. Le modele avait ecrit `{% block html_attrs %}lang="fr"{% endblock %}`
sous une mise en page `<html{% block html_attrs %}{% endblock %}>` : sans l'espace, la page
servie disait `<htmllang="fr">`. Et `hreflang_defined_but_html_lang_missing` rendait « AUCUN
PATCH » : son plan protege ne lit que du HTML litteral.

Les deux familles passent par `jinja_heritage.served_lang`, qui pose l'espace et relit la langue
rendue ; et toute reecriture d'un gabarit Python par le modele est refusee si la page rendue colle
un attribut a `<html`.
"""
from __future__ import annotations

import base64
from urllib.parse import unquote

import pytest

from backend import app as m
from backend import jinja_heritage as jh
from backend import repo_index

SITE = "https://site.test"
PAGE_URL = SITE + "/gauntlet/sans-lang"
LAYOUT = ("<!doctype html>\n<html{% block html_attrs %}{% endblock %}>\n  <head>\n    <meta charset=\"utf-8\" />\n"
          "{% block head %}{% endblock %}\n  </head>\n  <body>{% block body %}{% endblock %}</body>\n</html>\n")
SANS = ("{% extends \"gauntlet/_layout.html\" %}\n{% block html_attrs %}{% endblock %}\n{% block head %}\n"
        "    <title>Sans langue</title>\n    <link rel=\"canonical\" href=\"" + PAGE_URL + "\" />\n"
        "{% endblock %}\n{% block body %}<h1>Page</h1>{% endblock %}\n")
AVEC_HREFLANG = SANS.replace("    <title>", "    <link rel=\"alternate\" hreflang=\"de\" href=\"" + PAGE_URL + "\" />\n    <title>")
PATHS = ["app.py", "requirements.txt", "templates/gauntlet/_layout.html", "templates/gauntlet/sans-lang.html"]
FR = [{"url": SITE + "/p%d" % i, "lang": "fr"} for i in range(4)]


def lire(page):
    return {"templates/gauntlet/_layout.html": LAYOUT, "templates/gauntlet/sans-lang.html": page}.get


def prepare(key, paths=PATHS, pages=FR):
    return m._prepare_issue_fix(issue_key=key, issues={key: {"count": 1, "examples": [PAGE_URL]}}, impacted=[PAGE_URL],
                                all_paths=paths, site_name="site.test", owner="o", repo_name="r", branch="main",
                                token="t", pages=pages)


@pytest.mark.parametrize("key", ["html_lang_attribute_missing", "hreflang_defined_but_html_lang_missing"])
def test_a_python_template_site_gets_the_deterministic_rewriter(key) -> None:
    prep = prepare(key)
    assert getattr(prep["link_rewriter"], "lit_les_gabarits", False) and not prep["rewriter_ai_fallback"]
    assert not getattr(prepare(key, paths=["index.html", "gauntlet/sans-lang.html"])["link_rewriter"],
                       "lit_les_gabarits", False)


def test_without_a_language_of_its_own_the_page_takes_the_sites_with_its_space() -> None:
    reecrire = prepare("html_lang_attribute_missing")["link_rewriter"]
    nouveau, n = reecrire(SANS, "templates/gauntlet/sans-lang.html", lire(SANS))
    assert n == 1 and '{% block html_attrs %} lang="fr"{% endblock %}' in nouveau
    assert jh.rendered_lang("templates/gauntlet/sans-lang.html", lire(nouveau)) == "fr"


def test_the_pages_own_hreflang_wins_over_the_sites_language() -> None:
    reecrire = prepare("hreflang_defined_but_html_lang_missing")["link_rewriter"]
    nouveau, n = reecrire(AVEC_HREFLANG, "templates/gauntlet/sans-lang.html", lire(AVEC_HREFLANG))
    assert n == 1 and jh.rendered_lang("templates/gauntlet/sans-lang.html", lire(nouveau)) == "de"


def test_without_any_language_to_trust_nothing_is_written() -> None:
    reecrire = prepare("html_lang_attribute_missing", pages=[])["link_rewriter"]
    assert reecrire(SANS, "templates/gauntlet/sans-lang.html", lire(SANS)) == (SANS, 0)


# --- la garde des reecritures par le modele -------------------------------------------------

INVALIDE = SANS.replace("{% block html_attrs %}{% endblock %}", "{% block html_attrs %} lang=\"francais\"{% endblock %}")


def run_modele(monkeypatch, ecrite):
    written = {}

    def get(api, **kw):
        name = unquote(api.split("/contents/", 1)[1])
        contenu = {"templates/gauntlet/_layout.html": LAYOUT, "templates/gauntlet/sans-lang.html": INVALIDE}[name]
        return {"sha": "s", "encoding": "base64", "content": base64.b64encode(contenu.encode()).decode()}

    def put(api, **kw):
        written[unquote(api.split("/contents/", 1)[1])] = base64.b64decode(kw["json_body"]["content"]).decode()
        return {"content": {"sha": "n"}}

    monkeypatch.setattr(m, "_github_api_get", get)
    monkeypatch.setattr(m, "_github_api_put", put)
    monkeypatch.setattr(m, "_openai_generate_file_patch", lambda **kw: {"patched_content": ecrite})
    m._deep_patch_issue_files(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="qa", all_paths=list(PATHS),
        issue_key="html_lang_attribute_invalid", issue_label="Langue invalide", impacted_urls=[PAGE_URL],
        site_name="site.test", file_state={}, max_files=2, index=repo_index.build_repo_index(PATHS),
        targets_override=["templates/gauntlet/sans-lang.html"])
    return written


def test_a_model_rewrite_that_glues_an_attribute_to_html_is_refused(monkeypatch) -> None:
    collee = INVALIDE.replace(' lang="francais"', 'lang="fr"')
    assert run_modele(monkeypatch, collee) == {}


def test_the_same_rewrite_with_its_space_goes_through(monkeypatch) -> None:
    juste = INVALIDE.replace('lang="francais"', 'lang="fr"')
    assert run_modele(monkeypatch, juste) == {"templates/gauntlet/sans-lang.html": juste}
