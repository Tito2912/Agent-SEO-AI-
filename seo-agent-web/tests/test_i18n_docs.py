"""La documentation en anglais : content/docs/en/<page>.md, servie a /en/docs/<page>.

Une traduction est la meme page dans une autre langue : memes jetons de quotas, memes liens,
memes sections. Une page sans traduction reste servie en francais (voir docs/I18N.md).
"""
from __future__ import annotations

import re
from collections import Counter

import pytest
from fastapi.testclient import TestClient

from backend import app as app_module
from backend import content_library as cl

TOKEN = re.compile(r"\{\{\s*([a-z0-9_]+)\s*\}\}")
TRANSLATED = cl.translated_docs("en")


def _french(page: dict) -> dict:
    french = cl.get_doc(page["slug"])
    assert french is not None, f"{page['source']} n'a pas de page francaise"
    return french


def test_english_docs_load_and_cover_most_of_the_documentation() -> None:
    cl.reload()
    assert cl.LOAD_ERRORS == []
    assert len(TRANSLATED) >= 20
    assert all(page["path"] == f"/en/docs/{page['slug']}" for page in TRANSLATED)


@pytest.mark.parametrize("page", TRANSLATED, ids=lambda p: p["slug"])
def test_a_translation_is_the_same_page(page: dict) -> None:
    french = _french(page)
    raw = lambda p: (cl.DOCS_DIR / ("en/" if p["lang"] == "en" else "") / p["source"]).read_text(encoding="utf-8")
    assert Counter(TOKEN.findall(raw(page))) == Counter(TOKEN.findall(raw(french)))
    assert page["order"] == french["order"] and page["related"] == french["related"]
    assert page["app_href"] == french["app_href"] and page["updated_at"] == french["updated_at"]
    for tag in ("<h2", "<h3", "<table", "<img"):
        assert page["body_html"].count(tag) == french["body_html"].count(tag), tag
    assert len(page["faq"]) == len(french["faq"])
    description = page["description"].replace("{{app_name}}", "Noyaru")
    assert 100 <= len(description) <= 160, description


@pytest.mark.parametrize("page", TRANSLATED, ids=lambda p: p["slug"])
def test_links_of_a_translation_lead_to_existing_pages(page: dict) -> None:
    translations = {p["path"] for c in cl.TRANSLATION_DIRS for p in cl.translated_pages(c, "en")}
    known = {p["path"] for p in cl.all_pages()} | translations | {"/docs", "/en/docs", "/ressources-seo", "/en/ressources-seo"}
    for href in re.findall(r'href="(/(?:en/)?(?:docs|ressources-seo)(?:/[a-z0-9-]+)?)"', page["body_html"]):
        assert href in known, f"{page['source']} : {href}"
    # Une page traduite mene a la traduction de ses voisines quand elle existe.
    for slug in re.findall(r'href="/docs/([a-z0-9-]+)"', page["body_html"]):
        assert cl.get_doc(slug, "en") is None, f"{page['source']} : /docs/{slug} a une traduction"


def test_english_doc_page_is_served_and_announced() -> None:
    page = cl.get_doc("connecter-github", "en")
    assert page is not None
    english = TestClient(app_module.app).get("/en/docs/connecter-github")
    assert english.status_code == 200
    assert '<html lang="en">' in english.text and page["title"] in english.text
    assert '<link rel="canonical" href="http://testserver/en/docs/connecter-github" />' in english.text
    assert '<link rel="alternate" hreflang="fr" href="http://testserver/docs/connecter-github" />' in english.text
    assert '"inLanguage": "en"' in english.text
    assert "Read next" in english.text and "À lire ensuite" not in english.text

    french = TestClient(app_module.app).get("/docs/connecter-github").text
    assert '<html lang="fr">' in french
    assert '<link rel="alternate" hreflang="en" href="http://testserver/en/docs/connecter-github" />' in french

    index = TestClient(app_module.app).get("/en/docs").text
    assert "How Noyaru works, screen by screen" in index and page["title"] in index
    assert 'href="/en/docs/connecter-github"' in index


def test_a_page_without_translation_stays_french() -> None:
    untranslated = [p for p in cl.docs_pages() if cl.get_doc(p["slug"], "en") is None]
    if not untranslated:
        pytest.skip("toutes les pages sont traduites")
    slug = untranslated[0]["slug"]
    response = TestClient(app_module.app).get(f"/en/docs/{slug}", follow_redirects=False)
    assert (response.status_code, response.headers["location"]) == (302, f"/docs/{slug}")
    french = TestClient(app_module.app).get(f"/docs/{slug}").text
    assert 'hreflang="en"' not in french.split("</head>")[0]


def test_english_docs_use_english_numbers_and_prices() -> None:
    page = TestClient(app_module.app).get("/en/docs/plans-et-quotas").text
    assert "/month" in page and "€/mois" not in page
    assert not re.search(r"\d \d{3}", page)


def test_sitemap_lists_the_english_docs() -> None:
    sitemap = TestClient(app_module.app).get("/sitemap.xml").text
    assert "<loc>http://testserver/en/docs</loc>" in sitemap
    for page in TRANSLATED:
        assert f"<loc>http://testserver/en/docs/{page['slug']}</loc>" in sitemap
