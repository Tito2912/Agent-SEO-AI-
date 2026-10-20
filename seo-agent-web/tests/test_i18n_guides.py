"""Les guides SEO en anglais : content/blog/en/<guide>.md, servis a /en/ressources-seo/<guide>.

Meme methode que la documentation (tests/test_i18n_docs.py) : une traduction est le meme guide
dans une autre langue, et un guide sans traduction reste servi en francais.
"""
from __future__ import annotations

import re
from collections import Counter

import pytest
from fastapi.testclient import TestClient

from backend import app as app_module
from backend import content_library as cl

TOKEN = re.compile(r"\{\{\s*([a-z0-9_]+)\s*\}\}")
TRANSLATED = cl.translated_pages("blog", "en")


def test_english_guides_load() -> None:
    cl.reload()
    assert cl.LOAD_ERRORS == []
    assert len(TRANSLATED) == len(cl.blog_pages())
    assert all(page["path"] == f"/en/ressources-seo/{page['slug']}" for page in TRANSLATED)


@pytest.mark.parametrize("page", TRANSLATED, ids=lambda p: p["slug"])
def test_a_translated_guide_is_the_same_guide(page: dict) -> None:
    french = cl.get_article(page["slug"])
    assert french is not None
    raw = lambda p: (cl.BLOG_DIR / ("en/" if p["lang"] == "en" else "") / p["source"]).read_text(encoding="utf-8")
    assert Counter(TOKEN.findall(raw(page))) == Counter(TOKEN.findall(raw(french)))
    assert (page["published_at"], page["updated_at"], page["related"], page["featured"]) == \
        (french["published_at"], french["updated_at"], french["related"], french["featured"])
    for tag in ("<h2", "<h3", "<table", "<img"):
        assert page["body_html"].count(tag) == french["body_html"].count(tag), tag
    assert len(page["faq"]) == len(french["faq"]) and bool(page["cta"]) == bool(french["cta"])
    description = page["description"].replace("{{app_name}}", "Noyaru")
    assert 100 <= len(description) <= 160, description


@pytest.mark.parametrize("page", TRANSLATED, ids=lambda p: p["slug"])
def test_links_of_a_translated_guide_lead_to_translations(page: dict) -> None:
    # Un lien reste francais seulement si la page visee n'a pas de traduction.
    translation = {"docs": cl.get_doc, "ressources-seo": cl.get_article}
    for hub, slug in re.findall(r'href="/(docs|ressources-seo)/([a-z0-9-]+)"', page["body_html"]):
        assert translation[hub](slug, "en") is None, f"{page['source']} : /{hub}/{slug} a une traduction"


def test_english_guide_is_served_and_announced() -> None:
    page = TRANSLATED[0]
    url = f"/en/ressources-seo/{page['slug']}"
    english = TestClient(app_module.app).get(url)
    assert english.status_code == 200 and '<html lang="en">' in english.text
    assert f'<link rel="canonical" href="http://testserver{url}" />' in english.text
    assert f'<link rel="alternate" hreflang="fr" href="http://testserver/ressources-seo/{page["slug"]}" />' in english.text
    assert '"inLanguage": "en"' in english.text and "On this page" in english.text
    french = TestClient(app_module.app).get(f"/ressources-seo/{page['slug']}").text
    assert f'<link rel="alternate" hreflang="en" href="http://testserver{url}" />' in french

    index = TestClient(app_module.app).get("/en/ressources-seo").text
    assert "The method, not just the tool" in index and f'href="{url}"' in index
    home = TestClient(app_module.app).get("/en/").text
    featured = cl.featured_articles(3, "en")
    assert all(f'href="{item["path"]}"' in home for item in featured)


def test_sitemap_lists_the_english_guides() -> None:
    sitemap = TestClient(app_module.app).get("/sitemap.xml").text
    assert "<loc>http://testserver/en/ressources-seo</loc>" in sitemap
    for page in TRANSLATED:
        assert f"<loc>http://testserver/en/ressources-seo/{page['slug']}</loc>" in sitemap
