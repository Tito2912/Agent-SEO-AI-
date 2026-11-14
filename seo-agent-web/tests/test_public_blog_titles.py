"""A long reviewed heading need not create the site's own title-length warning."""

import yaml
import pytest
from bs4 import BeautifulSoup
from fastapi.testclient import TestClient

from backend import app as m
from backend import content_library as cl


@pytest.mark.parametrize("lang", ["fr", "en"], ids=["fr", "en"])
@pytest.mark.parametrize("heading", [
    "Mesurer les résultats de son site",
    "SEO et GEO : quels résultats mesurer pour une petite entreprise ?",
    "Mesurer les résultats SEO et comprendre les citations dans les réponses IA pour une petite entreprise",
    'Lire les résultats "avant et après" sans confondre une corrélation avec une preuve de progression de son site',
], ids=["short", "brand-overflow", "long", "quotes"])
def test_browser_title_fits_without_rewriting_reviewed_article(tmp_path, monkeypatch, lang, heading):
    slug = "titre-public-de-recette"
    description = "Un exemple fictif pour vérifier le titre de la page sans modifier le texte relu, ses références ou sa langue."
    body = "Le texte relu et ses sources restent inchangés."
    meta = dict(slug=slug, title=heading, meta_title=heading, description=description,
                kind="Guide", updated_at="2026-11-14", published_at="2026-11-14")
    path = tmp_path / "article.md"
    raw = "---\n" + yaml.safe_dump(meta, allow_unicode=True) + "---\n\n" + body + "\n"
    path.write_text(raw, encoding="utf-8")
    pages = {locale: cl._load_page(path, collection="blog", lang=locale) for locale in ("fr", "en")}
    original = cl.get_article
    monkeypatch.setattr(cl, "get_article", lambda value, lang="fr": pages[lang] if value == slug else original(value, lang))
    monkeypatch.setattr(m, "_app_name", lambda: "Noyaru")
    url = ("/en" if lang == "en" else "") + "/ressources-seo/" + slug
    response = TestClient(m.app).get(url)
    assert response.status_code == 200
    soup = BeautifulSoup(response.text, "html.parser")
    browser_title = soup.title.get_text(strip=True)
    assert 15 <= len(browser_title) <= 70
    assert soup.select_one("article h1").get_text() == heading
    assert soup.select_one("article .content-body").get_text(strip=True) == body
    assert soup.select_one('meta[name="description"]')["content"] == description
    assert soup.select_one('meta[property="og:title"]')["content"] == heading
    assert soup.html["lang"] == lang
    assert path.read_text(encoding="utf-8") == raw
    if len(heading) <= 61:
        assert browser_title == heading + " - Noyaru"
    elif len(heading) <= 70:
        assert browser_title == heading
    else:
        assert browser_title.endswith("…")
        assert heading.startswith(browser_title[:-1])
        assert heading[len(browser_title) - 1] == " "
