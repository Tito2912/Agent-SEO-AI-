"""Public help resolves real localized guides and never propagates private URL parameters."""

from urllib.parse import urlsplit

from bs4 import BeautifulSoup
from fastapi.testclient import TestClient
import pytest

from backend import app as m, content_library as cl, i18n


@pytest.mark.parametrize("lang,prefix", [("fr", ""), ("en", "/en")])
def test_help_paths_keep_the_language_and_choose_real_guides(lang, prefix):
    client = TestClient(m.app)
    private = "private-query-never-propagated"
    for path in ("/support", "/docs"):
        response = client.get(prefix + path, params={"lang": lang, "token": private})
        assert response.status_code == 200
        assert private not in response.text
        soup = BeautifulSoup(response.text, "html.parser")
        assert soup.html["lang"] == lang
        assert (
            urlsplit(soup.select_one("link[rel=canonical]")["href"]).path
            == prefix + path
        )
        links = [urlsplit(link["href"]).path for link in soup.select("main a[href]")]
        expected = (
            ("depannage", "verifier-une-correction", "plans-et-quotas")
            if path == "/support"
            else (
                "corriger-dans-wordpress",
                "fix-pack-sans-depot",
                "corrections-automatiques",
            )
        )
        for slug in expected:
            page = cl.get_doc(slug, lang)
            assert page and page["path"] in links
            linked = client.get(page["path"])
            assert linked.status_code == 200
            assert BeautifulSoup(linked.text, "html.parser").html["lang"] == lang
        assert client.head(prefix + path).status_code == 200
        if path == "/support":
            assert prefix + "/status" in links
            assert "/auth/forgot" in links and "/auth/login" in links
            assert "live" not in soup.select_one("main").get_text().lower()
        else:
            assert prefix + "/support" in links
            assert response.context["total_docs"] == len(
                soup.select("[data-help-card]")
            )
            assert "{{app_name}}" not in response.text


def test_help_uses_french_page_and_french_tokens_when_translation_is_absent(
    monkeypatch,
):
    get_doc = cl.get_doc

    def without_translation(slug, lang="fr"):
        if slug == "lancer-un-crawl" and lang == "en":
            return None
        return get_doc(slug, lang)

    monkeypatch.setattr(cl, "get_doc", without_translation)
    token = i18n._current_lang_var.set("en")
    try:
        pages = m._public_help_pages(("lancer-un-crawl", "unknown-doc-page"))
    finally:
        i18n._current_lang_var.reset(token)
    assert len(pages) == 1
    assert pages[0]["path"] == "/docs/lancer-un-crawl" and pages[0]["lang"] == "fr"
    assert "{{" not in pages[0]["description"]
