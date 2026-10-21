"""Les réglages de l'analyse, en anglais (docs/I18N.md, lot 15)."""
from __future__ import annotations

from bs4 import BeautifulSoup

from backend import i18n
from test_analysis_activity_journey import journey as journey, owner_client


def _page(journey, lang: str) -> BeautifulSoup:
    client = owner_client(journey)
    response = client.get("/projects/" + journey["slug"] + "/settings/crawl", params={"lang": lang})
    assert response.status_code == 200
    return BeautifulSoup(response.text, "html.parser")


def test_the_analysis_settings_read_in_english(journey) -> None:
    soup = _page(journey, "en")
    form = soup.select_one("#analysis-settings").get_text(" ")
    for english in ("Pages and scope", "Pages to analyze", "Include subdomains", "Additional data (optional)",
                    "Site audit", "Analysis period (days)", "Advanced settings", "Save settings"):
        assert english in form, english
    for french in ("Pages et périmètre", "Inclure les sous-domaines", "Données complémentaires", "Période d'analyse",
                   "Paramètres avancés", "Enregistrer les paramètres", "Votre plan"):
        assert french not in form, french
    assert soup.select_one("h1").get_text(strip=True) == "Analysis settings"
    assert "GitHub repository for fixes" in soup.select_one("#github-repo").get_text(" ")
    assert soup.select_one("#cs-include-regex")["placeholder"] == "e.g. ^https://example.com/blog"
    script = next(s.get_text() for s in soup.select("script") if "noyaruSettingsJson" in s.get_text())
    assert "Your session has expired." in script and "Chargement" not in script


def test_the_analysis_settings_stay_in_french_by_default(journey) -> None:
    text = _page(journey, "fr").get_text(" ")
    assert "Enregistrer ne lance pas d’analyse" in text and "Pages et périmètre" in text


def test_the_settings_messages_follow_the_customer_language() -> None:
    for message in ("Paramètres enregistrés", "Seul le propriétaire du site peut enregistrer ces réglages d’analyse.",
                    "GitHub non connecté. Connectez-le dans « Mon compte → Connexions »."):
        assert i18n.translate(message, "en") != message, message
    assert i18n.translate("Dépôt introuvable ou inaccessible : 404 Not Found", "en") == (
        "Repository not found or not accessible: 404 Not Found"
    )
