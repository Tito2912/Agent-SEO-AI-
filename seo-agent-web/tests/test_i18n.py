"""Noyaru en anglais : le francais reste la langue source, l'anglais une traduction (backend/i18n.py)."""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from jinja2 import Environment, nodes

from backend import app as app_module
from backend import i18n
from test_customer_activation import client as session_client, lab as lab, project, user

WEB_ROOT = Path(__file__).resolve().parents[1]
CATALOG = json.loads((WEB_ROOT / "locales" / "en.json").read_text(encoding="utf-8"))
_PLACEHOLDER = re.compile(r"\{[a-z_0-9]*\}")


@pytest.fixture(scope="module")
def client() -> TestClient:
    return TestClient(app_module.app)


def _template_keys() -> dict[str, str]:
    env = Environment()
    keys: dict[str, str] = {}
    for path in sorted((WEB_ROOT / "templates").glob("*.html")):
        tree = env.parse(path.read_text(encoding="utf-8"))
        for call in tree.find_all(nodes.Call):
            if (
                isinstance(call.node, nodes.Name)
                and call.node.name in ("tr", "tr_html")
                and call.args
                and isinstance(call.args[0], nodes.Const)
            ):
                keys.setdefault(call.args[0].value, path.name)
    return keys


def test_every_wrapped_template_text_has_an_english_translation() -> None:
    missing = {key: name for key, name in _template_keys().items() if key not in CATALOG}
    assert not missing, (
        "Texte entoure de tr() sans traduction dans locales/en.json : " + json.dumps(missing, ensure_ascii=False, indent=2)
    )


def test_translations_keep_their_placeholders() -> None:
    wrong = {
        key: value
        for key, value in CATALOG.items()
        if sorted(_PLACEHOLDER.findall(key)) != sorted(_PLACEHOLDER.findall(value))
    }
    assert not wrong, wrong


def test_french_stays_the_default(client: TestClient) -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert '<html lang="fr">' in response.text
    assert "Comprenez votre SEO." in response.text


def test_lang_parameter_switches_to_english_and_is_remembered() -> None:
    fresh = TestClient(app_module.app)
    response = fresh.get("/", params={"lang": "en"})
    assert response.status_code == 200
    assert '<html lang="en">' in response.text
    assert "Understand your SEO." in response.text
    assert "Comprenez votre SEO." not in response.text
    assert fresh.cookies.get(i18n.COOKIE_NAME) == "en"

    again = fresh.get("/pricing")
    assert '<html lang="en">' in again.text
    assert "Real quotas, feature by feature." in again.text

    back = fresh.get("/pricing", params={"lang": "fr"})
    assert "Des quotas réels, fonction par fonction." in back.text


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("en-US,en;q=0.9", "en"),
        ("fr-FR,fr;q=0.9,en;q=0.8", "fr"),
        ("de-DE,de;q=0.9", "en"),
        ("de-DE,fr;q=0.5", "fr"),
        ("", "fr"),
    ],
)
def test_browser_language(header: str, expected: str) -> None:
    fresh = TestClient(app_module.app)
    response = fresh.get("/auth/login", headers={"accept-language": header} if header else {})
    assert f'<html lang="{expected}">' in response.text


def test_unknown_lang_parameter_is_ignored(client: TestClient) -> None:
    response = client.get("/", params={"lang": "xx"}, headers={"accept-language": "fr"})
    assert '<html lang="fr">' in response.text


def test_language_switch_keeps_next_but_drops_private_parameters(client: TestClient) -> None:
    response = client.get("/auth/login", params={"next": "/projects", "gclid": "private-click", "lang": "en"})
    assert 'href="/auth/login?next=%2Fprojects&amp;lang=fr"' in response.text
    assert "private-click" not in response.text


def test_computed_plan_texts_are_translated_with_their_numbers() -> None:
    assert i18n.translate("12 sites suivis", "en") == "12 tracked sites"
    assert i18n.translate("20 000 pages analysées/mois", "en") == "20,000 pages analyzed/month"
    assert i18n.translate("20 000 pages analysées/mois", "fr") == "20 000 pages analysées/mois"
    assert i18n.translate("Trop de tentatives. Réessaie dans 9 min.", "en") == "Too many attempts. Try again in 9 min."


def test_untranslated_text_falls_back_to_french() -> None:
    assert i18n.translate("Une phrase que personne n'a traduite.", "en") == "Une phrase que personne n'a traduite."


def test_pricing_grid_and_countries_in_english(client: TestClient) -> None:
    pricing = client.get("/pricing", params={"lang": "en"}).text
    assert "Compare plans, feature by feature" in pricing
    assert "Tracked sites" in pricing
    signup = client.get("/auth/signup", params={"lang": "en"}).text
    assert ">Germany</option>" in signup
    assert ">Allemagne</option>" not in signup


def test_account_emails_follow_the_request_language(monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[dict[str, str]] = []
    monkeypatch.setattr(app_module, "_send_email", lambda **kwargs: sent.append(kwargs))
    token = i18n._current_lang_var.set("en")
    try:
        app_module._send_welcome_email(to_email="welcome@example.invalid", dashboard_url="https://noyaru.test/")
    finally:
        i18n._current_lang_var.reset(token)
    app_module._send_welcome_email(to_email="bienvenue@example.invalid", dashboard_url="https://noyaru.test/")

    english, french = sent
    assert english["subject"].startswith("Welcome to ")
    assert '<html lang="en">' in english["html_body"]
    assert french["subject"].startswith("Bienvenue sur ")
    assert '<html lang="fr">' in french["html_body"]



def test_connected_app_shell_projects_and_billing_in_english(lab) -> None:
    owner = user(lab["db"])
    project(lab["db"], owner, slug="atelier")
    browser = session_client(owner)

    projects = browser.get("/", params={"lang": "en"}).text
    assert "Your sites" in projects
    assert "Analysis settings" in projects  # barre laterale
    assert "Vos sites" not in projects
    billing_page = browser.get("/billing").text
    assert "Current plan" in billing_page
    assert "Plan actuel" not in billing_page
    assert "Plan actuel" in browser.get("/billing", params={"lang": "fr"}).text


def test_project_overview_and_issues_in_english(lab, tmp_path) -> None:
    owner = user(lab["db"])
    project(lab["db"], owner, slug="atelier")
    url = "https://atelier.example/"
    report_dir = tmp_path / "runs" / str(owner.id) / "atelier" / "20261018-120000" / "audit"
    report_dir.mkdir(parents=True)
    (report_dir / "report.json").write_text(json.dumps({
        "meta": {"base_url": url},
        "pages": [{"url": url, "status_code": 200, "content_type": "text/html", "indexable": True}],
        "issues": {"missing_title": {"count": 1, "examples": [url]}},
    }))
    browser = session_client(owner)

    overview = browser.get("/projects/atelier", params={"lang": "en"}).text
    assert "Health score" in overview and "Top issues" in overview
    assert "Title tag missing / empty" in overview
    assert "Score de santé" not in overview
    issues = browser.get("/projects/atelier/issues").text
    assert "Filters and exports" in issues and "Title tag missing / empty" in issues
    assert "Balise title manquante / vide" in browser.get("/projects/atelier/issues", params={"lang": "fr"}).text


def test_static_scripts_receive_only_their_translations(client: TestClient) -> None:
    keys = i18n._js_keys()
    assert "Aucune donnée reçue sur cette période." in keys
    missing = [key for key in keys if key not in CATALOG]
    assert not missing, "Texte noyaruT() sans traduction dans locales/en.json : %s" % missing
    def page_catalog(lang: str) -> dict[str, str]:
        html = client.get("/", params={"lang": lang}).text
        match = re.search(r"window\.NOYARU_I18N = (\{.*?\});", html)
        assert match
        return json.loads(match.group(1))

    english = page_catalog("en")
    assert english["Aucune donnée reçue sur cette période."] == "No data received for this period."
    assert set(english) <= set(keys)
    assert page_catalog("fr") == {}
