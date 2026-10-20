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
    assert "Filter" in issues and "PDF and CSV exports" in issues and "Title tag missing / empty" in issues
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


def test_every_issue_explanation_has_an_english_translation() -> None:
    """Chaque explication « Pourquoi agir ? » et chaque étape de correction s'affiche en anglais.

    Modifier un texte de backend/fix_suggestions.py change sa clé : ajouter la nouvelle ligne
    dans locales/en.json (voir docs/I18N.md).
    """
    from backend import audit_dashboard, fix_suggestions

    url = "https://atelier.example/page"
    missing: dict[str, str] = {}
    for key, meta in audit_dashboard.ISSUE_CATALOG.items():
        report = {"meta": {"base_url": "https://atelier.example/"},
                  "pages": [{"url": url, "status_code": 200}],
                  "issues": {key: {"count": 2, "examples": [url]}}}
        advice = fix_suggestions.suggest_issue_fix(
            issue_key=key, label=meta.label, category=meta.category, severity=meta.severity,
            count=2, report=report, site_name="Atelier", base_url="https://atelier.example/")
        texts = [advice.get("why"), advice.get("auto_fix_note"), *(advice.get("fix") or []), *(advice.get("verify") or [])]
        for text in texts:
            if text and i18n.translate(text, "en") == text:
                missing.setdefault(text, key)
    assert not missing, "Explications sans traduction dans locales/en.json : " + json.dumps(missing, ensure_ascii=False, indent=2)


def test_composed_explanations_keep_the_site_values() -> None:
    assert i18n.translate("Inclure le sujet principal de la page + la marque (atelier42.fr) quand c’est pertinent.", "en") \
        == "Include the page's main topic + the brand (atelier42.fr) when relevant."
    assert i18n.translate("Issue détectée: Balise title trop longue. Elle peut impacter SEO/UX selon le contexte.", "en") \
        == "Issue detected: Title tag too long. It may affect SEO/UX depending on the context."
    assert i18n.translate("3 de ces redirections forment une boucle : l'URL ne parvient jamais à une page, elle est donc "
                          "inatteignable pour Google.", "en").startswith("3 of these redirects form a loop")
    # Un trou de gabarit ({n}, {plan}...) n'est jamais deviné dans une phrase libre.
    assert i18n.translate("Choisir quelque chose d'inconnu", "en") == "Choisir quelque chose d'inconnu"


def test_issue_detail_in_english(lab, tmp_path) -> None:
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
    page = session_client(owner).get("/projects/atelier/issues/missing_title", params={"lang": "en"}).text
    assert "Recommended fix" in page and "Actions to take" in page
    assert "Some pages have no &lt;title&gt; tag." in page
    assert "Correction recommandée" not in page and "Certaines pages n’ont pas de balise" not in page


def test_corrections_tracker_in_english(lab, tmp_path) -> None:
    owner = user(lab["db"])
    project(lab["db"], owner, slug="atelier")
    url = "https://atelier.example/"
    report_dir = tmp_path / "runs" / str(owner.id) / "atelier" / "20261018-120000" / "audit"
    report_dir.mkdir(parents=True)
    (report_dir / "report.json").write_text(json.dumps({
        "meta": {"base_url": url},
        "pages": [{"url": url, "status_code": 200, "content_type": "text/html", "indexable": True}],
        "issues": {"missing_title": {"count": 2, "examples": [url]}},
    }))
    page = session_client(owner).get("/projects/atelier/corrections", params={"lang": "en"}).text
    assert "Issues still detected" in page and "View instructions" in page
    assert "2 occurrences in the latest analysis." in page
    assert "Anomalies encore détectées" not in page
    assert i18n.translate("Page rédigée : /blog/article", "en") == "Page written: /blog/article"


def test_account_settings_in_english(lab) -> None:
    owner = user(lab["db"])
    project(lab["db"], owner, slug="atelier")
    browser = session_client(owner)

    account_page = browser.get("/settings/accounts", params={"lang": "en"}).text
    assert "Time zone" in account_page and "Danger zone" in account_page
    assert "Fuseau horaire" not in account_page and "Zone à risque" not in account_page
    assert "Members of your personal account" in browser.get("/settings/team").text
    assert "Google and Bing for this site" in browser.get("/projects/atelier/settings/search").text
    flash = browser.get("/settings/team", params={"err": "Cette personne fait déjà partie de votre compte."}).text
    assert "This person is already part of your account." in flash
    assert i18n.translate("Invitation envoyée à client@exemple.fr.", "en") == "Invitation sent to client@exemple.fr."


def test_language_chosen_in_my_account_follows_the_customer(lab) -> None:
    owner = user(lab["db"])
    browser = session_client(owner)
    browser.get("/settings/accounts")
    response = browser.post(
        "/settings/account/preferences",
        data={"language": "en", "_csrf": browser.cookies.get(app_module._CSRF_COOKIE_NAME)},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert browser.cookies.get(i18n.COOKIE_NAME) == "en"
    assert "Preferences saved." in browser.get(response.headers["location"]).text

    # Un autre appareil, sans cookie ni langue de navigateur : la preference du compte s'applique.
    other_device = session_client(owner)
    assert '<html lang="en">' in other_device.get("/settings/accounts", headers={"accept-language": "fr"}).text

    # Une langue que l'interface ne parle pas encore oublie le choix de l'appareil.
    response = browser.post(
        "/settings/account/preferences",
        data={"language": "de", "_csrf": browser.cookies.get(app_module._CSRF_COOKIE_NAME)},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert browser.cookies.get(i18n.COOKIE_NAME) is None


def test_content_keywords_and_competitors_in_english(lab, tmp_path) -> None:
    owner = user(lab["db"], admin=True)  # l'acces Pro sans passer par la facturation
    project(lab["db"], owner, slug="atelier")
    url = "https://atelier.example/"
    report_dir = tmp_path / "runs" / str(owner.id) / "atelier" / "20261018-120000" / "audit"
    report_dir.mkdir(parents=True)
    (report_dir / "report.json").write_text(json.dumps({
        "meta": {"base_url": url},
        "pages": [{"url": url, "status_code": 200, "content_type": "text/html", "indexable": True},
                  {"url": url + "blog/velo", "status_code": 200, "content_type": "text/html", "indexable": True}],
        "issues": {},
    }))
    browser = session_client(owner)

    content = browser.get("/projects/atelier/content", params={"lang": "en"}).text
    assert "Write a page" in content and "Customize length and tone" in content
    assert '"Write the article"' in content  # texte du script
    assert "Écrire une page" not in content and "Rédiger l\\u0027article" not in content
    competitors = browser.get("/projects/atelier/competitors").text
    assert "Add a competitor" in competitors and "Find with AI" in competitors
    assert "Ajouter un concurrent" not in competitors
    keywords = browser.get("/projects/atelier/keywords/opportunities").text
    assert "Tracked keywords" in keywords and "Connect Google for this site" in keywords
    performance = browser.get("/projects/atelier/performance").text
    assert "Source, period and filters" in performance and "Click-through rate" in performance
    assert "Écrire une page" in browser.get("/projects/atelier/content", params={"lang": "fr"}).text


def test_json_errors_follow_the_customer_language(lab) -> None:
    owner = user(lab["db"])
    project(lab["db"], owner, slug="atelier")
    browser = session_client(owner)
    browser.get("/projects/atelier/content", params={"lang": "en"})
    headers = {"x-csrf-token": browser.cookies.get(app_module._CSRF_COOKIE_NAME) or ""}

    english = browser.post("/api/projects/atelier/content/article", json={}, headers=headers)
    assert english.status_code == 400
    assert english.json()["error"] == "Topic or section missing."
    browser.get("/projects/atelier/content", params={"lang": "fr"})
    french = browser.post("/api/projects/atelier/content/article", json={}, headers=headers)
    assert french.json()["error"] == "Sujet ou section manquant."
    # Hors requete (une tache de fond), rien ne change.
    assert json.loads(i18n.JSONResponse({"error": "Session expirée."}).body)["error"] == "Session expirée."


def test_next_automatic_page_date_in_english() -> None:
    from datetime import datetime
    from zoneinfo import ZoneInfo

    auto = {"fuseau": "Europe/London"}
    creneau = datetime(2026, 10, 5, 9, tzinfo=ZoneInfo("Europe/London"))
    token = i18n._current_lang_var.set("en")
    try:
        english = app_module._dire_creneau(creneau, auto, maintenant=0)
    finally:
        i18n._current_lang_var.reset(token)
    assert english == "Monday, October 5 at 09:00 (London)"
    assert app_module._dire_creneau(creneau, auto, maintenant=0) == "lundi 5 octobre à 09:00 (Londres)"


def test_backlinks_outreach_and_automation_in_english(lab, tmp_path) -> None:
    owner = user(lab["db"], admin=True)  # l'acces Solo+ sans passer par la facturation
    project(lab["db"], owner, slug="atelier")
    url = "https://atelier.example/"
    report_dir = tmp_path / "runs" / str(owner.id) / "atelier" / "20261018-120000" / "audit"
    report_dir.mkdir(parents=True)
    (report_dir / "report.json").write_text(json.dumps({
        "meta": {"base_url": url},
        "pages": [{"url": url, "status_code": 200, "content_type": "text/html", "indexable": True}],
        "issues": {},
    }))
    browser = session_client(owner)

    backlinks = browser.get("/projects/atelier/backlinks", params={"lang": "en", "err": "Fichier vide"}).text
    assert "Links to your site" in backlinks and "Empty file" in backlinks
    assert "Liens vers votre site" not in backlinks
    outreach = browser.get("/projects/atelier/backlinks/opportunities").text
    assert "Replies to review and post" in outreach and "Turn on automatic search" in outreach
    assert '"Generate AI reply"' in outreach  # texte du script
    assert "Réponses à relire et à envoyer" not in outreach
    automation = browser.get("/projects/atelier/automation").text
    assert "No fix recorded for this project." in automation
    assert "Aucune correction enregistrée" not in automation


def test_public_pages_have_an_english_address_announced_to_google() -> None:
    fresh = TestClient(app_module.app)
    english = fresh.get("/en/pricing")
    assert english.status_code == 200
    assert '<html lang="en">' in english.text and "Real quotas, feature by feature." in english.text
    assert '<link rel="canonical" href="http://testserver/en/pricing" />' in english.text
    assert '<link rel="alternate" hreflang="fr" href="http://testserver/pricing" />' in english.text
    assert '<link rel="alternate" hreflang="en" href="http://testserver/en/pricing" />' in english.text
    assert '<link rel="alternate" hreflang="x-default" href="http://testserver/pricing" />' in english.text
    assert 'href="/en/support"' in english.text  # les liens restent dans la version anglaise
    assert 'href="/pricing?lang=fr"' in english.text  # retour au francais
    assert fresh.cookies.get(i18n.COOKIE_NAME) == "en"  # l'inscription suivra en anglais

    # Un robot (ni cookie ni langue) voit le francais a l'adresse francaise.
    french = TestClient(app_module.app).get("/pricing")
    assert '<html lang="fr">' in french.text
    assert '<link rel="canonical" href="http://testserver/pricing" />' in french.text
    assert '<link rel="alternate" hreflang="en" href="http://testserver/en/pricing" />' in french.text
    assert 'href="/en/pricing" hreflang="en"' in french.text

    home = TestClient(app_module.app).get("/en", follow_redirects=False)
    assert (home.status_code, home.headers["location"]) == (308, "/en/")
    assert '<link rel="canonical" href="http://testserver/en/" />' in TestClient(app_module.app).get("/en/").text
    for page in ("/en/support", "/en/status"):
        assert '<html lang="en">' in TestClient(app_module.app).get(page).text
    # Seules les pages publiques ont une adresse anglaise.
    assert TestClient(app_module.app).get("/en/projects", follow_redirects=False).status_code != 200
    sitemap = TestClient(app_module.app).get("/sitemap.xml").text
    assert "<loc>http://testserver/en/pricing</loc>" in sitemap and "<loc>http://testserver/pricing</loc>" in sitemap


def test_english_public_pages_pass_the_beta_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BETA_BASIC_AUTH_USER", "beta")
    monkeypatch.setenv("BETA_BASIC_AUTH_PASS", "secret")
    fresh = TestClient(app_module.app)
    assert fresh.get("/pricing").status_code == 200
    assert fresh.get("/en/pricing").status_code == 200
    assert fresh.get("/auth/login").status_code == 401  # la porte beta est bien fermee ailleurs


def test_assistant_answers_in_the_customer_language(lab, monkeypatch: pytest.MonkeyPatch) -> None:
    owner = user(lab["db"], admin=True)  # sans quota a consommer
    browser = session_client(owner)
    asked: list[str] = []

    def fake_claude(messages, *, system, model):  # type: ignore[no-untyped-def]
        asked.append(system)
        return "ok"

    monkeypatch.setattr(app_module, "_assistant_effective_provider", lambda: "claude")
    monkeypatch.setattr(app_module, "_assistant_claude_chat", fake_claude)
    browser.get("/", params={"lang": "en"})
    headers = {"x-csrf-token": browser.cookies.get(app_module._CSRF_COOKIE_NAME) or ""}
    assert browser.post("/api/assistant/chat", json={"message": "Hello"}, headers=headers).json()["ok"] is True
    browser.get("/", params={"lang": "fr"})
    browser.post("/api/assistant/chat", json={"message": "Bonjour"}, headers=headers)
    english, french = asked
    assert "Réponds en anglais" in english and "Réponds en français" in french
    # Les ecrans sont cites sous le nom que la personne voit, dans sa langue.
    assert "Overview" in english and "Analysis history" in english and "/en/docs" in english
    assert "Vue d'ensemble" in french and "Historique des analyses" in french
    for old_name in ("Jobs", "All issues", "Crawl log", "Paramètres crawl"):
        assert old_name not in english and old_name not in french

    monkeypatch.setattr(app_module, "_assistant_effective_provider", lambda: "none")
    browser.get("/", params={"lang": "en"})
    refused = browser.post("/api/assistant/chat", json={"message": "Hello"}, headers=headers).json()
    assert refused["error"] == "Assistant temporarily unavailable."


def test_assistant_names_screens_that_exist_in_the_sidebar() -> None:
    layout = (WEB_ROOT / "templates" / "layout.html").read_text(encoding="utf-8")
    # « Activité » n'est pas encore traduit dans la barre laterale : present, mais hors tr().
    assert "Activité" in layout
    names = [n for n in app_module._ECRANS_DU_COMPTE + app_module._ECRANS_DU_PROJET if n != "Activité"]
    missing = [n for n in names if f'tr("{n}")' not in layout]
    assert missing == []
    assert all(n in CATALOG for n in names)


def test_assistant_script_texts_are_translated() -> None:
    keys = i18n._js_keys()
    for text in ("Comment puis-je vous aider ?", "Erreur:", "Effacer la conversation ?"):
        assert text in keys and text in CATALOG
