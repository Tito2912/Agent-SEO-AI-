"""L'écran Activité et le suivi d'un traitement, en anglais (docs/I18N.md, lot 15)."""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from bs4 import BeautifulSoup

from backend import activity
from test_analysis_activity_journey import job, journey as journey, owner_client

CATALOG = json.loads((Path(__file__).resolve().parents[1] / "locales" / "en.json").read_text(encoding="utf-8"))


def test_every_activity_sentence_has_an_english_translation() -> None:
    seen: list[str] = []

    def record(text: str, **values: str) -> str:
        seen.append(text)
        return text.format(**values) if values else text

    for status in ("queued", "running", "cancel_requested", "done", "failed", "canceled", "inconnu"):
        for kind in ("crawl", "autopilot", ""):
            for progress in ({}, {"current": 10, "total": 10}):
                for report_url in ("", "/projects/atelier?crawl=20261021-090000"):
                    activity.view(
                        SimpleNamespace(status=status, progress=progress),
                        site_name="Atelier", report_url=report_url, kind=kind, tr=record,
                    )
    assert sorted({text for text in seen if text not in CATALOG}) == []


def test_the_activity_screen_reads_in_english(journey) -> None:
    job(journey)
    page = owner_client(journey).get("/jobs", params={"lang": "en"})
    soup = BeautifulSoup(page.text, "html.parser")
    text = soup.select_one(".activity-page").get_text(" ")
    for english in ("Activity", "Tasks shown", "In progress", "View tracking", "Site analysis", "Clear filters"):
        assert english in text, english
    for french in ("Traitement", "Avancement", "Voir le suivi", "Retrouvez", "Effacer les filtres"):
        assert french not in text, french
    assert soup.select_one('a.nav-item[href="/jobs"]').get_text(strip=True) == "Activity"
    assert soup.select_one('td[data-label="Progress"]') is not None


def test_the_tracking_page_and_its_live_updates_read_in_english(journey) -> None:
    row = job(journey)
    client = owner_client(journey)
    soup = BeautifulSoup(client.get("/jobs/" + row.id, params={"lang": "en"}).text, "html.parser")
    assert soup.select_one("h1").get_text().startswith("Analysis of ")
    assert soup.select_one("#activity-headline").get_text() == "Analysis in progress"
    assert soup.select_one("#job-status-pill").get_text() == "In progress"
    assert "Request a stop" in soup.get_text()
    live = client.get("/api/jobs/" + row.id).json()["activity"]
    assert live["headline"] == "Analysis in progress" and live["title"].startswith("Analysis of ")

    french = client.get("/jobs/" + row.id, params={"lang": "fr"}).text
    assert "Analyse en cours" in french and "Demander l’arrêt" in french
