"""A guided link preserves the exact page without rewriting the audit or task state."""
import json
from urllib.parse import parse_qs, urlsplit

import pytest
from bs4 import BeautifulSoup
from sqlalchemy import select

from backend import app as m
from backend.models import IssueTask
from test_first_verified_correction import BASELINE, URL, _post, journey as journey


TARGET = "https://atelier.example/service/?lang=fr&offre=été%202026"


def read(journey, **params):
    return journey["client"].get(f'/projects/{journey["slug"]}/issues/missing_title',
                                 params={"crawl": BASELINE, **params})


def no_writes(monkeypatch):
    def unexpected(*args, **kwargs):
        pytest.fail("Reading a correction context must not call AI, charge or write to GitHub")
    for name in ("_openai_url_fix", "_openai_generate_file_patch", "_correction_charge",
                 "_github_api_get", "_github_api_post", "_github_api_put"):
        monkeypatch.setattr(m, name, unexpected)


def test_guided_link_round_trips_the_page_query_and_crawl_without_declaring_a_change(journey, monkeypatch):
    journey["report"](urls=(TARGET,), issues={"missing_title": {"count": 1, "examples": [TARGET]}})
    selected = _post(journey, action="select", issue_key="missing_title", url=TARGET, crawl_ts=BASELINE)
    assert selected.status_code == 303
    guided = journey["client"].get(selected.headers["location"])
    link = BeautifulSoup(guided.text, "html.parser").find("a", string="Ouvrir les instructions détaillées")
    parts = urlsplit(link["href"])
    assert parse_qs(parts.query) == {"crawl": [BASELINE], "page_url": [TARGET]}
    assert parts.fragment == "page-selection"
    with m.DB.session() as db:
        task = db.scalar(select(IssueTask).where(IssueTask.project_id == journey["project_id"]))
        before = (task.status, task.note, task.updated_at)
    no_writes(monkeypatch)
    page = journey["client"].get(link["href"])
    assert page.status_code == 200 and page.context["focused_url"] == TARGET
    soup = BeautifulSoup(page.text, "html.parser")
    target = soup.select_one("#page-selection.issue-selected-page")
    assert target.select_one(".gh-fix-btn")["data-url"] == TARGET
    assert target.select_one(".gh-fix-btn")["data-crawl"] == BASELINE
    assert target.find("a", string="Revenir à ma correction")["href"] == f'/projects/{journey["slug"]}/first-correction'
    with m.DB.session() as db:
        task = db.get(IssueTask, task.id)
        assert (task.status, task.note, task.updated_at) == before


def test_a_flagged_page_outside_the_six_samples_is_reachable_without_changing_the_count(journey, monkeypatch):
    urls = [f"https://atelier.example/page-{i:02}/" for i in range(9)]
    journey["report"](urls=urls, issues={"missing_title": {"count": 9, "examples": urls}})
    original = read(journey)
    assert urls[-1] not in original.context["fix_suggestion"]["sample_urls"]
    no_writes(monkeypatch)
    page = read(journey, page_url=urls[-1])
    assert page.context["fix_suggestion"]["sample_urls"][0] == urls[-1]
    assert len(page.context["fix_suggestion"]["sample_urls"]) == 6
    assert page.context["project"]["issue"]["count"] == original.context["project"]["issue"]["count"] == 9
    assert page.context["project"]["issue"]["raw"] == original.context["project"]["issue"]["raw"]


@pytest.mark.parametrize("candidate", ["https://unrelated.invalid/page", "https://atelier.example/healthy/",
                                      'javascript:alert(1)', 'https://invalid.test/\"><img src=x onerror=alert(1)>'])
def test_unlisted_or_unsafe_page_only_shows_a_context_notice(journey, monkeypatch, candidate):
    journey["report"](urls=(URL, "https://atelier.example/healthy/"))
    no_writes(monkeypatch)
    page = read(journey, page_url=candidate)
    assert page.status_code == 200 and page.context["focus_unavailable"] and not page.context["focused_url"]
    soup = BeautifulSoup(page.text, "html.parser")
    assert not soup.select_one(".issue-selected-page")
    assert soup.select_one("#page-selection[role=status]")
    assert not soup.find("a", href=candidate) and not soup.select("img[onerror]")
    assert page.context["fix_suggestion"]["sample_urls"] == [URL]


def test_missing_crawl_does_not_silently_assign_the_page_to_the_fallback_analysis(journey):
    journey["report"]()
    page = read(journey, page_url=URL, crawl="missing-analysis")
    assert page.status_code == 200 and page.context["project"]["timestamp"] == BASELINE
    assert page.context["focus_unavailable"] and not page.context["focused_url"]


def test_page_context_is_not_an_access_grant_to_another_account(journey):
    journey["report"]()
    page = journey["stranger"].get(f'/projects/{journey["slug"]}/issues/missing_title',
        params={"crawl": BASELINE, "page_url": URL})
    assert page.status_code == 404 and URL not in page.text


def test_existing_receipt_stays_attached_to_the_highlighted_page(journey):
    journey["report"]()
    pr = "https://github.com/fixture/site/pull/42"
    with m.DB.session() as db:
        db.add(IssueTask(project_id=journey["project_id"], user_id=journey["owner"], issue_key="missing_title",
            crawl_ts=BASELINE, url=URL, status="in_progress", note=json.dumps({"pr_url": pr, "pr_number": 42})))
        db.commit()
    page = read(journey, page_url=URL)
    selected = BeautifulSoup(page.text, "html.parser").select_one("#page-selection")
    assert selected.find("a", href=pr) and "PR en cours" in selected.get_text()
    assert selected.select_one("a.primary")["href"] == pr
    assert selected.select_one(".gh-fix-btn") and not selected.select(".gh-fix-btn.primary")
    assert "Vérifié par analyse" not in selected.get_text()


def test_filters_and_pagination_keep_the_focused_page_without_filtering_the_audit(journey):
    urls = [f"https://atelier.example/page-{i:02}/" for i in range(70)]
    journey["report"](urls=urls, issues={"missing_title": {"count": 70, "examples": urls}})
    audit = journey["runs_dir"] / journey["slug"] / BASELINE / "audit" / "issues"
    audit.mkdir()
    (audit / "missing_title.json").write_text(json.dumps([{"url": url} for url in urls]))
    page = read(journey, page_url=urls[-1], per_page=50)
    soup = BeautifulSoup(page.text, "html.parser")
    assert soup.select_one('form.filters input[name="page_url"]')["value"] == urls[-1]
    next_page = soup.find("a", string="Suivant →")
    assert parse_qs(urlsplit(next_page["href"]).query)["page_url"] == [urls[-1]]
    result = journey["client"].get(next_page["href"])
    assert result.context["focused_url"] == urls[-1]
    assert result.context["project"]["issue"]["count"] == 70
    assert result.context["project"]["issue"]["pagination"]["page"] == 2


def test_plain_issue_links_keep_the_existing_samples_and_do_not_select_a_page(journey):
    journey["report"]()
    page = read(journey)
    assert not page.context["focused_url"] and not page.context["focus_unavailable"]
    assert not BeautifulSoup(page.text, "html.parser").select_one("#page-selection")
