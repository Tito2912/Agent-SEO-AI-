"""The next action follows actual report/selection state, with no implied success or write."""
from __future__ import annotations

import json
import os
import time
import uuid

import pytest
from bs4 import BeautifulSoup

from backend import app as m, auth
from backend.models import AccountMember, Project, User
from test_first_verified_correction import (
    BASELINE, OTHER, URL, _choose, _post, _ready, _verify, journey as journey,
)


def page(data, suffix=""):
    response = data["client"].get(f'/projects/{data["slug"]}{suffix}')
    assert response.status_code == 200
    return response, BeautifulSoup(response.text, "html.parser")


def action(soup):
    cards = soup.select('.journey-card')
    assert len(cards) == 1
    actions = cards[0].select('.btn.primary')
    assert len(actions) == 1
    return actions[0]


def job(data, status, *, owner=None):
    result = m.Job(id=uuid.uuid4().hex, status=status, created_at=time.time(),
                   result={"type": "crawl", "slug": data["slug"], "user_id": owner or data["owner"]})
    m._save_job(result)
    return result


def test_ready_audit_points_to_one_guided_choice_without_consuming_another_analysis(journey):
    journey["report"]()
    for path in ("/", f'/projects/{journey["slug"]}', f'/projects/{journey["slug"]}/issues'):
        response = journey["client"].get(path)
        soup = BeautifulSoup(response.text, "html.parser")
        assert action(soup)["href"] == f'/projects/{journey["slug"]}/first-correction'
        assert action(soup).get_text(strip=True) == "Choisir ma première correction"
    home = BeautifulSoup(journey["client"].get("/").text, "html.parser")
    assert len(home.select('.journey-milestones [aria-current="step"]')) == 1
    assert not home.select('.onboarding-card, .first-audit-resume, .onboarding-success')
    assert not m._list_jobs(comptes=[journey["owner"]])


@pytest.mark.parametrize("status", ["queued", "running", "cancel_requested"])
def test_active_analysis_remains_the_only_next_action_even_with_an_older_report(journey, status):
    journey["report"]()
    active = job(journey, status)
    for path in ("/", f'/projects/{journey["slug"]}', f'/projects/{journey["slug"]}/issues'):
        soup = BeautifulSoup(journey["client"].get(path).text, "html.parser")
        assert action(soup)["href"] == f'/projects/{journey["slug"]}?job={active.id}'
        assert not soup.select('.journey-card form')
        if status == "cancel_requested":
            assert "confirmation de l’arrêt" in soup.get_text()


def test_selected_correction_and_declared_edit_have_different_explanations(journey):
    task = _choose(journey)
    response, soup = page(journey, "/issues")
    assert action(soup).get_text(strip=True) == "Reprendre ma correction"
    assert response.context["next_step"]["title"] == "Poursuivez votre correction"
    _post(journey, action="ready", task_id=task.id)
    response, _ = page(journey)
    assert response.context["next_step"]["title"] == "Votre modification attend son contrôle"
    _verify(journey, issues={"missing_title": {"count": 1, "examples": [URL]}})
    response, _ = page(journey)
    assert response.context["next_step"]["title"] == "La correction reste à ajuster"


def test_an_old_job_link_does_not_hide_an_active_analysis(journey):
    journey["report"]()
    old = job(journey, "done")
    active = job(journey, "running")
    response, soup = page(journey, f'?job={old.id}')
    assert response.context["live_job"]["id"] == active.id
    assert action(soup)["href"] == f'/projects/{journey["slug"]}?job={active.id}'


def test_first_success_survives_but_resume_follows_the_next_explicit_selection(journey):
    _ready(journey)
    verified = _verify(journey, issues={"missing_title": {"count": 1, "examples": [OTHER]}}, urls=(URL, OTHER))
    ts = verified.context["state"]["verify"]["crawl_ts"]
    home = journey["client"].get("/")
    assert home.context["onboarding"]["complete"]
    assert home.context["onboarding"]["resume"]["href"].endswith('/first-correction?choose=1')
    _post(journey, action="select", issue_key="missing_title", url=OTHER, crawl_ts=ts)
    home = journey["client"].get("/")
    assert home.context["onboarding"]["complete"]
    assert home.context["onboarding"]["resume"]["label"] == "Reprendre ma correction"
    response, soup = page(journey)
    assert not response.context["first_correction_state"]["verified"]
    assert action(soup).get_text(strip=True) == "Reprendre ma correction"


def test_filter_miss_can_be_cleared_without_losing_analysis_or_comparison(journey):
    journey["report"]()
    journey["report"]("20261002-100000")
    response, soup = page(journey, f'/issues?crawl={BASELINE}&compare=20261002-100000&q=absent')
    assert "Aucun problème ne correspond à vos filtres" in response.text
    assert "Aucun problème détecté sur les pages analysées" not in response.text
    clear = soup.find('a', string='Voir tous les problèmes')
    assert clear["href"] == f'/projects/{journey["slug"]}/issues?crawl={BASELINE}&compare=20261002-100000'
    cleared = journey["client"].get(clear["href"])
    assert cleared.context["issues"] and cleared.context["project"]["current"]["timestamp"] == BASELINE
    assert action(soup).get_text(strip=True) == "Ouvrir le bilan actuel"
    assert action(soup)["href"] == f'/projects/{journey["slug"]}'


@pytest.mark.parametrize("condition", ["complete", "blocked", "time_budget", "missing_issues", "empty"])
def test_zero_issues_is_a_clean_result_only_with_a_usable_complete_report(journey, condition):
    payload = journey["report"](issues={}, blocked=condition == "blocked", urls=() if condition == "empty" else (URL,))
    if condition == "time_budget":
        payload["meta"]["stopped_on_time_budget"] = True
    if condition == "missing_issues":
        del payload["issues"]
    dest = journey["runs_dir"] / journey["slug"] / BASELINE / "audit" / "report.json"
    dest.write_text(json.dumps(payload))
    response, _ = page(journey, "/issues")
    clean = condition == "complete"
    assert ("Aucun problème détecté sur les pages analysées" in response.text) == clean
    assert response.context["project"]["current"]["summary"]["analysis_complete"] == clean
    home = journey["client"].get("/")
    assert home.context["onboarding"]["complete"] == clean
    if not clean and condition != "empty":
        assert response.context["next_step"]["label"] == "Comprendre mon bilan"


def test_no_report_is_an_explained_empty_state_and_foreign_project_stays_hidden(journey):
    response, _ = page(journey, "/issues")
    assert "Votre bilan n’est pas encore disponible" in response.text
    assert "Projet introuvable" not in response.text
    assert response.headers["cache-control"] == "no-store"
    assert journey["stranger"].get(f'/projects/{journey["slug"]}/issues').status_code == 404
    assert journey["client"].get('/projects/unknown-project/issues').status_code == 404
    job(journey, "running")
    response, _ = page(journey, "/issues")
    assert "Votre analyse est en cours" in response.text


def test_lost_report_is_not_presented_as_a_fresh_clean_audit(journey):
    journey["report"](issues={})
    (journey["runs_dir"] / journey["slug"] / "20991020-100000" / "audit").mkdir(parents=True)
    home = journey["client"].get("/")
    assert home.context["onboarding"]["resume"]["title"] == "Votre dernier rapport est indisponible"
    response, soup = page(journey, "/issues")
    assert soup.select_one('[data-repli="perdu"]')
    assert action(soup)["href"] == f'/projects/{journey["slug"]}/crawls'


def test_same_slug_in_a_shared_account_never_attaches_its_job_to_the_personal_project(journey):
    journey["report"]()
    with m.DB.session() as db:
        host = User(email=f'host-{uuid.uuid4().hex}@example.invalid', password_hash="test")
        db.add(host)
        db.commit()
        host_id = str(host.id)
        db.add(AccountMember(owner_user_id=host_id, member_user_id=journey["owner"]))
        db.add(Project(owner_user_id=host_id, slug=journey["slug"], base_url="https://autre.example/", site_name="Autre"))
        db.commit()
    foreign = job(journey, "running", owner=host_id)
    journey["client"].cookies.set(auth.SESSION_COOKIE_NAME,
        auth.make_session_token(user_id=journey["owner"], secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    response, _ = page(journey, f'?job={foreign.id}')
    assert not response.context["live_job"]
    assert foreign.id not in response.text
    response, _ = page(journey, "/issues")
    assert not response.context["analyse_en_cours"]
