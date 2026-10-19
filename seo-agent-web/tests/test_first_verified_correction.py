"""A real customer reaches completion only after a usable audit and a verified result."""
from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from bs4 import BeautifulSoup
from fastapi.testclient import TestClient
from sqlalchemy import select

from backend import app as m, auth, first_correction
from backend.models import AccountMember, IssueTask, Project, User

URL = "https://atelier.example/service/"
OTHER = "https://atelier.example/autre/"
BASELINE = "20261001-100000"


@pytest.fixture()
def journey(tmp_path, monkeypatch):
    m.DB.create_tables()
    tag = uuid.uuid4().hex
    with m.DB.session() as db:
        users = [User(email=f"first-{kind}-{tag}@example.invalid", password_hash="test", is_admin=False)
                 for kind in ("owner", "member", "stranger")]
        db.add_all(users)
        db.commit()
        owner, member, stranger = [str(user.id) for user in users]
        project = Project(owner_user_id=owner, slug=f"first-{tag}", site_name="Atelier", base_url="https://atelier.example/",
                          settings={"crawl": {"max_pages": 50}})
        db.add(project)
        db.add(AccountMember(owner_user_id=owner, member_user_id=member))
        db.commit()
        project_id, slug = str(project.id), project.slug
    # Ni travaux, ni Search Console, ni GitHub débranchés : les comptes sont neufs, donc le vrai
    # code répond « rien » de lui-même. Les débrancher a caché que la première correction
    # cherchait son analyse en cours dans les travaux de TOUS les comptes (19/10/2026).
    monkeypatch.setattr(m, "_runs_dir_for_user", lambda uid: tmp_path / uid)

    def client(uid):
        result = TestClient(m.app)
        result.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
        return result

    def report(ts=BASELINE, *, issues=None, urls=(URL,), blocked=False):
        payload = {"meta": {"base_url": "https://atelier.example/", "blocked_by_host": {"count": 1 if blocked else 0}},
                   "pages": [{"url": url, "status_code": 200, "content_type": "text/html", "title": "Service de réparation", "indexable": True} for url in urls],
                   "issues": issues if issues is not None else {"missing_title": {"count": 1, "examples": [URL]}}}
        audit = tmp_path / owner / slug / ts / "audit"
        audit.mkdir(parents=True, exist_ok=True)
        (audit / "report.json").write_text(json.dumps(payload))
        return payload

    return {"client": client(member), "stranger": client(stranger), "owner": owner, "member": member,
            "slug": slug, "project_id": project_id, "report": report, "runs_dir": tmp_path / owner}


def _state(data):
    page = data["client"].get("/")
    assert page.status_code == 200
    return page.context["onboarding"]


def _post(data, **payload):
    client, slug = data["client"], data["slug"]
    page = client.get(f"/projects/{slug}/first-correction")
    assert page.status_code == 200
    token = re.search(r'name="_csrf"\s+value="([^"]+)"', page.text).group(1)
    return client.post(f"/projects/{slug}/first-correction", data={"_csrf": token, **payload}, follow_redirects=False)


def _choose(data):
    data["report"]()
    response = _post(data, action="select", issue_key="missing_title", url=URL, crawl_ts=BASELINE)
    assert response.status_code == 303 and "err=" not in response.headers["location"]
    with m.DB.session() as db:
        return db.scalar(select(IssueTask).where(IssueTask.project_id == data["project_id"]))


def _ready(data):
    task = _choose(data)
    response = _post(data, action="ready", task_id=str(task.id))
    assert response.status_code == 303
    return task


def _verify(data, *, issues=None, urls=(URL,), blocked=False, ts=None):
    ts = ts or (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y%m%d-%H%M%S")
    report = data["report"](ts, issues=issues if issues is not None else {}, urls=urls, blocked=blocked)
    m._verify_corrections_after_crawl(data["slug"], report, data["runs_dir"], user_id=data["owner"], crawl_ts=ts)
    return data["client"].get(f'/projects/{data["slug"]}/first-correction')


@pytest.mark.parametrize("status", ["queued", "running", "failed", "canceled"])
def test_attempted_analysis_does_not_complete_the_audit_step(journey, status):
    slug = journey["slug"]
    job = m.Job(id=str(uuid.uuid4()), status=status, created_at=datetime.now(timezone.utc).timestamp(),
                config_path="test", result={"type": "crawl", "slug": slug, "user_id": journey["owner"]})
    m._save_job(job)
    state = _state(journey)
    assert state["required_done"] == 1 and state["progress_pct"] == 25 and not state["complete"]
    step = next(step for step in state["steps"] if step["key"] == "crawl")
    assert not step["done"]


def test_report_availability_does_not_claim_that_an_anomaly_was_treated(journey):
    journey["report"]()
    state = _state(journey)
    assert state["progress_pct"] == 50 and not state["complete"]
    assert not next(step for step in state["steps"] if step["key"] == "issues")["done"]


def test_manual_choice_is_persisted_for_the_owner_and_can_resume_without_github(journey):
    task = _choose(journey)
    assert task.status == "todo" and task.user_id == journey["owner"] and task.created_by == journey["member"]
    assert task.url == URL and task.crawl_ts == BASELINE
    again = _post(journey, action="select", issue_key="missing_title", url=URL, crawl_ts=BASELINE)
    assert again.status_code == 303
    with m.DB.session() as db:
        assert len(list(db.scalars(select(IssueTask).where(IssueTask.project_id == journey["project_id"])))) == 1
    page = journey["client"].get(again.headers["location"])
    assert "J’ai modifié cette page" in page.text and "Modifiez votre page" in page.text
    state = _state(journey)
    assert state["progress_pct"] == 75 and not state["complete"]


def test_declared_edit_is_pending_until_a_new_crawl_confirms_it(journey):
    task = _ready(journey)
    with m.DB.session() as db:
        row = db.get(IssueTask, task.id)
        assert row.status == "in_progress" and not first_correction.note_data(row.note).get("verify")
    assert not _state(journey)["complete"]
    page = _verify(journey)
    assert page.status_code == 200 and "Votre correction est vérifiée" in page.text
    state = _state(journey)
    assert state["complete"] and state["progress_pct"] == 100 and state["success_kind"] == "verified"
    dashboard = journey["client"].get("/")
    assert "Votre première correction est vérifiée" in dashboard.text
    assert "Première correction vérifiée" in journey["client"].get(f'/projects/{journey["slug"]}').text


def test_choosing_another_page_follows_the_latest_explicit_choice(journey):
    journey["report"](issues={"missing_title": {"count": 2, "examples": [URL, OTHER]}}, urls=(URL, OTHER))
    for url in (URL, OTHER, URL):
        response = _post(journey, action="select", issue_key="missing_title", url=url, crawl_ts=BASELINE)
        page = journey["client"].get(response.headers["location"])
        assert page.context["state"]["url"] == url


def test_customer_can_reopen_cancel_and_submit_the_page_choice(journey):
    task = _choose(journey)
    journey["report"](issues={"missing_title": {"count": 2, "examples": [URL, OTHER]}}, urls=(URL, OTHER))
    path = f'/projects/{journey["slug"]}/first-correction'
    with m.DB.session() as db:
        original = db.get(IssueTask, task.id)
        saved = (original.status, original.note, original.updated_at)

    current = journey["client"].get(path)
    link = BeautifulSoup(current.text, "html.parser").find("a", string="Choisir une autre page")
    choice = journey["client"].get(link["href"])
    assert choice.context["choosing"] and choice.context["state"]["url"] == URL
    soup = BeautifulSoup(choice.text, "html.parser")
    form = soup.select_one(f'form[action="{path}"]')
    selector = form.find("select", attrs={"name": "url"})
    assert selector.has_attr("required")
    assert selector.find("option", selected=True)["value"] == ""
    assert selector.find("option", attrs={"value": OTHER})

    cancel = journey["client"].get(soup.find("a", string="Revenir à ma correction")["href"])
    assert not cancel.context["choosing"] and cancel.context["state"]["url"] == URL
    with m.DB.session() as db:
        original = db.get(IssueTask, task.id)
        assert (original.status, original.note, original.updated_at) == saved

    payload = {field["name"]: field.get("value", "") for field in form.select("input[name]")}
    response = journey["client"].post(form["action"], data={**payload, "url": OTHER}, follow_redirects=False)
    assert response.status_code == 303 and "err=" not in response.headers["location"]
    selected = journey["client"].get(response.headers["location"])
    assert not selected.context["choosing"] and selected.context["state"]["url"] == OTHER
    with m.DB.session() as db:
        tasks = list(db.scalars(select(IssueTask).where(IssueTask.project_id == journey["project_id"])))
        assert len(tasks) == 2
        original = db.get(IssueTask, task.id)
        assert (original.status, original.note, original.updated_at) == saved


@pytest.mark.parametrize("case", ["unlisted_page", "stale_report"])
def test_invalid_reselection_keeps_the_current_task_and_reopens_choices(journey, case):
    task = _choose(journey)
    if case == "stale_report":
        journey["report"]("20261002-100000")
    response = _post(journey, action="select", issue_key="missing_title",
                     url=OTHER if case == "unlisted_page" else URL, crawl_ts=BASELINE)
    assert response.status_code == 303 and "choose=1" in response.headers["location"]
    page = journey["client"].get(response.headers["location"])
    assert page.context["choosing"] and page.context["state"]["task_id"] == str(task.id)
    assert "L’analyse a changé ou cette page n’est pas proposée" in page.text
    with m.DB.session() as db:
        tasks = list(db.scalars(select(IssueTask).where(IssueTask.project_id == journey["project_id"])))
        assert len(tasks) == 1 and tasks[0].status == "todo" and tasks[0].url == URL


def test_reopening_the_choice_does_not_reset_a_verified_success(journey):
    task = _ready(journey)
    _verify(journey, issues={"missing_title": {"count": 1, "examples": [OTHER]}}, urls=(URL, OTHER))
    path = f'/projects/{journey["slug"]}/first-correction'
    current = journey["client"].get(path)
    link = BeautifulSoup(current.text, "html.parser").find("a", string="Choisir une autre correction")
    with m.DB.session() as db:
        previous = db.get(IssueTask, task.id)
        saved = (previous.status, previous.note, previous.updated_at)
    page = journey["client"].get(link["href"])
    assert page.context["state"]["verified"] and page.context["choosing"]
    soup = BeautifulSoup(page.text, "html.parser")
    assert soup.find("option", attrs={"value": OTHER})
    assert not soup.select(".first-correction-steps")
    cancel = journey["client"].get(soup.find("a", string="Revenir au résultat vérifié")["href"])
    assert "Votre correction est vérifiée" in cancel.text
    assert not cancel.context["choosing"]
    assert _state(journey)["complete"]
    with m.DB.session() as db:
        previous = db.get(IssueTask, task.id)
        assert (previous.status, previous.note, previous.updated_at) == saved

    form = soup.select_one(f'form[action="{path}"]')
    payload = {field["name"]: field.get("value", "") for field in form.select("input[name]")}
    invalid = journey["client"].post(path, data={**payload, "url": URL}, follow_redirects=False)
    invalid_page = journey["client"].get(invalid.headers["location"])
    assert invalid_page.context["choosing"] and invalid_page.context["state"]["verified"]
    response = journey["client"].post(path, data={**payload, "url": OTHER}, follow_redirects=False)
    following = journey["client"].get(response.headers["location"])
    assert following.context["state"]["url"] == OTHER and not following.context["state"]["verified"]
    assert not following.context["choosing"] and "Votre correction est vérifiée" not in following.text
    assert _state(journey)["complete"]  # The first achievement survives a new, unfinished correction.
    corrections = journey["client"].get(f'/projects/{journey["slug"]}/corrections')
    assert corrections.context["first_correction_complete"]
    assert "Être guidé pour ma première correction" not in corrections.text
    with m.DB.session() as db:
        previous = db.get(IssueTask, task.id)
        assert (previous.status, previous.note, previous.updated_at) == saved
        assert len(list(db.scalars(select(IssueTask).where(IssueTask.project_id == journey["project_id"])))) == 2


@pytest.mark.parametrize("case,result", [("still_present", "still_present"), ("not_visited", "unverified"),
                                         ("blocked", "unverified"), ("predates_edit", "unverified"),
                                         ("incomplete_examples", "unverified")])
def test_failed_or_inconclusive_controls_keep_the_journey_open(journey, case, result):
    _ready(journey)
    options = {}
    if case == "still_present":
        options["issues"] = {"missing_title": {"count": 1, "examples": [URL]}}
    elif case == "not_visited":
        options["urls"] = (OTHER,)
    elif case == "blocked":
        options["blocked"] = True
    elif case == "predates_edit":
        options["ts"] = "20261002-100000"
    else:
        options["issues"] = {"missing_title": {"count": 10, "examples": [OTHER]}}
    page = _verify(journey, **options)
    assert page.status_code == 200
    assert page.context["state"]["verify"]["result"] == result
    assert not page.context["state"]["verified"] and not _state(journey)["complete"]
    assert "Votre correction est vérifiée" not in page.text


def test_one_page_can_be_verified_while_other_fully_listed_occurrences_remain(journey):
    _ready(journey)
    page = _verify(journey, issues={"missing_title": {"count": 1, "examples": [OTHER]}}, urls=(URL, OTHER))
    assert page.context["state"]["verified"]
    assert _state(journey)["complete"]


def test_a_clean_first_audit_finishes_with_an_honest_audit_result(journey):
    journey["report"](issues={})
    state = _state(journey)
    assert state["complete"] and state["success_kind"] == "clean"
    page = journey["client"].get(f'/projects/{journey["slug"]}/first-correction')
    assert "Aucune anomalie détectée sur les pages analysées" in page.text
    assert "Votre correction est vérifiée" not in page.text


def test_a_partial_clean_audit_does_not_finish_the_journey(journey):
    journey["report"](issues={}, blocked=True)
    assert not _state(journey)["complete"]
    page = journey["client"].get(f'/projects/{journey["slug"]}/first-correction')
    assert not page.context["clean"]


def test_existing_notes_and_pr_receipts_survive_manual_guidance(journey):
    journey["report"]()
    with m.DB.session() as db:
        task = IssueTask(project_id=journey["project_id"], user_id=journey["owner"], issue_key="missing_title",
                         issue_label="Titre manquant", crawl_ts=BASELINE, url=URL, status="todo", note="Prévenir le prestataire")
        db.add(task)
        db.commit()
        task_id = str(task.id)
    _post(journey, action="select", issue_key="missing_title", url=URL, crawl_ts=BASELINE)
    with m.DB.session() as db:
        task = db.get(IssueTask, task_id)
        note = json.loads(task.note)
        assert note["user_note"] == "Prévenir le prestataire"
        note.update({"pr_url": "https://github.com/client/site/pull/1", "pr_number": 1})
        task.note = json.dumps(note)
        db.commit()
    _post(journey, action="ready", task_id=task_id)
    with m.DB.session() as db:
        note = json.loads(db.get(IssueTask, task_id).note)
    assert note["pr_number"] == 1 and note["user_note"] == "Prévenir le prestataire"
    corrections_page = journey["client"].get(f'/projects/{journey["slug"]}/corrections')
    assert "Prévenir le prestataire" in corrections_page.text
    assert '"first_correction"' not in corrections_page.text


def test_another_accounts_success_cannot_complete_this_accounts_journey(journey):
    _ready(journey)
    _verify(journey)
    # The same slug on a different account must resolve to that account's own project.
    with m.DB.session() as db:
        # Use the actual session's owner rather than relying on test collection order.
        token = journey["stranger"].cookies.get(auth.SESSION_COOKIE_NAME)
        outsider_id = auth.parse_session_token(token, secret=os.environ["SEO_AGENT_SECRET_KEY"])["uid"]
        db.add(Project(owner_user_id=outsider_id, slug=journey["slug"], site_name="Autre site", base_url="https://autre.example/"))
        db.commit()
    page = journey["stranger"].get("/")
    assert not page.context["onboarding"]["complete"]
    assert "Votre correction est vérifiée" not in page.text


def test_notes_or_a_declared_done_status_cannot_forge_a_verified_result(journey):
    task = _choose(journey)
    client = journey["client"]
    client.get(f'/projects/{journey["slug"]}/first-correction')
    response = client.post(f'/api/projects/{journey["slug"]}/issues/missing_title/task',
                           json={"status": "done", "url": URL, "note": json.dumps({"verify": {
                               "result": "resolved", "verified_at": "2026-11-01T00:00:00Z", "crawl_ts": "20261101-100000"}})},
                           headers={m._CSRF_HEADER_NAME: client.cookies.get(m._CSRF_COOKIE_NAME, "")})
    assert response.status_code == 200 and not _state(journey)["complete"]
    with m.DB.session() as db:
        note = first_correction.note_data(db.get(IssueTask, task.id).note)
    assert not note.get("verify") and note.get("first_correction")
    corrections = client.get(f'/projects/{journey["slug"]}/corrections')
    assert not corrections.context["first_correction_complete"]
    assert "Être guidé pour ma première correction" in corrections.text


@pytest.mark.parametrize("field,value", [("url", "https://stranger.example/"), ("issue_key", "invented"),
                                        ("crawl_ts", "../../other-account")])
def test_selection_requires_current_report_evidence(journey, field, value):
    journey["report"]()
    payload = {"action": "select", "issue_key": "missing_title", "url": URL, "crawl_ts": BASELINE, field: value}
    response = _post(journey, **payload)
    assert response.status_code == 303 and "err=" in response.headers["location"]
    with m.DB.session() as db:
        assert db.scalar(select(IssueTask).where(IssueTask.project_id == journey["project_id"])) is None


def test_mutations_require_csrf_and_project_access(journey):
    slug, client = journey["slug"], journey["client"]
    assert client.post(f"/projects/{slug}/first-correction", data={"action": "ready", "task_id": "unknown"}).status_code == 403
    assert journey["stranger"].get(f"/projects/{slug}/first-correction").status_code == 404
    assert _post(journey, action="ready", task_id="unrelated-task").status_code == 404


def test_verification_uses_existing_crawl_quota_and_owner_without_spending_ai(journey, monkeypatch):
    _ready(journey)
    saved, charges = [], []
    monkeypatch.setattr(m, "_save_job", lambda job: saved.append(job))
    monkeypatch.setattr(m.billing, "ensure_within_quota", lambda *a, **kw: (True, 1000))
    monkeypatch.setattr(m.billing, "usage_add", lambda *a, **kw: charges.append(kw))
    monkeypatch.setattr(m, "_correction_charge", lambda *a, **kw: pytest.fail("Manual guidance must not spend AI credits"))
    client = journey["client"]
    token = re.search(r'name="_csrf"\s+value="([^"]+)"', client.get(f'/projects/{journey["slug"]}/first-correction').text).group(1)
    response = client.post(f'/projects/{journey["slug"]}/crawl', data={"_csrf": token}, follow_redirects=False)
    assert response.status_code == 303 and len(saved) == 1
    assert saved[0].result["user_id"] == journey["owner"]
    assert charges and all(charge["metric"] == "pages_crawled_month" and charge["user_id"] == journey["owner"] for charge in charges)
