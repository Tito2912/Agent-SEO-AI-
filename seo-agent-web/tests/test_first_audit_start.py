"""The first audit creates a site and a real queued job through the existing billing path."""
from __future__ import annotations

import json
import os
import socket
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from backend import app as m, auth, billing
from backend.models import AccountActivation, AccountMember, JobRecord, Project, UsageEvent, User


@pytest.fixture()
def first(tmp_path, monkeypatch):
    m.DB.create_tables()
    with m.DB.session() as db:
        user = User(email=f"first-audit-{uuid.uuid4().hex}@example.invalid", password_hash="test", country="FR")
        db.add(user)
        db.commit()
        uid = str(user.id)
    monkeypatch.setattr(m, "_crawl_defaults_from_config", lambda *args: {})
    monkeypatch.setattr(m, "_runs_dir_for_user", lambda owner: tmp_path / owner)
    monkeypatch.setattr(m, "_email_verification_enabled", lambda: False)
    monkeypatch.setattr(m, "_gsc_oauth_connected", lambda *args: False)
    monkeypatch.setattr(m, "_build_github_connection_state", lambda **kwargs: {})
    monkeypatch.setattr(m.socket, "getaddrinfo", lambda *args, **kwargs: [
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0))])
    client = TestClient(m.app)
    client.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    client.get("/")
    return {"client": client, "uid": uid, "path": tmp_path}


def submit(first, value="atelier.example", **fields):
    client = first["client"]
    return client.post("/projects/first-audit", data={"site_url": value,
        "_csrf": client.cookies.get(m._CSRF_COOKIE_NAME), **fields}, follow_redirects=False)


def rows(first, model):
    owner_field = model.user_id if model in {UsageEvent, AccountActivation} else model.owner_user_id
    with m.DB.session() as db:
        return list(db.scalars(select(model).where(owner_field == first["uid"])))


def spend(first, amount):
    with m.DB.session() as db:
        billing.usage_add(db, user_id=first["uid"], metric="pages_crawled_month", amount=amount)


def report(first, slug="atelier-example", *, issues=None):
    dest = first["path"] / first["uid"] / slug / "20261018-120000" / "audit"
    dest.mkdir(parents=True, exist_ok=True)
    (dest / "report.json").write_text(json.dumps({"meta": {"base_url": "https://atelier.example/"},
        "pages": [{"url": "https://atelier.example/", "status_code": 200, "content_type": "text/html", "indexable": True}],
        "issues": issues or {}}))


def test_empty_home_shows_actual_allowance_without_reserving_pages(first):
    page = first["client"].get("/")
    assert page.status_code == 200 and page.headers["cache-control"] == "no-store"
    assert 'action="/projects/first-audit"' in page.text and 'name="site_url"' in page.text
    assert page.context["first_audit"]["remaining"] == 800
    assert "Ajouter et analyser" in page.text and "800 pages disponibles" in page.text
    assert not rows(first, Project) and not rows(first, UsageEvent) and not rows(first, JobRecord)


@pytest.mark.parametrize("value", ["atelier.example", "https://atelier.example/service?x=1#section"])
def test_one_submission_adds_site_and_queues_owned_crawl_with_real_reservation(first, value):
    response = submit(first, value)
    assert response.status_code == 303 and response.headers["location"].startswith("/projects/atelier-example?job=")
    project, = rows(first, Project)
    job, = rows(first, JobRecord)
    assert project.base_url == "https://atelier.example/" and job.owner_user_id == first["uid"]
    assert job.status == "queued" and job.kind == "crawl" and job.result["started_by"] == first["uid"]
    events = rows(first, UsageEvent)
    assert len(events) == 1 and events[0].metric == "pages_crawled_month"
    assert events[0].amount == job.result["quota_reserved_pages"] <= 800
    assert events[0].meta["job_id"] == job.id
    activation, = rows(first, AccountActivation)
    assert activation.project_added_at and not activation.audit_available_at
    page = first["client"].get(response.headers["location"])
    assert page.status_code == 200 and "Votre analyse est en attente" in page.text
    assert "Lancer la première analyse" not in page.text and "Suivre la progression" in page.text


def test_remaining_pages_are_shown_and_reserved_without_exceeding_plan(first):
    spend(first, 793)
    page = first["client"].get("/")
    assert page.context["first_audit"]["page_cap"] == 7 and "7 pages disponibles" in page.text
    submit(first)
    job, = rows(first, JobRecord)
    assert job.result["quota_reserved_pages"] == 7 and job.result["override_max_pages"] == 7


def test_exhausted_quota_disables_start_and_cannot_create_a_site_on_post(first):
    spend(first, 800)
    assert not first["client"].get("/").context["first_audit"]["can_start"]
    response = submit(first)
    assert response.status_code == 400 and "/billing" in response.text
    assert not rows(first, Project) and not rows(first, JobRecord) and len(rows(first, UsageEvent)) == 1


def test_personal_site_limit_is_checked_before_creating_another_site(first):
    submit(first)
    response = submit(first, "second.example")
    assert response.status_code == 400 and len(rows(first, Project)) == 1 and len(rows(first, JobRecord)) == 1


@pytest.mark.parametrize("value", ["", "bad host.example", "ftp://atelier.example", "https://client:secret@atelier.example",
                                  "https://atelier.example:wrong", "https://[broken", "a" * 2049])
def test_invalid_address_keeps_form_and_never_creates_or_charges(first, value):
    response = submit(first, value)
    assert response.status_code == 400 and 'name="site_url"' in response.text
    assert not rows(first, Project) and not rows(first, JobRecord) and not rows(first, UsageEvent)


@pytest.mark.parametrize("value", ["localhost", "127.0.0.1", "https://atelier.example:8000", "private.local"])
def test_existing_target_guards_also_apply_to_one_click_start(first, value):
    assert submit(first, value).status_code == 400
    assert not rows(first, Project) and not rows(first, JobRecord)


def test_private_dns_target_is_rejected(first, monkeypatch):
    monkeypatch.setattr(m.socket, "getaddrinfo", lambda *args, **kwargs: [
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.2", 0))])
    assert submit(first).status_code == 400
    assert not rows(first, Project)


def test_csrf_and_authentication_are_required(first):
    response = first["client"].post("/projects/first-audit", data={"site_url": "atelier.example"}, follow_redirects=False)
    assert response.status_code == 403
    anonymous = TestClient(m.app)
    anonymous.get("/auth/login")
    response = anonymous.post("/projects/first-audit", data={"site_url": "atelier.example",
        "_csrf": anonymous.cookies.get(m._CSRF_COOKIE_NAME)}, follow_redirects=False)
    assert response.status_code == 303 and response.headers["location"].startswith("/auth/login")
    assert not rows(first, Project) and not rows(first, JobRecord)


def test_repeated_submission_reuses_active_job_even_after_all_pages_are_reserved(first):
    spend(first, 799)
    response = submit(first)
    again = submit(first)
    assert again.headers["location"] == response.headers["location"]
    assert len(rows(first, JobRecord)) == 1 and len(rows(first, Project)) == 1
    assert len(rows(first, UsageEvent)) == 2


def test_concurrent_double_click_in_one_web_process_creates_one_job_and_reservation(first):
    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(lambda _: submit(first), range(2)))
    assert all(r.status_code == 303 for r in responses)
    assert responses[0].headers["location"] == responses[1].headers["location"]
    assert len(rows(first, JobRecord)) == len(rows(first, Project)) == len(rows(first, UsageEvent)) == 1


def test_existing_site_keeps_name_settings_and_uses_its_own_crawl_bounds(first):
    with m.DB.session() as db:
        db.add(Project(owner_user_id=first["uid"], slug="atelier-example", base_url="https://atelier.example/",
                       site_name="Mon atelier", settings={"crawl": {"max_pages": 2}, "cms": "wordpress"}))
        db.commit()
    response = submit(first)
    assert response.status_code == 303
    project, = rows(first, Project)
    assert project.site_name == "Mon atelier" and project.settings["cms"] == "wordpress"
    job, = rows(first, JobRecord)
    assert job.result["quota_reserved_pages"] == 2


def test_site_slug_collision_does_not_replace_or_analyze_another_domain(first):
    with m.DB.session() as db:
        db.add(Project(owner_user_id=first["uid"], slug="atelier-example", base_url="https://atelier-example/", site_name="Autre domaine"))
        db.commit()
    assert submit(first).status_code == 400
    assert not rows(first, JobRecord) and rows(first, Project)[0].base_url == "https://atelier-example/"


def test_preview_and_real_job_obey_configured_per_analysis_cap(first, monkeypatch):
    monkeypatch.setenv("PLAN_CONFIG_JSON", json.dumps({"free": {"crawl": {"max_pages_per_crawl": 4}}}))
    assert first["client"].get("/").context["first_audit"]["page_cap"] == 4
    assert submit(first).status_code == 303
    assert rows(first, JobRecord)[0].result["quota_reserved_pages"] == 4


def test_first_site_creation_rate_limit_does_not_create_or_reserve(first, monkeypatch):
    monkeypatch.setattr(m, "_rate_limit_retry_after", lambda **kwargs: 30)
    response = submit(first)
    assert response.status_code == 429 and response.headers["retry-after"] == "30"
    assert not rows(first, Project) and not rows(first, JobRecord) and not rows(first, UsageEvent)


def test_existing_report_is_reopened_without_starting_another_audit(first):
    submit(first)
    with m.DB.session() as db:
        job = db.get(JobRecord, rows(first, JobRecord)[0].id)
        job.status = "done"
        db.commit()
    report(first)
    assert submit(first).headers["location"] == "/projects/atelier-example"
    assert len(rows(first, JobRecord)) == len(rows(first, UsageEvent)) == 1
    page = first["client"].get("/")
    assert page.context["onboarding"]["resume"]["label"] == "Voir mes résultats"


@pytest.mark.parametrize("status", ["failed", "canceled"])
def test_failed_or_canceled_analysis_can_resume_without_recreating_the_site(first, status):
    submit(first)
    with m.DB.session() as db:
        job = db.get(JobRecord, rows(first, JobRecord)[0].id)
        job.status = status
        db.commit()
    # Match the worker's existing quota adjustment after an unsuccessful run.
    spend(first, -rows(first, UsageEvent)[0].amount)
    page = first["client"].get("/")
    resume = page.context["onboarding"]["resume"]
    assert resume["post"] and resume["label"] == "Réessayer l’analyse"
    assert resume["href"] == "/projects/atelier-example/crawl"
    response = first["client"].post(resume["href"], data={
        "_csrf": first["client"].cookies.get(m._CSRF_COOKIE_NAME)}, follow_redirects=False)
    assert response.status_code == 303 and "?job=" in response.headers["location"]
    assert len(rows(first, Project)) == 1 and len(rows(first, JobRecord)) == 2


def test_another_accounts_site_or_job_cannot_be_reused_or_charged(first):
    with m.DB.session() as db:
        other = User(email=f"first-audit-other-{uuid.uuid4().hex}@example.invalid", password_hash="test")
        db.add(other)
        db.commit()
        other_id = str(other.id)
        db.add(Project(owner_user_id=other_id, slug="atelier-example", base_url="https://atelier.example/", site_name="Autre client"))
        db.add(JobRecord(id=uuid.uuid4().hex, owner_user_id=other_id, slug="atelier-example", kind="crawl", status="running"))
        db.commit()
    response = submit(first, owner_user_id=other_id)
    assert response.status_code == 303 and len(rows(first, JobRecord)) == len(rows(first, Project)) == 1
    assert rows(first, JobRecord)[0].result["user_id"] == first["uid"]


def test_resume_does_not_use_a_host_accounts_job_with_the_same_slug(first):
    submit(first)
    own_job = rows(first, JobRecord)[0]
    with m.DB.session() as db:
        host = User(email=f"first-audit-host-{uuid.uuid4().hex}@example.invalid", password_hash="test")
        db.add(host)
        db.commit()
        host_id = str(host.id)
        db.add(AccountMember(owner_user_id=host_id, member_user_id=first["uid"]))
        db.add(Project(owner_user_id=host_id, slug="atelier-example", base_url="https://atelier.example/", site_name="Hôte"))
        db.add(JobRecord(id=uuid.uuid4().hex, owner_user_id=host_id, slug="atelier-example", kind="crawl", status="running",
                         created_at=time.time() + 1, result={"type": "crawl", "slug": "atelier-example", "user_id": host_id}))
        db.commit()
    page = first["client"].get("/")
    assert page.context["onboarding"]["resume"]["href"] == f"/projects/atelier-example?job={own_job.id}"
