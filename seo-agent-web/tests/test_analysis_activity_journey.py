"""Settings and job milestones reflect effects supported by the owner-scoped server."""

import time
import uuid
from urllib.parse import urlencode

import pytest
from bs4 import BeautifulSoup
from fastapi.testclient import TestClient

from backend import app as m, auth
from backend.models import Project
from test_first_verified_correction import journey as journey, BASELINE
from test_content_visibility_journey import subscribe


def owner_client(data):
    client = TestClient(m.app)
    client.cookies.set(
        auth.SESSION_COOKIE_NAME,
        auth.make_session_token(
            user_id=data["owner"], secret=m._safe_env("SEO_AGENT_SECRET_KEY")
        ),
    )
    return client


def job(data, status="running", timestamp=BASELINE):
    row = m.Job(
        id=uuid.uuid4().hex,
        status=status,
        created_at=time.time(),
        started_at=time.time(),
        progress={"type": "crawl", "current": 5, "total": 10},
        result={
            "type": "crawl",
            "user_id": data["owner"],
            "slug": data["slug"],
            "timestamp": timestamp,
            "project_url": "https://untrusted.example.invalid/result",
        },
    )
    m._save_job(row)
    return row


def form_data(form):
    data = {}
    for field in form.select("input[name], select[name]"):
        if field.has_attr("disabled") or field.get("type") in {"submit", "button"}:
            continue
        if field.get("type") == "checkbox":
            if field.has_attr("checked"):
                data[field["name"]] = field.get("value", "on")
        elif field.name == "select":
            data[field["name"]] = (
                field.select_one("option[selected]") or field.select_one("option")
            ).get("value", "")
        else:
            data[field["name"]] = field.get("value", "")
    return data


def test_plan_values_and_names_for_every_settings_control(journey):
    page = owner_client(journey).get("/projects/" + journey["slug"] + "/settings/crawl")
    assert page.status_code == 200 and page.headers["cache-control"] == "no-store"
    soup = BeautifulSoup(page.text, "html.parser")
    pages = soup.select_one("input[name=max_pages]")
    assert int(pages["value"]) <= int(pages["max"])
    for field in soup.select(
        "#analysis-settings input:not([type=hidden]), #analysis-settings select"
    ):
        assert field.get("aria-label") or (
            field.get("id") and soup.find("label", attrs={"for": field["id"]})
        ), field
        if field.get("aria-describedby"):
            assert soup.find(id=field["aria-describedby"])
    for name in (
        "strip_params",
        "max_depth",
        "min_delay_s",
        "max_url_length",
        "max_query_params",
        "deny_param_regex",
        "respect_robots_crawl_delay",
        "follow_nofollow",
    ):
        assert soup.select_one("[name=" + name + "]") is None
    assert not soup.select_one("#analysis-sources").has_attr("open")
    assert "Enregistrer ne lance pas d’analyse" in soup.text


def test_folded_form_preserves_effective_choices_and_unrelated_data(journey):
    subscribe(journey)
    with m.DB.session() as db:
        site = db.get(Project, journey["project_id"])
        site.settings = {
            **site.settings,
            "github_repo": "atelier/site",
            "cms": {"platform": "wordpress"},
            "crawl": {
                "max_pages": 123,
                "workers": 4,
                "timeout_s": 9,
                "profile": "default",
                "check_resources": True,
                "pagespeed": True,
                "user_agent": "AtelierRobot",
                "include_regex": "/guides",
                "exclude_regex": "/compte",
                "ai_keywords": True,
                "backlinks_research": False,
                "future_setting": "retained",
            },
            "gsc_api": {"enabled": False, "inspection_enabled": True, "days": 60},
            "bing": {"enabled": False, "fetch_sitemaps": True},
        }
        db.commit()
    viewer = owner_client(journey)
    path = "/projects/" + journey["slug"] + "/settings/crawl"
    payload = form_data(
        BeautifulSoup(viewer.get(path).text, "html.parser").select_one(
            "#analysis-settings"
        )
    )
    payload["max_pages"] = "124"
    result = viewer.post(path, data=payload, follow_redirects=False)
    assert result.status_code == 303 and "msg=" in result.headers["location"]
    with m.DB.session() as db:
        settings = db.get(Project, journey["project_id"]).settings
    assert (
        settings["crawl"]["max_pages"] == 124
        and settings["crawl"]["profile"] == "default"
    )
    assert (
        settings["crawl"]["workers"] == 4
        and settings["crawl"]["include_regex"] == "/guides"
    )
    assert (
        settings["crawl"]["ai_keywords"] is True
        and settings["crawl"]["backlinks_research"] is False
    )
    assert settings["crawl"]["future_setting"] == "retained"
    assert (
        settings["gsc_api"]["inspection_enabled"] is True
        and settings["bing"]["fetch_sitemaps"] is True
    )
    assert (
        settings["github_repo"] == "atelier/site"
        and settings["cms"]["platform"] == "wordpress"
    )


def test_shared_settings_readonly_without_expanding_write_access(journey):
    path = "/projects/" + journey["slug"] + "/settings/crawl"
    page = journey["client"].get(path)
    soup = BeautifulSoup(page.text, "html.parser")
    assert (
        soup.select_one("#analysis-settings fieldset[disabled]")
        and "Consultation uniquement" in page.text
    )
    payload = form_data(soup.select_one("#analysis-settings"))
    payload["max_pages"] = "1"
    response = journey["client"].post(path, data=payload, follow_redirects=False)
    assert response.status_code == 303 and "err=" in response.headers["location"]
    with m.DB.session() as db:
        assert (
            db.get(Project, journey["project_id"]).settings["crawl"]["max_pages"] == 50
        )
    assert journey["stranger"].get(path).status_code == 404


def test_explicit_repository_detach_is_scoped_and_never_touches_provider(
    journey, monkeypatch
):
    with m.DB.session() as db:
        site = db.get(Project, journey["project_id"])
        site.settings = {
            **site.settings,
            "github_repo": "atelier/site",
            "github_branch": "main",
            "github_mode": "auto",
            "cms": {"platform": "wordpress"},
        }
        db.commit()
    monkeypatch.setattr(
        m, "_github_api_get", lambda *a, **k: pytest.fail("No provider call for detach")
    )
    path = "/api/projects/" + journey["slug"] + "/github/connect"
    journey["client"].get("/settings/accounts")
    csrf = journey["client"].cookies.get(m._CSRF_COOKIE_NAME)
    assert (
        journey["client"].post(path, json={"repo": "", "disconnect": True}).status_code
        == 403
    )
    assert (
        journey["client"]
        .post(
            path,
            json={"repo": "", "disconnect": True},
            headers={m._CSRF_HEADER_NAME: csrf},
        )
        .json()["ok"]
    )
    with m.DB.session() as db:
        settings = db.get(Project, journey["project_id"]).settings
        assert not any(
            k in settings for k in ("github_repo", "github_mode", "github_branch")
        )
        assert (
            settings["cms"]["platform"] == "wordpress"
            and settings["crawl"]["max_pages"] == 50
        )
    journey["stranger"].get("/settings/accounts")
    assert (
        journey["stranger"]
        .post(
            path,
            json={"repo": "", "disconnect": True},
            headers={
                m._CSRF_HEADER_NAME: journey["stranger"].cookies.get(
                    m._CSRF_COOKIE_NAME
                )
            },
        )
        .status_code
        == 404
    )


@pytest.mark.parametrize(
    "status,readable,headline,ready,stage",
    [
        ("queued", False, "en attente", False, 1),
        ("running", True, "Analyse en cours", False, 2),
        ("done", True, "Votre bilan est disponible", True, 4),
        ("done", False, "bilan indisponible", False, 3),
        ("failed", True, "n’a pas abouti", False, 2),
        ("canceled", True, "Analyse annulée", False, 2),
    ],
)
def test_result_needs_execution_and_the_exact_readable_report(
    journey, status, readable, headline, ready, stage
):
    if readable:
        journey["report"]()
    row = job(journey, status)
    response = journey["client"].get("/jobs/" + row.id)
    assert (
        response.status_code == 200 and response.headers["cache-control"] == "no-store"
    )
    view = response.context["activity"]
    assert (
        view["report_ready"] is ready
        and view["stage"] == stage
        and headline in view["headline"]
    )
    link = BeautifulSoup(response.text, "html.parser").select_one("#job-result-link")
    assert link.has_attr("hidden") is not ready
    if ready:
        assert link["href"] == "/projects/" + journey["slug"] + "?crawl=" + BASELINE
    assert "https://untrusted.example.invalid" not in response.text
    assert (
        journey["client"].get("/api/jobs/" + row.id).json()["activity"]["report_ready"]
        is ready
    )
    assert journey["stranger"].get("/jobs/" + row.id).status_code == 404
    assert journey["stranger"].get("/api/jobs/" + row.id).status_code == 404


def test_wrong_report_and_same_slug_are_never_substituted(journey):
    journey["report"]()
    row = job(journey, "done", timestamp="20990101-000000")
    assert (
        not journey["client"].get("/jobs/" + row.id).context["activity"]["report_ready"]
    )
    with m.DB.session() as db:
        db.add(
            Project(
                owner_user_id=journey["member"],
                slug=journey["slug"],
                site_name="Autre site",
                base_url="https://other.example/",
                settings={},
            )
        )
        db.commit()
    response = journey["client"].get("/jobs/" + job(journey, "done").id)
    assert response.context["activity"]["site_name"] == "Atelier"
    assert (
        response.context["activity"]["project_url"] == ""
        and not response.context["activity"]["report_ready"]
    )


@pytest.mark.parametrize("status", ["queued", "running", "failed", "canceled"])
def test_project_opens_the_exact_analysis_tracking_without_the_list(journey, status):
    if status in {"queued", "running"}:
        journey["report"]()
    row = job(journey, status)
    page = journey["client"].get(f'/projects/{journey["slug"]}?job={row.id}')
    assert page.status_code == 200
    soup = BeautifulSoup(page.text, "html.parser")
    link = soup.find("a", href="/jobs/" + row.id)
    assert link
    if status in {"queued", "running"}:
        assert link.get_text(strip=True) == "Voir le suivi"
    detail = journey["client"].get(link["href"])
    assert detail.status_code == 200 and detail.context["job"].id == row.id
    assert journey["stranger"].get(link["href"]).status_code == 404


def test_activity_names_and_return_are_authorized_and_filtered(journey):
    row = job(journey)
    page = journey["client"].get("/jobs")
    soup = BeautifulSoup(page.text, "html.parser")
    own = soup.select_one('[data-job-id="' + row.id + '"]')
    assert "Atelier" in own.text and own["data-site"] == journey["project_id"]
    assert row.id not in journey["stranger"].get("/jobs").text
    return_to = "/jobs?" + urlencode(
        {"site": journey["project_id"], "kind": "crawl", "status": "active"}
    )
    assert (
        journey["client"]
        .get("/jobs/" + row.id, params={"return_to": return_to})
        .context["activity_return"]
        == return_to
    )
    for bad in ("https://evil.invalid", "/settings/system", "//evil.invalid"):
        assert (
            journey["client"]
            .get("/jobs/" + row.id, params={"return_to": bad})
            .context["activity_return"]
            == "/jobs"
        )
    assert journey["client"].get("/jobs/missing-job").status_code == 404


def test_finalizing_and_malformed_progress_never_imply_a_result(journey):
    row = job(journey)
    row.progress = {"current": 10, "total": 10}
    m._save_job(row)
    view = journey["client"].get("/api/jobs/" + row.id).json()["activity"]
    assert view["stage"] == 3 and not view["report_ready"]
    row.result = {**row.result, "type": "autopilot"}
    m._save_job(row)
    generic = journey["client"].get("/api/jobs/" + row.id).json()["activity"]
    assert generic["headline"] == "Finalisation en cours"
    assert "pages" not in generic["message"] and not generic["report_ready"]
    row.progress = {"current": "not-a-number", "total": "not-a-number"}
    m._save_job(row)
    response = journey["client"].get("/api/jobs/" + row.id)
    assert (
        response.status_code == 200 and not response.json()["activity"]["report_ready"]
    )
