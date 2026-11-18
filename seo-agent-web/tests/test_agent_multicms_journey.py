"""Project-scoped, explicitly reviewed publication from a real stored Noyaru article."""
import json
import re
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from backend import app as m, cms_publication_routes as routes, agent_routes
from backend.models import AgentActionChoice, AgentPageEvent, CmsPublication, CmsPublicationConsent, IssueTask, Project
from backend.cms_content_transport import ContentTransport
from backend.cms_commun import CmsError
import test_first_verified_correction as first

journey = first.journey


def csrf(data, path=None):
    page = data["client"].get(path or f'/projects/{data["slug"]}/agent')
    assert page.status_code == 200
    return re.search(r'name="_csrf"\s+value="([^"]+)"', page.text).group(1)


def post(data, suffix, **body):
    token = csrf(data)
    return data["client"].post(f'/projects/{data["slug"]}/{suffix}',
                               data={"_csrf": token, **body}, follow_redirects=False)


def test_plan_uses_local_evidence_and_never_refreshes_search_on_get(journey, monkeypatch):
    journey["report"]()
    monkeypatch.setattr(m, "_fetch_gsc_live_items", lambda **kw: pytest.fail("GET must not call search APIs"))
    page = journey["client"].get(f'/projects/{journey["slug"]}/agent')
    assert page.status_code == 200
    assert page.headers["cache-control"] == "no-store"
    assert page.context["actions"]
    action = page.context["actions"][0]
    assert action["evidence"] and action["page_url"] == first.URL
    assert journey["stranger"].get(f'/projects/{journey["slug"]}/agent').status_code == 404
    result = post(journey, "agent/decision", action_key=action["action_key"], state="dismissed")
    assert result.status_code == 303
    refreshed = journey["client"].get(f'/projects/{journey["slug"]}/agent')
    assert all(a["action_key"] != action["action_key"] for a in refreshed.context["actions"])
    with m.DB.session() as db:
        choice = db.scalar(select(AgentActionChoice).where(AgentActionChoice.project_id == journey["project_id"]))
        assert choice.state == "dismissed"
        assert not list(db.scalars(select(IssueTask).where(IssueTask.project_id == journey["project_id"])))


def test_context_rejects_pages_outside_the_project(journey):
    result = post(journey, "agent/context", activity="Réparation de vélos", services="Entretien",
                  priority_urls="https://another.example/service")
    assert "err=" in result.headers["location"]
    with m.DB.session() as db:
        assert not (db.get(Project, journey["project_id"]).settings or {}).get("agent")


def test_a_direct_github_content_request_cannot_bypass_the_new_checkbox(journey, monkeypatch):
    monkeypatch.setattr(m, "_project_github_cfg", lambda *args: pytest.fail("No provider setup without consent"))
    token = csrf(journey)
    response = journey["client"].post(f'/api/projects/{journey["slug"]}/content/draft',
             json={"sujet": "Entretenir un vélo", "route": "/guides/velo/"},
             headers={m._CSRF_HEADER_NAME: token})
    assert response.status_code == 400 and "accord" in response.json()["error"]


def test_page_results_use_real_scoped_periods_and_exclude_change_day(journey, monkeypatch):
    @contextmanager
    def credentials(**kwargs):
        assert kwargs["user_id"] == journey["owner"]
        yield Path("fictional-credentials.json"), "test", ""
    calls = []
    def fetch(**kwargs):
        calls.append(kwargs)
        return {"ok": True, "complete": True, "dimension": "page", "filters": {"type": "web"},
                "page_url": kwargs["page_url"], "property": kwargs["property_url"],
                "start_date": kwargs["start_date"].isoformat(), "end_date": kwargs["end_date"].isoformat(),
                "rows": [{"page": kwargs["page_url"], "clicks": 10 * len(calls), "impressions": 100,
                          "position": 8.0}]}
    monkeypatch.setattr(m, "_gsc_live_credentials", credentials)
    monkeypatch.setattr(m, "_load_gsc_fetch_module", lambda: object())
    monkeypatch.setattr(m, "_gsc_property_candidates", lambda *args: ["sc-domain:atelier.example"])
    monkeypatch.setattr(agent_routes.agent_gsc, "fetch_page_period", fetch)
    monkeypatch.setattr(agent_routes.agent_gsc, "_today_pacific", lambda: date(2026, 11, 18))
    response = post(journey, "agent/results", page_url=first.URL, change_date="2026-10-01", days="7")
    assert response.status_code == 303 and "err=" not in response.headers["location"]
    assert [(c["start_date"], c["end_date"]) for c in calls] == [
        (date(2026, 9, 24), date(2026, 9, 30)), (date(2026, 10, 2), date(2026, 10, 8))]
    with m.DB.session() as db:
        result = db.scalar(select(AgentPageEvent).where(AgentPageEvent.project_id == journey["project_id"]))
        assert result.data["comparison"]["comparable"] is True
        assert result.data["comparison"]["causality"] == "not_established"
        assert result.data["change_source"] == "customer_date"
    page = journey["client"].get(f'/projects/{journey["slug"]}/agent/results')
    assert page.status_code == 200 and "2026-09-24" in page.text
    assert "10" in page.text and "20" in page.text
    assert journey["stranger"].get(f'/projects/{journey["slug"]}/agent/results').status_code == 404
    board = journey["client"].get(f'/projects/{journey["slug"]}/agent')
    assert not board.context["events"]  # A measurement is not an accepted CMS publication.


def test_a_recent_change_does_not_invent_results_or_call_google(journey, monkeypatch):
    monkeypatch.setattr(m, "_gsc_live_credentials", lambda **kwargs: pytest.fail("Window is too recent"))
    monkeypatch.setattr(agent_routes.agent_gsc, "_today_pacific", lambda: date(2026, 11, 18))
    response = post(journey, "agent/results", page_url=first.URL, change_date="2026-11-17", days="28")
    assert response.status_code == 303
    with m.DB.session() as db:
        result = db.scalar(select(AgentPageEvent).where(AgentPageEvent.project_id == journey["project_id"]))
        assert result.data["comparison"]["reason"] == "period_not_final"
        assert not result.data["comparison"]["before"]


def test_site_wide_webflow_publish_is_forbidden_and_shopify_has_no_customer_surface(journey, monkeypatch):
    transport = ContentTransport("webflow", {"site_id": "site-1", "token": "fictional"}, project_url=first.URL)
    with pytest.raises(CmsError):
        transport("POST", "/v2/sites/site-1/publish", {"domains": ["atelier.example"]})
    monkeypatch.setattr(routes, "bound_transport", lambda *args: pytest.fail("No Shopify connection is authorized"))
    root = f'/projects/{journey["slug"]}/content/publication'
    assert journey["client"].get(root + "?platform=shopify").status_code == 404
    assert post(journey, "content/publication/connect", platform="shopify").status_code == 404
    assert "Shopify" not in journey["client"].get(root).text
    assert routes.policy(m, "shopify")["enabled"] is False


@pytest.mark.parametrize("platform", list(routes.LABELS), ids=list(routes.LABELS))
def test_each_platform_has_a_scoped_connection_screen(journey, platform):
    path = f'/projects/{journey["slug"]}/content/publication?platform={platform}'
    page = journey["client"].get(path)
    assert page.status_code == 200 and routes.LABELS[platform] in page.text
    assert page.headers["cache-control"] == "no-store"
    assert journey["stranger"].get(path).status_code == 404
    if platform == "prestashop":
        assert "page CMS" in page.text


@pytest.fixture()
def stored_article(journey):
    versions = [{"code": "fr", "langue": "Français", "titre": "Entretenir votre vélo", "slug": "entretien-velo",
                 "adresse": "/guides/entretien-velo", "description": "Les bons gestes d’entretien.",
                 "markdown": "Un entretien adapté protège les pièces de votre vélo.", "notes": []}]
    with m.DB.session() as db:
        row = IssueTask(project_id=journey["project_id"], user_id=journey["owner"], created_by=journey["member"],
                        issue_key=m._CONTENT_ARTICLE_KEY, issue_label="Article rédigé", url="/guides/entretien-velo",
                        status="done", severity="notice", note=json.dumps({"versions": versions}, ensure_ascii=False))
        db.add(row)
        db.commit()
        return str(row.id)


@pytest.mark.parametrize("has_receipt", [True, False], ids=["dated-receipt", "missing-receipt"])
def test_publication_measurement_uses_its_immutable_receipt_date(journey, stored_article, monkeypatch, has_receipt):
    with m.DB.session() as db:
        publication = CmsPublication(project_id=journey["project_id"], task_id=stored_article,
            created_by=journey["member"], platform="wordpress", language="fr", intent_key="dated-publication",
            state="published", remote_url=first.URL, data={"article": {"title": "Entretien vélo"}},
            updated_at=datetime(2026, 11, 17, 12, tzinfo=timezone.utc))
        db.add(publication)
        db.flush()
        publication_id = publication.id
        if has_receipt:
            db.add(AgentPageEvent(project_id=journey["project_id"], event_key="cms:" + publication_id + ":published",
                page_url=first.URL, kind="publication_accepted", data={"publication_id": publication_id},
                created_at=datetime(2026, 11, 15, 1, tzinfo=timezone.utc)))
        db.commit()
    monkeypatch.setattr(agent_routes.agent_gsc, "_today_pacific", lambda: date(2026, 11, 18))
    monkeypatch.setattr(m, "_gsc_live_credentials", lambda **kwargs: pytest.fail("Window is too recent"))
    response = post(journey, "agent/results", publication_id=publication_id, days="7")
    assert response.status_code == (303 if has_receipt else 409)
    with m.DB.session() as db:
        measurement = db.scalar(select(AgentPageEvent).where(AgentPageEvent.project_id == journey["project_id"],
                                                            AgentPageEvent.kind == "search_comparison"))
        if has_receipt:
            assert measurement.data["change_date"] == "2026-11-14"  # Search Console uses Pacific dates.
            assert measurement.data["change_source"] == "cms_publication"
        else:
            assert measurement is None


@pytest.mark.parametrize("platform", ["wordpress", "webflow"], ids=["wordpress", "webflow-item"])
def test_review_create_publish_is_a_complete_separate_flow(journey, stored_article, monkeypatch, platform):
    calls = []
    state = {"status": "draft", "revision": "r1"}

    class Transport:
        def __init__(self, *args, **kwargs):
            pass

        def check(self):
            return {"verified": True, "label": "Atelier", "destinations": [{"id": "collection-1", "title": "Guides"}]}

        def describe_destination(self, ident):
            assert ident == "collection-1"
            return {"fields": [{"slug": "body", "type": "RichText", "editable": True}]}

    class Adapter:
        def create_draft(self, draft):
            calls.append("create")
            assert "<h1>" not in draft.html
            with m.DB.session() as db:
                consent = db.scalar(select(CmsPublicationConsent).where(CmsPublicationConsent.project_id == journey["project_id"]))
                assert consent.actor_id == journey["member"] and consent.approved_at
                assert consent.targets["platform"] == platform and consent.operation == "create"
            return self.read("42")

        def read(self, remote_id):
            return SimpleNamespace(id=remote_id, status=state["status"], revision=state["revision"],
                                   url="https://atelier.example/guides/entretien-velo")

        def publish(self, remote_id, expected_revision):
            assert expected_revision == "r1"
            calls.append("publish")
            state.update(status="published", revision="r2")
            return self.read(remote_id)

    monkeypatch.setattr(routes, "ContentTransport", Transport)
    monkeypatch.setattr(routes, "connection", lambda *args: ({"base_url": "https://atelier.example", "username": "editor", "password": "fictional"}, {}))
    monkeypatch.setattr(routes, "get_adapter", lambda *args, **kwargs: Adapter())
    response = post(journey, "content/publication/prepare", platform=platform, version=stored_article + "|fr",
                    destination="collection-1", body_field="body")
    assert response.status_code == 303 and "err=" not in response.headers["location"]
    assert not calls
    review_path = response.headers["location"]
    publication_id = review_path.rsplit("/", 1)[-1]
    review = journey["client"].get(review_path)
    assert review.status_code == 200 and "brouillon" in review.text
    assert "frame-src 'self'" in review.headers["content-security-policy"]
    assert journey["stranger"].get(review_path).status_code == 404
    preview = journey["client"].get(review_path + "/preview")
    assert preview.status_code == 200 and "sandbox" in preview.headers["content-security-policy"]
    assert "noindex" in preview.headers["x-robots-tag"]
    assert journey["stranger"].get(review_path + "/preview").status_code == 404
    suffix = "content/publication/" + publication_id + "/execute"
    refused = post(journey, suffix, operation="create")
    assert "err=" in refused.headers["location"] and not calls
    assert "err=" not in post(journey, suffix, operation="create", approved="true").headers["location"]
    assert calls == ["create"]
    post(journey, suffix, operation="create", approved="true")
    assert calls == ["create"]
    assert "err=" not in post(journey, suffix, operation="publish", approved="true").headers["location"]
    assert calls == ["create", "publish"]
    with m.DB.session() as db:
        row = db.get(CmsPublication, publication_id)
        assert row.state == "published" and row.remote_id == "42"
        consents = list(db.scalars(select(CmsPublicationConsent).where(CmsPublicationConsent.publication_id == publication_id)))
        assert {consent.operation for consent in consents} == {"create", "publish"}
        assert all(consent.actor_id == journey["member"] and consent.content_digest for consent in consents)
        assert next(c for c in consents if c.operation == "publish").targets["remote_id"] == "42"
    final = journey["client"].get(review_path)
    assert "Le CMS confirme la publication" in final.text
    assert "Une nouvelle analyse" in final.text
