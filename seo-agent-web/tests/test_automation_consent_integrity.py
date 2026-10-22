"""A changed automatic target needs consent; its receipt and settings commit together."""
from __future__ import annotations

import copy

import pytest
from sqlalchemy import event, select
from sqlalchemy.exc import OperationalError

from backend import app as m, legal
from backend.models import AccountMember, AuditLog, Project
from test_customer_activation import client, user
from test_les_modes_automatiques_sont_prouves import (
    _auto, client_connecte as client_connecte, connect, depot as depot,
    github as github, lab as lab, modele as modele, plan as plan, projet as projet,
)


def settings(pid):
    with m.DB.session() as db:
        return copy.deepcopy(db.get(Project, pid).settings)


def receipts(slug):
    with m.DB.session() as db:
        return list(db.scalars(select(AuditLog).where(
            AuditLog.target_id == slug, AuditLog.action.like("automation.%"))))


@pytest.mark.parametrize("target", [{"repo": "atelier/autre"}, {"branch": "release"}])
def test_a_different_target_cannot_inherit_automatic_consent(depot, target):
    assert connect(depot, mode="auto", consent=True).status_code == 200
    before, logs = settings(depot["project_id"]), receipts(depot["slug"])
    response = connect(depot, mode="auto", **target)
    assert response.status_code == 400 and "Confirmez" in response.json()["error"]
    assert settings(depot["project_id"]) == before
    assert len(receipts(depot["slug"])) == len(logs)


def test_repository_spelling_does_not_create_another_activation(depot):
    assert connect(depot, mode="auto", consent=True).status_code == 200
    before = len(receipts(depot["slug"]))
    assert connect(depot, repo="  ATELIER/Site  ", mode="auto").status_code == 200
    assert len(receipts(depot["slug"])) == before


def test_explicit_target_change_is_kept_once_with_its_project_and_actor(depot):
    assert connect(depot, mode="auto", consent=True).status_code == 200
    before = {row.id for row in receipts(depot["slug"])}
    assert connect(depot, repo="atelier/autre", branch="release", mode="auto", consent=True).status_code == 200
    assert connect(depot, repo="atelier/autre", branch="release", mode="auto").status_code == 200
    added = [row for row in receipts(depot["slug"]) if row.id not in before]
    assert len(added) == 1
    meta = added[0].meta
    assert meta["previous_repo"] == "atelier/site" and meta["repo"] == "atelier/autre"
    assert meta["previous_branch"] == "main" and meta["branch"] == "release"
    assert meta["previous"] == meta["mode"] == "auto" and meta["consent"] is True
    assert meta["project_id"] == str(depot["project_id"])
    assert meta["owner_user_id"] == added[0].actor_user_id
    assert meta["warning_version"] == legal.AUTO_MODE_WARNING_VERSION and meta["lang"] == "fr"


@pytest.mark.parametrize("operation", ["activate", "review", "disconnect", "replace"])
def test_github_audit_failure_rolls_back_settings_and_can_be_retried(depot, operation):
    assert connect(depot, mode="review" if operation == "activate" else "auto", consent=True).status_code == 200
    before = settings(depot["project_id"])
    count = len(receipts(depot["slug"]))
    body = {"mode": "auto", "consent": True}
    body.update({"review": {"mode": "review"}, "disconnect": {"repo": "", "disconnect": True},
                 "replace": {"repo": "atelier/autre"}}.get(operation, {}))

    def unavailable(_mapper, _connection, row):
        if row.action.startswith("automation.") and row.target_id == depot["slug"]:
            raise OperationalError("receipt unavailable", {}, RuntimeError("local injected failure"))

    event.listen(AuditLog, "before_insert", unavailable)
    try:
        response = connect(depot, **body)
    finally:
        event.remove(AuditLog, "before_insert", unavailable)
    assert response.status_code == 503 and response.json()["ok"] is False
    assert "Actualisez" in response.json()["error"]
    assert settings(depot["project_id"]) == before
    assert len(receipts(depot["slug"])) == count
    assert connect(depot, **body).status_code == 200
    assert len(receipts(depot["slug"])) == count + 1


def test_settings_failure_does_not_leave_a_consent_receipt(depot):
    assert connect(depot, mode="review").status_code == 200
    before, count = settings(depot["project_id"]), len(receipts(depot["slug"]))

    def unavailable(_mapper, _connection, row):
        if str(row.id) == str(depot["project_id"]):
            raise OperationalError("settings unavailable", {}, RuntimeError("local injected failure"))

    event.listen(Project, "before_update", unavailable)
    try:
        response = connect(depot, mode="auto", consent=True)
    finally:
        event.remove(Project, "before_update", unavailable)
    assert response.status_code == 503
    assert settings(depot["project_id"]) == before
    assert len(receipts(depot["slug"])) == count


@pytest.mark.parametrize("operation", ["activate", "disable", "review", "section"])
def test_content_audit_failure_rolls_back_settings_and_can_be_retried(client_connecte, plan, operation):
    browser, slug, pid = client_connecte
    initial = "verification" if operation == "activate" else "auto"
    assert _auto._regler(browser, slug, enabled=True, section="/blog", publication=initial, consent=True).status_code == 200
    before, count = settings(pid), len(receipts(slug))
    body = {"enabled": True, "section": "/blog", "publication": "auto", "consent": True}
    body.update({"disable": {"enabled": False}, "review": {"publication": "verification"},
                 "section": {"section": "/guides"}}.get(operation, {}))

    def unavailable(_mapper, _connection, row):
        if row.action.startswith("automation.") and row.target_id == slug:
            raise OperationalError("receipt unavailable", {}, RuntimeError("local injected failure"))

    event.listen(AuditLog, "before_insert", unavailable)
    try:
        response = _auto._regler(browser, slug, **body)
    finally:
        event.remove(AuditLog, "before_insert", unavailable)
    assert response.status_code == 503 and response.json()["ok"] is False
    assert settings(pid) == before and len(receipts(slug)) == count
    assert _auto._regler(browser, slug, **body).status_code == 200
    assert len(receipts(slug)) == count + 1


def test_automatic_content_section_change_needs_a_new_consent(client_connecte, plan):
    browser, slug, pid = client_connecte
    assert _auto._regler(browser, slug, enabled=True, section="/blog", publication="auto", consent=True).status_code == 200
    before, count = settings(pid), len(receipts(slug))
    response = _auto._regler(browser, slug, enabled=True, section="/guides", publication="auto")
    assert response.status_code == 400
    assert settings(pid) == before and len(receipts(slug)) == count
    assert _auto._regler(browser, slug, enabled=True, section="/guides", publication="auto", consent=True).status_code == 200
    added = receipts(slug)
    assert len(added) == count + 1
    assert any(row.meta.get("previous_section") == "/blog" and row.meta["section"] == "/guides" for row in added)


def test_consent_is_compared_to_the_state_after_the_provider_read(depot, monkeypatch):
    assert connect(depot, mode="auto", consent=True).status_code == 200

    def provider(*args, **kwargs):
        with m.DB.session() as db:
            row = db.get(Project, depot["project_id"])
            row.settings = {**row.settings, "github_mode": "review", "concurrent_setting": "preserved"}
            db.commit()
        return {"permissions": {"push": True}, "default_branch": "main"}

    monkeypatch.setattr(m, "_github_api_get", provider)
    count = len(receipts(depot["slug"]))
    assert connect(depot, mode="auto").status_code == 400
    saved = settings(depot["project_id"])
    assert saved["github_mode"] == "review" and saved["concurrent_setting"] == "preserved"
    assert connect(depot, mode="review").status_code == 200
    assert settings(depot["project_id"])["concurrent_setting"] == "preserved"
    assert len(receipts(depot["slug"])) == count


def test_member_consent_keeps_the_actor_and_the_project_owner_distinct(depot, lab):
    member = user(lab["db"])
    with m.DB.session() as db:
        owner = db.get(Project, depot["project_id"]).owner_user_id
        db.add(AccountMember(owner_user_id=owner, member_user_id=str(member.id)))
        db.commit()
    browser = client(member)
    browser.get("/settings/accounts?lang=en")
    path = f'/api/projects/{depot["slug"]}/github/connect'
    assert browser.post(path, json={"repo": "atelier/site", "mode": "auto", "consent": True}).status_code == 403
    token = browser.cookies.get(m._CSRF_COOKIE_NAME)
    response = browser.post(path, json={"repo": "atelier/site", "mode": "auto", "consent": True},
                            headers={m._CSRF_HEADER_NAME: token})
    assert response.status_code == 200
    row = receipts(depot["slug"])[0]
    assert row.actor_user_id == str(member.id) and row.meta["owner_user_id"] == str(owner)
    assert row.meta["project_id"] == str(depot["project_id"]) and row.meta["lang"] == "en"
    stranger = client(user(lab["db"]))
    stranger.get("/settings/accounts")
    assert stranger.post(path, json={"repo": "atelier/site", "mode": "auto", "consent": True},
                         headers={m._CSRF_HEADER_NAME: stranger.cookies.get(m._CSRF_COOKIE_NAME)}).status_code == 404
    assert len(receipts(depot["slug"])) == 1
