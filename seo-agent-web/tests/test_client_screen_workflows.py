"""Customer-facing counts and archive access stay aligned with real report data and billing."""
from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from backend import app as m, auth, billing
from backend.models import AccountMember, Project, User


@pytest.fixture()
def client_report(tmp_path: Path, monkeypatch):
    m.DB.create_tables()
    tag = uuid.uuid4().hex
    with m.DB.session() as db:
        owner = User(email=f"screens-{tag}@example.invalid", password_hash="test", is_admin=False)
        member = User(email=f"screens-member-{tag}@example.invalid", password_hash="test", is_admin=False)
        db.add_all([owner, member])
        db.commit()
        owner_id, member_id = str(owner.id), str(member.id)
        slug = f"screens-{tag}"
        db.add(Project(owner_user_id=owner_id, slug=slug, site_name="Atelier", base_url="https://atelier.example/"))
        db.add(AccountMember(owner_user_id=owner_id, member_user_id=member_id))
        db.commit()
    ts = "20261017-100000"
    audit = tmp_path / slug / ts / "audit"
    audit.mkdir(parents=True)
    pages = [{"url": f"https://atelier.example/page-{i}", "status_code": 200,
              "content_type": "text/html", "indexable": True} for i in range(12)]
    report = {"meta": {"base_url": "https://atelier.example/", "site_name": "Atelier"},
              "pages": pages, "issues": {
                  "title_tag_changed": {"count": 6, "examples": [p["url"] for p in pages[:6]]},
                  "missing_title": {"count": 2, "examples": [p["url"] for p in pages[:2]]},
                  "title_too_long_indexable": {"count": 4, "examples": [p["url"] for p in pages[2:6]]},
                  "meta_description_too_long_indexable": {"count": 3, "examples": [p["url"] for p in pages[6:9]]},
              }}
    (audit / "report.json").write_text(json.dumps(report), encoding="utf-8")
    (audit.parent / "run.json").write_text(json.dumps({"site_name": "Atelier", "base_url": "https://atelier.example/"}), encoding="utf-8")
    monkeypatch.setattr(m, "_runs_dir_pour_slug", lambda *args: tmp_path)
    plan = {"value": "free", "payer": None}

    def effective_plan(db, *, user_id):
        plan["payer"] = user_id
        return plan["value"]

    monkeypatch.setattr(billing, "effective_plan_key", effective_plan)
    client = TestClient(m.app)
    client.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(user_id=member_id, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    return client, slug, plan, owner_id


def test_overview_and_issues_agree_on_types_and_occurrences(client_report):
    client, slug, _, _ = client_report
    for suffix in ("", "/issues"):
        response = client.get(f"/projects/{slug}{suffix}")
        assert response.status_code == 200
        assert "3 types de problèmes · 9 occurrences" in response.text
    filtered = client.get(f"/projects/{slug}/issues?q=longue")
    assert filtered.status_code == 200
    assert "2 types de problèmes · 7 occurrences" in filtered.text
    assert "résultats filtrés" in filtered.text


@pytest.mark.parametrize("plan_key,unlocked", [("free", False), ("solo", True), ("pro", True)])
def test_corrections_archive_uses_the_project_owners_plan(client_report, plan_key, unlocked):
    client, slug, plan, owner_id = client_report
    plan["value"] = plan_key
    response = client.get(f"/projects/{slug}/corrections")
    assert response.status_code == 200
    assert plan["payer"] == owner_id
    assert response.context["fix_pack_unlocked"] is unlocked
    assert (f'href="/projects/{slug}/export/fix-pack.zip?' in response.text) is unlocked
    assert "Voir les instructions" in response.text
    assert "Rapport à transmettre (PDF)" in response.text
    assert "GitHub requis" not in response.text
    # The direct download route still enforces the same entitlement.
    download = client.get(f"/projects/{slug}/export/fix-pack.zip", follow_redirects=False)
    assert download.status_code == (200 if unlocked else 303)
    if unlocked:
        assert download.headers["content-type"] == "application/zip"
    else:
        assert download.headers["location"].startswith("/billing?")
