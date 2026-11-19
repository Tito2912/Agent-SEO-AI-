"""Migrate real SQLite/PostgreSQL schemas without inventing approvals for enabled settings."""
import os
from pathlib import Path
import subprocess
import sys

import pytest
from sqlalchemy import inspect, select, text

from backend.db import Database
from backend.models import AutomationMandate, AutomationRun, CmsPublicationConsent, Project, User


@pytest.mark.parametrize("precreated", [False, True], ids=["old-schema", "models-first"])
@pytest.mark.parametrize("backend", ["sqlite", "postgresql"])
def test_mode_migration_and_deletion_keep_archived_evidence(tmp_path, monkeypatch, precreated, backend):
    postgres = os.getenv("NOYARU_TEST_POSTGRES_URL")
    if backend == "postgresql" and not postgres:
        pytest.skip("Explicit disposable PostgreSQL laboratory required")
    url = postgres if backend == "postgresql" else "sqlite:///" + str(tmp_path / "modes.sqlite")
    if backend == "postgresql":
        from sqlalchemy.engine import make_url
        parsed = make_url(url)
        assert parsed.host == "127.0.0.1" and parsed.database == "noyaru_consents" and parsed.port == 35434
    monkeypatch.setenv("DATABASE_URL", url)
    database = Database(data_dir=tmp_path / "data")
    web = Path(__file__).resolve().parents[1]
    def migrate(direction, revision, *, success=True):
        result = subprocess.run([sys.executable, "-m", "alembic", "-c", str(web / "alembic.ini"), direction, revision],
            cwd=web, env=os.environ.copy(), capture_output=True, text=True, encoding="utf-8", timeout=90)
        if success:
            assert result.returncode == 0, result.stderr
        else:
            assert result.returncode != 0 and "Descente refusee" in result.stderr
    if backend == "postgresql":
        with database.engine.begin() as conn:
            marker = os.environ.get("NOYARU_TEST_POSTGRES_MARKER", "")
            assert len(marker) == 32 and all(c in "0123456789abcdef" for c in marker)
            assert conn.scalar(text("SELECT marker FROM noyaru_lab_marker")) == marker
            conn.execute(text("DROP SCHEMA public CASCADE"))
            conn.execute(text("CREATE SCHEMA public"))
            conn.execute(text("CREATE TABLE noyaru_lab_marker (marker text NOT NULL)"))
            conn.execute(text("INSERT INTO noyaru_lab_marker VALUES (:marker)"), {"marker": marker})
    if precreated:
        database.create_tables()
    migrate("upgrade", "20261119_0034")
    with database.session() as db:
        user = User(email="mode-lab@example.invalid", password_hash="fictional")
        db.add(user); db.flush()
        project = Project(owner_user_id=user.id, slug="mode-lab", site_name="Fictional", base_url="https://example.invalid/",
                          settings={"github_mode": "auto", "github_repo": "fixture/site", "content_auto": {"enabled": True}})
        db.add(project); db.commit()
        pid, uid = project.id, user.id
    migrate("upgrade", "head")
    migrate("upgrade", "head")
    schema = inspect(database.engine)
    for model in (AutomationMandate, AutomationRun):
        assert {c["name"] for c in schema.get_columns(model.__tablename__)} == set(model.__table__.columns.keys())
    with database.session() as db:
        assert db.get(Project, pid).settings["github_mode"] == "auto"
        assert db.scalar(select(AutomationMandate)) is None
        assert db.scalar(select(CmsPublicationConsent)) is None
        from backend import cms_consent
        proof = cms_consent.capture(db, project=db.get(Project, pid), actor=db.get(User, uid),
            operation="mode_activation", content_digest="a" * 64, targets={"mode": "github_merge", "repo": "fixture/site"})
        db.flush()
        grant = AutomationMandate(project_id=pid, mode="github_merge", scope_key="a" * 64, config={}, actor_id=uid, receipt_id=proof.id)
        db.add(grant); db.flush()
        run = AutomationRun(project_id=pid, mode="github_merge", mandate_key=grant.id, request_key="b" * 64)
        db.add(run); db.commit()
        receipt_id, grant_id, run_id = proof.id, grant.id, run.id
    migrate("downgrade", "20261119_0034", success=False)
    with database.session() as db:
        assert db.get(AutomationMandate, grant_id) and db.get(AutomationRun, run_id)
        db.delete(db.get(Project, pid)); db.commit(); db.expire_all()
        assert db.get(AutomationMandate, grant_id) is None and db.get(AutomationRun, run_id) is None
        assert db.get(CmsPublicationConsent, receipt_id).project_id is None
        db.delete(db.get(User, uid)); db.commit(); db.expire_all()
        assert db.get(CmsPublicationConsent, receipt_id).actor_id is None
        assert db.get(CmsPublicationConsent, receipt_id).actor_email == "mode-lab@example.invalid"
    migrate("downgrade", "20261119_0034")
    assert "automation_runs" not in inspect(database.engine).get_table_names()
    migrate("upgrade", "head")
    database.engine.dispose()
    if hasattr(database, "correction_engine"):
        database.correction_engine.dispose()
