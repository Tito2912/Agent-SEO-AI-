"""The first CMS publication migration is additive and preserves existing customer work."""
import os
from pathlib import Path
import subprocess
import sys

import pytest
from sqlalchemy import inspect, select

from backend.db import Database
from backend.models import AgentActionChoice, AgentPageEvent, CmsMediaTransfer, CmsPublication, CmsPublicationConsent, IssueTask, Project, User


@pytest.mark.parametrize("precreated", [False, True], ids=["migration-first", "models-first"])
def test_upgrade_rollback_and_reupgrade_keep_existing_project_and_article(tmp_path, monkeypatch, precreated):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + str(tmp_path / "multicms.sqlite"))
    database = Database(data_dir=tmp_path / "data")
    web = Path(__file__).resolve().parents[1]
    def migrate(direction, revision, *, success=True):
        result = subprocess.run([sys.executable, "-m", "alembic", "-c", str(web / "alembic.ini"), direction, revision],
                cwd=web, env=os.environ.copy(), capture_output=True, text=True, encoding="utf-8", timeout=45)
        if success:
            assert result.returncode == 0, result.stderr
        else:
            assert result.returncode != 0 and "Descente refusee" in result.stderr
    if precreated:
        database.create_tables()
    migrate("upgrade", "20261115_0032")
    with database.session() as db:
        user = User(email="multicms-migration@example.invalid", password_hash="fictional")
        db.add(user); db.flush()
        project = Project(owner_user_id=user.id, slug="preserved", base_url="https://atelier.example/",
                          site_name="Atelier", settings={"preserved": True})
        db.add(project); db.flush()
        task = IssueTask(project_id=project.id, user_id=user.id, issue_key="ai_content_article",
                         url="https://atelier.example/article", note="Article existant")
        db.add(task); db.commit()
        uid, pid, tid = user.id, project.id, task.id
    migrate("upgrade", "head")
    migrate("upgrade", "head")
    schema = inspect(database.engine)
    for model in (AgentActionChoice, AgentPageEvent, CmsMediaTransfer, CmsPublication, CmsPublicationConsent):
        assert schema.has_table(model.__tablename__)
        assert set(model.__table__.columns.keys()) == {c["name"] for c in schema.get_columns(model.__tablename__)}
    with database.session() as db:
        db.add(AgentActionChoice(project_id=pid, action_key="a" * 32, state="dismissed", created_by=uid))
        db.add(CmsPublication(project_id=pid, task_id=tid, intent_key="b" * 64, platform="ghost", language="fr", data={}))
        db.add(CmsMediaTransfer(project_id=pid, task_id=tid, media_key="c" * 64, platform="ghost",
                               source_name="velo.webp", state="ready", remote_id="https://atelier.example/" + "a" * 300,
                               attempt_id="current-attempt"))
        db.add(AgentPageEvent(project_id=pid, event_key="comparison", page_url="https://atelier.example/article", kind="search_comparison", data={}))
        db.commit()
    migrate("downgrade", "20261119_0035", success=False)
    with database.session() as db:
        transfer = db.scalar(select(CmsMediaTransfer))
        assert transfer.attempt_id == "current-attempt"
        transfer.attempt_id = None
        db.commit()
    migrate("downgrade", "20261115_0032")
    assert not inspect(database.engine).has_table("cms_publications")
    with database.session() as db:
        assert db.get(User, uid).email == "multicms-migration@example.invalid"
        assert db.get(Project, pid).settings == {"preserved": True}
        assert db.get(IssueTask, tid).note == "Article existant"
    migrate("upgrade", "head")
    database.engine.dispose()
