"""Real upgrades from the old schema preserve evidence and expose honest legacy provenance."""
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys

import pytest
from sqlalchemy import MetaData, Table, func, inspect, select

from backend import cms_consent
from backend.db import Database
from backend.models import CmsMediaTransfer, CmsPublication, CmsPublicationConsent, IssueTask, Project, User


@pytest.mark.parametrize("precreated", [False, True], ids=["old-schema", "models-first"])
@pytest.mark.parametrize("backend", ["sqlite", "postgresql"])
def test_existing_receipts_and_media_survive_upgrade_deletion_and_refused_downgrade(tmp_path, monkeypatch, precreated, backend):
    postgres = os.getenv("NOYARU_TEST_POSTGRES_URL")
    if backend == "postgresql" and not postgres:
        pytest.skip("Explicit disposable PostgreSQL laboratory URL required")
    url = postgres if backend == "postgresql" else "sqlite:///" + str(tmp_path / "consents.sqlite")
    if backend == "postgresql":
        from sqlalchemy.engine import make_url
        parsed = make_url(url)
        assert parsed.host == "127.0.0.1" and parsed.database == "noyaru_consents" and parsed.port == 35434
    monkeypatch.setenv("DATABASE_URL", url)
    database = Database(data_dir=tmp_path / "data")
    web = Path(__file__).resolve().parents[1]
    def migrate(direction, revision, success=True):
        result = subprocess.run([sys.executable, "-m", "alembic", "-c", str(web / "alembic.ini"), direction, revision],
            cwd=web, env=os.environ.copy(), capture_output=True, text=True, encoding="utf-8", timeout=90)
        if success:
            assert result.returncode == 0, result.stderr
        else:
            assert result.returncode != 0 and "Descente refusee" in result.stderr
    if backend == "postgresql":
        # The strict URL above is a freshly provisioned local lab only, never a normal DB.
        from sqlalchemy import text
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
    migrate("upgrade", "20261117_0033")
    stamp = datetime(2024, 2, 29, 12, tzinfo=timezone.utc)
    with database.session() as db:
        user = User(email="current-profile@example.invalid", password_hash="fictional")
        db.add(user); db.flush()
        project = Project(owner_user_id=user.id, slug="retention-lab", site_name="Fictional",
                          base_url="https://current-site.example/?token=PRIVATE")
        db.add(project); db.flush()
        task = IssueTask(project_id=project.id, user_id=user.id, issue_key="ai_content_article",
                         url="https://current-site.example/article", note="preserved")
        db.add(task); db.flush()
        publication = CmsPublication(project_id=project.id, task_id=task.id, intent_key="a" * 64,
            platform="ghost", language="fr", state="draft", data={"article": {"html": "PRIVATE-CONTENT"}},
            remote_id="remote-1", remote_url="https://current-site.example/article?token=PRIVATE")
        db.add(publication); db.flush()
        old = Table("cms_publication_consents", MetaData(), autoload_with=db.connection())
        db.execute(old.insert().values(id="existing-approval", project_id=project.id, publication_id=publication.id,
            actor_id=user.id, operation="create", content_digest="b" * 64, approved_at=stamp,
            targets={"platform": "ghost", "slug": "article", "html": "PRIVATE-CONTENT",
                     "destination": {"collection_id": "collection-1", "password": "PRIVATE"}}))
        transfer = CmsMediaTransfer(project_id=project.id, task_id=task.id, media_key="c" * 64,
            platform="ghost", source_name="PRIVATE-FILENAME.webp", approved_by=user.id, consent_digest="d" * 64,
            remote_id="asset-1", remote_url="https://current-site.example/one.webp?token=PRIVATE", created_at=stamp)
        db.add(transfer); db.commit()
        uid, pid, tid, transfer_id = user.id, project.id, task.id, transfer.id
    migrate("upgrade", "head")
    migrate("upgrade", "head")
    schema = inspect(database.engine)
    assert {c["name"] for c in schema.get_columns("cms_publication_consents")} == set(CmsPublicationConsent.__table__.columns.keys())
    fks = schema.get_foreign_keys("cms_publication_consents")
    for col in ("project_id", "publication_id", "owner_user_id", "actor_id"):
        fk, = [fk for fk in fks if fk["constrained_columns"] == [col]]
        assert fk["options"]["ondelete"].upper() == "SET NULL"
    with database.session() as db:
        proofs = list(db.scalars(select(CmsPublicationConsent)))
        assert len(proofs) == 2
        old = db.get(CmsPublicationConsent, "existing-approval")
        assert old.actor_email == "current-profile@example.invalid" and old.snapshot_source == "backfilled"
        assert old.site_url == "https://current-site.example/" and old.owner_user_id == uid
        assert old.content_digest == "b" * 64 and cms_consent.utc(old.approved_at) == stamp
        assert cms_consent.utc(old.expires_at) == datetime(2029, 2, 28, 12, tzinfo=timezone.utc)
        media = db.get(CmsPublicationConsent, cms_consent.media_receipt_id(transfer_id))
        assert media.operation == "media_upload" and media.targets["legacy_digest_kind"] == "intent"
        assert media.content_digest == "d" * 64 and cms_consent.utc(media.approved_at) == stamp
        assert "PRIVATE" not in str([p.targets for p in proofs])
        assert db.get(IssueTask, tid).note == "preserved"
        db.delete(db.get(Project, pid)); db.commit(); db.expire_all()
        assert all(p.project_id is None and p.publication_id is None for p in db.scalars(select(CmsPublicationConsent)))
        db.delete(db.get(User, uid)); db.commit(); db.expire_all()
        assert all(p.actor_id is None and p.owner_user_id is None for p in db.scalars(select(CmsPublicationConsent)))
    migrate("downgrade", "20261117_0033", success=False)
    with database.session() as db:
        assert db.scalar(select(func.count()).select_from(CmsPublicationConsent)) == 2
        assert cms_consent.purger(db, datetime(2029, 2, 28, 12, tzinfo=timezone.utc)) == 2
    migrate("downgrade", "20261117_0033")
    schema = inspect(database.engine)
    assert "actor_email" not in {c["name"] for c in schema.get_columns("cms_publication_consents")}
    migrate("upgrade", "head")
    database.engine.dispose()
    if hasattr(database, "correction_engine"):
        database.correction_engine.dispose()
