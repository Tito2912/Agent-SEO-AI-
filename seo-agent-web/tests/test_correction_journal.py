from __future__ import annotations

import contextvars
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from sqlalchemy import create_engine, event, inspect, select

from backend import billing, correction_journal as journal
from backend.correction_guard import CorrectionStoreUnavailable, correction_lock
from backend.db import Database
from backend.models import CorrectionOperation, Project, User


@pytest.fixture
def database(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + (tmp_path / "ledger.db").as_posix())
    db = Database(data_dir=tmp_path)
    db.create_tables()
    with db.session() as session:
        user = User(email="receipt@example.invalid", password_hash="test", is_admin=False)
        session.add(user)
        session.commit()
        project = Project(owner_user_id=user.id, slug="receipts", site_name="fixture.test", base_url="https://fixture.test/")
        session.add(project)
        session.commit()
        payer, project_id = user.id, project.id
    yield db, payer, project_id
    db.engine.dispose()


def begin(db, payer, project):
    return journal.operation(db, payer=payer, project=project, action="preview", key="fixture-key")


def charge(db, payer):
    billing.usage_add(db, user_id=payer, metric="ai_corrections_month", amount=1, commit=False)


def usage(db, payer):
    with db.session() as session:
        return billing.usage_sum(session, user_id=payer, metric="ai_corrections_month")


def test_the_charge_and_receipt_commit_together_and_cannot_be_repeated(database):
    db, payer, project = database
    with correction_lock(db, payer), begin(db, payer, project) as op:
        op.charge(lambda session: charge(session, payer))
        op.charge(lambda session: pytest.fail("duplicate ledger callback"))
    assert usage(db, payer) == 1
    assert journal.find(db, key="fixture-key").charged is True


def test_a_failed_transaction_rolls_back_both_the_charge_and_receipt(database):
    db, payer, project = database
    def interrupted(session):
        charge(session, payer)
        raise OSError("before commit")
    with correction_lock(db, payer), begin(db, payer, project) as op:
        with pytest.raises(CorrectionStoreUnavailable):
            op.charge(interrupted)
        assert usage(db, payer) == 0
        assert journal.find(db, key="fixture-key").charged is False
        op.charge(lambda session: charge(session, payer))
    assert usage(db, payer) == 1


def test_a_post_commit_acknowledgement_failure_does_not_repeat_the_charge(database):
    db, payer, project = database
    def interrupted(session):
        charge(session, payer)
        def lost_acknowledgement(_session):
            raise OSError("after commit")
        event.listen(session, "after_commit", lost_acknowledgement, once=True)
    with correction_lock(db, payer), begin(db, payer, project) as op:
        with pytest.raises(CorrectionStoreUnavailable):
            op.charge(interrupted)
        persisted = journal.find(db, key="fixture-key")
        assert persisted.charged is True
        journal.Operation(db, persisted).charge(lambda session: pytest.fail("charge was already committed"))
    assert usage(db, payer) == 1


def test_a_copied_thread_context_does_not_inherit_write_authority(database):
    db, payer, project = database
    with correction_lock(db, payer), begin(db, payer, project):
        assert journal.current()
        context = contextvars.copy_context()
        with ThreadPoolExecutor(max_workers=1) as executor:
            assert executor.submit(context.run, journal.current).result() is None
    assert journal.current() is None


def test_pr_metadata_survives_a_minimal_response_receipt(database):
    db, payer, project = database
    with correction_lock(db, payer), begin(db, payer, project) as op:
        journal.pr_intent(owner="client", repo="fixture", base="main", tasks=[], billable=0, motif="test")
        receipt = {"number": 42, "html_url": "https://github.com/client/fixture/pull/42"}
        journal.pr_received({**receipt, "head": {"sha": "a" * 40}, "node_id": "PR_test"})
        journal.pr_received(receipt)
        assert op.row.data["pr"]["head"]["sha"] == "a" * 40
        assert op.row.data["pr"]["node_id"] == "PR_test"


def test_request_identity_is_order_independent_but_never_crosses_payers_or_projects():
    key = journal.request_key("payer", "site", "preview", {"url": "https://site.test/", "crawl": "A"})
    assert key == journal.request_key("payer", "site", "preview", {"crawl": "A", "url": "https://site.test/"})
    assert key != journal.request_key("other", "site", "preview", {"url": "https://site.test/", "crawl": "A"})
    assert key != journal.request_key("payer", "other", "preview", {"url": "https://site.test/", "crawl": "A"})


@pytest.mark.parametrize("precreated", [False, True])
def test_the_migration_upgrades_an_existing_database_and_handles_create_all(tmp_path, monkeypatch, precreated):
    url = "sqlite:///" + (tmp_path / "migration.db").as_posix()
    monkeypatch.setenv("DATABASE_URL", url)
    root = Path(__file__).resolve().parents[1]
    def upgrade(revision):
        result = subprocess.run([sys.executable, "-m", "alembic", "-c", str(root / "alembic.ini"),
                                 "upgrade", revision], cwd=root, capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result.stdout + result.stderr
    upgrade("20260919_0012")
    engine = create_engine(url)
    try:
        assert "correction_operations" not in inspect(engine).get_table_names()
        if precreated:
            CorrectionOperation.__table__.create(engine)
        upgrade("head")
        upgrade("head")
        columns = {column["name"] for column in inspect(engine).get_columns("correction_operations")}
        assert columns == {column.name for column in CorrectionOperation.__table__.columns}
        with engine.connect() as connection:
            assert connection.scalar(select(CorrectionOperation.id)) is None
    finally:
        engine.dispose()
