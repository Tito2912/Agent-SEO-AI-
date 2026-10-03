"""Opt-in integration tests; only an explicitly named, loopback test database is allowed."""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import sessionmaker

from backend import correction_guard as guard
from backend.db import Database


URL = os.environ.get("SEO_TEST_POSTGRES_URL", "")
pytestmark = pytest.mark.skipif(not URL, reason="requires an isolated SEO_TEST_POSTGRES_URL")


@pytest.fixture
def database(monkeypatch, tmp_path):
    url = make_url(URL)
    assert url.get_backend_name() == "postgresql"
    assert url.host in {"localhost", "127.0.0.1", "::1"}
    assert (url.database or "").startswith("seo_corrector_test")
    monkeypatch.setenv("DATABASE_URL", URL)
    db = Database(data_dir=tmp_path)
    db.engine.dispose()
    db.engine = create_engine(URL, pool_size=1, max_overflow=0, pool_timeout=0.3,
                              pool_pre_ping=True, connect_args={"connect_timeout": 3})
    db.SessionLocal = sessionmaker(bind=db.engine, expire_on_commit=False)
    yield db
    db.engine.dispose()
    if getattr(db, "correction_engine", db.engine) is not db.engine:
        db.correction_engine.dispose()


def observer():
    url = make_url(URL)
    return psycopg.connect(host=url.host, port=url.port, user=url.username, password=url.password,
                           dbname=url.database, connect_timeout=3, autocommit=True)


def lock_owner(connection, account):
    digest = hashlib.sha256(("seo-correction:" + account).encode()).digest()
    unsigned = int.from_bytes(digest[:8], "big")
    row = connection.execute(
        "SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted "
        "AND classid::bigint=%s AND objid::bigint=%s AND objsubid=1",
        (unsigned >> 32, unsigned & 0xFFFFFFFF),
    ).fetchone()
    assert row, "the correction never acquired its advisory lock"
    return row[0]


def child(account, *, crash=False):
    code = (
        "import os,sys,tempfile; from pathlib import Path; from backend.db import Database; "
        "from backend.correction_guard import correction_lock,CorrectionBusy; "
        "d=Database(data_dir=Path(tempfile.mkdtemp(prefix='seo-pg-child-')));\n"
        "try:\n"
        " with correction_lock(d,sys.argv[1]):\n"
        "  if sys.argv[2]=='crash': os._exit(23)\n"
        "except CorrectionBusy: sys.exit(7)\n"
    )
    return subprocess.run([sys.executable, "-c", code, account, "crash" if crash else "normal"],
                          env={**os.environ, "DATABASE_URL": URL}, capture_output=True,
                          text=True, timeout=15, cwd=Path(__file__).resolve().parents[1])


def test_a_correction_does_not_starve_the_application_pool(database):
    with guard.correction_lock(database, uuid.uuid4().hex):
        with database.engine.connect() as connection:
            assert connection.scalar(text("SELECT 42")) == 42


def test_a_terminated_lock_session_is_not_reported_as_success(database):
    account = uuid.uuid4().hex
    with observer() as connection, pytest.raises(guard.CorrectionStoreUnavailable):
        with guard.correction_lock(database, account):
            pid = lock_owner(connection, account)
            assert connection.execute("SELECT pg_terminate_backend(%s)", (pid,)).fetchone()[0]


def test_another_process_cannot_enter_the_same_payer_and_can_enter_after_release(database):
    account = uuid.uuid4().hex
    with guard.correction_lock(database, account):
        refused = child(account)
        assert refused.returncode == 7, refused.stderr
        other = child(uuid.uuid4().hex)
        assert other.returncode == 0, other.stderr
    allowed = child(account)
    assert allowed.returncode == 0, allowed.stderr


def test_reentrant_confirmation_keeps_excluding_other_processes(database):
    account = uuid.uuid4().hex
    with guard.correction_lock(database, account), guard.correction_lock(database, account):
        assert child(account).returncode == 7
    assert child(account).returncode == 0


def test_a_body_exception_releases_the_lock(database):
    account = uuid.uuid4().hex
    with pytest.raises(RuntimeError):
        with guard.correction_lock(database, account):
            raise RuntimeError("provider unavailable")
    assert child(account).returncode == 0


def test_a_worker_crash_releases_the_lock(database):
    account = uuid.uuid4().hex
    crashed = child(account, crash=True)
    assert crashed.returncode == 23, crashed.stderr
    assert child(account).returncode == 0


def test_the_lock_does_not_leave_a_long_running_transaction(database):
    account = uuid.uuid4().hex
    with observer() as connection, guard.correction_lock(database, account):
        pid = lock_owner(connection, account)
        state, xact_start = connection.execute(
            "SELECT state,xact_start FROM pg_stat_activity WHERE pid=%s", (pid,),
        ).fetchone()
        assert state == "idle" and xact_start is None


def test_four_payers_can_read_quotas_without_reserving_the_application_pool(database):
    entered = threading.Barrier(4)
    release = threading.Event()
    def operation():
        with guard.correction_lock(database, uuid.uuid4().hex):
            entered.wait(timeout=5)
            with database.engine.connect() as connection:
                assert connection.scalar(text("SELECT 1")) == 1
            assert release.wait(5)
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(operation) for _ in range(4)]
        try:
            with observer() as connection:
                for _ in range(100):
                    count = connection.execute(
                        "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND granted",
                    ).fetchone()[0]
                    if count == 4:
                        break
                    release.wait(0.02)
                assert count == 4
            with database.engine.connect() as connection:
                assert connection.scalar(text("SELECT 2")) == 2
            with pytest.raises(guard.CorrectionStoreUnavailable):
                with guard.correction_lock(database, uuid.uuid4().hex):
                    pytest.fail("the bounded correction pool accepted a fifth operation")
        finally:
            release.set()
        for future in futures:
            future.result(timeout=10)


def test_unlock_does_not_leak_the_lock_when_a_connection_returns_to_its_pool(database):
    account = uuid.uuid4().hex
    for _ in range(3):
        with guard.correction_lock(database, account):
            assert child(account).returncode == 7
        assert child(account).returncode == 0
