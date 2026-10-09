from __future__ import annotations

import contextvars
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy.engine import make_url

from backend import correction_guard as guard


def database(tmp_path):
    return SimpleNamespace(engine=SimpleNamespace(url=make_url("sqlite:///" + (tmp_path / "test.db").as_posix()),
                                                  dialect=SimpleNamespace(name="sqlite")), data_dir=tmp_path)


def child(db, account, *, crash=False):
    code = (
        "import os,sys; from types import SimpleNamespace; from pathlib import Path; "
        "from sqlalchemy.engine import make_url; from backend.correction_guard import correction_lock, CorrectionBusy; "
        "d=SimpleNamespace(engine=SimpleNamespace(url=make_url(sys.argv[1]),dialect=SimpleNamespace(name='sqlite')),data_dir=Path('.'));\n"
        "try:\n"
        " with correction_lock(d,sys.argv[2]):\n"
        "  if sys.argv[3]=='crash': os._exit(23)\n"
        "except CorrectionBusy: sys.exit(7)\n"
    )
    return subprocess.run([sys.executable, "-c", code, str(db.engine.url), account, "crash" if crash else "normal"],
                          cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True, timeout=20)


def test_os_lock_excludes_another_process_and_releases_on_exit(tmp_path):
    db = database(tmp_path)
    with guard.correction_lock(db, "account"):
        other = child(db, "account")
        assert other.returncode == 7, other.stderr
    result = child(db, "account")
    assert result.returncode == 0, result.stderr


def test_another_account_is_not_blocked(tmp_path):
    db = database(tmp_path)
    with guard.correction_lock(db, "first-account"):
        other = child(db, "second-account")
        assert other.returncode == 0, other.stderr


def test_nested_auto_confirmation_is_reentrant(tmp_path):
    db = database(tmp_path)
    with guard.correction_lock(db, "account"):
        with guard.correction_lock(db, "account"):
            assert child(db, "account").returncode == 7


def test_exception_releases_the_lock(tmp_path):
    db = database(tmp_path)
    with pytest.raises(RuntimeError):
        with guard.correction_lock(db, "account"):
            raise RuntimeError("model unavailable")
    assert child(db, "account").returncode == 0


def test_a_worker_crash_does_not_leave_an_account_locked(tmp_path):
    db = database(tmp_path)
    crashed = child(db, "account", crash=True)
    assert crashed.returncode == 23, crashed.stderr
    assert child(db, "account").returncode == 0


def test_a_copied_context_cannot_make_another_thread_reentrant(tmp_path):
    db = database(tmp_path)
    def attempt():
        with pytest.raises(guard.CorrectionBusy):
            with guard.correction_lock(db, "account"):
                pytest.fail("another thread skipped the OS lock")
    with guard.correction_lock(db, "account"):
        context = contextvars.copy_context()
        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(context.run, attempt).result(timeout=10)


def test_unwritable_storage_refuses_instead_of_running_without_a_lock(tmp_path):
    db = database(tmp_path)
    (tmp_path / ".correction-locks").write_text("not a directory", encoding="ascii")
    with pytest.raises(guard.CorrectionStoreUnavailable):
        with guard.correction_lock(db, "account"):
            pytest.fail("unprotected correction")


@pytest.mark.parametrize("account,dialect", [("", "sqlite"), ("user", "mysql")])
def test_unknown_scope_or_backend_is_refused(tmp_path, account, dialect):
    db = database(tmp_path)
    db.engine.dialect.name = dialect
    with pytest.raises(guard.CorrectionStoreUnavailable):
        with guard.correction_lock(db, account):
            pytest.fail("unprotected correction")


def postgres(*, acquired=True, acquisition_error=False, unlock_error=False, lease_lost=False):
    events = []
    class Connection:
        closed = False
        invalidated = False

        def scalar(self, sql, params=None):
            if "pg_locks" in str(sql):
                events.append("check")
                return not lease_lost
            if str(sql) == "SELECT pg_backend_pid()":
                events.append("pid")
                return 123
            assert isinstance(params["key"], int)
            assert -(2**63) <= params["key"] < 2**63
            if "pg_try" in str(sql):
                events.append("try")
                if acquisition_error:
                    raise OSError("database unavailable")
                return acquired
            events.append("unlock")
            if unlock_error:
                raise OSError("connection lost")
            return True

        def commit(self):
            events.append("commit")

        def invalidate(self):
            self.invalidated = True
            events.append("invalidate")

        def close(self):
            self.closed = True
            events.append("close")

    engine = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"),
                             url=make_url("postgresql://test@localhost/fixture"), connect=Connection)
    return SimpleNamespace(engine=engine), events


def test_postgres_lock_is_session_scoped_without_a_long_transaction():
    db, events = postgres()
    with guard.correction_lock(db, "account"):
        assert events == ["try", "pid", "commit", "check", "commit"]
    assert events == ["try", "pid", "commit", "check", "commit", "check", "commit", "unlock", "commit", "close"]


def test_busy_postgres_lock_never_runs_or_unlocks_someone_elses_session():
    db, events = postgres(acquired=False)
    with pytest.raises(guard.CorrectionBusy):
        with guard.correction_lock(db, "account"):
            pytest.fail("concurrent operation")
    assert events == ["try", "commit", "close"]


def test_postgres_acquisition_failure_does_not_fail_open():
    db, events = postgres(acquisition_error=True)
    with pytest.raises(guard.CorrectionStoreUnavailable):
        with guard.correction_lock(db, "account"):
            pytest.fail("unprotected correction")
    assert events == ["try", "invalidate", "close"]


def test_postgres_unlock_failure_invalidates_before_pool_return():
    db, events = postgres(unlock_error=True)
    with pytest.raises(guard.CorrectionStoreUnavailable):
        with guard.correction_lock(db, "account"):
            pass
    assert events[-3:] == ["unlock", "invalidate", "close"]


def test_body_exception_still_releases_the_postgres_lock():
    db, events = postgres()
    with pytest.raises(RuntimeError):
        with guard.correction_lock(db, "account"):
            raise RuntimeError("GitHub unavailable")
    assert events[-3:] == ["unlock", "commit", "close"]


def test_a_lost_lease_is_never_reacquired_silently():
    db, events = postgres(lease_lost=True)
    with pytest.raises(guard.CorrectionStoreUnavailable):
        with guard.correction_lock(db, "account"):
            pytest.fail("an unowned lease entered the operation")
    assert events.count("try") == 1 and "unlock" not in events
    assert events[-2:] == ["invalidate", "close"]


def test_lease_checks_are_noops_outside_a_correction():
    guard.ensure_active()


@pytest.mark.parametrize("flag", ["closed", "invalidated"])
def test_unusable_connection_never_reconnects_to_unlock(flag):
    db, events = postgres()
    connection = db.engine.connect()
    db.engine.connect = lambda: connection
    with pytest.raises(guard.CorrectionStoreUnavailable):
        with guard.correction_lock(db, "account"):
            setattr(connection, flag, True)
            guard.ensure_active()
    assert events.count("check") == 1
    assert "unlock" not in events
    assert events[-1] == "close"
