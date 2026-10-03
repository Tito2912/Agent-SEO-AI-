"""Serialize quota-reading correction operations across workers, without a long DB transaction."""

from __future__ import annotations

import contextvars
import errno
import hashlib
import os
import threading
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import text


class CorrectionBusy(Exception):
    pass


class CorrectionStoreUnavailable(Exception):
    pass


_HELD = contextvars.ContextVar("held_correction_accounts", default=frozenset())
_CHECKS = contextvars.ContextVar("correction_lease_checks", default=())


def ensure_active():
    """Fence writes when the current worker's session no longer owns its lease."""
    for scope, check in _CHECKS.get():
        if scope[:2] == (os.getpid(), threading.get_ident()):
            check()


@contextmanager
def _sqlite_lock(database, digest: bytes):
    filename = str(database.engine.url.database or "")
    directory = (Path(filename).resolve().parent if filename and filename != ":memory:"
                 else database.data_dir) / ".correction-locks"
    try:
        directory.mkdir(parents=True, exist_ok=True)
        handle = (directory / (digest.hex() + ".lock")).open("a+b")
    except OSError as exc:
        raise CorrectionStoreUnavailable() from exc
    try:
        try:
            if os.name == "nt":
                import msvcrt

                if os.fstat(handle.fileno()).st_size == 0:
                    handle.write(b"0")
                    handle.flush()
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
                raise CorrectionBusy() from exc
            raise CorrectionStoreUnavailable() from exc
        yield
    finally:
        # Closing the descriptor releases the OS lock, also when a worker exits unexpectedly.
        handle.close()


@contextmanager
def _postgres_lock(database, digest: bytes):
    key = int.from_bytes(digest[:8], "big", signed=True)
    try:
        connection = getattr(database, "correction_engine", database.engine).connect()
    except Exception as exc:
        raise CorrectionStoreUnavailable() from exc
    acquired = False
    completed = False
    lost = False

    def check():
        nonlocal lost
        if lost or connection.closed or connection.invalidated:
            lost = True
            raise CorrectionStoreUnavailable()
        unsigned = int.from_bytes(digest[:8], "big")
        try:
            held = connection.scalar(text(
                "SELECT pg_backend_pid() = :pid AND EXISTS ("
                "SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND granted "
                "AND pid = pg_backend_pid() AND classid::bigint = :high "
                "AND objid::bigint = :low AND objsubid = 1)"
            ), {"pid": owner_pid, "high": unsigned >> 32, "low": unsigned & 0xFFFFFFFF})
            connection.commit()
            if not held:
                raise CorrectionStoreUnavailable()
        except Exception as exc:
            lost = True
            connection.invalidate()
            raise CorrectionStoreUnavailable() from exc

    try:
        try:
            acquired = bool(connection.scalar(text("SELECT pg_try_advisory_lock(:key)"), {"key": key}))
            owner_pid = connection.scalar(text("SELECT pg_backend_pid()")) if acquired else None
            connection.commit()
        except Exception as exc:
            lost = True
            connection.invalidate()
            raise CorrectionStoreUnavailable() from exc
        if not acquired:
            raise CorrectionBusy()
        check()
        yield check
        check()
        completed = True
    finally:
        try:
            if acquired and not lost:
                try:
                    unlocked = connection.scalar(text("SELECT pg_advisory_unlock(:key)"), {"key": key})
                    connection.commit()
                    if not unlocked:
                        lost = True
                        connection.invalidate()
                except Exception:
                    lost = True
                    # Never return a session with a leaked advisory lock to the connection pool.
                    connection.invalidate()
        finally:
            connection.close()
        if completed and lost:
            raise CorrectionStoreUnavailable()


@contextmanager
def correction_lock(database, account: str):
    if not account:
        raise CorrectionStoreUnavailable()
    scope = (os.getpid(), threading.get_ident(), str(database.engine.url), str(account))
    held = _HELD.get()
    if scope in held:
        yield
        return
    digest = hashlib.sha256(("seo-correction:" + str(account)).encode()).digest()
    dialect = database.engine.dialect.name
    manager = (_sqlite_lock if dialect == "sqlite" else _postgres_lock if dialect == "postgresql" else None)
    if manager is None:
        raise CorrectionStoreUnavailable()
    with manager(database, digest) as check:
        token = _HELD.set(held | {scope})
        checks_token = _CHECKS.set(_CHECKS.get() + ((scope, check),) if check else _CHECKS.get())
        try:
            yield
        finally:
            _CHECKS.reset(checks_token)
            _HELD.reset(token)
