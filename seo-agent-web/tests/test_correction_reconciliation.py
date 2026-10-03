from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest
from sqlalchemy import delete
from sqlalchemy.engine import make_url

from backend import billing, correction_journal as journal, correction_reconciliation as doctor
from backend.correction_guard import CorrectionBusy, CorrectionStoreUnavailable, correction_lock
from backend.db import Database
from backend.models import CorrectionOperation, Project, User

BASE = "a" * 40
HEAD = "b" * 40


@pytest.fixture(params=["sqlite", "postgres"])
def pending(request, tmp_path, monkeypatch):
    url = "sqlite:///" + (tmp_path / "doctor.db").as_posix()
    if request.param == "postgres":
        url = os.environ.get("SEO_TEST_POSTGRES_URL", "")
        if not url:
            pytest.skip("requires isolated SEO_TEST_POSTGRES_URL")
        parsed = make_url(url)
        assert parsed.get_backend_name() == "postgresql"
        assert parsed.host in {"localhost", "127.0.0.1", "::1"}
        assert parsed.database.startswith("seo_corrector_test")
    monkeypatch.setenv("DATABASE_URL", url)
    db = Database(data_dir=tmp_path)
    db.create_tables()
    tag = uuid.uuid4().hex
    with db.session() as session:
        user = User(email=tag + "@example.invalid", password_hash="test", is_admin=False)
        session.add(user)
        session.commit()
        project = Project(owner_user_id=user.id, slug=tag, site_name="fixture.test", base_url="https://fixture.test/")
        session.add(project)
        session.commit()
        payer, project_id = user.id, project.id
    key = "doctor-" + tag
    with correction_lock(db, payer), journal.operation(db, payer=payer, project=project_id, action="fix", key=key) as op:
        branch = journal.branch("seo-fix/title", owner="client", repo="fixture", base="main", base_sha=BASE)
        op.save(state="blocked")
        operation_id = op.row.id
    fixture = SimpleNamespace(db=db, payer=payer, project=project_id, branch=branch, key=key,
                              operation_id=operation_id, url=url, tmp_path=tmp_path)
    yield fixture
    with db.session() as session:
        session.execute(delete(User).where(User.id == payer))
        session.commit()
    db.engine.dispose()
    if hasattr(db, "correction_engine"):
        db.correction_engine.dispose()


class Reader:
    def __init__(self, fixture, *, head=BASE, pulls=None):
        self.fixture = fixture
        self.head = head
        self.pulls = [] if pulls is None else pulls
        self.calls = []

    def get(self, path, **kwargs):
        self.calls.append((path, kwargs))
        if path.endswith("/pulls"):
            assert kwargs["params"] == {"head": "client:" + self.fixture.branch, "state": "all",
                                        "per_page": 100, "page": 1}
            return self.pulls
        if "/git/ref/" in path:
            assert kwargs["missing"] is True
            return None if self.head is None else {"ref": "refs/heads/" + self.fixture.branch,
                                                   "object": {"type": "commit", "sha": self.head}}
        if "/git/commits/" in path:
            assert path.endswith("/" + BASE)
            return {"sha": BASE}
        assert path == "/repos/client/fixture"
        return {"full_name": "client/fixture"}

    def factory(self, payer):
        assert payer == self.fixture.payer
        return self


def run(fixture, reader, **kwargs):
    return doctor.reconcile(fixture.db, fixture.operation_id, reader.factory,
                            **({"operator": "test-operator", "reason": "synthetic interruption", **kwargs}))


def stored(fixture):
    with fixture.db.session() as session:
        return session.get(CorrectionOperation, fixture.operation_id)


def change(fixture, **data):
    with correction_lock(fixture.db, fixture.payer):
        journal.Operation(fixture.db, stored(fixture)).save(**data)


@pytest.mark.parametrize("head", [None, BASE])
def test_confirmed_absent_or_unchanged_branch_can_release_a_request_with_audit(pending, head):
    reader = Reader(pending, head=head)
    before = stored(pending).data
    result = run(pending, reader)
    assert result["decision"] == "can_abandon"
    assert stored(pending).data == before and stored(pending).state == "blocked"
    released = run(pending, reader, abandon=True)
    row = stored(pending)
    assert released["released"] is True and row.state == "abandoned" and row.request_key is None
    assert row.charged is False and journal.pending(pending.db, payer=pending.payer) is None
    audit = row.data["reconciliation"]
    assert audit["operator"] == "test-operator" and audit["reason"] == "synthetic interruption"
    assert audit["evidence"]["head_sha"] == head
    assert audit["remote_branch_retained"] is (head is not None)


def test_modified_branch_requires_explicit_retention_and_is_never_deleted(pending):
    reader = Reader(pending, head=HEAD)
    assert run(pending, reader)["decision"] == "retain_branch_required"
    with pytest.raises(doctor.ReconciliationRefused, match="unpublished_changes"):
        run(pending, reader, abandon=True)
    assert stored(pending).request_key == pending.key
    result = run(pending, reader, abandon=True, retain_branch=True)
    assert result["released"] is True and result["head_sha"] == HEAD
    assert stored(pending).data["reconciliation"]["remote_branch_retained"] is True


@pytest.mark.parametrize("pulls", [[{"state": "open"}], [{"state": "closed"}], [{"state": "closed", "merged_at": "now"}], [{}], [{}, {}]])
def test_any_existing_pr_even_closed_merged_or_on_another_base_refuses_release(pending, pulls):
    reader = Reader(pending, pulls=pulls)
    with pytest.raises(doctor.ReconciliationRefused, match="pull_request_exists"):
        run(pending, reader, abandon=True, retain_branch=True)
    assert stored(pending).state == "blocked" and stored(pending).request_key == pending.key
    assert len(reader.calls) == 2


@pytest.mark.parametrize("protected", ["charged", "preview", "pr"])
def test_existing_paid_or_saved_results_are_reserved_for_recovery(pending, protected):
    if protected == "charged":
        with correction_lock(pending.db, pending.payer):
            journal.Operation(pending.db, stored(pending)).charge(lambda session: billing.usage_add(
                session, user_id=pending.payer, metric="ai_corrections_month", amount=1, commit=False))
    else:
        change(pending, **{protected: {"private": "saved-content"}})
    reader = Reader(pending)
    with pytest.raises(doctor.ReconciliationRefused, match="saved_result_requires_recovery"):
        run(pending, reader, abandon=True, retain_branch=True)
    assert reader.calls == [] and stored(pending).state == "blocked"
    with pending.db.session() as session:
        assert billing.usage_sum(session, user_id=pending.payer, metric="ai_corrections_month") == (protected == "charged")
    assert "saved-content" not in json.dumps(doctor.list_pending(pending.db, payer=pending.payer))


@pytest.mark.parametrize("data", [
    {"repository": None}, {"branch": "main"}, {"branch": "seo-fix/not-this-operation"},
    {"repository": {"owner": "client", "repo": "fixture", "base": "main", "base_sha": "invalid"}},
    {"pr_intent": {"owner": "client", "repo": "other", "base": "main"}},
])
def test_legacy_or_inconsistent_repository_evidence_cannot_release_work(pending, data):
    change(pending, **data)
    reader = Reader(pending)
    with pytest.raises(doctor.ReconciliationRefused):
        run(pending, reader, abandon=True, retain_branch=True)
    assert reader.calls == [] and stored(pending).request_key == pending.key


@pytest.mark.parametrize("bad", ["wrong_repo", "malformed_pulls", "malformed_ref", "wrong_ref", "wrong_commit", "contents_denied", "transport"])
def test_unknown_remote_outcomes_cannot_release_work(pending, bad):
    reader = Reader(pending)
    get = reader.get
    def uncertain(path, **kwargs):
        if bad == "transport":
            raise OSError("private upstream detail")
        value = get(path, **kwargs)
        if bad == "wrong_repo" and path == "/repos/client/fixture":
            return {"full_name": "client/other"}
        if bad == "malformed_pulls" and path.endswith("/pulls"):
            return {}
        if "/git/commits/" in path:
            if bad == "wrong_commit":
                return {"sha": HEAD}
            if bad == "contents_denied":
                raise CorrectionStoreUnavailable()
        if "/git/ref/" in path:
            if bad == "malformed_ref":
                return {"object": {"sha": BASE}}
            if bad == "wrong_ref":
                value["ref"] = "refs/heads/main"
        return value
    reader.get = uncertain
    with pytest.raises(CorrectionStoreUnavailable):
        run(pending, reader, abandon=True, retain_branch=True)
    assert stored(pending).state == "blocked" and stored(pending).request_key == pending.key


def test_a_provider_crash_before_any_write_can_be_abandoned_without_github(pending):
    change(pending, writes_started=False, branch=None, repository=None)
    reader = Reader(pending)
    assert run(pending, reader, abandon=True)["released"] is True
    assert reader.calls == []
    with pytest.raises(doctor.ReconciliationRefused, match="operation_not_pending"):
        run(pending, reader, abandon=True)


def test_operator_action_must_wait_until_the_payer_has_no_active_worker(pending):
    entered, release = threading.Event(), threading.Event()
    def worker():
        with correction_lock(pending.db, pending.payer):
            entered.set()
            assert release.wait(10)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(worker)
        try:
            assert entered.wait(5)
            reader = Reader(pending)
            with pytest.raises(CorrectionBusy):
                run(pending, reader, abandon=True)
            assert reader.calls == []
        finally:
            release.set()
        future.result(timeout=10)
    assert run(pending, Reader(pending), abandon=True)["released"] is True


def test_a_database_commit_failure_does_not_release_the_key(pending, monkeypatch):
    save = journal.Operation.save
    def refuse(self, **kwargs):
        raise CorrectionStoreUnavailable()
    monkeypatch.setattr(journal.Operation, "save", refuse)
    with pytest.raises(CorrectionStoreUnavailable):
        run(pending, Reader(pending), abandon=True)
    assert stored(pending).state == "blocked" and stored(pending).request_key == pending.key
    monkeypatch.setattr(journal.Operation, "save", save)


def test_inspection_evidence_is_never_reused_after_a_pr_appears(pending):
    reader = Reader(pending)
    assert run(pending, reader)["decision"] == "can_abandon"
    reader.pulls = [{"state": "open"}]
    with pytest.raises(doctor.ReconciliationRefused, match="pull_request_exists"):
        run(pending, reader, abandon=True)
    assert stored(pending).state == "blocked"


def test_releasing_one_operation_keeps_other_pending_operations_blocking_the_account(pending):
    with correction_lock(pending.db, pending.payer), journal.operation(
            pending.db, payer=pending.payer, project=pending.project, action="preview", key="second-" + pending.key) as op:
        second_id = op.row.id
    assert run(pending, Reader(pending), abandon=True)["released"] is True
    assert journal.pending(pending.db, payer=pending.payer).id == second_id


def test_lease_loss_after_github_reads_prevents_release_on_real_postgres(pending):
    if pending.db.engine.dialect.name != "postgresql":
        pytest.skip("requires real PostgreSQL lease termination")
    reader = Reader(pending)
    get = reader.get
    def terminate(path, **kwargs):
        value = get(path, **kwargs)
        if "/git/ref/" in path:
            url = make_url(pending.url)
            with psycopg.connect(host=url.host, port=url.port, user=url.username, dbname=url.database,
                                 password=url.password, autocommit=True) as connection:
                assert connection.execute("SELECT pg_terminate_backend(pid) FROM pg_locks "
                                          "WHERE locktype='advisory' AND granted").fetchone()[0]
        return value
    reader.get = terminate
    with pytest.raises(CorrectionStoreUnavailable):
        run(pending, reader, abandon=True)
    assert stored(pending).state == "blocked" and stored(pending).request_key == pending.key


@pytest.mark.parametrize("status,missing", [(401, True), (403, True), (404, False), (429, True), (500, True), (302, True)])
def test_remote_failures_are_never_inferred_as_an_absent_branch(monkeypatch, status, missing):
    monkeypatch.setattr(doctor.requests, "get", lambda *a, **kw: SimpleNamespace(status_code=status))
    with pytest.raises(CorrectionStoreUnavailable):
        doctor.GitHubReader("test-token").get("/repos/client/fixture", missing=missing)


def test_an_explicit_branch_404_is_supported_without_printing_credentials(monkeypatch):
    def get(url, **kwargs):
        assert url.startswith("https://api.github.com/") and kwargs["allow_redirects"] is False
        return SimpleNamespace(status_code=404)
    monkeypatch.setattr(doctor.requests, "get", get)
    assert doctor.GitHubReader("test-token").get("/repos/client/fixture/git/ref/heads/fix", missing=True) is None


def test_the_cli_lists_without_exposing_the_preview_and_can_abandon_no_effect_work(pending):
    change(pending, writes_started=False, branch=None, repository=None)
    root = Path(__file__).resolve().parents[1]
    env = {**os.environ, "DATABASE_URL": pending.url, "SEO_AGENT_DATA_DIR": str(pending.tmp_path),
           "SEO_AGENT_RUNS_DIR": str(pending.tmp_path / "runs"), "SEO_AGENT_DISABLE_WORKER": "true"}
    def cli(*args):
        result = subprocess.run([sys.executable, "ops/correction_doctor.py", *args], cwd=root,
                                env=env, capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result.stdout + result.stderr
        return json.loads(result.stdout)
    listed = cli("list", "--payer", pending.payer)
    assert len(listed) == 1 and listed[0]["operation_id"] == pending.operation_id
    inspected = cli("inspect", pending.operation_id)
    assert inspected["decision"] == "can_abandon" and stored(pending).state == "blocked"
    assert cli("abandon", pending.operation_id, "--operator", "test", "--reason", "synthetic crash")["released"] is True


def test_abandonment_requires_an_operator_and_reason_before_any_database_access():
    with pytest.raises(doctor.ReconciliationRefused, match="operator_and_reason_required"):
        doctor.reconcile(None, "unused", None, abandon=True)
