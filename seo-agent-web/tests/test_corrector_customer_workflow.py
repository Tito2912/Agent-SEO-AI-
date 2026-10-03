from __future__ import annotations

import base64
import hashlib
import json
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest
from fastapi.testclient import TestClient

from sqlalchemy import delete, select

from backend import app as m, auth, billing, correction_guard, correction_journal, correction_reconciliation
from backend.models import AccountMember, CorrectionOperation, IssueTask, User, Project

URL = "https://fixture.test/"
OLD = '<!doctype html><html><head></head><body><h1>Fixture</h1></body></html>'
NEW = OLD.replace("</head>", "<title>A complete title for this fixture page</title></head>")
PR_IS_OPEN = m._github_pr_is_open
GITHUB_WRITE_HELPERS = {verb: getattr(m, "_github_api_" + verb) for verb in ("post", "put", "delete")}


@pytest.fixture
def customer(monkeypatch):
    m.DB.create_tables()
    tag = uuid.uuid4().hex
    with m.DB.session() as db:
        user = User(email=f"corrections-{tag}@example.com", password_hash="test", is_admin=False)
        db.add(user)
        db.commit()
        project = Project(owner_user_id=str(user.id), slug=f"client-{tag}", site_name="fixture.test",
                          base_url=URL, settings={"github_repo": "client/fixture",
                                                "github_branch": "main", "github_mode": "review"})
        db.add(project)
        db.commit()
        uid, slug = str(user.id), project.slug
    state = {"models": 0, "writes": [], "posts": [], "entered": threading.Event(),
             "release": threading.Event(), "block": False, "mode": "review"}
    monkeypatch.setattr(m, "_plan_correction_cfg", lambda user, **kw: {
        "plan": "pro", "model": "claude-sonnet-4-6", "max_files": 20, "unlimited": False})
    monkeypatch.setattr(billing, "plan_limits", lambda *a, **kw: {"ai_corrections_month": 1})
    monkeypatch.setattr(m, "_effective_user_connection_value", lambda **kw: ("test-token", "user"))
    monkeypatch.setattr(m, "_rate_limit_retry_after", lambda **kw: None)
    monkeypatch.setattr(m, "_project_github_cfg", lambda *a: {
        "repo": "client/fixture", "branch": "main", "mode": state["mode"]})
    monkeypatch.setattr(m, "_github_find_seo_files", lambda *a: [{"path": "index.html", "content": OLD}])
    monkeypatch.setattr(m, "_github_pr_is_open", lambda *a, **kw: True)

    def model(**kw):
        state["models"] += 1
        if state["block"] and state["models"] == 1:
            state["entered"].set()
            assert state["release"].wait(10)
        return {"patched_content": NEW, "pr_title": "fix: fixture title"}

    def get(path, **kw):
        if "/git/ref" in path:
            return {"object": {"sha": "base"}}
        if "/contents/" in path:
            return {"sha": "original", "content": base64.b64encode(OLD.encode()).decode()}
        raise AssertionError(path)

    def post(path, **kw):
        state["posts"].append((path, kw["json_body"]))
        return ({"number": 42, "html_url": "https://github.com/client/fixture/pull/42"}
                if path.endswith("/pulls") else {"ok": True})

    def put(path, **kw):
        state["writes"].append(path)
        return {"commit": {"sha": "commit", "html_url": "https://github.com/client/fixture/commit/a"}}

    monkeypatch.setattr(m, "_openai_generate_file_patch", model)
    monkeypatch.setattr(m, "_github_api_get", get)
    monkeypatch.setattr(m, "_github_api_post", post)
    monkeypatch.setattr(m, "_github_api_put", put)
    client = TestClient(m.app)
    client.cookies.set(auth.SESSION_COOKIE_NAME,
                       auth.make_session_token(user_id=uid, secret=m._safe_env("SEO_AGENT_SECRET_KEY")))
    client.get("/projects")
    state["client"] = client
    path = f"/api/projects/{slug}/issues/missing_title/github-fix"

    def send(**body):
        return client.post(path, json={"url": URL, "crawl_ts": "20261002-090000", **body},
                           headers={m._CSRF_HEADER_NAME: client.cookies.get(m._CSRF_COOKIE_NAME, "")})

    def used():
        with m.DB.session() as db:
            return billing.usage_sum(db, user_id=uid, metric="ai_corrections_month")

    yield state, send, used, uid, slug
    state["release"].set()
    client.close()


def test_last_credit_preview_can_be_confirmed_without_a_second_charge(customer):
    state, send, used, _, _ = customer
    preview = send()
    assert preview.status_code == 200, preview.text
    assert used() == 1
    confirm = send(confirm=True, file_path=preview.json()["file"], patched_content=preview.json()["patched_content"])
    assert confirm.status_code == 200, confirm.text
    assert used() == 1 and state["models"] == 1
    assert state["writes"]
    assert state["posts"][-1][1]["draft"] is True


def test_auto_mode_can_apply_the_patch_written_with_the_last_credit(customer):
    state, send, used, _, _ = customer
    state["mode"] = "auto"
    response = send()
    assert response.status_code == 200, response.text
    assert response.json()["pr_number"] == 42
    assert used() == 1 and state["models"] == 1


@pytest.mark.parametrize("open_pr", [True, False])
def test_confirmed_patch_cannot_open_a_duplicate_pr_unless_the_previous_pr_is_closed(customer, monkeypatch, open_pr):
    state, send, used, _, _ = customer
    preview = send()
    body = {"confirm": True, "file_path": preview.json()["file"],
            "patched_content": preview.json()["patched_content"]}
    assert send(**body).status_code == 200
    before = len(state["writes"]), len(state["posts"])
    monkeypatch.setattr(m, "_github_pr_is_open", lambda *a, **kw: open_pr)
    response = send(**body)
    assert response.status_code == (409 if open_pr else 200), response.text
    if open_pr:
        assert response.json()["duplicate"] is True
        assert response.json()["pr_url"] == "https://github.com/client/fixture/pull/42"
        assert (len(state["writes"]), len(state["posts"])) == before
    assert used() == 1 and state["models"] == 1


def test_an_open_pr_does_not_generate_and_charge_another_preview(customer, monkeypatch):
    state, send, used, _, _ = customer
    monkeypatch.setattr(billing, "plan_limits", lambda *a, **kw: {"ai_corrections_month": 2})
    preview = send()
    assert send(confirm=True, file_path=preview.json()["file"],
                patched_content=preview.json()["patched_content"]).status_code == 200
    response = send()
    assert response.status_code == 409, response.text
    assert used() == 1 and state["models"] == 1


@pytest.mark.parametrize("answer", ["transport_failure", {}, [], {"state": "unknown"}])
def test_an_unknown_pr_state_cannot_charge_or_open_another_pr(customer, monkeypatch, answer):
    state, send, used, _, _ = customer
    monkeypatch.setattr(billing, "plan_limits", lambda *a, **kw: {"ai_corrections_month": 2})
    preview = send()
    assert send(confirm=True, file_path=preview.json()["file"],
                patched_content=preview.json()["patched_content"]).status_code == 200
    before = len(state["writes"]), len(state["posts"])
    monkeypatch.setattr(m, "_github_pr_is_open", PR_IS_OPEN)
    def pr_status(*a, **kw):
        if answer == "transport_failure":
            raise RuntimeError("private upstream rate limit details")
        return answer
    monkeypatch.setattr(m, "_github_api_get", pr_status)
    response = send()
    assert response.status_code == 503, response.text
    assert "private upstream" not in response.text
    assert used() == 1 and state["models"] == 1
    assert (len(state["writes"]), len(state["posts"])) == before


def test_pr_tracking_store_failure_cannot_be_treated_as_no_existing_pr(customer, monkeypatch):
    def unavailable():
        raise OSError("tracking storage unavailable")
    monkeypatch.setattr(m.DB, "session", unavailable)
    with pytest.raises(correction_guard.CorrectionStoreUnavailable):
        m._open_pr_for_issue(project_id="fixture", issue_key="missing_title", url=URL,
                             owner="client", repo_name="fixture", token="test-token", strict=True)


@pytest.mark.parametrize("verb", ["post", "put", "delete"])
def test_github_writes_check_the_current_lease_before_the_network(monkeypatch, verb):
    def lost():
        raise correction_guard.CorrectionStoreUnavailable()
    def unexpected(*a, **kw):
        pytest.fail("a lost lease sent a GitHub write")
    monkeypatch.setattr(correction_guard, "ensure_active", lost)
    monkeypatch.setattr(m.requests, verb, unexpected)
    with pytest.raises(correction_guard.CorrectionStoreUnavailable):
        GITHUB_WRITE_HELPERS[verb]("/repos/client/fixture", token="test-token", json_body={})


def test_quota_charge_checks_the_current_lease_before_the_ledger(monkeypatch):
    def lost():
        raise correction_guard.CorrectionStoreUnavailable()
    def unexpected(*a, **kw):
        pytest.fail("a lost lease changed the ledger")
    monkeypatch.setattr(correction_guard, "ensure_active", lost)
    monkeypatch.setattr(billing, "usage_add", unexpected)
    with pytest.raises(correction_guard.CorrectionStoreUnavailable):
        m._correction_charge(SimpleNamespace(id="payer", is_admin=False), 1)


def terminate_test_lease(account):
    url = m.DB.engine.url
    assert url.host in {"127.0.0.1", "localhost", "::1"}
    assert (url.database or "").startswith("seo_corrector_test")
    digest = hashlib.sha256(("seo-correction:" + account).encode()).digest()
    unsigned = int.from_bytes(digest[:8], "big")
    with psycopg.connect(host=url.host, port=url.port, user=url.username, password=url.password,
                          dbname=url.database, connect_timeout=3, autocommit=True) as connection:
        row = connection.execute(
            "SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted "
            "AND classid::bigint=%s AND objid::bigint=%s AND objsubid=1",
            (unsigned >> 32, unsigned & 0xFFFFFFFF),
        ).fetchone()
        assert row
        assert connection.execute("SELECT pg_terminate_backend(%s)", (row[0],)).fetchone()[0]


@pytest.mark.skipif(m.DB.engine.dialect.name != "postgresql", reason="requires isolated PostgreSQL")
def test_a_provider_result_after_postgres_lease_loss_is_not_billed_or_applied(customer, monkeypatch):
    state, send, used, payer, _ = customer
    def model(**kw):
        state["models"] += 1
        terminate_test_lease(payer)
        return {"patched_content": NEW}
    monkeypatch.setattr(m, "_openai_generate_file_patch", model)
    response = send()
    assert response.status_code == 503, response.text
    assert used() == 0 and state["models"] == 1
    assert state["writes"] == [] and state["posts"] == []


@pytest.mark.skipif(m.DB.engine.dialect.name != "postgresql", reason="requires isolated PostgreSQL")
def test_six_api_processes_cannot_spend_the_same_last_credit(customer, tmp_path):
    _, _, used, payer, slug = customer
    url = m.DB.engine.url
    assert url.host in {"127.0.0.1", "localhost", "::1"}
    assert (url.database or "").startswith("seo_corrector_test")
    helper = Path(__file__).parent / "helpers" / "postgres_correction_worker.py"
    gate = tmp_path / "start"
    ready = [tmp_path / f"worker-{index}" for index in range(6)]

    def run(marker):
        result = subprocess.run([sys.executable, str(helper), payer, slug, str(gate), str(marker)],
                                cwd=Path(__file__).resolve().parents[1], capture_output=True,
                                text=True, timeout=60)
        assert result.returncode == 0, result.stdout + result.stderr
        rows = [line.removeprefix("QUOTA_RESULT=") for line in result.stdout.splitlines()
                if line.startswith("QUOTA_RESULT=")]
        assert len(rows) == 1, result.stdout
        return json.loads(rows[0])

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(run, marker) for marker in ready]
        try:
            deadline = time.monotonic() + 40
            while not all(marker.exists() for marker in ready):
                for future in futures:
                    if future.done():
                        future.result()
                assert time.monotonic() < deadline, "API workers did not reach the start barrier"
                time.sleep(0.05)
        finally:
            gate.touch()
        rows = [future.result(timeout=60) for future in futures]
    assert all(row["status"] in {200, 402, 409, 503} for row in rows), rows
    assert sum(row["status"] == 200 and not row["cached"] for row in rows) == 1, rows
    assert sum(row["model_calls"] for row in rows) == 1, rows
    assert used() == 1


def test_simultaneous_requests_cannot_spend_the_same_last_credit(customer):
    state, send, used, _, _ = customer
    state["block"] = True
    with ThreadPoolExecutor(max_workers=1) as executor:
        first = executor.submit(send)
        try:
            assert state["entered"].wait(5)
            second = send()
            assert second.status_code == 409, second.text
        finally:
            state["release"].set()
            result = first.result(timeout=15)
    assert result.status_code == 200
    assert used() == 1 and state["models"] == 1
    third = send()
    assert third.status_code == 200 and third.json()["cached"] is True
    assert state["models"] == 1


@pytest.mark.parametrize("method,route", [
    ("GET", "issues/missing_title/url-fix"),
    ("POST", "issues/missing_title/deep-fix"),
    ("POST", "github/bulk-fix"),
    ("POST", "keywords/rewrite-pr"),
])
def test_other_correction_routes_share_the_account_lock(customer, method, route):
    state, send, used, _, slug = customer
    state["block"] = True
    with ThreadPoolExecutor(max_workers=1) as executor:
        first = executor.submit(send)
        try:
            assert state["entered"].wait(5)
            client = state["client"]
            params = {"url": URL} if method == "GET" else None
            body = {"url": URL, "query": "fixture"} if method == "POST" else None
            response = client.request(method, f"/api/projects/{slug}/{route}", params=params, json=body,
                                      headers={m._CSRF_HEADER_NAME: client.cookies.get(m._CSRF_COOKIE_NAME, "")})
            assert response.status_code == 409, response.text
        finally:
            state["release"].set()
            assert first.result(timeout=15).status_code == 200
    assert used() == 1 and state["models"] == 1


def test_team_members_on_another_project_cannot_spend_the_owners_reserved_credit(customer):
    state, send, used, owner, _ = customer
    with m.DB.session() as db:
        member = User(email=f"member-{uuid.uuid4().hex}@example.com", password_hash="test", is_admin=False)
        db.add(member)
        db.commit()
        member_id = str(member.id)
        slug = "team-" + uuid.uuid4().hex
        db.add(AccountMember(owner_user_id=owner, member_user_id=member_id))
        db.add(Project(owner_user_id=owner, slug=slug, site_name="fixture.test", base_url=URL))
        db.commit()
    state["block"] = True
    with TestClient(m.app) as client, ThreadPoolExecutor(max_workers=1) as executor:
        client.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(
            user_id=member_id, secret=m._safe_env("SEO_AGENT_SECRET_KEY")))
        client.get("/projects")
        first = executor.submit(send)
        try:
            assert state["entered"].wait(5)
            response = client.post(f"/api/projects/{slug}/issues/missing_title/github-fix", json={"url": URL},
                                   headers={m._CSRF_HEADER_NAME: client.cookies.get(m._CSRF_COOKIE_NAME, "")})
            assert response.status_code == 409, response.text
        finally:
            state["release"].set()
            assert first.result(timeout=15).status_code == 200
    assert used() == 1 and state["models"] == 1
    with m.DB.session() as db:
        assert billing.usage_sum(db, user_id=member_id, metric="ai_corrections_month") == 0


def test_unknown_project_is_denied_before_lock_acquisition(customer, monkeypatch):
    state, _, _, _, _ = customer
    def unexpected(*a, **kw):
        pytest.fail("an inaccessible project must not acquire a payer lock")
    monkeypatch.setattr(correction_guard, "correction_lock", unexpected)
    client = state["client"]
    response = client.post("/api/projects/not-my-project/issues/missing_title/github-fix", json={"url": URL},
                           headers={m._CSRF_HEADER_NAME: client.cookies.get(m._CSRF_COOKIE_NAME, "")})
    assert response.status_code == 404
    assert state["models"] == 0


def test_lock_storage_failure_refuses_without_model_or_github(customer, monkeypatch):
    state, send, used, _, _ = customer
    def unavailable(*a, **kw):
        raise correction_guard.CorrectionStoreUnavailable()
    monkeypatch.setattr(correction_guard, "correction_lock", unavailable)
    assert send().status_code == 503
    assert used() == 0 and state["models"] == 0
    assert state["writes"] == [] and state["posts"] == []


def test_unavailable_model_does_not_charge_or_write(customer, monkeypatch):
    state, send, used, _, _ = customer
    monkeypatch.setattr(m, "_openai_generate_file_patch", lambda **kw: {})
    assert send().status_code == 503
    assert used() == 0
    assert state["writes"] == [] and state["posts"] == []


def test_quota_read_failure_refuses_before_model_or_github(customer, monkeypatch):
    state, send, _, _, _ = customer
    def unavailable(*a, **kw):
        raise OSError("database unavailable")
    monkeypatch.setattr(billing, "remaining_quota", unavailable)
    assert send().status_code == 503
    assert state["models"] == 0
    assert state["writes"] == [] and state["posts"] == []


def test_quota_write_failure_is_not_reported_as_success(customer, monkeypatch):
    _, send, _, _, _ = customer
    def unavailable(*a, **kw):
        raise OSError("database unavailable")
    monkeypatch.setattr(billing, "usage_add", unavailable)
    assert send().status_code == 503


def test_confirmation_still_checks_plan_access(customer, monkeypatch):
    state, send, used, _, _ = customer
    monkeypatch.setattr(m, "_plan_correction_cfg", lambda *a, **kw: {
        "plan": "free", "model": "", "max_files": 0, "unlimited": False})
    assert send(confirm=True, file_path="index.html", patched_content=NEW).status_code == 402
    assert used() == 0 and state["models"] == 0
    assert state["writes"] == [] and state["posts"] == []


@pytest.mark.parametrize("body", [{"file_path": "../secret.html"}, {"patched_content": ""}])
def test_confirmation_keeps_file_and_content_guards(customer, body):
    state, send, _, _, _ = customer
    response = send(confirm=True, **{"file_path": "index.html", "patched_content": NEW, **body})
    assert response.status_code == 400
    assert state["writes"] == [] and state["posts"] == []


def test_a_paid_preview_can_be_retrieved_without_another_model_call(customer):
    state, send, used, _, _ = customer
    first = send()
    second = send()
    assert first.status_code == second.status_code == 200
    assert second.json()["cached"] is True
    assert second.json()["patched_content"] == first.json()["patched_content"]
    assert used() == 1 and state["models"] == 1


def test_a_paid_preview_accepts_github_line_wrapped_base64(customer, monkeypatch):
    state, send, used, _, _ = customer
    assert send().status_code == 200
    monkeypatch.setattr(m, "_github_api_get", lambda *a, **kw: {
        "content": base64.encodebytes(OLD.encode()).decode()})
    result = send()
    assert result.status_code == 200 and result.json()["cached"] is True
    assert state["models"] == 1 and used() == 1


def test_a_url_fix_result_survives_a_lost_response_without_rebilling(customer, monkeypatch):
    state, _, used, _, slug = customer
    calls = []
    def generate(**kwargs):
        calls.append(kwargs)
        return {"ok": True, "title": "An existing paid suggestion"}
    monkeypatch.setattr(m, "_openai_url_fix", generate)
    finish = correction_journal.Operation.finish
    monkeypatch.setattr(correction_journal.Operation, "finish", lambda *a: (_ for _ in ()).throw(
        correction_guard.CorrectionStoreUnavailable()))
    url = f"/api/projects/{slug}/issues/missing_title/url-fix"
    assert state["client"].get(url, params={"url": URL, "crawl": "20261003-090000"}).status_code == 503
    monkeypatch.setattr(correction_journal.Operation, "finish", finish)
    response = state["client"].get(url, params={"url": URL, "crawl": "20261003-090000"})
    assert response.status_code == 200 and response.json()["recovered"] is True
    assert len(calls) == 1 and used() == 1


@pytest.mark.parametrize("action", ["replay", "confirm"])
def test_a_saved_preview_never_overwrites_a_newer_source_revision(customer, monkeypatch, action):
    state, send, used, _, _ = customer
    preview = send().json()
    def newer(path, **kwargs):
        if "/git/ref" in path:
            return {"object": {"sha": "new-base"}}
        assert "/contents/" in path
        return {"sha": "new-source", "content": base64.b64encode((OLD + "\n<!-- newer edit -->").encode()).decode()}
    monkeypatch.setattr(m, "_github_api_get", newer)
    body = {} if action == "replay" else {"confirm": True, "file_path": preview["file"],
                                         "patched_content": preview["patched_content"]}
    response = send(**body)
    assert response.status_code == 409 and response.json()["stale_preview"] is True
    assert used() == 1 and state["models"] == 1
    assert state["writes"] == [] and state["posts"] == []


def test_a_lost_receipt_response_after_charge_does_not_bill_twice(customer, monkeypatch):
    state, send, used, _, _ = customer
    finish = correction_journal.Operation.finish
    def unavailable(self, response):
        raise correction_guard.CorrectionStoreUnavailable()
    monkeypatch.setattr(correction_journal.Operation, "finish", unavailable)
    assert send().status_code == 503
    assert used() == 1
    monkeypatch.setattr(correction_journal.Operation, "finish", finish)
    recovered = send()
    assert recovered.status_code == 200 and recovered.json()["recovered"] is True
    assert used() == 1 and state["models"] == 1


def test_an_uncommitted_charge_is_rolled_back_and_the_saved_preview_is_reused(customer, monkeypatch):
    state, send, used, _, _ = customer
    add = billing.usage_add
    def interrupted(db, **kwargs):
        add(db, **kwargs)
        raise OSError("commit unavailable")
    monkeypatch.setattr(billing, "usage_add", interrupted)
    assert send().status_code == 503
    assert used() == 0
    monkeypatch.setattr(billing, "usage_add", add)
    result = send()
    assert result.status_code == 200 and result.json()["cached"] is True
    assert used() == 1 and state["models"] == 1


def _interrupted_pr(customer, monkeypatch):
    state, send, _, _, _ = customer
    preview = send().json()
    body = {"confirm": True, "file_path": preview["file"], "patched_content": preview["patched_content"]}
    opener = m._ouvrir_pull_request
    def lost_response(**kwargs):
        opener(**kwargs)
        raise OSError("connection lost after GitHub accepted the PR")
    monkeypatch.setattr(m, "_ouvrir_pull_request", lost_response)
    assert send(**body).status_code == 400
    branch = state["posts"][-1][1]["head"]
    pr = {"number": 42, "html_url": "https://github.com/client/fixture/pull/42", "state": "open",
          "head": {"ref": branch, "repo": {"full_name": "client/fixture"}},
          "base": {"ref": "main", "repo": {"full_name": "client/fixture"}}}
    return body, pr


def test_a_pr_accepted_before_connection_loss_is_recovered_without_new_github_writes(customer, monkeypatch):
    state, send, used, _, _ = customer
    body, pr = _interrupted_pr(customer, monkeypatch)
    before = len(state["posts"]), len(state["writes"])
    def recover(path, **kwargs):
        assert path.endswith("/pulls")
        assert kwargs["params"]["head"] == "client:" + pr["head"]["ref"]
        return [pr]
    monkeypatch.setattr(m, "_github_api_get", recover)
    result = send(**body)
    assert result.status_code == 200 and result.json()["recovered"] is True
    assert result.json()["pr_number"] == 42
    assert (len(state["posts"]), len(state["writes"])) == before
    assert used() == 1 and state["models"] == 1
    assert send(**body).status_code == 409


@pytest.mark.parametrize("bad", ["transport", "empty", "multiple", "wrong_head", "wrong_repo", "invalid_head", "invalid_number"])
def test_an_uncertain_pr_recovery_never_repeats_external_work(customer, monkeypatch, bad):
    state, send, used, _, _ = customer
    body, pr = _interrupted_pr(customer, monkeypatch)
    before = len(state["posts"]), len(state["writes"])
    if bad == "wrong_head":
        pr["head"]["ref"] = "another-branch"
    elif bad == "wrong_repo":
        pr["head"]["repo"]["full_name"] = "client/another-repo"
    elif bad == "invalid_head":
        pr["head"] = "not an object"
    elif bad == "invalid_number":
        pr["number"] = "42"
    def recover(*args, **kwargs):
        if bad == "transport":
            raise OSError("GitHub unavailable")
        return [] if bad == "empty" else [pr, pr] if bad == "multiple" else [pr]
    monkeypatch.setattr(m, "_github_api_get", recover)
    assert send(**body).status_code == 503
    assert (len(state["posts"]), len(state["writes"])) == before
    assert used() == 1 and state["models"] == 1


@pytest.mark.parametrize("lost_trace", ["deleted", "corrupted"])
def test_the_journal_prevents_duplicate_work_even_if_issue_task_metadata_is_lost(customer, lost_trace):
    state, send, used, payer, _ = customer
    preview = send().json()
    assert send(confirm=True, file_path=preview["file"], patched_content=preview["patched_content"]).status_code == 200
    with m.DB.session() as db:
        if lost_trace == "deleted":
            db.execute(delete(IssueTask).where(IssueTask.user_id == payer))
        else:
            for task in db.scalars(select(IssueTask).where(IssueTask.user_id == payer)):
                task.note = "lost legacy metadata"
        db.commit()
    before = len(state["posts"]), len(state["writes"])
    assert send().status_code == 409
    assert (len(state["posts"]), len(state["writes"])) == before
    assert used() == 1 and state["models"] == 1


def test_an_interrupted_partial_branch_blocks_retries_with_a_changed_request(customer, monkeypatch):
    state, send, used, payer, _ = customer
    preview = send().json()
    def interrupted(*args, **kwargs):
        raise OSError("ambiguous file write")
    monkeypatch.setattr(m, "_github_api_put", interrupted)
    body = {"confirm": True, "file_path": preview["file"], "patched_content": preview["patched_content"]}
    assert send(**body).status_code == 400
    before = len(state["posts"]), len(state["writes"])
    assert send(**body).status_code == 503
    assert send(**{**body, "crawl_ts": "20261003-100000"}).status_code == 503
    assert (len(state["posts"]), len(state["writes"])) == before
    assert used() == 1 and state["models"] == 1
    with m.DB.session() as db:
        assert db.scalar(select(CorrectionOperation).where(
            CorrectionOperation.payer_id == payer, CorrectionOperation.state == "blocked"))


@pytest.mark.parametrize("changed", [False, True])
def test_operator_reconciliation_unblocks_a_confirm_without_rebilling_its_preview(customer, monkeypatch, changed):
    state, send, used, payer, _ = customer
    original_get, original_post, original_put = m._github_api_get, m._github_api_post, m._github_api_put
    def get(path, **kwargs):
        return {"object": {"sha": "a" * 40}} if "/git/ref" in path else original_get(path, **kwargs)
    def post(path, **kwargs):
        if path.endswith("/git/refs"):
            row = correction_journal.current().row
            persisted = correction_journal.find(m.DB, key=row.request_key)
            assert persisted.data["repository"] == {"owner": "client", "repo": "fixture", "base": "main", "base_sha": "a" * 40}
            assert kwargs["json_body"]["ref"] == "refs/heads/" + persisted.data["branch"]
        return original_post(path, **kwargs)
    def interrupted(*args, **kwargs):
        raise OSError("ambiguous file write")
    monkeypatch.setattr(m, "_github_api_get", get)
    monkeypatch.setattr(m, "_github_api_post", post)
    preview = send().json()
    body = {"confirm": True, "file_path": preview["file"], "patched_content": preview["patched_content"]}
    monkeypatch.setattr(m, "_github_api_put", interrupted)
    assert send(**body).status_code == 400 and send(**body).status_code == 503
    row = correction_journal.pending(m.DB, payer=payer)
    class Reader:
        def get(self, path, **kwargs):
            if path.endswith("/pulls"):
                return []
            if "/git/ref/" in path:
                return {"ref": "refs/heads/" + row.data["branch"],
                        "object": {"type": "commit", "sha": ("b" if changed else "a") * 40}}
            if "/git/commits/" in path:
                return {"sha": "a" * 40}
            return {"full_name": "client/fixture"}
    result = correction_reconciliation.reconcile(m.DB, row.id, lambda payer: Reader(), abandon=True,
                                                 retain_branch=changed, operator="test", reason="interrupted confirm")
    assert result["released"] is True and used() == 1
    monkeypatch.setattr(m, "_github_api_put", original_put)
    confirmed = send(**body)
    assert confirmed.status_code == 200, confirmed.text
    assert used() == 1 and state["models"] == 1
    assert confirmed.json()["branch"] != row.data["branch"]
    assert len([path for path, _ in state["posts"] if path.endswith("/pulls")]) == 1
