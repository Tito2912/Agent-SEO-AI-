"""Kill/restart API workers and drop an accepted PR's response on one owned fixture.

At most two correction branches, one file commit and one draft PR. Never merge;
close only the PR created here, preserve branches, and use a fresh local database.
Claude is replaced with a deterministic preview to isolate recovery from SEO quality.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import socket
import subprocess
import sys
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

WEB_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(WEB_ROOT))

import requests  # noqa: E402
from ops.gauntlet.live_cycle import OWNER, _load_backend, save  # noqa: E402

REPO = "noyaru-stack-static-html"
PREFIX = f"/repos/{OWNER}/{REPO}"
FILE = "gauntlet/missing-title.html"
FILE_API = PREFIX + "/contents/" + FILE
PAGE = f"https://{REPO}.netlify.app/gauntlet/missing-title"
WORKER = Path(__file__).with_name("recovery_worker.py")


class BenchCheckFailed(Exception):
    pass


def require(condition, check):
    if not condition:
        raise BenchCheckFailed(check)


class FaultRelay:
    def __init__(self, token, *, forward=None):
        self.token = token
        self.forward = forward or requests.request
        self.main_sha = ""
        self.file_sha = ""
        self.patched = ""
        self.branches = []
        self.created_branches = []
        self.prs = []
        self.events = []
        self.puts = 0
        self.pr_attempts = 0
        self.pr_head = ""
        self.fault = ""
        self.accepted = threading.Event()
        self.release = threading.Event()
        self.lock = threading.Lock()
        self.checkpoint = lambda: None

    def _remote(self, method, path, **kwargs):
        if not (path == PREFIX or path.startswith(PREFIX + "/")):
            raise ValueError("Fixture scope violation")
        return self.forward(method, "https://api.github.com" + path,
                            headers={"Authorization": "Bearer " + self.token,
                                     "Accept": "application/vnd.github+json",
                                     "Content-Type": "application/json",
                                     "X-GitHub-Api-Version": "2022-11-28"},
                            timeout=30, allow_redirects=False, **kwargs)

    def get(self, path, *, params=None, missing=False):
        response = self._remote("GET", path, params=params)
        if missing and response.status_code == 404:
            return None
        if response.status_code != 200:
            raise RuntimeError("Fixture read unavailable")
        return response.json()

    def arm(self, fault):
        self.fault = fault
        self.accepted.clear()
        self.release.clear()

    def validate(self, method, path, body):
        parsed = urlsplit(path)
        if parsed.scheme or parsed.netloc or not (parsed.path == PREFIX or parsed.path.startswith(PREFIX + "/")):
            raise ValueError("Fixture scope violation")
        path = parsed.path
        if method == "GET":
            return ""
        if method == "POST" and path == PREFIX + "/git/refs":
            ref = body.get("ref", "")
            if (len(self.branches) >= 2 or not ref.startswith("refs/heads/seo-fix/missing_title-")
                    or body.get("sha") != self.main_sha or ref in self.branches):
                raise ValueError("Fixture branch limit or base violation")
            self.branches.append(ref)
            return "branch"
        if method == "PUT" and path == FILE_API:
            if (self.puts or "refs/heads/" + str(body.get("branch", "")) not in self.branches
                    or body.get("sha") != self.file_sha
                    or body.get("content") != base64.b64encode(self.patched.encode()).decode()):
                raise ValueError("Fixture file scope or commit budget violation")
            self.puts += 1
            return "file"
        if method == "POST" and path == PREFIX + "/pulls":
            if (self.pr_attempts or body.get("draft") is not True or body.get("base") != "main"
                    or "refs/heads/" + str(body.get("head", "")) not in self.branches):
                raise ValueError("Fixture PR budget or draft violation")
            self.pr_attempts += 1
            self.pr_head = body["head"]
            return "pr"
        raise ValueError("Fixture write is not allowed")

    def handle(self, handler):
        if handler.headers.get("Authorization") != "Bearer " + self.token:
            handler.send_error(403)
            return
        try:
            length = int(handler.headers.get("Content-Length", "0"))
            if not 0 <= length <= 100_000:
                raise ValueError("Fixture request too large")
            raw = handler.rfile.read(length)
            body = json.loads(raw) if raw else {}
            with self.lock:
                kind = self.validate(handler.command, handler.path, body)
            response = self._remote(handler.command, handler.path, data=raw or None)
            with self.lock:
                event = {"method": handler.command, "path": urlsplit(handler.path).path,
                         "status": response.status_code, "kind": kind}
                if kind == "branch" and response.status_code == 201:
                    self.created_branches.append(body["ref"])
                if kind == "pr" and response.status_code == 201:
                    pr = response.json()
                    self.prs.append({"number": pr["number"], "url": pr["html_url"], "branch": body["head"]})
                    event["pr_number"] = pr["number"]
                self.events.append(event)
                fault = self.fault if response.status_code == 201 else ""
                if (fault == "hold_branch" and kind == "branch") or (fault == "drop_pr" and kind == "pr"):
                    self.fault = ""
                    event["fault"] = fault
                else:
                    fault = ""
                self.checkpoint()
            if fault:
                self.accepted.set()
                if fault == "hold_branch":
                    self.release.wait(60)
                handler.close_connection = True
                try:
                    handler.connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                handler.connection.close()
                return
            handler.send_response(response.status_code)
            handler.send_header("Content-Type", "application/json")
            handler.send_header("Content-Length", str(len(response.content)))
            handler.end_headers()
            handler.wfile.write(response.content)
        except Exception:
            # No upstream body, headers or credentials in transport diagnostics.
            handler.send_error(502, "Fixture relay refused or unavailable")

    def close_prs(self):
        from backend.correction_journal import matches_pr

        intent = {"owner": OWNER, "repo": REPO, "base": "main"}
        outcomes = []
        if self.pr_attempts and not self.prs:
            try:
                rows = self.get(PREFIX + "/pulls", params={"head": OWNER + ":" + self.pr_head,
                                                          "base": "main", "state": "all"})
                if not isinstance(rows, list) or len(rows) != 1:
                    raise RuntimeError("Ambiguous fixture PR lookup")
                if not matches_pr(rows[0], intent, self.pr_head):
                    raise RuntimeError("Unconfirmed fixture PR identity")
                self.prs.append({"number": rows[0]["number"], "url": rows[0]["html_url"], "branch": self.pr_head})
            except Exception as exc:
                outcomes.append({"cleanup_error_type": type(exc).__name__, "outcome": "unknown_pr_acceptance"})
        for pr in self.prs:
            try:
                path = PREFIX + "/pulls/" + str(pr["number"])
                data = self.get(path)
                if (not matches_pr(data, intent, pr["branch"]) or data["number"] != pr["number"]
                        or data["html_url"].casefold() != pr["url"].casefold()):
                    raise RuntimeError("Unconfirmed fixture PR identity")
                if data["state"] != "closed":
                    response = self._remote("PATCH", path, json={"state": "closed"})
                    if response.status_code != 200:
                        raise RuntimeError("Fixture PR closure unavailable")
                    data = response.json()
                    if not matches_pr(data, intent, pr["branch"]) or data["number"] != pr["number"]:
                        raise RuntimeError("Unconfirmed fixture PR closure")
                outcomes.append({"number": pr["number"], "state": data["state"], "merged": bool(data.get("merged"))})
            except Exception as exc:
                outcomes.append({"number": pr["number"], "cleanup_error_type": type(exc).__name__})
        return outcomes


def start_relay(relay):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass
        def do_GET(self):
            return relay.handle(self)
        do_POST = do_GET
        do_PUT = do_GET
        do_PATCH = do_GET
        do_DELETE = do_GET
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = False
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def worker(workdir, mode, *, hold=None):
    log_path = workdir / (uuid.uuid4().hex + "-" + mode + ".log")
    with log_path.open("w", encoding="utf-8") as log:
        child = subprocess.Popen([sys.executable, str(WORKER), str(workdir), mode], cwd=WEB_ROOT,
                                 env=dict(os.environ), stdout=log, stderr=subprocess.STDOUT)
        try:
            if hold:
                if not hold.accepted.wait(45):
                    raise TimeoutError("Fixture effect not observed before worker termination")
                child.kill()
                child.wait(timeout=10)
                hold.release.set()
            else:
                child.wait(timeout=90)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=10)
            if hold:
                hold.release.set()
    lines = log_path.read_text(encoding="utf-8").splitlines()
    results = [json.loads(line.partition("=")[2]) for line in lines if line.startswith("RECOVERY_RESULT=")]
    return {"exit_code": child.returncode, "model_calls": lines.count("RECOVERY_MODEL_CALL=1"),
            "result": results[-1] if results else None, "log": log_path.name}


def cycle(m, token, workdir, *, forward=None):
    from backend import billing, correction_journal as journal, correction_reconciliation as doctor
    from backend.models import CorrectionOperation, IssueTask, Project, User
    from sqlalchemy import select

    if (m.DB.engine.dialect.name != "sqlite"
            or Path(m.DB.engine.url.database).resolve() != (workdir / "bench.db").resolve()):
        raise ValueError("Only the isolated bench database is allowed")
    m.DB.create_tables()
    relay = FaultRelay(token, forward=forward)
    result = {"repository": OWNER + "/" + REPO, "claude_calls": 0, "provider": "deterministic_stub",
              "database": "isolated_sqlite", "workers": [], "scenarios": [], "status": "failed"}
    relay.checkpoint = lambda: save(workdir / "recovery.json", {**result, "github_events": relay.events,
                                                               "pull_requests": relay.prs})
    server = thread = None
    try:
        metadata = relay.get(PREFIX)
        require(metadata["full_name"] == OWNER + "/" + REPO, "fixture_repository_identity")
        main = relay.get(PREFIX + "/git/ref/heads/main")["object"]["sha"]
        relay.main_sha = result["main_sha_before"] = main
        source = relay.get(FILE_API, params={"ref": main})
        original = base64.b64decode("".join(source["content"].split()), validate=True).decode("utf-8")
        require("<title" not in original.lower() and original.count("</head>") == 1, "missing_title_positive_control")
        relay.file_sha = source["sha"]
        relay.patched = m._respecter_les_fins_de_ligne(original, original.replace(
            "</head>", "<title>Parcours de test pour la reprise des corrections SEO</title>\n  </head>"))
        tag = uuid.uuid4().hex
        with m.DB.session() as db:
            user = User(email=f"recovery-{tag}@example.invalid", password_hash=uuid.uuid4().hex, is_admin=False)
            db.add(user)
            db.commit()
            m._mark_user_email_verified(db, user_id=user.id)
            project = Project(owner_user_id=user.id, slug="recovery-" + tag, site_name="Recovery fixture",
                              base_url=f"https://{REPO}.netlify.app/", settings={
                                  "github_repo": OWNER + "/" + REPO, "github_branch": "main", "github_mode": "review"})
            db.add(project)
            db.commit()
            payer, project_id, slug = user.id, project.id, project.slug
        server, thread = start_relay(relay)
        save(workdir / "worker.json", {"payer": payer, "project": project_id, "slug": slug,
                                      "relay": "http://127.0.0.1:" + str(server.server_port), "file": FILE,
                                      "url": PAGE, "original": original, "patched": relay.patched})
        def run(mode, **kwargs):
            value = worker(workdir, mode, **kwargs)
            result["workers"].append({"mode": mode, **value})
            relay.checkpoint()
            return value
        def used():
            with m.DB.session() as db:
                return billing.usage_sum(db, user_id=payer, metric="ai_corrections_month")
        preview = run("crash_preview")
        require(preview["exit_code"] == 23 and preview["model_calls"] == 1 and used() == 1, "paid_preview_process_exit")
        pending = journal.pending(m.DB, payer=payer)
        require(pending.charged and pending.data["preview"] and not relay.events, "paid_preview_durable_receipt")
        recovered = run("preview")
        require(recovered["result"]["status"] == 200 and recovered["result"]["recovered"] and used() == 1,
                "paid_preview_recovery")
        result["scenarios"].append("paid_preview_survives_process_exit")
        relay.arm("hold_branch")
        killed = run("confirm", hold=relay)
        require(killed["exit_code"] != 0 and len(relay.branches) == 1 and relay.puts == 0, "branch_worker_killed")
        pending = journal.pending(m.DB, payer=payer)
        require(pending.data["branch"] == relay.branches[0].removeprefix("refs/heads/"), "branch_intent_saved_before_exit")
        replay = run("confirm")
        require(replay["result"]["status"] == 503 and replay["result"]["recovery_required"], "partial_branch_replay_refused")
        require(len(relay.branches) == 1 and relay.puts == 0, "partial_branch_replay_no_write")
        inspected = doctor.reconcile(m.DB, pending.id, lambda payer: relay)
        require(inspected["decision"] == "can_abandon" and inspected["reason"] == "unchanged_branch",
                "unchanged_branch_verified")
        abandoned = doctor.reconcile(m.DB, pending.id, lambda payer: relay, abandon=True,
                                      operator="fixture-recovery-bench", reason="Worker killed before branch acknowledgement")
        require(abandoned["released"] and used() == 1, "partial_branch_released_without_charge")
        result["scenarios"].append("killed_branch_worker_reconciled_without_rebilling")
        relay.arm("drop_pr")
        failed_pr = run("confirm")
        require(failed_pr["result"]["status"] == 400 and relay.accepted.is_set(), "accepted_pr_response_dropped")
        require(len(relay.prs) == 1 and relay.puts == 1 and len(relay.branches) == 2, "github_write_budget")
        pending = journal.pending(m.DB, payer=payer)
        require(pending.data["pr_intent"] and not pending.data.get("pr"), "pr_intent_without_response_receipt")
        before = len(relay.events)
        recovered_pr = run("confirm")
        require(recovered_pr["result"]["status"] == 200 and recovered_pr["result"]["recovered"], "accepted_pr_recovered")
        require(recovered_pr["result"]["pr_number"] == relay.prs[0]["number"], "accepted_pr_identity_preserved")
        require(all(event["method"] == "GET" for event in relay.events[before:]), "pr_recovery_read_only")
        require(run("confirm")["result"]["status"] == 409, "recovered_pr_duplicate_refused")
        require(used() == 1 and sum(row["model_calls"] for row in result["workers"]) == 1, "one_model_call_one_debit")
        require(relay.pr_attempts == 1 and relay.puts == 1, "one_commit_one_pr")
        with m.DB.session() as db:
            operation = db.scalar(select(CorrectionOperation).where(
                CorrectionOperation.payer_id == payer, CorrectionOperation.action == "api_github_fix",
                CorrectionOperation.state == "completed", CorrectionOperation.charged.is_(False)))
            task = db.scalar(select(IssueTask).where(IssueTask.project_id == project_id))
            note = json.loads(task.note)
            require(note["pr_number"] == relay.prs[0]["number"], "task_trace_restored")
            require(note["verification"]["fusion_auto"] is False, "recovery_no_auto_merge")
            require(operation.data["pr"]["head"]["sha"] == note["verification"]["sha"], "verification_exact_commit")
        pr = relay.get(PREFIX + "/pulls/" + str(relay.prs[0]["number"]))
        require(pr["draft"] is True and not pr.get("merged") and pr["head"]["ref"] == relay.prs[0]["branch"],
                "recovered_pr_still_draft")
        committed = relay.get(FILE_API, params={"ref": pr["head"]["sha"]})
        require(base64.b64decode("".join(committed["content"].split()), validate=True).decode() == relay.patched,
                "remote_file_matches_paid_preview")
        result["scenarios"].append("socket_drop_after_github_pr_acceptance_recovered_once")
        result.update(status="passed", used_credits=used(), simulated_model_calls=1,
                      pr_head_sha=pr["head"]["sha"], verification_auto_merge=False)
    except Exception as exc:
        result["error_type"] = type(exc).__name__
        if isinstance(exc, BenchCheckFailed):
            result["failed_check"] = str(exc)
    finally:
        relay.release.set()
        if server:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
        result["cleanup"] = relay.close_prs()
        result["pull_requests_closed_without_merge"] = all(
            row.get("state") == "closed" and row.get("merged") is False for row in result["cleanup"])
        try:
            result["main_sha_after"] = relay.get(PREFIX + "/git/ref/heads/main")["object"]["sha"]
            result["main_unchanged"] = result["main_sha_after"] == result.get("main_sha_before")
        except Exception as exc:
            result["main_check_error_type"] = type(exc).__name__
            result["main_unchanged"] = False
        result.update(github_events=relay.events, pull_requests=relay.prs, branches_retained=relay.created_branches)
        save(workdir / "recovery.json", result)
    return result


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--workdir", type=Path, required=True)
    args = cli.parse_args()
    workdir = args.workdir.resolve()
    if workdir.exists() and any(workdir.iterdir()):
        cli.error("Use a fresh empty workdir, never a production data directory")
    workdir.mkdir(parents=True, exist_ok=True)
    m, token = _load_backend(workdir)
    result = cycle(m, token, workdir)
    print(json.dumps({key: result[key] for key in ("status", "scenarios", "main_unchanged",
                                                 "pull_requests_closed_without_merge")}, ensure_ascii=True))
    return int(result["status"] != "passed" or not result["main_unchanged"]
               or not result["pull_requests_closed_without_merge"])


if __name__ == "__main__":
    raise SystemExit(main())
