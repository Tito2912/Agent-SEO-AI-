"""Bounded real GitHub bulk effects with disposable SQLite or loopback PostgreSQL.

Two branches, four exact file commits and one draft PR, closed without merging.
Provider output is a deterministic stub; this tests receipts, not Claude quality.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path
from urllib.parse import unquote, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import inspect, select  # noqa: E402
from sqlalchemy.engine import make_url  # noqa: E402
from ops.gauntlet import recovery_cycle as previous  # noqa: E402

OWNER, REPO, PREFIX = previous.OWNER, previous.REPO, previous.PREFIX
FILES = tuple("gauntlet/" + name + ".html" for name in ("duplicate-a", "duplicate-b", "missing-title"))
SETTINGS = {"github_repo": OWNER + "/" + REPO, "github_branch": "main", "github_mode": "review"}
WORKER = Path(__file__).with_name("bulk_recovery_worker.py")
require = previous.require
save = previous.save


def database_url(work: Path, raw: str = "") -> str:
    expected = "sqlite:///" + (work / "bench.db").as_posix()
    if not raw or raw == expected:
        return expected
    url = make_url(raw)
    if (url.drivername != "postgresql+psycopg" or url.host != "127.0.0.1" or not url.port
            or url.username != "fixture" or url.password or url.query
            or not (url.database or "").startswith("seo_corrector_test_bulk_")):
        raise ValueError("Only a dedicated loopback fixture PostgreSQL database is allowed")
    return raw


def load_backend(work: Path, raw: str = ""):
    url = database_url(work, raw)
    m, token = previous._load_backend(work)
    from backend.db import Database

    m.DB.engine.dispose()
    os.environ["DATABASE_URL"] = url
    m.DB = Database(data_dir=work / "data")
    return m, token


def scoped_path(path):
    parsed = urlsplit(path)
    decoded = unquote(parsed.path)
    if (parsed.scheme or parsed.netloc or parsed.fragment or "\\" in decoded
            or any(p in {".", ".."} for p in decoded.split("/"))
            or not (decoded == PREFIX or decoded.startswith(PREFIX + "/"))):
        raise ValueError("Only the owned fixture API scope is allowed")
    return parsed.path


def patched_source(m, path, original):
    suffix = path.rsplit("/", 1)[1].removesuffix(".html")
    value = "Validation de la reprise du lot SEO : " + suffix
    found = m._find_head_text_value(original, "title")
    if suffix == "missing-title":
        require(found is None and original.count("</head>") == 1, "missing_title_positive_control")
        new = original.replace("</head>", "<title>" + value + "</title>\n  </head>", 1)
    else:
        require(found is not None, "duplicate_title_positive_control")
        new = original.replace(found[0], "<title>" + value + "</title>", 1)
        new, _ = m._sync_social_copies(new, "title", found[1], value)
    new, _ = m._complete_open_graph(new, original, f"https://{REPO}.netlify.app/og.png")
    return m._respecter_les_fins_de_ligne(original, new)


class BulkRelay(previous.FaultRelay):
    def __init__(self, token, *, forward=None):
        super().__init__(token, forward=forward)
        self.sources = {}
        self.file_attempts = []

    def _remote(self, method, path, **kwargs):
        scoped_path(path)
        return super()._remote(method, path, **kwargs)

    def validate(self, method, path, body):
        path = scoped_path(path)
        if method == "GET":
            return ""
        if method == "POST" and path == PREFIX + "/git/refs":
            ref = body.get("ref", "")
            if (len(self.branches) >= 2 or not ref.startswith("refs/heads/seo-fix/bulk-")
                    or body.get("sha") != self.main_sha or ref in self.branches):
                raise ValueError("Fixture branch budget or base violation")
            self.branches.append(ref)
            return "branch"
        if method == "PUT" and path.startswith(PREFIX + "/contents/"):
            file = unquote(path.partition("/contents/")[2])
            branch = body.get("branch", "")
            source = self.sources.get(file)
            pair = (branch, file)
            if (source is None or "refs/heads/" + branch not in self.branches or self.puts >= 4
                    or pair in self.file_attempts or body.get("sha") != source["sha"]
                    or body.get("content") != base64.b64encode(source["patched"].encode()).decode()):
                raise ValueError("Fixture exact-content or file budget violation")
            self.file_attempts.append(pair)
            self.puts += 1
            return "file"
        if method == "POST" and path == PREFIX + "/pulls":
            head = body.get("head", "")
            written = {file for branch, file in self.file_attempts if branch == head}
            if (self.pr_attempts or body.get("draft") is not True or body.get("base") != "main"
                    or "refs/heads/" + head not in self.branches or written != set(FILES)):
                raise ValueError("Fixture completed-batch draft PR budget violation")
            self.pr_attempts += 1
            self.pr_head = head
            return "pr"
        raise ValueError("Fixture write is not allowed")


def worker(work, mode, *, hold=None):
    log_path = work / (uuid.uuid4().hex + "-" + mode + ".log")
    with log_path.open("w", encoding="utf-8") as log:
        child = subprocess.Popen([sys.executable, str(WORKER), str(work), mode],
                                 cwd=previous.WEB_ROOT, env=dict(os.environ), stdout=log, stderr=subprocess.STDOUT)
        try:
            if hold:
                if not hold.accepted.wait(60):
                    raise TimeoutError("The accepted content write was not observed")
                child.kill()
                child.wait(timeout=10)
            else:
                child.wait(timeout=120)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=10)
            if hold:
                hold.release.set()
    lines = log_path.read_text(encoding="utf-8").splitlines()
    results = [json.loads(line.partition("=")[2]) for line in lines if line.startswith("BULK_RESULT=")]
    return {"exit_code": child.returncode, "model_calls": lines.count("BULK_MODEL_CALL=1"),
            "result": results[-1] if results else None, "log": log_path.name}


def cycle(m, token, work, *, forward=None):
    from backend import billing, correction_journal as journal, correction_reconciliation as doctor
    from backend.models import CorrectionOperation, IssueTask, Project, UsageEvent, User

    url = str(m.DB.engine.url)
    if url != database_url(work, url) or inspect(m.DB.engine).get_table_names():
        raise ValueError("Only a fresh isolated fixture database is allowed")
    m.DB.create_tables()
    relay = BulkRelay(token, forward=forward)
    result = {"repository": OWNER + "/" + REPO, "provider": "deterministic_stub", "claude_calls": 0,
              "database": m.DB.engine.dialect.name, "workers": [], "scenarios": [], "status": "failed"}
    relay.checkpoint = lambda: save(work / "bulk-recovery.json", {**result, "github_events": relay.events,
                                                               "pull_requests": relay.prs})
    server = thread = None
    try:
        require(relay.get(PREFIX)["full_name"] == OWNER + "/" + REPO, "fixture_repository_identity")
        relay.main_sha = result["main_sha_before"] = relay.get(PREFIX + "/git/ref/heads/main")["object"]["sha"]
        for file in FILES:
            source = relay.get(PREFIX + "/contents/" + file, params={"ref": relay.main_sha})
            original = base64.b64decode("".join(source["content"].split()), validate=True).decode("utf-8")
            relay.sources[file] = {"sha": source["sha"], "original": original,
                                   "patched": patched_source(m, file, original)}
        first, second = (m._find_head_text_value(relay.sources[file]["original"], "title")[1] for file in FILES[:2])
        require(first == second, "duplicate_title_pair_observed")
        tag = uuid.uuid4().hex
        with m.DB.session() as db:
            user = User(email=f"bulk-recovery-{tag}@example.invalid", password_hash=uuid.uuid4().hex, is_admin=False)
            db.add(user)
            db.commit()
            m._mark_user_email_verified(db, user_id=user.id)
            project = Project(owner_user_id=user.id, slug="bulk-recovery-" + tag, site_name="Bulk recovery fixture",
                              base_url=f"https://{REPO}.netlify.app/", settings=SETTINGS)
            db.add(project)
            db.commit()
            payer, project_id, slug = user.id, project.id, project.slug
        result.update(payer_id=payer, project_id=project_id)
        pages = [{"url": f"https://{REPO}.netlify.app/" + file.removesuffix(".html"),
                  "status_code": 200, "content_type": "text/html", "lang": "fr",
                  "title": m._find_head_text_value(s["original"], "title")[1] if file != FILES[2] else ""}
                 for file, s in relay.sources.items()]
        report = {"meta": {"pages_crawled": 3}, "pages": pages, "issues": {
            "duplicate_titles": {"count": 2, "examples": [p["url"] for p in pages[:2]]},
            "missing_title": {"count": 1, "examples": [pages[2]["url"]]}}}
        report_path = work / "runs" / payer / slug / "20261004-090000" / "audit/report.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        save(report_path, report)
        server, thread = previous.start_relay(relay)
        save(work / "worker.json", {"payer": payer, "project": project_id, "slug": slug,
                                   "relay": "http://127.0.0.1:" + str(server.server_port),
                                   "database_url": url, "sources": relay.sources})

        def run(mode="bulk", **kwargs):
            value = worker(work, mode, **kwargs)
            result["workers"].append({"mode": mode, **value})
            relay.checkpoint()
            return value

        def used():
            with m.DB.session() as db:
                return billing.usage_sum(db, user_id=payer, metric="ai_corrections_month")

        relay.arm("hold_file")
        killed = run(hold=relay)
        require(killed["exit_code"] != 0 and relay.puts == 1 and relay.pr_attempts == 0 and used() == 0,
                "killed_after_accepted_content_before_pr_no_debit")
        pending = journal.pending(m.DB, payer=payer)
        require(pending and not pending.charged and not pending.data.get("pr_intent"), "partial_write_pending_receipt")
        before = len(relay.events)
        replay = run()
        require(replay["result"]["status"] == 503 and replay["result"]["recovery_required"]
                and replay["model_calls"] == 0 and relay.puts == 1, "partial_write_replay_refused")
        require(all(e["method"] == "GET" for e in relay.events[before:]), "partial_write_replay_no_remote_write")
        inspected = doctor.reconcile(m.DB, pending.id, lambda payer: relay)
        require(inspected["decision"] == "retain_branch_required" and inspected["reason"] == "unpublished_changes",
                "operator_sees_unpublished_content")
        try:
            doctor.reconcile(m.DB, pending.id, lambda payer: relay, abandon=True,
                             operator="fixture-bulk-bench", reason="Forced accepted-file process kill")
        except doctor.ReconciliationRefused:
            pass
        else:
            raise previous.BenchCheckFailed("unpublished_content_abandonment_requires_retention")
        released = doctor.reconcile(m.DB, pending.id, lambda payer: relay, abandon=True, retain_branch=True,
                                    operator="fixture-bulk-bench", reason="Retain accepted content for operator review")
        retained = relay.get(PREFIX + "/git/ref/heads/" + pending.data["branch"])["object"]["sha"]
        require(released["released"] and retained == inspected["head_sha"] and used() == 0,
                "explicit_retention_releases_request_without_debit")
        result["retained_partial_branch"] = {"branch": pending.data["branch"], "head_sha": retained}
        result["scenarios"].append("accepted_partial_content_retained_with_explicit_operator_release")
        relay.arm("drop_pr")
        failed = run()
        require(failed["result"]["status"] == 400 and relay.accepted.is_set() and len(relay.prs) == 1
                and relay.puts == 4 and used() == 0, "accepted_batch_pr_response_lost_before_charge")
        pending = journal.pending(m.DB, payer=payer)
        require(pending.data["pr_intent"]["billable"] == 3 and len(pending.data["pr_intent"]["tasks"]) == 2
                and not pending.data.get("pr"), "batch_frozen_intent_saved")
        before = len(relay.events)
        crashed = run("crash_charge")
        require(crashed["exit_code"] == 24 and used() == 3 and crashed["model_calls"] == 0,
                "batch_charge_committed_before_process_exit")
        recovered = run()
        require(recovered["result"]["status"] == 200 and recovered["result"]["recovered"]
                and recovered["result"]["pr_number"] == relay.prs[0]["number"], "batch_pr_recovered")
        require(recovered["result"]["fixed_count"] == recovered["result"]["total_count"] == 2
                and recovered["result"]["partial"] is False, "frozen_batch_summary_recovered")
        duplicate = run()
        require(duplicate["result"]["status"] == 409 and duplicate["result"]["duplicate"], "duplicate_batch_refused")
        require(all(e["method"] == "GET" for e in relay.events[before:]), "batch_recovery_get_only")
        require(used() == 3 and relay.pr_attempts == 1 and relay.puts == 4 and len(relay.branches) == 2,
                "one_batch_debit_one_pr_no_repeated_writes")
        with m.DB.session() as db:
            ledger = db.scalars(select(UsageEvent).where(UsageEvent.user_id == payer)).all()
            operations = db.scalars(select(CorrectionOperation).where(CorrectionOperation.payer_id == payer)).all()
            completed = [row for row in operations if row.state == "completed"]
            tasks = db.scalars(select(IssueTask).where(IssueTask.project_id == project_id)).all()
            require(len(ledger) == 1 and ledger[0].amount == 3 and len(completed) == 1 and completed[0].charged,
                    "single_atomic_batch_ledger_receipt")
            require(ledger[0].meta["operation_id"] == completed[0].id and len(tasks) == 2,
                    "batch_task_and_ledger_operation_identity")
            notes = [json.loads(task.note) for task in tasks]
            require(all(note["pr_number"] == relay.prs[0]["number"] for note in notes), "all_family_tasks_restored")
            traces = [note["verification"] for note in notes if "verification" in note]
            require(len(traces) == 1 and traces[0]["fusion_auto"] is False
                    and traces[0]["sha"] == completed[0].data["pr"]["head"]["sha"], "one_exact_commit_trace_no_auto_merge")
        pr = relay.get(PREFIX + "/pulls/" + str(relay.prs[0]["number"]))
        require(pr["draft"] and not pr.get("merged") and not pr.get("merged_at"), "draft_batch_unmerged")
        for file in FILES:
            source = relay.get(PREFIX + "/contents/" + file, params={"ref": pr["head"]["sha"]})
            require(base64.b64decode("".join(source["content"].split()), validate=True).decode("utf-8")
                    == relay.sources[file]["patched"], "exact_batch_remote_content")
        result["scenarios"].append("lost_batch_pr_response_and_committed_charge_recovered_once")
        result.update(status="passed", used_credits=3, usage_events=1, tasks_restored=2,
                      simulated_model_calls=sum(r["model_calls"] for r in result["workers"]),
                      pr_head_sha=pr["head"]["sha"], verification_auto_merge=False)
    except Exception as exc:
        result["error_type"] = type(exc).__name__
        if isinstance(exc, previous.BenchCheckFailed):
            result["failed_check"] = str(exc)
    finally:
        relay.release.set()
        if server:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
        result["cleanup"] = relay.close_prs()
        result["pull_requests_closed_without_merge"] = (bool(relay.prs)
            and {r.get("number") for r in result["cleanup"]} == {r["number"] for r in relay.prs}
            and all(r.get("state") == "closed" and r.get("merged") is False for r in result["cleanup"]))
        try:
            result["main_sha_after"] = relay.get(PREFIX + "/git/ref/heads/main")["object"]["sha"]
            result["main_unchanged"] = result["main_sha_after"] == result.get("main_sha_before")
        except Exception as exc:
            result["main_check_error_type"] = type(exc).__name__
            result["main_unchanged"] = False
        result.update(github_events=relay.events, pull_requests=relay.prs, branches_retained=relay.created_branches)
        save(work / "bulk-recovery.json", result)
    return result


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--workdir", type=Path, required=True)
    cli.add_argument("--postgres-url", default="")
    args = cli.parse_args()
    work = args.workdir.resolve()
    if work.exists() and any(work.iterdir()):
        cli.error("Use an empty temporary workdir, never client data")
    work.mkdir(parents=True, exist_ok=True)
    m, token = load_backend(work, args.postgres_url)
    try:
        result = cycle(m, token, work)
        print(json.dumps({k: result[k] for k in ("status", "scenarios", "main_unchanged",
                                               "pull_requests_closed_without_merge")}, ensure_ascii=True))
        return int(result["status"] != "passed" or not result["main_unchanged"]
                   or not result["pull_requests_closed_without_merge"])
    finally:
        m.DB.engine.dispose()
        if getattr(m.DB, "correction_engine", m.DB.engine) is not m.DB.engine:
            m.DB.correction_engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
