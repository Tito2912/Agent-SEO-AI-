"""Owned Linux process smoke test, not a Docker/Render or customer-capacity certificate."""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import sys
import time
from urllib.request import urlopen

WEB = Path(__file__).resolve().parents[2]
SOURCES = ["backend/app.py", "backend/worker_main.py", "../render.yaml", "tests/test_service_startup_policy.py",
           "ops/gauntlet/startup_policy_cycle.py", "tests/test_startup_policy_cycle.py"]
PLAN = json.dumps({"pro": {"limits": {"projects": 7}}}, sort_keys=True)
MARKER = "[STARTUP_FIXTURE] "


def fixture_environment(work: Path, role: str) -> dict[str, str]:
    if role not in {"web", "worker"}:
        raise ValueError("Unknown fixture role")
    return {"PATH": os.environ.get("PATH", ""), "HOME": str(work), "PYTHONPATH": str(WEB),
        "PYTHONDONTWRITEBYTECODE": "1", "PYTHONUNBUFFERED": "1", "PYTHONNOUSERSITE": "1",
        "DATABASE_URL": "sqlite:///" + (work / "fixture.sqlite3").as_posix(),
        "SEO_AGENT_DATA_DIR": str(work / "data"), "SEO_AGENT_RUNS_DIR": str(work / "runs"),
        "SEO_AGENT_STRICT_CONFIG": "true", "SEO_AGENT_SERVICE_MODE": role,
        "SEO_AGENT_DISABLE_WORKER": "true", "SEO_AGENT_WORKER_CONCURRENCY": "2",
        "SEO_AGENT_ENCRYPTION_KEY": "fixture-encryption-" + "e" * 40,
        "SEO_AGENT_SECRET_KEY": "fixture-session-" + "s" * 40 if role == "web" else "",
        "CRON_SECRET": "fixture-cron-" + "c" * 40 if role == "web" else "",
        "PUBLIC_BASE_URL": "https://preproduction.example.invalid" if role == "web" else "",
        "SEO_AGENT_JOBS_RETENTION_DAYS": "0", "SEO_AGENT_RUNS_RETENTION_DAYS": "0",
        "ANTHROPIC_API_KEY": "", "OPENAI_API_KEY": "", "SENTRY_DSN": "",
        "SEO_CORRECTION_AI_PROVIDER": "none", "AWS_EC2_METADATA_DISABLED": "true"}


def validate_child_environment(work: Path) -> None:
    for key in ("DATABASE_URL", "SEO_AGENT_DATA_DIR", "SEO_AGENT_RUNS_DIR"):
        if os.environ.get(key) != fixture_environment(work, "web")[key]:
            raise ValueError("Child is not confined to its owned fixture: " + key)


def _restrict_network(state: dict) -> None:
    connect, resolve = socket.socket.connect, socket.getaddrinfo

    def allowed(host):
        try:
            return host == "localhost" or ipaddress.ip_address(host).is_loopback
        except ValueError:
            return False

    def guard(host):
        if not allowed(host):
            state["external_attempts"] += 1
            raise RuntimeError("External networking forbidden in startup fixture")

    def local_connect(sock, address):
        guard(address[0])
        return connect(sock, address)

    def local_resolve(host, *args, **kwargs):
        guard(host)
        return resolve(host, *args, **kwargs)

    socket.socket.connect = local_connect
    socket.getaddrinfo = local_resolve


def _child(work: Path, role: str, fd: int | None) -> int:
    validate_child_environment(work)
    sys.path.insert(0, str(WEB))
    state = {"role": role, "initialized": [], "plan_restored": False, "external_attempts": 0}
    _restrict_network(state)
    from backend import app as m

    initialize = m._initialize_service
    os.environ["PLAN_CONFIG_JSON"] = "stale-fixture-value"

    def observe(**kwargs):
        initialize(**kwargs)
        state["initialized"].append(kwargs["service_mode"])
        state["plan_restored"] = os.environ.get("PLAN_CONFIG_JSON") == PLAN

    m._initialize_service = observe
    emitted = False

    def emit():
        nonlocal emitted
        if not emitted:
            state.update(worker_threads=len(m._WORKER_THREADS), stop_requested=m._WORKER_STOP.is_set(),
                pr_scheduler=m._PR_VERIF_STARTED, content_scheduler=m._CONTENU_AUTO_STARTED)
            print(MARKER + json.dumps(state), flush=True)
            emitted = True

    shutdown = m._shutdown

    def observe_shutdown():
        shutdown()
        emit()

    m._shutdown = observe_shutdown
    try:
        if role == "worker":
            from backend import worker_main
            return worker_main.main()
        if fd is None:
            raise ValueError("Web fixture needs its owned listening socket")
        import uvicorn

        with socket.fromfd(fd, socket.AF_INET, socket.SOCK_STREAM) as listener:
            if listener.getsockname()[0] != "127.0.0.1":
                raise ValueError("Web fixture must bind loopback")
            uvicorn.Server(uvicorn.Config(m.app, log_level="warning")).run(sockets=[listener])
        return 0
    finally:
        emit()


def _wait(predicate, process: subprocess.Popen, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("Fixture process exited before the expected state")
        if predicate():
            return
        time.sleep(0.1)
    raise TimeoutError("Fixture process did not reach the expected state")


def _stop(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


def run(work: Path) -> dict:
    if work.exists():
        raise ValueError("Use a new empty fixture directory")
    if os.name != "posix":
        raise ValueError("This process fixture requires POSIX signals and inherited sockets")
    if any((WEB.parent / name).exists() for name in (".env", ".env.local", ".env.gsc")):
        raise ValueError("Do not run the fixture alongside repository secret override files")
    work.mkdir(parents=True)
    (work / "data").mkdir()
    (work / "data/.env.local").write_text("PLAN_CONFIG_JSON=" + PLAN + "\n", encoding="utf-8")
    sources = lambda: {p: hashlib.sha256((WEB / p).read_text(encoding="utf-8").encode()).hexdigest() for p in SOURCES}
    before = sources()
    checks, children = [], []
    result = {"status": "unverified", "client_readiness_certified": False,
        "source_before": before, "checks": checks}

    def jobs():
        with sqlite3.connect(work / "fixture.sqlite3") as db:
            return db.execute("SELECT id,status,attempts,worker_id FROM jobs ORDER BY id").fetchall()

    def seed(round_number):
        now = time.time()
        with sqlite3.connect(work / "fixture.sqlite3") as db:
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("INSERT OR IGNORE INTO users (id,email,password_hash,is_admin) VALUES (?,?,?,?)",
                ("startup-fixture-owner", "startup-fixture@example.invalid", "not-a-login-hash", 0))
            for idx in range(3):
                db.execute("INSERT INTO jobs (id,status,kind,owner_user_id,slug,created_at,updated_at,result,attempts,max_attempts) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)", (f"fixture-{round_number}-{idx}", "queued", "fixture",
                    "startup-fixture-owner", "", now, now,
                    json.dumps({"type": "unsupported_startup_fixture"}), 0, 1))

    def launch(role, name, *, invalid=False, listener=None):
        env = fixture_environment(work, role)
        if invalid:
            env["SEO_AGENT_ENCRYPTION_KEY"] = "change_me"
        args = [sys.executable, str(Path(__file__).resolve()), "--workdir", str(work), "--child", role]
        if listener is not None:
            args.extend(["--fd", str(listener.fileno())])
        log = (work / (name + ".log")).open("w", encoding="utf-8")
        try:
            process = subprocess.Popen(args, cwd=WEB, env=env, stdout=log, stderr=subprocess.STDOUT,
                pass_fds=() if listener is None else (listener.fileno(),))
        finally:
            log.close()
        children.append(process)
        return process

    def finish(process, name, role, *, invalid=False):
        _stop(process)
        log = (work / (name + ".log")).read_text(encoding="utf-8")
        marks = [json.loads(line[len(MARKER):]) for line in log.splitlines() if line.startswith(MARKER)]
        if len(marks) != 1 or marks[0]["external_attempts"]:
            raise RuntimeError("Missing fixture evidence or external request attempted")
        mark = marks[0]
        if invalid:
            assert process.returncode != 0 and mark["initialized"] == [] and mark["worker_threads"] == 0
            assert "Invalid production configuration" in log
        else:
            expected_code = -signal.SIGTERM if role == "web" else 0
            assert process.returncode == expected_code and mark["initialized"] == [role] and mark["plan_restored"]
            assert mark["worker_threads"] == (2 if role == "worker" else 0)
            assert mark["stop_requested"]
            assert mark["pr_scheduler"] is (role == "web") and mark["content_scheduler"] is (role == "web")
        checks.append({"name": name, "exit_code": process.returncode, **mark,
            "log_sha256": hashlib.sha256((work / (name + ".log")).read_bytes()).hexdigest()})

    try:
        with (work / "migration.log").open("w", encoding="utf-8") as log:
            subprocess.run([sys.executable, "-m", "alembic", "-c", "alembic.ini", "upgrade", "head"],
                cwd=WEB, env=fixture_environment(work, "web"), stdout=log, stderr=subprocess.STDOUT,
                check=True, timeout=60)
        for round_number in (1, 2):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
                web = launch("web", f"web-{round_number}", listener=listener)

            def healthy():
                try:
                    with urlopen(f"http://127.0.0.1:{port}/healthz", timeout=1) as response:
                        return response.status == 200 and json.load(response) == {"status": "ok"}
                except OSError:
                    return False

            _wait(healthy, web)
            finish(web, f"web-{round_number}", "web")
            seed(round_number)
            if round_number == 1:
                invalid = launch("worker", "worker-invalid", invalid=True)
                invalid.wait(timeout=30)
                finish(invalid, "worker-invalid", "worker", invalid=True)
                assert all(row[1] == "queued" and row[2] == 0 for row in jobs())
            worker = launch("worker", f"worker-{round_number}")
            _wait(lambda: all(row[1] == "failed" and row[2] == 1 and row[3] for row in jobs()), worker)
            finish(worker, f"worker-{round_number}", "worker")
        assert len(jobs()) == 6 and sources() == before
        with sqlite3.connect(work / "fixture.sqlite3") as db:
            revision = db.execute("SELECT version_num FROM alembic_version").fetchall()
        from alembic.config import Config
        from alembic.script import ScriptDirectory

        config = Config(str(WEB / "alembic.ini"))
        config.set_main_option("script_location", str(WEB / "alembic"))
        head = ScriptDirectory.from_config(config).get_current_head()
        assert revision == [(head,)]
        result.update(status="passed_owned_linux_process_smoke", checks=checks, migration_revision=head,
            synthetic_unsupported_jobs_processed_once=6, source_after=sources(),
            shutdown_scope="SIGTERM after synthetic jobs finish, not interrupted real crawl recovery",
            docker_or_render_exercised=False, postgres_or_customer_capacity_certified=False,
            github_provider_or_payment_requests=0)
    finally:
        for process in children:
            _stop(process)
        result["all_children_stopped"] = all(p.poll() is not None for p in children)
        (work / "result.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--child", choices=("web", "worker"), help=argparse.SUPPRESS)
    parser.add_argument("--fd", type=int, help=argparse.SUPPRESS)
    args = parser.parse_args()
    work = args.workdir.resolve()
    if args.child:
        return _child(work, args.child, args.fd)
    result = run(work)
    print(json.dumps({k: result[k] for k in ("status", "all_children_stopped", "client_readiness_certified")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
