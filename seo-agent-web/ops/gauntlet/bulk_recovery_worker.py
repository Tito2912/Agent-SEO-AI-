"""Disposable authenticated bulk API process; no real provider call is permitted."""

from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import closing
from pathlib import Path
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ops.gauntlet import bulk_recovery_cycle as bench  # noqa: E402


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("workdir", type=Path)
    cli.add_argument("mode", choices=["bulk", "crash_charge"])
    args = cli.parse_args()
    for stream in (sys.stdout, sys.stderr):
        stream.reconfigure(encoding="utf-8", errors="replace")
    work = args.workdir.resolve()
    cfg = json.loads((work / "worker.json").read_text(encoding="utf-8"))
    relay = urlsplit(cfg["relay"])
    if (relay.scheme != "http" or relay.hostname != "127.0.0.1" or not relay.port
            or relay.path or relay.query or relay.fragment or relay.username or relay.password):
        raise ValueError("Only a loopback fixture relay is allowed")
    m, token = bench.load_backend(work, cfg["database_url"])
    from ops.gauntlet.ai_budget import ClaudeBudget
    budget = ClaudeBudget(0)
    budget.install(m)
    from backend import auth, billing, correction_journal
    from backend.models import Project, User
    from fastapi.testclient import TestClient

    with m.DB.session() as db:
        user, project = db.get(User, cfg["payer"]), db.get(Project, cfg["project"])
        if (user is None or user.is_admin or not user.email.startswith("bulk-recovery-")
                or not user.email.endswith("@example.invalid") or project is None
                or project.owner_user_id != user.id or project.slug != cfg["slug"]
                or project.settings != bench.SETTINGS):
            raise ValueError("Only the synthetic fixture account is allowed")
    original_url = m._github_api_url

    def scoped_url(path):
        original_url(path)
        bench.scoped_path(path)
        return cfg["relay"] + path

    m._github_api_url = scoped_url
    m._plan_correction_cfg = lambda user, **kw: {
        "plan": "pro", "model": "fixture-stub", "max_files": 3, "unlimited": False}
    billing.plan_limits = lambda *a, **kw: {"ai_corrections_month": 3}
    m._effective_user_connection_value = lambda **kw: (token, "user")
    m._rate_limit_retry_after = lambda **kw: None

    def unexpected(*a, **kw):
        raise RuntimeError("Real provider calls and heuristic targeting are forbidden in this bench")

    m._correction_ai_json = unexpected
    m._ai_pick_repo_files = unexpected
    m._ai_map_urls_to_files = unexpected
    m._github_code_search_paths = unexpected
    m._github_tarball_grep = unexpected
    calls = 0

    def model(**kw):
        nonlocal calls
        path = kw["file_path"]
        if path not in cfg["sources"] or kw["file_content"] != cfg["sources"][path]["original"]:
            raise ValueError("Unrecognized fixture source")
        expected = "missing_title" if path.endswith("missing-title.html") else "duplicate_titles"
        if kw["issue_key"] != expected or calls >= 3:
            raise ValueError("Fixture model scope or budget violation")
        calls += 1
        print("BULK_MODEL_CALL=1", flush=True)
        return {"patched_content": cfg["sources"][path]["patched"]}

    m._openai_generate_file_patch = model
    if args.mode == "crash_charge":
        charge = correction_journal.Operation.charge

        def crash_after_commit(self, callback):
            charge(self, callback)
            if self.row.action == "api_github_bulk_fix" and self.row.charged:
                print("BULK_CRASH=committed_charge", flush=True)
                os._exit(24)

        correction_journal.Operation.charge = crash_after_commit
    with closing(TestClient(m.app)) as client:
        client.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(
            user_id=cfg["payer"], secret=m._safe_env("SEO_AGENT_SECRET_KEY")))
        client.get("/projects")
        response = client.post(f"/api/projects/{cfg['slug']}/github/bulk-fix",
                               headers={m._CSRF_HEADER_NAME: client.cookies.get(m._CSRF_COOKIE_NAME, "")})
        data = response.json()
        with m.DB.session() as db:
            used = billing.usage_sum(db, user_id=cfg["payer"], metric="ai_corrections_month")
        result = {"status": response.status_code, "used": used, "ai_budget": budget.summary(),
                  **{k: data[k] for k in ("ok", "recovered", "recovery_required", "duplicate", "partial",
                                         "pr_number", "pr_url", "branch", "fixed_count", "total_count") if k in data}}
        print("BULK_RESULT=" + json.dumps(result, ensure_ascii=True), flush=True)
    m.DB.engine.dispose()
    if getattr(m.DB, "correction_engine", m.DB.engine) is not m.DB.engine:
        m.DB.correction_engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
