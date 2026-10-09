"""Disposable authenticated API process for the owned-fixture recovery bench."""

from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import closing
from pathlib import Path
from urllib.parse import urlsplit

WEB_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(WEB_ROOT))

from ops.gauntlet.live_cycle import _load_backend  # noqa: E402


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("workdir", type=Path)
    cli.add_argument("mode", choices=["crash_preview", "preview", "confirm"])
    args = cli.parse_args()
    workdir = args.workdir.resolve()
    cfg = json.loads((workdir / "worker.json").read_text(encoding="utf-8"))
    relay = urlsplit(cfg["relay"])
    if (relay.scheme != "http" or relay.hostname != "127.0.0.1" or not relay.port
            or relay.path or relay.query or relay.fragment or relay.username or relay.password):
        raise ValueError("Only a loopback fixture relay is allowed")
    m, token = _load_backend(workdir)
    from fastapi.testclient import TestClient
    from backend import auth, billing, correction_journal
    from backend.models import Project, User

    with m.DB.session() as db:
        user = db.get(User, cfg["payer"])
        project = db.get(Project, cfg["project"])
        if (user is None or user.is_admin or not user.email.startswith("recovery-")
                or not user.email.endswith("@example.invalid") or project is None
                or project.owner_user_id != user.id or project.slug != cfg["slug"]
                or project.settings != {"github_repo": "pployeraffiliation-a11y/noyaru-stack-static-html",
                                        "github_branch": "main", "github_mode": "review"}):
            raise ValueError("Only the synthetic fixture account is allowed")
    api_url = m._github_api_url
    def scoped_url(path):
        api_url(path)
        if not path.startswith("/repos/pployeraffiliation-a11y/noyaru-stack-static-html/"):
            raise ValueError("Fixture API scope violation")
        return cfg["relay"] + path
    m._github_api_url = scoped_url
    m._plan_correction_cfg = lambda user, **kw: {
        "plan": "pro", "model": "fixture-stub", "max_files": 1, "unlimited": False}
    billing.plan_limits = lambda *a, **kw: {"ai_corrections_month": 1}
    m._effective_user_connection_value = lambda **kw: (token, "user")
    m._rate_limit_retry_after = lambda **kw: None
    m._github_find_seo_files = lambda *a, **kw: [{"path": cfg["file"], "content": cfg["original"]}]
    def model(**kwargs):
        print("RECOVERY_MODEL_CALL=1", flush=True)
        return {"patched_content": cfg["patched"], "pr_title": "QA recovery fixture (never merge)"}
    m._openai_generate_file_patch = model
    if args.mode == "crash_preview":
        finish = correction_journal.Operation.finish
        def crash_after_charge(self, response):
            if 200 <= response.status_code < 300 and self.row.charged and self.row.data.get("preview"):
                print("RECOVERY_CRASH=paid_preview", flush=True)
                os._exit(23)
            return finish(self, response)
        correction_journal.Operation.finish = crash_after_charge
    # The parent created the schema; do not run production startup or scheduler threads.
    with closing(TestClient(m.app)) as client:
        client.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(
            user_id=cfg["payer"], secret=m._safe_env("SEO_AGENT_SECRET_KEY")))
        client.get("/projects")
        body = {"url": cfg["url"], "crawl_ts": "fixture-recovery"}
        if args.mode == "confirm":
            body.update(confirm=True, file_path=cfg["file"], patched_content=cfg["patched"])
        response = client.post(f"/api/projects/{cfg['slug']}/issues/missing_title/github-fix", json=body,
                               headers={m._CSRF_HEADER_NAME: client.cookies.get(m._CSRF_COOKIE_NAME, "")})
        data = response.json()
        with m.DB.session() as db:
            used = billing.usage_sum(db, user_id=cfg["payer"], metric="ai_corrections_month")
        result = {"status": response.status_code, "used": used,
                  **{key: data[key] for key in ("ok", "cached", "recovered", "recovery_required", "duplicate",
                                               "pr_url", "pr_number", "branch") if key in data}}
        print("RECOVERY_RESULT=" + json.dumps(result, ensure_ascii=True), flush=True)
    m.DB.engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
