"""Isolated API worker for the cross-process quota regression test."""

from __future__ import annotations

import json
import base64
import os
import sys
import time
from pathlib import Path

from sqlalchemy.engine import make_url


def main():
    url = make_url(os.environ["DATABASE_URL"])
    assert url.get_backend_name() == "postgresql"
    assert url.host in {"127.0.0.1", "localhost", "::1"}
    assert (url.database or "").startswith("seo_corrector_test")
    assert os.environ.get("SEO_AGENT_DISABLE_WORKER") == "true"
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

    from fastapi.testclient import TestClient
    from backend import app as m, auth, billing

    user_id, slug, gate_path, ready_path = sys.argv[1:]
    old = '<!doctype html><html><head></head><body><h1>Fixture</h1></body></html>'
    new = old.replace("</head>", "<title>A complete title for this fixture page</title></head>")
    calls = 0

    def model(**kwargs):
        nonlocal calls
        calls += 1
        time.sleep(0.5)
        return {"patched_content": new, "pr_title": "fix: fixture title"}

    def unexpected(*args, **kwargs):
        raise AssertionError("The quota probe must not contact GitHub")

    def read_source(path, **kwargs):
        assert "/contents/" in path
        return {"content": base64.b64encode(old.encode()).decode()}

    m._plan_correction_cfg = lambda user, **kw: {
        "plan": "pro", "model": "claude-sonnet-4-6", "max_files": 20, "unlimited": False}
    billing.plan_limits = lambda *a, **kw: {"ai_corrections_month": 1}
    m._effective_user_connection_value = lambda **kw: ("test-token", "user")
    m._rate_limit_retry_after = lambda **kw: None
    m._github_find_seo_files = lambda *a, **kw: [{"path": "index.html", "content": old}]
    m._openai_generate_file_patch = model
    for verb in ("get", "post", "put", "delete"):
        setattr(m, "_github_api_" + verb, unexpected)
    m._github_api_get = read_source

    try:
        with TestClient(m.app) as client:
            client.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(
                user_id=user_id, secret=m._safe_env("SEO_AGENT_SECRET_KEY")))
            client.get("/projects")
            assert client.cookies.get(m._CSRF_COOKIE_NAME)
            Path(ready_path).touch()
            deadline = time.monotonic() + 40
            while not Path(gate_path).exists():
                if time.monotonic() > deadline:
                    raise TimeoutError("quota worker start barrier")
                time.sleep(0.02)
            response = client.post(f"/api/projects/{slug}/issues/missing_title/github-fix",
                                   json={"url": "https://fixture.test/", "crawl_ts": "20261002-090000"},
                                   headers={m._CSRF_HEADER_NAME: client.cookies.get(m._CSRF_COOKIE_NAME, "")})
            print("QUOTA_RESULT=" + json.dumps({"status": response.status_code, "model_calls": calls,
                                               "cached": response.json().get("cached", False)}))
    finally:
        m.DB.engine.dispose()
        m.DB.correction_engine.dispose()


if __name__ == "__main__":
    main()
