from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pytest
from fastapi import HTTPException
from starlette.requests import Request


WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-test-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as app_module  # noqa: E402
from backend import object_store  # noqa: E402
from backend.app import app  # noqa: E402


def test_public_routes_and_auth_redirect() -> None:
    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200
        assert client.get("/").status_code == 200

        response = client.get("/settings/accounts", follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["location"] == "/auth/login?next=/settings/accounts"

        response = client.get("/settings/operations", follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["location"] == "/auth/login?next=/settings/operations"


def test_csrf_blocks_unsafe_request_without_token() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/auth/login",
            data={"email": "nobody@example.com", "password": "bad-password"},
            follow_redirects=False,
        )
    assert response.status_code == 403


def test_cors_is_strict_when_public_base_url_is_configured() -> None:
    with TestClient(app) as client:
        denied = client.options(
            "/healthz",
            headers={"Origin": "http://evil.test", "Access-Control-Request-Method": "GET"},
        )
        assert denied.status_code == 403
        assert "access-control-allow-origin" not in denied.headers

        allowed = client.options(
            "/healthz",
            headers={"Origin": "http://testserver", "Access-Control-Request-Method": "GET"},
        )
        assert allowed.status_code == 204
        assert allowed.headers["access-control-allow-origin"] == "http://testserver"


def test_cors_stays_disabled_when_public_base_url_is_missing() -> None:
    previous = os.environ.pop("PUBLIC_BASE_URL", None)
    try:
        with TestClient(app) as client:
            response = client.options(
                "/healthz",
                headers={"Origin": "http://any.test", "Access-Control-Request-Method": "GET"},
            )
        assert response.status_code == 403
        assert "access-control-allow-origin" not in response.headers
    finally:
        if previous is not None:
            os.environ["PUBLIC_BASE_URL"] = previous


def test_security_headers_include_csp_and_trust_proxy_for_hsts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEO_AGENT_CSP_ENABLED", "true")
    monkeypatch.setenv("SEO_AGENT_CSP_REPORT_ONLY", "false")
    monkeypatch.delenv("SEO_AGENT_CSP", raising=False)
    monkeypatch.setenv("SEO_AGENT_TRUST_PROXY_HEADERS", "false")
    monkeypatch.delenv("RENDER", raising=False)
    monkeypatch.delenv("RENDER_SERVICE_NAME", raising=False)

    with TestClient(app) as client:
        untrusted = client.get("/healthz", headers={"x-forwarded-proto": "https"})

    assert untrusted.headers["content-security-policy"].startswith("default-src 'self'")
    assert "frame-ancestors 'none'" in untrusted.headers["content-security-policy"]
    assert "strict-transport-security" not in untrusted.headers

    monkeypatch.setenv("SEO_AGENT_TRUST_PROXY_HEADERS", "true")
    with TestClient(app) as client:
        trusted = client.get("/healthz", headers={"x-forwarded-proto": "https"})

    assert trusted.headers["strict-transport-security"] == "max-age=31536000; includeSubDomains"


def test_cron_routes_reach_cron_auth_without_login_redirect() -> None:
    cron_routes = [
        "/cron/check-backlinks",
        "/cron/autopilot",
        "/cron/auto-search-backlinks",
        "/cron/auto-post-backlinks",
    ]
    with TestClient(app) as client:
        for route in cron_routes:
            response = client.get(route, follow_redirects=False)
            assert response.status_code == 401
            assert "location" not in response.headers


def test_operations_page_renders_for_system_owner() -> None:
    email = f"ops-{os.urandom(4).hex()}@example.com"
    with app_module.DB.session() as db:
        user = app_module.User(email=email, password_hash=app_module.auth.hash_password("test-password"), is_admin=True)
        db.add(user)
        db.commit()
        user_id = str(user.id)

    token = app_module.auth.make_session_token(user_id=user_id, secret=os.environ["SEO_AGENT_SECRET_KEY"])
    with TestClient(app) as client:
        client.cookies.set(app_module.auth.SESSION_COOKIE_NAME, token)
        response = client.get("/settings/operations")

    assert response.status_code == 200
    assert "Exploitation prod" in response.text
    assert "Readiness ouverture" in response.text


def test_dashboard_onboarding_renders_for_authenticated_user() -> None:
    suffix = os.urandom(4).hex()
    email = f"onboarding-{suffix}@example.com"
    with app_module.DB.session() as db:
        user = app_module.User(email=email, password_hash=app_module.auth.hash_password("test-password"), is_admin=False)
        db.add(user)
        db.flush()
        db.add(
            app_module.Project(
                owner_user_id=str(user.id),
                slug=f"onboarding-{suffix}",
                base_url=f"https://onboarding-{suffix}.example.com",
                site_name="Onboarding Test",
            )
        )
        db.commit()
        user_id = str(user.id)

    token = app_module.auth.make_session_token(user_id=user_id, secret=os.environ["SEO_AGENT_SECRET_KEY"])
    with TestClient(app) as client:
        client.cookies.set(app_module.auth.SESSION_COOKIE_NAME, token)
        response = client.get("/")

    assert response.status_code == 200
    assert "Prochaines étapes" in response.text
    assert "Lancer un premier crawl" in response.text


def test_corrections_page_renders_accelerator_for_authenticated_user() -> None:
    suffix = os.urandom(4).hex()
    email = f"corrections-{suffix}@example.com"
    slug = f"corrections-{suffix}"
    with app_module.DB.session() as db:
        user = app_module.User(email=email, password_hash=app_module.auth.hash_password("test-password"), is_admin=False)
        db.add(user)
        db.flush()
        db.add(
            app_module.Project(
                owner_user_id=str(user.id),
                slug=slug,
                base_url=f"https://{slug}.example.com",
                site_name="Corrections Test",
            )
        )
        db.commit()
        user_id = str(user.id)

    token = app_module.auth.make_session_token(user_id=user_id, secret=os.environ["SEO_AGENT_SECRET_KEY"])
    with TestClient(app) as client:
        client.cookies.set(app_module.auth.SESSION_COOKIE_NAME, token)
        response = client.get(f"/projects/{slug}/corrections")

    assert response.status_code == 200
    assert "Accélérateur de correction" in response.text
    assert "Connecter GitHub" in response.text


def test_strict_config_rejects_weak_or_missing_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEO_AGENT_STRICT_CONFIG", "true")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("PUBLIC_BASE_URL", "http://localhost:8000")
    monkeypatch.setenv("SEO_AGENT_SECRET_KEY", "change_me")
    monkeypatch.delenv("SEO_AGENT_ENCRYPTION_KEY", raising=False)
    monkeypatch.delenv("SEO_AGENT_ENCRYPTION_KEYS", raising=False)
    monkeypatch.delenv("CRON_SECRET", raising=False)

    with pytest.raises(RuntimeError, match="Invalid production configuration"):
        app_module._validate_startup_config()


def test_strict_config_accepts_required_production_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEO_AGENT_STRICT_CONFIG", "true")
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@example.com:5432/db")
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://app.example.com")
    monkeypatch.setenv("SEO_AGENT_SECRET_KEY", "session-secret-0123456789abcdef0123456789")
    monkeypatch.setenv("SEO_AGENT_ENCRYPTION_KEY", "encryption-secret-0123456789abcdef0123")
    monkeypatch.delenv("SEO_AGENT_ENCRYPTION_KEYS", raising=False)
    monkeypatch.setenv("CRON_SECRET", "cron-secret-0123456789abcdef0123456789")

    app_module._validate_startup_config()


def test_proxy_ip_headers_are_ignored_unless_trusted(monkeypatch: pytest.MonkeyPatch) -> None:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [(b"x-forwarded-for", b"198.51.100.10"), (b"x-real-ip", b"198.51.100.11")],
        "client": ("203.0.113.20", 12345),
        "scheme": "http",
    }
    request = Request(scope)

    monkeypatch.setenv("SEO_AGENT_TRUST_PROXY_HEADERS", "false")
    monkeypatch.delenv("RENDER", raising=False)
    monkeypatch.delenv("RENDER_SERVICE_NAME", raising=False)
    assert app_module._request_client_ip(request) == "203.0.113.20"
    assert app_module._request_is_secure(request) is False

    monkeypatch.setenv("SEO_AGENT_TRUST_PROXY_HEADERS", "true")
    scope["headers"] = [
        (b"x-forwarded-for", b"198.51.100.10, 203.0.113.30"),
        (b"x-forwarded-proto", b"https"),
    ]
    trusted_request = Request(scope)
    assert app_module._request_client_ip(trusted_request) == "198.51.100.10"
    assert app_module._request_is_secure(trusted_request) is True


def test_public_base_url_only_uses_forwarded_headers_when_trusted(monkeypatch: pytest.MonkeyPatch) -> None:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [
            (b"host", b"internal.test"),
            (b"x-forwarded-host", b"public.example.com"),
            (b"x-forwarded-proto", b"https"),
        ],
        "client": ("203.0.113.20", 12345),
        "scheme": "http",
    }
    request = Request(scope)

    monkeypatch.delenv("PUBLIC_BASE_URL", raising=False)
    monkeypatch.delenv("RENDER", raising=False)
    monkeypatch.delenv("RENDER_SERVICE_NAME", raising=False)
    monkeypatch.setenv("SEO_AGENT_TRUST_PROXY_HEADERS", "false")
    assert app_module._public_base_url(request) == "http://internal.test"

    monkeypatch.setenv("SEO_AGENT_TRUST_PROXY_HEADERS", "true")
    assert app_module._public_base_url(request) == "https://public.example.com"


def test_non_admin_config_path_is_limited_to_default() -> None:
    request = Request({"type": "http", "method": "POST", "path": "/", "headers": [], "client": ("127.0.0.1", 1)})
    request.state.user = type("User", (), {"is_admin": False})()

    default_path = app_module._resolve_request_config_path(request, str(app_module.DEFAULT_CONFIG))
    assert default_path == app_module.DEFAULT_CONFIG.resolve()

    with pytest.raises(HTTPException) as exc:
        app_module._resolve_request_config_path(request, "/etc/passwd")
    assert exc.value.status_code == 403


def test_env_bool_default_keeps_default_for_invalid_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEO_AGENT_CSP_ENABLED", "maybe")
    assert app_module._csp_enabled() is True

    monkeypatch.setenv("SEO_AGENT_TRUST_PROXY_HEADERS", "maybe")
    monkeypatch.delenv("RENDER", raising=False)
    monkeypatch.delenv("RENDER_SERVICE_NAME", raising=False)
    assert app_module._trust_proxy_headers() is False


def test_public_target_validation_rejects_non_http_and_local_hosts() -> None:
    assert "Schéma non autorisé" in (app_module._validate_public_crawl_target("ftp://example.com") or "")
    assert "localhost" in (app_module._validate_public_crawl_target("http://localhost/") or "")
    assert "adresse IP" in (app_module._validate_public_crawl_target("http://127.0.0.1/") or "")


def test_csv_exports_escape_formula_like_values() -> None:
    content = app_module._csv_bytes([{"title": "=cmd|calc", "count": 2}], fieldnames=["title", "count"])
    assert b"'=cmd|calc" in content
    assert b",2" in content


def test_next_path_rejects_external_and_backslash_variants() -> None:
    assert app_module._safe_next_path("https://evil.example/") == "/"
    assert app_module._safe_next_path("//evil.example/") == "/"
    assert app_module._safe_next_path("/\\evil.example/") == "/"
    assert app_module._safe_next_path("/projects/demo?tab=issues") == "/projects/demo?tab=issues"


def test_s3_restore_tree_keeps_downloads_under_runs_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls: list[Path] = []

    class DummyClient:
        def download_file(self, _bucket: str, _key: str, filename: str) -> None:
            calls.append(Path(filename).resolve())

    runs_dir = tmp_path / "runs"
    monkeypatch.setattr(object_store, "_s3_client", lambda: DummyClient())
    monkeypatch.setattr(object_store, "s3_bucket_name", lambda: "bucket")
    monkeypatch.setattr(object_store, "s3_prefix", lambda: "seo-runs")
    monkeypatch.setattr(
        object_store,
        "_iter_object_keys",
        lambda prefix: ["seo-runs/project/report.json", "seo-runs/../escape.txt"],
    )

    assert object_store.restore_runs_tree(runs_dir, runs_dir / "project") is True
    assert calls == [(runs_dir / "project" / "report.json").resolve()]


def test_system_setting_value_validation_rejects_risky_values(tmp_path: Path) -> None:
    assert "trop faible" in (
        app_module._validate_settings_env_value("SEO_AGENT_SECRET_KEY", "change_me") or ""
    )
    assert "booléenne" in (
        app_module._validate_settings_env_value("SEO_AGENT_CSP_ENABLED", "maybe") or ""
    )
    assert "identifiants" in (
        app_module._validate_settings_env_value("PUBLIC_BASE_URL", "https://user:pass@example.com") or ""
    )
    assert "Chemin JSON refusé" in (
        app_module._validate_settings_env_value("GOOGLE_APPLICATION_CREDENTIALS", "/etc/passwd") or ""
    )

    allowed_json = app_module.DATA_DIR / "service-account.json"
    assert app_module._validate_settings_env_value("GOOGLE_APPLICATION_CREDENTIALS", str(allowed_json)) is None


def test_file_view_max_bytes_is_clamped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEO_AGENT_FILE_VIEW_MAX_BYTES", "1")
    assert app_module._file_view_max_bytes() == 64 * 1024

    monkeypatch.setenv("SEO_AGENT_FILE_VIEW_MAX_BYTES", str(100 * 1024 * 1024))
    assert app_module._file_view_max_bytes() == 20 * 1024 * 1024

    monkeypatch.setenv("SEO_AGENT_CSRF_BODY_MAX_BYTES", "1")
    assert app_module._csrf_body_max_bytes() == 64 * 1024

    monkeypatch.setenv("SEO_AGENT_CSRF_BODY_MAX_BYTES", str(100 * 1024 * 1024))
    assert app_module._csrf_body_max_bytes() == 50 * 1024 * 1024


def test_github_path_helpers_reject_ambiguous_values() -> None:
    assert app_module._github_repo_parts("owner/repo") == ("owner", "repo")
    assert app_module._github_repo_parts("owner/repo/extra") is None

    assert app_module._github_branch_allowed("feature/seo-fix-1")
    assert not app_module._github_branch_allowed("../main")
    assert not app_module._github_branch_allowed("feature//seo")

    assert app_module._github_file_path_allowed("src/templates/index.html")
    assert not app_module._github_file_path_allowed("../secret")
    assert not app_module._github_file_path_allowed("src\\secret")

    assert app_module._github_api_path("repos", "owner", "repo", "contents", "a b.html") == (
        "/repos/owner/repo/contents/a%20b.html"
    )


def test_github_patched_content_has_size_limit() -> None:
    assert app_module._github_patched_content_error("valid content") is None
    assert "manquant" in (app_module._github_patched_content_error("") or "")
    assert "volumineux" in (
        app_module._github_patched_content_error("x" * (app_module._GITHUB_MAX_PATCHED_CONTENT_BYTES + 1)) or ""
    )


def test_github_fixable_issue_detection_covers_common_audit_keys() -> None:
    assert app_module._github_issue_auto_fixable("duplicate_titles")
    assert app_module._github_issue_auto_fixable("title_too_long_indexable")
    assert app_module._github_issue_auto_fixable("http_404")
    assert app_module._github_issue_auto_fixable("missing_alt_text")
    assert not app_module._github_issue_auto_fixable("gsc_indexing_errors")

    title_candidates = app_module._seo_file_candidates_for_issue("duplicate_titles")
    assert "app/layout.tsx" in title_candidates

    redirect_candidates = app_module._seo_file_candidates_for_issue("http_404")
    assert "netlify.toml" in redirect_candidates


def test_github_fixable_issue_candidates_are_prioritized() -> None:
    proj = type(
        "ProjectCtx",
        (),
        {
            "slug": "demo",
            "site_name": "Demo",
            "base_url": "https://example.com",
        },
    )()
    report = {
        "meta": {"pages_crawled": 1},
        "pages": [{"url": "https://example.com/a", "status_code": 404}],
        "issues": {
            "missing_title": {"count": 2, "examples": ["https://example.com/a"]},
            "gsc_indexing_errors": {"count": 9, "examples": ["https://example.com/gsc"]},
            "http_404": {"count": 1, "examples": ["https://example.com/missing"]},
        },
    }

    candidates = app_module._github_fixable_issue_candidates(report=report, proj=proj, limit=10)

    assert [c["key"] for c in candidates] == ["http_404", "missing_title"]
    assert candidates[0]["url"] == "https://example.com/missing"
    assert candidates[0]["priority"] == "high"


def test_provider_api_url_helpers_reject_ambiguous_paths() -> None:
    assert app_module._github_api_url("/user") == "https://api.github.com/user"
    assert app_module._netlify_api_url("/api/v1/sites") == "https://api.netlify.com/api/v1/sites"

    with pytest.raises(RuntimeError):
        app_module._github_api_url("//evil.test/path")
    with pytest.raises(RuntimeError):
        app_module._netlify_api_url("https://evil.test/path")


def test_operations_snapshot_does_not_expose_secret_values(monkeypatch: pytest.MonkeyPatch) -> None:
    session_secret = "session-secret-0123456789abcdef0123456789"
    encryption_secret = "encryption-secret-0123456789abcdef0123"
    cron_secret = "cron-secret-0123456789abcdef0123456789"
    monkeypatch.setenv("SEO_AGENT_SECRET_KEY", session_secret)
    monkeypatch.setenv("SEO_AGENT_ENCRYPTION_KEY", encryption_secret)
    monkeypatch.setenv("CRON_SECRET", cron_secret)

    snapshot = app_module._production_operations_snapshot()
    serialized = repr(snapshot)

    assert session_secret not in serialized
    assert encryption_secret not in serialized
    assert cron_secret not in serialized
    assert "checks" in snapshot
