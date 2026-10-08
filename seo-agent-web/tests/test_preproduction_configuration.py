"""Preproduction must use new resources and opt-in guards, not production credentials."""

import json
import os
from pathlib import Path
import sys
import tempfile

import pytest
import yaml

WEB = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB))
SCRATCH = Path(tempfile.mkdtemp(prefix="seo-preproduction-tests-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(SCRATCH / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(SCRATCH / "runs"))
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
from backend import app as m  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402


@pytest.mark.parametrize("disabled", [None, "false", "true", "1", "yes"])
def test_scheduler_switch_only_changes_web_schedulers(monkeypatch, disabled):
    calls = []
    if disabled is None:
        monkeypatch.delenv("SEO_AGENT_DISABLE_SCHEDULERS", raising=False)
    else:
        monkeypatch.setenv("SEO_AGENT_DISABLE_SCHEDULERS", disabled)
    monkeypatch.setattr(m, "_initialize_service", lambda **kw: calls.append(("init", kw)))
    for name in ("_start_job_worker", "_start_retention", "_start_verification_pr", "_start_contenu_auto"):
        monkeypatch.setattr(m, name, lambda name=name: calls.append(name))
    m._startup()
    expected = [("init", {"service_mode": "web"}), "_start_job_worker", "_start_retention"]
    if disabled in {None, "false"}:
        expected += ["_start_verification_pr", "_start_contenu_auto"]
    assert calls == expected


@pytest.mark.parametrize("method,path,status", [
    ("GET", "/", 200), ("GET", "/docs", 200), ("HEAD", "/docs", 200),
    ("GET", "/robots.txt", 200), ("GET", "/sitemap.xml", 200),
    ("GET", "/healthz", 200), ("GET", "/docs/nonexistent-preproduction-page", 404),
    ("GET", "/api/settings/operations", 401), ("GET", "/settings/operations", 303),
    ("POST", "/auth/login", 403),
])
def test_noindex_covers_public_handled_errors_and_early_auth_responses(monkeypatch, method, path, status):
    monkeypatch.setenv("SEO_AGENT_NOINDEX", "true")
    monkeypatch.setenv("PUBLIC_BASE_URL", "http://testserver")
    monkeypatch.delenv("BETA_BASIC_AUTH_USER", raising=False)
    monkeypatch.delenv("BETA_BASIC_AUTH_PASS", raising=False)
    monkeypatch.setattr(m, "_load_user_from_session", lambda request: None)
    response = TestClient(m.app).request(method, path, follow_redirects=False)
    assert response.status_code == status
    assert response.headers["x-robots-tag"] == "noindex, nofollow"


@pytest.mark.parametrize("flag", [None, "false"])
def test_production_default_does_not_add_noindex(monkeypatch, flag):
    if flag is None:
        monkeypatch.delenv("SEO_AGENT_NOINDEX", raising=False)
    else:
        monkeypatch.setenv("SEO_AGENT_NOINDEX", flag)
    monkeypatch.setattr(m, "_load_user_from_session", lambda request: None)
    assert "x-robots-tag" not in TestClient(m.app).get("/healthz").headers


@pytest.fixture
def blueprint():
    data = yaml.safe_load((WEB.parent / "render.preproduction.yaml").read_text(encoding="utf-8"))
    services = {s["type"]: s for s in data["services"]}
    group = data["envVarGroups"][0]
    shared = {e["key"]: e for e in group["envVars"]}
    return data, services, group, shared


def test_resources_are_new_and_only_manually_deployed(blueprint):
    data, services, group, shared = blueprint
    production = yaml.safe_load((WEB.parent / "render.yaml").read_text(encoding="utf-8"))
    old_names = {r["name"] for k in ("services", "databases", "envVarGroups") for r in production.get(k, [])}
    new_names = [r["name"] for k in ("services", "databases", "envVarGroups") for r in data.get(k, [])]
    assert len(new_names) == len(set(new_names)) and not set(new_names) & old_names
    assert all("preproduction" in name for name in new_names)
    assert set(services) == {"web", "worker"} and len(data["databases"]) == 1
    assert data["previews"]["generation"] == "off" and "projects" not in data
    for service in services.values():
        assert service["runtime"] == "docker" and service["dockerfilePath"] == "./Dockerfile"
        assert service["branch"] == "codex/corrector-recovery-20261003"
        assert service["autoDeployTrigger"] == "off" and service["numInstances"] == 1
        assert not any(k in service for k in ("domains", "schedule", "scaling", "initialDeployHook"))
        assert service["envVars"].count({"fromGroup": group["name"]}) == 1


def test_both_roles_use_only_the_new_database_and_shared_encryption_seed(blueprint):
    data, services, group, shared = blueprint
    database = data["databases"][0]
    assert database["ipAllowList"] == [] and database["connectionPool"] == "none"
    assert shared["SEO_AGENT_ENCRYPTION_KEY"] == {"key": "SEO_AGENT_ENCRYPTION_KEY", "generateValue": True}
    assert shared["SEO_AGENT_ENCRYPTION_KEYS"]["value"] == ""
    assert json.loads(shared["PLAN_CONFIG_JSON"]["value"]) == {}
    for role, service in services.items():
        env = {e["key"]: e for e in service["envVars"] if "key" in e}
        assert env["DATABASE_URL"]["fromDatabase"] == {"name": database["name"], "property": "connectionString"}
        assert env["SEO_AGENT_SERVICE_MODE"]["value"] == role
        assert "SEO_AGENT_ENCRYPTION_KEY" not in env and "PLAN_CONFIG_JSON" not in env
    web = {e["key"]: e for e in services["web"]["envVars"] if "key" in e}
    assert web["PUBLIC_BASE_URL"]["fromService"] == {
        "type": "web", "name": services["web"]["name"], "envVarKey": "RENDER_EXTERNAL_URL"}
    assert services["web"]["region"] == services["worker"]["region"] == database["region"]


@pytest.mark.parametrize("key", ["ANTHROPIC_API_KEY", "OPENAI_API_KEY", "GOOGLE_GEMINI_API_KEY",
    "STRIPE_SECRET_KEY", "STRIPE_WEBHOOK_SECRET", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
    "S3_BUCKET_NAME", "SMTP_PASSWORD", "SENDGRID_API_KEY", "GITHUB_TOKEN"])
def test_external_credentials_are_blank_at_initial_boot(blueprint, key):
    assert blueprint[3][key] == {"key": key, "value": ""}


def test_initial_boot_guards_keep_accounts_and_automation_closed(blueprint):
    data, services, group, shared = blueprint
    for key in ("SEO_AGENT_STRICT_CONFIG", "SEO_AGENT_DISABLE_SCHEDULERS", "SEO_AGENT_NOINDEX"):
        assert shared[key]["value"] == "true"
    for key in ("SEO_CORRECTION_AI_PROVIDER", "SEO_AUDIT_ASSISTANT_PROVIDER"):
        assert shared[key]["value"] == "none"
    for key in ("SEO_AGENT_JOBS_RETENTION_DAYS", "SEO_AGENT_RUNS_RETENTION_DAYS"):
        assert shared[key]["value"] == "0"
    assert shared["SENTRY_ENVIRONMENT"]["value"] == "preproduction"
    assert shared["S3_PREFIX"]["value"] == "noyaru-preproduction/seo-runs"
    web = {e["key"]: e for e in services["web"]["envVars"] if "key" in e}
    assert web["SEO_AGENT_DISABLE_WORKER"]["value"] == web["SIGNUP_DISABLED"]["value"] == "true"
    assert web["BOOTSTRAP_ADMIN_EMAIL"] == {"key": "BOOTSTRAP_ADMIN_EMAIL", "sync": False}
    for key in ("SEO_AGENT_SECRET_KEY", "CRON_SECRET", "SIGNUP_INVITE_CODE", "BETA_BASIC_AUTH_PASS"):
        assert web[key] == {"key": key, "generateValue": True}
    assert services["worker"]["plan"] == "1c-2g" and "disk" not in services["worker"]
    assert services["web"]["disk"]["name"] == "noyaru-preproduction-data"
