"""The separate worker must initialize before consuming jobs, without web schedulers."""

import threading
from pathlib import Path

import pytest

from backend import app as m, worker_main


@pytest.fixture
def production(monkeypatch):
    monkeypatch.setenv("SEO_AGENT_STRICT_CONFIG", "true")
    monkeypatch.setenv("DATABASE_URL", "sqlite:///fixture.db")
    monkeypatch.setenv("SEO_AGENT_ENCRYPTION_KEY", "fixture-encryption-" + "e" * 40)
    monkeypatch.delenv("SEO_AGENT_ENCRYPTION_KEYS", raising=False)
    for key in ("PUBLIC_BASE_URL", "SEO_AGENT_SECRET_KEY", "CRON_SECRET"):
        monkeypatch.delenv(key, raising=False)


def test_strict_worker_requires_only_its_actual_secrets(production):
    m._validate_startup_config(service_mode="worker")


@pytest.mark.parametrize("variable,value", [("DATABASE_URL", ""), ("SEO_AGENT_ENCRYPTION_KEY", ""),
    ("SEO_AGENT_ENCRYPTION_KEY", "change_me"), ("SEO_AGENT_ENCRYPTION_KEY", "test-" + "e" * 40)])
def test_worker_refuses_missing_or_weak_required_configuration(production, monkeypatch, variable, value):
    monkeypatch.setenv(variable, value)
    with pytest.raises(RuntimeError, match=variable):
        m._validate_startup_config(service_mode="worker")


def test_worker_does_not_fall_back_to_the_session_secret(production, monkeypatch):
    monkeypatch.delenv("SEO_AGENT_ENCRYPTION_KEY")
    monkeypatch.setenv("SEO_AGENT_SECRET_KEY", "fixture-session-" + "s" * 40)
    with pytest.raises(RuntimeError, match="SEO_AGENT_ENCRYPTION_KEY"):
        m._validate_startup_config(service_mode="worker")


def test_worker_accepts_strong_rotation_seed(production, monkeypatch):
    monkeypatch.delenv("SEO_AGENT_ENCRYPTION_KEY")
    monkeypatch.setenv("SEO_AGENT_ENCRYPTION_KEYS", "fixture-rotation-" + "r" * 40 + ",previous-key")
    m._validate_startup_config(service_mode="worker")


def test_worker_rejects_a_shared_session_and_encryption_key(production, monkeypatch):
    monkeypatch.setenv("SEO_AGENT_SECRET_KEY", m._current_encryption_seed())
    with pytest.raises(RuntimeError, match="distinct"):
        m._validate_startup_config(service_mode="worker")


def test_default_validation_still_requires_all_web_settings(production):
    with pytest.raises(RuntimeError) as error:
        m._validate_startup_config()
    assert all(key in str(error.value) for key in ("PUBLIC_BASE_URL", "SEO_AGENT_SECRET_KEY", "CRON_SECRET"))


@pytest.mark.parametrize("role", ["cron", "", "WORKER"])
def test_unknown_service_mode_is_rejected_before_any_initialization(monkeypatch, role):
    monkeypatch.setenv("SEO_AGENT_STRICT_CONFIG", "false")
    monkeypatch.setattr(m, "_run_alembic_upgrade_head", lambda: pytest.fail("No migration for unknown role"))
    with pytest.raises(ValueError, match="service mode"):
        m._initialize_service(service_mode=role)


@pytest.mark.parametrize("role", ["web", "worker"])
@pytest.mark.parametrize("migration", ["explicit", "default_sqlite", "auto_create", "already_migrated"])
def test_initialization_shares_validation_migration_monitoring_and_plan_restore(monkeypatch, role, migration):
    calls = []
    monkeypatch.setenv("DATABASE_URL", "sqlite:///fixture.db")
    monkeypatch.setenv("SEO_AGENT_DB_AUTO_MIGRATE", "false")
    monkeypatch.setenv("SEO_AGENT_DB_AUTO_CREATE", "false")
    if migration == "explicit":
        monkeypatch.setenv("SEO_AGENT_DB_AUTO_MIGRATE", "true")
    elif migration == "default_sqlite":
        monkeypatch.delenv("DATABASE_URL")
    elif migration == "auto_create":
        monkeypatch.setenv("SEO_AGENT_DB_AUTO_CREATE", "true")
    monkeypatch.setattr(m, "_validate_startup_config", lambda **kw: calls.append(("validate", kw)))
    monkeypatch.setattr(m, "_run_alembic_upgrade_head", lambda: calls.append("migrate"))
    monkeypatch.setattr(m.DB, "create_tables", lambda: calls.append("create"))
    monkeypatch.setattr(m, "_init_sentry", lambda: calls.append("monitor"))
    monkeypatch.setattr(m, "_apply_effective_env", lambda key: calls.append(("restore", key)))
    m._initialize_service(service_mode=role)
    expected = ["migrate"] if migration in {"explicit", "default_sqlite"} else ["create"] if migration == "auto_create" else []
    assert calls == [("validate", {"service_mode": role}), *expected, "monitor", ("restore", "PLAN_CONFIG_JSON")]


def test_web_initializes_before_any_background_task(monkeypatch):
    calls = []
    monkeypatch.setattr(m, "_initialize_service", lambda **kw: calls.append(("init", kw)), raising=False)
    for name in ("_start_job_worker", "_start_retention", "_start_verification_pr", "_start_contenu_auto"):
        monkeypatch.setattr(m, name, lambda name=name: calls.append(name))
    monkeypatch.setattr(m, "_validate_startup_config", lambda **kw: None)
    monkeypatch.setattr(m, "_init_sentry", lambda: None)
    monkeypatch.setattr(m, "_apply_effective_env", lambda *a: None)
    m._startup()
    assert calls == [("init", {"service_mode": "web"}), "_start_job_worker", "_start_retention",
                     "_start_verification_pr", "_start_contenu_auto"]


@pytest.mark.parametrize("role", ["web", "worker"])
def test_configuration_failure_precedes_migrations_and_other_initialization(monkeypatch, role):
    def invalid(**kw):
        raise RuntimeError("invalid fixture config")

    def forbidden(*args, **kw):
        pytest.fail("Configuration must be checked before any side effect")

    monkeypatch.setattr(m, "_validate_startup_config", invalid)
    for name in ("_run_alembic_upgrade_head", "_init_sentry", "_apply_effective_env"):
        monkeypatch.setattr(m, name, forbidden)
    monkeypatch.setattr(m.DB, "create_tables", forbidden)
    with pytest.raises(RuntimeError, match="invalid fixture config"):
        m._initialize_service(service_mode=role)


def test_render_separate_worker_explicitly_enables_strict_configuration():
    import yaml

    config = yaml.safe_load((Path(__file__).resolve().parents[2] / "render.yaml").read_text(encoding="utf-8"))
    services = {service["name"]: {item["key"]: item for item in service.get("envVars", [])}
                for service in config["services"]}
    worker = services["seo-agent-worker"]
    assert worker["SEO_AGENT_SERVICE_MODE"]["value"] == "worker"
    assert worker["SEO_AGENT_STRICT_CONFIG"]["value"] == "true"
    assert worker["DATABASE_URL"]["fromDatabase"] == services["seo-agent-web"]["DATABASE_URL"]["fromDatabase"]


@pytest.fixture
def worker(monkeypatch):
    calls = []
    monkeypatch.setenv("SEO_AGENT_DISABLE_WORKER", "true")
    stop = threading.Event()
    monkeypatch.setattr(m, "_WORKER_STOP", stop)
    monkeypatch.setattr(worker_main.signal, "signal", lambda *a: None)
    monkeypatch.setattr(m, "_init_sentry", lambda: calls.append("old-monitor"))
    monkeypatch.setattr(m, "_start_job_worker", lambda: calls.append("jobs"))

    def retention():
        calls.append("retention")
        stop.set()

    monkeypatch.setattr(m, "_start_retention", retention)
    for name in ("_start_verification_pr", "_start_contenu_auto"):
        monkeypatch.setattr(m, name, lambda: pytest.fail("Worker must not start web schedulers"))
    return calls


def test_separate_worker_initializes_before_consuming_jobs(monkeypatch, worker):
    monkeypatch.setattr(m, "_initialize_service", lambda **kw: worker.append(("init", kw)), raising=False)
    assert worker_main.main() == 0
    assert worker == [("init", {"service_mode": "worker"}), "jobs", "retention"]
    assert m._worker_enabled()


def test_separate_worker_cannot_ignore_an_initialization_failure(monkeypatch, worker):
    def fail(**kw):
        raise RuntimeError("invalid fixture startup")

    monkeypatch.setattr(m, "_initialize_service", fail, raising=False)
    with pytest.raises(RuntimeError, match="invalid fixture startup"):
        worker_main.main()
    assert not worker
