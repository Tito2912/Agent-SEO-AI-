"""The local process bench must never inherit a production DSN or service credentials."""

import pytest

from ops.gauntlet import startup_policy_cycle as cycle


@pytest.mark.parametrize("role", ["web", "worker"])
def test_fixture_environment_is_confined_and_does_not_inherit_service_secrets(monkeypatch, tmp_path, role):
    for key in ("DATABASE_URL", "ANTHROPIC_API_KEY", "STRIPE_SECRET_KEY", "AWS_ACCESS_KEY_ID", "SENTRY_DSN"):
        monkeypatch.setenv(key, "production-value-must-not-leak")
    env = cycle.fixture_environment(tmp_path, role)
    assert env["DATABASE_URL"] == "sqlite:///" + (tmp_path / "fixture.sqlite3").as_posix()
    assert env["SEO_AGENT_DATA_DIR"] == str(tmp_path / "data")
    assert env["SEO_AGENT_DISABLE_WORKER"] == "true" and env["SEO_AGENT_SERVICE_MODE"] == role
    assert env["SEO_AGENT_STRICT_CONFIG"] == "true"
    assert env["ANTHROPIC_API_KEY"] == env["OPENAI_API_KEY"] == env["SENTRY_DSN"] == ""
    assert "STRIPE_SECRET_KEY" not in env and "AWS_ACCESS_KEY_ID" not in env
    assert env["SEO_AGENT_SECRET_KEY"] if role == "web" else not env["SEO_AGENT_SECRET_KEY"]


@pytest.mark.parametrize("key", ["DATABASE_URL", "SEO_AGENT_DATA_DIR", "SEO_AGENT_RUNS_DIR"])
def test_child_rejects_any_nonfixture_storage_target(monkeypatch, tmp_path, key):
    for name, value in cycle.fixture_environment(tmp_path, "web").items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv(key, "not-the-owned-fixture")
    with pytest.raises(ValueError, match="confined"):
        cycle.validate_child_environment(tmp_path)


def test_fixture_rejects_unknown_service_roles(tmp_path):
    with pytest.raises(ValueError, match="fixture role"):
        cycle.fixture_environment(tmp_path, "cron")


def test_existing_evidence_directory_is_never_overwritten(tmp_path):
    with pytest.raises(ValueError, match="new empty"):
        cycle.run(tmp_path)


def test_fixture_forbids_external_dns_and_connect_before_networking(monkeypatch):
    state = {"external_attempts": 0}
    monkeypatch.setattr(cycle.socket, "getaddrinfo", lambda *a, **kw: pytest.fail("Must not resolve external host"))
    monkeypatch.setattr(cycle.socket.socket, "connect", lambda *a, **kw: pytest.fail("Must not connect externally"))
    cycle._restrict_network(state)
    for action in (lambda: cycle.socket.getaddrinfo("example.invalid", 443),
                   lambda: cycle.socket.socket.connect(None, ("203.0.113.1", 443))):
        with pytest.raises(RuntimeError, match="External networking forbidden"):
            action()
    assert state["external_attempts"] == 2
