"""Public status describes observed reads without assuming provider health or exposing errors."""

from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from urllib.parse import urlsplit

from bs4 import BeautifulSoup
from fastapi.testclient import TestClient
import pytest

from backend import app as m, public_service_status


@pytest.mark.parametrize("failure", ["open", "query", "close"])
def test_database_failure_has_no_public_exception_payload(failure):
    secret = "postgresql://private:password@internal.invalid/customer-db"

    @contextmanager
    def session():
        if failure == "open":
            raise RuntimeError(secret)

        def execute(_query):
            if failure == "query":
                raise RuntimeError(secret)

        yield SimpleNamespace(execute=execute)
        if failure == "close":
            raise RuntimeError(secret)

    assert (
        public_service_status.database_read_available(SimpleNamespace(session=session))
        is False
    )


def test_database_probe_uses_a_read_and_closes_its_session():
    calls = []

    @contextmanager
    def session():
        try:
            yield SimpleNamespace(execute=lambda query: calls.append(str(query)))
        finally:
            calls.append("closed")

    assert (
        public_service_status.database_read_available(SimpleNamespace(session=session))
        is True
    )
    assert calls == ["SELECT 1", "closed"]


@pytest.mark.parametrize("lang,path", [("fr", "/status?lang=fr"), ("en", "/en/status")])
@pytest.mark.parametrize("db_available", [True, False])
def test_public_status_never_infers_provider_health(
    monkeypatch, lang, path, db_available
):
    monkeypatch.setattr(
        public_service_status, "database_read_available", lambda _db: db_available
    )

    def not_a_health_probe(*_args, **_kwargs):
        pytest.fail(
            "Configuration and provider calls must not be used as public availability checks"
        )

    monkeypatch.setattr(m, "_smtp_config", not_a_health_probe)
    monkeypatch.setattr(m.object_store, "s3_enabled", not_a_health_probe)
    monkeypatch.setattr(m.billing, "stripe_enabled", not_a_health_probe)
    client = TestClient(m.app)
    response = client.get(path)
    assert (
        response.status_code == 200 and response.headers["cache-control"] == "no-store"
    )
    soup = BeautifulSoup(response.text, "html.parser")
    assert soup.html["lang"] == lang
    readings = {
        row["data-service"]: row.select_one("dd").get_text(strip=True)
        for row in soup.select("[data-service]")
    }
    assert len(readings) == 5
    assert readings["web"] == ("opérationnel" if lang == "fr" else "operational")
    assert readings["database"] == (
        ("opérationnel" if lang == "fr" else "operational")
        if db_available
        else ("hors service" if lang == "fr" else "down")
    )
    assert all(
        readings[key] == ("Non vérifiable" if lang == "fr" else "Not verifiable")
        for key in ("email", "storage", "payment")
    )
    assert "db_error" not in response.context
    assert "en direct" not in soup.select_one("meta[name=description]")["content"]
    assert "best-effort" not in response.text
    checked_at = datetime.fromisoformat(soup.select_one("time")["datetime"])
    assert checked_at.tzinfo == timezone.utc
    assert 0 <= (datetime.now(timezone.utc) - checked_at).total_seconds() < 30
    prefix = "/en" if lang == "en" else ""
    assert (
        urlsplit(soup.select_one("link[rel=canonical]")["href"]).path
        == prefix + "/status"
    )
    assert {link["hreflang"] for link in soup.select("link[rel=alternate]")} == {
        "fr",
        "en",
        "x-default",
    }
    actions = [
        urlsplit(link["href"]).path for link in soup.select(".service-status-page a")
    ]
    assert actions == [prefix + "/status", prefix + "/support", prefix + "/"]
    assert client.head(path).status_code == 200
    assert client.head(path).headers["cache-control"] == "no-store"
    assert client.post(path).status_code == 403
    csrf = client.cookies.get(m._CSRF_COOKIE_NAME)
    assert client.post(path, headers={m._CSRF_HEADER_NAME: csrf}).status_code == 405


def test_real_database_read_is_available_in_the_local_app():
    assert public_service_status.database_read_available(m.DB) is True
