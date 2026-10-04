"""Real loopback HTTP/TLS control, isolated from all production/provider access."""

import os
import socket
from urllib.parse import urlsplit

import pytest

from backend import app as m
from ops.gauntlet import http_canonical_advice_cycle as b


def test_real_http_witness_is_refused_then_only_manual_redirect_resolves_it(monkeypatch, tmp_path):
    def forbidden(*args, **kwargs):
        pytest.fail("No GitHub or provider request permitted")

    for name in ("_github_api_get", "_github_api_post", "_github_api_put", "_openai_generate_file_patch",
                 "_anthropic_messages_text", "_openai_chat_text"):
        monkeypatch.setattr(m, name, forbidden)
    idle = os.environ.get("SEO_AGENT_NETWORKIDLE_MS")
    result = b.cycle(m, tmp_path)
    assert result["status"] == "measured_hosting_advice_and_manual_redirect_control"
    assert result["counts_before_advice_after_advice_after_manual_redirect"] == [1, 1, 0]
    assert result["direct_http_status_before_after"] == [200, 301] and result["final_https_status"] == 200
    assert result["canonical_unchanged"] and result["before_and_after_html_identical"]
    assert result["real_tls_verified_by_requests"] and result["local_servers_stopped"]
    assert not result["corrector_repaired_http_exposure"] and result["manual_fixture_redirect_only"]
    assert result["remote_writes"] == result["provider_requests"] == 0
    assert os.environ.get("SEO_AGENT_NETWORKIDLE_MS") == idle
    assert not (tmp_path / "loopback-only-key.pem").exists()
    assert (tmp_path / "before-http.html").read_bytes() == (tmp_path / "before-https.html").read_bytes()
    assert (tmp_path / "before-http.html").read_bytes() == (tmp_path / "after-manual-redirect.html").read_bytes()


def test_fixture_servers_and_generated_private_key_are_cleaned_on_error(tmp_path):
    with pytest.raises(ValueError, match="owned_test_failure"):
        with b.fixture(tmp_path) as fixture:
            ports = [urlsplit(fixture[k]).port for k in ("http", "destination")]
            raise ValueError("owned_test_failure")
    for port in ports:
        with socket.socket() as sock:
            sock.settimeout(1)
            assert sock.connect_ex((b.HOST, port)) != 0
    assert not (tmp_path / "loopback-only-key.pem").exists()


def test_local_fixture_does_not_weaken_the_default_private_host_policy(monkeypatch):
    monkeypatch.delenv("SEO_AUDIT_ALLOW_PRIVATE_HOSTS", raising=False)
    audit = b.auditor()
    assert audit._allow_private_hosts() is False
    assert audit._host_is_public(b.HOST)[0] is False


@pytest.mark.parametrize("name", ["loopback-only-key.pem", "loopback-only-cert.pem"])
def test_certificate_refuses_preexisting_files_without_deleting_them(tmp_path, name):
    existing = tmp_path / name
    existing.write_bytes(b"existing-file-sentinel")
    with pytest.raises(ValueError, match="loopback_certificate_already_exists"):
        with b.fixture(tmp_path):
            pytest.fail("Preexisting certificate material must not be overwritten")
    assert existing.read_bytes() == b"existing-file-sentinel"
