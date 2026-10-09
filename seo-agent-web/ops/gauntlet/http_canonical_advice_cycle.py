"""Local HTTP/TLS witness: hosting advice, never a corrector-written redirect."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import dataclasses
import datetime as dt
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import ipaddress
import os
from pathlib import Path
import ssl
import sys
from threading import Event, Thread
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from cryptography import x509  # noqa: E402
from cryptography.hazmat.primitives import hashes, serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: E402
from cryptography.x509.oid import NameOID  # noqa: E402
from ops.gauntlet import live_cycle as live  # noqa: E402

KEY = "canonical_from_http_to_https"
HOST = "127.0.0.1"
ROUTE = "/owned-http-page"


def certificate(work: Path) -> tuple[Path, Path]:
    cert_path, key_path = work / "loopback-only-cert.pem", work / "loopback-only-key.pem"
    if cert_path.exists() or key_path.exists():
        raise ValueError("loopback_certificate_already_exists")
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, HOST)])
    now = dt.datetime.now(dt.UTC)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
        .serial_number(x509.random_serial_number()).not_valid_before(now - dt.timedelta(minutes=1))
        .not_valid_after(now + dt.timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address(HOST))]), critical=False)
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True).sign(key, hashes.SHA256()))
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption()))
    return cert_path, key_path


@contextmanager
def fixture(work: Path):
    redirect = Event()
    state = {"destination": "", "html": b"", "redirect": redirect}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            if self.path != ROUTE:
                self.send_error(404)
            elif self.server is http and redirect.is_set():
                self.send_response(301)
                self.send_header("Location", state["destination"])
                self.send_header("Content-Length", "0")
                self.end_headers()
            else:
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(state["html"])))
                self.end_headers()
                self.wfile.write(state["html"])

    servers, threads, key = [], [], None
    try:
        http = ThreadingHTTPServer((HOST, 0), Handler)
        servers.append(http)
        tls = ThreadingHTTPServer((HOST, 0), Handler)
        servers.append(tls)
        cert, key = certificate(work)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert, key)
        tls.socket = context.wrap_socket(tls.socket, server_side=True)
        state.update(http=f"http://{HOST}:{http.server_port}" + ROUTE,
            destination=f"https://{HOST}:{tls.server_port}" + ROUTE, certificate=cert)
        state["html"] = ('<!doctype html><html lang="fr"><head><meta charset="utf-8" />'
            '<title>Noyaru local HTTP hosting witness</title><link rel="canonical" href="' + state["destination"]
            + '" /></head><body><h1>Owned loopback fixture</h1></body></html>\n').encode()
        for server in servers:
            thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
            thread.start()
            threads.append(thread)
        yield state
    finally:
        for server in servers[:len(threads)]:
            server.shutdown()
        for server in servers:
            server.server_close()
        for thread in threads:
            thread.join(timeout=5)
        if key is not None:
            key.unlink(missing_ok=True)
        if any(thread.is_alive() for thread in threads):
            raise RuntimeError("loopback_server_failed_to_stop")


def auditor():
    name = "gauntlet_http_canonical_advice_audit"
    spec = importlib.util.spec_from_file_location(name, live.AUDITOR)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def cycle(m, work: Path) -> dict:
    audit = auditor()
    original_idle = os.environ.get("SEO_AGENT_NETWORKIDLE_MS")
    os.environ["SEO_AGENT_NETWORKIDLE_MS"] = "0"
    result = {"status": "unverified", "remote_writes": 0, "provider_requests": 0,
        "corrector_repaired_http_exposure": False, "manual_fixture_redirect_only": True,
        "universal_certification": False, "production_ssrf_policy_changed": False}
    try:
        with fixture(work) as state, live.requests.Session() as session:
            session.trust_env = False
            session.verify = str(state["certificate"])
            response = session.get(state["http"], timeout=10, allow_redirects=False)
            secure = session.get(state["destination"], timeout=10, allow_redirects=False)
            if response.status_code != 200 or secure.status_code != 200:
                raise ValueError("missing_real_http_tls_witness")
            if response.content != state["html"] or secure.content != state["html"]:
                raise ValueError("incorrect_fixture_html")
            (work / "before-http.html").write_bytes(response.content)
            (work / "before-https.html").write_bytes(secure.content)
            cfg = audit._parse_args([state["http"], "--max-pages", "2", "--workers", "1", "--timeout", "10",
                "--no-discover-canonicals", "--no-discover-hreflang", "--output-dir", str(work / "local-only")])
            source = audit._extract_page(state["http"], cfg, None, urlsplit(state["http"]))
            destination = audit._extract_page(state["destination"], cfg, None, urlsplit(state["destination"]))
            before = {"pages": [dataclasses.asdict(p) for p in (source, destination)],
                "issues": audit._score_issues([source, destination], base_url=state["http"])}
            live.save(work / "before.json", before)
            if (source.error or destination.error or source.status_code != 200 or destination.status_code != 200
                    or source.final_url != state["http"] or source.canonical != state["destination"]
                    or before["issues"][KEY]["count"] != 1):
                raise ValueError("missing_tracked_http_positive_control")
            prep = m._prepare_issue_fix(issue_key=KEY, issues=before["issues"], impacted=[state["http"]],
                all_paths=["index.html", "_redirects", "netlify.toml"], site_name=HOST, owner="loopback-fixture",
                repo_name="loopback-fixture", branch="main", token="unused", pages=before["pages"])
            if not prep["refusal"] or prep["link_rewriter"] is not None or m._github_issue_auto_fixable(KEY):
                raise ValueError("hosting_problem_still_offered_as_html_repair")
            repeated = audit._extract_page(state["http"], cfg, None, urlsplit(state["http"]))
            unchanged = audit._score_issues([repeated, destination], base_url=state["http"])
            if repeated.canonical != state["destination"] or unchanged[KEY]["count"] != 1:
                raise ValueError("advice_falsely_claims_resolution")
            # The operator changes this local server, never the application's corrector.
            state["redirect"].set()
            redirect = session.get(state["http"], timeout=10, allow_redirects=False)
            followed = session.get(state["http"], timeout=10)
            (work / "after-manual-redirect.html").write_bytes(followed.content)
            final = audit._extract_page(state["http"], cfg, None, urlsplit(state["http"]))
            after = {"pages": [dataclasses.asdict(p) for p in (final, destination)],
                "issues": audit._score_issues([final, destination], base_url=state["http"])}
            live.save(work / "after-manual-redirect.json", after)
            if (redirect.status_code != 301 or redirect.headers.get("location") != state["destination"]
                    or followed.status_code != 200 or followed.url != state["destination"] or followed.content != state["html"]
                    or final.error or final.redirect_statuses != [301] or final.canonical != source.canonical
                    or after["issues"][KEY]["count"] != 0):
                raise ValueError("manual_redirect_not_measured")
            result.update(status="measured_hosting_advice_and_manual_redirect_control", http=state["http"],
                destination=state["destination"], canonical_unchanged=True, real_tls_verified_by_requests=True,
                source_html_sha256=hashlib.sha256(state["html"]).hexdigest(), before_and_after_html_identical=True,
                counts_before_advice_after_advice_after_manual_redirect=[1, 1, 0],
                direct_http_status_before_after=[200, 301], final_https_status=200,
                application_refusal=prep["refusal"], private_key_recorded_in_report=False)
        result["local_servers_stopped"] = True
    finally:
        browser = getattr(audit._BROWSER_TLS, "session", None)
        if browser is not None:
            browser.close()
            audit._BROWSER_TLS.session = None
        if original_idle is None:
            os.environ.pop("SEO_AGENT_NETWORKIDLE_MS", None)
        else:
            os.environ["SEO_AGENT_NETWORKIDLE_MS"] = original_idle
    live.save(work / "cycle.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", type=Path, required=True)
    args = parser.parse_args()
    args.workdir.mkdir(parents=True, exist_ok=True)
    if any(args.workdir.iterdir()):
        parser.error("Use an empty temporary directory, never customer data.")
    os.environ.update({"DATABASE_URL": "sqlite:///" + (args.workdir / "bench.db").as_posix(),
        "SEO_AGENT_DATA_DIR": str(args.workdir / "data"), "SEO_AGENT_RUNS_DIR": str(args.workdir / "runs"),
        "SEO_AGENT_DISABLE_WORKER": "true", "SEO_AGENT_SECRET_KEY": "fixture-only-test-secret",
        "SEO_CORRECTION_AI_PROVIDER": "none", "ANTHROPIC_API_KEY": "", "OPENAI_API_KEY": ""})
    from backend import app as m

    def forbidden(*a, **kw):
        raise AssertionError("No GitHub or provider requests permitted in the local witness.")

    for name in ("_github_api_get", "_github_api_post", "_github_api_put", "_openai_generate_file_patch",
                 "_anthropic_messages_text", "_openai_chat_text"):
        setattr(m, name, forbidden)
    result = cycle(m, args.workdir)
    print(result["status"], flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
