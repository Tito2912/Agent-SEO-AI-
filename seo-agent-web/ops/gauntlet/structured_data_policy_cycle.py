"""Owned loopback HTTP witness of local JSON-LD advice, never a successful repair."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import dataclasses
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sys
from threading import Thread
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ops.gauntlet import http_canonical_advice_cycle as local, live_cycle as live  # noqa: E402

SCHEMA = "structured_data_schema_org_validation_error"
FAQ = "structured_data_google_rich_results_validation_error"


def payloads() -> dict[str, tuple[str, list[str]]]:
    def faq(**fields):
        return json.dumps({"@context": "https://schema.org", "@type": "FAQPage", **fields})

    question = {"@type": "Question", "name": "Published question?"}
    rows = {
        "/faq-main": (faq(), ["faq_mainEntity_missing"]),
        "/faq-question": (faq(mainEntity=[None]), ["faq_question_invalid"]),
        "/faq-name": (faq(mainEntity=[{"@type": "Question", "acceptedAnswer": {"@type": "Answer", "text": "Published answer."}}]), ["faq_question_name_missing"]),
        "/faq-answer": (faq(mainEntity=[question]), ["faq_answer_missing"]),
        "/invalid-json": ('{"@context":"https://schema.org", oops}', ["invalid_json"]),
        "/missing-type": ('{"@context":"https://schema.org","name":"Existing content"}', ["missing_type"]),
    }
    for kind, price in (("Product", "29.90"), ("SoftwareApplication", "0")):
        rows["/valid-" + kind.lower()] = (json.dumps({"@context": "https://schema.org", "@type": kind,
            "name": "Control", "offers": {"@type": "Offer", "price": price, "priceCurrency": "EUR"}}), [])
    return rows


@contextmanager
def fixture():
    state = {"bodies": {}, "requests": [], "write_attempts": 0, "stopped": False}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            state["requests"].append(self.path)
            body = state["bodies"].get(self.path)
            if body is None:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            state["write_attempts"] += 1
            self.send_error(405)

        do_PUT = do_PATCH = do_DELETE = do_POST

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    state["base"] = f"http://127.0.0.1:{server.server_port}"
    for route, (raw, _) in payloads().items():
        state["bodies"][route] = ('<!doctype html><html lang="fr"><head><meta charset="utf-8">'
            '<title>Local JSON-LD control ' + route + '</title><link rel="canonical" href="' + state["base"] + route
            + '"><script type="application/ld+json">' + raw
            + '</script></head><body><h1>Owned local control</h1><p>Existing published fixture content.</p></body></html>').encode()
    thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        state["stopped"] = not thread.is_alive()
        if not state["stopped"]:
            raise RuntimeError("local_fixture_failed_to_stop")


def cycle(m, work: Path) -> dict:
    audit = local.auditor()
    old_idle = os.environ.get("SEO_AGENT_NETWORKIDLE_MS")
    os.environ["SEO_AGENT_NETWORKIDLE_MS"] = "0"
    hashes, reports = [], []
    try:
        with fixture() as state, live.requests.Session() as session:
            session.trust_env = False
            cfg = audit._parse_args([state["base"], "--max-pages", "8", "--workers", "1", "--timeout", "10",
                "--no-discover-canonicals", "--no-discover-hreflang", "--output-dir", str(work / "local-only")])
            for phase in ("before", "after-advice"):
                pages, phase_hashes = [], {}
                for route, (_, errors) in payloads().items():
                    url = state["base"] + route
                    response = session.get(url, timeout=10, allow_redirects=False)
                    try:
                        if response.status_code != 200 or response.content != state["bodies"][route]:
                            raise ValueError("local_body_not_verified")
                        phase_hashes[route] = hashlib.sha256(response.content).hexdigest()
                        (work / (phase + route.replace("/", "_") + ".html")).write_bytes(response.content)
                    finally:
                        response.close()
                    page = audit._extract_page(url, cfg, None, urlsplit(state["base"]))
                    if page.error or page.status_code != 200 or page.final_url != url or page.schema_org_errors != errors:
                        raise ValueError("actual_crawler_control_lost")
                    pages.append(page)
                report = {"pages": [dataclasses.asdict(p) for p in pages],
                          "issues": audit._score_issues(pages, base_url=state["base"])}
                if report["issues"][SCHEMA]["count"] != 2 or report["issues"][FAQ]["count"] != 4:
                    raise ValueError("local_diagnostic_erased_or_price_false_positive")
                if (report["issues"][SCHEMA]["external_schema_org_validation"] is not False
                        or report["issues"][FAQ]["external_google_validation"] is not False):
                    raise ValueError("local_check_misrepresented")
                for key in (SCHEMA, FAQ):
                    prep = m._prepare_issue_fix(issue_key=key, issues=report["issues"], impacted=report["issues"][key]["examples"],
                        all_paths=["index.html", "app/layout.tsx"], site_name="127.0.0.1", owner="owned-loopback",
                        repo_name="owned-loopback", branch="main", token="unused", pages=report["pages"])
                    if not prep["refusal"] or prep["link_rewriter"] is not None or m._github_issue_auto_fixable(key):
                        raise ValueError("unproved_structured_repair_still_offered")
                    out = m._apply_prepared_issue_fix(owner="owned-loopback", repo_name="owned-loopback", branch="main",
                        token="unused", fix_branch="qa", all_paths=["index.html"], issue_key=key, issue_label=key,
                        impacted=report["issues"][key]["examples"], site_name="127.0.0.1", file_state={}, max_files=8,
                        prep={"refusal": None, "loop_paths": ["index.html"]}, pages=report["pages"], index=None)
                    if not out["error"] or any(out[field] for field in ("patched", "targets", "ai_files", "config_changes")):
                        raise ValueError("forged_plan_not_refused")
                live.save(work / (phase + ".json"), report)
                hashes.append(phase_hashes)
                reports.append(report)
            if hashes[0] != hashes[1] or state["write_attempts"]:
                raise ValueError("advice_changed_source_bytes")
        result = {"status": "measured_local_structured_advice_not_repair", "real_http_routes": 8,
            "schema_counts": [r["issues"][SCHEMA]["count"] for r in reports],
            "faq_counts": [r["issues"][FAQ]["count"] for r in reports], "valid_price_negative_controls": 2,
            "bodies_sha256": hashes[0], "bodies_unchanged": True, "write_attempts": state["write_attempts"],
            "local_servers_stopped": state["stopped"], "automatic_repair_performed": False,
            "external_validator_called": False, "production_ssrf_policy_changed": False,
            "public_https_or_universal_certification": False}
        live.save(work / "cycle.json", result)
        return result
    finally:
        browser = getattr(audit._BROWSER_TLS, "session", None)
        if browser is not None:
            browser.close()
            audit._BROWSER_TLS.session = None
        if old_idle is None:
            os.environ.pop("SEO_AGENT_NETWORKIDLE_MS", None)
        else:
            os.environ["SEO_AGENT_NETWORKIDLE_MS"] = old_idle


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", type=Path, required=True)
    args = parser.parse_args()
    args.workdir.mkdir(parents=True, exist_ok=True)
    if any(args.workdir.iterdir()):
        parser.error("Use an empty temporary directory, never customer data.")
    os.environ.update(DATABASE_URL="sqlite:///" + (args.workdir / "bench.db").as_posix(),
        SEO_AGENT_DATA_DIR=str(args.workdir / "data"), SEO_AGENT_RUNS_DIR=str(args.workdir / "runs"),
        SEO_AGENT_DISABLE_WORKER="true", SEO_AGENT_SECRET_KEY="fixture-only-test-secret",
        SEO_CORRECTION_AI_PROVIDER="none", ANTHROPIC_API_KEY="", OPENAI_API_KEY="")
    from backend import app as m

    def forbidden(*args, **kwargs):
        raise AssertionError("No GitHub, billing or provider request permitted in this local witness")

    for name in ("_github_api_get", "_github_api_post", "_github_api_put", "_openai_generate_file_patch",
                 "_anthropic_messages_text", "_openai_chat_text", "_correction_charge"):
        setattr(m, name, forbidden)
    print(cycle(m, args.workdir)["status"], flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
