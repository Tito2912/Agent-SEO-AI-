"""The refusal bench must not turn an unchanged diagnostic into a successful repair."""

import copy
import json
from types import SimpleNamespace

import pytest

from backend import app as m
from ops.gauntlet import missing_canonical_policy_cycle as b
from ops.gauntlet.ai_budget import ClaudeBudget


def reports():
    urls = [*b.URLS, *[b.SITE + "control-" + str(i) for i in range(62)]]
    before = {"meta": {"max_pages": 90, "urls_uncrawled": 0, "stopped_on_time_budget": False},
        "pages": [{"url": url, "final_url": url, "status_code": 200, "content_type": "text/html",
                   "canonical": None if url in b.URLS else url, "title": url} for url in urls],
        "issues": {b.KEY: {"count": 2, "examples": list(b.URLS)}, "missing_title": {"count": 0, "examples": []}}}
    return before, copy.deepcopy(before)


def test_diagnostic_remains_in_report_without_becoming_a_fix_or_readiness_claim():
    before, after = reports()
    original = copy.deepcopy(before)
    result = b.checks(m, before, after)
    assert result["diagnostic_counts_retained"] == [2, 2]
    assert not result["automatic_canonical_repair_performed"]
    assert result["healthy_html_routes"] == [64, 64]
    assert before == original


@pytest.mark.parametrize("bad", ["false_zero", "float_count", "missing_block", "missing_control", "foreign_control",
    "canonical_added", "lost_route", "collateral", "status", "incomplete", "time_budget", "blocked", "settings",
    "new_issue", "different_examples", "automatic_offer", "shown"])
def test_false_repairs_incomplete_crawls_or_other_changes_cannot_pass(monkeypatch, bad):
    before, after = reports()
    if bad == "false_zero":
        after["issues"][b.KEY] = {"count": 0, "examples": []}
    elif bad == "float_count":
        after["issues"][b.KEY]["count"] = 2.0
    elif bad == "missing_block":
        after["issues"].pop(b.KEY)
    elif bad == "missing_control":
        after["issues"][b.KEY]["examples"].pop()
    elif bad == "foreign_control":
        after["issues"][b.KEY]["examples"][0] = "https://other.test/page"
    elif bad == "canonical_added":
        after["pages"][0]["canonical"] = b.URLS[0]
    elif bad == "lost_route":
        after["pages"].pop()
    elif bad == "collateral":
        after["pages"][-1]["title"] = "Changed"
    elif bad == "status":
        after["pages"][0]["status_code"] = 404
    elif bad == "incomplete":
        after["meta"]["urls_uncrawled"] = 1
    elif bad == "time_budget":
        after["meta"]["stopped_on_time_budget"] = True
    elif bad == "blocked":
        after["meta"]["blocked_by_host"] = {"count": 1}
    elif bad == "settings":
        after["meta"]["max_pages"] = 89
    elif bad == "new_issue":
        after["issues"]["missing_title"] = {"count": 1, "examples": [b.URLS[0]]}
    elif bad == "different_examples":
        before["issues"]["missing_title"] = {"count": 1, "examples": [b.URLS[0]]}
        after["issues"]["missing_title"] = {"count": 1, "examples": [b.URLS[1]]}
    elif bad == "automatic_offer":
        monkeypatch.setattr(m, "_github_issue_auto_fixable", lambda key: True)
    elif bad == "shown":
        monkeypatch.setattr(m.dash, "summarize_report", lambda report: {"issues": [{"key": b.KEY.upper()}]})
    with pytest.raises(ValueError):
        b.checks(m, before, after)


def test_observation_order_is_not_mistaken_for_a_change():
    before, after = reports()
    after["issues"][b.KEY]["examples"].reverse()
    after["pages"].reverse()
    assert b.checks(m, before, after)["other_issue_counts_and_examples_preserved"]


def test_literal_parser_ignores_comment_and_script_decoys():
    raw = b'<html lang="fr"><head><title>Actual</title><!-- <link rel="canonical" href="decoy"> -->'
    raw += b'<script>const decoy = \'<link rel="canonical" href="decoy">\';</script></head><body><h1>Actual</h1></body></html>'
    assert b.observe(raw) == {"canonical_count": 0, "canonicals": [], "title": "Actual", "lang": "fr", "h1": ["Actual"]}


def test_real_uppercase_canonical_is_not_ignored():
    out = b.observe(b'<html><head><title>Actual</title><link REL="CANONICAL" href="actual"></head></html>')
    assert out["canonical_count"] == 1 and out["canonicals"] == ["actual"]


@pytest.mark.parametrize("raw", [b"<html></html>", b"<head></head>", b"<html><head></head><head></head></html>",
                               b"<html><head></head></html><html></html>", b"\xff"])
def test_ambiguous_or_undecodable_controls_do_not_pass(raw):
    with pytest.raises((ValueError, UnicodeError)):
        b.observe(raw)


def test_nonzero_provider_budget_refuses_before_any_io(tmp_path):
    with pytest.raises(ValueError):
        b.cycle(None, "unused", tmp_path, ClaudeBudget(1))
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("returned", [False, True, None])
def test_native_probes_use_the_existing_strict_signature_and_explicit_false(returned):
    calls = []

    def probe(url, canonical):
        calls.append((url, canonical))
        return returned

    assert b.default_native_rejections(SimpleNamespace(_sitemap_https_page=probe)) == (2 if returned is False else 0)
    assert calls == [(b.PREVIEW.rstrip("/") + route, canonical)
                     for route, canonical in zip(b.previous.ROUTES[:2], (b.previous.SOURCE, b.previous.ABSENT))]


@pytest.mark.parametrize("write", ["_github_api_post", "_github_api_put", "raw_patch", "raw_post"])
def test_failure_restores_io_helpers_and_records_denied_write_attempt(tmp_path, write):
    def original(*args, **kwargs):
        pytest.fail("No real remote writes")

    backend = SimpleNamespace(_github_api_post=original, _github_api_put=original,
                             _github_api_path=lambda *args: "/" + "/".join(args))
    session_request = b.live.requests.sessions.Session.request

    def get(*args, **kwargs):
        if write.startswith("raw_"):
            return b.live.requests.request(write.removeprefix("raw_"), "https://fixture.invalid/")
        return getattr(backend, write)("unused")

    backend._github_api_get = get
    result = b.cycle(backend, "unused", tmp_path, ClaudeBudget(0))
    assert result["status"] == "failed" and result["write_attempts"] == 1 and not result["writes"]
    assert backend._github_api_post is original and backend._github_api_put is original
    assert b.live.requests.sessions.Session.request is session_request
    assert json.loads((tmp_path / "cycle.json").read_text())["status"] == "failed"
    assert result["claude_budget"]["attempted"] == result["claude_budget"]["denied"] == 0
