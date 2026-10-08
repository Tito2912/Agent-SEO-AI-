"""Every catalogued manual issue must refuse individual correction before cost or replay."""

import json
from types import SimpleNamespace

import pytest
from starlette.requests import Request
from urllib.parse import quote

from backend import app as m
from tests.test_corrector_operational import harness, HTML, URL  # noqa: F401
from tests.test_corrector_customer_workflow import customer  # noqa: F401

MANUAL_KEYS = sorted(key for key in m.dash.ISSUE_CATALOG if not m._github_issue_auto_fixable(key))
EXTRA_KEYS = ["some_future_issue_key", "keyword_rewrite", m._KEYWORD_REWRITE_KEY, "", "SLOW_PAGE", " slow_page ",
              "slow_page_indexable", "slow_page_not_indexable"]


def forbid_costs_and_replay(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("An unclaimed anomaly must refuse before payer, quota, replay, GitHub or AI")

    for name in ("_compte_payeur", "_correction_gate", "_correction_charge", "_github_api_get",
                 "_github_api_post", "_github_api_put", "_openai_generate_file_patch", "_openai_url_fix"):
        monkeypatch.setattr(m, name, forbidden)
    for name in ("find", "pending", "operation"):
        monkeypatch.setattr(m.correction_journal, name, forbidden)


@pytest.mark.parametrize("key", MANUAL_KEYS + EXTRA_KEYS)
@pytest.mark.parametrize("mode", ["individual", "url_preview", "github_preview", "github_confirm"])
def test_unclaimed_keys_cannot_reach_any_individual_correction(harness, monkeypatch, key, mode):
    state, run = harness
    state.update(key=key, sources={"index.html": HTML}, report={"issues": {key: {"count": 1, "examples": [URL]}}})
    forbid_costs_and_replay(monkeypatch)
    if mode == "individual":
        result = run(mode)
        status, body = result["status"], result["response"]
    else:
        request = Request({"type": "http", "method": "POST", "path": "/", "headers": []})
        request.state.user = state["user"]
        response = (m.api_issue_url_fix(request, "review", key, url=URL) if mode == "url_preview" else
                    m.api_github_fix(request, "review", key, m._GithubFixBody(url=URL, confirm=mode == "github_confirm",
                        file_path="index.html", patched_content=HTML)))
        status, body = response.status_code, json.loads(response.body)
    assert status == 422 and body["ok"] is False and body["advisory"] is True
    assert not state["charges"] and not state["written"] and not state["ai_calls"] and not state["pr_bodies"]


@pytest.mark.parametrize("key", MANUAL_KEYS + EXTRA_KEYS)
def test_preparation_never_authorizes_an_unclaimed_anomaly(monkeypatch, key):
    forbid_costs_and_replay(monkeypatch)
    prep = m._prepare_issue_fix(issue_key=key, issues={key: {"count": 1, "examples": [URL]}}, impacted=[URL],
        all_paths=["index.html", "netlify.toml", "app/layout.tsx"], site_name="site.test",
        owner="fixture", repo_name="fixture", branch="main", token="unused", pages=[])
    assert prep["refusal"] and prep["link_rewriter"] is None
    assert not prep["rewriter_ai_fallback"] and not prep["rewriter_is_ai"]


@pytest.mark.parametrize("key", MANUAL_KEYS + EXTRA_KEYS)
def test_execution_does_not_trust_a_forged_unclaimed_plan(monkeypatch, key):
    forbid_costs_and_replay(monkeypatch)
    monkeypatch.setattr(m, "_deep_patch_issue_files", lambda **kw: pytest.fail("Must refuse before the patch utility"))
    out = m._apply_prepared_issue_fix(owner="fixture", repo_name="fixture", branch="main", token="unused", fix_branch="qa",
        all_paths=["index.html"], issue_key=key, issue_label=key, impacted=[URL], site_name="site.test",
        file_state={}, max_files=8, prep={"refusal": None, "loop_paths": ["index.html"]}, pages=[], index=None)
    assert out["error"] and not out["patched"] and not out["targets"] and not out["ai_files"]


@pytest.mark.parametrize("key", MANUAL_KEYS + ["slow_page_indexable", "slow_page_not_indexable"])
@pytest.mark.parametrize("forged", [False, True])
def test_tracked_manual_keys_also_refuse_the_low_level_patch_utility(monkeypatch, key, forged):
    forbid_costs_and_replay(monkeypatch)
    state = {"index.html": {"sha": "original", "content": HTML}}
    patched, skipped, targets, ai_files = m._deep_patch_issue_files(owner="fixture", repo_name="fixture", branch="main",
        token="unused", fix_branch="qa", all_paths=["index.html"], issue_key=key, issue_label=key,
        impacted_urls=[URL], site_name="site.test", file_state=state, targets_override=["index.html"],
        link_rewriter=(lambda raw: (raw.replace("Une page", "Different content"), 1)) if forged else None)
    assert not patched and skipped and not targets and not ai_files
    assert state == {"index.html": {"sha": "original", "content": HTML}}


@pytest.mark.parametrize("key", ["some_future_issue_key", m._KEYWORD_REWRITE_KEY, ""])
def test_unknown_keys_cannot_use_an_unrestricted_low_level_model_fallback(monkeypatch, key):
    forbid_costs_and_replay(monkeypatch)
    patched, skipped, targets, ai_files = m._deep_patch_issue_files(owner="fixture", repo_name="fixture", branch="main",
        token="unused", fix_branch="qa", all_paths=["index.html"], issue_key=key, issue_label=key,
        impacted_urls=[URL], site_name="site.test", file_state={}, targets_override=["index.html"])
    assert not patched and skipped and not targets and not ai_files


@pytest.mark.parametrize("key", ["slow_page", "no_hsts", "redirect_chain", "some_future_issue_key"])
@pytest.mark.parametrize("mode", ["individual", "url_preview", "github_preview", "github_confirm"])
@pytest.mark.parametrize("owned", [False, True])
def test_authenticated_customer_api_checks_access_then_refuses_without_debit(customer, monkeypatch, key, mode, owned):
    state, _, used, uid, slug = customer
    forbid_costs_and_replay(monkeypatch)
    try:
        route = "url-fix" if mode == "url_preview" else "deep-fix" if mode == "individual" else "github-fix"
        url = f"/api/projects/{slug if owned else 'unowned-project'}/issues/{key}/{route}"
        client = state["client"]
        body = {"url": URL, "confirm": mode == "github_confirm", "file_path": "index.html", "patched_content": HTML}
        response = client.request("GET" if mode == "url_preview" else "POST", url,
            params={"url": URL} if mode == "url_preview" else None, json=body if mode != "url_preview" else None,
            headers={m._CSRF_HEADER_NAME: client.cookies.get(m._CSRF_COOKIE_NAME, "")})
        assert response.status_code == (422 if owned else 404), response.text
        if owned:
            assert response.json()["advisory"] is True
        assert used() == 0 and not state["models"] and not state["writes"] and not state["posts"]
    finally:
        from sqlalchemy import delete
        from backend.models import User
        with m.DB.session() as db:
            db.execute(delete(User).where(User.id == uid))
            db.commit()


@pytest.mark.parametrize("key", ["MISSING_TITLE", " missing_title "])
def test_normalized_supported_keys_share_the_preview_cache_and_single_debit(customer, key):
    from tests.test_corrector_customer_workflow import URL as customer_url

    state, send, used, uid, slug = customer
    try:
        preview = send()
        assert preview.status_code == 200, preview.text
        client = state["client"]
        path = f"/api/projects/{slug}/issues/{quote(key, safe='')}/github-fix"
        headers = {m._CSRF_HEADER_NAME: client.cookies.get(m._CSRF_COOKIE_NAME, "")}
        again = client.post(path, json={"url": customer_url, "crawl_ts": "20261002-090000"}, headers=headers)
        assert again.status_code == 200 and again.json()["cached"] is True, again.text
        assert used() == 1 and state["models"] == 1
        confirmed = client.post(path, json={"url": customer_url, "crawl_ts": "20261002-090000", "confirm": True,
            "file_path": preview.json()["file"], "patched_content": preview.json()["patched_content"]}, headers=headers)
        assert confirmed.status_code == 200, confirmed.text
        assert used() == 1 and state["models"] == 1 and state["posts"][-1][1]["draft"] is True
    finally:
        from sqlalchemy import delete
        from backend.models import User
        with m.DB.session() as db:
            db.execute(delete(User).where(User.id == uid))
            db.commit()


@pytest.mark.parametrize("key", ["MISSING_TITLE", " missing_title "])
@pytest.mark.parametrize("receipt_state", ["completed", "blocked"])
def test_legacy_supported_alias_receipt_is_recovered_without_rebilling(customer, key, receipt_state):
    from sqlalchemy import delete, select
    from backend.models import CorrectionOperation, Project, User
    from tests.test_corrector_customer_workflow import URL as customer_url

    state, send, used, uid, slug = customer
    try:
        preview = send()
        assert preview.status_code == 200, preview.text
        with m.DB.session() as db:
            project = db.scalar(select(Project).where(Project.slug == slug))
            row = db.scalar(select(CorrectionOperation).where(CorrectionOperation.payer_id == uid))
            payload = {"values": {"issue_key": key, "body": m._GithubFixBody(url=customer_url,
                crawl_ts="20261002-090000").model_dump(mode="json")}, "github": m._project_github_cfg(project)}
            legacy_key = m.correction_journal.request_key(uid, str(project.id), "api_github_fix", payload)
            row.request_key, row.state = legacy_key, receipt_state
            db.commit()
        client = state["client"]
        response = client.post(f"/api/projects/{slug}/issues/{quote(key, safe='')}/github-fix",
            json={"url": customer_url, "crawl_ts": "20261002-090000"},
            headers={m._CSRF_HEADER_NAME: client.cookies.get(m._CSRF_COOKIE_NAME, "")})
        assert response.status_code == 200, response.text
        assert response.json()["patched_content"] == preview.json()["patched_content"]
        assert response.json().get("cached") or response.json().get("recovered")
        assert used() == 1 and state["models"] == 1 and not state["writes"] and not state["posts"]
        with m.DB.session() as db:
            rows = db.scalars(select(CorrectionOperation).where(CorrectionOperation.payer_id == uid)).all()
            assert len(rows) == 1 and rows[0].request_key == legacy_key and rows[0].state == "completed"
    finally:
        with m.DB.session() as db:
            db.execute(delete(User).where(User.id == uid))
            db.commit()


def render_detail(key, examples, suggestion=None, **controls):
    project = {"slug": "fixture", "timestamp": "20261008", "report_json": "unused.json",
        "issue": {"key": key, "label": key, "category": "Other", "severity": "notice", "count": len(examples), "examples": examples}}
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    return m.templates.get_template("issue_detail.html").render(request=request, project=project,
        issue_key=key, fix_suggestion=suggestion or {}, fix_suggestions_path="", gh_tasks={}, q="", **controls)


@pytest.mark.parametrize("key", ["slow_page", "no_hsts", "redirect_chain", "structured_data_schema_org_validation_error"])
@pytest.mark.parametrize("example", [URL, URL + " -> https://site.test/other", URL + " missing value"])
def test_manual_diagnostics_do_not_offer_dead_or_billable_correction_buttons(key, example):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(render_detail(key, [example], correction_controls={"deep_fix": False, "url_fix": False}), "html.parser")
    assert not soup.select("button.url-ai-btn, button.gh-fix-btn, button.deep-fix-btn")
    assert soup.select(".task-status-btn") and soup.find("a", href=URL)


@pytest.mark.parametrize("key", ["slow_page", "no_hsts", "redirect_chain", "structured_data_schema_org_validation_error"])
def test_manual_sample_urls_also_keep_advice_without_correction_buttons(key):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(render_detail(key, [], suggestion={"sample_urls": [URL], "why": "Manual diagnostic"},
        correction_controls=m._issue_correction_controls(key)), "html.parser")
    assert not soup.select("button.url-ai-btn, button.gh-fix-btn")
    assert soup.find("a", href=URL) and "Manual diagnostic" in soup.get_text()


@pytest.mark.parametrize("key", ["missing_title", "viewport_not_set"])
@pytest.mark.parametrize("example", [URL, URL + " -> https://site.test/other", URL + " missing value", "samples"])
def test_claimed_buttons_use_the_allowed_preview_or_protected_deep_route(key, example):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(render_detail(key, [] if example == "samples" else [example],
        suggestion={"sample_urls": [URL]} if example == "samples" else {},
        correction_controls=m._issue_correction_controls(key)), "html.parser")
    buttons = soup.select("button.gh-fix-btn")
    assert buttons and all(b["data-fix-route"] == ("deep-fix" if key == "viewport_not_set" else "github-fix") for b in buttons)
    assert bool(soup.select("button.url-ai-btn")) is (key == "missing_title")


def test_missing_policy_context_does_not_restore_a_generic_correction_offer():
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(render_detail("missing_title", [URL]), "html.parser")
    assert not soup.select("button.url-ai-btn, button.gh-fix-btn")


@pytest.mark.parametrize("key", ["missing_title", "viewport_not_set", "slow_page", "some_future_issue_key"])
def test_detail_controls_are_derived_from_current_policy_not_a_saved_plan(monkeypatch, key):
    project = SimpleNamespace(site_name="site.test", slug="fixture", base_url=URL)
    data = {"slug": "fixture", "timestamp": "20261008", "issue": {"key": key, "label": key, "severity": "notice", "count": 1}}
    monkeypatch.setattr(m, "_db_project_or_404", lambda *a: project)
    monkeypatch.setattr(m, "_runs_dir_pour_slug", lambda *a: "unused")
    monkeypatch.setattr(m.dash, "issue_detail", lambda *a, **kw: data)
    monkeypatch.setattr(m.dash, "load_report_json", lambda *a, **kw: {"issues": {key: {"count": 1, "examples": [URL]}}})
    monkeypatch.setattr(m, "_fix_suggestions_path", lambda *a: None)
    monkeypatch.setattr(m, "_load_fix_suggestion_for_issue", lambda *a: {"auto_fixable": True, "mode": "auto"})
    monkeypatch.setattr(m.templates, "TemplateResponse", lambda name, context, **kw: SimpleNamespace(context=context, headers={}))
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    response = m.project_issue_detail(request, "fixture", key)
    expected = m._github_issue_auto_fixable(key)
    assert response.context["correction_controls"]["deep_fix"] is expected
    assert response.context["correction_controls"]["url_fix"] is (expected and key != "viewport_not_set")
    if not expected:
        assert response.context["fix_suggestion"]["mode"] == "suggest-only"
        assert response.context["fix_suggestion"]["why"]
