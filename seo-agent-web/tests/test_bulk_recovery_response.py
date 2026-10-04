from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from backend import app as m


def recover(monkeypatch, summary=None):
    repo = {"full_name": "fixture/site"}
    pr = {"number": 4, "html_url": "https://github.com/fixture/site/pull/4", "state": "open",
          "head": {"ref": "seo-fix/bulk-fixture", "repo": repo}, "base": {"ref": "main", "repo": repo}}
    intent = {"owner": "fixture", "repo": "site", "base": "main", "billable": 3, "motif": "bulk", "tasks": []}
    if summary is not None:
        intent["result"] = summary
    op = SimpleNamespace(row=SimpleNamespace(payer_id="fixture-payer", action="api_github_bulk_fix",
        data={"branch": "seo-fix/bulk-fixture", "pr_intent": intent}), finish=lambda response: None)
    monkeypatch.setattr(m, "_effective_user_connection_value", lambda **kw: ("unused", "user"))
    monkeypatch.setattr(m, "_github_api_get", lambda *a, **kw: [pr])
    monkeypatch.setattr(m.correction_journal, "pr_received", lambda data: None)
    monkeypatch.setattr(m, "_correction_charge", lambda *a, **kw: None)
    monkeypatch.setattr(m, "_restore_correction_tasks", lambda *a: None)
    response = m._recover_correction(op, user=SimpleNamespace(id="fixture-payer"), slug="fixture")
    assert response.status_code == 200
    return json.loads(response.body)


@pytest.mark.parametrize("partial", [False, True])
def test_a_recovered_bulk_pr_keeps_the_frozen_counts_scope_and_partial_flag(monkeypatch, partial):
    summary = {"fixed_count": 2, "total_count": 4, "partial": partial, "results": [{"issue_key": "duplicate_titles", "partial": partial}],
               "not_attempted": ["c.html"], "not_attempted_count": 1, "not_attempted_issues_count": 2,
               "cap": {"files": 3, "left": 3}}
    result = recover(monkeypatch, summary)
    assert all(result[k] == v for k, v in summary.items())
    assert result["ok"] and result["recovered"] and result["pr_number"] == 4


def test_an_old_bulk_receipt_without_a_frozen_summary_does_not_claim_complete_scope(monkeypatch):
    result = recover(monkeypatch)
    assert result["partial"] is True and result["scope_unknown"] is True


def test_a_saved_summary_cannot_override_the_verified_pr_identity(monkeypatch):
    result = recover(monkeypatch, {"partial": True, "ok": False, "pr_number": 999, "pr_url": "https://untrusted.example/"})
    assert result["ok"] is True and result["pr_number"] == 4
    assert result["pr_url"] == "https://github.com/fixture/site/pull/4"


@pytest.mark.parametrize("summary", [{}, {"partial": "false"}, {"partial": 0}])
def test_an_incomplete_or_untyped_summary_keeps_scope_unknown(monkeypatch, summary):
    result = recover(monkeypatch, summary)
    assert result["partial"] is True and result["scope_unknown"] is True
