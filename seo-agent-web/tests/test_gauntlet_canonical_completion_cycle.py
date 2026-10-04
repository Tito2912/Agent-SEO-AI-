"""A residual completion cannot pass by losing witnesses, routes or old repairs."""

import base64
import copy

import pytest

from ops.gauntlet import canonical_completion_cycle as b
from ops.gauntlet.ai_budget import ClaudeBudget


def reports():
    canonicals = {**{b.previous.ROUTES[n]: b.previous.URLS["qa-master"] if n in b.previous.SCOPES.values()
                    else b.previous.BEFORE[n] for n in b.previous.NAMES},
                  b.ROUTE: b.OLD, b.EXTRA["canonical-relay"]: b.DESTINATION, b.EXTRA["missing-h1"]: b.DESTINATION}
    before = {"meta": {"max_pages": 90, "urls_uncrawled": 0, "stopped_on_time_budget": False},
        "pages": [{"url": b.SITE.rstrip("/") + route, "canonical": canonical, "status_code": 200,
                   "content_type": "text/html", "title": route} for route, canonical in canonicals.items()],
        "issues": {b.KEY: {"count": 1, "evidence": {"items": [dict(b.PAIR)]}}, b.REDIRECT_KEY: {"count": 0}}}
    after = copy.deepcopy(before)
    next(p for p in after["pages"] if p["url"] == b.SOURCE)["canonical"] = b.DESTINATION
    after["issues"][b.KEY] = {"count": 0, "evidence": {"items": []}}
    return before, after


def test_nonzero_budget_is_refused_before_io(tmp_path):
    with pytest.raises(ValueError):
        b.cycle(None, "unused", tmp_path, ClaudeBudget(1))
    assert not list(tmp_path.iterdir())


def test_single_canonical_replacement_preserves_crlf_alternate_and_navigation():
    tag = 'rel="canonical" href="' + b.OLD + '"'
    source = '<head>\r\n<link ' + tag + ' />\r\n<link rel="alternate" href="' + b.OLD + '" /></head>'
    source += '<a href="' + b.OLD + '">Relay</a>\r\n'
    assert b.expected_source(source) == source.replace(tag, 'rel="canonical" href="' + b.DESTINATION + '"')


@pytest.mark.parametrize("n", [0, 2])
def test_missing_or_duplicate_source_canonical_is_refused(n):
    with pytest.raises(ValueError):
        b.expected_source(('<link rel="canonical" href="' + b.OLD + '" />') * n)


def body():
    branch = b.PREFIX + "correction-test"
    return branch, {"branch": branch, "sha": "known-blob", "content": base64.b64encode(b"expected").decode()}


def test_write_requires_exact_single_file_branch_blob_and_bytes():
    branch, payload = body()
    assert b.write_allowed(b.FILE, payload, branch, 0, "expected", "known-blob")


@pytest.mark.parametrize("bad", ["path", "main", "branch", "retry", "sha", "no_sha", "content", "base64"])
def test_outside_write_scope_is_refused(bad):
    branch, payload = body()
    path, attempts, sha = b.FILE, 0, "known-blob"
    if bad == "path":
        path = "index.html"
    elif bad == "main":
        branch = payload["branch"] = "main"
    elif bad == "branch":
        payload["branch"] += "-other"
    elif bad == "retry":
        attempts = 1
    elif bad == "sha":
        payload["sha"] = "stale"
    elif bad == "no_sha":
        sha = payload["sha"] = ""
    elif bad == "content":
        payload["content"] = base64.b64encode(b"collateral").decode()
    else:
        payload["content"] = "!!!!"
    assert not b.write_allowed(path, payload, branch, attempts, "expected", sha)


def test_both_canonical_families_resolved_on_same_route_set():
    before, after = reports()
    result = b.checks(before, after)
    assert result["target_counts"] == {b.KEY: [1, 0], b.REDIRECT_KEY: [0, 0]}
    assert result["html_routes_before"] == result["html_routes_after"] == 8
    assert not result["increased_counts"]


@pytest.mark.parametrize("bad", ["witness", "old_repair", "destination", "unchanged", "collateral", "lost_route",
                                 "incomplete", "blocked", "settings", "false_zero", "count"])
def test_missing_witness_or_incomplete_collateral_results_cannot_pass(bad):
    before, after = reports()
    if bad == "witness":
        before["issues"][b.KEY]["evidence"]["items"] = []
    elif bad == "old_repair":
        before["pages"][0]["canonical"] = b.previous.BEFORE["qa-hop-source"]
    elif bad == "destination":
        before["pages"][-1]["canonical"] = b.OLD
    elif bad == "unchanged":
        next(p for p in after["pages"] if p["url"] == b.SOURCE)["canonical"] = b.OLD
    elif bad == "collateral":
        after["pages"][0]["title"] = "Unexpected edit"
    elif bad == "lost_route":
        after["pages"].pop()
    elif bad == "incomplete":
        after["meta"]["urls_uncrawled"] = 1
    elif bad == "blocked":
        before["meta"]["blocked_by_host"] = {"count": 1}
    elif bad == "settings":
        after["meta"]["max_pages"] = 89
    elif bad == "false_zero":
        after["issues"][b.KEY]["evidence"]["items"] = [dict(b.PAIR)]
    else:
        after["issues"][b.KEY]["count"] = 1
    with pytest.raises(ValueError):
        b.checks(before, after)


def test_increases_are_reported_not_hidden():
    before, after = reports()
    after["issues"]["og_url_not_matching_canonical"] = {"count": 2}
    assert b.checks(before, after)["increased_counts"] == {"og_url_not_matching_canonical": [0, 2]}


@pytest.mark.parametrize("bad", [None, "missing", "duplicate", "open", "merged", "unknown"])
def test_cleanup_requires_two_exact_closed_unmerged_prs(bad):
    prs = [{"number": 1}, {"number": 2}]
    cleanup = [{"number": n, "state": "closed", "merged": False} for n in (1, 2)]
    if bad == "missing":
        cleanup.pop()
    elif bad == "duplicate":
        cleanup[1]["number"] = 1
    elif bad == "open":
        cleanup[1]["state"] = "open"
    elif bad == "merged":
        cleanup[1]["merged"] = True
    elif bad == "unknown":
        del cleanup[1]["merged"]
    assert b.closed_cleanup(prs, cleanup) is (bad is None)
