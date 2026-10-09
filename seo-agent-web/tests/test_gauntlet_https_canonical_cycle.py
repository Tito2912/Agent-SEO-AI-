"""The protocol upgrade proof retains routes, old repairs and collateral controls."""

import base64
import copy

import pytest

from ops.gauntlet import https_canonical_cycle as b
from ops.gauntlet.ai_budget import ClaudeBudget


def reports():
    p = b.previous.previous
    canonicals = {**{p.ROUTES[n]: p.URLS["qa-master"] if n in p.SCOPES.values() else p.BEFORE[n] for n in p.NAMES},
        b.previous.ROUTE: b.previous.DESTINATION, b.ROUTE: b.OLD}
    before = {"meta": {"max_pages": 90, "urls_uncrawled": 0, "stopped_on_time_budget": False},
        "pages": [{"url": b.SITE.rstrip("/") + route, "canonical": canonical, "status_code": 200,
                   "content_type": "text/html", "title": route} for route, canonical in canonicals.items()],
        "issues": {b.KEY: {"count": 1, "evidence": {"items": [dict(b.PAIR)]}}}}
    after = copy.deepcopy(before)
    next(p for p in after["pages"] if p["url"] == b.SOURCE)["canonical"] = b.SOURCE
    after["issues"][b.KEY] = {"count": 0, "evidence": {"items": []}}
    return before, after


def test_nonzero_provider_budget_is_refused_before_io(tmp_path):
    with pytest.raises(ValueError):
        b.cycle(None, "unused", tmp_path, ClaudeBudget(1))
    assert not list(tmp_path.iterdir())


def test_only_the_canonical_changes_not_alternates_navigation_or_newlines():
    tag = 'rel="canonical" href="' + b.OLD + '"'
    source = '<head>\r\n<link ' + tag + ' />\r\n<link rel="alternate" href="' + b.OLD + '" /></head>'
    source += '<a href="' + b.OLD + '">Control</a>\r\n'
    assert b.expected_source(source) == source.replace(tag, 'rel="canonical" href="' + b.SOURCE + '"')


@pytest.mark.parametrize("n", [0, 2])
def test_missing_or_duplicate_literal_canonical_refuses(n):
    with pytest.raises(ValueError):
        b.expected_source(('<link rel="canonical" href="' + b.OLD + '" />') * n)


@pytest.mark.parametrize("bad", [None, "path", "main", "branch", "retry", "sha", "content", "base64"])
def test_write_is_bounded_by_branch_file_blob_bytes_and_single_attempt(bad):
    branch, path, attempts = b.PREFIX + "correction-test", b.FILE, 0
    body = {"branch": branch, "sha": "known", "content": base64.b64encode(b"expected").decode()}
    if bad == "path":
        path = "_redirects"
    elif bad == "main":
        branch = body["branch"] = "main"
    elif bad == "branch":
        body["branch"] += "-other"
    elif bad == "retry":
        attempts = 1
    elif bad == "sha":
        body["sha"] = "stale"
    elif bad == "content":
        body["content"] = base64.b64encode(b"collateral").decode()
    elif bad == "base64":
        body["content"] = "!!!!"
    assert b.write_allowed(path, body, branch, attempts, "expected", "known") is (bad is None)


def test_one_to_zero_requires_same_html_routes_and_preserved_controls():
    before, after = reports()
    result = b.checks(before, after)
    assert result["target_counts"] == {b.KEY: [1, 0]}
    assert result["html_routes_before"] == result["html_routes_after"] == 7
    assert not result["increased_counts"]


@pytest.mark.parametrize("bad", ["witness", "old_repair", "unchanged", "collateral", "lost_route",
                                 "incomplete", "blocked", "settings", "false_zero", "count"])
def test_dropped_witnesses_incomplete_crawls_or_collateral_cannot_pass(bad):
    before, after = reports()
    if bad == "witness":
        before["issues"][b.KEY]["evidence"]["items"] = []
    elif bad == "old_repair":
        before["pages"][0]["canonical"] = b.OLD
    elif bad == "unchanged":
        after["pages"][-1]["canonical"] = b.OLD
    elif bad == "collateral":
        after["pages"][0]["title"] = "Unexpected"
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


def test_counter_increases_are_not_hidden():
    before, after = reports()
    after["issues"]["example"] = {"count": 2}
    assert b.checks(before, after)["increased_counts"] == {"example": [0, 2]}
