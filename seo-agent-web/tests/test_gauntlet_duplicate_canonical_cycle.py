"""The duplicate proof cannot silently consolidate metadata-only negative controls."""

import base64
import copy

import pytest

from ops.gauntlet import duplicate_canonical_cycle as b
from ops.gauntlet.ai_budget import ClaudeBudget


def reports():
    urls = [*b.URLS.values(), *b.NEGATIVES, b.previous.SOURCE]
    urls += [b.SITE + "control-" + str(i) for i in range(52 - len(urls))]
    before = {"meta": {"max_pages": 90, "urls_uncrawled": 0, "stopped_on_time_budget": False},
        "pages": [{"url": u, "status_code": 200, "content_type": "text/html", "title": u,
                   "canonical": None if u in b.TWINS or u in b.NEGATIVES else u, "og_url": u} for u in urls],
        "issues": {b.KEY: {"count": 4, "examples": sorted(b.TWINS | set(b.NEGATIVES))}}}
    after = copy.deepcopy(before)
    for page in after["pages"]:
        if page["url"] in b.TWINS:
            page["canonical"] = page["og_url"] = b.URLS[b.NAMES[0]]
    after["issues"][b.KEY] = {"count": 2, "examples": list(b.NEGATIVES)}
    return before, after


def test_nonzero_provider_budget_is_refused_before_io(tmp_path):
    with pytest.raises(ValueError):
        b.cycle(None, "unused", tmp_path, ClaudeBudget(1))
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("name", b.NAMES[:2])
def test_expected_changes_are_exactly_canonical_and_existing_og_url(name):
    from backend import app as m
    source = b.page(name)
    assert m._add_duplicate_canonical(source, b.URLS[b.NAMES[0]]) == (b.expected_source(name), 1)
    assert m._duplicate_html_document(source)["body"] == m._duplicate_html_document(b.expected_source(name))["body"]


@pytest.mark.parametrize("phase", ["baseline", "correction"])
@pytest.mark.parametrize("bad", [None, "path", "main", "branch", "retry", "sha", "content", "base64", "unverified"])
def test_writes_require_exact_branch_scope_blob_and_bytes(phase, bad):
    path = "index.html" if phase == "baseline" else b.FILES[b.NAMES[0]]
    branch, written = b.PREFIX + phase + "-test", set()
    expected, shas = {path: "expected"}, {path: "known"}
    body = {"branch": branch, "sha": "known", "content": base64.b64encode(b"expected").decode()}
    if bad == "path":
        path = "_redirects"
    elif bad == "main":
        branch = body["branch"] = "main"
    elif bad == "branch":
        body["branch"] += "-other"
    elif bad == "retry":
        written.add(path)
    elif bad == "sha":
        body["sha"] = "stale"
    elif bad == "content":
        body["content"] = base64.b64encode(b"collateral").decode()
    elif bad == "base64":
        body["content"] = "!!!!"
    elif bad == "unverified":
        expected.clear()
    assert b.write_allowed(path, body, branch, phase, written, expected, shas) is (bad is None)


def test_true_duplicates_resolve_but_metadata_only_witnesses_remain():
    before, after = reports()
    result = b.checks(before, after)
    assert result["selected_occurrences"] == [2, 0]
    assert result["target_counts"] == {b.KEY: [4, 2]}
    assert result["html_routes_before"] == result["html_routes_after"] == 52
    assert not result["increased_counts"]


@pytest.mark.parametrize("bad", ["missing_witness", "false_zero", "missing_negative", "negative_consolidated",
    "unchanged", "loop", "og", "old_repair", "collateral", "lost_route", "incomplete", "blocked", "settings"])
def test_false_zeros_missing_controls_loops_and_collateral_cannot_pass(bad):
    before, after = reports()
    if bad == "missing_witness":
        before["issues"][b.KEY]["examples"].pop()
    elif bad == "false_zero":
        after["issues"][b.KEY] = {"count": 0, "examples": []}
    elif bad == "missing_negative":
        after["issues"][b.KEY]["examples"].pop()
    elif bad == "negative_consolidated":
        next(p for p in after["pages"] if p["url"] == b.NEGATIVES[0])["canonical"] = b.URLS[b.NAMES[0]]
    elif bad == "unchanged":
        after["pages"][0]["canonical"] = None
    elif bad == "loop":
        after["pages"][0]["canonical"] = b.URLS[b.NAMES[1]]
    elif bad == "og":
        after["pages"][1]["og_url"] = b.URLS[b.NAMES[1]]
    elif bad == "old_repair":
        next(p for p in after["pages"] if p["url"] == b.previous.SOURCE)["canonical"] = b.previous.OLD
    elif bad == "collateral":
        after["pages"][2]["title"] = "Changed"
    elif bad == "lost_route":
        after["pages"].pop()
    elif bad == "incomplete":
        after["meta"]["urls_uncrawled"] = 1
    elif bad == "blocked":
        before["meta"]["blocked_by_host"] = {"count": 1}
    elif bad == "settings":
        after["meta"]["max_pages"] = 89
    with pytest.raises(ValueError):
        b.checks(before, after)


def test_counter_increases_are_exposed():
    before, after = reports()
    after["issues"]["example"] = {"count": 2}
    assert b.checks(before, after)["increased_counts"] == {"example": [0, 2]}
