"""The sitemap proof keeps refused entries, masters and all served pages."""

import base64
import copy

import pytest

from ops.gauntlet import canonical_sitemap_cycle as b
from ops.gauntlet.ai_budget import ClaudeBudget


def source():
    urls = sorted({*b.TARGETS, *b.TARGETS.values(), *b.NEGATIVES, b.SITE})
    return '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\r\n' + "\r\n".join(
        '<url><loc>' + u + '</loc></url>' for u in urls) + '\r\n</urlset>\r\n'


def reports():
    urls = sorted({*b.previous.URLS.values(), *b.TARGETS, *b.TARGETS.values(), *b.NEGATIVES, *b.previous.NEGATIVES})
    urls.extend(b.SITE + "control-" + str(i) for i in range(52 - len(urls)))
    before = {"meta": {"max_pages": 90, "urls_uncrawled": 0, "stopped_on_time_budget": False},
        "pages": [{"url": u, "status_code": 200, "content_type": "text/html", "title": u,
                   "canonical": b.TARGETS.get(u, u)} for u in urls],
        "issues": {b.KEY: {"count": 8, "examples": sorted(set(b.TARGETS) | b.NEGATIVES)},
            b.previous.KEY: {"count": 2, "examples": list(b.previous.NEGATIVES)}}}
    after = copy.deepcopy(before)
    after["issues"][b.KEY] = {"count": 2, "examples": sorted(b.NEGATIVES)}
    return before, after


def test_nonzero_provider_budget_refuses_before_io(tmp_path):
    with pytest.raises(ValueError):
        b.cycle(None, "unused", tmp_path, ClaudeBudget(1))
    assert not list(tmp_path.iterdir())


def test_expected_sitemap_removes_exactly_six_entries_and_preserves_every_other_byte():
    original = source()
    expected = b.expected_source(original)
    from backend import app as m
    assert m._rewrite_sitemap_locs(original, b.PAIRS, canonical=True) == (expected, 6)
    assert b.locs(expected) == [u for u in b.locs(original) if u not in b.TARGETS]


@pytest.mark.parametrize("bad", ["alias", "master", "negative", "duplicate", "extra_metadata", "namespace"])
def test_unverified_owned_source_shape_is_not_accepted(bad):
    original = source()
    if bad in {"alias", "master", "negative"}:
        url = next(iter(b.TARGETS if bad == "alias" else b.TARGETS.values() if bad == "master" else b.NEGATIVES))
        original = original.replace('<url><loc>' + url + '</loc></url>', '')
    elif bad == "duplicate":
        original = original.replace('</urlset>', '<url><loc>' + next(iter(b.TARGETS)) + '</loc></url></urlset>')
    elif bad == "extra_metadata":
        url = next(iter(b.TARGETS))
        original = original.replace('<loc>' + url + '</loc>', '<loc>' + url + '</loc><lastmod>2020-01-01</lastmod>')
    else:
        original = original.replace('http://www.sitemaps.org/schemas/sitemap/0.9', 'urn:unknown')
    with pytest.raises(ValueError):
        b.expected_source(original)


@pytest.mark.parametrize("bad", [None, "path", "main", "branch", "retry", "sha", "bytes", "base64"])
def test_write_requires_exact_branch_file_blob_bytes_and_single_attempt(bad):
    path, branch, attempts = b.FILE, b.PREFIX + "correction-test", 0
    body = {"branch": branch, "sha": "original", "content": base64.b64encode(b"expected").decode()}
    if bad == "path":
        path = "index.html"
    elif bad == "main":
        branch = body["branch"] = "main"
    elif bad == "branch":
        body["branch"] += "-other"
    elif bad == "retry":
        attempts = 1
    elif bad == "sha":
        body["sha"] = "stale"
    elif bad == "bytes":
        body["content"] = base64.b64encode(b"collateral").decode()
    elif bad == "base64":
        body["content"] = "!!!!"
    assert b.write_allowed(path, body, branch, attempts, "expected", "original") is (bad is None)


def test_cleanup_is_partial_and_preserves_every_html_route_and_canonical():
    before, after = reports()
    result = b.checks(before, after)
    assert result["target_counts"] == {b.KEY: [8, 2]}
    assert result["selected_occurrences"] == [6, 0]
    assert result["html_routes_before"] == result["html_routes_after"] == 52
    assert not result["increased_counts"]


@pytest.mark.parametrize("bad", ["missing_witness", "false_zero", "missing_negative", "duplicate_controls", "collateral",
    "lost_route", "master", "incomplete", "blocked", "settings"])
def test_false_zeros_lost_routes_or_changed_masters_cannot_pass(bad):
    before, after = reports()
    if bad == "missing_witness":
        before["issues"][b.KEY]["examples"].pop()
    elif bad == "false_zero":
        after["issues"][b.KEY] = {"count": 0, "examples": []}
    elif bad == "missing_negative":
        after["issues"][b.KEY]["examples"].pop()
    elif bad == "duplicate_controls":
        after["issues"][b.previous.KEY] = {"count": 0, "examples": []}
    elif bad == "collateral":
        after["pages"][0]["title"] = "Changed"
    elif bad == "lost_route":
        after["pages"].pop()
    elif bad == "master":
        next(p for p in after["pages"] if p["url"] == b.previous.URLS["qa-dupe-a"])["canonical"] = b.previous.URLS["qa-dupe-b"]
    elif bad == "incomplete":
        after["meta"]["urls_uncrawled"] = 1
    elif bad == "blocked":
        before["meta"]["blocked_by_host"] = {"count": 1}
    else:
        after["meta"]["max_pages"] = 89
    with pytest.raises(ValueError):
        b.checks(before, after)


def test_other_counter_increases_are_exposed():
    before, after = reports()
    after["issues"]["example"] = {"count": 2}
    assert b.checks(before, after)["increased_counts"] == {"example": [0, 2]}
