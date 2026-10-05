"""A noindex sitemap acceptance run must retain robots instructions and every observed page."""

import base64
import copy

import pytest

from ops.gauntlet import sitemap_noindex_cycle as b
from ops.gauntlet.ai_budget import ClaudeBudget
from tests.test_gauntlet_sitemap_redirect_cycle import reports as previous_reports


def source():
    urls = sorted(b.DIRECT) + [b.SITE + "control-" + str(i) for i in range(48)]
    return '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\r\n' + '\r\n'.join(
        '<url><loc>' + url + '</loc></url>' for url in urls) + '\r\n' + b.previous.alias_entry(b.ALIAS) + '\r\n</urlset>\r\n'


def reports():
    _, before = previous_reports()
    before["pages"][0] = dict(before["pages"][0], url=b.SITE + "gauntlet/noindex-no-description",
                              final_url=b.SITE + "gauntlet/noindex-no-description")
    for row in before["pages"]:
        if row["url"] in b.TARGETS:
            row.update(meta_robots="noindex, follow", meta_robots_tag_count=1)
    before["issues"][b.KEY] = {"count": 3, "examples": sorted(b.TARGETS)}
    after = copy.deepcopy(before)
    after["issues"][b.KEY] = {"count": 0, "examples": []}
    after["issues"][b.previous.KEY] = {"count": 1, "examples": sorted(set(b.previous.NEGATIVES) - {b.ALIAS})}
    return before, after


def test_nonzero_model_budget_refuses_before_remote_io(tmp_path):
    with pytest.raises(ValueError):
        b.cycle(None, "unused", tmp_path, ClaudeBudget(1))
    assert not list(tmp_path.iterdir())


def test_exact_cleanup_removes_only_three_xml_entries():
    from backend import app as m
    original = source()
    expected = b.expected_source(original)
    assert m._remove_sitemap_locs(original, sorted(b.TARGETS), verified=True) == (expected, 3)
    assert len(b.locs(expected)) == 48


@pytest.mark.parametrize("bad", ["count", "missing", "duplicate", "bytes", "namespace"])
def test_drifted_or_ambiguous_pinned_source_refuses(bad):
    original = source()
    url = next(iter(b.DIRECT))
    if bad == "count":
        original = original.replace('</urlset>', '<url><loc>' + b.SITE + 'extra</loc></url></urlset>')
    elif bad == "missing":
        original = original.replace(url, b.SITE + "different")
    elif bad == "duplicate":
        original = original.replace(b.SITE + "control-0", url)
    elif bad == "bytes":
        original = original.replace('<loc>' + url, '<loc> ' + url)
    else:
        original = original.replace("http://www.sitemaps.org/schemas/sitemap/0.9", "urn:wrong")
    with pytest.raises(ValueError):
        b.expected_source(original)


@pytest.mark.parametrize("bad", [None, "file", "main", "baseline", "branch", "retry", "sha", "bytes", "base64"])
def test_write_guard_requires_exact_corrective_branch_path_sha_bytes_and_no_retry(bad):
    path, branch, attempts = b.FILE, b.PREFIX + "correction-test", 0
    body = {"branch": branch, "sha": "original", "content": base64.b64encode(b"expected").decode()}
    if bad == "file":
        path = "index.html"
    elif bad == "main":
        branch = body["branch"] = "main"
    elif bad == "baseline":
        branch = body["branch"] = b.PREFIX + "baseline-test"
    elif bad == "branch":
        body["branch"] += "other"
    elif bad == "retry":
        attempts = 1
    elif bad == "sha":
        body["sha"] = "stale"
    elif bad == "bytes":
        body["content"] = base64.b64encode(b"collateral").decode()
    elif bad == "base64":
        body["content"] = "!!!!"
    assert b.write_allowed(path, body, branch, attempts, "expected", "original") is (bad is None)


def test_measured_zero_keeps_noindex_pages_observed_and_robots_unchanged():
    before, after = reports()
    result = b.checks(before, after)
    assert result["target_counts"] == {b.KEY: [3, 0], b.previous.KEY: [2, 1]}
    assert result["html_routes"] == [52, 52] and result["noindex_instructions_preserved"]
    assert not result["increased_counts"]


@pytest.mark.parametrize("bad", ["positive", "false_zero", "hidden_noindex", "lost_route", "robots", "robots_count",
    "title", "chain", "canonical_controls", "duplicate_controls", "redirect_controls", "incomplete", "settings"])
def test_false_zeros_lost_observations_and_changed_instructions_refuse(bad):
    before, after = reports()
    if bad == "positive":
        before["issues"][b.KEY]["examples"].pop()
    elif bad == "false_zero":
        after["issues"][b.KEY] = {"count": 1, "examples": [b.ALIAS]}
    elif bad in {"hidden_noindex", "lost_route"}:
        url = b.ALIAS if bad == "hidden_noindex" else after["pages"][0]["url"]
        after["pages"] = [row for row in after["pages"] if row["url"] != url]
    elif bad in {"robots", "robots_count", "chain"}:
        row = next(row for row in after["pages"] if row["url"] == b.ALIAS)
        row[{"robots": "meta_robots", "robots_count": "meta_robots_tag_count", "chain": "redirect_statuses"}[bad]] = {
            "robots": "index, follow", "robots_count": 0, "chain": [301]}[bad]
    elif bad == "title":
        after["pages"][0]["title"] = "changed"
    elif bad in {"canonical_controls", "duplicate_controls", "redirect_controls"}:
        bench = {"canonical_controls": b.previous.previous, "duplicate_controls": b.previous.previous.previous,
                 "redirect_controls": b.previous}[bad]
        after["issues"][bench.KEY]["examples"] = ["unrelated"]
    elif bad == "incomplete":
        after["meta"]["urls_uncrawled"] = 1
    else:
        after["meta"]["max_pages"] = 89
    with pytest.raises(ValueError):
        b.checks(before, after)


def test_increases_are_reported_not_hidden():
    before, after = reports()
    after["issues"]["example"] = {"count": 2}
    assert b.checks(before, after)["increased_counts"] == {"example": [0, 2]}
