"""Missing-entry acceptance cannot pass by losing 404 witnesses or changing healthy pages."""

import base64
import copy

import pytest

from ops.gauntlet import sitemap_error_cycle as b
from ops.gauntlet.ai_budget import ClaudeBudget
from tests.test_gauntlet_sitemap_noindex_cycle import reports as previous_reports


def sources():
    return {"sitemap.xml": '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\r\n' + '\r\n'.join(
        '<url><loc>' + b.SITE + 'control-' + str(i) + '</loc></url>' for i in range(48)) + '\r\n</urlset>\r\n',
        "index.html": '<html><body>Owned fixture</body></html>\r\n'}


def reports():
    _, before = previous_reports()
    before["pages"] += [{"url": url, "final_url": url, "status_code": 404, "content_type": "text/html",
                         "redirect_chain": [], "redirect_statuses": []} for url in sorted(b.TARGETS)]
    before["issues"][b.KEY] = {"count": 2, "examples": sorted(b.TARGETS)}
    after = copy.deepcopy(before)
    after["issues"][b.KEY] = {"count": 0, "examples": []}
    return before, after


def test_nonzero_provider_budget_refuses_before_remote_io(tmp_path):
    with pytest.raises(ValueError):
        b.cycle(None, "unused", tmp_path, ClaudeBudget(1))
    assert not list(tmp_path.iterdir())


def test_setup_preserves_sources_and_cleanup_removes_exactly_two_entries():
    from backend import app as m
    original = sources()
    setup = b.setup_sources(original)
    expected = b.expected_source(setup["sitemap.xml"])
    assert m._remove_sitemap_locs(setup["sitemap.xml"], sorted(b.TARGETS), verified=True) == (expected, 2)
    assert len(b.locs(expected)) == 48
    assert all(url.rsplit('/', 1)[-1] in setup["index.html"] for url in b.TARGETS)


@pytest.mark.parametrize("bad", ["missing_path", "count", "close", "existing", "namespace"])
def test_drifted_setup_refuses(bad):
    values = sources()
    if bad == "missing_path":
        values.pop("index.html")
    elif bad == "count":
        values["sitemap.xml"] = values["sitemap.xml"].replace('</urlset>', b.entry(next(iter(b.TARGETS))) + '</urlset>')
    elif bad == "close":
        values["index.html"] = '<body>unfinished'
    elif bad == "existing":
        values["index.html"] += '<!-- qa-sitemap-missing-a already present -->'
    else:
        values["sitemap.xml"] = values["sitemap.xml"].replace("http://www.sitemaps.org/schemas/sitemap/0.9", "urn:wrong")
    with pytest.raises(ValueError):
        b.setup_sources(values)


@pytest.mark.parametrize("bad", ["count", "missing", "duplicate", "bytes"])
def test_drifted_cleanup_refuses(bad):
    value = b.setup_sources(sources())["sitemap.xml"]
    url = next(iter(b.TARGETS))
    if bad == "count":
        value = value.replace('</urlset>', '<url><loc>' + b.SITE + 'extra</loc></url></urlset>')
    elif bad == "missing":
        value = value.replace(url, b.SITE + "different")
    elif bad == "duplicate":
        value = value.replace(b.SITE + "control-0", url)
    else:
        value = value.replace('<loc>' + url, '<loc> ' + url)
    with pytest.raises(ValueError):
        b.expected_source(value)


@pytest.mark.parametrize("bad", [None, "file", "main", "branch", "retry", "sha", "bytes", "base64"])
def test_write_guard_requires_exact_qa_scope_sha_and_bytes_without_retry(bad):
    path, branch, attempts = "sitemap.xml", b.PREFIX + "correction-test", 0
    body = {"branch": branch, "sha": "original", "content": base64.b64encode(b"expected").decode()}
    if bad == "file":
        path = "page.html"
    elif bad == "main":
        branch = body["branch"] = "main"
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
    assert b.write_allowed(path, body, branch, attempts, {"sitemap.xml": "expected"}, "original") is (bad is None)


def test_measured_zero_preserves_all_healthy_routes_and_missing_witnesses():
    before, after = reports()
    result = b.checks(before, after)
    assert result["target_counts"] == {b.KEY: [2, 0]} and result["html_routes"] == [52, 52]
    assert result["missing_pages_still_observed"] and not result["increased_counts"]


@pytest.mark.parametrize("bad", ["positive", "false_zero", "hidden_missing", "missing_returned", "redirected", "lost_route",
    "title", "noindex_family", "noindex_instruction", "redirect_family", "canonical_controls", "duplicate_controls", "incomplete", "settings"])
def test_false_success_or_collateral_change_refuses(bad):
    before, after = reports()
    if bad == "positive":
        before["issues"][b.KEY]["examples"].pop()
    elif bad == "false_zero":
        after["issues"][b.KEY] = {"count": 1, "examples": [next(iter(b.TARGETS))]}
    elif bad == "hidden_missing":
        after["pages"] = [row for row in after["pages"] if row["url"] != next(iter(b.TARGETS))]
    elif bad in {"missing_returned", "redirected"}:
        row = next(row for row in after["pages"] if row["url"] in b.TARGETS)
        row["status_code" if bad == "missing_returned" else "redirect_chain"] = 200 if bad == "missing_returned" else [row["url"]]
    elif bad == "lost_route":
        after["pages"].pop(0)
    elif bad == "title":
        after["pages"][0]["title"] = "changed"
    elif bad == "noindex_instruction":
        next(row for row in after["pages"] if row["url"] in b.previous.TARGETS)["meta_robots"] = "index, follow"
    elif bad in {"noindex_family", "redirect_family", "canonical_controls", "duplicate_controls"}:
        bench = {"noindex_family": b.previous, "redirect_family": b.previous.previous,
                 "canonical_controls": b.previous.previous.previous, "duplicate_controls": b.previous.previous.previous.previous}[bad]
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
