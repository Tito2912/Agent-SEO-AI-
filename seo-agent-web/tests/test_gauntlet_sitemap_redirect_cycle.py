"""A sitemap redirect acceptance run cannot pass by hiding refused URLs or losing routes."""

import base64
import copy

import pytest

from ops.gauntlet import sitemap_redirect_cycle as b
from ops.gauntlet.ai_budget import ClaudeBudget


def sources():
    urls = sorted({b.SITE, *b.TARGETS.values()})
    urls += [b.SITE + "control-" + str(i) for i in range(49 - len(urls))]
    return {"sitemap.xml": '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\r\n' + "\r\n".join(
        '<url><loc>' + url + '</loc></url>' for url in urls) + '\r\n</urlset>\r\n',
        "index.html": '<html><body><p>Existing fixture</p></body></html>\r\n',
        "_redirects": '/gauntlet/ancienne-page /gauntlet/ 301\n/gauntlet/qa-hop-old /gauntlet/qa-master 301!\n'}


def reports():
    urls = sorted({b.SITE, *b.TARGETS.values(), *b.NEGATIVES.values(), *b.previous.NEGATIVES, *b.previous.previous.NEGATIVES})
    urls += [b.SITE + "control-" + str(i) for i in range(52 - len(urls))]
    pages = [{"url": url, "final_url": url, "status_code": 200, "content_type": "text/html", "canonical": url,
              "title": url, "redirect_chain": [], "redirect_statuses": []} for url in urls]
    direct = {row["url"]: row for row in pages}
    for url, target in b.ALIASES.items():
        chain, codes = [url], [301] if url in b.TARGETS else [302]
        if url.endswith("qa-sitemap-chain"):
            chain, codes = [url, b.SITE + "gauntlet/qa-hop-old"], [302, 301]
        pages.append(dict(direct[target], url=url, final_url=target, redirect_chain=chain, redirect_statuses=codes))
    before = {"meta": {"max_pages": 90, "urls_uncrawled": 0, "stopped_on_time_budget": False}, "pages": pages,
        "issues": {b.KEY: {"count": 5, "examples": sorted(b.ALIASES)},
                   b.previous.KEY: {"count": 2, "examples": sorted(b.previous.NEGATIVES)},
                   b.previous.previous.KEY: {"count": 2, "examples": sorted(b.previous.previous.NEGATIVES)}}}
    after = copy.deepcopy(before)
    after["issues"][b.KEY] = {"count": 2, "examples": sorted(b.NEGATIVES)}
    return before, after


def test_nonzero_model_budget_refuses_before_remote_io(tmp_path):
    with pytest.raises(ValueError):
        b.cycle(None, "unused", tmp_path, ClaudeBudget(1))
    assert not list(tmp_path.iterdir())


def test_exact_setup_and_cleanup_preserve_original_masters_rules_and_refused_entries():
    original = sources()
    setup = b.setup_sources(original)
    expected = b.corrected_source(setup["sitemap.xml"])
    from backend import app as m
    assert m._rewrite_sitemap_locs(setup["sitemap.xml"], b.PAIRS, canonical=True) == (expected, 3)
    assert len(b.previous.locs(setup["sitemap.xml"])) == 54 and len(b.previous.locs(expected)) == 51
    assert setup["_redirects"] == b.RULES + original["_redirects"]
    assert all(b.alias_entry(url) in expected and urlsplit_path(url) in setup["index.html"] for url in b.NEGATIVES)


def urlsplit_path(url):
    from urllib.parse import urlsplit
    return urlsplit(url).path


@pytest.mark.parametrize("bad", ["missing_path", "wrong_count", "alias_present", "master_missing", "no_close", "existing_controls", "namespace"])
def test_ambiguous_or_drifted_setup_refuses(bad):
    original = sources()
    if bad == "missing_path":
        original.pop("_redirects")
    elif bad == "wrong_count":
        original["sitemap.xml"] = original["sitemap.xml"].replace('</urlset>', '<url><loc>' + b.SITE + 'extra</loc></url></urlset>')
    elif bad in {"alias_present", "master_missing"}:
        url = next(iter(b.TARGETS)) if bad == "alias_present" else b.SITE + "unlisted"
        original["sitemap.xml"] = original["sitemap.xml"].replace(next(iter(b.TARGETS.values())), url)
    elif bad == "no_close":
        original["index.html"] = "<body>unfinished"
    elif bad == "existing_controls":
        original["_redirects"] += "# qa-sitemap- controls already present\n"
    else:
        original["sitemap.xml"] = original["sitemap.xml"].replace("http://www.sitemaps.org/schemas/sitemap/0.9", "urn:wrong")
    with pytest.raises(ValueError):
        b.setup_sources(original)


@pytest.mark.parametrize("bad", [None, "file", "main", "branch", "retry", "sha", "bytes", "base64"])
def test_write_guard_requires_exact_owned_branch_path_sha_bytes_and_no_retry(bad):
    path, branch, attempts = "sitemap.xml", b.PREFIX + "correction-test", 0
    body = {"branch": branch, "sha": "original", "content": base64.b64encode(b"expected").decode()}
    if bad == "file":
        path = "index.html"
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


def test_only_three_verified_entries_are_fixed_with_all_redirects_still_visible():
    before, after = reports()
    result = b.checks(before, after)
    assert result["target_counts"] == {b.KEY: [5, 2]} and result["selected_occurrences"] == [3, 0]
    assert result["html_routes"] == [52, 52] and result["redirect_chain_observations_preserved"]
    assert not result["increased_counts"]


@pytest.mark.parametrize("bad", ["positive", "false_zero", "negative", "hidden_redirect", "lost_route", "collateral",
    "chain", "duplicate_controls", "canonical_controls", "incomplete", "settings"])
def test_false_zeros_lost_observations_and_collateral_changes_refuse(bad):
    before, after = reports()
    if bad == "positive":
        before["issues"][b.KEY]["examples"].pop()
    elif bad == "false_zero":
        after["issues"][b.KEY] = {"count": 0, "examples": []}
    elif bad == "negative":
        after["issues"][b.KEY]["examples"].pop()
    elif bad == "hidden_redirect":
        after["pages"] = [row for row in after["pages"] if row["url"] != next(iter(b.TARGETS))]
    elif bad == "lost_route":
        after["pages"] = [row for row in after["pages"] if row["url"] != b.SITE]
    elif bad == "collateral":
        after["pages"][0]["title"] = "changed"
    elif bad == "chain":
        after["pages"][-1]["redirect_statuses"] = [308]
    elif bad in {"duplicate_controls", "canonical_controls"}:
        key = b.previous.previous.KEY if bad == "duplicate_controls" else b.previous.KEY
        after["issues"][key]["examples"] = ["unrelated"]
    elif bad == "incomplete":
        after["meta"]["urls_uncrawled"] = 1
    else:
        after["meta"]["max_pages"] = 89
    with pytest.raises(ValueError):
        b.checks(before, after)


def test_increases_are_reported_not_disguised_as_all_green():
    before, after = reports()
    after["issues"]["example"] = {"count": 2}
    assert b.checks(before, after)["increased_counts"] == {"example": [0, 2]}
