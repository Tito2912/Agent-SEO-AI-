from __future__ import annotations

from types import SimpleNamespace

import pytest

from ops.gauntlet import live_cycle as bench


def report(tmp_path, name, *, issues=None, pages=None, meta=None):
    path = tmp_path / name
    bench.save(path, {"issues": {k: {"count": n, "examples": ["https://fixture.test/broken"]}
                                 for k, n in (issues or {}).items()},
                      "pages": pages if pages is not None else [page()],
                      "meta": {"max_pages": 90, **(meta or {})}})
    return path


def page(path="/broken", **kw):
    return {"url": "https://fixture.test" + path, "status_code": 200,
            "content_type": "text/html", "x_robots_tag": "noindex", **kw}


def comparison(tmp_path, *, before=1, after=0, before_kw=None, after_kw=None):
    prod = report(tmp_path, "prod.json", issues={"missing_title": 1})
    baseline = report(tmp_path, "baseline.json", issues={"missing_title": before}, **(before_kw or {}))
    corrected = report(tmp_path, "corrected.json", issues={"missing_title": after}, **(after_kw or {}))
    return bench.compare(prod, baseline, corrected,
                         {"results": [["missing_title", "ok", "", 1, 1]]})


def test_positive_control_is_required_even_with_a_healthy_preview(tmp_path):
    row = comparison(tmp_path, before=0)["targeted_families"][0]
    assert row["verdict"] == "unverified"
    assert "defect_absent_from_unchanged_preview" in row["reasons"]


def test_observed_repair_is_measured_within_preview_scope(tmp_path):
    result = comparison(tmp_path)
    assert result["comparable"]
    assert result["targeted_families"][0]["verdict"] == "resolved_in_preview_scope"
    assert result["increases"] == []


@pytest.mark.parametrize("kw,reason", [
    ({"pages": [page(status_code=404)]}, "affected_html_pages_not_successfully_revisited"),
    ({"pages": [page(error="browser unavailable")]}, "too_few_successful_html_pages"),
    ({"pages": [page(content_type="application/json")]}, "too_few_successful_html_pages"),
    ({"pages": [page("/other")]}, "affected_html_pages_not_successfully_revisited"),
    ({"meta": {"max_pages": 30}}, "different_crawl_settings"),
    ({"meta": {"resources_checked": True}}, "different_crawl_settings"),
    ({"meta": {"thresholds": {"title_too_short_chars": 5}}}, "different_crawl_settings"),
    ({"meta": {"urls_uncrawled": 1}}, "incomplete_crawl"),
    ({"meta": {"stopped_on_time_budget": True}}, "incomplete_crawl"),
    ({"meta": {"blocked_by_host": {"count": 1}}}, "blocked_by_host"),
    ({"pages": [page(meta_robots="noindex, follow")]}, "added_noindex_on_affected_page"),
])
def test_disappearing_alert_is_not_enough(tmp_path, kw, reason):
    row = comparison(tmp_path, after_kw=kw)["targeted_families"][0]
    assert row["verdict"] == "unverified"
    assert reason in row["reasons"]


def test_counts_fold_indexability_without_double_counting(tmp_path):
    prod = report(tmp_path, "prod.json")
    baseline = report(tmp_path, "b.json", issues={"missing_h1": 2, "missing_h1_indexable": 2})
    after = report(tmp_path, "a.json", issues={"missing_h1_not_indexable": 1})
    result = bench.compare(prod, baseline, after, {"results": [["missing_h1_indexable", "ok"]]})
    assert result["targeted_families"][0]["preview_before"] == 2
    assert result["targeted_families"][0]["preview_after"] == 1
    assert result["targeted_families"][0]["verdict"] == "partial"


def test_collateral_increases_are_reported(tmp_path):
    prod = report(tmp_path, "prod.json")
    baseline = report(tmp_path, "b.json", issues={"missing_title": 1})
    after = report(tmp_path, "a.json", issues={"title_too_long": 1})
    result = bench.compare(prod, baseline, after, {"results": [["missing_title", "ok"]]})
    assert {"family": "title_too_long", "before": 0, "after": 1} in result["increases"]


def test_host_changes_do_not_make_identical_routes_disappear(tmp_path):
    result = comparison(tmp_path, after_kw={"pages": [page(url="https://other-preview.test/broken")]})
    assert result["targeted_families"][0]["verdict"] == "resolved_in_preview_scope"


def test_route_case_query_and_slash_are_significant():
    assert len({bench._route("https://host" + p) for p in ("/A", "/a", "/a/", "/a?q=1")}) == 4


def test_unrelated_green_status_cannot_prove_a_build():
    assert bench.preview_status([{"context": "CI tests", "state": "success"}]) == "pending"


def test_latest_preview_build_status_wins():
    statuses = [{"id": 1, "context": "netlify/site/deploy-preview", "state": "success"},
                {"id": 2, "context": "netlify/site/deploy-preview", "state": "failure"}]
    assert bench.preview_status(statuses) == "failure"


@pytest.mark.parametrize("state,error", [("failure", RuntimeError), ("pending", TimeoutError)])
def test_bad_or_missing_preview_build_never_reaches_crawl(state, error):
    def get(path, **kw):
        return ({"head": {"sha": "exact-head"}} if "pulls" in path
                else {"statuses": [{"id": 1, "context": "netlify/site/deploy-preview", "state": state,
                                    "target_url": "https://deploy-preview-10--fixture.netlify.app"}]})
    module = SimpleNamespace(_github_api_get=get, _github_api_path=lambda *p: "/".join(p))
    with pytest.raises(error):
        bench.wait_build(module, "fixture", "not-a-real-token", 10, timeout=0)


def test_green_status_for_another_pr_at_the_same_sha_cannot_prove_this_preview():
    def get(path, **kw):
        return ({"head": {"sha": "reused-sha"}} if "pulls" in path else {"statuses": [
            {"id": 1, "context": "netlify/site/deploy-preview", "state": "success",
             "target_url": "https://deploy-preview-9--fixture.netlify.app"}]})
    module = SimpleNamespace(_github_api_get=get, _github_api_path=lambda *p: "/".join(p))
    with pytest.raises(TimeoutError):
        bench.wait_build(module, "fixture", "unused", 10, timeout=0)


def test_current_preview_status_must_match_the_exact_https_host():
    host = "deploy-preview-10--fixture.netlify.app"
    status = {"id": 1, "context": "netlify/site/deploy-preview", "state": "success"}
    for url in ("", "http://" + host, "https://" + host + ".example.com", "https://example.com/" + host):
        assert bench.preview_status([{**status, "target_url": url}], preview_host=host) == "pending"
    assert bench.preview_status([{**status, "target_url": "https://" + host}], preview_host=host) == "success"


def test_failed_build_closes_all_created_prs_without_merging(tmp_path, monkeypatch):
    work = tmp_path / "astro"
    work.mkdir()
    report(work, "dummy.json")
    (work / "before").mkdir()
    report(work / "before", "report.json")
    bench.save(work / "gauntlet_run.json", {"branch": "gauntlet-astro-test", "results": []})
    calls = []
    created = iter((11, 12))

    def open_pr(**kw):
        calls.append(kw)
        return {"number": next(created), "html_url": "https://github.com/fixture"}

    module = SimpleNamespace(
        _github_api_get=lambda *a, **kw: {"object": {"sha": "main-sha"},
                                        "merge_base_commit": {"sha": "main-sha"}},
        _github_api_path=lambda *p: "/".join(p),
        _github_ref_api_path=lambda *p: "/".join(p),
        _github_content_api_path=lambda *p: "/".join(p),
        _github_api_post=lambda *a, **kw: calls.append(kw),
        _github_api_put=lambda *a, **kw: calls.append(kw),
        _github_branch_allowed=lambda b: True,
        _ouvrir_pull_request=open_pr,
    )
    def fail(*args, **kw):
        raise RuntimeError("build failed")

    closed = []
    def close(repo, token, prs):
        closed.extend(pr["number"] for pr in prs)
        return [{"number": pr["number"], "state": "closed", "merged": False} for pr in prs]

    monkeypatch.setattr(bench, "wait_build", fail)
    monkeypatch.setattr(bench, "close_prs", close)
    result = bench.cycle(module, "test-token", "astro", tmp_path, reuse=True)
    assert result["status"] == "failed"
    assert closed == [11, 12]
    assert result["pull_requests_closed_without_merge"]
    assert all(kw["draft"] and kw["base"] == "main" for kw in calls if "draft" in kw)
    assert bench.load(work / "cycle.json")["cleanup"] == result["cleanup"]


def test_cleanup_failure_is_not_a_success(monkeypatch):
    def failed(*args, **kw):
        raise requests_error("not printed")
    requests_error = bench.requests.RequestException
    monkeypatch.setattr(bench.requests, "patch", failed)
    result = bench.close_prs("fixture", "test-token", [{"number": 11}])
    assert result == [{"number": 11, "cleanup_error": "RequestException"}]


def test_customer_repo_cannot_be_passed_to_the_cycle(tmp_path):
    with pytest.raises(ValueError, match="allowlisted"):
        bench.cycle(None, "unused", "customer", tmp_path)


@pytest.mark.parametrize("limit", [-1, 101])
def test_invalid_ai_budget_cannot_reach_github(tmp_path, limit):
    with pytest.raises(ValueError, match="Claude call limit"):
        bench.cycle(None, "unused", "astro", tmp_path, ai_max_calls=limit)


def field_report(key, pages, count=1):
    return {"meta": {"base_url": "https://fixture.test/"}, "pages": pages,
            "issues": {key: {"count": count, "examples": ["https://fixture.test/broken"]}}}


@pytest.mark.parametrize("served,alternates,status,expected", [
    ("en", {"en": "https://fixture.test/broken"}, 200, "resolved_on_observed_html"),
    ("fr", {"en": "https://fixture.test/broken"}, 200, "still_present"),
    ("en", {}, 200, "still_present"),
    ("en", {"en": "https://another-site.test/broken"}, 200, "still_present"),
    ("en", {"en": "https://fixture.test/broken"}, 404, "unverified"),
])
def test_served_language_is_measured_without_trusting_a_muted_counter(served, alternates, status, expected):
    key = "served_html_lang_mismatch"
    old = page(served_lang="fr", hreflang={"en": "https://fixture.test/broken"})
    prod = field_report(key, [old])
    baseline = field_report(key, [old], count=0)
    corrected = field_report(key, [page(served_lang=served, hreflang=alternates, status_code=status)], count=0)
    assert bench.html_checks(prod, baseline, corrected, {key})[0]["verdict"] == expected


@pytest.mark.parametrize("canonical,expected", [
    ("https://fixture.test/broken", "resolved_on_observed_html"),
    ("https://fixture.test/absent", "still_present"),
    ("https://malicious.test/broken", "still_present"),
    ("http://fixture.test/broken", "still_present"),
])
def test_canonical_requires_an_observed_healthy_destination(canonical, expected):
    key = "canonical_points_to_4xx"
    old = page(canonical="https://fixture.test/absent")
    prod = field_report(key, [old, page("/absent", status_code=404)])
    baseline = field_report(key, [old], count=0)
    corrected = field_report(key, [page(canonical=canonical)], count=0)
    assert bench.html_checks(prod, baseline, corrected, {key})[0]["verdict"] == expected


@pytest.mark.parametrize("links,expected", [
    (["https://fixture.test/blog"], "resolved_on_observed_html"),
    (["http://fixture.test/blog"], "still_present"),
    ([], "still_present"),
])
def test_http_link_requires_its_https_replacement_not_just_deletion(links, expected):
    key = "https_page_has_internal_links_to_http"
    old = page(internal_links=["http://fixture.test/blog"])
    prod = field_report(key, [old])
    baseline = field_report(key, [old], count=0)
    corrected = field_report(key, [page(external_links=links)], count=0)
    assert bench.html_checks(prod, baseline, corrected, {key})[0]["verdict"] == expected


def test_capped_scope_cannot_prove_all_field_repairs():
    key = "served_html_lang_mismatch"
    old = page(served_lang="fr", hreflang={"en": "https://fixture.test/broken"})
    prod = field_report(key, [old], count=10)
    after = field_report(key, [page(served_lang="en", hreflang=old["hreflang"])])
    assert bench.html_checks(prod, prod, after, {key})[0]["verdict"] == "unverified"


def test_one_repaired_occurrence_cannot_hide_a_new_one_at_the_same_count(tmp_path):
    prod = report(tmp_path, "prod.json")
    before = report(tmp_path, "b.json", issues={"page_in_multiple_sitemaps": 1})
    after = report(tmp_path, "a.json", issues={"page_in_multiple_sitemaps": 1})
    data = bench.load(after)
    data["issues"]["page_in_multiple_sitemaps"]["examples"] = ["https://fixture.test/new-duplicate"]
    bench.save(after, data)
    result = bench.compare(prod, before, after, {"results": []})
    assert result["increases"] == []
    assert {"family": "page_in_multiple_sitemaps", "routes": ["/new-duplicate"]} in result["new_issue_routes"]


@pytest.mark.parametrize("comparable,increases,added,exhausted,expected", [
    (True, [], [], False, "measured"),
    (False, [], [], False, "unverified"),
    (True, [{"family": "missing_title"}], [], False, "regression_detected"),
    (True, [], [{"family": "page_in_multiple_sitemaps", "routes": ["/new"]}], False, "regression_detected"),
    (True, [], [], True, "unverified"),
])
def test_cycle_exit_status_does_not_hide_a_bad_measurement(tmp_path, monkeypatch, comparable, increases, added, exhausted, expected):
    work = tmp_path / "astro"
    (work / "before").mkdir(parents=True)
    report(work / "before", "report.json")
    bench.save(work / "gauntlet_run.json", {"branch": "gauntlet-astro-test", "results": [],
                                           "ai_budget": {"exhausted": exhausted}})
    module = SimpleNamespace(
        _github_api_get=lambda *a, **kw: {"object": {"sha": "main-sha"},
                                        "merge_base_commit": {"sha": "main-sha"}},
        _github_api_path=lambda *p: "/".join(p),
        _github_ref_api_path=lambda *p: "/".join(p),
        _github_content_api_path=lambda *p: "/".join(p),
        _github_api_post=lambda *a, **kw: {}, _github_api_put=lambda *a, **kw: {},
        _github_branch_allowed=lambda b: True,
        _ouvrir_pull_request=lambda **kw: {"number": 1, "html_url": "https://fixture.test/pr"},
    )
    monkeypatch.setattr(bench, "wait_build", lambda *a, **kw: {"head_sha": "s"})
    monkeypatch.setattr(bench, "crawl", lambda *a, **kw: None)
    monkeypatch.setattr(bench, "compare", lambda *a, **kw: {"comparable": comparable,
                                                         "increases": increases, "new_issue_routes": added})
    monkeypatch.setattr(bench, "close_prs", lambda *a: [{"state": "closed", "merged": False}])
    assert bench.cycle(module, "test-token", "astro", tmp_path, reuse=True)["status"] == expected


def test_stale_fixture_branch_is_refused_before_any_pr_is_created(tmp_path, monkeypatch):
    work = tmp_path / "astro"
    (work / "before").mkdir(parents=True)
    report(work / "before", "report.json")
    bench.save(work / "gauntlet_run.json", {"branch": "gauntlet-astro-test", "results": []})
    module = SimpleNamespace(
        _github_api_get=lambda *a, **kw: {"object": {"sha": "new-main"},
                                        "merge_base_commit": {"sha": "old-main"}},
        _github_api_path=lambda *p: "/".join(p), _github_ref_api_path=lambda *p: "/".join(p),
        _github_branch_allowed=lambda b: True,
    )
    monkeypatch.setattr(bench, "close_prs", lambda *a: [])
    result = bench.cycle(module, "test-token", "astro", tmp_path, reuse=True)
    assert result["status"] == "failed"
    assert result["error_type"] == "ValueError"
    assert result["pull_requests"] == []
