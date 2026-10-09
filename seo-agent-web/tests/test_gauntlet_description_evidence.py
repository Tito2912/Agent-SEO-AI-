from __future__ import annotations

import pytest

from ops.gauntlet import live_cycle as bench


def page(description=None, tags=0, path="/a", **kw):
    return {"url": "https://fixture.test" + path, "status_code": 200,
            "content_type": "text/html", "meta_description": description,
            "meta_description_tag_count": tags, "x_robots_tag": "noindex", **kw}


def report(pages, key="missing_meta_description", count=1):
    return {"pages": pages, "meta": {"base_url": "https://fixture.test/", "max_pages": 90},
            "issues": {key: {"count": count, "examples": [row["url"] for row in pages]}}}


def check(old, new, key="missing_meta_description"):
    production, baseline, corrected = report([old], key), report([old], key), report([new], key, 0)
    return bench.html_checks(production, baseline, corrected, {bench.base(key)})[0]


@pytest.mark.parametrize("key,old", [
    ("missing_meta_description", page()),
    ("meta_description_too_short_indexable", page()),
    ("meta_description_too_short_not_indexable", page("court", 1)),
    ("meta_description_too_long", page("A" * 219, 1)),
    ("multiple_meta_description_tags", page("A" * 150, 2)),
])
def test_each_description_shape_requires_observed_html(key, old):
    result = check(old, page("B" * 150, 1), key)
    assert result["verdict"] == "resolved_on_observed_html"
    assert result["observations"][0]["baseline_defect_observed"]


@pytest.mark.parametrize("length", [100, 137, 155, 158, 160])
def test_hard_thresholds_not_the_preferred_window_decide_success(length):
    assert check(page(), page("A" * length, 1))["verdict"] == "resolved_on_observed_html"


@pytest.mark.parametrize("description,tags", [(None, 0), ("A" * 99, 1), ("A" * 161, 1),
                                              ("A" * 150, 0), ("A" * 150, 2)])
def test_a_zero_issue_counter_cannot_hide_bad_rendered_metadata(description, tags):
    assert check(page(), page(description, tags))["verdict"] == "still_present"


@pytest.mark.parametrize("tags", [None, "1", True])
def test_missing_or_invalid_tag_measurement_is_unknown(tags):
    assert check(page(), page("A" * 150, tags))["verdict"] == "unverified"


def test_a_healthy_baseline_is_not_a_positive_control():
    assert check(page("A" * 150, 1), page("B" * 150, 1))["verdict"] == "unverified"


@pytest.mark.parametrize("changes", [{"status_code": 404}, {"error": "fetch failed"},
                                      {"content_type": "application/json"},
                                      {"meta_robots": "noindex"}])
def test_failed_fetch_or_added_noindex_cannot_prove_a_description(changes):
    assert check(page(), page("A" * 150, 1, **changes))["verdict"] == "unverified"


def test_all_description_subfamilies_must_be_observed_without_double_counting():
    pages = [page(path="/a"), page("court", 1, path="/b"), page("A" * 219, 1, path="/c")]
    prod = report(pages)
    prod["issues"] = {key: {"count": 1, "examples": [row["url"]]}
                      for key, row in zip(("missing_meta_description", "meta_description_too_short",
                                           "meta_description_too_long"), pages)}
    prod["issues"]["meta_description_too_long_not_indexable"] = prod["issues"]["meta_description_too_long"]
    after = report([page("B" * 150, 1, path="/" + name) for name in ("a", "b", "c")])
    result = bench.html_checks(prod, prod, after, {"description"})[0]
    assert result["verdict"] == "resolved_on_observed_html"
    assert len(result["observations"]) == 3
    prod["issues"]["missing_meta_description"]["count"] = 2
    assert bench.html_checks(prod, prod, after, {"description"})[0]["verdict"] == "unverified"


def test_crawl_green_counter_cannot_override_incomplete_html_evidence(tmp_path):
    paths = [tmp_path / name for name in ("prod.json", "baseline.json", "after.json")]
    for path, data in zip(paths, (report([page()]), report([page()]),
                                 report([page("A" * 150, None)], count=0))):
        bench.save(path, data)
    result = bench.compare(*paths, {"results": [["missing_meta_description", "ok"]]})
    row = result["targeted_families"][0]
    assert row["verdict"] == "unverified"
    assert "rendered_description_evidence_not_resolved" in row["reasons"]


def test_rendered_unicode_characters_are_counted_not_utf8_bytes(tmp_path):
    paths = [tmp_path / name for name in ("prod.json", "baseline.json", "after.json")]
    for path, data in zip(paths, (report([page()]), report([page()]),
                                 report([page("\u00e9" * 150, 1)], count=0))):
        import json

        path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    result = bench.compare(*paths, {"results": [["missing_meta_description", "ok"]]})
    assert result["targeted_families"][0]["verdict"] == "resolved_in_preview_scope"
    assert result["html_field_checks"][0]["observations"][0]["after_length"] == 150
