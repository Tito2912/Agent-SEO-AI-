from __future__ import annotations

import copy
from types import SimpleNamespace

import pytest

from backend import app as m
from ops.gauntlet import hreflang_ai_cycle as bench
from ops.gauntlet.ai_budget import ClaudeBudget

SITE = "https://fixture.test/"
CONTROL = "/gauntlet/qa-reciprocal-en/"
FR = SITE + "gauntlet/qa-reciprocal-fr/"


def page(route, alternates=None, **kw):
    return {"url": SITE.rstrip("/") + route, "final_url": SITE.rstrip("/") + route,
            "status_code": 200, "content_type": "text/html", "canonical": SITE.rstrip("/") + route,
            "hreflang": alternates or {},
            "hreflang_raw": [{"hreflang": c, "href": url} for c, url in (alternates or {}).items()], **kw}


def reports():
    broken = page("/invalid/", {"fr_fr": SITE + "invalid/"})
    control = page(CONTROL, {"en": SITE.rstrip("/") + CONTROL, "fr": FR})
    before = {"pages": [broken, control, page("/gauntlet/qa-reciprocal-fr/"), page("/")],
              "issues": {bench.KEYS[0]: {"count": 1, "examples": [broken["url"]]},
                         bench.KEYS[1]: {"count": 2, "examples": [broken["url"], control["url"]]}}}
    after = copy.deepcopy(before)
    after["pages"][0].update(page("/invalid/", {"fr-fr": SITE + "invalid/", "x-default": SITE + "invalid/"}))
    after["pages"][1].update(page(CONTROL, {**control["hreflang"], "x-default": FR}))
    return before, after


def verdicts(before, after):
    return {r["family"]: r["verdict"] for r in bench.html_checks(before, after, SITE)}


def test_both_families_require_and_resolve_literal_positive_controls():
    before, after = reports()
    assert set(verdicts(before, after).values()) == {"resolved_on_observed_html"}


@pytest.mark.parametrize("mutate", [
    lambda p: p[1]["hreflang"].update({"x-default": SITE}),
    lambda p: p[1]["hreflang"].update({"fr": SITE}),
    lambda p: p[1]["hreflang_raw"].append({"hreflang": "x-default", "href": FR}),
    lambda p: p[1].update(canonical=SITE),
    lambda p: p[1].update(title="Unrequested title edit"),
    lambda p: p[1].update(meta_robots="noindex"),
    lambda p: p[1].update(x_robots_tag="noindex, follow"),
    lambda p: p[2].update(meta_robots="noindex"),
    lambda p: p[2].update(status_code=202),
    lambda p: p[2].update(canonical=SITE),
    lambda p: p[1].update(hreflang_raw=[]),
])
def test_zero_counters_cannot_hide_semantically_wrong_default_or_collateral_edits(mutate):
    before, after = reports()
    mutate(after["pages"])
    after["issues"] = {k: {"count": 0} for k in bench.KEYS}
    assert verdicts(before, after)[bench.KEYS[1]] == "unverified"


@pytest.mark.parametrize("alternates", [{}, {"fr-fr": SITE}, {"fr_fr": SITE + "invalid/"},
                                       {"fr-fr": SITE + "invalid/", "de": SITE + "de/"}])
def test_invalid_code_must_be_repaired_not_dropped_or_redirected(alternates):
    before, after = reports()
    after["pages"][0]["hreflang"] = alternates
    assert verdicts(before, after)[bench.KEYS[0]] == "unverified"


def test_missing_successful_html_observation_is_not_a_repair():
    before, after = reports()
    after["pages"] = after["pages"][2:]
    assert set(verdicts(before, after).values()) == {"unverified"}


def test_absent_positive_control_is_not_certified():
    before, after = reports()
    before["issues"] = {k: {"count": 0} for k in bench.KEYS}
    assert set(verdicts(before, after).values()) == {"unverified"}


def test_incomplete_baseline_example_list_is_not_certified():
    before, after = reports()
    before["issues"][bench.KEYS[1]]["count"] = 3
    assert verdicts(before, after)[bench.KEYS[1]] == "unverified"


@pytest.mark.parametrize("stack", ["hugo", "nuxt"])
def test_only_one_existing_control_default_is_removed(stack):
    source = next(v for k, v in bench.previous.seed_pages(stack).items() if "-en." in k)
    expected = f"https://noyaru-stack-{stack}.netlify.app/gauntlet/qa-reciprocal-fr/"
    patched = bench.remove_control_default(m, source, stack, expected)
    assert "x-default" not in patched
    assert patched == "".join(line for line in source.splitlines(keepends=True) if "x-default" not in line)
    assert m._canonical_ecrit_dans(patched) == m._canonical_ecrit_dans(source)
    assert m._unbalanced_delimiters(patched) == ""
    with pytest.raises(ValueError, match="Exactly one"):
        bench.remove_control_default(m, patched, stack, expected)
    with pytest.raises(ValueError, match="Exactly one"):
        bench.remove_control_default(m, source, stack, SITE + "wrong/")


def test_customer_repository_is_rejected_before_io(tmp_path):
    with pytest.raises(ValueError, match="Hugo/Nuxt"):
        bench.cycle(None, "unused", "customer", tmp_path, ClaudeBudget(0))
    assert list(tmp_path.iterdir()) == []


def test_changed_seed_is_refused_before_any_write(tmp_path, monkeypatch):
    module = SimpleNamespace(_github_api_get=lambda *a, **kw: {"object": {"sha": "changed"}},
                             _github_ref_api_path=lambda *a: "/".join(a))
    monkeypatch.setattr(bench.live, "close_prs", lambda *a: [])
    result = bench.cycle(module, "unused", "hugo", tmp_path, ClaudeBudget(0))
    assert result["status"] == "failed" and result["error_type"] == "ValueError"
    assert result["pull_requests"] == []
    assert result["ai_budget"]["attempted"] == 0
