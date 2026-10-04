"""Positive controls and source preservation for the canonical destination bench."""

import copy

import pytest

from ops.gauntlet import canonical_target_cycle as b
from ops.gauntlet.ai_budget import ClaudeBudget


def reports():
    before = {"pages": [{"url": b.URLS[name], "status_code": 200, "content_type": "text/html",
                         "canonical": b.BEFORE[name], "title": name} for name in b.NAMES],
              "issues": {key: {"count": 1, "evidence": {"kind": "url_pairs", "items": [
                  {"page": b.URLS[name], "from": b.BEFORE[name], "to": b.URLS["qa-master"]}]}}
                  for key, name in b.SCOPES.items()}}
    after = copy.deepcopy(before)
    for page in after["pages"]:
        if page["url"] in {b.URLS[name] for name in b.SCOPES.values()}:
            page["canonical"] = b.URLS["qa-master"]
    for key in after["issues"]:
        after["issues"][key] = {"count": 0, "evidence": {"kind": "url_pairs", "items": []}}
    return before, after


def test_nonfixture_pages_are_refused():
    with pytest.raises(ValueError):
        b.page("customer")


def test_nonzero_budget_is_refused_before_io(tmp_path):
    with pytest.raises(ValueError):
        b.cycle(None, "unused", tmp_path, ClaudeBudget(1))
    assert not list(tmp_path.iterdir())


def test_page_has_both_positive_controls_and_relative_navigation():
    for name in b.NAMES:
        source = b.page(name)
        assert 'rel="canonical" href="' + b.BEFORE[name] + '"' in source
        assert 'hreflang="fr" href="' + b.BEFORE[name] + '"' in source
        assert 'href="' + b.OLD + '"' in source
        assert b.previous.DEAD not in source


def test_two_positive_controls_are_required_and_route_set_preserved():
    before, after = reports()
    result = b.checks(before, after)
    assert result["selected_occurrences"] == {key: [1, 0] for key in b.SCOPES}
    assert not result["increased_counts"] and result["html_routes_before"] == result["html_routes_after"] == 5


@pytest.mark.parametrize("bad", ["missing", "unchanged", "collateral", "lost_route"])
def test_missing_witnesses_or_collateral_changes_cannot_pass(bad):
    before, after = reports()
    if bad == "missing":
        before["issues"][next(iter(b.SCOPES))]["evidence"]["items"] = []
    elif bad == "unchanged":
        after["pages"][0]["canonical"] = b.BEFORE[b.NAMES[0]]
    elif bad == "collateral":
        after["pages"][2]["title"] = "Unexpected edit"
    else:
        after["pages"].pop()
    with pytest.raises(ValueError):
        b.checks(before, after)


def test_increased_counters_are_retained_not_presented_as_all_green():
    before, after = reports()
    after["issues"]["hreflang_to_non_canonical"] = {"count": 2}
    assert b.checks(before, after)["increased_counts"] == {"hreflang_to_non_canonical": [0, 2]}
