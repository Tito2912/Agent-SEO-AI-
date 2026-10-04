"""Named records, including failed records, must never be promoted to certification."""

from types import SimpleNamespace

import pytest

from ops.gauntlet.acceptance_inventory import family, inventory, names


@pytest.mark.parametrize("key", ["missing_h1", "missing_h1_indexable", "missing_h1_not_indexable"])
def test_variants_are_grouped_without_merging_distinct_families(key):
    assert family(key) == "missing_h1"
    assert family("missing_meta_description") != "meta_description_too_short"


def backend():
    dash = SimpleNamespace(ISSUE_CATALOG={key: None for key in (
        "missing_h1", "missing_h1_indexable", "missing_h1_not_indexable", "missing_canonical", "slow_page", "missing_title")},
        NON_ISSUE_KEYS={"missing_canonical"}, is_delta_issue_key=lambda key: False)
    return SimpleNamespace(dash=dash, _github_issue_auto_fixable=lambda key: key != "slow_page")


def test_failed_partial_or_zero_count_records_are_not_success():
    records = {"failed.json": {"status": "failed", "family": "missing_h1"},
               "zero.json": {"issues": {"missing_title": {"count": 0}}},
               "partial.json": {"family": "missing_title", "verdict": "partial"}}
    result = inventory(backend(), records)
    assert result["catalog_key_count"] == 6
    assert result["claimed_family_count"] == 3 and result["visible_offered_family_count"] == 2
    assert result["client_readiness_certified"] is False
    rows = {row["family"]: row for row in result["families"]}
    assert rows["missing_h1"]["named_validation_records"] == ["failed.json"]
    assert rows["missing_title"]["named_validation_records"] == ["partial.json", "zero.json"]
    assert rows["missing_canonical"]["visible_offered_keys"] == []
    assert not any(row["all_occurrences_or_stacks_certified"] for row in rows.values())
    assert "slow_page" not in rows


def test_prose_or_longer_key_substrings_are_not_named_records():
    result = inventory(backend(), {"note.json": {"note": "missing_h1 works everywhere", "family": "missing_h1_other"}})
    assert result["families_without_named_record"] == ["missing_canonical", "missing_h1", "missing_title"]


def test_structured_arrays_and_indexability_variants_are_recognized():
    assert "missing_h1" in names({"results": ["missing_h1_not_indexable", {"family": "missing_title"}]})


def test_variant_dictionary_keys_are_also_grouped():
    assert "missing_h1" in names({"issues": {"missing_h1_indexable": {"count": 0}}})


def test_output_order_is_deterministic():
    a = inventory(backend(), {"b.json": {"family": "missing_title"}, "a.json": {"family": "missing_title"}})
    b = inventory(backend(), {"a.json": {"family": "missing_title"}, "b.json": {"family": "missing_title"}})
    assert a == b
