"""A policy matrix cannot turn mentions, zero counters or a failed run into readiness."""

import pytest

from ops.gauntlet.preclients_audit import audit
from tests.test_acceptance_inventory import backend


def setup_backend():
    m = backend()
    m._issue_correction_controls = lambda key: {"deep_fix": m._github_issue_auto_fixable(key),
        "url_fix": m._github_issue_auto_fixable(key) and not key.startswith("missing_h1")}
    return m


def test_all_catalog_keys_are_classified_without_claiming_live_repair_or_deployment():
    m = setup_backend()
    out = audit(m, {"failed.json": {"family": "missing_title", "status": "failed"},
                    "zero.json": {"issues": {"missing_h1": {"count": 0}}}})
    assert len(out["individual_policy"]) == 6
    assert out["manual_catalog_key_count"] == 1 and out["deep_only_families"] == ["missing_h1"]
    assert "missing_title" in out["preview_capable_families"]
    assert not out["client_readiness_certified"] and not out["deployed_agent_checked"]
    assert not out["individual_api_requests_exercised_by_this_inventory"]
    assert all(not row["all_occurrences_or_stacks_certified"] for row in out["individual_policy"])
    assert out["remaining_release_gates"]


@pytest.mark.parametrize("controls", [{}, {"deep_fix": True, "url_fix": True},
    {"deep_fix": False, "url_fix": True}, {"deep_fix": 0, "url_fix": False},
    {"deep_fix": False, "url_fix": False, "extra": True}])
def test_inconsistent_or_untyped_policy_is_not_silently_published(controls):
    m = setup_backend()
    original = m._issue_correction_controls
    m._issue_correction_controls = lambda key: controls if key == "slow_page" else original(key)
    with pytest.raises(ValueError, match="Inconsistent individual correction policy"):
        audit(m, {})


def test_matrix_is_deterministic_and_keeps_only_named_record_semantics():
    m = setup_backend()
    a = audit(m, {"b.json": {"family": "missing_title"}, "a.json": {"family": "missing_h1"}})
    b = audit(m, {"a.json": {"family": "missing_h1"}, "b.json": {"family": "missing_title"}})
    assert a == b and a["record_mentions_are_not_success_verdicts"] is True
