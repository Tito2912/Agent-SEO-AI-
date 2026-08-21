"""The crawler side of the evidence contract.

Every fixer family reads evidence through the same shape, so the helper that writes it is
the single point where a malformed pair could poison a whole family. It is deliberately
strict: no destination, a self-pointing pair, or an unknown kind produces nothing rather
than something the corrector would act on.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
_SPEC = importlib.util.spec_from_file_location(
    "seo_audit_for_tests", REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py"
)
assert _SPEC and _SPEC.loader
seo_audit = importlib.util.module_from_spec(_SPEC)
sys.modules["seo_audit_for_tests"] = seo_audit
_SPEC.loader.exec_module(seo_audit)


def _issues() -> dict[str, Any]:
    return {"a": {"count": 1, "examples": []}, "b": {"count": 2, "examples": []}}


def test_url_pairs_are_attached_to_every_listed_issue() -> None:
    issues = _issues()
    seo_audit._attach_issue_evidence(
        issues, ("a", "b"), "url_pairs",
        [{"page": "https://s/p", "from": "https://s/x/", "to": "https://s/x"}],
    )

    assert issues["a"]["evidence"] == {
        "kind": "url_pairs",
        "items": [{"page": "https://s/p", "from": "https://s/x/", "to": "https://s/x"}],
    }
    assert issues["b"]["evidence"] == issues["a"]["evidence"]
    # The existing block is enriched, never replaced.
    assert issues["a"]["count"] == 1


def test_unusable_pairs_are_dropped_rather_than_attached() -> None:
    issues = _issues()
    seo_audit._attach_issue_evidence(issues, ("a",), "url_pairs", [
        {"page": "p", "from": "https://s/x", "to": ""},           # no destination
        {"page": "p", "from": "https://s/y", "to": "https://s/y"},  # points at itself
        {"page": "p", "from": "", "to": "https://s/z"},            # nothing to replace
    ])

    # No evidence at all beats evidence that would drive a wrong rewrite.
    assert "evidence" not in issues["a"]


def test_duplicates_are_collapsed_and_the_list_is_capped() -> None:
    issues = _issues()
    dupes = [{"page": "p", "from": "https://s/x", "to": "https://s/y"}] * 5
    many = [{"page": f"p{i}", "from": f"https://s/{i}", "to": f"https://s/{i}b"} for i in range(60)]
    seo_audit._attach_issue_evidence(issues, ("a",), "url_pairs", dupes + many)

    items = issues["a"]["evidence"]["items"]
    assert len(items) == seo_audit.EVIDENCE_CAP
    assert items[0] == {"page": "p", "from": "https://s/x", "to": "https://s/y"}
    assert len({(i["page"], i["from"], i["to"]) for i in items}) == len(items)


def test_page_values_require_the_value_and_ignore_pair_fields() -> None:
    issues = _issues()
    seo_audit._attach_issue_evidence(issues, ("a",), "page_values", [
        {"page": "https://s/p", "field": "og_manquants", "value": "og:image"},
        {"page": "https://s/q", "field": "og_manquants", "value": ""},  # nothing to report
    ])

    assert issues["a"]["evidence"] == {
        "kind": "page_values",
        "items": [{"page": "https://s/p", "field": "og_manquants", "value": "og:image"}],
    }


def test_an_unknown_kind_writes_nothing() -> None:
    issues = _issues()
    seo_audit._attach_issue_evidence(issues, ("a",), "something_new", [{"page": "p", "from": "x", "to": "y"}])

    assert "evidence" not in issues["a"]


def test_a_missing_issue_block_is_skipped_silently() -> None:
    issues = _issues()
    seo_audit._attach_issue_evidence(
        issues, ("a", "not_emitted_this_run"), "url_pairs",
        [{"page": "p", "from": "https://s/x", "to": "https://s/y"}],
    )

    assert "evidence" in issues["a"]
    assert "not_emitted_this_run" not in issues
