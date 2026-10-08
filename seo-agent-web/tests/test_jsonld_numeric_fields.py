"""Compatibility tests of explicit coercion; price strings are valid, not repair targets.

The historical helper stays tested, but neither structured-data diagnostic may invoke it.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")

from backend import app as app_module  # noqa: E402


def _block(payload: str) -> str:
    return '<script type="application/ld+json">' + payload + "</script>"


def test_it_unquotes_a_bare_number() -> None:
    out, n = app_module._rewrite_jsonld_numeric_strings(
        _block('{"@type":"Offer","price":"0","priceCurrency":"USD"}'))
    assert n == 1
    assert '"price":0' in out
    assert '"priceCurrency":"USD"' in out, "currency is Text and must not move"


def test_the_result_is_still_valid_json() -> None:
    out, _ = app_module._rewrite_jsonld_numeric_strings(
        _block('{"@type":"Offer","price":"29.99","priceCurrency":"EUR"}'))
    payload = out.split(">", 1)[1].rsplit("<", 1)[0]
    assert json.loads(payload)["price"] == 29.99


def test_a_value_that_is_not_a_bare_number_stays_a_string() -> None:
    """Unquoting `29.99 USD` would produce JSON that does not parse."""
    src = _block('{"price":"29.99 USD"}')
    out, n = app_module._rewrite_jsonld_numeric_strings(src)
    assert n == 0 and out == src


def test_the_other_numeric_fields_are_covered() -> None:
    out, n = app_module._rewrite_jsonld_numeric_strings(
        _block('{"@type":"AggregateOffer","lowPrice":"5","highPrice":"20","offerCount":"3"}'))
    assert n == 3 and '"lowPrice":5' in out and '"offerCount":3' in out


def test_json_outside_a_ld_json_block_is_never_touched() -> None:
    src = '<script>var conf={"price":"0"};</script>'
    out, n = app_module._rewrite_jsonld_numeric_strings(src)
    assert n == 0 and out == src


def test_a_block_that_would_stop_parsing_is_left_alone() -> None:
    """The guard that matters: never hand back a page whose JSON-LD no longer parses."""
    src = _block('{"price":"0", oops}')
    out, n = app_module._rewrite_jsonld_numeric_strings(src)
    assert n == 0 and out == src


def test_the_family_now_has_a_deterministic_rewriter() -> None:
    """Keep the historical case: its former binding must now be refused, not silently run."""
    prep = app_module._prepare_issue_fix(
        issue_key="structured_data_google_rich_results_validation_error", issues={},
        impacted=["https://x.fr/"], all_paths=["index.html"], site_name="x.fr", owner="o",
        repo_name="r", branch="main", token="t", model_override="")
    assert prep["refusal"] and "price" in prep["refusal"]
    assert prep["link_rewriter"] is None and not prep["rewriter_is_ai"] and not prep["rewriter_ai_fallback"]
    assert not app_module._github_issue_auto_fixable("structured_data_google_rich_results_validation_error")
