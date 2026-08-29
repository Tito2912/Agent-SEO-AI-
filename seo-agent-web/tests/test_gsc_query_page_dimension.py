"""Search Console can return the query AND the page that ranks for it. We never asked.

`fetch_gsc` has always taken a LIST of dimensions; the caller narrowed it to a single entry, and
`_gsc_rows_to_perf_items` read only `keys[0]`. So the product could say "this query
underperforms" but never "THIS PAGE underperforms on this query" — and the second is the one the
corrector can be pointed at. Without the pairing, a keyword opportunity is a remark; with it, it
is a target.

The joint dimension is Google-only: Bing's reporting here has no equivalent, and offering a
dimension that silently degrades would be worse than not offering it.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")

from backend import app as app_module  # noqa: E402
from backend import keywords as kw  # noqa: E402


def test_a_single_key_row_still_yields_a_keyword_and_no_page() -> None:
    # The existing shape must not move: every caller asking for one dimension still works.
    rows = [{"keys": ["ma requête"], "clicks": 3, "impressions": 500, "ctr": 0.006, "position": 7.2}]
    items = app_module._gsc_rows_to_perf_items(rows)
    assert items == [{"keyword": "ma requête", "clicks": 3, "impressions": 500,
                      "ctr": 0.006, "position": 7.2}]
    assert "page" not in items[0]


def test_a_two_key_row_carries_the_page_that_ranks() -> None:
    rows = [{"keys": ["ma requête", "https://site.fr/blog"], "clicks": 3, "impressions": 500,
             "ctr": 0.006, "position": 7.2}]
    assert app_module._gsc_rows_to_perf_items(rows)[0]["page"] == "https://site.fr/blog"


@pytest.mark.parametrize("second", ["", "   ", None])
def test_an_empty_second_key_adds_no_page(second) -> None:
    """A blank page is worse than no page: it would render as a target nobody can open."""
    rows = [{"keys": ["ma requête", second], "clicks": 0, "impressions": 500,
             "ctr": 0.0, "position": 7.2}]
    assert "page" not in app_module._gsc_rows_to_perf_items(rows)[0]


def test_a_row_with_no_key_at_all_is_dropped() -> None:
    assert app_module._gsc_rows_to_perf_items([{"keys": [], "clicks": 1}]) == []
    assert app_module._gsc_rows_to_perf_items([{"clicks": 1}]) == []


def test_the_opportunity_carries_the_page_through_to_the_corrector() -> None:
    """The whole point of the pairing: the opportunity names the file to fix."""
    rows = app_module._gsc_rows_to_perf_items([
        {"keys": ["pictory ai pricing", "https://site.fr/pictory"], "clicks": 0,
         "impressions": 481, "ctr": 0.0, "position": 7.91},
    ])
    found = kw.find_opportunities(rows)
    assert len(found) == 1
    assert found[0]["kind"] == kw.KIND_NO_CLICKS
    assert found[0]["page"] == "https://site.fr/pictory"


def test_an_opportunity_without_a_page_is_still_reported() -> None:
    # A query-only crawl must not lose its opportunities just because the pairing is missing;
    # the row simply carries no target.
    rows = app_module._gsc_rows_to_perf_items([
        {"keys": ["pictory ai pricing"], "clicks": 0, "impressions": 481, "ctr": 0.0,
         "position": 7.91},
    ])
    found = kw.find_opportunities(rows)
    assert len(found) == 1 and found[0]["page"] == ""
