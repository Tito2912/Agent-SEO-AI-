"""Under-linked pages are NAMED, not rewritten.

Measured on the four reference sites carrying `page_has_only_one_dofollow_incoming_internal_link`
(7 occurrences): there is no deterministic rule for choosing which page should carry the new link.
On prosperfactory the target has a single sibling, so no candidate exists at all — 3 of the 7.
On easyshopbuilder the best-connected candidates sit in another language until you filter on it.
The hreflang siblings are already symmetric, so translations add no signal.

Inserting a link into a customer's prose on that basis would be the most editorial act the
corrector performs, on the weakest evidence, for a notice-level issue. The link graph is already
in the crawl, so the advice names the page that links today and the pages that plausibly should —
and says plainly when there are none.
"""

from __future__ import annotations

import sys
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

from backend import fix_suggestions as fs  # noqa: E402

KEY = "page_has_only_one_dofollow_incoming_internal_link_indexable"


def _report(pages, examples):
    return {"issues": {KEY: {"count": len(examples), "examples": examples}}, "pages": pages}


def _link(u, nofollow=False):
    return {"target_url": u, "nofollow": nofollow, "anchor": ""}


TARGET = "https://x.fr/blog/cible"
PAGES = [
    {"url": TARGET, "lang": "fr", "title": "La cible",
     "internal_link_items": [_link(TARGET)]},                      # self-link: not an incoming one
    {"url": "https://x.fr/blog/source", "lang": "fr",
     "internal_link_items": [_link(TARGET)]},                      # the only page linking today
    {"url": "https://x.fr/blog/voisine", "lang": "fr",             # covers two siblings, not the target
     "internal_link_items": [_link("https://x.fr/blog/a"), _link("https://x.fr/blog/b")]},
    {"url": "https://x.fr/en/blog/other", "lang": "en",            # right shape, wrong language
     "internal_link_items": [_link("https://x.fr/blog/a"), _link("https://x.fr/blog/b")]},
    {"url": "https://x.fr/blog/a", "lang": "fr", "internal_link_items": []},
    {"url": "https://x.fr/blog/b", "lang": "fr", "internal_link_items": []},
]


def test_it_names_the_page_that_links_today_and_the_ones_that_could() -> None:
    rows = fs._under_linked_context(_report(PAGES, [TARGET]), KEY)
    assert rows and rows[0]["linked_by"] == ["https://x.fr/blog/source"]
    assert rows[0]["candidates"] == ["https://x.fr/blog/voisine"]


def test_a_candidate_in_another_language_is_not_a_candidate() -> None:
    rows = fs._under_linked_context(_report(PAGES, [TARGET]), KEY)
    assert "https://x.fr/en/blog/other" not in rows[0]["candidates"]


def test_a_page_linking_to_itself_is_not_an_incoming_link() -> None:
    """Three of the seven real cases had the target as its own top "source"."""
    rows = fs._under_linked_context(_report(PAGES, [TARGET]), KEY)
    assert TARGET not in rows[0]["linked_by"]


def test_no_neighbourhood_is_said_out_loud() -> None:
    """prosperfactory: the target has one sibling and no candidate. Generic advice there would
    send the owner looking for a page that does not exist."""
    lonely = [
        {"url": TARGET, "lang": "fr", "internal_link_items": []},
        {"url": "https://x.fr/blog/source", "lang": "fr", "internal_link_items": [_link(TARGET)]},
    ]
    out = fs.suggest_issue_fix(issue_key=KEY, label="l", category="Links", severity="notice",
                               count=1, report=_report(lonely, [TARGET]), site_name="x.fr",
                               base_url="https://x.fr/")
    assert any("Aucune page candidate" in line for line in out["fix"])
    assert out["auto_fixable"] is False
    assert "éditorial" in out["auto_fix_note"]


def test_the_family_is_not_offered_as_an_automatic_correction() -> None:
    out = fs.suggest_issue_fix(issue_key=KEY, label="l", category="Links", severity="notice",
                               count=1, report=_report(PAGES, [TARGET]), site_name="x.fr",
                               base_url="https://x.fr/")
    assert out["auto_fixable"] is False
    import os
    os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
    os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
    from backend import app as app_module
    assert not app_module._github_issue_auto_fixable(KEY), (
        "the corrector must not claim a family it cannot decide")
