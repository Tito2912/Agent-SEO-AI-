"""og:url has exactly one correct value — the page's canonical — and the crawl already holds it.

Measured on videocaptionstudio.com: 12 pages declare og:url pointing at `…-2026` slugs that
return 404, while their canonical is the `…-2025` URL the page is actually served at. A slug
rename that touched one tag and not the other.

The family had no deterministic path at all: it went to the model with a prose hint, for a
replacement where nothing is open to interpretation.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")

from backend import app as app_module  # noqa: E402

PAGE = (
    '<link rel="canonical" href="https://x.fr/blog/a-2025"/>\n'
    '<meta property="og:url" content="https://x.fr/blog/a-2026"/>\n'
    '<meta name="twitter:url" content="https://x.fr/blog/a-2026"/>\n'
)
PAIRS = [{"page": "https://x.fr/blog/a-2025",
          "from": "https://x.fr/blog/a-2026", "to": "https://x.fr/blog/a-2025"}]


def test_it_rewrites_only_the_og_url_tag() -> None:
    out, n = app_module._rewrite_og_url(PAGE, PAIRS)
    assert n == 1
    assert 'property="og:url" content="https://x.fr/blog/a-2025"' in out
    assert 'name="twitter:url" content="https://x.fr/blog/a-2026"' in out, (
        "twitter:url is a different family and must not move")
    assert 'rel="canonical" href="https://x.fr/blog/a-2025"' in out


def test_a_value_it_was_not_given_is_left_alone() -> None:
    out, n = app_module._rewrite_og_url(PAGE, [{"from": "https://x.fr/autre", "to": "https://x.fr/z"}])
    assert n == 0 and out == PAGE


def test_pairs_come_from_the_crawl_not_from_a_guess() -> None:
    pages = [
        {"url": "https://x.fr/blog/a-2025", "og_url": "https://x.fr/blog/a-2026",
         "canonical": "https://x.fr/blog/a-2025"},
        {"url": "https://x.fr/ok", "og_url": "https://x.fr/ok", "canonical": "https://x.fr/ok"},
    ]
    pairs = app_module._og_url_pairs_from_pages(["https://x.fr/blog/a-2025"], pages)
    assert len(pairs) == 1 and pairs[0]["to"] == "https://x.fr/blog/a-2025"


def test_a_page_whose_tags_already_agree_yields_nothing() -> None:
    pages = [{"url": "https://x.fr/ok", "og_url": "https://x.fr/ok/", "canonical": "https://x.fr/ok"}]
    assert app_module._og_url_pairs_from_pages(["https://x.fr/ok"], pages) == []


def test_the_family_now_has_a_deterministic_rewriter() -> None:
    """The defect: a bounded, exact replacement was being handed to the model."""
    pages = [{"url": "https://x.fr/a", "og_url": "https://x.fr/b", "canonical": "https://x.fr/a"}]
    prep = app_module._prepare_issue_fix(
        issue_key="open_graph_url_not_matching_canonical", issues={},
        impacted=["https://x.fr/a"], all_paths=["index.html"], site_name="x.fr", owner="o",
        repo_name="r", branch="main", token="t", model_override="", pages=pages)
    assert prep["link_rewriter"] is not None
    assert prep["rewriter_is_ai"] is False, "a mechanical swap must not be billed as model work"
    assert "og:url" in prep["extra_hint"]
