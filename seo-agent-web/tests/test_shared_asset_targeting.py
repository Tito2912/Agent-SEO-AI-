"""One image, seventy pages, one component — and the component was evicted by the file cap.

Measured on prosperfactory.com: all 70 `missing_alt_text` occurrences are the same src, the site
logo, rendered by components/SiteHeader.tsx on every page. The grep locator found that component.
Target resolution then listed four `app/(sales)/…/page.tsx` route files ahead of it and max_files
dropped the only file that would have fixed anything — while the four it kept are not even among
the seventy flagged pages.

A located file that is not a route renders on many pages; a route file renders on one.
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
from backend import repo_index  # noqa: E402

TREE = [
    "app/(sales)/page.tsx", "app/(sales)/de/page.tsx", "app/(sales)/es/page.tsx",
    "app/(sales)/fr/page.tsx", "components/SiteHeader.tsx", "content/es/etoro.mdx",
]
LOCATED = ["app/(sales)/de/page.tsx", "app/(sales)/es/page.tsx", "app/(sales)/fr/page.tsx",
           "app/(sales)/page.tsx", "components/SiteHeader.tsx"]


def _targets(key: str, max_files: int = 4) -> list[str]:
    return app_module._resolve_issue_targets(
        issue_key=key, issue_label="l", all_paths=TREE,
        impacted_urls=["https://x.fr/brokers/etoro", "https://x.fr/es/brokers/etoro"],
        evidence=["/assets/logo.png"], located=list(LOCATED),
        index=repo_index.build_repo_index(TREE), max_files=max_files)


def test_the_component_comes_before_the_route_files() -> None:
    assert _targets("missing_alt_text")[0] == "components/SiteHeader.tsx"


def test_the_cap_no_longer_evicts_the_only_useful_file() -> None:
    """With max_files=1 the old order kept a single sales page and fixed nothing."""
    assert _targets("missing_alt_text", max_files=1) == ["components/SiteHeader.tsx"]


def test_asset_rewrite_families_get_the_same_order() -> None:
    """A redirecting logo has the same shape: flagged everywhere, written once."""
    key = sorted(app_module._ASSET_REWRITE_KEYS)[0]
    assert _targets(key)[0] == "components/SiteHeader.tsx"


def test_the_route_files_are_kept_not_dropped() -> None:
    """Reordering, not filtering: an image can also sit in a page."""
    out = _targets("missing_alt_text")
    assert len(out) == 4 and any(p.startswith("app/(sales)") for p in out)


def test_a_page_family_is_not_pulled_toward_the_component() -> None:
    """The rule is about assets. For a page-level family the route map decides, and a shared
    component must NOT be promoted ahead of the page that carries the flagged value."""
    out = app_module._resolve_issue_targets(
        issue_key="missing_title", issue_label="l", all_paths=TREE,
        impacted_urls=["https://x.fr/"], evidence=[], located=list(LOCATED),
        index=repo_index.build_repo_index(TREE), max_files=4)
    assert out[0] == "app/(sales)/page.tsx", out
    assert "components/SiteHeader.tsx" not in out
