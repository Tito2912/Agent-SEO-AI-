"""Which repo files the corrector decides to patch, for one issue.

Targeting is where the corrector historically went wrong: patching a shared layout for a
per-page issue regressed pages that were already correct, and a mis-mapped URL patched a
file that had nothing to do with the flagged page. These tests pin the deterministic
ordering and the safety guards, with the AI fallbacks stubbed out.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-targeting-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402
from backend import repo_index as ri  # noqa: E402


TREE = [
    "package.json",
    "next.config.js",
    "app/layout.tsx",
    "app/page.tsx",
    "app/sitemap.ts",
    "app/about/page.tsx",
    "app/en/page.tsx",
    "app/en/[slug]/page.tsx",
    "content/en/avis-bitpanda.mdx",
    "content/en/guide-etoro.mdx",
    "public/sources/etoro-en.html",
    "components/Header.tsx",
]
SITE = "https://avis-invest.com"


def _resolve(issue_key: str, urls: list[str], **kw):
    """Resolve targets with both AI fallbacks stubbed, recording whether they were called."""
    calls: list[str] = []
    ai_map_result = kw.pop("ai_map_result", [])

    def _ai_map() -> list[str]:
        calls.append("map")
        return ai_map_result

    def _ai_pick() -> list[str]:
        calls.append("pick")
        return []

    targets = app_module._resolve_issue_targets(
        all_paths=TREE,
        index=kw.pop("index", ri.build_repo_index(TREE)),
        issue_key=issue_key,
        issue_label=issue_key,
        impacted_urls=[f"{SITE}{u}" for u in urls],
        located=kw.pop("located", []),
        max_files=kw.pop("max_files", 8),
        ai_map=_ai_map,
        ai_pick=_ai_pick,
        **kw,
    )
    return targets, calls


def test_mdx_backed_route_targets_the_content_file_not_the_slug_template() -> None:
    targets, calls = _resolve("open_graph_url_not_matching_canonical", ["/en/avis-bitpanda"])

    assert targets[0] == "content/en/avis-bitpanda.mdx"
    assert "app/en/[slug]/page.tsx" not in targets
    # The route map answered every impacted URL, so no AI call was needed at all.
    assert calls == []


def test_per_page_head_issue_never_targets_the_shared_layout() -> None:
    targets, _ = _resolve(
        "open_graph_url_not_matching_canonical",
        ["/", "/about", "/en", "/en/avis-bitpanda", "/en/guide-etoro", "/sources/etoro-en"],
    )

    assert "app/layout.tsx" not in targets
    # Every flagged page is targeted — a subset would trade one issue for another.
    assert set(targets) == {
        "app/page.tsx", "app/about/page.tsx", "app/en/page.tsx",
        "content/en/avis-bitpanda.mdx", "content/en/guide-etoro.mdx",
        "public/sources/etoro-en.html",
    }


def test_title_length_issue_drops_shared_templates() -> None:
    targets, _ = _resolve("title_too_long", ["/en/avis-bitpanda", "/about"])

    assert set(targets) == {"content/en/avis-bitpanda.mdx", "app/about/page.tsx"}
    # A layout with a `%s | Marque` template would lengthen EVERY page of the site.
    assert not any(ri.is_shared_path({}, p) for p in targets)


def test_sitemap_issue_keeps_the_generator_even_when_many_pages_are_flagged() -> None:
    targets, _ = _resolve(
        "indexable_page_not_in_sitemap",
        ["/en/avis-bitpanda", "/en/guide-etoro", "/about", "/sources/etoro-en"],
        max_files=2,
    )

    # The impacted URLs are the DATA to append; the generator is the only file to edit.
    assert "app/sitemap.ts" in targets


def test_unresolved_urls_still_fall_back_to_the_ai_mapping() -> None:
    targets, calls = _resolve(
        "open_graph_url_not_matching_canonical", ["/page-built-elsewhere"],
        ai_map_result=["components/Header.tsx"],
    )

    assert "map" in calls
    assert "components/Header.tsx" in targets


def test_evidence_hits_stay_ahead_of_every_heuristic() -> None:
    targets, _ = _resolve(
        "missing_alt_text", ["/en/avis-bitpanda"], located=["components/Header.tsx"],
    )

    assert targets[0] == "components/Header.tsx"


def test_targeting_works_without_an_index_so_a_missing_map_is_never_fatal() -> None:
    targets, calls = _resolve(
        "open_graph_url_not_matching_canonical", ["/about"], index=None,
        ai_map_result=[],
    )

    # Falls back to conventional-path guessing, and still refuses the shared layout.
    assert "app/about/page.tsx" in targets
    assert "app/layout.tsx" not in targets
    assert "map" in calls
