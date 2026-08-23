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


STATIC_TREE = [
    "index.html",
    "de/index.html",
    "de/blog.html",
    "de/legal-notice.html",
    "en/legal-notice.html",
    "es/legal-notice.html",
    "mentions-legales.html",
    "fr/index.html",
    "sitemap.xml",
]

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
    tree = kw.pop("tree", TREE)

    def _ai_map() -> list[str]:
        calls.append("map")
        return ai_map_result

    def _ai_pick() -> list[str]:
        calls.append("pick")
        return []

    targets = app_module._resolve_issue_targets(
        all_paths=tree,
        index=kw.pop("index", ri.build_repo_index(tree)),
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


def test_head_tag_families_drop_files_that_merely_mention_the_url() -> None:
    # Regression from PR#4 on elevenlabs-avis.com: fixing the hreflang of 3 legal pages also
    # rewrote sitemap.xml -- including a <loc>, which would have made the sitemap point at a
    # redirecting URL -- plus the target page itself. Both were found by grepping the evidence
    # URL, which matches any file that merely MENTIONS the page.
    targets, _ = _resolve(
        "hreflang_to_non_canonical",
        ["/en/legal-notice", "/es/legal-notice", "/mentions-legales"],
        located=["sitemap.xml", "de/legal-notice.html"],
        tree=STATIC_TREE,
    )

    assert set(targets) == {"en/legal-notice.html", "es/legal-notice.html", "mentions-legales.html"}


def test_mixed_content_keeps_located_files_outside_the_flagged_pages() -> None:
    # The counterpart: there the http:// references ARE the fix and legitimately live in files
    # the route map never names, so dropping them would break that family.
    targets, _ = _resolve(
        "https_page_has_internal_links_to_http", ["/de/blog"],
        located=["CSS/main.css"], tree=STATIC_TREE, wants_page_targeting=True,
    )

    assert "CSS/main.css" in targets


def test_a_resolved_page_family_never_pulls_in_an_unflagged_basename_match() -> None:
    # Regression from PR#2 on elevenlabs-avis.com: fixing the meta description of /de/blog also
    # rewrote de/index.html, matched only because the candidate list contains "index.html".
    # That page was never flagged and its 160-char description was valid.
    targets, calls = _resolve(
        "meta_description_too_long_indexable", ["/de/blog"], tree=STATIC_TREE,
    )

    assert targets == ["de/blog.html"]
    assert calls == []


def test_the_candidate_list_still_applies_when_the_map_cannot_resolve_a_page() -> None:
    # Without a resolved route the heuristics are all we have, so they must stay in play.
    targets, calls = _resolve(
        "meta_description_too_long_indexable", ["/de/nowhere"], tree=STATIC_TREE,
    )

    assert "index.html" in targets or "de/index.html" in targets
    assert "map" in calls


def test_sitemap_issues_still_reach_the_generator_through_the_candidate_list() -> None:
    # The candidate skip must not touch families whose fix is NOT in the flagged pages.
    targets, _ = _resolve(
        "indexable_page_not_in_sitemap", ["/de/blog"], tree=STATIC_TREE,
    )

    assert "sitemap.xml" in targets


def test_current_state_issues_target_the_flagged_pages() -> None:
    # A duplicated <h1> lives in the page's own source, so the page files come first.
    targets, calls = _resolve("multiple_h1", ["/about", "/en/avis-bitpanda"])

    assert targets[:2] == ["app/about/page.tsx", "content/en/avis-bitpanda.mdx"]
    assert calls == []


def test_asset_issues_keep_the_component_holding_the_src_ahead_of_the_pages() -> None:
    # A redirected logo lives in ONE shared component but flags every page that renders it.
    pages = ["/", "/about", "/en", "/en/avis-bitpanda", "/en/guide-etoro", "/sources/etoro-en"]
    targets, _ = _resolve(
        "page_has_redirected_image", pages,
        located=["components/Header.tsx"], max_files=3, wants_page_targeting=True,
    )

    assert targets[0] == "components/Header.tsx"

    # Contrast: a link family DOES want its flagged pages first, and there the located file
    # legitimately gives way to them.
    link_targets, _ = _resolve(
        "page_has_links_to_redirect_indexable", pages,
        located=["components/Header.tsx"], max_files=3, wants_page_targeting=True,
    )
    assert link_targets[0] != "components/Header.tsx"


def test_targeting_works_without_an_index_so_a_missing_map_is_never_fatal() -> None:
    targets, calls = _resolve(
        "open_graph_url_not_matching_canonical", ["/about"], index=None,
        ai_map_result=[],
    )

    # Falls back to conventional-path guessing, and still refuses the shared layout.
    assert "app/about/page.tsx" in targets
    assert "app/layout.tsx" not in targets
    assert "map" in calls
