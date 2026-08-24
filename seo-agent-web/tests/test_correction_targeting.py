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


def test_every_sitemap_family_finds_the_sitemap() -> None:
    # Two bugs met here, both silent. The families set a rewriter, which switched page-targeting
    # on, which filled the targets with page files that the sitemap-only filter then wiped. And
    # the ordered substring chain routed sitemap_3xx_redirect to the redirect config,
    # sitemap_non_canonical_page to a layout, and the hreflang conflict to lib/seo.ts.
    # sitemap_3xx_redirect only ever worked because its slashed URL happened to be unresolvable.
    for key in ("more_than_one_page_for_same_language_in_hreflang", "sitemap_3xx_redirect",
                "sitemap_non_canonical_page", "indexable_page_not_in_sitemap"):
        targets, _ = _resolve(key, ["/de/blog", "/about"], tree=STATIC_TREE, wants_page_targeting=True)
        assert targets == ["sitemap.xml"], key


def test_sitemap_fixes_land_in_the_sitemap_and_nowhere_else() -> None:
    # The flagged URL also appears in every page that links to it, so the evidence grep drags
    # those in. A sitemap issue is fixed in the sitemap, full stop.
    targets, _ = _resolve(
        "sitemap_non_canonical_page", ["/de/blog"],
        located=["de/index.html", "index.html", "sitemap.xml"], tree=STATIC_TREE,
    )

    assert targets == ["sitemap.xml"]


def test_sitemap_issues_still_reach_the_generator_through_the_candidate_list() -> None:
    # The candidate skip must not touch families whose fix is NOT in the flagged pages.
    targets, _ = _resolve(
        "indexable_page_not_in_sitemap", ["/de/blog"], tree=STATIC_TREE,
    )

    assert "sitemap.xml" in targets


# The real shape of avis-invest.com: a route GROUP for the default locale, one layout per
# locale, nested dynamic routes, and MDX both as flat files and as directory bundles.
NEXT_MULTILOCALE_TREE = [
    "package.json", "next.config.mjs", "netlify.toml",
    "app/(fr)/layout.tsx", "app/(fr)/page.tsx", "app/(fr)/[slug]/page.tsx", "app/(fr)/blog/page.tsx",
    "app/de/layout.tsx", "app/de/page.tsx", "app/de/[slug]/page.tsx",
    "app/de/blog/page.tsx", "app/de/blog/[slug]/page.tsx",
    "app/sitemap.ts", "app/robots.ts",
    "content/guide-etoro.mdx", "content/de/bitpanda.mdx",
    "content/de/blog/etoro-copytrading-2026/index.mdx",
]


def test_route_map_handles_a_real_multilocale_next_app() -> None:
    index = ri.build_repo_index(NEXT_MULTILOCALE_TREE)

    # The route group is transparent, so the French pages live at the root.
    assert ri.route_files(index, "/") == ["app/(fr)/page.tsx"]
    assert ri.route_files(index, "/de") == ["app/de/page.tsx"]
    assert ri.route_files(index, "/de/blog") == ["app/de/blog/page.tsx"]
    # Content reaches its URL through the dynamic template that renders it...
    assert ri.route_files(index, "/guide-etoro") == ["content/guide-etoro.mdx"]
    assert ri.route_files(index, "/de/bitpanda") == ["content/de/bitpanda.mdx"]
    # ...including a directory bundle, whose URL drops the /index.
    assert ri.route_files(index, "/de/blog/etoro-copytrading-2026") == [
        "content/de/blog/etoro-copytrading-2026/index.mdx"
    ]
    # Every layout and every [slug] template counts as shared.
    for path in ("app/(fr)/layout.tsx", "app/de/layout.tsx",
                 "app/(fr)/[slug]/page.tsx", "app/de/blog/[slug]/page.tsx"):
        assert ri.is_shared_path(index, path), path


def test_the_shared_template_guard_catches_what_the_route_map_cannot() -> None:
    # While the map resolves, per-page families target page sources and a layout never enters.
    resolved, _ = _resolve(
        "missing_h1_indexable", ["/de/bitpanda", "/de/blog", "/guide-etoro"],
        tree=NEXT_MULTILOCALE_TREE,
    )
    assert resolved == ["content/de/bitpanda.mdx", "app/de/blog/page.tsx", "content/guide-etoro.mdx"]

    # When it CANNOT resolve, the heuristics fall back to the AI mapping, which happily
    # proposes the layouts -- writing an h1 there would hit every page of two locales. The
    # guard is the only thing standing between that suggestion and a commit.
    unresolved, calls = _resolve(
        "missing_h1_indexable", ["/page-built-elsewhere"], tree=NEXT_MULTILOCALE_TREE,
        ai_map_result=["app/de/layout.tsx", "app/(fr)/layout.tsx"],
    )
    assert "map" in calls
    assert unresolved == []


def test_indexability_variants_inherit_their_family() -> None:
    # Ahrefs splits many issues into Indexable / Not indexable and the crawler emits the
    # SUFFIXED key. A freshly injected missing-h1 defect showed up in Anomalies but never
    # reached the corrections page because only the bare `missing_h1` was claimed.
    for key in ("missing_h1_indexable", "missing_h1_not_indexable"):
        assert app_module._github_issue_auto_fixable(key), key
        assert key in app_module._PER_PAGE_ONLY_KEYS, key
        targets, _ = _resolve(key, ["/about"])
        assert targets == ["app/about/page.tsx"], key

    # The expansion must not claim a key whose own base is advisory.
    assert not app_module._github_issue_auto_fixable("orphan_page_indexable")
    assert not app_module._github_issue_auto_fixable("page_has_links_to_broken_page_indexable")


def test_a_missing_content_tag_is_written_on_the_page_never_in_the_layout() -> None:
    # Before the guard, asking for a meta description on 2 flagged pages targeted
    # app/layout.tsx + pages/_document.tsx and NOT the pages: every page of the site would have
    # inherited one shared description, turning "missing" into "duplicate" sitewide.
    for key in ("missing_meta_description", "missing_title", "missing_h1",
                "duplicate_titles", "duplicate_meta_descriptions"):
        targets, _ = _resolve(key, ["/about", "/en"])
        assert set(targets) == {"app/about/page.tsx", "app/en/page.tsx"}, key
        assert not any(ri.is_shared_path({}, p) for p in targets), key


def test_a_missing_canonical_may_still_be_computed_in_a_shared_layout() -> None:
    # Unlike a literal title, a canonical derived from the route is correct for every page, so
    # the shared file is not FORBIDDEN here — it simply isn't needed while the route map can
    # name the pages. When the map can't resolve the URL, the layout is reachable again.
    resolved, _ = _resolve("missing_canonical", ["/about"])
    assert resolved == ["app/about/page.tsx"]

    unresolved, _ = _resolve("missing_canonical", ["/page-built-elsewhere"])
    assert "app/layout.tsx" in unresolved


def test_current_state_issues_target_the_flagged_pages() -> None:
    # A duplicated <h1> lives in the page's own source, so the page files come first.
    targets, calls = _resolve("multiple_h1", ["/about", "/en/avis-bitpanda"])

    assert targets[:2] == ["app/about/page.tsx", "content/en/avis-bitpanda.mdx"]
    assert calls == []


def test_asset_issues_never_target_the_routing_config() -> None:
    # A redirect config MENTIONS the redirecting asset URL (it is the rule's source), so the
    # evidence grep finds it. The asset rewriter requires no attribute prefix by design, so it
    # would rewrite the rule's left-hand side and turn `/old.png -> /new.png` into a self-
    # redirect. Only the extensionless `_redirects` name kept it out of range in PR#10.
    for config in ("netlify.toml", "vercel.json", "_redirects", "next.config.js", "nginx.conf"):
        targets, _ = _resolve(
            "image_redirects", ["/de/legal-notice"],
            located=[config, "de/legal-notice.html"],
            tree=STATIC_TREE + [config, "de/legal-notice.html"],
        )
        assert config not in targets, config
        assert "de/legal-notice.html" in targets


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
