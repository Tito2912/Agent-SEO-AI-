"""A sitemap that lists a `noindex` page contradicts itself — and this is the ONE sitemap family
worth correcting.

Chosen from data, not from the catalogue. Twelve sibling sitemap/robots families were candidates;
crawling ten real reference sites showed `sitemap_noindex_page` on 2 of them (35 occurrences) and
the other twelve on none. Building all thirteen would have been thirteen correctors for one real
case.

The repair removes the sitemap ENTRY, never the `noindex`: the sitemap is a hint, the meta robots
tag is an instruction, and reversing the smaller of the two is the safer half of the
contradiction. That choice is a premise, so it ships with a premise note and can never auto-merge.
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

SITEMAP = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    "  <url><loc>https://x.fr/</loc><changefreq>daily</changefreq></url>\n"
    "  <url><loc>https://x.fr/blog/tag/a/</loc><lastmod>2026-01-01</lastmod></url>\n"
    "  <url><loc>https://x.fr/blog/tag/b/</loc></url>\n"
    "  <url><loc>https://x.fr/contact</loc></url>\n"
    "</urlset>\n"
)


def test_it_removes_only_the_flagged_entries() -> None:
    out, n = app_module._remove_sitemap_locs(
        SITEMAP, ["https://x.fr/blog/tag/a/", "https://x.fr/blog/tag/b/"])
    assert n == 2
    assert "tag/a" not in out and "tag/b" not in out
    assert "https://x.fr/" in out and "https://x.fr/contact" in out
    assert out.count("<url>") == 2, "an untouched entry was lost"


def test_a_sitemap_index_is_never_gutted() -> None:
    """A sitemap index lists its children in `<sitemap>` blocks that also carry a `<loc>`.
    Matching those would delete an entire sitemap instead of one page."""
    index = (
        "<sitemapindex>\n"
        "  <sitemap><loc>https://x.fr/sitemap-0.xml</loc></sitemap>\n"
        "</sitemapindex>"
    )
    out, n = app_module._remove_sitemap_locs(index, ["https://x.fr/sitemap-0.xml"])
    assert n == 0 and out == index


def test_a_url_it_was_not_asked_about_is_left_alone() -> None:
    out, n = app_module._remove_sitemap_locs(SITEMAP, ["https://x.fr/absente"])
    assert n == 0 and out == SITEMAP


def test_trailing_slash_and_case_do_not_hide_a_match() -> None:
    """The crawler reports the URL it requested; the sitemap may spell it differently."""
    _, n = app_module._remove_sitemap_locs(SITEMAP, ["https://x.fr/blog/tag/a"])
    assert n == 1, "a trailing slash made the entry unmatchable"


def test_the_diff_does_not_reflow_the_file() -> None:
    out, _ = app_module._remove_sitemap_locs(SITEMAP, ["https://x.fr/blog/tag/a/"])
    assert "\n\n" not in out, "the removed block left a blank line behind"
    assert out.startswith('<?xml version="1.0" encoding="UTF-8"?>')


def test_the_family_is_claimed_targeted_and_never_auto_merged() -> None:
    key = "sitemap_noindex_page"
    assert key in app_module._handled_issue_keys()
    assert app_module._github_issue_auto_fixable(key)
    # A sitemap issue is fixed in the sitemap, never in the pages that link to it.
    assert app_module._resolve_issue_targets(
        issue_key=key, issue_label="l", all_paths=["sitemap.xml", "index.html", "blog.html"],
        impacted_urls=["https://x.fr/blog/tag/a/"], evidence=[], located=[], index=None,
        max_files=5) == ["sitemap.xml"]
    # Removing the entry decides which of two contradicting sources wins — a human reads that.
    assert app_module._fix_premise_note(key), "the premise note is what blocks auto-merge"


def test_the_hint_forbids_the_other_half_of_the_contradiction() -> None:
    """A model told only "make them agree" can just as well delete the noindex. On a generated
    sitemap it must reach for the generator's exclusion rule instead of an XML file."""
    prep = app_module._prepare_issue_fix(
        issue_key="sitemap_noindex_page", issues={}, impacted=["https://x.fr/blog/tag/a/"],
        all_paths=["astro.config.mjs"], site_name="x.fr", owner="o", repo_name="r",
        branch="main", token="t", model_override="",
    )
    hint = prep["extra_hint"]
    assert "noindex" in hint and "exclusion" in hint
    assert "astro.config" in hint, "a generated sitemap has no <loc> to delete"
    # Measured on creativeai-tools.com: told only "exclude these paths", the model wrote
    # /blog/(category|tag)/ — 32 entries dropped where 31 are flagged, and the extra one serves
    # `index, follow`. A correction that removes an indexable page is a new defect.
    assert "motif d'URL" in hint, "nothing steers the model away from a path pattern"
    assert "EXACTEMENT" in hint
    assert prep["link_rewriter"] is not None
    assert prep["rewriter_ai_fallback"] is True


def test_a_second_dead_sitemap_is_not_handed_to_the_model() -> None:
    """Measured on a real repository: creativeai-tools.com ships app/sitemap.ts (served) AND
    legacy-static/sitemap.xml (dead). Both match the sitemap-file filter and none of the 31
    flagged URLs is in the legacy one. The AI fallback exists for a GENERATED sitemap; letting it
    reach a literal XML file that simply does not list the page invites an edit to a file that is
    neither served nor wrong."""
    assert app_module._looks_like_sitemap_xml(SITEMAP)
    assert not app_module._looks_like_sitemap_xml(
        "export default async function sitemap() { return metas.map(m => ({url: m.url})); }")
    source = __import__("inspect").getsource(app_module._deep_patch_issue_files)
    assert "_looks_like_sitemap_xml" in source, "the guard is not on the fallback path"
    assert source.index("rewriter_ai_fallback") < source.index("_looks_like_sitemap_xml")


import pytest  # noqa: E402


@pytest.mark.parametrize("stack,tree,expected", [
    # Generated from a config whose NAME says nothing — the two stacks the family used to refuse.
    ("astro", ["astro.config.mjs", "package.json", "src/pages/index.astro"], "astro.config.mjs"),
    ("nuxt", ["nuxt.config.ts", "package.json", "pages/index.vue"], "nuxt.config.ts"),
    ("next-pages", ["package.json", "next-sitemap.config.js"], "next-sitemap.config.js"),
    ("next-app", ["package.json", "app/sitemap.ts"], "app/sitemap.ts"),
    # A committed sitemap always wins, even when a generator config sits next to it.
    ("gatsby", ["gatsby-config.js", "static/sitemap.xml"], "static/sitemap.xml"),
    ("sveltekit", ["svelte.config.js", "static/sitemap.xml"], "static/sitemap.xml"),
    ("hugo", ["hugo.toml", "static/sitemap.xml"], "static/sitemap.xml"),
    ("jekyll", ["_config.yml", "sitemap.xml"], "sitemap.xml"),
    ("static-html", ["index.html", "sitemap.xml"], "sitemap.xml"),
])
def test_every_stack_has_a_sitemap_to_fix(stack: str, tree: list[str], expected: str) -> None:
    """The family is advertised on nine stacks and refused on two of them: the target filter kept
    only paths with "sitemap" in the FILE NAME, while astro and nuxt declare theirs inside a
    config. `nuxt.config.ts` was even in the candidate list already — offered, then filtered out."""
    targets = app_module._resolve_issue_targets(
        issue_key="sitemap_noindex_page", issue_label="l", all_paths=tree,
        impacted_urls=["https://x.fr/a"], evidence=[], located=[], index=None, max_files=5)
    assert targets and targets[0] == expected, f"{stack}: {targets}"


def test_a_generator_config_never_outranks_a_committed_sitemap() -> None:
    """Editing the rule that produced a file, while the file itself sits in the repository, fixes
    the wrong thing — the committed sitemap would keep its stale entries."""
    targets = app_module._resolve_issue_targets(
        issue_key="sitemap_3xx_redirect", issue_label="l",
        all_paths=["gatsby-config.js", "static/sitemap.xml"],
        impacted_urls=["https://x.fr/a"], evidence=[], located=[], index=None, max_files=5)
    assert "gatsby-config.js" not in targets
