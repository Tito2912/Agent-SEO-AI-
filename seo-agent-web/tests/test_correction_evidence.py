"""The unified evidence contract and the canonical/hreflang rewriter it enables.

Every URL-rewrite family the corrector shipped went wrong the same way before it became
deterministic: the LLM inverted a direction, rewrote a code literal, or turned a relative
link absolute. These tests pin what the rewriter is allowed to touch — and, just as
importantly, what it must leave alone.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-evidence-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402


def _pairs(*items: tuple[str, str]) -> list[dict[str, str]]:
    return [{"page": "https://site.com/p", "from": f, "to": t} for f, t in items]


# ── Reading the contract ─────────────────────────────────────────────────────────────

def test_url_pairs_are_read_from_the_unified_evidence_block() -> None:
    block = {
        "count": 2,
        "examples": [],
        "evidence": {
            "kind": "url_pairs",
            "items": [
                {"page": "https://site.com/a", "from": "https://site.com/x/", "to": "https://site.com/x"},
                {"page": "https://site.com/b", "from": "", "to": "https://site.com/y"},
            ],
        },
    }
    pairs = app_module._issue_url_pairs(block)

    assert pairs == [{"page": "https://site.com/a", "from": "https://site.com/x/", "to": "https://site.com/x"}]


def test_missing_or_foreign_evidence_degrades_to_empty() -> None:
    # A report produced before the contract existed must not break the corrector.
    assert app_module._issue_url_pairs({"count": 3, "examples": []}) == []
    assert app_module._issue_url_pairs({"evidence": {"kind": "elements", "items": [1]}}) == []
    assert app_module._issue_url_pairs(None) == []


# ── Rewriting ────────────────────────────────────────────────────────────────────────

def test_canonical_link_tag_is_repointed_to_the_final_url() -> None:
    html = (
        '<head>\n'
        '  <link rel="canonical" href="https://site.com/guide/">\n'
        '  <link rel="alternate" hreflang="en" href="https://site.com/en/guide/">\n'
        '</head>'
    )
    new, n = app_module._rewrite_head_url_values(
        html, _pairs(("https://site.com/guide/", "https://site.com/guide"),
                     ("https://site.com/en/guide/", "https://site.com/en/guide")),
    )

    assert n == 2
    assert '<link rel="canonical" href="https://site.com/guide">' in new
    assert 'hreflang="en" href="https://site.com/en/guide"' in new


def test_a_navigation_link_to_the_same_url_is_never_touched() -> None:
    html = (
        '<link rel="canonical" href="https://site.com/guide/">\n'
        '<a href="https://site.com/guide/">Le guide</a>\n'
        '<link rel="stylesheet" href="https://site.com/guide/">'
    )
    new, n = app_module._rewrite_head_url_values(
        html, _pairs(("https://site.com/guide/", "https://site.com/guide")),
    )

    assert n == 1
    # The menu still points where the site owner put it — this fix is about the declared
    # canonical, not about navigation.
    assert '<a href="https://site.com/guide/">' in new
    assert '<link rel="stylesheet" href="https://site.com/guide/">' in new


def test_next_metadata_canonical_and_languages_map_are_rewritten() -> None:
    src = """
export const metadata = {
  alternates: {
    canonical: '/guide/',
    languages: { 'en': '/en/guide/', 'fr': '/fr/guide' },
  },
};
const redirectTo = { to: '/en/guide/' };
"""
    new, n = app_module._rewrite_head_url_values(
        src, _pairs(("https://site.com/guide/", "https://site.com/guide"),
                    ("https://site.com/en/guide/", "https://site.com/en/guide")),
    )

    assert n == 2
    # Relative values stay relative — the fix must not rewrite the site's URL style.
    assert "canonical: '/guide'," in new
    assert "'en': '/en/guide'," in new
    assert "'fr': '/fr/guide'" in new
    # A same-valued key OUTSIDE a languages map is not a hreflang entry.
    assert "const redirectTo = { to: '/en/guide/' };" in new


def test_only_exact_values_match_so_a_prefix_never_rewrites_a_child_page() -> None:
    html = (
        '<link rel="canonical" href="/en/">\n'
        '<link rel="alternate" hreflang="en" href="/en/guide-etoro">'
    )
    new, n = app_module._rewrite_head_url_values(html, _pairs(("https://site.com/en/", "https://site.com/en")))

    assert n == 1
    assert '<link rel="canonical" href="/en">' in new
    # `/en/` redirecting says nothing about `/en/guide-etoro`, which is a different page.
    assert 'href="/en/guide-etoro"' in new


def test_http_to_https_canonical_keeps_the_absolute_form() -> None:
    html = '<link rel="canonical" href="http://site.com/a">'
    new, n = app_module._rewrite_head_url_values(
        html, _pairs(("http://site.com/a", "https://site.com/a")),
    )

    assert n == 1
    assert 'href="https://site.com/a"' in new


def test_a_dynamically_built_canonical_yields_no_change_so_the_ai_fallback_runs() -> None:
    src = "export const metadata = { alternates: { canonical: getSiteUrl(path) } };"
    new, n = app_module._rewrite_head_url_values(src, _pairs(("https://site.com/x/", "https://site.com/x")))

    assert (new, n) == (src, 0)


# ── Asset references ─────────────────────────────────────────────────────────────────

def test_redirected_asset_is_repointed_in_every_reference_form() -> None:
    src = """
<img src="/img/logo.png" srcset="/img/logo.png 1x, /img/logo@2x.png 2x">
<div style="background: url('/img/logo.png')"></div>
<script src="/js/app.js"></script>
"""
    new, n = app_module._rewrite_asset_srcs(
        src, _pairs(("https://site.com/img/logo.png", "https://site.com/img/logo.v2.png")),
    )

    assert n == 3  # src attribute, srcset first entry, and the CSS url()
    assert "/img/logo.v2.png" in new
    assert "/img/logo.png" not in new
    # A different asset that merely shares the prefix is untouched.
    assert "/img/logo@2x.png 2x" in new
    assert '<script src="/js/app.js">' in new


def test_asset_rewrite_keeps_the_absolute_form_when_the_page_uses_it() -> None:
    src = '<img src="https://site.com/img/a.png">'
    new, n = app_module._rewrite_asset_srcs(
        src, _pairs(("https://site.com/img/a.png", "https://cdn.site.com/img/a.png")),
    )

    assert n == 1
    assert 'src="https://cdn.site.com/img/a.png"' in new


def test_a_bundled_asset_yields_no_change_so_the_file_is_skipped() -> None:
    src = "import logo from '../assets/logo.png';\n<img src={logo} />"
    new, n = app_module._rewrite_asset_srcs(
        src, _pairs(("https://site.com/_next/static/logo.png", "https://site.com/static/logo.png")),
    )

    assert (new, n) == (src, 0)


def test_an_unbalanced_languages_block_is_skipped_not_guessed() -> None:
    src = "alternates: { languages: { 'en': '/en/guide/'"  # truncated file
    new, n = app_module._rewrite_head_url_values(
        src, _pairs(("https://site.com/en/guide/", "https://site.com/en/guide")),
    )

    assert (new, n) == (src, 0)
