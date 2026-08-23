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


# ── Current-state evidence (page_values) ─────────────────────────────────────────────

def test_page_values_are_read_and_kept_separate_from_url_pairs() -> None:
    block = {
        "evidence": {
            "kind": "page_values",
            "items": [
                {"page": "https://site.com/a", "field": "og_manquants", "value": "og:image, og:type"},
                {"page": "", "field": "og_manquants", "value": "og:url"},
            ],
        }
    }

    assert app_module._issue_page_values(block) == [
        {"page": "https://site.com/a", "field": "og_manquants", "value": "og:image, og:type"}
    ]
    # The two kinds never bleed into each other.
    assert app_module._issue_url_pairs(block) == []
    assert app_module._issue_page_values({"evidence": {"kind": "url_pairs", "items": [
        {"page": "p", "from": "a", "to": "b"}]}}) == []


def test_page_values_hint_names_the_page_and_what_is_wrong_on_it() -> None:
    hint = app_module._build_page_values_hint([
        {"page": "https://site.com/a", "field": "og_manquants", "value": "og:image, og:type"},
        {"page": "https://site.com/b", "field": "lang invalide", "value": "en_US"},
    ])

    assert "https://site.com/a → og_manquants : og:image, og:type" in hint
    assert "https://site.com/b → lang invalide : en_US" in hint
    # The instruction that stops the patch from clobbering correct values.
    assert "n'écrase pas les valeurs" in hint
    assert app_module._build_page_values_hint([]) == ""


# ── Collateral damage of a fix ───────────────────────────────────────────────────────

def test_a_fix_that_creates_another_issue_is_reported() -> None:
    # The real July regression: shortening titles pushed 9 pages over the long threshold.
    before = {"title_too_short": 15, "title_too_long": 0, "missing_alt_text": 4}
    after = {"title_too_short": 7, "title_too_long": 9, "missing_alt_text": 4}

    grown = app_module._collateral_introduced(before, after)

    assert grown == [{"key": "title_too_long", "before": 0, "after": 9, "delta": 9}]


def test_a_brand_new_issue_key_counts_as_introduced() -> None:
    grown = app_module._collateral_introduced({}, {"missing_reciprocal_hreflang": 4})

    assert grown[0]["key"] == "missing_reciprocal_hreflang"
    assert grown[0]["delta"] == 4


def test_between_crawl_change_metrics_are_not_damage() -> None:
    # These move by construction as soon as anything is fixed; counting them would drown
    # the signal they exist to support.
    after = {
        "title_tag_changed": 9, "canonical_url_changed": 3,
        "indexable_page_became_non_indexable": 1, "pages_added_to_sitemaps": 4,
        "no_of_urls_in_sitemap_decreased": 1,
    }

    assert app_module._collateral_introduced({}, after) == []


def test_a_clean_fix_reports_nothing() -> None:
    counts = {"title_too_short": 3, "missing_alt_text": 2}

    assert app_module._collateral_introduced(counts, {"title_too_short": 0, "missing_alt_text": 2}) == []


def test_issue_counts_survive_a_malformed_report() -> None:
    assert app_module._report_issue_counts({}) == {}
    assert app_module._report_issue_counts({"issues": "nope"}) == {}
    assert app_module._report_issue_counts({"issues": {"a": {"count": "3"}, "b": {"count": None}, "c": 7}}) == {"a": 3, "b": 0}


# ── Mechanical vs model-written ──────────────────────────────────────────────────────

def test_a_pr_says_whether_a_human_must_read_the_diff() -> None:
    mechanical = app_module._fix_nature_note(False)
    editorial = app_module._fix_nature_note(True)

    assert "mécanique" in mechanical and "prévisible" in mechanical
    assert "À relire avant de merger" in editorial and "rédigé par le" in editorial
    # The two must not be confusable: identical-looking PRs are what made auto-merge unsafe.
    assert mechanical != editorial


# ── Advice for the issues we refuse to fix ───────────────────────────────────────────

def _redirect_report(*fields: str) -> dict[str, object]:
    return {"issues": {"redirect_3xx": {"evidence": {"kind": "page_values", "items": [
        {"page": f"https://site.com/{i}", "field": f, "value": "x"} for i, f in enumerate(fields)
    ]}}}}


def _advice(report: dict[str, object]) -> dict:
    from backend import fix_suggestions
    return fix_suggestions.suggest_issue_fix(
        issue_key="redirect_3xx", label="Redirection 3XX", category="Redirects",
        severity="warning", count=4, report=report, site_name="s", base_url="https://site.com",
    )


def test_advice_tells_a_healthy_site_to_change_nothing() -> None:
    out = _advice(_redirect_report("canonicalisation attendue", "canonicalisation attendue"))

    # We refuse to auto-fix these, so the advice is all we offer -- it must not imply a defect.
    assert "canonicalisation volontaire" in out["why"]
    assert any("Ne rien changer" in f for f in out["fix"])


def test_advice_singles_out_the_loop_and_spares_the_rest() -> None:
    out = _advice(_redirect_report("canonicalisation attendue", "boucle: redirige vers elle-meme"))

    assert "Une de ces redirections forme une boucle" in out["why"]
    assert any("Casser la boucle" in f for f in out["fix"])
    assert any("Laisser l'autre telle quelle" in f for f in out["fix"])


def test_advice_degrades_without_evidence_but_never_lies() -> None:
    out = _advice({})

    # A report predating the evidence contract still gets usable, non-committal guidance.
    assert "Certaines sont voulues" in out["why"]
    assert out["fix"]


# ── Sitemap entries ──────────────────────────────────────────────────────────────────

SITEMAP = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:xhtml="http://www.w3.org/1999/xhtml">
<url><loc>https://site.com/a/</loc>
<xhtml:link rel="alternate" hreflang="en" href="https://site.com/a/"/>
<lastmod>2026-08-01</lastmod></url>
<url><loc>https://site.com/keep</loc></url>
</urlset>"""


def test_a_flagged_loc_is_replaced_and_nothing_else_is() -> None:
    new, n = app_module._rewrite_sitemap_locs(
        SITEMAP, _pairs(("https://site.com/a/", "https://site.com/a")),
    )

    assert n == 1
    assert "<loc>https://site.com/a</loc>" in new
    assert "<loc>https://site.com/keep</loc>" in new
    # The hreflang alternate carries the same URL but belongs to another issue family:
    # widening the blast radius here is how a sitemap fix once rewrote a <loc> by mistake.
    assert 'href="https://site.com/a/"' in new


def test_a_generated_sitemap_yields_no_change_so_the_ai_fallback_runs() -> None:
    src = "export default function sitemap() { return routes.map(toEntry); }"
    assert app_module._rewrite_sitemap_locs(src, _pairs(("https://site.com/a/", "https://site.com/a"))) == (src, 0)


def test_only_the_sitemap_file_can_carry_a_sitemap_fix() -> None:
    assert app_module._is_sitemap_path("sitemap.xml")
    assert app_module._is_sitemap_path("public/sitemap.xml")
    assert app_module._is_sitemap_path("app/sitemap.ts")
    assert app_module._is_sitemap_path("next-sitemap.config.js")
    assert not app_module._is_sitemap_path("de/legal-notice.html")
    assert not app_module._is_sitemap_path("app/layout.tsx")


# ── Duplicate-PR guard ───────────────────────────────────────────────────────────────

def test_an_open_pr_blocks_a_second_one_but_a_closed_one_does_not(monkeypatch) -> None:
    calls: list[int] = []

    def fake_get(path, **kw):
        number = int(path.rstrip("/").rsplit("/", 1)[-1])
        calls.append(number)
        return {11: {"state": "open"},
                12: {"state": "closed"},
                13: {"state": "closed", "merged": True, "merged_at": "2026-08-23T10:00:00Z"}}[number]

    monkeypatch.setattr(app_module, "_github_api_get", fake_get)

    assert app_module._github_pr_is_open("o", "r", 11, "tok") is True
    # A closed or merged PR must NOT block: the anomaly can legitimately come back.
    assert app_module._github_pr_is_open("o", "r", 12, "tok") is False
    assert app_module._github_pr_is_open("o", "r", 13, "tok") is False
    assert calls == [11, 12, 13]


def test_an_unreachable_github_lets_the_user_through(monkeypatch) -> None:
    def boom(*a, **kw):
        raise RuntimeError("rate limited")

    monkeypatch.setattr(app_module, "_github_api_get", boom)

    # This check gates an action, so anything unknown must not block on a guess.
    assert app_module._github_pr_is_open("o", "r", 11, "tok") is False
    assert app_module._github_pr_is_open("o", "r", 0, "tok") is False
    assert app_module._github_pr_is_open("o", "r", 11, "") is False


# ── Redirect config family ───────────────────────────────────────────────────────────

def _redirect_block(*items: tuple[str, str]) -> dict[str, object]:
    return {"evidence": {"kind": "page_values", "items": [
        {"page": p, "field": f, "value": p} for p, f in items
    ]}}


def test_only_self_redirecting_urls_are_offered_to_the_config_fixer() -> None:
    block = _redirect_block(
        ("http://site.test/", "canonicalisation attendue"),
        ("https://www.site.test/", "canonicalisation attendue"),
        ("https://site.test/sources/etoro-en", app_module._SELF_LOOP_FIELD),
        ("https://site.test/old", "redirection"),
    )

    # The deliberate canonicalisation redirects are NOT touchable: the config file that
    # produces them also carries the site's HSTS, CSP and cache rules.
    assert app_module._redirect_3xx_self_loops(block) == ["/sources/etoro-en"]


def test_a_site_whose_redirects_are_all_deliberate_offers_nothing_to_fix() -> None:
    block = _redirect_block(
        ("http://site.test/", "canonicalisation attendue"),
        ("https://www.site.test/", "canonicalisation attendue"),
    )

    assert app_module._redirect_3xx_self_loops(block) == []
    # And a report with no evidence at all must not invent work either.
    assert app_module._redirect_3xx_self_loops({"count": 4, "examples": []}) == []


def test_the_domain_root_is_never_treated_as_a_self_loop() -> None:
    # A root that redirects is domain canonicalisation, out of scope by design.
    block = _redirect_block(("https://site.test/", app_module._SELF_LOOP_FIELD))

    assert app_module._redirect_3xx_self_loops(block) == []


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
