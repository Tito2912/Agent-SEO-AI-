"""Every deterministic rewriter, against the shapes a real component actually uses.

The corrector declares 56 auto-fixable anomalies. Eleven of them are repaired WITHOUT a model,
by five rewriters — and the "correctif mécanique" badge, plus auto-merge eligibility, rest on
those rewriters actually matching. When one does not match, the failure is SILENT: the family
either falls back to a model-written patch or, for the redirect-link family which bypasses the
AI by design, produces an empty patch.

Extending the Astro fixture from one injected defect to five put every rewriter through a real
build, and two of them did nothing:

  * `_rewrite_redirect_links` reduced every pair to its PATH, so a site writing internal links
    absolutely — `href="https://site.fr/x/"`, ordinary in generated MDX and in content pasted
    from the live site — matched nothing at all.
  * `_rewrite_head_url_values` knew `<link rel="alternate">` and `languages: {…}` but not an
    alternates ARRAY of `{ lang, href }` objects, which is how a component that feeds a shared
    layout writes them. That is the same gap as the original Astro canonical finding, one branch
    over.

`_rewrite_asset_srcs` was ALSO reported as failing at first; it was not. The harness had passed
no located files, so the asset family simply had no target. Measured again with a locator, it
worked. Recorded because the wrong diagnosis was one commit away.
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

FROM_ABS = "https://site.fr/a-propos/"
TO_ABS = "https://site.fr/a-propos"
PAIR = [{"page": "https://site.fr/en", "from": FROM_ABS, "to": TO_ABS}]


# ── redirect-link rewriter ────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "label, source, expected",
    [
        ("absolute href — the one that matched nothing",
         f'<a href="{FROM_ABS}">x</a>', f'<a href="{TO_ABS}">x</a>'),
        ("relative href — already worked, must keep working",
         '<a href="/a-propos/">x</a>', '<a href="/a-propos">x</a>'),
        ("markdown link", f'[x]({FROM_ABS})', f'[x]({TO_ABS})'),
    ],
)
def test_a_redirecting_link_is_rewritten_however_it_is_written(label, source, expected) -> None:
    new, count = app_module._rewrite_redirect_links(source, PAIR)
    assert count == 1, label
    assert new == expected


@pytest.mark.parametrize(
    "label, source",
    [
        ("a code literal is not a link", "if (p.startsWith('/a-propos/')) return;"),
        ("a longer path merely starting with it", '<a href="/a-propos/equipe">x</a>'),
        ("an absolute path merely starting with it", f'<a href="{FROM_ABS}equipe">x</a>'),
    ],
)
def test_the_redirect_rewriter_still_refuses_what_it_always_refused(label, source) -> None:
    """Widening to absolute URLs must not widen the blast radius.

    A redirecting `/en/` turning the valid `/en/guide` into `/enguide` is the bug this
    rewriter's delimiter rule exists to prevent; it shipped once already.
    """
    new, count = app_module._rewrite_redirect_links(source, PAIR)
    assert count == 0, label
    assert new == source


def test_a_relative_link_never_becomes_absolute_and_the_reverse() -> None:
    # Each writing maps to its own target; mixing them would rewrite a site's whole link style.
    rel, _ = app_module._rewrite_redirect_links('<a href="/a-propos/">x</a>', PAIR)
    assert rel == '<a href="/a-propos">x</a>' and "https://" not in rel
    absolute, _ = app_module._rewrite_redirect_links(f'<a href="{FROM_ABS}">x</a>', PAIR)
    assert absolute.count("https://site.fr") == 1


# ── head/alternate rewriter ───────────────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "label, source, expected",
    [
        ("alternates array of objects — the one that matched nothing",
         "const alternates = [{ lang: 'fr', href: '%s' }];" % FROM_ABS,
         "const alternates = [{ lang: 'fr', href: '%s' }];" % TO_ABS),
        ("languages map — already worked",
         "languages: { 'fr': '%s' }" % FROM_ABS,
         "languages: { 'fr': '%s' }" % TO_ABS),
        ("raw alternate tag — already worked",
         f'<link rel="alternate" hreflang="fr" href="{FROM_ABS}" />',
         f'<link rel="alternate" hreflang="fr" href="{TO_ABS}" />'),
    ],
)
def test_an_hreflang_target_is_rewritten_however_it_is_declared(label, source, expected) -> None:
    new, count = app_module._rewrite_head_url_values(source, PAIR)
    assert count == 1, label
    assert new == expected


def test_a_navigation_array_is_still_left_alone() -> None:
    """The rule that survives the widening: this rewriter fixes what a page DECLARES about
    itself, never where its menu points. Scoping to an alternates-named binding is what keeps
    a plain `href:` list out."""
    nav = "const nav = [{ label: 'A propos', href: '%s' }];" % FROM_ABS
    new, count = app_module._rewrite_head_url_values(nav, PAIR)
    assert count == 0 and new == nav


def test_an_alternates_array_is_read_whole() -> None:
    """Brace matching stopped at the first inner object and saw one entry out of N."""
    source = (
        "const alternates = [\n"
        "  { lang: 'en', href: 'https://site.fr/en' },\n"
        "  { lang: 'fr', href: '%s' },\n"
        "  { lang: 'x-default', href: 'https://site.fr/en' },\n"
        "];" % FROM_ABS
    )
    new, count = app_module._rewrite_head_url_values(source, PAIR)
    assert count == 1, "the entry past the first object was never reached"
    assert TO_ABS + "'" in new


# ── the fixture that found all of this ────────────────────────────────────────────────────────

FIXTURE = WEB_ROOT / "tests" / "fixtures" / "astro"


@pytest.mark.parametrize(
    "relative, needle",
    [
        ("src/pages/blog.astro", "127.0.0.1:8741/blog/"),
        ("src/pages/en.astro", "127.0.0.1:8741/a-propos/"),
        ("src/pages/liens.astro", 'href="http://127.0.0.1:8741/a-propos/"'),
        ("src/pages/liens.astro", 'src="http://127.0.0.1:8741/og.png/"'),
        ("public/sitemap.xml", "127.0.0.1:8741/liens/"),
    ],
)
def test_the_astro_fixture_still_carries_all_five_defects(relative: str, needle: str) -> None:
    """Five families, five rewriters. A fixture that quietly loses a defect stops proving
    anything, and this one is the only place four of these rewriters are exercised end to end."""
    path = FIXTURE / relative
    if not path.exists():  # pragma: no cover - the fixture is committed alongside this test
        pytest.skip("astro fixture missing")
    assert needle in path.read_text(encoding="utf-8"), f"{relative} lost {needle}"


def test_the_shared_layout_declares_no_literal_url() -> None:
    # Base.astro renders `href={canonical}` and maps the alternates: bindings, not values.
    # Nothing there for any rewriter to touch, which is what keeps a per-page fix per-page.
    layout = FIXTURE / "src" / "layouts" / "Base.astro"
    if not layout.exists():  # pragma: no cover
        pytest.skip("astro fixture missing")
    text = layout.read_text(encoding="utf-8")
    _new, count = app_module._rewrite_head_url_values(text, PAIR)
    assert count == 0
    _new2, count2 = app_module._rewrite_redirect_links(text, PAIR)
    assert count2 == 0


# ── the two model-written families the fixture also carries ───────────────────────────────────
# Their CONTENT cannot be asserted — a model writes it. What can be asserted is that the fixture
# still poses the question, and what the measured answer was.

def test_the_fixture_still_carries_a_too_long_meta_description() -> None:
    """Exercised for real against the production model (gpt-4o-mini) on 2026-08-29: rewritten to
    140 characters, inside [100, 160], and the anomaly cleared after rebuild."""
    import re

    page = FIXTURE / "src" / "pages" / "a-propos.astro"
    if not page.exists():  # pragma: no cover
        pytest.skip("astro fixture missing")
    match = re.search(r'description="([^"]*)"', page.read_text(encoding="utf-8"))
    assert match, "the control page lost its description"
    assert len(match.group(1)) > 160, (
        "the fixture no longer poses the meta_description_too_long question"
    )


def test_the_fixture_still_declares_alternates_without_x_default() -> None:
    """Hint-only family: no evidence at all, the model works from the instruction alone.
    Measured 2026-08-29: the x-default entry was added to the right array, anomaly cleared."""
    page = FIXTURE / "src" / "pages" / "liens.astro"
    if not page.exists():  # pragma: no cover
        pytest.skip("astro fixture missing")
    text = page.read_text(encoding="utf-8")
    assert "const alternates" in text, "the alternates array is gone"
    # Match an ENTRY, not the word: the comment above the array names the family it poses, and
    # a bare substring check failed on its own documentation.
    assert "lang: 'x-default'" not in text, (
        "the fixture no longer poses the x_default_hreflang_missing question"
    )
