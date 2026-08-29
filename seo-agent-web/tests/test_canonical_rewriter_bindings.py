"""A canonical is written three ways, and only one of them was recognised.

Found by running the full loop on a real buildable Astro site (tests/fixtures/astro): inject the
defect, build, serve, crawl, target, rewrite, rebuild, re-crawl. Detection and targeting were
perfect — `src/pages/blog.astro` alone, not the shared layout, not the control page — and then
`_rewrite_head_url_values` made **zero** replacements.

In Astro the `<link rel="canonical">` lives in the shared layout as `href={canonical}`, an
expression, while the VALUE sits in the page as `const canonical = '…'`. `_JS_CANONICAL_RE` only
knew the object-property form `canonical:`, so the family silently fell through to the AI
fallback — losing its "correctif mécanique" badge, and its auto-merge eligibility, on the most
idiomatic way to write a canonical in that stack.

This is the class of bug the fixture TREES cannot reach: they prove which file gets picked, not
what gets written into it.
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

PAIR = [{"page": "https://site.fr/blog", "from": "https://site.fr/blog/", "to": "https://site.fr/blog"}]


def _rewrite(content: str) -> tuple[str, int]:
    return app_module._rewrite_head_url_values(content, PAIR)


@pytest.mark.parametrize(
    "label, source, expected",
    [
        (
            "Astro/JSX binding — the one that was missed",
            "const canonical = 'https://site.fr/blog/';",
            "const canonical = 'https://site.fr/blog';",
        ),
        (
            "object property — already worked, must keep working",
            'canonical: "https://site.fr/blog/",',
            'canonical: "https://site.fr/blog",',
        ),
        (
            "literal component prop",
            '<Base canonical="https://site.fr/blog/">',
            '<Base canonical="https://site.fr/blog">',
        ),
        (
            "raw link tag — the original branch",
            '<link rel="canonical" href="https://site.fr/blog/">',
            '<link rel="canonical" href="https://site.fr/blog">',
        ),
    ],
)
def test_every_way_a_canonical_is_written_is_rewritten(label: str, source: str, expected: str) -> None:
    new, count = _rewrite(source)
    assert count == 1, f"{label}: not recognised, so the family degrades to a model-written patch"
    assert new == expected


@pytest.mark.parametrize(
    "label, source",
    [
        ("a data- attribute is not a declaration", 'data-canonical="https://site.fr/blog/"'),
        ("a different identifier that merely ends in canonical", 'mycanonical: "https://site.fr/blog/"'),
        ("an identifier that merely starts with it", 'canonicalUrlBackup: "https://site.fr/blog/"'),
    ],
)
def test_a_lookalike_binding_is_left_alone(label: str, source: str) -> None:
    """Widening the pattern must not widen the blast radius.

    `mycanonical:` matched the ORIGINAL pattern, which had no boundary — so this is a narrowing
    as much as a widening.
    """
    new, count = _rewrite(source)
    assert count == 0, label
    assert new == source


def test_a_navigation_link_to_the_same_url_is_still_untouched() -> None:
    # The deliberate rule of this rewriter: the fix is about which URL the page DECLARES as
    # canonical, never about where its menu points.
    source = '<nav><a href="https://site.fr/blog/">Blog</a></nav>'
    new, count = _rewrite(source)
    assert count == 0 and new == source


def test_the_real_astro_page_from_the_fixture_is_fixed_in_one_line() -> None:
    """The exact file the loop ran on, so the test and the fixture cannot drift apart."""
    page = WEB_ROOT / "tests" / "fixtures" / "astro" / "src" / "pages" / "blog.astro"
    if not page.exists():  # pragma: no cover - the fixture is committed alongside this test
        pytest.skip("astro fixture missing")
    source = page.read_text(encoding="utf-8")
    assert "127.0.0.1:8741/blog/" in source, (
        "the fixture must ship WITH its defect — that is the whole point of it"
    )

    pair = [{
        "page": "http://127.0.0.1:8741/blog",
        "from": "http://127.0.0.1:8741/blog/",
        "to": "http://127.0.0.1:8741/blog",
    }]
    new, count = app_module._rewrite_head_url_values(source, pair)
    assert count == 1, "the fixture's canonical is no longer reachable by the rewriter"
    changed = [
        (a, b) for a, b in zip(source.splitlines(), new.splitlines()) if a != b
    ]
    assert len(changed) == 1 and "const canonical" in changed[0][0]


def test_the_shared_layout_is_not_a_target_for_a_per_page_value() -> None:
    # Base.astro renders `href={canonical}` — an expression with no literal URL. Nothing to
    # rewrite there, and writing one page's value into it would stamp it on every page.
    layout = WEB_ROOT / "tests" / "fixtures" / "astro" / "src" / "layouts" / "Base.astro"
    if not layout.exists():  # pragma: no cover
        pytest.skip("astro fixture missing")
    _new, count = _rewrite(layout.read_text(encoding="utf-8"))
    assert count == 0


def test_the_real_hugo_page_from_the_fixture_is_fixed_in_one_line() -> None:
    """Second stack, second language: TOML front matter, `canonical = "…"`.

    Hugo separates what Astro joins — the <head> is a `layouts/` template, the per-page value is
    front matter — so this is the same concept written a third way. It passes only because of the
    widening the Astro loop forced; that is the point of doing these one stack at a time.
    """
    page = WEB_ROOT / "tests" / "fixtures" / "hugo" / "content" / "blog.md"
    if not page.exists():  # pragma: no cover - the fixture is committed alongside this test
        pytest.skip("hugo fixture missing")
    source = page.read_text(encoding="utf-8")
    assert "127.0.0.1:8742/blog/" in source, (
        "the fixture must ship WITH its defect — that is the whole point of it"
    )

    pair = [{
        "page": "http://127.0.0.1:8742/blog",
        "from": "http://127.0.0.1:8742/blog/",
        "to": "http://127.0.0.1:8742/blog",
    }]
    new, count = app_module._rewrite_head_url_values(source, pair)
    assert count == 1, "TOML front matter is not reachable by the deterministic rewriter"
    changed = [(a, b) for a, b in zip(source.splitlines(), new.splitlines()) if a != b]
    assert len(changed) == 1 and changed[0][0].startswith("canonical = ")


def test_the_hugo_template_holds_no_literal_url_to_rewrite() -> None:
    # `href="{{ .Params.canonical }}"` is a binding, not a value. Nothing to rewrite in the
    # shared template — and writing one page's canonical there would stamp it on every page.
    layout = WEB_ROOT / "tests" / "fixtures" / "hugo" / "layouts" / "_default" / "baseof.html"
    if not layout.exists():  # pragma: no cover
        pytest.skip("hugo fixture missing")
    _new, count = _rewrite(layout.read_text(encoding="utf-8"))
    assert count == 0
