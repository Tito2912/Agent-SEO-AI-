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


def test_the_real_sveltekit_page_from_the_fixture_is_fixed_in_one_line() -> None:
    """`const canonical` inside a Svelte `<script>`, with `href={canonical}` in <svelte:head>."""
    page = WEB_ROOT / "tests" / "fixtures" / "sveltekit" / "src" / "routes" / "blog" / "+page.svelte"
    if not page.exists():  # pragma: no cover - the fixture is committed alongside this test
        pytest.skip("sveltekit fixture missing")
    source = page.read_text(encoding="utf-8")
    assert "127.0.0.1:8743/blog/" in source, "the fixture must ship WITH its defect"

    pair = [{
        "page": "http://127.0.0.1:8743/blog",
        "from": "http://127.0.0.1:8743/blog/",
        "to": "http://127.0.0.1:8743/blog",
    }]
    new, count = app_module._rewrite_head_url_values(source, pair)
    assert count == 1
    changed = [(a, b) for a, b in zip(source.splitlines(), new.splitlines()) if a != b]
    assert len(changed) == 1 and "const canonical" in changed[0][0]


def test_the_real_gatsby_page_from_the_fixture_is_fixed_in_one_line() -> None:
    """JSX with the Gatsby Head API: the value is a module-level binding."""
    page = WEB_ROOT / "tests" / "fixtures" / "gatsby" / "src" / "pages" / "blog.js"
    if not page.exists():  # pragma: no cover
        pytest.skip("gatsby fixture missing")
    source = page.read_text(encoding="utf-8")
    assert "127.0.0.1:8744/blog/" in source, "the fixture must ship WITH its defect"

    pair = [{
        "page": "http://127.0.0.1:8744/blog",
        "from": "http://127.0.0.1:8744/blog/",
        "to": "http://127.0.0.1:8744/blog",
    }]
    new, count = app_module._rewrite_head_url_values(source, pair)
    assert count == 1
    changed = [(a, b) for a, b in zip(source.splitlines(), new.splitlines()) if a != b]
    assert len(changed) == 1 and "const canonical" in changed[0][0]


def test_the_gatsby_fixture_is_never_handed_the_next_js_idiom() -> None:
    """The bug that made this stack worth proving: right file, another framework's code.

    A Gatsby repo detected as next-pages got told to import `next/head`, which does not exist
    there — a PR that reads fine and breaks the build.
    """
    from backend import repo_index as ri

    root = WEB_ROOT / "tests" / "fixtures" / "gatsby"
    if not root.exists():  # pragma: no cover
        pytest.skip("gatsby fixture missing")
    skip = {"node_modules", "public", ".cache"}
    paths = sorted(
        str(p.relative_to(root)).replace("\\", "/")
        for p in root.rglob("*")
        if p.is_file() and not (skip & set(p.parts))
    )
    index = ri.build_repo_index(paths)
    assert index["stack"] == "gatsby"
    assert index["routes"].get("/blog") == ["src/pages/blog.js"]

    hint = ri.stack_idiom_hint(index)
    assert "Gatsby" in hint and "JAMAIS" in hint


@pytest.mark.parametrize(
    "stack, relative, port",
    [
        ("next-pages", "pages/blog.js", 8745),
        ("nuxt", "pages/blog.vue", 8746),
    ],
)
def test_the_remaining_fixtures_are_fixed_in_one_line(stack: str, relative: str, port: int) -> None:
    """Next Pages writes the canonical through `next/head`, Nuxt through a `useHead()` call.

    Different idioms, same underlying shape: the VALUE is a module-level binding. Six stacks now
    agree on that, which is why one widening covered five of them.
    """
    page = WEB_ROOT / "tests" / "fixtures" / stack / relative
    if not page.exists():  # pragma: no cover - fixtures are committed alongside this test
        pytest.skip(f"{stack} fixture missing")
    source = page.read_text(encoding="utf-8")
    assert f"127.0.0.1:{port}/blog/" in source, "the fixture must ship WITH its defect"

    pair = [{
        "page": f"http://127.0.0.1:{port}/blog",
        "from": f"http://127.0.0.1:{port}/blog/",
        "to": f"http://127.0.0.1:{port}/blog",
    }]
    new, count = app_module._rewrite_head_url_values(source, pair)
    assert count == 1
    changed = [(a, b) for a, b in zip(source.splitlines(), new.splitlines()) if a != b]
    assert len(changed) == 1 and "const canonical" in changed[0][0]


@pytest.mark.parametrize(
    "stack, expected, route, source_file",
    [
        ("astro", "astro", "/blog", "src/pages/blog.astro"),
        ("hugo", "hugo", "/blog", "content/blog.md"),
        ("sveltekit", "sveltekit", "/blog", "src/routes/blog/+page.svelte"),
        ("gatsby", "gatsby", "/blog", "src/pages/blog.js"),
        ("next-pages", "next-pages", "/blog", "pages/blog.js"),
        ("nuxt", "nuxt", "/blog", "pages/blog.vue"),
        ("jekyll", "jekyll", "/blog", "blog.html"),
    ],
)
def test_every_buildable_fixture_maps_its_page_to_its_own_source(
    stack: str, expected: str, route: str, source_file: str
) -> None:
    """One assertion per stack that has been through the full loop.

    These trees are the REAL repos the loop ran against, not hand-written path lists, so a
    detection or routing regression shows up here rather than on a customer's repo.
    """
    from backend import repo_index as ri

    root = WEB_ROOT / "tests" / "fixtures" / stack
    if not root.exists():  # pragma: no cover
        pytest.skip(f"{stack} fixture missing")
    generated = {
        "node_modules", "public", "dist", "build", "out", ".next", ".nuxt",
        ".output", ".svelte-kit", ".cache", "resources",
    }
    paths = sorted(
        str(p.relative_to(root)).replace("\\", "/")
        for p in root.rglob("*")
        if p.is_file() and not (generated & set(p.parts)) and not p.name.endswith(".log")
    )
    index = ri.build_repo_index(paths)
    assert index["stack"] == expected
    assert index["routes"].get(route) == [source_file], (
        f"{stack}: {route} resolved to {index['routes'].get(route)}"
    )
    assert ri.stack_idiom_hint(index), f"{stack} has routes but no idiom — the Gatsby failure"


def test_the_real_jekyll_page_from_the_fixture_is_fixed_in_one_line() -> None:
    """YAML front matter — the third serialisation, after Astro's JS and Hugo's TOML.

    This one needed no widening: `canonical:` as an object property is the form the rewriter
    already knew. Measured anyway, because an unverified prediction is worth nothing.
    """
    page = WEB_ROOT / "tests" / "fixtures" / "jekyll" / "blog.html"
    if not page.exists():  # pragma: no cover
        pytest.skip("jekyll fixture missing")
    source = page.read_text(encoding="utf-8")
    assert "127.0.0.1:8747/blog/" in source, "the fixture must ship WITH its defect"

    pair = [{
        "page": "http://127.0.0.1:8747/blog",
        "from": "http://127.0.0.1:8747/blog/",
        "to": "http://127.0.0.1:8747/blog",
    }]
    new, count = app_module._rewrite_head_url_values(source, pair)
    assert count == 1
    changed = [(a, b) for a, b in zip(source.splitlines(), new.splitlines()) if a != b]
    assert len(changed) == 1 and changed[0][0].startswith("canonical:")


def test_the_jekyll_layout_holds_no_literal_url_to_rewrite() -> None:
    # Liquid: `href="{{ page.canonical }}"` is a binding, not a value.
    layout = WEB_ROOT / "tests" / "fixtures" / "jekyll" / "_layouts" / "default.html"
    if not layout.exists():  # pragma: no cover
        pytest.skip("jekyll fixture missing")
    _new, count = _rewrite(layout.read_text(encoding="utf-8"))
    assert count == 0
