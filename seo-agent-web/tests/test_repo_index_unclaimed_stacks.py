"""A stack we cannot handle must fail visibly, never confidently.

Measured against generators the detector had never been shown, before any of this existed:

    Gatsby      -> next-pages    2 routes   target src/pages/about.js   idiom "use next/head"
    Docusaurus  -> next-pages    1 route
    SvelteKit   -> static-html   1 route    (src/app.html, the site-wide shell)
    Remix/Eleventy/WordPress -> unknown, 0 routes

The last group is fine: no targets means the deep-fix answers "aucun fichier corrigeable" and
the customer is merely disappointed. **Gatsby was the dangerous one.** `src/pages/*` really is
its routing convention, so the route map came out CORRECT and the patcher was then handed the
Next.js idiom — it would have written `import Head from 'next/head'` into a Gatsby page. That
PR reads fine to a human reviewer and breaks the build.

The rule this file pins: a stack is either mapped AND given its own idiom, or it is named and
left with neither. Never routes without the matching idiom.
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
from backend import repo_index as ri  # noqa: E402

TREES: dict[str, list[str]] = {
    "gatsby": [
        "gatsby-config.js", "gatsby-node.js", "src/pages/index.js", "src/pages/about.js",
        "src/templates/post.js", "package.json",
    ],
    "sveltekit": [
        "svelte.config.js", "src/routes/+page.svelte", "src/routes/about/+page.svelte",
        "src/routes/+layout.svelte", "src/app.html", "package.json",
    ],
    "docusaurus": ["docusaurus.config.js", "docs/intro.md", "src/pages/index.js", "package.json"],
    "remix": ["remix.config.js", "app/routes/_index.tsx", "app/routes/about.tsx", "app/root.tsx", "package.json"],
    "eleventy": [".eleventy.js", "src/index.njk", "src/about.njk", "package.json"],
    "wordpress": ["wp-config.php", "wp-content/themes/t/header.php", "wp-content/themes/t/single.php", "index.php"],
}


@pytest.mark.parametrize("expected", sorted(TREES))
def test_each_generator_is_identified_as_itself(expected: str) -> None:
    assert ri.detect_stack(TREES[expected]) == expected


def test_gatsby_is_no_longer_told_to_write_next_js() -> None:
    """The bug this file exists for: right file, wrong framework, full confidence."""
    hint = ri.stack_idiom_hint(ri.build_repo_index(TREES["gatsby"]))
    assert "Gatsby" in hint
    assert "next/head" in hint and "JAMAIS" in hint, (
        "the Gatsby hint must forbid next/head explicitly — importing it breaks the build"
    )


def test_gatsby_still_maps_its_pages() -> None:
    # Naming the stack must not cost the mapping: `src/pages/*` IS Gatsby's routing convention,
    # and it was the only part that was already right.
    routes = ri.build_repo_index(TREES["gatsby"])["routes"]
    assert routes == {"/": ["src/pages/index.js"], "/about": ["src/pages/about.js"]}


def test_sveltekit_maps_page_files_and_not_its_shell() -> None:
    index = ri.build_repo_index(TREES["sveltekit"])
    routes = index["routes"]
    assert routes == {"/": ["src/routes/+page.svelte"], "/about": ["src/routes/about/+page.svelte"]}
    flat = [f for files in routes.values() for f in files]
    assert "src/app.html" not in flat, (
        "src/app.html is the document shell for the whole site; offering it as the file to patch "
        "for one page's title is how SvelteKit was read as static HTML"
    )
    assert "+layout.svelte" not in " ".join(flat), "a layout is not a page"


@pytest.mark.parametrize("stack", sorted(ri.UNSUPPORTED_STACKS))
def test_an_unsupported_stack_offers_nothing_rather_than_a_guess(stack: str) -> None:
    index = ri.build_repo_index(TREES[stack])
    assert index["stack"] == stack, "it must still be NAMED, so logs and support are truthful"
    assert index["routes"] == {}, f"{stack} produced a route map we cannot stand behind"
    assert ri.stack_idiom_hint(index) == "", (
        f"{stack} would tell the patcher to write in a framework idiom we have not verified"
    )


@pytest.mark.parametrize("name", sorted(TREES))
def test_no_stack_ever_gets_routes_without_its_own_idiom(name: str) -> None:
    """The invariant behind the whole file.

    Routes make the corrector confident enough to patch; the idiom decides what it writes. A
    stack with one and not the other is exactly the Gatsby failure in either direction.
    """
    index = ri.build_repo_index(TREES[name])
    has_routes = bool(index["routes"])
    has_idiom = bool(ri.stack_idiom_hint(index))
    assert has_routes == has_idiom, (
        f"{name}: routes={has_routes} idiom={has_idiom} — a stack must have both or neither"
    )


@pytest.mark.parametrize("name", ["docusaurus", "remix", "eleventy", "wordpress"])
def test_the_corrector_finds_no_target_on_an_unsupported_stack(name: str) -> None:
    # End of the chain: the customer gets "aucun fichier corrigeable trouvé", not a wrong PR.
    paths = TREES[name]
    targets = app_module._resolve_issue_targets(
        all_paths=paths, index=ri.build_repo_index(paths),
        issue_key="title_too_long_indexable", issue_label="Titre trop long",
        impacted_urls=["https://exemple.fr/about"], located=[], max_files=8,
        evidence=None, wants_page_targeting=True, ai_map=lambda: [], ai_pick=lambda: [],
    )
    assert targets == [], f"{name} handed the patcher {targets}"


def test_a_dotfile_at_the_repo_root_keeps_its_dot() -> None:
    """`lstrip("./")` strips a SET of characters, not a prefix.

    Every root dotfile was silently renamed before anything could match on it: `.htaccess` became
    `htaccess`, `.github/...` became `github/...`, and `.eleventy.js` became `eleventy.js` —
    which is why the Eleventy marker did not fire when it was first added.
    """
    assert ri._clean_paths([".eleventy.js"]) == [".eleventy.js"]
    assert ri._clean_paths([".htaccess"]) == [".htaccess"]
    assert ri._clean_paths([".github/workflows/ci.yml"]) == [".github/workflows/ci.yml"]
    # A genuine `./` prefix must still go.
    assert ri._clean_paths(["./src/index.js"]) == ["src/index.js"]


@pytest.mark.parametrize(
    "name, tree",
    [
        ("next-app", ["app/layout.tsx", "app/page.tsx", "app/about/page.tsx"]),
        ("next-pages", ["next.config.js", "package.json", "pages/index.tsx", "pages/about.tsx"]),
        ("astro", ["astro.config.mjs", "src/pages/index.astro", "src/pages/about.astro"]),
        ("nuxt", ["nuxt.config.ts", "pages/index.vue", "pages/about.vue"]),
        ("hugo", ["config.toml", "layouts/_default/single.html", "content/about.md"]),
        ("jekyll", ["_config.yml", "_layouts/default.html", "_posts/2026-01-01-x.md"]),
        ("static-html", ["index.html", "about.html"]),
    ],
)
def test_the_stacks_that_already_worked_still_do(name: str, tree: list[str]) -> None:
    # Six new markers were inserted into an ordered chain; the cheapest way for that to go wrong
    # is to shadow a stack that was already detected correctly.
    assert ri.detect_stack(tree) == name
