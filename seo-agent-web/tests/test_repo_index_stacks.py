"""Route mapping for the stacks the corrector claimed to support but had never met.

The corrector's whole pitch is that it edits the right source file. `repo_index` was validated
against 11 real repositories, but all of them were next-app or static-html; Astro, Nuxt, Hugo,
Jekyll and next-pages were declared supported on the strength of the code reading plausibly.

Measured before any fix, on the fixtures below: Hugo 5/5 and next-pages 4/4 were genuinely
fine, Astro resolved 3/5, Nuxt 3/4, and **Jekyll 1/4** — build_repo_index had no Jekyll branch
at all, so the one hit was a plain .html file caught by the static-HTML fallback. A customer on
Jekyll would have received fixes aimed at nothing.

Each fixture is a realistic minimal tree for its generator, including the parts that bite:
content collections, page bundles, language suffixes, framework-private files.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

from backend import repo_index as ri  # noqa: E402

ASTRO = [
    "package.json", "astro.config.mjs",
    "src/pages/index.astro", "src/pages/a-propos.astro",
    "src/pages/blog/index.astro", "src/pages/blog/[slug].astro",
    "src/content/blog/premier-article.md", "src/content/blog/second-article.mdx",
    "src/content/config.ts", "src/layouts/Base.astro", "src/components/Header.astro",
    "public/favicon.svg", "public/robots.txt",
]
NUXT = [
    "package.json", "nuxt.config.ts", "app.vue",
    "pages/index.vue", "pages/a-propos.vue", "pages/blog/index.vue", "pages/blog/[slug].vue",
    "content/blog/premier-article.md", "layouts/default.vue", "components/TheHeader.vue",
]
HUGO = [
    "hugo.toml", "content/_index.md", "content/a-propos.md", "content/blog/_index.md",
    "content/blog/premier-article.md", "content/blog/page-bundle/index.md",
    "layouts/_default/baseof.html", "layouts/_default/single.html",
    "layouts/partials/header.html", "static/robots.txt",
]
HUGO_I18N = [
    "config/_default/hugo.toml", "content/a-propos.fr.md", "content/a-propos.en.md",
    "layouts/_default/baseof.html",
]
JEKYLL = [
    "_config.yml", "index.md", "a-propos.md",
    "_posts/2026-08-26-premier-article.md", "_posts/2026-08-20-second-article.markdown",
    "_layouts/default.html", "_layouts/post.html", "_includes/header.html",
    "blog/index.html", "assets/style.css",
]
NEXT_PAGES = [
    "package.json", "next.config.js",
    "pages/index.tsx", "pages/a-propos.tsx", "pages/blog/index.tsx", "pages/blog/[slug].tsx",
    "pages/_app.tsx", "pages/api/hello.ts",
    "content/blog/premier-article.mdx", "components/Header.tsx",
]


@pytest.mark.parametrize(
    "paths,expected_stack",
    [
        (ASTRO, ri.STACK_ASTRO),
        (NUXT, ri.STACK_NUXT),
        (HUGO, ri.STACK_HUGO),
        # `hugo new site` splits the config per environment; this used to read as static HTML.
        (HUGO_I18N, ri.STACK_HUGO),
        (JEKYLL, ri.STACK_JEKYLL),
        (NEXT_PAGES, ri.STACK_NEXT_PAGES),
    ],
)
def test_the_generator_is_recognised(paths: list[str], expected_stack: str) -> None:
    assert ri.detect_stack(paths) == expected_stack


@pytest.mark.parametrize(
    "paths,url,expected",
    [
        (ASTRO, "/", "src/pages/index.astro"),
        (ASTRO, "/a-propos", "src/pages/a-propos.astro"),
        (ASTRO, "/blog", "src/pages/blog/index.astro"),
        # Astro documents src/content/; only content/ was being looked at.
        (ASTRO, "/blog/premier-article", "src/content/blog/premier-article.md"),
        (ASTRO, "/blog/second-article", "src/content/blog/second-article.mdx"),
        (NUXT, "/", "pages/index.vue"),
        (NUXT, "/blog", "pages/blog/index.vue"),
        # Nuxt Content was simply left out of the content-collection branch.
        (NUXT, "/blog/premier-article", "content/blog/premier-article.md"),
        (HUGO, "/", "content/_index.md"),
        (HUGO, "/blog", "content/blog/_index.md"),
        (HUGO, "/blog/premier-article", "content/blog/premier-article.md"),
        (HUGO, "/blog/page-bundle", "content/blog/page-bundle/index.md"),
        (HUGO_I18N, "/fr/a-propos", "content/a-propos.fr.md"),
        (HUGO_I18N, "/en/a-propos", "content/a-propos.en.md"),
        (JEKYLL, "/", "index.md"),
        (JEKYLL, "/a-propos", "a-propos.md"),
        (JEKYLL, "/blog", "blog/index.html"),
        # Jekyll's default permalink is date-based.
        (JEKYLL, "/2026/08/26/premier-article", "_posts/2026-08-26-premier-article.md"),
        (NEXT_PAGES, "/", "pages/index.tsx"),
        (NEXT_PAGES, "/blog/premier-article", "content/blog/premier-article.mdx"),
    ],
)
def test_a_url_resolves_to_the_file_that_actually_renders_it(
    paths: list[str], url: str, expected: str
) -> None:
    index = ri.build_repo_index(paths)
    assert expected in ri.route_files(index, url), (
        f"{url} resolved to {ri.route_files(index, url) or '(nothing)'}"
    )


@pytest.mark.parametrize(
    "paths,url",
    [
        (ASTRO, "/blog/[slug]"),          # a template renders MANY pages
        (NEXT_PAGES, "/api/hello"),       # not a page
        (NEXT_PAGES, "/_app"),            # framework-private
        (JEKYLL, "/_layouts/default"),    # Jekyll internals
        (JEKYLL, "/_includes/header"),
        (HUGO, "/layouts/_default/single"),
        # A language variant must never claim the bare path: two languages would compete for it
        # and a fix could land on the wrong one.
        (HUGO_I18N, "/a-propos"),
    ],
)
def test_a_url_that_has_no_single_source_file_resolves_to_nothing(
    paths: list[str], url: str
) -> None:
    index = ri.build_repo_index(paths)
    assert ri.route_files(index, url) == []


@pytest.mark.parametrize("paths", [ASTRO, NUXT, HUGO, JEKYLL, NEXT_PAGES])
def test_no_route_ever_points_at_a_shared_template(paths: list[str]) -> None:
    # Editing a layout to fix one page's title would rewrite every page on the site.
    index = ri.build_repo_index(paths)
    for url in index["routes"]:
        for path in ri.route_files(index, url):
            assert not ri.is_shared_path(index, path), f"{url} -> shared file {path}"


def test_a_jekyll_post_with_no_date_prefix_is_not_guessed_at() -> None:
    # Jekyll requires the date prefix; a file without one is a draft or a mistake, and inventing
    # a permalink for it would aim a fix at a page that does not exist.
    index = ri.build_repo_index(["_config.yml", "_layouts/default.html", "_posts/sans-date.md"])
    assert index["routes"] == {}


def test_a_custom_jekyll_permalink_yields_no_route_rather_than_a_wrong_one() -> None:
    # _config.yml can set `permalink: /blog/:title`, which we cannot read. The date-based
    # default is registered; the custom URL simply misses and the caller falls back.
    index = ri.build_repo_index(JEKYLL)
    assert ri.route_files(index, "/blog/premier-article") == []
    assert ri.route_files(index, "/2026/08/26/premier-article") == ["_posts/2026-08-26-premier-article.md"]
