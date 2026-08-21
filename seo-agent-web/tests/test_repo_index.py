from __future__ import annotations

import sys
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

from backend import repo_index as ri  # noqa: E402


# A Next.js App Router site exported statically (`output: 'export'`) that ALSO ships
# hand-written flat HTML under public/ and drives part of its routes from an MDX content
# collection. This is the hardest real-world shape the corrector meets.
AVIS_INVEST_TREE = [
    "package.json",
    "next.config.js",
    "netlify.toml",
    "public/_redirects",
    "app/layout.tsx",
    "app/page.tsx",
    "app/sitemap.ts",
    "app/robots.ts",
    "app/(marketing)/about/page.tsx",
    "app/checkout/page.tsx",
    "app/en/page.tsx",
    "app/en/[slug]/page.tsx",
    "app/es/[slug]/page.tsx",
    "components/LanguageSelect.tsx",
    "lib/seo.ts",
    "content/en/avis-bitpanda.mdx",
    "content/en/guide-etoro.mdx",
    "content/es/opinion-bitpanda.mdx",
    "public/sources/etoro-en.html",
    "public/help_en.html",
    "out/en/index.html",
    "node_modules/react/index.js",
    ".next/server/app/page.js",
]


def test_norm_route_normalises_urls_to_a_comparable_path() -> None:
    assert ri.norm_route("https://example.com/x/") == "/x"
    assert ri.norm_route("https://example.com/x?a=1#b") == "/x"
    assert ri.norm_route("x") == "/x"
    assert ri.norm_route("https://example.com/") == "/"
    assert ri.norm_route("") == "/"


def test_detect_stack_recognises_next_app_router_despite_static_html() -> None:
    assert ri.detect_stack(AVIS_INVEST_TREE) == ri.STACK_NEXT_APP


def test_detect_stack_covers_the_other_generators() -> None:
    assert ri.detect_stack(["astro.config.mjs", "src/pages/index.astro"]) == ri.STACK_ASTRO
    assert ri.detect_stack(["nuxt.config.ts", "pages/index.vue"]) == ri.STACK_NUXT
    assert ri.detect_stack(["package.json", "next.config.js", "pages/index.tsx"]) == ri.STACK_NEXT_PAGES
    assert ri.detect_stack(["config.toml", "layouts/index.html", "content/a.md"]) == ri.STACK_HUGO
    assert ri.detect_stack(["_config.yml", "_layouts/default.html"]) == ri.STACK_JEKYLL
    assert ri.detect_stack(["index.html", "about.html"]) == ri.STACK_STATIC
    assert ri.detect_stack(["main.py"]) == ri.STACK_UNKNOWN


def test_build_index_maps_app_router_routes_and_ignores_build_output() -> None:
    index = ri.build_repo_index(AVIS_INVEST_TREE)
    routes = index["routes"]

    assert routes["/"] == ["app/page.tsx"]
    assert routes["/en"] == ["app/en/page.tsx"]
    # Route groups are transparent in the URL.
    assert routes["/about"] == ["app/(marketing)/about/page.tsx"]
    # A directory whose name merely CONTAINS a build-output name stays in the map.
    assert routes["/checkout"] == ["app/checkout/page.tsx"]
    # Flat HTML shipped alongside the app router is a real, patchable page.
    assert routes["/sources/etoro-en"] == ["public/sources/etoro-en.html"]
    assert routes["/help_en"] == ["public/help_en.html"]
    # Build output and vendored code never enter the map.
    mapped = {p for hits in routes.values() for p in hits}
    assert not any(set(p.split("/")[:-1]) & {"out", "node_modules", ".next"} for p in mapped)


def test_content_collection_files_are_mapped_through_their_dynamic_route() -> None:
    index = ri.build_repo_index(AVIS_INVEST_TREE)

    assert index["dynamic"]["app/en/[slug]/page.tsx"] == "/en"
    assert index["dynamic"]["app/es/[slug]/page.tsx"] == "/es"
    # The MDX file — not the [slug] template — is the per-page source of the article.
    assert ri.route_files(index, "https://avis-invest.com/en/avis-bitpanda") == ["content/en/avis-bitpanda.mdx"]
    assert ri.route_files(index, "/es/opinion-bitpanda") == ["content/es/opinion-bitpanda.mdx"]


def test_route_files_never_returns_a_shared_template() -> None:
    index = ri.build_repo_index(AVIS_INVEST_TREE)
    for url in ("/", "/en", "/en/avis-bitpanda", "/sources/etoro-en"):
        for path in ri.route_files(index, url):
            assert not ri.is_shared_path(index, path), f"{url} → {path} is shared"


def test_unknown_urls_return_no_opinion_so_the_caller_can_fall_back() -> None:
    index = ri.build_repo_index(AVIS_INVEST_TREE)
    assert ri.route_files(index, "/nowhere/at/all") == []


def test_shared_paths_cover_layouts_and_dynamic_templates() -> None:
    index = ri.build_repo_index(AVIS_INVEST_TREE)

    assert ri.is_shared_path(index, "app/layout.tsx")
    assert ri.is_shared_path(index, "app/en/[slug]/page.tsx")
    assert ri.is_shared_path(index, "pages/_document.tsx")
    assert ri.is_shared_path(index, "base.html")
    assert not ri.is_shared_path(index, "app/en/page.tsx")
    assert not ri.is_shared_path(index, "content/en/avis-bitpanda.mdx")
    assert not ri.is_shared_path(index, "public/sources/etoro-en.html")


def test_pages_router_and_astro_use_filename_routing() -> None:
    next_pages = ri.build_repo_index([
        "package.json", "next.config.js",
        "pages/_app.tsx", "pages/_document.tsx", "pages/index.tsx",
        "pages/blog/index.tsx", "pages/blog/hello.tsx", "pages/blog/[id].tsx",
        "pages/api/ping.ts",
    ])
    assert next_pages["routes"]["/"] == ["pages/index.tsx"]
    assert next_pages["routes"]["/blog"] == ["pages/blog/index.tsx"]
    assert next_pages["routes"]["/blog/hello"] == ["pages/blog/hello.tsx"]
    assert "/api/ping" not in next_pages["routes"]
    assert next_pages["dynamic"]["pages/blog/[id].tsx"] == "/blog"

    astro = ri.build_repo_index(["astro.config.mjs", "src/pages/index.astro", "src/pages/tarifs.astro"])
    assert astro["routes"]["/tarifs"] == ["src/pages/tarifs.astro"]


def test_hugo_content_routing_handles_section_indexes() -> None:
    index = ri.build_repo_index([
        "config.toml", "layouts/_default/baseof.html",
        "content/_index.md", "content/blog/_index.md", "content/blog/post-1.md",
    ])
    assert index["routes"]["/"] == ["content/_index.md"]
    assert index["routes"]["/blog"] == ["content/blog/_index.md"]
    assert index["routes"]["/blog/post-1"] == ["content/blog/post-1.md"]


def test_static_site_maps_directory_indexes_and_flat_files() -> None:
    index = ri.build_repo_index(["index.html", "about.html", "guide/index.html"])
    assert index["routes"]["/"] == ["index.html"]
    assert index["routes"]["/about"] == ["about.html"]
    assert index["routes"]["/guide"] == ["guide/index.html"]


def test_stack_idiom_hint_is_specific_per_framework() -> None:
    app_router = ri.build_repo_index(AVIS_INVEST_TREE)
    assert "App Router" in ri.stack_idiom_hint(app_router)
    assert "metadata" in ri.stack_idiom_hint(app_router)

    static = ri.build_repo_index(["index.html"])
    assert "HTML statique" in ri.stack_idiom_hint(static)
    assert ri.stack_idiom_hint({"stack": ri.STACK_UNKNOWN}) == ""
