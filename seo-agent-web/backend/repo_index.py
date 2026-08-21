"""Deterministic index of a connected git repository.

The corrector needs two different things to fix an issue:

  * WHAT to fix, and with which value  → that comes from the CRAWL (the issue evidence).
    Only a crawl sees the deployed, rendered site: build output, CDN/redirect rules,
    middleware, CMS content. The repo may be a different branch, a monorepo, or simply
    not contain the content at all.
  * WHERE to fix it, and in which framework idiom → that is what this module answers.

So: the crawl stays the source of truth, the repo is only a map. Nothing here decides
whether a page has an issue; it only maps a URL to the source file(s) behind it and
tells a shared template apart from a per-page file.

Everything is derived from the repo file tree alone (the recursive `git/trees` listing
the corrector already fetches), so building an index costs zero extra API calls. When a
route cannot be resolved the map simply has no entry and the caller falls back to its
previous AI-based mapping — this module is only ever an accelerator, never a gate.
"""

from __future__ import annotations

from typing import Any

# Build output / vendored code: never a source file to patch. Compared SEGMENT by segment,
# never as a substring — `out/` as a substring would also drop `app/about/page.tsx`.
_NOISE_DIRS = frozenset({
    "node_modules", "dist", "build", ".next", ".nuxt", "vendor", ".git",
    "coverage", ".cache", "out", "_site", ".vercel", ".netlify", "target",
})

# Extensions that can define a page/route in the supported stacks.
_ROUTE_EXTS = ("tsx", "jsx", "ts", "js", "mjs", "astro", "vue", "svelte", "mdx", "md", "html", "htm", "php")
_CONTENT_EXTS = ("mdx", "md", "markdown")

STACK_NEXT_APP = "next-app"
STACK_NEXT_PAGES = "next-pages"
STACK_ASTRO = "astro"
STACK_NUXT = "nuxt"
STACK_HUGO = "hugo"
STACK_JEKYLL = "jekyll"
STACK_STATIC = "static-html"
STACK_UNKNOWN = "unknown"

# Per-stack instruction telling the patcher which idiom to write head tags in. Without
# it the prompts say "or the framework's equivalent", i.e. the model guesses — which is
# how a Next.js `metadata` export once got replaced by a raw <head> block.
_STACK_IDIOMS: dict[str, str] = {
    STACK_NEXT_APP: (
        "Stack: Next.js App Router. Les balises <head> se déclarent via l'export `metadata` "
        "(ou `generateMetadata`) et `export const viewport` dans le fichier de route — "
        "n'écris JAMAIS de balise <head>/<meta> en JSX dans une page."
    ),
    STACK_NEXT_PAGES: (
        "Stack: Next.js Pages Router. Les balises <head> se déclarent avec le composant "
        "`<Head>` de `next/head` (ou dans `pages/_document.tsx` pour le global)."
    ),
    STACK_ASTRO: (
        "Stack: Astro. Les balises <head> s'écrivent directement en HTML dans le layout "
        "`.astro` ou la page `.astro`, entre les délimiteurs du template."
    ),
    STACK_NUXT: (
        "Stack: Nuxt. Les balises <head> se déclarent via `useHead()`/`definePageMeta` "
        "(Nuxt 3) ou la clé `head` du composant, jamais en HTML brut."
    ),
    STACK_HUGO: (
        "Stack: Hugo. Le <head> vit dans les templates `layouts/` (partials) ; les valeurs "
        "par page viennent du front matter des fichiers `content/`."
    ),
    STACK_JEKYLL: (
        "Stack: Jekyll. Le <head> vit dans `_includes`/`_layouts` ; les valeurs par page "
        "viennent du front matter."
    ),
    STACK_STATIC: (
        "Stack: HTML statique. Édite directement le <head> du fichier .html de la page."
    ),
}


def is_noise_path(path: str) -> bool:
    """True for build output / vendored code. Matched SEGMENT by segment: a substring test
    would also drop legitimate sources — `out/` occurs inside `app/about/page.tsx`."""
    return any(seg.lower() in _NOISE_DIRS for seg in str(path or "").split("/")[:-1])


def _clean_paths(all_paths: list[str]) -> list[str]:
    out: list[str] = []
    for p in all_paths or []:
        s = str(p or "").strip().lstrip("./")
        if not s or s.startswith("/") or is_noise_path(s):
            continue
        out.append(s)
    return out


def _ext(path: str) -> str:
    base = path.rsplit("/", 1)[-1]
    return base.rsplit(".", 1)[-1].lower() if "." in base else ""


def _strip_src(path: str) -> str:
    return path[4:] if path.startswith("src/") else path


def norm_route(route: str) -> str:
    """Normalise a URL path for map lookups: no query/fragment, no trailing slash, always
    leading slash. `/x/`, `x`, `https://h/x?a=1#b` all become `/x`; the root stays `/`."""
    s = str(route or "").strip()
    for sep in ("#", "?"):
        if sep in s:
            s = s.split(sep, 1)[0]
    if "://" in s:
        rest = s.split("://", 1)[1]
        slash = rest.find("/")
        s = rest[slash:] if slash >= 0 else "/"
    if not s.startswith("/"):
        s = "/" + s
    s = s.rstrip("/")
    return s or "/"


def _route_from_segments(segments: list[str]) -> str:
    parts = [s for s in segments if s]
    return "/" + "/".join(parts) if parts else "/"


def detect_stack(all_paths: list[str]) -> str:
    """Identify the site generator from the file tree. Order matters: a Next.js repo can
    contain `public/*.html` too, so framework markers are checked before static HTML."""
    paths = set(_clean_paths(all_paths))
    has = lambda *names: any(n in paths for n in names)  # noqa: E731

    if any(p.startswith(("app/", "src/app/")) and p.rsplit("/", 1)[-1].startswith("layout.") for p in paths):
        return STACK_NEXT_APP
    if any(_strip_src(p).startswith("app/") and _strip_src(p).rsplit("/", 1)[-1].startswith("page.") for p in paths):
        return STACK_NEXT_APP
    if has("astro.config.mjs", "astro.config.ts", "astro.config.js"):
        return STACK_ASTRO
    if has("nuxt.config.ts", "nuxt.config.js"):
        return STACK_NUXT
    if any(_strip_src(p).startswith("pages/") for p in paths) and any(
        p.startswith("next.config.") or p == "package.json" for p in paths
    ):
        return STACK_NEXT_PAGES
    if has("config.toml", "config.yaml", "hugo.toml", "hugo.yaml") and any(p.startswith("layouts/") for p in paths):
        return STACK_HUGO
    if has("_config.yml") and any(p.startswith(("_layouts/", "_posts/")) for p in paths):
        return STACK_JEKYLL
    if any(_ext(p) in {"html", "htm"} for p in paths):
        return STACK_STATIC
    return STACK_UNKNOWN


def _next_app_route(path: str) -> str | None:
    """`app/sources/etoro/page.tsx` → `/sources/etoro`. Route groups `(marketing)` and
    parallel/intercepting segments are transparent. Returns None if not an app-router page."""
    rel = _strip_src(path)
    if not rel.startswith("app/"):
        return None
    base = rel.rsplit("/", 1)[-1]
    if not base.startswith("page.") or _ext(rel) not in _ROUTE_EXTS:
        return None
    segments = rel.split("/")[1:-1]
    keep = [s for s in segments if not (s.startswith("(") and s.endswith(")")) and not s.startswith("@")]
    return _route_from_segments(keep)


def _flat_page_route(path: str, root: str) -> str | None:
    """Filename-based routing (Next Pages Router, Astro, Nuxt): `pages/a/b.tsx` → `/a/b`,
    `pages/a/index.tsx` → `/a`. Framework-private files are skipped."""
    rel = _strip_src(path)
    prefix = root.rstrip("/") + "/"
    if not rel.startswith(prefix):
        return None
    if _ext(rel) not in _ROUTE_EXTS:
        return None
    inner = rel[len(prefix):]
    base = inner.rsplit("/", 1)[-1]
    stem = base.rsplit(".", 1)[0]
    if stem.startswith("_") or inner.startswith("api/") or stem in {"middleware", "404", "500"}:
        return None
    segments = inner.split("/")[:-1]
    if stem != "index":
        segments.append(stem)
    return _route_from_segments(segments)


def _hugo_route(path: str) -> str | None:
    """`content/blog/post.md` → `/blog/post`; `content/blog/_index.md` → `/blog`."""
    if not path.startswith("content/") or _ext(path) not in _CONTENT_EXTS:
        return None
    inner = path[len("content/"):]
    base = inner.rsplit("/", 1)[-1]
    stem = base.rsplit(".", 1)[0]
    segments = inner.split("/")[:-1]
    if stem not in {"_index", "index"}:
        segments.append(stem)
    return _route_from_segments(segments)


def _static_routes(path: str) -> list[str]:
    """`public/a.html` → `/a`; `public/a/index.html` → `/a`; `a.html` at the root → `/a`.
    A served-from-root directory prefix (`public/`, `static/`, `docs/`) is transparent."""
    if _ext(path) not in {"html", "htm"}:
        return []
    rel = path
    for prefix in ("public/", "static/", "docs/", "www/"):
        if rel.startswith(prefix):
            rel = rel[len(prefix):]
            break
    segments = rel.split("/")
    stem = segments[-1].rsplit(".", 1)[0]
    parents = segments[:-1]
    if stem == "index":
        return [_route_from_segments(parents)]
    return [_route_from_segments(parents + [stem])]


def _is_dynamic(path: str) -> bool:
    """A file whose route contains a parameter segment (`[slug]`, `[...all]`, `_id.vue`,
    `:id`) — it renders MANY pages, so it is a shared template, never a per-page source."""
    if "[" in path or "]" in path or ":" in path:
        return True
    base = path.rsplit("/", 1)[-1]
    stem = base.rsplit(".", 1)[0]
    return stem.startswith("_") and stem not in {"_index", "_app", "_document"}


def _shared_basename(path: str) -> bool:
    base = (path or "").rsplit("/", 1)[-1].lower()
    return base.startswith(("layout.", "_document", "_app", "template.")) or base in {
        "base.html", "_layout.html", "default.html", "head.html", "baseof.html",
    }


def _dynamic_prefix(template_path: str) -> str | None:
    """Static prefix of a dynamic route template: `app/en/[slug]/page.tsx` → `/en`,
    `app/[slug]/page.tsx` → `/`. Used to attach content-collection files to their route."""
    rel = _strip_src(template_path)
    for root in ("app/", "pages/"):
        if rel.startswith(root):
            rel = rel[len(root):]
            break
    else:
        return None
    segments = rel.split("/")[:-1] if rel.rsplit("/", 1)[-1].startswith(("page.", "index.")) else rel.split("/")
    keep: list[str] = []
    for s in segments:
        if "[" in s or ":" in s:
            break
        if (s.startswith("(") and s.endswith(")")) or s.startswith("@"):
            continue
        keep.append(s)
    return _route_from_segments(keep)


def build_repo_index(all_paths: list[str]) -> dict[str, Any]:
    """Build the URL→source-file map for a repo tree.

    Returns {stack, routes, dynamic, shared}: `routes` maps a normalised URL path to the
    per-page source files behind it (most specific first); `dynamic` maps each dynamic
    route template to its static prefix; `shared` lists layout/template files. A URL that
    cannot be resolved is simply absent — callers must treat a miss as "no opinion"."""
    paths = _clean_paths(all_paths)
    stack = detect_stack(paths)
    routes: dict[str, list[str]] = {}
    dynamic: dict[str, str] = {}
    shared: list[str] = []

    def add(route: str | None, path: str) -> None:
        if not route:
            return
        key = norm_route(route)
        bucket = routes.setdefault(key, [])
        if path not in bucket:
            bucket.append(path)

    for p in paths:
        if _shared_basename(p):
            shared.append(p)
            continue
        if _is_dynamic(p):
            if _ext(p) in _ROUTE_EXTS:
                prefix = _dynamic_prefix(p)
                if prefix is not None:
                    dynamic[p] = prefix
                shared.append(p)
            continue
        if stack == STACK_NEXT_APP:
            add(_next_app_route(p), p)
        if stack in {STACK_NEXT_APP, STACK_NEXT_PAGES}:
            add(_flat_page_route(p, "pages"), p)
        if stack == STACK_ASTRO:
            add(_flat_page_route(p, "pages"), p)
        if stack == STACK_NUXT:
            add(_flat_page_route(p, "pages"), p)
        if stack == STACK_HUGO:
            add(_hugo_route(p), p)
        # Static HTML is additive on every stack: a Next.js export or a Hugo site can ship
        # hand-written pages under public/, and those ARE the file to patch.
        for r in _static_routes(p):
            add(r, p)

    # Content collections (`content/**/*.mdx` feeding a dynamic route): the MDX file is the
    # real per-page source — patching the `[slug]` template instead would hardcode one value
    # onto every page of the route. Only mapped when a dynamic template covers the prefix.
    if stack in {STACK_NEXT_APP, STACK_NEXT_PAGES, STACK_ASTRO}:
        prefixes = sorted({v for v in dynamic.values()}, key=len, reverse=True)
        for p in paths:
            if not p.startswith("content/") or _ext(p) not in _CONTENT_EXTS:
                continue
            inner = p[len("content/"):]
            stem = inner.rsplit("/", 1)[-1].rsplit(".", 1)[0]
            parents = inner.split("/")[:-1]
            candidate = _route_from_segments(parents + ([] if stem in {"index", "_index"} else [stem]))
            for prefix in prefixes:
                if prefix == "/" or candidate == prefix or candidate.startswith(prefix + "/"):
                    add(candidate, p)
                    break

    return {"stack": stack, "routes": routes, "dynamic": dynamic, "shared": sorted(set(shared))}


def route_files(index: dict[str, Any], url: str, *, limit: int = 4) -> list[str]:
    """Per-page source file(s) for one URL. Empty when the URL is not in the map — the
    caller must then fall back to its own resolution. Never returns a shared template."""
    if not isinstance(index, dict):
        return []
    routes = index.get("routes") or {}
    key = norm_route(url)
    hits = routes.get(key) or []
    if not hits and key != "/":
        # A crawled URL may carry an extension the map stored without it (or vice versa).
        stem = key.rsplit(".", 1)[0] if "." in key.rsplit("/", 1)[-1] else key
        hits = routes.get(stem) or []
    return [p for p in hits if not is_shared_path(index, p)][:limit]


def is_shared_path(index: dict[str, Any], path: str) -> bool:
    """True when editing this file changes MANY pages: a layout/template by name, or a
    dynamic route file that renders a whole family of URLs. Per-page issues must never
    target one — that is what turned an Open Graph fix into a site-wide regression."""
    if not path:
        return False
    if _shared_basename(path) or _is_dynamic(path):
        return True
    if isinstance(index, dict) and path in (index.get("dynamic") or {}):
        return True
    return False


def stack_idiom_hint(index: dict[str, Any]) -> str:
    """One line telling the patcher which framework idiom to write head tags in."""
    stack = (index or {}).get("stack") if isinstance(index, dict) else None
    return _STACK_IDIOMS.get(str(stack or ""), "")


def index_summary(index: dict[str, Any]) -> str:
    """Compact description for logs and the corrections UI."""
    if not isinstance(index, dict):
        return "repo-index: n/a"
    return (
        f"repo-index: stack={index.get('stack')} routes={len(index.get('routes') or {})} "
        f"dynamic={len(index.get('dynamic') or {})} shared={len(index.get('shared') or [])}"
    )
