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

import re
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

# Stacks added 2026-08-29 after measuring what the detector did with generators it had never
# been shown. Gatsby was the dangerous one: `src/pages/*` is its routing convention too, so it
# was read as next-pages, the route map came out CORRECT, and the patcher was then told to write
# `next/head` — a module that does not exist in Gatsby. Right file, wrong idiom, full confidence:
# the PR reads fine and breaks the build. A stack we cannot map is a visible "no fixable file";
# a stack we map with the wrong idiom is a broken customer site.
STACK_GATSBY = "gatsby"
STACK_SVELTEKIT = "sveltekit"
STACK_REMIX = "remix"
STACK_ELEVENTY = "eleventy"
STACK_DOCUSAURUS = "docusaurus"
STACK_WORDPRESS = "wordpress"

# Identified on purpose, and deliberately NOT route-mapped. Each has a routing model we have not
# implemented (Remix's flat dotted filenames, Eleventy and Docusaurus permalink/slug front
# matter, WordPress routing that lives in a database rather than the repo). Naming them buys an
# honest refusal and a truthful log line instead of a confident wrong guess; supporting them is
# a separate piece of work, and this constant is what a future PR should shrink.
UNSUPPORTED_STACKS: frozenset[str] = frozenset(
    {STACK_REMIX, STACK_ELEVENTY, STACK_DOCUSAURUS, STACK_WORDPRESS}
)

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
    STACK_GATSBY: (
        "Stack: Gatsby. Les balises <head> se déclarent via l'export `Head` de la page "
        "(Gatsby Head API : `export const Head = () => (<><title>…</title><meta …/></>)`), "
        "ou via `<Helmet>` si le projet importe déjà react-helmet. N'importe JAMAIS `next/head` "
        "ni `next/…` : ces modules n'existent pas dans un projet Gatsby et le build casse."
    ),
    STACK_SVELTEKIT: (
        "Stack: SvelteKit. Les balises <head> se déclarent dans un bloc "
        "`<svelte:head>…</svelte:head>` du fichier `+page.svelte` de la route. "
        "`src/app.html` est le gabarit de TOUT le site : n'y mets jamais une valeur propre à "
        "une seule page."
    ),
    STACK_STATIC: (
        "Stack: HTML statique. Édite directement le <head> du fichier .html de la page."
    ),
    # UNSUPPORTED_STACKS deliberately get NO entry. An empty idiom makes the prompts fall back
    # to "or the framework's equivalent", i.e. the model guesses — and with no route map there
    # are no targets either, so the deep-fix refuses out loud instead of guessing quietly.
}


def is_noise_path(path: str) -> bool:
    """True for build output / vendored code. Matched SEGMENT by segment: a substring test
    would also drop legitimate sources — `out/` occurs inside `app/about/page.tsx`."""
    return any(seg.lower() in _NOISE_DIRS for seg in str(path or "").split("/")[:-1])


def _clean_paths(all_paths: list[str]) -> list[str]:
    out: list[str] = []
    for p in all_paths or []:
        s = str(p or "").strip()
        # `lstrip("./")` strips a SET of characters, not the prefix: it turned `.htaccess` into
        # `htaccess`, `.eleventy.js` into `eleventy.js` and `.github/...` into `github/...`, so
        # every dotfile at the repo root was renamed before anything could match on it. Same
        # shape as the `out/` substring bug: a loose string operation deciding something
        # structural.
        if s.startswith("./"):
            s = s[2:]
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
    # These must be checked BEFORE next-pages and static-html: a Gatsby or Docusaurus repo has
    # `src/pages/*`, and a SvelteKit repo ships `src/app.html`, so each was being mistaken for a
    # stack it is not. A config file at the root is the strongest signal a repo gives.
    if has("docusaurus.config.js", "docusaurus.config.ts", "docusaurus.config.mjs"):
        return STACK_DOCUSAURUS
    if has("gatsby-config.js", "gatsby-config.ts", "gatsby-config.mjs"):
        return STACK_GATSBY
    if has("svelte.config.js", "svelte.config.ts"):
        return STACK_SVELTEKIT
    if has("remix.config.js", "remix.config.ts", "remix.config.mjs"):
        return STACK_REMIX
    if has(".eleventy.js", ".eleventy.cjs", "eleventy.config.js", "eleventy.config.mjs"):
        return STACK_ELEVENTY
    if has("wp-config.php") or any(p.startswith("wp-content/") for p in paths):
        return STACK_WORDPRESS
    if any(_strip_src(p).startswith("pages/") for p in paths) and any(
        p.startswith("next.config.") or p == "package.json" for p in paths
    ):
        return STACK_NEXT_PAGES
    if any(p.startswith("layouts/") for p in paths) and (
        has("config.toml", "config.yaml", "hugo.toml", "hugo.yaml")
        # `hugo new site` splits config per environment; a repo using it was being read as a
        # pile of static HTML, which maps no routes at all.
        or any(p.startswith("config/") and p.rsplit("/", 1)[-1].startswith(("hugo.", "config."))
               for p in paths)
    ):
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


def _jekyll_routes(path: str) -> list[str]:
    """Jekyll has no `pages/` directory: a page is any Markdown/HTML file that is not under an
    underscore directory, and a post is `_posts/YYYY-MM-DD-title.ext`.

    Posts use Jekyll's DEFAULT permalink (`/:year/:month/:day/:title`). A site that overrides
    `permalink:` in _config.yml gets no route from us rather than a wrong one — we only see
    file paths, and pointing a fix at the wrong page is worse than declining to guess.
    """
    if _ext(path) not in _CONTENT_EXTS + ("html", "htm"):
        return []
    segments = path.split("/")
    base = segments[-1]
    stem = base.rsplit(".", 1)[0]

    if segments[0] == "_posts":
        m = re.match(r"^(\d{4})-(\d{2})-(\d{2})-(.+)$", stem)
        if not m:
            return []
        year, month, day, title = m.groups()
        return [_route_from_segments([year, month, day, title])]

    # Underscore directories are Jekyll internals (_layouts, _includes, _data, _drafts).
    if any(seg.startswith("_") for seg in segments[:-1]):
        return []
    parents = segments[:-1]
    if stem == "index":
        return [_route_from_segments(parents)]
    return [_route_from_segments(parents + [stem])]


_LANG_SUFFIX_RE = re.compile(r"^[a-z]{2}(-[a-z]{2})?$", re.IGNORECASE)


def _hugo_route(path: str) -> str | None:
    """`content/blog/post.md` → `/blog/post`; `content/blog/_index.md` → `/blog`.

    A language suffix (`a-propos.fr.md`) maps to `/fr/a-propos`, never to `/a-propos`. Whether
    Hugo serves the default language at the bare path depends on
    `defaultContentLanguageInSubdir`, which is in the config we cannot read — and if two
    languages both claimed `/a-propos`, a fix aimed at that URL could land on the wrong one.
    Being absent is recoverable; being wrong is not.
    """
    if not path.startswith("content/") or _ext(path) not in _CONTENT_EXTS:
        return None
    inner = path[len("content/"):]
    base = inner.rsplit("/", 1)[-1]
    stem = base.rsplit(".", 1)[0]
    lang = ""
    if "." in stem:
        head, tail = stem.rsplit(".", 1)
        if head and _LANG_SUFFIX_RE.match(tail):
            stem, lang = head, tail.lower()
    segments = inner.split("/")[:-1]
    if stem not in {"_index", "index"}:
        segments.append(stem)
    if lang:
        segments = [lang] + segments
    return _route_from_segments(segments)


# Directories holding the generator's TEMPLATES. Their .html files look exactly like static
# pages to `_static_routes`, which runs on every stack — so a Hugo repo mapped
# `/layouts/_default/single` to the template that renders the whole site, and Jekyll mapped
# `/_includes/header`. No crawled URL looks like that, but the corrector must never hold a
# mapping whose file rewrites every page.
_TEMPLATE_DIRS: dict[str, tuple[str, ...]] = {
    STACK_HUGO: ("layouts/", "themes/", "archetypes/"),
    STACK_JEKYLL: ("_layouts/", "_includes/", "_data/", "_drafts/", "_sass/"),
}

# Single files that are the document shell for the WHOLE site. The static-HTML mapping runs
# additively on every stack, so without this `src/app.html` becomes a route and SvelteKit gets
# offered its own shell as the file to patch for one page's title.
_SHELL_FILES: dict[str, tuple[str, ...]] = {
    STACK_SVELTEKIT: ("src/app.html",),
}


def _is_template_path(stack: str, path: str) -> bool:
    if path in _SHELL_FILES.get(stack, ()):
        return True
    prefixes = _TEMPLATE_DIRS.get(stack)
    if not prefixes:
        return False
    rel = _strip_src(path)
    return rel.startswith(prefixes) or any(f"/{d}" in f"/{rel}" for d in prefixes)


def _sveltekit_route(path: str) -> str | None:
    """`src/routes/about/+page.svelte` → `/about`; `src/routes/+page.svelte` → `/`.

    Only `+page.*` is a page. `+layout.*`, `+server.*` and `src/app.html` are not: `app.html` is
    the document shell for the WHOLE site, and mapping it to a route is how SvelteKit was read
    as static HTML and offered as the file to patch for one page's title.
    """
    rel = path
    if not rel.startswith("src/routes/"):
        return None
    base = rel.rsplit("/", 1)[-1]
    if not base.startswith("+page.") or _ext(rel) not in _ROUTE_EXTS:
        return None
    segments = rel[len("src/routes/"):].split("/")[:-1]
    # Route groups `(marketing)` are organisational and do not appear in the URL.
    segments = [s for s in segments if not (s.startswith("(") and s.endswith(")"))]
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
        if stack == STACK_GATSBY:
            # Gatsby's file-system routing IS `src/pages/*`, the same shape as Next Pages, so
            # the mapping was never the problem — only the idiom the patcher was handed.
            add(_flat_page_route(p, "pages"), p)
        if stack == STACK_SVELTEKIT:
            add(_sveltekit_route(p), p)
        if stack == STACK_HUGO:
            add(_hugo_route(p), p)
        if stack == STACK_JEKYLL:
            for r in _jekyll_routes(p):
                add(r, p)
        # Static HTML is additive on every stack: a Next.js export or a Hugo site can ship
        # hand-written pages under public/, and those ARE the file to patch. Template
        # directories are the exception — there the .html IS the site-wide template.
        if not _is_template_path(stack, p):
            for r in _static_routes(p):
                add(r, p)

    # Content collections (`content/**/*.mdx` feeding a dynamic route): the MDX file is the
    # real per-page source — patching the `[slug]` template instead would hardcode one value
    # onto every page of the route. Only mapped when a dynamic template covers the prefix.
    if stack in {STACK_NEXT_APP, STACK_NEXT_PAGES, STACK_ASTRO, STACK_NUXT}:
        prefixes = sorted({v for v in dynamic.values()}, key=len, reverse=True)
        for p in paths:
            # Astro's documented location is `src/content/`; Next and Nuxt use `content/`.
            rel = _strip_src(p)
            if not rel.startswith("content/") or _ext(rel) not in _CONTENT_EXTS:
                continue
            inner = rel[len("content/"):]
            stem = inner.rsplit("/", 1)[-1].rsplit(".", 1)[0]
            parents = inner.split("/")[:-1]
            candidate = _route_from_segments(parents + ([] if stem in {"index", "_index"} else [stem]))
            for prefix in prefixes:
                if prefix == "/" or candidate == prefix or candidate.startswith(prefix + "/"):
                    add(candidate, p)
                    break

    return {"stack": stack, "routes": routes, "dynamic": dynamic, "shared": sorted(set(shared))}



_DATE_JEKYLL_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})-(.+)$")
_DATE_ROUTE_RE = re.compile(r"^/\d{4}/\d{2}/\d{2}/.+$")


def _slug_de_route(route: str) -> str:
    """Le dernier segment d'une route : `/blog/mon-sujet` -> `mon-sujet`."""
    return norm_route(route).rsplit("/", 1)[-1]


def _section_de_route(route: str) -> str:
    """La route parente : `/blog/mon-sujet` -> `/blog`, `/contact` -> `/`."""
    parts = [p for p in norm_route(route).split("/") if p]
    return _route_from_segments(parts[:-1])


def _transposer(chemin_soeur: str, slug_soeur: str, slug_neuf: str, *, date: str = "") -> str:
    """Le chemin de la soeur, son slug remplace par le neuf. "" si le slug ne s'y lit pas.

    On REMPLACE dans le chemin existant au lieu de reconstruire depuis la stack. Une
    reconstruction demanderait de reimplementer, a l'envers, les sept conventions que ce module
    connait — et de les tenir a jour deux fois. La transposition les herite toutes, y compris
    les variantes qu'aucune regle generale ne capture : `src/content/` chez Astro contre
    `content/` chez Next, un theme qui range ses articles ailleurs, un suffixe de langue.

    Le slug est remplace sur sa DERNIERE occurrence : `content/blog/blog.md` ne doit pas voir
    son dossier renomme.
    """
    if not slug_soeur or slug_soeur not in chemin_soeur:
        return ""
    coupe = chemin_soeur.rfind(slug_soeur)
    neuf = chemin_soeur[:coupe] + slug_neuf + chemin_soeur[coupe + len(slug_soeur):]
    if date:
        # Jekyll date ses articles dans le NOM DE FICHIER, et la route en decoule. Transposer
        # tel quel donnerait a l'article neuf la date de son ainee — publie dans le passe, a la
        # mauvaise URL. On ne remplace que le prefixe du nom de fichier, jamais un dossier.
        dossier, _, base = neuf.rpartition("/")
        m = _DATE_JEKYLL_RE.match(base)
        if m:
            base = "%s-%s" % (date, m.group(4))
            neuf = (dossier + "/" + base) if dossier else base
    return neuf


def placement_pour_route(all_paths: list[str], route: str, *, date: str = "") -> dict[str, Any]:
    """Ou creer le fichier d'une page qui n'existe pas encore, et quoi lier.

    Rend `{fichier, section, soeurs, index, stack, refus}`. **`refus` non vide veut dire qu'on
    ne sait pas, et alors on ne devine pas** : creer une arborescence chez un client sur une
    supposition est pire que de lui dire qu'on ne sait pas ou poser sa page.

    LA METHODE : transposer le chemin d'une page SOEUR — une page existante de la meme
    section — plutot que de deduire un chemin de la stack. C'est la methode que ce projet
    applique deja partout ailleurs (`_inserer_og_complet` recopie les champs de la mise en
    page, `_complete_open_graph` clone la ligne que le modele vient d'ecrire) : **decrire une
    grammaire bat decrire une forme**. Elle a trois proprietes qu'une reconstruction n'a pas :

    * elle herite des sept conventions de ce module sans les reecrire a l'envers ;
    * elle suit les ecarts qu'aucune regle ne capture — un theme qui range ses articles
      ailleurs, un suffixe de langue, `src/content/` contre `content/` ;
    * **elle refuse toute seule quand il n'y a pas de soeur** — le tout premier article d'un
      blog, ou la convention n'existe simplement pas encore.

    `index` est le fichier de la page de SECTION (`/blog`) quand il existe. Savoir s'il faut le
    modifier demande de le LIRE — un gabarit Hugo qui parcourt le dossier n'a besoin de rien,
    une liste ecrite a la main si — et cette fonction ne lit aucun fichier. Elle desigen le
    candidat ; la decision revient a l'etape suivante.
    """
    chemins = _clean_paths(all_paths)
    index_repo = build_repo_index(chemins)
    routes = index_repo.get("routes") or {}
    cible = norm_route(route)
    slug = _slug_de_route(cible)
    vide: dict[str, Any] = {
        "fichier": "", "section": _section_de_route(cible), "soeurs": [],
        "soeur_slug": "", "index": "", "stack": index_repo.get("stack", ""), "refus": "",
    }

    if cible == "/" or not slug:
        return {**vide, "refus": "la racine n'est pas une page a creer"}
    if cible in routes:
        return {**vide, "refus": "cette page existe deja : %s" % ", ".join(routes[cible][:3])}

    section = _section_de_route(cible)
    # JEKYLL DATE SES PERMALIENS (`/2026/01/15/titre`), donc chaque article est SEUL dans sa
    # section et le modele par section ne trouve jamais de soeur. Ce n'est pas un defaut du
    # modele : c'est une convention ou la section n'est pas un dossier mais une date. Les
    # soeurs d'un article Jekyll sont les autres fichiers de `_posts/`, point. Une regle par
    # stack est justifiee ici — ce module en porte deja sept dans l'autre sens.
    if index_repo.get("stack") == STACK_JEKYLL and _DATE_ROUTE_RE.match(cible):
        posts = [f for f in chemins if f.startswith("_posts/") and _ext(f) in _CONTENT_EXTS + ("html", "htm")]
        if not posts:
            return {**vide, "refus": "aucun article sous _posts/ : la convention de ce site "
                                     "pour les articles est inconnue"}
        modele = sorted(posts)[0]
        stem = modele.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        m = _DATE_JEKYLL_RE.match(stem)
        slug_modele = m.group(4) if m else stem
        fichier = _transposer(modele, slug_modele, slug, date=date)
        if not fichier or fichier in set(chemins):
            return {**vide, "soeurs": sorted(posts)[:5],
                    "refus": "impossible de transposer %s" % modele}
        return {**vide, "fichier": fichier, "soeurs": sorted(posts)[:5],
                "soeur_slug": slug_modele,
                "index": (routes.get("/") or [""])[0], "refus": ""}

    # UN SEUL passage. Ma premiere version en faisait deux — collecter les soeurs, puis les
    # regrouper par forme — et le second refiltrait exactement comme le premier. Une mutation
    # qui desactivait le filtre du premier a SURVECU : il ne servait qu'a produire un refus.
    # Deux passages qui posent la meme question finissent par diverger.
    #
    # Les soeurs DIRECTES seulement : `/blog/a` est une soeur de `/blog/b`, pas `/blog/2026/a`.
    # Une petite-fille porte une convention differente.
    #
    # On regroupe par FORME (dossier + extension) : un site qui a dix pages regulieres et une
    # page bricolee doit suivre les dix.
    soeurs: list[str] = []
    formes: dict[str, list[tuple[str, str]]] = {}
    for r, fichiers in routes.items():
        if r == section or _section_de_route(r) != section:
            continue
        s = _slug_de_route(r)
        for f in fichiers:
            if f not in soeurs:
                soeurs.append(f)
            if s and s in f:
                # LA FORME, C'EST LE CHEMIN DONT ON A RETIRE LE SLUG, pas le dossier qui le
                # contient. Chez Next App Router le slug EST un dossier
                # (`app/blog/<slug>/page.tsx`) : grouper par dossier rangeait chaque soeur dans
                # son propre groupe, la majorite ne departageait donc plus rien, et le modele
                # retenu etait simplement celui dont le dossier se classait en dernier. Une page
                # bricolee bien placee dans l'alphabet gagnait contre dix pages regulieres.
                formes.setdefault(_transposer(f, s, "{slug}"), []).append((f, s))

    if not soeurs:
        return {**vide, "refus": "aucune page soeur sous %s : la convention de ce site pour "
                                 "cette section est inconnue" % section}
    if not formes:
        return {**vide, "soeurs": soeurs[:5],
                "refus": "les pages de %s ne portent pas leur slug dans leur chemin : "
                         "transposition impossible" % section}

    forme_majoritaire = max(formes, key=lambda k: (len(formes[k]), k))
    modele, slug_modele = sorted(formes[forme_majoritaire])[0]
    fichier = _transposer(modele, slug_modele, slug, date=date)
    if not fichier:
        return {**vide, "soeurs": soeurs[:5],
                "refus": "impossible de transposer %s" % modele}
    if fichier in set(chemins):
        return {**vide, "soeurs": soeurs[:5],
                "refus": "le fichier %s existe deja sans servir cette route" % fichier}

    index_fichiers = routes.get(section) or []
    return {
        "fichier": fichier,
        "section": section,
        "soeurs": [f for f, _s in sorted(formes[forme_majoritaire])][:5],
        # Le slug de la soeur MODELE, celui qu'on vient de transposer. Sans lui, l'appelant qui
        # veut cloner l'entree de liste de cette soeur devrait le rededuire de son chemin — donc
        # reimplementer, mal, la regle Jekyll des noms dates juste au-dessus.
        "soeur_slug": slug_modele,
        "index": index_fichiers[0] if index_fichiers else "",
        "stack": index_repo.get("stack", ""),
        "refus": "",
    }


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
