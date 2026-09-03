"""Markdown-backed public content: the product documentation and the SEO guides.

Both collections were dictionaries in Python before. That worked at six articles and stopped
working at twenty: a 600-line literal is not a thing anyone proof-reads, and every typo in it
was a deploy. Here a page is a file — YAML front matter, then Markdown — so writing one is
writing prose, and a broken one is caught at import rather than at 3 a.m. in a template.

Two collections, deliberately separate:

* ``docs``   — how {{app_name}} itself works, one page per screen. Ordered, sectioned, and
               linked from inside the app. Not marketing.
* ``blog``   — SEO guides for people who have not signed up yet. Ordered by date, indexed,
               and the only one of the two that carries a sales CTA.

They share this loader because they share every mechanic that matters (front matter, reading
time, anchors, related links) and differ only in how they are listed.

Tokens: content must never hardcode the product name or a plan quota, both of which are
environment- and admin-tunable. A page writes ``{{app_name}}`` or ``{{corrections_pro}}``;
:func:`render_html` substitutes at request time from a mapping the caller builds. Substitution
happens on the rendered HTML, not the source, so a token can never inject markup structure.
"""

from __future__ import annotations

import html
import os
import re
from pathlib import Path
from typing import Any, Iterable

import yaml

try:  # pragma: no cover - exercised by the import itself
    import markdown as _markdown
except ImportError as exc:  # pragma: no cover
    raise RuntimeError(
        "The 'markdown' package is required to serve /docs and /ressources-seo. "
        "Add markdown to seo-agent-web/requirements.txt and reinstall."
    ) from exc


CONTENT_ROOT = Path(__file__).resolve().parent.parent / "content"
DOCS_DIR = CONTENT_ROOT / "docs"
BLOG_DIR = CONTENT_ROOT / "blog"

# Front matter is fenced the way every static-site generator fences it, so the files stay
# portable if this ever moves to a real SSG.
_FRONT_MATTER_RE = re.compile(r"\A---\s*\n(.*?)\n---\s*\n?(.*)\Z", re.DOTALL)

# A token is a bare identifier in braces. Anything else — a stray brace, a JS snippet in a code
# block — is left exactly as written.
_TOKEN_RE = re.compile(r"\{\{\s*([a-z0-9_]+)\s*\}\}")

_SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")

# 200 words/minute, the figure every reading-time widget uses. Rounded up, floored at 1: "0 min"
# reads as a bug and "1 min" reads as a short page.
_WORDS_PER_MINUTE = 200

_MARKDOWN_EXTENSIONS = ["extra", "sane_lists", "toc", "admonition"]
_MARKDOWN_EXTENSION_CONFIGS = {
    "toc": {"permalink": False, "toc_depth": "2-3"},
}


class ContentError(RuntimeError):
    """A page that cannot be trusted to render. Raised at load, never at request time."""


def _markdown_instance() -> Any:
    # A fresh converter per page: python-markdown carries per-document state (the toc tokens
    # among them) and reset() has historically leaked footnote ids between documents.
    return _markdown.Markdown(
        extensions=list(_MARKDOWN_EXTENSIONS),
        extension_configs=dict(_MARKDOWN_EXTENSION_CONFIGS),
        output_format="html",
    )


def _split_front_matter(raw: str, *, source: Path) -> tuple[dict[str, Any], str]:
    match = _FRONT_MATTER_RE.match(raw.lstrip("﻿"))
    if not match:
        raise ContentError(f"{source.name}: missing '---' front matter block")
    try:
        meta = yaml.safe_load(match.group(1)) or {}
    except yaml.YAMLError as exc:
        raise ContentError(f"{source.name}: front matter is not valid YAML ({exc})") from exc
    if not isinstance(meta, dict):
        raise ContentError(f"{source.name}: front matter must be a mapping")
    return meta, match.group(2)


def _reading_time(body: str) -> str:
    words = len(re.findall(r"[\w'’-]+", body))
    return f"{max(1, -(-words // _WORDS_PER_MINUTE))} min"


def _wrap_tables(html_text: str) -> str:
    """Put every table in its own horizontal scroller.

    A wide comparison table is the one element that will otherwise make the whole page scroll
    sideways on a phone. Markdown emits a bare ``<table>``, so the wrapper is added here rather
    than asked of every author.
    """
    return html_text.replace("<table>", '<div class="content-table-wrap"><table>').replace(
        "</table>", "</table></div>"
    )


def _clean_str(value: Any) -> str:
    return str(value or "").strip()


def _clean_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, list):
        return [_clean_str(item) for item in value if _clean_str(item)]
    return []


def _clean_faq(value: Any, *, source: Path) -> list[dict[str, str]]:
    if not value:
        return []
    if not isinstance(value, list):
        raise ContentError(f"{source.name}: 'faq' must be a list of question/answer mappings")
    out: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            raise ContentError(f"{source.name}: every 'faq' entry must be a mapping")
        question = _clean_str(item.get("question"))
        answer = _clean_str(item.get("answer"))
        if question and answer:
            out.append({"question": question, "answer": answer})
    return out


def _load_page(path: Path, *, collection: str) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    meta, body = _split_front_matter(raw, source=path)

    slug = _clean_str(meta.get("slug")) or path.stem
    if not _SLUG_RE.match(slug):
        raise ContentError(f"{path.name}: slug {slug!r} must be lowercase-kebab-case")

    title = _clean_str(meta.get("title"))
    description = _clean_str(meta.get("description"))
    if not title:
        raise ContentError(f"{path.name}: 'title' is required")
    if not description:
        raise ContentError(f"{path.name}: 'description' is required — it is the meta description")

    updated_at = _clean_str(meta.get("updated_at"))
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", updated_at):
        raise ContentError(f"{path.name}: 'updated_at' must be YYYY-MM-DD, got {updated_at!r}")

    md = _markdown_instance()
    body_html = _wrap_tables(md.convert(body))
    headings = [
        {"id": _clean_str(token.get("id")), "title": _clean_str(token.get("name")), "level": 2}
        for token in (getattr(md, "toc_tokens", None) or [])
        if _clean_str(token.get("id")) and _clean_str(token.get("name"))
    ]

    return {
        "collection": collection,
        "slug": slug,
        "path": f"/docs/{slug}" if collection == "docs" else f"/ressources-seo/{slug}",
        "title": title,
        "meta_title": _clean_str(meta.get("meta_title")) or title,
        "description": description,
        "kind": _clean_str(meta.get("kind")) or ("Documentation" if collection == "docs" else "Guide"),
        "section": _clean_str(meta.get("section")) or "Général",
        "order": int(meta.get("order") or 999),
        "updated_at": updated_at,
        "published_at": _clean_str(meta.get("published_at")) or updated_at,
        "reading_time": _clean_str(meta.get("reading_time")) or _reading_time(body),
        "audience": _clean_str(meta.get("audience")),
        "keywords": _clean_list(meta.get("keywords")),
        "related": _clean_list(meta.get("related")),
        "faq": _clean_faq(meta.get("faq"), source=path),
        "cta": _clean_str(meta.get("cta")),
        "featured": bool(meta.get("featured")),
        "app_href": _clean_str(meta.get("app_href")),
        "headings": headings,
        "body_html": body_html,
        "source": path.name,
    }


def _load_collection(directory: Path, *, collection: str, errors: list[str]) -> list[dict[str, Any]]:
    """Load every page in ``directory``, recording rather than raising per-file failures.

    A malformed page must not be able to take the product down — the docs are served by the
    same process as the dashboard. So a broken file is skipped and its reason recorded in
    :data:`LOAD_ERRORS`, which ``tests/test_content_library.py`` asserts is empty. Production
    degrades by one page; CI fails on the commit that broke it.
    """
    if not directory.is_dir():
        return []
    pages: list[dict[str, Any]] = []
    seen: dict[str, str] = {}
    for path in sorted(directory.glob("*.md")):
        try:
            page = _load_page(path, collection=collection)
        except (ContentError, OSError, ValueError) as exc:
            errors.append(f"{collection}/{path.name}: {exc}")
            continue
        slug = page["slug"]
        if slug in seen:
            errors.append(f"{collection}/{path.name}: slug {slug!r} already used by {seen[slug]}")
            continue
        seen[slug] = path.name
        pages.append(page)
    if collection == "docs":
        pages.sort(key=lambda p: (p["order"], p["title"]))
    else:
        pages.sort(key=lambda p: (p["published_at"], p["slug"]), reverse=True)
    return pages


_CACHE: dict[str, list[dict[str, Any]]] = {}
_CACHE_STAMP: tuple[Any, ...] = ()

#: Reasons individual pages were skipped at the last load. Empty in a healthy checkout; the
#: test suite asserts as much, which is what turns a content typo into a red CI run.
LOAD_ERRORS: list[str] = []


def _dir_stamp() -> tuple[Any, ...]:
    stamp: list[Any] = []
    for directory in (DOCS_DIR, BLOG_DIR):
        if not directory.is_dir():
            continue
        for path in sorted(directory.glob("*.md")):
            stamp.append((path.name, path.stat().st_mtime))
    return tuple(stamp)


def _hot_reload_enabled() -> bool:
    return _clean_str(os.environ.get("CONTENT_HOT_RELOAD")).lower() in {"1", "true", "yes", "on"}


def reload() -> None:
    """Re-read both collections from disk. Called at import, and per request in authoring mode."""
    global _CACHE_STAMP
    errors: list[str] = []
    _CACHE["docs"] = _load_collection(DOCS_DIR, collection="docs", errors=errors)
    _CACHE["blog"] = _load_collection(BLOG_DIR, collection="blog", errors=errors)
    _CACHE_STAMP = _dir_stamp()
    LOAD_ERRORS[:] = errors
    for reason in errors:
        print(f"[content] skipped {reason}", flush=True)


def _ensure_loaded() -> None:
    if not _CACHE:
        reload()
        return
    if _hot_reload_enabled() and _dir_stamp() != _CACHE_STAMP:
        reload()


def docs_pages() -> list[dict[str, Any]]:
    _ensure_loaded()
    return list(_CACHE.get("docs") or [])


def blog_pages() -> list[dict[str, Any]]:
    _ensure_loaded()
    return list(_CACHE.get("blog") or [])


def all_pages() -> list[dict[str, Any]]:
    return docs_pages() + blog_pages()


def get_doc(slug: str) -> dict[str, Any] | None:
    clean = _clean_str(slug).lower()
    return next((p for p in docs_pages() if p["slug"] == clean), None)


def get_article(slug: str) -> dict[str, Any] | None:
    clean = _clean_str(slug).lower()
    return next((p for p in blog_pages() if p["slug"] == clean), None)


def featured_articles(limit: int = 3) -> list[dict[str, Any]]:
    """Pages the author marked ``featured``, newest first, topped up with the newest others.

    The old implementation took the first three of a hand-ordered list, which meant the home
    page changed whenever someone reordered the file for an unrelated reason.
    """
    pages = blog_pages()
    picked = [p for p in pages if p["featured"]]
    for page in pages:
        if len(picked) >= limit:
            break
        if page not in picked:
            picked.append(page)
    return picked[: max(0, int(limit))]


def docs_sections() -> list[dict[str, Any]]:
    """Docs grouped for the sidebar, sections in first-page order."""
    sections: list[dict[str, Any]] = []
    index: dict[str, dict[str, Any]] = {}
    for page in docs_pages():
        name = page["section"]
        if name not in index:
            index[name] = {"name": name, "pages": []}
            sections.append(index[name])
        index[name]["pages"].append(page)
    return sections


def related_pages(page: dict[str, Any], *, limit: int = 4) -> list[dict[str, Any]]:
    """Explicit ``related:`` slugs first, then same-collection neighbours to fill the gap.

    Cross-collection links are allowed on purpose: a guide that ends on "and here is how the
    product does it" is the whole point of keeping the two collections in one loader.
    """
    by_slug = {p["slug"]: p for p in all_pages()}
    out: list[dict[str, Any]] = []
    for slug in page.get("related") or []:
        candidate = by_slug.get(slug)
        if candidate and candidate["slug"] != page["slug"] and candidate not in out:
            out.append(candidate)
    siblings = docs_pages() if page["collection"] == "docs" else blog_pages()
    for candidate in siblings:
        if len(out) >= limit:
            break
        if candidate["slug"] != page["slug"] and candidate not in out:
            out.append(candidate)
    return out[: max(0, int(limit))]


#: Text fields a page may write a ``{{token}}`` into. The body is handled separately because
#: it is already HTML by the time it gets here.
_TOKEN_FIELDS = ("title", "meta_title", "description", "cta", "audience")


def resolve(page: dict[str, Any], tokens: dict[str, Any] | None = None) -> dict[str, Any]:
    """A copy of ``page`` with every ``{{token}}`` resolved, body included.

    Templates receive this, never the cached page: the cache is shared between requests and
    must stay free of anything environment-dependent.
    """
    out = dict(page)
    for field in _TOKEN_FIELDS:
        out[field] = substitute(str(out.get(field) or ""), tokens)
    out["body_html"] = substitute(str(out.get("body_html") or ""), tokens)
    out["faq"] = [
        {
            "question": substitute(item.get("question", ""), tokens),
            "answer": substitute(item.get("answer", ""), tokens),
        }
        for item in (page.get("faq") or [])
    ]
    return out


def resolve_all(pages: Iterable[dict[str, Any]], tokens: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    return [resolve(page, tokens) for page in pages]


def render_html(page: dict[str, Any], tokens: dict[str, Any] | None = None) -> str:
    """The page body with ``{{token}}`` placeholders resolved, HTML-escaped.

    An unknown token is left visible as written rather than blanked: a page reading
    ``{{corrections_gold}}`` in production is a bug someone should see, not one that silently
    turns into an empty sentence.
    """
    return substitute(page.get("body_html") or "", tokens)


def substitute(text: str, tokens: dict[str, Any] | None = None) -> str:
    mapping = {str(k): str(v) for k, v in (tokens or {}).items()}

    def _replace(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in mapping:
            return match.group(0)
        return html.escape(mapping[key], quote=False)

    return _TOKEN_RE.sub(_replace, text)


def sitemap_entries() -> Iterable[dict[str, str]]:
    """Every public content URL, with the page's own date as ``lastmod``."""
    for page in all_pages():
        yield {"path": page["path"], "lastmod": page["updated_at"], "changefreq": "monthly"}


reload()
