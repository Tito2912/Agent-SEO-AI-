"""Deterministic new Markdown articles from reviewed, structured private drafts."""

import hashlib
import html
import re
from urllib.parse import urlsplit

import yaml
from bs4 import BeautifulSoup

try:
    from . import own_seo as s, own_writing as w, seo_watch
except ImportError:
    import own_seo as s
    import own_writing as w
    import seo_watch

ROOT = "seo-agent-web/content/blog/"
REPO = "Tito2912/Agent-SEO-AI-"
REPO_ID = 1178767307
BASE = "main"


def sha(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def blob(text):
    value = text.encode("utf-8")
    return hashlib.sha1(
        b"blob " + str(len(value)).encode() + b"\0" + value, usedforsecurity=False
    ).hexdigest()


def tokenized(text):
    return re.sub(r"\bNoyaru\b", "{{app_name}}", text, flags=re.I)


def prose(text):
    text = re.sub(r"([\\`*_[\]#!|~])", r"\\\1", html.escape(text))
    return tokenized(text)


def source_url(source, lang):
    key = source["id"]
    if source["kind"] == "official":
        entry = seo_watch.REGISTRY.get(key)
        if entry and entry.url == source["url"]:
            return entry.url
    if source["kind"] == "product" and key in {
        "product_lire-le-rapport",
        "product_verifier-une-correction",
    }:
        expected = (
            s.BASE.rstrip("/")
            + ("/en" if lang == "en" else "")
            + "/docs/"
            + key.removeprefix("product_")
        )
        if source["url"] == expected:
            return expected
    raise ValueError("sources_required")


def article(row, slug, date):
    """EN deliberately receives the FR canonical slug; the owner reviews that pairing."""
    lang = row.bundle["brief"]["lang"]
    content = w.validate(row.content, row.bundle["sources"])
    sources = {x["id"]: x for x in row.bundle["sources"]}
    used = []

    def paragraph(p):
        refs = []
        for ident in p["sources"]:
            if ident not in used:
                used.append(ident)
            refs.append(
                "["
                + str(used.index(ident) + 1)
                + "](#reference-"
                + str(used.index(ident) + 1)
                + ")"
            )
        return prose(p["text"]) + " " + " ".join(refs)

    body = [paragraph(content["intro"])]
    for section in content["sections"]:
        body += ["## " + prose(section["heading"])]
        body += [paragraph(p) for p in section["paragraphs"]]
    body += ["## " + ("Sources and limits" if lang == "en" else "Sources et limites")]
    body += [
        (
            "A technical observation does not prove indexing, a ranking gain or visibility in AI answers."
            if lang == "en"
            else "Un constat technique ne prouve ni indexation, ni gain de classement, ni visibilité dans les réponses des IA."
        )
    ]
    for i, ident in enumerate(used, 1):
        source = sources[ident]
        url = source_url(source, lang)
        # The server's closed registry is the only link authority. Never model URLs.
        if urlsplit(url).scheme != "https" or any(c in url for c in '\r\n<>"()'):
            raise ValueError("sources_required")
        body += [
            '<span id="reference-' + str(i) + '"></span>',
            str(i) + ". [" + prose(source["title"]) + "](" + url + ")",
        ]
    meta = dict(
        slug=slug,
        title=tokenized(content["title"]),
        meta_title=tokenized(content["title"]),
        description=tokenized(content["description"]),
        kind="Guide",
        updated_at=date,
        published_at=date,
        audience="Independent professionals and small businesses"
        if lang == "en"
        else "Indépendants et petites entreprises",
        related=[],
        featured=False,
    )
    text = (
        "---\n"
        + yaml.safe_dump(meta, allow_unicode=True, sort_keys=False, width=200)
        + "---\n\n"
        + "\n\n".join(body)
        + "\n"
    )
    if len(text.encode("utf-8")) > 24000:
        raise ValueError("content_limit")
    return {
        "lang": lang,
        "path": ROOT + ("en/" if lang == "en" else "") + slug + ".md",
        "url": s.BASE.rstrip("/")
        + ("/en" if lang == "en" else "")
        + "/ressources-seo/"
        + slug,
        "text": text,
        "blob": blob(text),
        "sha256": sha(text),
    }


def public_matches(file, document):
    """Check the rendered article as well as its release/source response headers."""
    library = s.module().content_library
    _, front, body = file["text"].split("---\n", 2)
    meta = yaml.safe_load(front)
    expected = library.resolve(
        {**meta, "body_html": library._markdown_instance().convert(body)},
        s.module()._content_tokens(file["lang"]),
    )
    parsed = BeautifulSoup(document, "html.parser")
    root = parsed.find("html")
    canonical = parsed.select('link[rel="canonical"]')
    description = parsed.select('meta[name="description"]')
    title = parsed.select("article h1")
    content = parsed.select("article .content-body")

    def text(node):
        return " ".join(node.get_text(" ", strip=True).split())

    expected_body = BeautifulSoup(expected["body_html"], "html.parser")
    return (
        root is not None
        and root.get("lang") == file["lang"]
        and len(canonical) == 1
        and canonical[0].get("href") == file["url"]
        and len(description) == 1
        and description[0].get("content") == expected["description"]
        and len(title) == 1
        and text(title[0]) == " ".join(expected["title"].split())
        and len(content) == 1
        and text(content[0]) == text(expected_body)
    )


def drafts(db, owner, fr_id, en_id=None, *, current=True):
    rows = [w.row_for(db, owner, fr_id)]
    if en_id:
        rows.append(w.row_for(db, owner, en_id))
    for row, lang in zip(rows, ("fr", "en")):
        if (
            row.status != "reviewed"
            or not row.reviewed_at
            or row.bundle["brief"]["lang"] != lang
            or row.content_hash != s.digest(row.content)
            or (current and not w.current(db, row))
        ):
            raise ValueError("draft_required")
    if len(rows) == 2:
        # Same measured editorial opportunity, not just two unrelated reviewed drafts.
        _, _, fr = s.brief_for(db, owner, rows[0].brief_id)
        _, _, en = s.brief_for(db, owner, rows[1].brief_id)

        def opportunity(brief):
            evidence = {**brief.evidence}
            if "references" in evidence:
                evidence["references"] = [
                    {k: r[k] for k in ("key", "url", "fingerprint")}
                    for r in evidence["references"]
                ]
            return s.digest([brief.kind, evidence])

        if opportunity(fr) != opportunity(en):
            raise ValueError("translation_required")
    return rows


def files(db, owner, fr_id, en_id, date):
    rows = drafts(db, owner, fr_id, en_id)
    slug = rows[0].content["slug"]
    return slug, [article(row, slug, date) for row in rows]
