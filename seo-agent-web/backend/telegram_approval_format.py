"""Deterministic presentation only: full evidence, no model call or decision authority."""

import html
import re

PAGE_UNITS = 2800


def units(text):
    return len(text.encode("utf-16-le")) // 2


def split_text(text, limit=PAGE_UNITS):
    """Keep paragraphs or words together where possible, preserving every character."""
    result = []
    while text:
        size = 0
        end = 0
        for char in text:
            width = 2 if ord(char) > 0xFFFF else 1
            if size + width > limit:
                break
            size += width
            end += 1
        if not end:
            raise ValueError("document_too_long")
        if end < len(text):
            # Avoid tiny pages when the only paragraph break is near the start.
            floor = end // 2
            paragraph = text.rfind("\n\n", floor, end)
            word = max(text.rfind("\n", floor, end), text.rfind(" ", floor, end))
            if paragraph >= floor:
                end = paragraph + 2
            elif word >= floor:
                end = word + 1
        result.append(text[:end])
        text = text[end:]
    return result


def document(doc, sections, *, summary=""):
    sections = [(str(title), str(body)) for title, body in sections if body]
    return {
        **doc,
        "text": "\n\n".join(
            (title + "\n" if title else "") + body for title, body in sections
        ),
        "display_sections": sections,
        "decision_summary": summary,
    }


def pages(doc):
    sections = doc.get("display_sections") or [("", doc["text"])]
    result = []
    current = ""
    for title, body in sections:
        prefix = title + "\n" if title else ""
        if units(prefix) > 500:
            raise ValueError("document_too_long")
        chunks = split_text(body, PAGE_UNITS - units(prefix))
        for chunk in chunks:
            block = prefix + chunk
            if current and units(current + "\n\n" + block) > PAGE_UNITS:
                result.append(current)
                current = ""
            current += ("\n\n" if current else "") + block
    if current:
        result.append(current)
    if not result or len(result) > 16:
        raise ValueError("document_too_long")
    return result


def source_sections(sources, en=False):
    result = []
    for index, source in enumerate(sources, 1):
        title = source["title"]
        lines = [source["url"]]
        if source.get("checked_at"):
            lines.append(
                ("Checked: " if en else "Consultée le : ") + str(source["checked_at"])
            )
        if source.get("fingerprint"):
            lines.append(
                ("Version: " if en else "Version : ") + source["fingerprint"][:12]
            )
        if source.get("excerpt_limited"):
            lines.append(
                "Excerpt — full source at the link above."
                if en
                else "Extrait — source complète via le lien ci-dessus."
            )
        lines += ["", source.get("text", source.get("guidance", ""))]
        result.append((f"📚 Source {index} · {title}", "\n".join(lines)))
    return result


_LABELS = {
    "source": ("Origine", "Source"),
    "property": ("Propriété", "Property"),
    "crawl": ("Analyse", "Analysis"),
    "start": ("Début", "From"),
    "end": ("Fin", "Until"),
    "page": ("Page", "Page"),
    "query": ("Recherche", "Query"),
    "impressions": ("Impressions", "Impressions"),
    "clicks": ("Clics", "Clicks"),
    "position": ("Position moyenne", "Average position"),
    "references": ("Références", "References"),
    "title": ("Titre", "Title"),
    "url": ("Adresse", "URL"),
    "guidance": ("Repère", "Guidance"),
    "checked_at": ("Consultée le", "Checked"),
    "reviewed_at": ("Relue le", "Reviewed"),
    "fingerprint": ("Version de la source", "Source version"),
    "slug": ("Adresse de l’article", "Article path"),
    "kind": ("Type", "Type"),
    "lang": ("Langue", "Language"),
    "key": ("Référence", "Reference"),
}


def evidence(value, en=False, indent=""):
    """Human labels for a brief's evidence; retain unknown fields instead of guessing."""
    lines = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"version", "catalog", "catalog_hash"}:
                continue
            label = _LABELS.get(key, (key.replace("_", " "),) * 2)[int(en)]
            if isinstance(item, (dict, list)):
                lines += [indent + label + " :", evidence(item, en, indent + "  ")]
            elif item is not None and item != "":
                text = str(item)
                if key == "fingerprint":
                    text = text[:12]
                elif key in {"source", "kind", "lang"}:
                    labels = {
                        "gsc_api": ("Google Search Console", "Google Search Console"),
                        "bing": ("Bing", "Bing"),
                        "editorial": ("Veille éditoriale", "Editorial research"),
                        "research": ("Sujet à documenter", "Research topic"),
                        "guide": ("Guide pratique", "Practical guide"),
                        "page": ("Page du site", "Site page"),
                        "fr": ("Français", "French"),
                        "en": ("Anglais", "English"),
                    }
                    text = labels.get(text, (text, text))[int(en)]
                lines.append(indent + label + " : " + text)
    elif isinstance(value, list):
        for item in value:
            lines.append(indent + "• " + evidence(item, en, indent + "  ").lstrip())
    else:
        lines.append(indent + str(value))
    return "\n".join(lines)


def writing_sections(packet, en=False):
    brief = packet["brief"]
    result = [("🧭 Brief", evidence(brief, en))]
    result += source_sections(packet["sources"], en)
    catalog = "\n".join(
        "• " + title + " · " + slug for slug, title in packet["catalog"]
    )
    if packet.get("catalog_truncated"):
        catalog += "\n" + ("Partial catalog." if en else "Catalogue partiel.")
    if catalog:
        result.append(
            ("📖 Existing articles" if en else "📖 Articles déjà présents", catalog)
        )
    return result


def brief_sections(row, en=False):
    note = (
        "Research and writing brief, not a publishable article. Compare existing resources to avoid duplicates. Observed search demand does not prove a content gap. Do not invent volume, results, customer stories or product capabilities. Content effects need later observation and do not prove causality. This is an evidence snapshot: recheck current source status and dates before writing."
        if en
        else "Brief à documenter et à rédiger. Compare les ressources existantes pour éviter les doublons. Une recherche observée ne prouve pas un manque de contenu. Ne pas inventer de volume, de résultat, de cas client ou de fonction produit. Les effets éditoriaux demandent une observation ultérieure et ne prouvent pas une causalité. Il s’agit d’une photographie des preuves : revérifie l’état actuel et les dates des sources avant rédaction."
    )
    outline = (
        [
            "Clarify the reader’s question",
            "Use a sourced Noyaru example",
            "Explain the steps and their limits",
            "Distinguish crawling, indexing, ranking and AI citations",
            "Cite current official sources",
        ]
        if en
        else [
            "Préciser la question du lecteur",
            "Montrer un exemple Noyaru sourcé",
            "Expliquer les étapes et leurs limites",
            "Distinguer exploration, indexation, classement et citations IA",
            "Citer les sources officielles actuelles",
        ]
    )
    return [
        (
            "🧭 Brief",
            row.title + "\n" + row.lang.upper() + " · " + row.created_at.isoformat(),
        ),
        (
            "📊 Observed evidence" if en else "📊 Preuves observées",
            evidence(row.evidence, en),
        ),
        (
            "✍️ Suggested outline" if en else "✍️ Plan proposé",
            "\n".join("• " + item for item in outline),
        ),
        ("🔎 Before writing" if en else "🔎 Avant rédaction", note),
    ]


def draft_sections(row, en=False):
    content = row.content
    sources = {s["id"]: index for index, s in enumerate(row.bundle["sources"], 1)}

    def paragraph(value):
        refs = " ".join("[" + str(sources[key]) + "]" for key in value["sources"])
        return value["text"] + "\n" + ("Sources: " if en else "Sources : ") + refs

    result = [
        ("📝 " + content["title"], content["description"]),
        ("📄 Introduction", paragraph(content["intro"])),
    ]
    for section in content["sections"]:
        result.append(
            (
                "📄 " + section["heading"],
                "\n\n".join(paragraph(p) for p in section["paragraphs"]),
            )
        )
    result += source_sections(row.bundle["sources"], en)
    return result


def escaped(text):
    return html.escape(text, quote=False)


def one_line(text, limit=160):
    # Topics stay data even when a subject contains line breaks or bidi controls.
    text = re.sub(r"[\u202a-\u202e\u2066-\u2069]", "", str(text))
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 1] + "…"
