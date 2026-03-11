#!/usr/bin/env python3
"""
Generate a cross-site internal linking plan ("maillage inter-sites") based on SEO audit reports.

Input: one or more `report.json` files produced by `seo_audit.py`
Output: `interlinking-plan.md` + `interlinking-plan.csv`

This is designed for linking ONLY between your own sites (no external outreach).
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit


STOPWORDS_FR = {
    "a",
    "à",
    "au",
    "aux",
    "avec",
    "ce",
    "ces",
    "cette",
    "cet",
    "chez",
    "comme",
    "dans",
    "de",
    "des",
    "du",
    "elle",
    "en",
    "et",
    "eux",
    "il",
    "ils",
    "je",
    "la",
    "le",
    "les",
    "leur",
    "lui",
    "ma",
    "mais",
    "me",
    "mes",
    "moi",
    "mon",
    "ne",
    "nos",
    "notre",
    "nous",
    "on",
    "ou",
    "par",
    "pas",
    "pour",
    "qu",
    "que",
    "qui",
    "sa",
    "se",
    "ses",
    "son",
    "sur",
    "ta",
    "te",
    "tes",
    "toi",
    "ton",
    "tu",
    "un",
    "une",
    "vos",
    "votre",
    "vous",
    "y",
}

STOPWORDS_EN = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "has",
    "have",
    "in",
    "is",
    "it",
    "its",
    "of",
    "on",
    "or",
    "that",
    "the",
    "to",
    "was",
    "were",
    "with",
}


def _root_url(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, "", "", ""))


def _looks_noindex(value: str | None) -> bool:
    v = (value or "").lower()
    return "noindex" in v


def _tokenize(text: str) -> set[str]:
    words = re.findall(r"[0-9A-Za-zÀ-ÖØ-öø-ÿ]{2,}", text.lower())
    out: set[str] = set()
    for w in words:
        if w in STOPWORDS_FR or w in STOPWORDS_EN:
            continue
        if len(w) <= 2:
            continue
        out.add(w)
    return out


def _safe_title(value: str | None, fallback: str) -> str:
    v = (value or "").strip()
    v = re.sub(r"\s+", " ", v)
    if not v:
        return fallback
    return v[:160]


@dataclass(frozen=True)
class Page:
    site: str
    url: str
    title: str
    tokens: set[str]
    word_count: int


def _load_pages(report_path: Path) -> list[Page]:
    data = json.loads(report_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return []

    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    base_url = str(meta.get("base_url") or "").strip()
    site_root = _root_url(base_url) if base_url else ""

    pages = data.get("pages")
    if not isinstance(pages, list):
        return []

    out: list[Page] = []
    for p in pages:
        if not isinstance(p, dict):
            continue
        status = p.get("status_code")
        if not isinstance(status, int) or status != 200:
            continue
        content_type = str(p.get("content_type") or "")
        if "html" not in content_type:
            continue
        if _looks_noindex(p.get("meta_robots")) or _looks_noindex(p.get("x_robots_tag")):
            continue

        url = str(p.get("final_url") or p.get("url") or "").strip()
        if not url:
            continue

        title = _safe_title(p.get("title"), fallback=url)
        h1 = p.get("h1") if isinstance(p.get("h1"), list) else []
        h2 = p.get("h2") if isinstance(p.get("h2"), list) else []
        text = " ".join([title, *[str(x) for x in h1[:1]], *[str(x) for x in h2[:3]]])

        tokens = _tokenize(text)
        if not tokens:
            continue

        wc = p.get("text_word_count")
        word_count = int(wc) if isinstance(wc, int) else 0

        out.append(Page(site=site_root or _root_url(url), url=url, title=title, tokens=tokens, word_count=word_count))
    return out


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    if inter == 0:
        return 0.0
    union = len(a | b)
    return inter / union if union else 0.0


def _find_report_jsons(find_in: Path) -> list[Path]:
    return sorted(find_in.rglob("report.json"))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers: list[str] = []
    for r in rows:
        for k in r.keys():
            if k not in headers:
                headers.append(k)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _render_md(rows: list[dict[str, Any]]) -> str:
    by_site: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        by_site.setdefault(str(r.get("source_site") or ""), []).append(r)

    lines: list[str] = []
    lines.append("# Plan de maillage inter-sites (proposé)")
    lines.append("")
    lines.append("Règle: liens uniquement entre tes sites, **pertinents pour l’utilisateur**, sans sur-optimisation.")
    lines.append("")
    lines.append(f"- Suggestions: **{len(rows)}**")
    lines.append(f"- Sites sources: **{len([s for s in by_site.keys() if s])}**")
    lines.append("")

    for site, items in sorted(by_site.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        if not site:
            continue
        lines.append(f"## {site}")
        for r in items[:50]:
            lines.append(f"- Source: {r['source_url']}")
            lines.append(f"  - Cible: {r['target_url']}")
            lines.append(f"  - Ancre suggérée: {r['anchor_text']}")
            lines.append(f"  - Score: {r['score']}")
        lines.append("")
    return "\n".join(lines)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate cross-site internal linking plan from seo_audit report.json files.")
    p.add_argument("--report-json", action="append", default=[], help="Path to a report.json (repeatable).")
    p.add_argument("--find-in", default="seo-runs", help="Directory to search for report.json (default: seo-runs).")
    p.add_argument("--output-dir", default=".", help="Output directory (default: current dir).")
    p.add_argument("--min-score", type=float, default=0.18, help="Minimum similarity score (default: 0.18).")
    p.add_argument("--per-page", type=int, default=2, help="Max outgoing cross-site links per page (default: 2).")
    p.add_argument("--max-inbound", type=int, default=15, help="Max inbound suggestions per target URL (default: 15).")
    return p.parse_args(argv)


def main(argv: list[str]) -> int:
    args = _parse_args(argv)
    report_paths: list[Path] = []
    for p in args.report_json:
        report_paths.append(Path(p).expanduser().resolve())
    if not report_paths:
        report_paths = _find_report_jsons(Path(args.find_in).expanduser().resolve())

    report_paths = [p for p in report_paths if p.exists() and p.is_file()]
    if len(report_paths) < 2:
        print("[ERROR] Need at least 2 report.json files to generate inter-site linking.", file=sys.stderr)
        return 2

    all_pages: list[Page] = []
    for rp in report_paths:
        all_pages.extend(_load_pages(rp))

    if len(all_pages) < 2:
        print("[ERROR] Not enough pages extracted from reports.", file=sys.stderr)
        return 2

    # Build inverted index token -> page indices (targets)
    token_to_pages: dict[str, list[int]] = {}
    for i, page in enumerate(all_pages):
        for t in page.tokens:
            token_to_pages.setdefault(t, []).append(i)

    per_page = max(0, int(args.per_page))
    min_score = float(args.min_score)
    max_inbound = max(1, int(args.max_inbound))
    inbound_counts: dict[str, int] = {}

    rows: list[dict[str, Any]] = []
    for i, source in enumerate(all_pages):
        candidates: set[int] = set()
        for t in source.tokens:
            for idx in token_to_pages.get(t, []):
                candidates.add(idx)
        scored: list[tuple[float, int, int]] = []
        for idx in candidates:
            if idx == i:
                continue
            target = all_pages[idx]
            if target.site == source.site:
                continue
            if inbound_counts.get(target.url, 0) >= max_inbound:
                continue
            score = _jaccard(source.tokens, target.tokens)
            if score < min_score:
                continue
            scored.append((score, target.word_count, idx))

        scored.sort(key=lambda x: (-x[0], -x[1]))
        used = 0
        for score, _wc, idx in scored:
            if used >= per_page:
                break
            target = all_pages[idx]
            inbound_counts[target.url] = inbound_counts.get(target.url, 0) + 1
            common = sorted(source.tokens & target.tokens)
            rows.append(
                {
                    "source_site": source.site,
                    "source_url": source.url,
                    "target_site": target.site,
                    "target_url": target.url,
                    "anchor_text": target.title,
                    "score": round(score, 4),
                    "common_tokens": " ".join(common[:12]),
                }
            )
            used += 1

    out_dir = Path(args.output_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    md_path = out_dir / "interlinking-plan.md"
    csv_path = out_dir / "interlinking-plan.csv"

    md_path.write_text(_render_md(rows), encoding="utf-8")
    _write_csv(csv_path, rows)

    print(f"[OK] Wrote {md_path}")
    print(f"[OK] Wrote {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

