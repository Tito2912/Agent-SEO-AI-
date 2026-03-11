#!/usr/bin/env python3
"""
Analyze Google Search Console "Performance" CSV exports (queries/pages) and
surface prioritizable SEO opportunities.

Input: one CSV export file from GSC (Search results report) or multiple files.
Output: Markdown report (stdout or --output).
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import math
import os
import re
import sys
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Row:
    dimension: str
    clicks: int
    impressions: int
    ctr: float
    position: float


def _to_int(value: str) -> int:
    value = (value or "").strip().replace("\u202f", "").replace("\xa0", "").replace(" ", "")
    if not value:
        return 0
    value = value.replace(",", ".")
    try:
        return int(float(value))
    except ValueError:
        return 0


def _to_float(value: str) -> float:
    value = (value or "").strip().replace("\u202f", "").replace("\xa0", "").replace(" ", "")
    if not value:
        return 0.0
    value = value.replace(",", ".")
    try:
        return float(value)
    except ValueError:
        return 0.0


def _to_ctr(value: str) -> float:
    raw = (value or "").strip().replace("\u202f", "").replace("\xa0", "").replace(" ", "")
    if not raw:
        return 0.0
    raw = raw.replace(",", ".")
    if raw.endswith("%"):
        return max(0.0, min(1.0, _to_float(raw[:-1]) / 100.0))
    as_float = _to_float(raw)
    if as_float > 1.0:
        return max(0.0, min(1.0, as_float / 100.0))
    return max(0.0, min(1.0, as_float))


def _norm_header(header: str) -> str:
    return re.sub(r"\s+", " ", (header or "").strip().lower())


def _detect_columns(headers: list[str]) -> dict[str, str]:
    normalized = {_norm_header(h): h for h in headers}
    colmap: dict[str, str] = {}

    def pick(candidates: list[str]) -> str | None:
        for c in candidates:
            if c in normalized:
                return normalized[c]
        return None

    dimension = pick(
        [
            "top queries",
            "requêtes principales",
            "top pages",
            "pages principales",
            "query",
            "page",
        ]
    )
    clicks = pick(["clicks", "clics"])
    impressions = pick(["impressions"])
    ctr = pick(["ctr", "taux de clics"])
    position = pick(["position", "average position", "position moyenne"])

    if not dimension:
        dimension = headers[0] if headers else None

    if dimension:
        colmap["dimension"] = dimension
    if clicks:
        colmap["clicks"] = clicks
    if impressions:
        colmap["impressions"] = impressions
    if ctr:
        colmap["ctr"] = ctr
    if position:
        colmap["position"] = position
    return colmap


def _expected_ctr(position: float) -> float:
    if position <= 1:
        return 0.30
    if position <= 2:
        return 0.16
    if position <= 3:
        return 0.10
    if position <= 4:
        return 0.07
    if position <= 5:
        return 0.05
    if position <= 6:
        return 0.04
    if position <= 7:
        return 0.03
    if position <= 8:
        return 0.025
    if position <= 9:
        return 0.020
    if position <= 10:
        return 0.018
    if position <= 15:
        return 0.012
    if position <= 20:
        return 0.008
    return 0.004


def _read_rows(csv_path: str) -> list[Row]:
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return []
        col = _detect_columns(list(reader.fieldnames))
        rows: list[Row] = []
        for r in reader:
            dim = (r.get(col.get("dimension", ""), "") or "").strip()
            if not dim:
                continue
            clicks = _to_int(r.get(col.get("clicks", ""), "0") or "0")
            impressions = _to_int(r.get(col.get("impressions", ""), "0") or "0")
            ctr = _to_ctr(r.get(col.get("ctr", ""), "0") or "0")
            position = _to_float(r.get(col.get("position", ""), "0") or "0")
            rows.append(Row(dimension=dim, clicks=clicks, impressions=impressions, ctr=ctr, position=position))
        return rows


def _render_report(title: str, rows: list[Row], min_impressions: int) -> str:
    total_clicks = sum(r.clicks for r in rows)
    total_impressions = sum(r.impressions for r in rows)
    avg_ctr = (total_clicks / total_impressions) if total_impressions else 0.0
    weighted_pos = (
        sum(r.position * r.impressions for r in rows) / total_impressions if total_impressions else 0.0
    )

    opportunities: list[dict[str, Any]] = []
    for r in rows:
        if r.impressions < min_impressions or r.position <= 0:
            continue
        exp_ctr = _expected_ctr(r.position)
        exp_clicks = r.impressions * exp_ctr
        delta = exp_clicks - r.clicks
        if delta <= 0:
            continue
        opportunities.append(
            {
                "dimension": r.dimension,
                "clicks": r.clicks,
                "impressions": r.impressions,
                "ctr": r.ctr,
                "position": r.position,
                "expected_ctr": exp_ctr,
                "missing_clicks": delta,
            }
        )

    opportunities.sort(key=lambda d: (-d["missing_clicks"], -d["impressions"], d["dimension"]))

    quick_wins = [o for o in opportunities if 3.0 <= o["position"] <= 10.0][:25]
    push_page_1 = [o for o in opportunities if 10.0 < o["position"] <= 20.0][:25]

    def fmt_pct(x: float) -> str:
        return f"{x * 100:.2f}%"

    def fmt_float(x: float) -> str:
        if math.isfinite(x):
            return f"{x:.2f}"
        return "0.00"

    lines: list[str] = []
    lines.append(f"# {title}")
    lines.append("")
    lines.append("## Résumé")
    lines.append(f"- Clicks: **{total_clicks:,}**".replace(",", " "))
    lines.append(f"- Impressions: **{total_impressions:,}**".replace(",", " "))
    lines.append(f"- CTR global: **{fmt_pct(avg_ctr)}**")
    lines.append(f"- Position moyenne (pondérée): **{fmt_float(weighted_pos)}**")
    lines.append("")
    lines.append("## Quick wins (CTR) — positions 3 à 10")
    if not quick_wins:
        lines.append("- (Aucun)")
    else:
        for o in quick_wins:
            lines.append(
                f"- {o['dimension']} — impr {o['impressions']:,} / clicks {o['clicks']:,} / CTR {fmt_pct(o['ctr'])} / pos {fmt_float(o['position'])} / +{o['missing_clicks']:.0f} clicks est."
                .replace(",", " ")
            )
    lines.append("")
    lines.append("## À pousser en page 1 — positions 11 à 20")
    if not push_page_1:
        lines.append("- (Aucun)")
    else:
        for o in push_page_1:
            lines.append(
                f"- {o['dimension']} — impr {o['impressions']:,} / clicks {o['clicks']:,} / CTR {fmt_pct(o['ctr'])} / pos {fmt_float(o['position'])} / +{o['missing_clicks']:.0f} clicks est."
                .replace(",", " ")
            )
    lines.append("")
    lines.append("## Next actions recommandées")
    lines.append("1. Réécrire titles/meta descriptions des quick wins (alignement intention + promesse + bénéfice).")
    lines.append("2. Ajouter données structurées pertinentes (FAQ/HowTo/Product/Article) si applicable.")
    lines.append("3. Renforcer maillage interne vers les URLs à pousser (ancres cohérentes + pages hubs).")
    lines.append("4. Mettre à jour le contenu (exemples, sections manquantes, E-E-A-T) sur les URLs pos 11–20.")
    lines.append("")
    return "\n".join(lines)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyze GSC CSV exports and output a Markdown opportunity report.")
    p.add_argument("csv", nargs="+", help="Path(s) to GSC CSV export files.")
    p.add_argument("--min-impressions", type=int, default=100, help="Ignore rows below this impressions threshold (default: 100).")
    p.add_argument("--output", help="Write Markdown report to this file (default: stdout).")
    return p.parse_args(argv)


def main(argv: list[str]) -> int:
    args = _parse_args(argv)
    all_rows: list[Row] = []
    for path in args.csv:
        all_rows.extend(_read_rows(path))

    stamp = dt.datetime.now().strftime("%Y-%m-%d")
    title = f"GSC Opportunités SEO ({stamp})"
    report = _render_report(title=title, rows=all_rows, min_impressions=args.min_impressions)

    if args.output:
        os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"[OK] Wrote {args.output}")
    else:
        print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
