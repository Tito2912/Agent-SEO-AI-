#!/usr/bin/env python3
"""
Extract a clean domain list from a domains CSV export.

This helper is intentionally privacy-preserving: it only reads the domain column
and never prints other columns (owner/contact details).

Supports delimiter-separated CSV (default: ';' as in many FR exports).
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path


def _norm_header(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _pick_domain_column(headers: list[str], preferred: str | None) -> str:
    if not headers:
        raise ValueError("CSV has no headers")

    if preferred:
        preferred_norm = _norm_header(preferred)
        for h in headers:
            if _norm_header(h) == preferred_norm:
                return h

    candidates = {
        "nom du domaine ascii",
        "domain",
        "domaine",
        "domain name",
        "domain ascii",
    }
    for h in headers:
        if _norm_header(h) in candidates:
            return h

    return headers[0]


def extract_domains(csv_path: Path, delimiter: str, column: str | None) -> list[str]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        if not reader.fieldnames:
            return []
        domain_col = _pick_domain_column(list(reader.fieldnames), preferred=column)
        domains: list[str] = []
        for row in reader:
            value = (row.get(domain_col) or "").strip().lower()
            value = value.strip().lstrip(".")
            if not value:
                continue
            # Keep only valid-looking domains.
            if not re.match(r"^[a-z0-9][a-z0-9.-]*[a-z0-9]$", value):
                continue
            domains.append(value)

    # Unique preserving order.
    seen: set[str] = set()
    out: list[str] = []
    for d in domains:
        if d in seen:
            continue
        seen.add(d)
        out.append(d)
    return out


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extract domains from a CSV export into txt/json.")
    p.add_argument("--csv", required=True, help="Path to domains CSV export.")
    p.add_argument("--delimiter", default=";", help="CSV delimiter (default: ';').")
    p.add_argument("--column", help="Domain column name (optional).")
    p.add_argument("--output-txt", help="Write domains (one per line).")
    p.add_argument("--output-json", help="Write domains as JSON array.")
    return p.parse_args(argv)


def main(argv: list[str]) -> int:
    args = _parse_args(argv)
    csv_path = Path(args.csv).expanduser().resolve()
    domains = extract_domains(csv_path, delimiter=str(args.delimiter), column=args.column)

    if args.output_txt:
        out = Path(args.output_txt).expanduser()
        out.write_text("\n".join(domains) + ("\n" if domains else ""), encoding="utf-8")
        print(f"[OK] Wrote {out}")
    if args.output_json:
        out = Path(args.output_json).expanduser()
        out.write_text(json.dumps(domains, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"[OK] Wrote {out}")

    if not args.output_txt and not args.output_json:
        for d in domains:
            print(d)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

