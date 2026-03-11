#!/usr/bin/env python3
"""
Map custom domains -> Netlify site metadata (id, name, url, build settings).

Requires: NETLIFY_TOKEN (recommended) or --token.

Use case:
- You have a list of domains (CSV export) and you want to know which Netlify site
  serves each domain, plus the linked repo/branch/build settings (if any).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests


def _norm_domain(value: str) -> str:
    value = (value or "").strip().lower()
    value = value.strip().rstrip(".")
    if value.startswith("http://") or value.startswith("https://"):
        value = re.sub(r"^https?://", "", value)
    value = value.split("/", 1)[0]
    return value


def _read_domains_from_txt(path: Path) -> list[str]:
    domains: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        d = _norm_domain(raw)
        if d:
            domains.append(d)
    seen: set[str] = set()
    out: list[str] = []
    for d in domains:
        if d in seen:
            continue
        seen.add(d)
        out.append(d)
    return out


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


def _read_domains_from_csv(path: Path, delimiter: str, column: str | None) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        if not reader.fieldnames:
            return []
        domain_col = _pick_domain_column(list(reader.fieldnames), preferred=column)
        domains: list[str] = []
        for row in reader:
            d = _norm_domain(row.get(domain_col) or "")
            if d:
                domains.append(d)
    seen: set[str] = set()
    out: list[str] = []
    for d in domains:
        if d in seen:
            continue
        seen.add(d)
        out.append(d)
    return out


@dataclass(frozen=True)
class NetlifySite:
    id: str
    name: str | None
    url: str | None
    ssl_url: str | None
    admin_url: str | None
    custom_domain: str | None
    domain_aliases: list[str]
    repo_url: str | None
    repo_branch: str | None
    build_cmd: str | None
    publish_dir: str | None


def _netlify_get_sites(token: str) -> list[NetlifySite]:
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/json"})

    sites: list[NetlifySite] = []
    page = 1
    while True:
        resp = session.get("https://api.netlify.com/api/v1/sites", params={"per_page": 100, "page": page}, timeout=30)
        if resp.status_code >= 400:
            raise RuntimeError(f"Netlify API error: HTTP {resp.status_code} - {resp.text[:200]}")
        data = resp.json()
        if not isinstance(data, list) or not data:
            break
        for s in data:
            if not isinstance(s, dict):
                continue
            build = s.get("build_settings") if isinstance(s.get("build_settings"), dict) else {}
            domain_aliases = s.get("domain_aliases") if isinstance(s.get("domain_aliases"), list) else []
            domain_aliases_clean = [_norm_domain(str(d)) for d in domain_aliases if isinstance(d, str) and _norm_domain(d)]

            sites.append(
                NetlifySite(
                    id=str(s.get("id") or ""),
                    name=str(s.get("name") or "") or None,
                    url=str(s.get("url") or "") or None,
                    ssl_url=str(s.get("ssl_url") or "") or None,
                    admin_url=str(s.get("admin_url") or "") or None,
                    custom_domain=_norm_domain(str(s.get("custom_domain") or "")) or None,
                    domain_aliases=sorted(set(domain_aliases_clean)),
                    repo_url=str(build.get("repo_url") or "") or None,
                    repo_branch=str(build.get("repo_branch") or "") or None,
                    build_cmd=str(build.get("cmd") or "") or None,
                    publish_dir=str(build.get("dir") or "") or None,
                )
            )
        page += 1
        if page > 50:
            break
    return sites


def _build_domain_index(sites: list[NetlifySite]) -> dict[str, NetlifySite]:
    index: dict[str, NetlifySite] = {}
    for s in sites:
        candidates = []
        if s.custom_domain:
            candidates.append(s.custom_domain)
        candidates.extend(s.domain_aliases)
        for d in candidates:
            if d and d not in index:
                index[d] = s
    return index


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


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Map domains to Netlify sites using the Netlify API.")
    p.add_argument("--token", help="Netlify token (prefer env NETLIFY_TOKEN).")
    p.add_argument("--domains", help="Text file with one domain per line.")
    p.add_argument("--csv", help="CSV file containing domains (only the domain column is read).")
    p.add_argument("--delimiter", default=";", help="CSV delimiter (default: ';').")
    p.add_argument("--column", help="Domain column name (optional).")
    p.add_argument("--output-json", default="netlify-domain-map.json", help="Output JSON path.")
    p.add_argument("--output-csv", default="netlify-domain-map.csv", help="Output CSV path.")
    return p.parse_args(argv)


def main(argv: list[str]) -> int:
    args = _parse_args(argv)
    token = args.token or os.environ.get("NETLIFY_TOKEN")
    if not token:
        print("[ERROR] Missing Netlify token. Set NETLIFY_TOKEN or pass --token.", file=sys.stderr)
        return 2

    domains: list[str] = []
    if args.domains:
        domains = _read_domains_from_txt(Path(args.domains).expanduser().resolve())
    elif args.csv:
        domains = _read_domains_from_csv(Path(args.csv).expanduser().resolve(), delimiter=str(args.delimiter), column=args.column)
    else:
        print("[ERROR] Provide --domains or --csv.", file=sys.stderr)
        return 2

    sites = _netlify_get_sites(token)
    index = _build_domain_index(sites)

    mapped: list[dict[str, Any]] = []
    for d in domains:
        s = index.get(d)
        mapped.append(
            {
                "domain": d,
                "found": bool(s),
                "site_id": s.id if s else "",
                "site_name": s.name if s else "",
                "ssl_url": s.ssl_url if s else "",
                "url": s.url if s else "",
                "custom_domain": s.custom_domain if s else "",
                "repo_url": s.repo_url if s else "",
                "repo_branch": s.repo_branch if s else "",
                "build_cmd": s.build_cmd if s else "",
                "publish_dir": s.publish_dir if s else "",
            }
        )

    out_json = Path(args.output_json).expanduser()
    out_json.write_text(json.dumps(mapped, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    out_csv = Path(args.output_csv).expanduser()
    _write_csv(out_csv, mapped)

    found = sum(1 for r in mapped if r.get("found"))
    print(f"[OK] Mapped {found}/{len(mapped)} domain(s)")
    print(f"[OK] Wrote {out_json}")
    print(f"[OK] Wrote {out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

