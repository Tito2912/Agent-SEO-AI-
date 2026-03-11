#!/usr/bin/env python3
"""
SEO Autopilot Orchestrator

Run a repeatable SEO workflow from config:
- Crawl audit (tech + on-page)
- Analyze GSC CSV exports
- Generate a prioritized backlog file
- Optionally run deploy commands (explicit --execute)

This is an orchestrator: it does not attempt to "hack" credentials nor do spam.
Secrets should be provided via env vars / .env files, never committed.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import shlex
import subprocess
import sys
import csv
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit, urlunsplit

try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:
    BeautifulSoup = None  # type: ignore


PROJECT_ROOT = Path(__file__).resolve()
for _ in range(6):
    # Locate the repo root (this script lives in `skills/public/seo-autopilot/scripts/`).
    if (PROJECT_ROOT / "seo-agent-web").exists() or (PROJECT_ROOT / "seo-autopilot.yml").exists():
        break
    PROJECT_ROOT = PROJECT_ROOT.parent


def _load_yaml_or_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    if path.suffix.lower() in {".json"}:
        return json.loads(path.read_text(encoding="utf-8"))
    if path.suffix.lower() in {".yml", ".yaml"}:
        try:
            import yaml  # type: ignore
        except ModuleNotFoundError as e:
            raise RuntimeError("PyYAML is required for .yml/.yaml config files. Use .json or install PyYAML.") from e
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("Config root must be an object/dict")
        return data
    raise ValueError("Unsupported config format. Use .yml/.yaml or .json")


_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")


def _expand_env(value: str, missing: set[str]) -> str:
    def repl(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in os.environ:
            missing.add(name)
            return ""
        return os.environ.get(name, "")

    return _ENV_PATTERN.sub(repl, value)


def _expand_env_in_config(data: Any, missing: set[str]) -> Any:
    if isinstance(data, dict):
        return {k: _expand_env_in_config(v, missing) for k, v in data.items()}
    if isinstance(data, list):
        return [_expand_env_in_config(v, missing) for v in data]
    if isinstance(data, str):
        return _expand_env(data, missing)
    return data


def _load_env_file(path: Path) -> None:
    if not path.exists() or not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        os.environ.setdefault(key, value)


def _slugify(value: str) -> str:
    value = (value or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = value.strip("-")
    return value or "site"


def _coalesce(*values: Any) -> Any:
    for v in values:
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None


def _run_cmd(cmd: str | list[str], cwd: Path | None) -> None:
    """
    Run a command without invoking a shell.

    We accept either:
    - `list[str]` (preferred)
    - `str` (parsed with shlex.split)
    """
    argv = cmd if isinstance(cmd, list) else shlex.split(cmd)
    proc = subprocess.run(argv, cwd=str(cwd) if cwd else None, check=False)
    if proc.returncode != 0:
        pretty = " ".join(shlex.quote(a) for a in argv)
        raise RuntimeError(f"Command failed ({proc.returncode}): {pretty}")

def _root_url(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, "", "", ""))


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


def _sites_from_inventory(config: dict[str, Any], config_path: Path) -> list[dict[str, Any]]:
    inv = config.get("inventory") if isinstance(config.get("inventory"), dict) else {}
    domains_csv = inv.get("domains_csv") if isinstance(inv.get("domains_csv"), str) else ""
    if not domains_csv.strip():
        return []

    delimiter = str(inv.get("delimiter") or ";")
    column = inv.get("domain_column") if isinstance(inv.get("domain_column"), str) else None
    scheme = str(inv.get("scheme") or "https").strip().lower()
    scheme = scheme if scheme in {"http", "https"} else "https"

    include = inv.get("include_domains") if isinstance(inv.get("include_domains"), list) else []
    exclude = inv.get("exclude_domains") if isinstance(inv.get("exclude_domains"), list) else []
    include_set = {str(d).strip().lower() for d in include if isinstance(d, str) and str(d).strip()}
    exclude_set = {str(d).strip().lower() for d in exclude if isinstance(d, str) and str(d).strip()}
    max_domains = int(inv.get("max_domains") or 200)

    csv_path = Path(domains_csv).expanduser()
    if not csv_path.is_absolute():
        csv_path = (config_path.parent / csv_path).resolve()
    if not csv_path.exists():
        raise FileNotFoundError(f"inventory.domains_csv not found: {csv_path}")

    domains: list[str] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        if not reader.fieldnames:
            return []
        domain_col = _pick_domain_column(list(reader.fieldnames), preferred=column)
        for row in reader:
            d = str(row.get(domain_col) or "").strip().lower().lstrip(".")
            if not d:
                continue
            d = re.sub(r"^https?://", "", d).split("/", 1)[0].strip().rstrip(".")
            if not d:
                continue
            if include_set and d not in include_set:
                continue
            if exclude_set and d in exclude_set:
                continue
            domains.append(d)
            if len(domains) >= max_domains:
                break

    seen: set[str] = set()
    sites: list[dict[str, Any]] = []
    for d in domains:
        if d in seen:
            continue
        seen.add(d)
        sites.append({"name": d, "base_url": f"{scheme}://{d}/"})
    return sites


def _load_issue_counts(report_json: Path) -> dict[str, int]:
    try:
        data = json.loads(report_json.read_text(encoding="utf-8"))
    except Exception:
        return {}
    issues = data.get("issues") if isinstance(data, dict) else None
    if not isinstance(issues, dict):
        return {}

    out: dict[str, int] = {}
    for k, v in issues.items():
        if not isinstance(v, dict):
            continue
        c = v.get("count")
        if isinstance(c, int):
            out[str(k)] = c
    return out


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


def _render_sites_summary_md(rows: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    lines.append("# SEO Autopilot — résumé multi-sites")
    lines.append("")
    lines.append(f"- Sites: **{len(rows)}**")
    lines.append("")
    lines.append("| Site | Pages | 4xx/5xx | Missing canonical | Missing H1 | Multiple H1 | Missing meta desc |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for r in rows:
        lines.append(
            f"| {r.get('site')} | {r.get('pages_crawled')} | {r.get('bad_status')} | {r.get('missing_canonical')} | {r.get('missing_h1')} | {r.get('multiple_h1')} | {r.get('missing_meta_description')} |"
        )
    lines.append("")
    return "\n".join(lines)


def _url_to_filepath(url: str, repo_path: Path, base_url: str) -> Path | None:
    """Helper to map a URL to a local file path."""
    if not repo_path or not base_url:
        return None

    def _host_no_www(raw: str) -> str:
        try:
            host = (urlsplit(raw).hostname or "").strip().lower()
        except Exception:
            host = ""
        if host.startswith("www."):
            host = host[4:]
        return host

    base_host = _host_no_www(base_url)
    try:
        parts = urlsplit(url)
        url_host = _host_no_www(url)
        if base_host and url_host and url_host != base_host:
            return None

        url_path = unquote(parts.path or "/")
        if not url_path.startswith("/"):
            url_path = "/" + url_path

        rel = Path(url_path.lstrip("/"))

        candidates: list[Path] = []
        if str(rel) in {"", "."}:
            candidates.extend([repo_path / "index.html", repo_path / "index.htm"])
        else:
            rel_str = rel.as_posix()
            if rel_str.endswith("/"):
                rel_str = rel_str.rstrip("/")

            if rel.suffix.lower() in {".html", ".htm"}:
                candidates.append(repo_path / rel)
                # Netlify-like: `/foo.html` can map to `/foo/index.html`
                no_ext = rel.with_suffix("")
                candidates.extend([repo_path / no_ext / "index.html", repo_path / no_ext / "index.htm"])
            elif rel.suffix:
                candidates.append(repo_path / rel)
            else:
                candidates.extend(
                    [
                        repo_path / rel.with_suffix(".html"),
                        repo_path / rel.with_suffix(".htm"),
                        repo_path / rel / "index.html",
                        repo_path / rel / "index.htm",
                    ]
                )

        repo_resolved = repo_path.resolve()
        for cand in candidates:
            try:
                resolved = cand.resolve()
            except Exception:
                continue
            if not resolved.is_relative_to(repo_resolved):
                continue
            if resolved.exists() and resolved.is_file():
                return resolved
    except Exception:
        return None
    return None


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip()).strip()


def _humanize_slug(value: str) -> str:
    v = _clean_text(unquote(value or "")).strip("/")
    v = re.sub(r"\.(html?|php|aspx?)$", "", v, flags=re.IGNORECASE)
    v = v.replace("-", " ").replace("_", " ")
    v = re.sub(r"\s+", " ", v).strip()
    if not v:
        return ""
    # Title-case, but keep existing uppercase words (acronyms).
    parts = []
    for w in v.split(" "):
        if len(w) > 1 and w.isupper():
            parts.append(w)
        else:
            parts.append(w[:1].upper() + w[1:])
    return " ".join(parts)


def _fit_with_brand(base: str, brand: str, *, max_len: int = 60) -> str:
    base = _clean_text(base)
    brand = _clean_text(brand)
    sep = " | "

    def _trim(s: str, limit: int) -> str:
        s = _clean_text(s)
        if len(s) <= limit:
            return s
        if limit <= 1:
            return s[:limit]
        cut = s[: max(0, limit - 1)].rstrip()
        cut = cut.rstrip(" -–—|:").rstrip()
        return (cut + "…") if cut else s[:limit]

    if brand and (brand.lower() not in base.lower()):
        allowed = max_len - len(sep) - len(brand)
        base_trimmed = _trim(base, allowed) if allowed > 0 else ""
        out = f"{base_trimmed}{sep}{brand}" if base_trimmed else _trim(brand, max_len)
    else:
        out = _trim(base, max_len)
    return out


def _suggest_unique_title(
    *,
    old_title: str,
    h1: str,
    brand_name: str,
    url: str,
    reserved: set[str],
) -> str | None:
    old_norm = _clean_text(old_title)
    h1_norm = _clean_text(h1)

    try:
        slug = _humanize_slug((urlsplit(url).path or "/").rstrip("/").split("/")[-1])
    except Exception:
        slug = ""

    candidates: list[str] = []
    for c in [h1_norm, slug, old_norm]:
        c = _clean_text(c)
        if not c:
            continue
        if c not in candidates:
            candidates.append(c)

    # If everything is empty, don't attempt.
    if not candidates:
        return None

    # Build variants: base, base + slug, slug + base.
    variants: list[str] = []
    for base in candidates:
        variants.append(base)
        if slug and slug.lower() not in base.lower():
            variants.append(f"{base} — {slug}")
            variants.append(f"{slug} — {base}")

    seen: set[str] = set()
    for base in variants:
        base = _clean_text(base)
        if not base or base in seen:
            continue
        seen.add(base)
        title = _fit_with_brand(base, brand_name, max_len=60)
        norm = title.lower()
        if old_norm and title == old_norm:
            continue
        if norm in reserved:
            continue
        reserved.add(norm)
        return title

    # Last resort: append a counter.
    base = _fit_with_brand(candidates[0], brand_name, max_len=55)
    for i in range(2, 99):
        title = _clean_text(f"{base} ({i})")
        if title.lower() not in reserved and title != old_norm:
            reserved.add(title.lower())
            return title
    return None


def _extract_text_snippet(html_content: str) -> str:
    if not isinstance(html_content, str) or not html_content.strip():
        return ""

    if BeautifulSoup is None:
        cleaned = re.sub(r"<script\\b[^>]*>.*?</script>", " ", html_content, flags=re.IGNORECASE | re.DOTALL)
        cleaned = re.sub(r"<style\\b[^>]*>.*?</style>", " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
        cleaned = re.sub(r"<[^>]+>", " ", cleaned)
        return _clean_text(cleaned)

    soup = BeautifulSoup(html_content, "html.parser")
    for tag in soup.find_all(["script", "style", "noscript"]):
        try:
            tag.decompose()
        except Exception:
            pass
    container = soup.find("main") or soup.find("article") or soup.body or soup
    try:
        for p in container.find_all("p"):
            t = _clean_text(p.get_text(" ", strip=True))
            if len(t) >= 60:
                return t
        t = _clean_text(container.get_text(" ", strip=True))
        return t
    except Exception:
        return _clean_text(soup.get_text(" ", strip=True))


def _fit_meta_description(text: str, *, max_len: int = 160) -> str:
    text = _clean_text(text)
    if len(text) <= max_len:
        return text
    if max_len <= 1:
        return text[:max_len]
    cut = text[: max(0, max_len - 1)]
    # Try not to cut mid-word.
    if " " in cut:
        cut = cut.rsplit(" ", 1)[0]
    cut = cut.rstrip(" -–—|:").rstrip()
    return (cut + "…") if cut else text[:max_len]


def _suggest_meta_description(
    *,
    filepath: Path,
    h1: str,
    brand_name: str,
    url: str,
    reserved: set[str],
) -> str | None:
    try:
        content = filepath.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return None

    snippet = _extract_text_snippet(content)
    if snippet:
        # Prefer 1–2 first sentences.
        parts = re.split(r"(?<=[.!?])\\s+", snippet)
        chosen = ""
        for s in parts:
            s = _clean_text(s)
            if not s:
                continue
            cand = f"{chosen} {s}".strip() if chosen else s
            if len(cand) > 160:
                break
            chosen = cand
            if len(chosen) >= 90:
                break
        desc = chosen or snippet
    else:
        base = _clean_text(h1) or _humanize_slug((urlsplit(url).path or "/").rstrip("/").split("/")[-1])
        if base:
            desc = f"{base} — {brand_name}".strip(" —")
        else:
            desc = str(brand_name or "").strip()

    desc = _fit_meta_description(desc, max_len=160)
    if not desc or len(desc) < 30:
        return None
    norm = desc.lower()
    if norm in reserved:
        # Add a short differentiator from the slug.
        try:
            slug = _humanize_slug((urlsplit(url).path or "/").rstrip("/").split("/")[-1])
        except Exception:
            slug = ""
        if slug and slug.lower() not in norm:
            desc2 = _fit_meta_description(f"{desc} ({slug})", max_len=160)
            if desc2.lower() not in reserved:
                reserved.add(desc2.lower())
                return desc2
        return None
    reserved.add(norm)
    return desc


def _propose_ai_corrections(report_path: Path, repo_path: Path | None, base_url: str, site_config: dict) -> None:
    """
    Analyzes a report, proposes AI-driven corrections, and saves them to a plan.
    """
    if not report_path.exists() or not repo_path:
        return

    print(f"\n[AI AGENT] Analyzing report for improvement: {report_path.as_posix()}")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    issues = report.get("issues", {})
    pages = report.get("pages", [])
    
    corrections_plan: list[dict[str, str]] = []
    brand_name = site_config.get("name", "My-Brand")

    def _page_url(p: dict[str, Any]) -> str:
        u = p.get("final_url") or p.get("url") or ""
        return str(u).strip()

    def _page_h1(p: dict[str, Any]) -> str:
        h1 = p.get("h1")
        if isinstance(h1, list) and h1:
            return str(h1[0] or "").strip()
        if isinstance(h1, str):
            return h1.strip()
        return ""

    def _plan_path(p: Path) -> str:
        try:
            return p.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
        except Exception:
            return p.resolve().as_posix()

    # Pre-index pages by title and meta description (helps handle groups quickly).
    pages_by_title: dict[str, list[dict[str, Any]]] = {}
    pages_by_md: dict[str, list[dict[str, Any]]] = {}
    missing_title_pages: list[dict[str, Any]] = []
    missing_md_pages: list[dict[str, Any]] = []

    if isinstance(pages, list):
        for p in pages:
            if not isinstance(p, dict):
                continue
            u = _page_url(p)
            if not u.startswith(("http://", "https://")):
                continue
            title = p.get("title")
            if isinstance(title, str) and title.strip():
                pages_by_title.setdefault(title.strip(), []).append(p)
            else:
                missing_title_pages.append(p)

            md = p.get("meta_description")
            if isinstance(md, str) and md.strip():
                pages_by_md.setdefault(md.strip(), []).append(p)
            else:
                missing_md_pages.append(p)

    # Reserve existing titles/meta descriptions (avoid suggesting duplicates).
    reserved_titles = {t.lower() for t in pages_by_title.keys() if isinstance(t, str) and t.strip()}
    reserved_mds = {d.lower() for d in pages_by_md.keys() if isinstance(d, str) and d.strip()}

    # --- 1) Duplicate titles -> propose unique titles for all but 1 page per group ---
    duplicate_titles_issue = issues.get("duplicate_titles") if isinstance(issues, dict) else None
    if isinstance(duplicate_titles_issue, dict) and int(duplicate_titles_issue.get("count") or 0) > 0:
        print("[AI AGENT] Found duplicate titles. Proposing corrections...")
        top = duplicate_titles_issue.get("top") if isinstance(duplicate_titles_issue.get("top"), list) else []
        # top items are [title, count]
        for row in top:
            if not (isinstance(row, (list, tuple)) and len(row) >= 1):
                continue
            title_to_fix = str(row[0] or "").strip()
            if not title_to_fix:
                continue
            group = pages_by_title.get(title_to_fix) or []
            if len(group) < 2:
                continue

            # Pick one page to keep this title (heuristic: shortest path).
            def _url_path_len(p: dict[str, Any]) -> int:
                try:
                    return len(urlsplit(_page_url(p)).path or "")
                except Exception:
                    return 10**9

            keep = sorted(group, key=lambda p: (_url_path_len(p), _page_url(p)))[0]
            for p in group:
                if p is keep:
                    continue
                url = _page_url(p)
                h1 = _page_h1(p)
                filepath = _url_to_filepath(url, repo_path, base_url)
                if not filepath:
                    print(f"[AI AGENT]    - Could not map URL to file (skip): {url}")
                    continue

                suggestion = _suggest_unique_title(
                    old_title=title_to_fix,
                    h1=h1,
                    brand_name=str(brand_name),
                    url=url,
                    reserved=reserved_titles,
                )
                if not suggestion:
                    continue

                corrections_plan.append(
                    {
                        "file_path": _plan_path(filepath),
                        "url": url,
                        "issue_type": "duplicate_title",
                        "current_value": title_to_fix,
                        "suggested_value": suggestion,
                        "ai_explanation": "Title dupliqué. Proposition basée sur H1 + URL pour garantir unicité et pertinence.",
                    }
                )

    # --- 2) Missing titles -> add a title based on H1/URL + brand ---
    if isinstance(issues, dict) and isinstance(issues.get("missing_title"), dict) and int(issues["missing_title"].get("count") or 0) > 0:
        print("[AI AGENT] Found missing titles. Proposing corrections...")
        for p in missing_title_pages:
            url = _page_url(p)
            if not url:
                continue
            h1 = _page_h1(p)
            filepath = _url_to_filepath(url, repo_path, base_url)
            if not filepath:
                continue
            suggestion = _suggest_unique_title(
                old_title="",
                h1=h1,
                brand_name=str(brand_name),
                url=url,
                reserved=reserved_titles,
            )
            if not suggestion:
                continue
            corrections_plan.append(
                {
                    "file_path": _plan_path(filepath),
                    "url": url,
                    "issue_type": "missing_title",
                    "current_value": "",
                    "suggested_value": suggestion,
                    "ai_explanation": "Title manquant. Proposition basée sur H1/URL + marque, sous ~60 caractères.",
                }
            )

    # --- 3) Duplicate meta descriptions -> propose new snippets from page content ---
    duplicate_md_issue = issues.get("duplicate_meta_descriptions") if isinstance(issues, dict) else None
    if isinstance(duplicate_md_issue, dict) and int(duplicate_md_issue.get("count") or 0) > 0:
        print("[AI AGENT] Found duplicate meta descriptions. Proposing corrections...")
        top = duplicate_md_issue.get("top") if isinstance(duplicate_md_issue.get("top"), list) else []
        for row in top:
            if not (isinstance(row, (list, tuple)) and len(row) >= 1):
                continue
            md_to_fix = str(row[0] or "").strip()
            if not md_to_fix:
                continue
            group = pages_by_md.get(md_to_fix) or []
            if len(group) < 2:
                continue
            keep = sorted(group, key=lambda p: (len(urlsplit(_page_url(p)).path or ""), _page_url(p)))[0]
            for p in group:
                if p is keep:
                    continue
                url = _page_url(p)
                h1 = _page_h1(p)
                filepath = _url_to_filepath(url, repo_path, base_url)
                if not filepath:
                    continue
                suggestion = _suggest_meta_description(
                    filepath=filepath,
                    h1=h1,
                    brand_name=str(brand_name),
                    url=url,
                    reserved=reserved_mds,
                )
                if not suggestion:
                    continue
                corrections_plan.append(
                    {
                        "file_path": _plan_path(filepath),
                        "url": url,
                        "issue_type": "duplicate_meta_description",
                        "current_value": md_to_fix,
                        "suggested_value": suggestion,
                        "ai_explanation": "Meta description dupliquée. Proposition extraite du contenu (snippet) pour éviter les doublons.",
                    }
                )

    # --- 4) Missing meta descriptions -> add a snippet-based description ---
    if isinstance(issues, dict) and isinstance(issues.get("missing_meta_description"), dict) and int(issues["missing_meta_description"].get("count") or 0) > 0:
        print("[AI AGENT] Found missing meta descriptions. Proposing corrections...")
        for p in missing_md_pages:
            url = _page_url(p)
            if not url:
                continue
            h1 = _page_h1(p)
            filepath = _url_to_filepath(url, repo_path, base_url)
            if not filepath:
                continue
            suggestion = _suggest_meta_description(
                filepath=filepath,
                h1=h1,
                brand_name=str(brand_name),
                url=url,
                reserved=reserved_mds,
            )
            if not suggestion:
                continue
            corrections_plan.append(
                {
                    "file_path": _plan_path(filepath),
                    "url": url,
                    "issue_type": "missing_meta_description",
                    "current_value": "",
                    "suggested_value": suggestion,
                    "ai_explanation": "Meta description manquante. Proposition extraite du contenu (snippet) pour un résumé pertinent.",
                }
            )

    if corrections_plan:
        plan_path = report_path.parent.parent / "corrections-plan.json"
        plan_path.write_text(json.dumps(corrections_plan, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"[AI AGENT] Saved {len(corrections_plan)} correction(s) to: {plan_path}")


def _render_backlog(
    site_name: str,
    base_url: str,
    audit_report_path: Path | None,
    gsc_reports: list[Path],
) -> str:
    lines: list[str] = []
    lines.append(f"# Backlog SEO — {site_name}")
    lines.append("")
    lines.append(f"- Site: {base_url}")
    if audit_report_path:
        lines.append(f"- Audit: {audit_report_path.as_posix()}")
    for p in gsc_reports:
        lines.append(f"- GSC: {p.as_posix()}")
    lines.append("")

    issues: dict[str, Any] = {}
    if audit_report_path and audit_report_path.exists():
        try:
            report = json.loads(audit_report_path.read_text(encoding="utf-8"))
            issues = report.get("issues", {}) if isinstance(report, dict) else {}
        except Exception:
            issues = {}

    def count(key: str) -> int:
        block = issues.get(key) or {}
        c = block.get("count")
        return int(c) if isinstance(c, int) else 0

    def examples(key: str) -> list[str]:
        block = issues.get(key) or {}
        ex = block.get("examples")
        if isinstance(ex, list):
            return [str(x) for x in ex[:10]]
        return []

    lines.append("## Priorités")
    lines.append("")

    if count("bad_status"):
        lines.append("### P0 — Corriger erreurs 4xx/5xx")
        lines.append(f"- Preuve: {count('bad_status')} URL(s) en erreur.")
        for u in examples("bad_status"):
            lines.append(f"  - {u}")
        lines.append("- Critère: URLs importantes en 200 (ou redirection propre), sans boucles.")
        lines.append("")

    if count("timed_out") or count("redirect_loop") or count("broken_redirect") or count("redirect_chain_too_long"):
        lines.append("### P0 — Crawl & redirects (fiabilité)")
        if count("timed_out"):
            lines.append(f"- Timeouts: {count('timed_out')}")
        if count("redirect_loop"):
            lines.append(f"- Redirect loops: {count('redirect_loop')}")
        if count("broken_redirect"):
            lines.append(f"- Redirects cassés: {count('broken_redirect')}")
        if count("redirect_chain_too_long"):
            lines.append(f"- Chaînes trop longues: {count('redirect_chain_too_long')}")
        lines.append("- Critère: crawl stable (pas de timeouts/loops) + redirects courts et propres.")
        lines.append("")

    if count("image_broken") or count("javascript_broken") or count("css_broken"):
        lines.append("### P0 — Assets cassés (images / JS / CSS)")
        if count("image_broken"):
            lines.append(f"- Images cassées: {count('image_broken')} (pages impactées: {count('page_has_broken_image')})")
        if count("javascript_broken"):
            lines.append(f"- JavaScript cassé: {count('javascript_broken')} (pages impactées: {count('page_has_broken_javascript')})")
        if count("css_broken"):
            lines.append(f"- CSS cassé: {count('css_broken')} (pages impactées: {count('page_has_broken_css')})")
        lines.append("- Critère: 0 asset 4xx/5xx, pas de redirects inutiles sur assets.")
        lines.append("")

    if count("blocked_by_robots"):
        lines.append("### P0 — Vérifier blocage robots.txt (si non voulu)")
        lines.append(f"- Preuve: {count('blocked_by_robots')} URL(s) bloquées.")
        for u in examples("blocked_by_robots"):
            lines.append(f"  - {u}")
        lines.append("- Critère: règles cohérentes avec l’indexation attendue.")
        lines.append("")

    if count("missing_title") or count("duplicate_titles"):
        lines.append("### P1 — Titles (manquants / dupliqués)")
        if count("missing_title"):
            lines.append(f"- Manquants: {count('missing_title')}")
        if count("duplicate_titles"):
            lines.append(f"- Dupliqués (nb de titles): {count('duplicate_titles')}")
        lines.append("- Critère: 1 title unique par page, orienté intention.")
        lines.append("")

    if count("missing_meta_description") or count("duplicate_meta_descriptions"):
        lines.append("### P2 — Meta descriptions (CTR)")
        if count("missing_meta_description"):
            lines.append(f"- Manquantes: {count('missing_meta_description')}")
        if count("duplicate_meta_descriptions"):
            lines.append(f"- Dupliquées (nb de descriptions): {count('duplicate_meta_descriptions')}")
        lines.append("- Critère: descriptions uniques sur les pages où le CTR est un enjeu (GSC).")
        lines.append("")

    if count("missing_h1") or count("multiple_h1"):
        lines.append("### P1 — H1 (structure)")
        if count("missing_h1"):
            lines.append(f"- Manquants: {count('missing_h1')}")
        if count("multiple_h1"):
            lines.append(f"- Multiples: {count('multiple_h1')}")
        lines.append("- Critère: 1 H1 descriptif + hiérarchie H2/H3 logique.")
        lines.append("")

    if count("missing_canonical"):
        lines.append("### P1 — Canonicals")
        lines.append(f"- Manquants: {count('missing_canonical')}")
        lines.append("- Critère: canonical présent et cohérent (facettes/pagination/paramètres).")
        lines.append("")

    if count("canonical_points_to_4xx") or count("canonical_points_to_5xx") or count("canonical_points_to_redirect"):
        lines.append("### P1 — Canonicals cassés / incohérents")
        if count("canonical_points_to_4xx"):
            lines.append(f"- Canonical → 4xx: {count('canonical_points_to_4xx')}")
        if count("canonical_points_to_5xx"):
            lines.append(f"- Canonical → 5xx: {count('canonical_points_to_5xx')}")
        if count("canonical_points_to_redirect"):
            lines.append(f"- Canonical → redirect: {count('canonical_points_to_redirect')}")
        lines.append("- Critère: canonical pointe vers une URL 200, non redirigée, canonique.")
        lines.append("")

    if count("https_http_mixed_content"):
        lines.append("### P1 — Mixed content (HTTPS → HTTP)")
        lines.append(f"- Pages concernées: {count('https_http_mixed_content')}")
        lines.append("- Critère: aucun lien/asset HTTP depuis une page HTTPS.")
        lines.append("")

    if count("orphan_page") or count("page_has_links_to_broken_page") or count("page_has_links_to_redirect"):
        lines.append("### P1 — Maillage interne (qualité)")
        if count("orphan_page"):
            lines.append(f"- Pages orphelines (0 lien entrant): {count('orphan_page')}")
        if count("page_has_links_to_broken_page"):
            lines.append(f"- Pages qui lient vers 4xx/5xx: {count('page_has_links_to_broken_page')}")
        if count("page_has_links_to_redirect"):
            lines.append(f"- Pages qui lient vers redirect: {count('page_has_links_to_redirect')}")
        if count("canonical_url_has_no_incoming_internal_links"):
            lines.append(f"- Canonicals sans lien entrant: {count('canonical_url_has_no_incoming_internal_links')}")
        lines.append("- Critère: pas de liens cassés/redirect en interne + pages importantes non orphelines.")
        lines.append("")

    if count("open_graph_tags_missing") or count("twitter_card_missing"):
        lines.append("### P2 — Social tags (OpenGraph / Twitter)")
        if count("open_graph_tags_missing"):
            lines.append(f"- OpenGraph manquants: {count('open_graph_tags_missing')}")
        if count("twitter_card_missing"):
            lines.append(f"- Twitter card manquante: {count('twitter_card_missing')}")
        lines.append("- Critère: OG/Twitter complets sur les pages partagées (blog, landing, produit).")
        lines.append("")

    if gsc_reports:
        lines.append("### P1 — Quick wins GSC (CTR / positions 3–10)")
        lines.append("- Action: réécrire titles/meta + enrichir snippets (schema) + maillage interne.")
        lines.append("")

    lines.append("## Maillage inter-sites (tes domaines uniquement)")
    lines.append("- Générer un plan de liens pertinents entre tes sites (pas d’outreach).")
    lines.append("- Voir `references/interlinking-between-sites.md`.")
    lines.append("")

    return "\n".join(lines)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SEO Autopilot orchestrator (audit + GSC + backlog + optional deploy).")
    p.add_argument("--config", default="seo-autopilot.yml", help="Path to config (.yml/.yaml/.json).")
    p.add_argument("--site", help="Run only one site (match by name or slug).")
    p.add_argument("--mode", choices=["audit-only", "execute"], help="Override config autopilot.mode.")
    p.add_argument("--auto-deploy", action="store_true", help="Override config autopilot.auto_deploy=true.")
    p.add_argument("--no-auto-deploy", action="store_true", help="Override config autopilot.auto_deploy=false.")
    p.add_argument("--execute", action="store_true", help="Actually run deploy commands (dangerous).")
    p.add_argument("--no-audit", action="store_true", help="Skip crawl audit.")
    p.add_argument("--no-gsc", action="store_true", help="Skip GSC analysis.")
    p.add_argument("--no-backlog", action="store_true", help="Skip backlog generation.")
    return p.parse_args(argv)


def main(argv: list[str]) -> int:
    args = _parse_args(argv)
    config_path = Path(args.config).resolve()

    raw_config = _load_yaml_or_json(config_path)
    env_files = raw_config.get("secrets", {}).get("env_files", []) if isinstance(raw_config.get("secrets"), dict) else []
    if isinstance(env_files, list):
        for ef in env_files:
            if isinstance(ef, str) and ef.strip():
                _load_env_file((config_path.parent / ef).resolve())

    missing: set[str] = set()
    config = _expand_env_in_config(raw_config, missing)

    required_env = config.get("secrets", {}).get("required_env", []) if isinstance(config.get("secrets"), dict) else []
    if isinstance(required_env, list):
        for name in required_env:
            if isinstance(name, str) and name and name not in os.environ:
                missing.add(name)

    if missing:
        missing_list = ", ".join(sorted(missing))
        print(f"[ERROR] Missing env var(s): {missing_list}", file=sys.stderr)
        return 2

    runs_dir = Path(_coalesce(config.get("output", {}).get("runs_dir") if isinstance(config.get("output"), dict) else None, "seo-runs"))
    runs_dir = (config_path.parent / runs_dir).resolve()
    runs_dir.mkdir(parents=True, exist_ok=True)

    timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")

    sites_from_cfg = config.get("sites") if isinstance(config.get("sites"), list) else []
    try:
        sites_from_inv = _sites_from_inventory(config, config_path=config_path)
    except Exception as e:
        print(f"[ERROR] inventory.domains_csv: {type(e).__name__}: {e}", file=sys.stderr)
        return 2

    # Merge inventory + config sites (dedupe by slug). Prefer explicit config entries as overrides.
    sites_by_slug: dict[str, dict[str, Any]] = {}
    ordered_slugs: list[str] = []

    def site_slug(site: dict[str, Any]) -> str:
        name = str(_coalesce(site.get("name"), site.get("base_url"), "site"))
        return _slugify(name)

    for s in sites_from_inv:
        if not isinstance(s, dict):
            continue
        slug = site_slug(s)
        if slug not in sites_by_slug:
            sites_by_slug[slug] = dict(s)
            ordered_slugs.append(slug)

    for s in sites_from_cfg:
        if not isinstance(s, dict):
            continue
        slug = site_slug(s)
        if slug in sites_by_slug:
            merged = dict(sites_by_slug[slug])
            merged.update(s)
            sites_by_slug[slug] = merged
        else:
            sites_by_slug[slug] = dict(s)
            ordered_slugs.append(slug)

    sites: list[dict[str, Any]] = [sites_by_slug[slug] for slug in ordered_slugs if slug in sites_by_slug]

    if not sites:
        print("[ERROR] No sites configured. Provide config.sites or inventory.domains_csv.", file=sys.stderr)
        return 2

    autopilot_cfg = config.get("autopilot") if isinstance(config.get("autopilot"), dict) else {}
    autopilot_mode = str(
        _coalesce(
            args.mode,
            autopilot_cfg.get("mode") if isinstance(autopilot_cfg, dict) else None,
            "audit-only",
        )
    )
    auto_deploy = bool(autopilot_cfg.get("auto_deploy")) if isinstance(autopilot_cfg, dict) else False
    if args.auto_deploy:
        auto_deploy = True
    if args.no_auto_deploy:
        auto_deploy = False

    defaults = config.get("defaults") if isinstance(config.get("defaults"), dict) else {}
    crawl_defaults = defaults.get("crawl") if isinstance(defaults.get("crawl"), dict) else {}
    gsc_defaults = defaults.get("gsc") if isinstance(defaults.get("gsc"), dict) else {}
    bing_defaults = defaults.get("bing") if isinstance(defaults.get("bing"), dict) else {}
    gsc_api_defaults = defaults.get("gsc_api") if isinstance(defaults.get("gsc_api"), dict) else {}

    try:
        import seo_audit
        import gsc_analyze_csv
        import interlinking_plan
    except Exception as e:
        print(f"[ERROR] Unable to import local scripts: {type(e).__name__}: {e}", file=sys.stderr)
        return 2

    any_failed = False
    audit_reports_for_interlinking: list[Path] = []

    selected_sites: list[tuple[dict[str, Any], str, str, str]] = []
    for site in sites:
        if not isinstance(site, dict):
            continue
        name = str(_coalesce(site.get("name"), site.get("base_url"), "site"))
        base_url = str(_coalesce(site.get("base_url"), "")).strip()
        if not base_url:
            continue
        slug = _slugify(name)
        if args.site and args.site not in {name, slug}:
            continue
        selected_sites.append((site, name, base_url, slug))

    total_sites = len(selected_sites)
    for idx, (site, name, base_url, slug) in enumerate(selected_sites, start=1):
        print(f"[AUTOPILOT] site {idx}/{total_sites}: {name}", flush=True)

        site_dir = runs_dir / slug / timestamp
        site_dir.mkdir(parents=True, exist_ok=True)

        deploy_cfg = {}

        run_meta = {
            "site_name": name,
            "base_url": base_url,
            "timestamp": timestamp,
            "config_path": str(config_path),
            "autopilot_mode": autopilot_mode,
        }
        (site_dir / "run.json").write_text(json.dumps(run_meta, ensure_ascii=False, indent=2), encoding="utf-8")

        audit_dir = site_dir / "audit"
        gsc_dir = site_dir / "gsc"
        bing_dir = site_dir / "bing"

        audit_report_json: Path | None = None
        gsc_reports: list[Path] = []

        if not args.no_audit:
            audit_dir.mkdir(parents=True, exist_ok=True)
            crawl_cfg = site.get("crawl") if isinstance(site.get("crawl"), dict) else {}
            max_pages = int(_coalesce(crawl_cfg.get("max_pages"), crawl_defaults.get("max_pages"), 300))
            workers = int(_coalesce(crawl_cfg.get("workers"), crawl_defaults.get("workers"), 6))
            timeout_s = float(_coalesce(crawl_cfg.get("timeout_s"), crawl_defaults.get("timeout_s"), 15))
            ignore_robots = bool(_coalesce(crawl_cfg.get("ignore_robots"), crawl_defaults.get("ignore_robots"), False))
            allow_subdomains = bool(_coalesce(crawl_cfg.get("allow_subdomains"), crawl_defaults.get("allow_subdomains"), False))
            include_regex = _coalesce(crawl_cfg.get("include_regex"), crawl_defaults.get("include_regex"))
            exclude_regex = _coalesce(crawl_cfg.get("exclude_regex"), crawl_defaults.get("exclude_regex"))
            user_agent = str(_coalesce(crawl_cfg.get("user_agent"), crawl_defaults.get("user_agent"), "SEOAutopilot/1.0"))
            check_resources = bool(_coalesce(crawl_cfg.get("check_resources"), crawl_defaults.get("check_resources"), True))
            max_resources = int(_coalesce(crawl_cfg.get("max_resources"), crawl_defaults.get("max_resources"), 250))
            pagespeed = bool(_coalesce(crawl_cfg.get("pagespeed"), crawl_defaults.get("pagespeed"), False))
            pagespeed_strategy = str(_coalesce(crawl_cfg.get("pagespeed_strategy"), crawl_defaults.get("pagespeed_strategy"), "mobile"))
            pagespeed_max_urls = int(_coalesce(crawl_cfg.get("pagespeed_max_urls"), crawl_defaults.get("pagespeed_max_urls"), 50))
            pagespeed_timeout_s = float(_coalesce(crawl_cfg.get("pagespeed_timeout_s"), crawl_defaults.get("pagespeed_timeout_s"), 60))
            pagespeed_workers = int(_coalesce(crawl_cfg.get("pagespeed_workers"), crawl_defaults.get("pagespeed_workers"), 2))

            gsc_api_cfg = site.get("gsc_api") if isinstance(site.get("gsc_api"), dict) else {}
            gsc_api_enabled = (not args.no_gsc) and bool(_coalesce(gsc_api_cfg.get("enabled"), gsc_api_defaults.get("enabled"), False))
            gsc_days = int(_coalesce(gsc_api_cfg.get("days"), gsc_api_defaults.get("days"), 28))
            gsc_search_type = str(_coalesce(gsc_api_cfg.get("search_type"), gsc_api_defaults.get("search_type"), "web"))
            gsc_property = str(_coalesce(gsc_api_cfg.get("property_url") if isinstance(gsc_api_cfg, dict) else None, "") or "").strip()
            gsc_cfg = site.get("gsc") if isinstance(site.get("gsc"), dict) else {}
            gsc_min_impressions = int(_coalesce(gsc_cfg.get("min_impressions"), gsc_defaults.get("min_impressions"), 200))

            bing_cfg = site.get("bing") if isinstance(site.get("bing"), dict) else {}
            bing_enabled = bool(_coalesce(bing_cfg.get("enabled"), bing_defaults.get("enabled"), False))
            bing_min_impressions = int(_coalesce(bing_cfg.get("min_impressions"), bing_defaults.get("min_impressions"), 200))
            bing_days = int(_coalesce(bing_cfg.get("days"), bing_defaults.get("days"), 28))
            bing_site_url = str(_coalesce(bing_cfg.get("site_url"), "") or "").strip()
            bing_queries_csv = str(_coalesce(bing_cfg.get("queries_csv"), "") or "").strip()
            bing_pages_csv = str(_coalesce(bing_cfg.get("pages_csv"), "") or "").strip()
            bing_fetch_crawl_issues = bool(_coalesce(bing_cfg.get("fetch_crawl_issues"), bing_defaults.get("fetch_crawl_issues"), True))
            bing_fetch_blocked_urls = bool(_coalesce(bing_cfg.get("fetch_blocked_urls"), bing_defaults.get("fetch_blocked_urls"), True))
            bing_urlinfo_max = int(_coalesce(bing_cfg.get("urlinfo_max"), bing_defaults.get("urlinfo_max"), 0))

            argv_audit = [
                base_url,
                "--max-pages",
                str(max_pages),
                "--workers",
                str(workers),
                "--timeout",
                str(timeout_s),
                "--user-agent",
                user_agent,
                "--output-dir",
                str(audit_dir),
            ]
            if ignore_robots:
                argv_audit.append("--ignore-robots")
            if allow_subdomains:
                argv_audit.append("--allow-subdomains")
            if isinstance(include_regex, str) and include_regex.strip():
                argv_audit.extend(["--include", include_regex.strip()])
            if isinstance(exclude_regex, str) and exclude_regex.strip():
                argv_audit.extend(["--exclude", exclude_regex.strip()])
            if check_resources:
                argv_audit.append("--check-resources")
                argv_audit.extend(["--max-resources", str(max(0, max_resources))])
            if pagespeed:
                argv_audit.append("--pagespeed")
                argv_audit.extend(["--pagespeed-strategy", pagespeed_strategy.strip().lower() or "mobile"])
                argv_audit.extend(["--pagespeed-max-urls", str(max(0, pagespeed_max_urls))])
                argv_audit.extend(["--pagespeed-timeout", str(max(1.0, float(pagespeed_timeout_s)))])
                argv_audit.extend(["--pagespeed-workers", str(max(1, pagespeed_workers))])
            if gsc_api_enabled:
                gsc_dir.mkdir(parents=True, exist_ok=True)
                argv_audit.append("--gsc-api")
                if gsc_property:
                    argv_audit.extend(["--gsc-property", gsc_property])
                argv_audit.extend(["--gsc-days", str(max(1, gsc_days))])
                argv_audit.extend(["--gsc-search-type", gsc_search_type.strip().lower() or "web"])
                argv_audit.extend(["--gsc-min-impressions", str(max(0, gsc_min_impressions))])
                argv_audit.extend(["--gsc-output-dir", str(gsc_dir)])

            if bing_enabled:
                bing_dir.mkdir(parents=True, exist_ok=True)
                argv_audit.append("--bing")
                argv_audit.extend(["--bing-min-impressions", str(max(0, bing_min_impressions))])
                argv_audit.extend(["--bing-days", str(max(1, bing_days))])
                if bing_site_url:
                    argv_audit.extend(["--bing-site-url", bing_site_url])
                argv_audit.extend(["--bing-output-dir", str(bing_dir)])
                if not bing_fetch_crawl_issues:
                    argv_audit.append("--bing-no-crawl-issues")
                if not bing_fetch_blocked_urls:
                    argv_audit.append("--bing-no-blocked-urls")
                if bing_urlinfo_max > 0:
                    argv_audit.extend(["--bing-urlinfo-max", str(max(0, bing_urlinfo_max))])
                if bing_queries_csv:
                    argv_audit.extend(["--bing-queries-csv", bing_queries_csv])
                if bing_pages_csv:
                    argv_audit.extend(["--bing-pages-csv", bing_pages_csv])

            try:
                seo_audit.main(argv_audit)
                audit_report_json = audit_dir / "report.json"
                if audit_report_json.exists():
                    audit_reports_for_interlinking.append(audit_report_json)
            except Exception as e:
                any_failed = True
                print(f"[WARN] Audit failed for {name}: {type(e).__name__}: {e}", file=sys.stderr)

        if not args.no_gsc:
            gsc_dir.mkdir(parents=True, exist_ok=True)
            gsc_cfg = site.get("gsc") if isinstance(site.get("gsc"), dict) else {}
            min_impressions = int(_coalesce(gsc_cfg.get("min_impressions"), gsc_defaults.get("min_impressions"), 200))
            csv_paths = site.get("gsc_csv", [])

            gsc_api_cfg = site.get("gsc_api") if isinstance(site.get("gsc_api"), dict) else {}
            gsc_api_enabled = bool(_coalesce(gsc_api_cfg.get("enabled"), gsc_api_defaults.get("enabled"), False))
            if gsc_api_enabled:
                # The crawl (`seo_audit.py`) already fetched GSC API CSVs when enabled; reuse them here for analysis/backlog.
                queries_csv = gsc_dir / "gsc-queries.csv"
                pages_csv = gsc_dir / "gsc-pages.csv"
                api_csvs: list[str] = []
                if queries_csv.exists():
                    api_csvs.append(str(queries_csv))
                if pages_csv.exists():
                    api_csvs.append(str(pages_csv))
                if api_csvs:
                    extra = [str(p) for p in (csv_paths or [])] if isinstance(csv_paths, list) else []
                    csv_paths = [*api_csvs, *extra]
            if isinstance(csv_paths, list):
                for i, raw_path in enumerate(csv_paths):
                    if not isinstance(raw_path, str) or not raw_path.strip():
                        continue
                    csv_path = Path(raw_path).expanduser()
                    if not csv_path.is_absolute():
                        csv_path = (config_path.parent / csv_path).resolve()
                    if not csv_path.exists():
                        continue
                    out_path = gsc_dir / f"gsc-{i+1}.md"
                    try:
                        gsc_analyze_csv.main([str(csv_path), "--min-impressions", str(min_impressions), "--output", str(out_path)])
                        gsc_reports.append(out_path)
                    except Exception as e:
                        any_failed = True
                        print(f"[WARN] GSC analysis failed for {name}: {type(e).__name__}: {e}", file=sys.stderr)

        deploy_cfg = site.get("deploy") if isinstance(site.get("deploy"), dict) else {}
        deploy_cmds = deploy_cfg.get("commands", []) if deploy_cfg else []
        deploy_cwd_raw = _coalesce(deploy_cfg.get("working_dir"), site.get("repo_path"))
        deploy_cwd = Path(str(deploy_cwd_raw)).expanduser().resolve() if isinstance(deploy_cwd_raw, str) and deploy_cwd_raw else None

        if not args.no_backlog:
            backlog = _render_backlog(
                site_name=name,
                base_url=base_url,
                audit_report_path=audit_report_json,
                gsc_reports=gsc_reports,
            )
            (site_dir / "backlog.md").write_text(backlog, encoding="utf-8")
            print(f"[OK] Backlog: {(site_dir / 'backlog.md').as_posix()}")

        if audit_report_json:
            repo_path_for_corrections = deploy_cwd
            _propose_ai_corrections(audit_report_json, repo_path_for_corrections, base_url, site)

        if autopilot_mode == "execute" and auto_deploy and args.execute and deploy_cmds:
            try:
                for cmd in deploy_cmds:
                    if not isinstance(cmd, str) or not cmd.strip():
                        continue
                    _run_cmd(cmd, cwd=deploy_cwd)
                print(f"[OK] Deploy completed for {name}")
            except Exception as e:
                any_failed = True
                print(f"[WARN] Deploy failed for {name}: {type(e).__name__}: {e}", file=sys.stderr)
        else:
            if deploy_cmds and autopilot_mode == "execute" and auto_deploy and not args.execute:
                print(f"[INFO] Deploy is configured for {name} but skipped (run with --execute).")

    inter_cfg = config.get("interlinking") if isinstance(config.get("interlinking"), dict) else {}
    inter_enabled = bool(_coalesce(inter_cfg.get("enabled") if isinstance(inter_cfg, dict) else None, True))
    if audit_reports_for_interlinking:
        global_dir = runs_dir / "_global" / timestamp
        global_dir.mkdir(parents=True, exist_ok=True)

        # Write multi-site summary.
        summary_rows: list[dict[str, Any]] = []
        for rp in audit_reports_for_interlinking:
            site = rp.parents[2].name
            counts = _load_issue_counts(rp)
            try:
                data = json.loads(rp.read_text(encoding="utf-8"))
                meta = data.get("meta") if isinstance(data, dict) else {}
                pages_crawled = meta.get("pages_crawled") if isinstance(meta, dict) else None
            except Exception:
                pages_crawled = None
            summary_rows.append(
                {
                    "site": site,
                    "pages_crawled": int(pages_crawled) if isinstance(pages_crawled, int) else "",
                    "bad_status": counts.get("bad_status", 0),
                    "blocked_by_robots": counts.get("blocked_by_robots", 0),
                    "missing_title": counts.get("missing_title", 0),
                    "missing_meta_description": counts.get("missing_meta_description", 0),
                    "missing_h1": counts.get("missing_h1", 0),
                    "multiple_h1": counts.get("multiple_h1", 0),
                    "missing_canonical": counts.get("missing_canonical", 0),
                }
            )
        summary_rows.sort(key=lambda r: (-int(r["bad_status"] or 0), str(r["site"])))
        _write_csv(global_dir / "sites-summary.csv", summary_rows)
        (global_dir / "sites-summary.md").write_text(_render_sites_summary_md(summary_rows), encoding="utf-8")
        print(f"[OK] Summary: {(global_dir / 'sites-summary.md').as_posix()}")

        if inter_enabled and len(audit_reports_for_interlinking) >= 2 and not args.no_audit:
            try:
                argv_inter: list[str] = ["--output-dir", str(global_dir)]
                min_score = inter_cfg.get("min_score") if isinstance(inter_cfg.get("min_score"), (int, float)) else None
                per_page = inter_cfg.get("per_page") if isinstance(inter_cfg.get("per_page"), int) else None
                max_inbound = inter_cfg.get("max_inbound") if isinstance(inter_cfg.get("max_inbound"), int) else None
                if min_score is not None:
                    argv_inter.extend(["--min-score", str(float(min_score))])
                if per_page is not None:
                    argv_inter.extend(["--per-page", str(int(per_page))])
                if max_inbound is not None:
                    argv_inter.extend(["--max-inbound", str(int(max_inbound))])
                for rp in audit_reports_for_interlinking:
                    argv_inter.extend(["--report-json", str(rp)])
                interlinking_plan.main(argv_inter)
            except Exception as e:
                any_failed = True
                print(f"[WARN] interlinking plan failed: {type(e).__name__}: {e}", file=sys.stderr)

    return 1 if any_failed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
