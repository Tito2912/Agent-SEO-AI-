#!/usr/bin/env python3
"""
SEO Autopilot - Lightweight technical/on-page crawl audit.

Goal: crawl a site (starting from base URL and optionally sitemaps), extract
SEO-relevant signals, and produce a JSON + Markdown report.

This script intentionally avoids non-stdlib HTML parsers. It uses Python's
html.parser for portability; install richer parsers only if you extend it.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import datetime as dt
import gzip
import ipaddress
import json
import os
import re
import socket
import ssl
import sys
import threading
import time
from collections import Counter, defaultdict, deque
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urldefrag, urlsplit, urlunsplit
from xml.etree import ElementTree

import requests
import zipfile
from io import BytesIO, TextIOWrapper
import csv


@dataclasses.dataclass(frozen=True)
class CrawlConfig:
    base_url: str
    max_pages: int
    max_sitemap_urls: int
    timeout_s: float
    workers: int
    user_agent: str
    ignore_robots: bool
    allow_subdomains: bool
    include_re: re.Pattern[str] | None
    exclude_re: re.Pattern[str] | None
    sitemap_urls: list[str]
    output_dir: str
    check_resources: bool
    max_resources: int
    pagespeed_enabled: bool = False
    pagespeed_strategy: str = "mobile"
    pagespeed_max_urls: int = 50
    pagespeed_timeout_s: float = 60.0
    pagespeed_workers: int = 2
    pagespeed_api_key: str | None = None
    gsc_api_enabled: bool = False
    gsc_property_url: str | None = None
    gsc_days: int = 28
    gsc_search_type: str = "web"
    gsc_row_limit: int = 25000
    gsc_timeout_s: float = 30.0
    gsc_output_dir: str | None = None
    gsc_credentials: str | None = None
    gsc_min_impressions: int = 200
    gsc_inspection_enabled: bool = False
    gsc_inspection_max_urls: int = 0
    gsc_inspection_timeout_s: float = 30.0
    gsc_inspection_language: str | None = None

    # Optional: Bing search performance (CSV exports).
    bing_enabled: bool = False
    bing_queries_csv: str | None = None
    bing_pages_csv: str | None = None
    bing_min_impressions: int = 200
    bing_output_dir: str | None = None
    bing_site_url: str | None = None
    bing_days: int = 28
    bing_timeout_s: float = 30.0
    bing_api_key: str | None = None
    bing_access_token: str | None = None
    bing_fetch_crawl_issues: bool = True
    bing_fetch_blocked_urls: bool = True
    bing_fetch_sitemaps: bool = True
    bing_urlinfo_max: int = 0
    # HTTP behavior tuning (for parity with external crawlers)
    http_retries: int = 2  # retries after the first attempt (default: 2 => up to 3 attempts total)
    connection_close: bool = True  # send "Connection: close" (reduces flaky keep-alives)
    discover_canonicals: bool = True
    discover_hreflang: bool = True
    strict_link_counts: bool = False  # when True, count "* - links" per occurrence (Ahrefs-like)
    profile: str = "default"


def _write_issue_rows(issues_dir: Path | None, issue_key: str, rows: list[Any]) -> None:
    """
    Store full issue rows (the "real" affected URLs/rows) as a separate artifact file.

    This keeps `report.json` compact while allowing the UI to show complete lists with pagination.
    """
    if not issues_dir:
        return
    key = re.sub(r"[^a-zA-Z0-9_.-]+", "_", (issue_key or "").strip()) or "issue"
    max_rows = int(os.getenv("SEO_AUDIT_ISSUE_ROWS_MAX", "100000") or 100000)
    safe_rows = rows[: max(0, max_rows)]

    # Ensure JSON serializable; fall back to strings when needed.
    try:
        payload = json.dumps(safe_rows, ensure_ascii=False, indent=2)
    except TypeError:
        payload = json.dumps([str(x) for x in safe_rows], ensure_ascii=False, indent=2)

    try:
        issues_dir.mkdir(parents=True, exist_ok=True)
        (issues_dir / f"{key}.json").write_text(payload + "\n", encoding="utf-8")
    except Exception:
        return


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




def _perf_items_from_api_rows(rows: list[dict[str, Any]], *, dim: str) -> list[dict[str, Any]]:
    def as_int(value: Any) -> int:
        if isinstance(value, bool):
            return 0
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(round(value))
        if isinstance(value, str):
            try:
                return int(float(value))
            except ValueError:
                return 0
        return 0

    def as_float(value: Any) -> float:
        if isinstance(value, bool):
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                return 0.0
        return 0.0

    out: list[dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        keys = r.get("keys") if isinstance(r.get("keys"), list) else []
        dimension = str(keys[0]) if keys else ""
        out.append(
            {
                dim: dimension,
                "clicks": as_int(r.get("clicks")),
                "impressions": as_int(r.get("impressions")),
                "ctr": as_float(r.get("ctr")),
                "position": as_float(r.get("position")),
            }
        )
    return out


def _perf_opportunities(
    items: list[dict[str, Any]],
    *,
    dim: str,
    min_impressions: int,
    pos_min: float,
    pos_max: float,
    limit: int = 25,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        impressions = it.get("impressions")
        clicks = it.get("clicks")
        ctr = it.get("ctr")
        position = it.get("position")
        if not isinstance(impressions, int) or not isinstance(clicks, int):
            continue
        if not isinstance(ctr, (int, float)) or not isinstance(position, (int, float)):
            continue
        if impressions < int(min_impressions):
            continue
        if not (float(pos_min) <= float(position) <= float(pos_max)):
            continue
        exp_ctr = _expected_ctr(float(position))
        missing_clicks = (impressions * exp_ctr) - clicks
        if missing_clicks <= 0:
            continue
        entry = dict(it)
        entry["expected_ctr"] = round(exp_ctr, 6)
        entry["missing_clicks"] = float(missing_clicks)
        out.append(entry)
    out.sort(key=lambda d: (-float(d.get("missing_clicks") or 0.0), -int(d.get("impressions") or 0)))
    return out[: max(0, int(limit))]


def _issue_block_from_opps(opps: list[dict[str, Any]], *, dim: str, normalize_url: bool) -> dict[str, Any]:
    examples: list[dict[str, Any]] = []
    for o in opps[:10]:
        if not isinstance(o, dict):
            continue
        ex = dict(o)
        if normalize_url:
            url = str(ex.get(dim) or "").strip()
            if url:
                ex["url"] = url
        examples.append(ex)
    return {"count": len(opps), "examples": examples}


def _csv_rows(path: Path) -> list[dict[str, Any]]:
    import csv

    if not path.exists() or not path.is_file():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return []
        return [r for r in reader if isinstance(r, dict)]


def _detect_perf_columns(headers: list[str]) -> dict[str, str]:
    def norm(h: str) -> str:
        return re.sub(r"\\s+", " ", (h or "").strip().lower())

    normalized = {norm(h): h for h in headers}

    def pick(candidates: list[str]) -> str | None:
        for c in candidates:
            if c in normalized:
                return normalized[c]
        return None

    dimension = pick(
        [
            "top queries",
            "requêtes principales",
            "query",
            "keyword",
            "mot-clé",
            "mot cle",
            "top pages",
            "pages principales",
            "page",
            "url",
        ]
    )
    clicks = pick(["clicks", "clics"])
    impressions = pick(["impressions", "impr."])
    ctr = pick(["ctr", "taux de clics", "click-through rate", "click through rate", "ctr (%)", "ctr %"])
    position = pick(["position", "average position", "position moyenne", "avg position", "average pos."])

    if not dimension:
        dimension = headers[0] if headers else None

    colmap: dict[str, str] = {}
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


def _to_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(round(value))
    s = str(value or "").strip().replace("\\u202f", "").replace("\\xa0", "").replace(" ", "")
    if not s:
        return 0
    s = s.replace(",", ".")
    try:
        return int(float(s))
    except ValueError:
        return 0


def _to_float(value: Any) -> float:
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value or "").strip().replace("\\u202f", "").replace("\\xa0", "").replace(" ", "")
    if not s:
        return 0.0
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _to_ctr(value: Any) -> float:
    s = str(value or "").strip().replace("\\u202f", "").replace("\\xa0", "").replace(" ", "")
    if not s:
        return 0.0
    s = s.replace(",", ".")
    if s.endswith("%"):
        return max(0.0, min(1.0, _to_float(s[:-1]) / 100.0))
    x = _to_float(s)
    if x > 1.0:
        x = x / 100.0
    return max(0.0, min(1.0, x))


def _perf_items_from_csv(path: Path, *, dim: str) -> list[dict[str, Any]]:
    rows = _csv_rows(path)
    if not rows:
        return []
    headers = list(rows[0].keys())
    col = _detect_perf_columns(headers)
    dcol = col.get("dimension") or headers[0]
    out: list[dict[str, Any]] = []
    for r in rows:
        dimension = str(r.get(dcol) or "").strip()
        if not dimension:
            continue
        out.append(
            {
                dim: dimension,
                "clicks": _to_int(r.get(col.get("clicks", ""), 0)),
                "impressions": _to_int(r.get(col.get("impressions", ""), 0)),
                "ctr": _to_ctr(r.get(col.get("ctr", ""), 0)),
                "position": _to_float(r.get(col.get("position", ""), 0)),
            }
        )
    return out


def _summarize_perf_items(items: list[dict[str, Any]], *, dim: str) -> dict[str, Any]:
    total_clicks = 0
    total_impressions = 0
    weighted_pos = 0.0
    for it in items:
        if not isinstance(it, dict):
            continue
        clicks = it.get("clicks")
        impressions = it.get("impressions")
        position = it.get("position")
        if isinstance(clicks, int):
            total_clicks += clicks
        if isinstance(impressions, int):
            total_impressions += impressions
        if isinstance(impressions, int) and isinstance(position, (int, float)):
            weighted_pos += float(position) * impressions

    avg_ctr = (total_clicks / total_impressions) if total_impressions else 0.0
    avg_position = (weighted_pos / total_impressions) if total_impressions else 0.0
    items_sorted = [it for it in items if isinstance(it, dict)]
    items_sorted.sort(key=lambda d: (-int(d.get("impressions") or 0), -int(d.get("clicks") or 0), str(d.get(dim) or "")))
    return {
        "rows": len(items_sorted),
        "total_clicks": total_clicks,
        "total_impressions": total_impressions,
        "avg_ctr": round(avg_ctr, 6),
        "avg_position": round(avg_position, 4),
        "top": items_sorted[:10],
    }


@dataclasses.dataclass
class PageData:
    url: str
    final_url: str | None = None
    status_code: int | None = None
    content_type: str | None = None
    fetched_at: str | None = None
    error: str | None = None
    redirect_chain: list[str] = dataclasses.field(default_factory=list)
    redirect_statuses: list[int] = dataclasses.field(default_factory=list)
    x_robots_tag: str | None = None
    content_encoding: str | None = None
    response_bytes: int | None = None
    elapsed_ms: int | None = None
    pagespeed: dict[str, Any] | None = None

    title: str | None = None
    title_tag_count: int = 0
    meta_description: str | None = None
    meta_description_tag_count: int = 0
    meta_robots: str | None = None
    meta_robots_tag_count: int = 0
    meta_viewport: str | None = None
    meta_viewport_tag_count: int = 0
    meta_refresh: str | None = None
    meta_refresh_tag_count: int = 0
    canonical: str | None = None
    lang: str | None = None
    h1_tag_count: int = 0
    h2_tag_count: int = 0
    h1: list[str] = dataclasses.field(default_factory=list)
    h2: list[str] = dataclasses.field(default_factory=list)
    hreflang: dict[str, str] = dataclasses.field(default_factory=dict)
    hreflang_raw: list[dict[str, str]] = dataclasses.field(default_factory=list)
    ld_json_blocks: int = 0
    schema_org_errors: list[str] = dataclasses.field(default_factory=list)
    schema_types: list[str] = dataclasses.field(default_factory=list)
    article_like: bool = False

    og_title: str | None = None
    og_description: str | None = None
    og_image: str | None = None
    og_url: str | None = None
    og_type: str | None = None
    twitter_card: str | None = None
    twitter_title: str | None = None
    twitter_description: str | None = None
    twitter_image: str | None = None

    text_word_count: int | None = None
    images_total: int = 0
    images_missing_alt: int = 0
    image_urls: list[str] = dataclasses.field(default_factory=list)
    script_urls: list[str] = dataclasses.field(default_factory=list)
    css_urls: list[str] = dataclasses.field(default_factory=list)

    internal_links: list[str] = dataclasses.field(default_factory=list)
    external_links: list[str] = dataclasses.field(default_factory=list)
    internal_links_dofollow: list[str] = dataclasses.field(default_factory=list)
    internal_links_nofollow: list[str] = dataclasses.field(default_factory=list)
    external_links_dofollow: list[str] = dataclasses.field(default_factory=list)
    external_links_nofollow: list[str] = dataclasses.field(default_factory=list)
    internal_link_items: list[dict[str, Any]] = dataclasses.field(default_factory=list)
    links_without_anchor_text: list[dict[str, Any]] = dataclasses.field(default_factory=list)


@dataclasses.dataclass(frozen=True)
class RobotsRule:
    directive: str  # "allow" | "disallow"
    pattern: str
    regex: re.Pattern[str]
    specificity: int


@dataclasses.dataclass(frozen=True)
class RobotsGroup:
    user_agents: tuple[str, ...]
    rules: tuple[RobotsRule, ...]


@dataclasses.dataclass(frozen=True)
class RobotsRules:
    groups: tuple[RobotsGroup, ...]

    def can_fetch(self, user_agent: str, url: str) -> bool:
        ua = (user_agent or "").strip().lower()
        if not ua:
            ua = "*"

        parts = urlsplit(url)
        path = parts.path or "/"
        path_query = path + (("?" + parts.query) if parts.query else "")

        best_ua_len = -1
        selected: list[RobotsGroup] = []

        for g in self.groups:
            match_len = -1
            for token in g.user_agents:
                t = (token or "").strip().lower()
                if not t:
                    continue
                if t == "*":
                    match_len = max(match_len, 0)
                    continue
                # Common crawler behaviour: UA token matches prefix of UA string.
                if ua.startswith(t):
                    match_len = max(match_len, len(t))
            if match_len < 0:
                continue
            if match_len > best_ua_len:
                best_ua_len = match_len
                selected = [g]
            elif match_len == best_ua_len:
                selected.append(g)

        if best_ua_len < 0:
            return True

        # Merge rules from all equally-specific groups (handles duplicate "User-agent: *" blocks).
        rules: list[RobotsRule] = []
        for g in selected:
            rules.extend(g.rules)

        if not rules:
            return True

        best: RobotsRule | None = None
        best_spec = -1
        for rule in rules:
            if not rule.regex.match(path_query):
                continue
            spec = int(rule.specificity)
            if spec > best_spec:
                best = rule
                best_spec = spec
                continue
            if spec == best_spec and best is not None:
                # Tie-breaker: Allow beats Disallow.
                if best.directive != "allow" and rule.directive == "allow":
                    best = rule

        if best is None:
            return True
        return best.directive == "allow"


def _robots_rule_regex(pattern: str) -> re.Pattern[str]:
    pat = (pattern or "").strip()
    end_anchor = pat.endswith("$")
    if end_anchor:
        pat = pat[:-1]
    # Escape all regex chars except "*" which is a wildcard in robots patterns.
    escaped = re.escape(pat).replace(r"\*", ".*")
    return re.compile(r"^" + escaped + (r"$" if end_anchor else r""))


def _parse_robots_rules(text: str) -> RobotsRules:
    groups: list[RobotsGroup] = []
    current_user_agents: list[str] = []
    current_rules: list[RobotsRule] = []
    seen_rule = False

    def flush() -> None:
        nonlocal current_user_agents, current_rules, seen_rule
        if not current_user_agents:
            current_rules = []
            seen_rule = False
            return
        groups.append(
            RobotsGroup(
                user_agents=tuple([ua for ua in current_user_agents if ua]),
                rules=tuple(current_rules),
            )
        )
        current_user_agents = []
        current_rules = []
        seen_rule = False

    for raw in (text or "").splitlines():
        line = raw.split("#", 1)[0].strip()
        # Blank lines are not reliable group separators in real-world robots.txt;
        # many files use them for readability within a group.
        if not line:
            continue
        if ":" not in line:
            continue
        field, value = line.split(":", 1)
        field = field.strip().lower()
        value = value.strip()

        if field == "user-agent":
            if current_user_agents and seen_rule:
                flush()
            ua = value.lower()
            if ua:
                current_user_agents.append(ua)
            continue

        if field in {"allow", "disallow"}:
            if not current_user_agents:
                continue
            # Empty disallow means "allow all"; empty allow is useless.
            if not value:
                continue
            pat = value
            spec = len(pat.replace("*", "").rstrip("$"))
            current_rules.append(
                RobotsRule(
                    directive=field,
                    pattern=pat,
                    regex=_robots_rule_regex(pat),
                    specificity=spec,
                )
            )
            seen_rule = True
            continue

        # Ignore other directives here (crawl-delay, host, etc).

    flush()
    return RobotsRules(groups=tuple(groups))


class PageHTMLExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.title_texts: list[str] = []
        self._current_title_parts: list[str] = []
        self.title_tag_count: int = 0
        self.meta: dict[str, str] = {}
        self.meta_property: dict[str, str] = {}
        self.meta_description_tag_count: int = 0
        self.meta_robots_tag_count: int = 0
        self.meta_viewport_tag_count: int = 0
        self.meta_refresh_tag_count: int = 0
        self.meta_refresh: str | None = None
        self.canonical: str | None = None
        self.lang: str | None = None
        self.h1: list[str] = []
        self.h2: list[str] = []
        self.h1_tag_count: int = 0
        self.h2_tag_count: int = 0
        self._current_h1_parts: list[str] = []
        self._current_h2_parts: list[str] = []
        self.hreflang: dict[str, str] = {}
        self.hreflang_pairs: list[tuple[str, str]] = []
        self.ld_json_blocks: int = 0
        self.ld_json_texts: list[str] = []
        self._in_ld_json = False
        self._current_ld_json_parts: list[str] = []
        self.links: list[tuple[str, str]] = []
        self.link_items: list[dict[str, str]] = []
        self.images_total: int = 0
        self.images_missing_alt: int = 0
        self.image_srcs: list[str] = []
        self.script_srcs: list[str] = []
        self.css_hrefs: list[str] = []
        self._in_title = False
        self._in_h1 = False
        self._in_h2 = False
        self._in_a = False
        self._current_a_href: str | None = None
        self._current_a_rel: str | None = None
        self._current_a_title: str | None = None
        self._current_a_aria_label: str | None = None
        self._current_a_parts: list[str] = []
        self._skip_text_stack: list[str] = []
        self._text_chunks: list[str] = []
        self._content_depth: int = 0
        self._seen_content_container: bool = False
        self._text_chunks_content: list[str] = []
        self._p_depth: int = 0
        self._p_count_stack: list[bool] = []
        self._text_chunks_p_all: list[str] = []
        self._text_chunks_p_content: list[str] = []
        self._main_article_hint: bool = False
        self.article_tag_count: int = 0

    def is_article_like_page(self) -> bool:
        if self._main_article_hint:
            return True
        if self.article_tag_count == 1 and self.h1_tag_count == 1:
            return True
        return False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = {k.lower(): (v if v is not None else "") for k, v in attrs}
        tag = tag.lower()

        # Exclude common "chrome" areas from the main text word count (menu, footer, etc).
        # Keep title/H1/H2 extraction independent (handled in handle_data before skipping).
        skip_text_tags = {"script", "style", "noscript", "head", "header", "nav", "footer", "aside", "form"}
        content_tags = {"main", "article"}

        if tag == "html":
            self.lang = (attrs_dict.get("lang") or "").strip() or None
        elif tag in content_tags:
            self._content_depth += 1
            self._seen_content_container = True
            if tag == "article":
                self.article_tag_count += 1
            if tag == "main":
                cls = (attrs_dict.get("class") or "").strip().lower()
                if any(k in cls for k in ("article", "post", "entry")):
                    self._main_article_hint = True
        elif tag == "p":
            self._p_depth += 1
            cls = (attrs_dict.get("class") or "").strip().lower()
            pid = (attrs_dict.get("id") or "").strip().lower()
            marker = f"{cls} {pid}"
            excluded = any(k in marker for k in ("article-date", "disclaimer", "note", "byline", "breadcrumb", "cookie"))
            self._p_count_stack.append(not excluded)
        elif tag == "title":
            self._in_title = True
            self.title_tag_count += 1
            self._current_title_parts = []
        elif tag in skip_text_tags:
            self._skip_text_stack.append(tag)
            if tag == "script":
                script_type = (attrs_dict.get("type") or "").strip().lower()
                if script_type == "application/ld+json":
                    self.ld_json_blocks += 1
                    self._in_ld_json = True
                    self._current_ld_json_parts = []
                src = (attrs_dict.get("src") or "").strip()
                if src:
                    self.script_srcs.append(src)
        elif tag == "meta":
            name = (attrs_dict.get("name") or "").strip().lower()
            prop = (attrs_dict.get("property") or "").strip().lower()
            http_equiv = (attrs_dict.get("http-equiv") or "").strip().lower()
            content = (attrs_dict.get("content") or "").strip()
            if name:
                self.meta[name] = content
                if name == "description":
                    self.meta_description_tag_count += 1
                elif name == "robots":
                    self.meta_robots_tag_count += 1
                elif name == "viewport":
                    self.meta_viewport_tag_count += 1
            if prop:
                self.meta_property[prop] = content
            if http_equiv == "refresh":
                self.meta_refresh_tag_count += 1
                if content and not self.meta_refresh:
                    self.meta_refresh = content
        elif tag == "link":
            rel = (attrs_dict.get("rel") or "").strip().lower()
            href = (attrs_dict.get("href") or "").strip()
            hreflang = (attrs_dict.get("hreflang") or "").strip().lower()
            if href and "canonical" in rel:
                self.canonical = href
            if href and hreflang:
                self.hreflang[hreflang] = href
                self.hreflang_pairs.append((hreflang, href))
            if href and "stylesheet" in rel:
                self.css_hrefs.append(href)
        elif tag == "a":
            href = (attrs_dict.get("href") or "").strip()
            rel = (attrs_dict.get("rel") or "").strip()
            title = (attrs_dict.get("title") or "").strip()
            aria_label = (attrs_dict.get("aria-label") or "").strip()
            if href:
                self.links.append((href, rel))
                self._in_a = True
                self._current_a_href = href
                self._current_a_rel = rel
                self._current_a_title = title
                self._current_a_aria_label = aria_label
                self._current_a_parts = []
        elif tag == "img":
            self.images_total += 1
            alt = (attrs_dict.get("alt") or "").strip()
            src = (attrs_dict.get("src") or "").strip()
            if src:
                self.image_srcs.append(src)
            if not alt:
                self.images_missing_alt += 1
            # Semrush-like: treat image alt text as anchor text when <img> is nested inside <a>.
            if self._in_a and alt:
                self._current_a_parts.append(alt)
        elif tag == "h1":
            self._in_h1 = True
            self.h1_tag_count += 1
            self._current_h1_parts = []
        elif tag == "h2":
            self._in_h2 = True
            self.h2_tag_count += 1
            self._current_h2_parts = []

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        skip_text_tags = {"script", "style", "noscript", "head", "header", "nav", "footer", "aside", "form"}
        content_tags = {"main", "article"}
        if tag == "title":
            self._in_title = False
            title = re.sub(r"\s+", " ", "".join(self._current_title_parts)).strip()
            if title:
                self.title_texts.append(title)
        elif tag == "h1":
            self._in_h1 = False
            text = re.sub(r"\s+", " ", "".join(self._current_h1_parts)).strip()
            if text:
                self.h1.append(text)
        elif tag == "h2":
            self._in_h2 = False
            text = re.sub(r"\s+", " ", "".join(self._current_h2_parts)).strip()
            if text:
                self.h2.append(text)
        elif tag == "a" and self._in_a:
            text = re.sub(r"\s+", " ", "".join(self._current_a_parts)).strip()
            href = (self._current_a_href or "").strip()
            rel = (self._current_a_rel or "").strip()
            title = (self._current_a_title or "").strip()
            aria_label = (self._current_a_aria_label or "").strip()
            if href:
                self.link_items.append(
                    {
                        "href": href,
                        "rel": rel,
                        "text": text,
                        "title": title,
                        "aria_label": aria_label,
                    }
                )
            self._in_a = False
            self._current_a_href = None
            self._current_a_rel = None
            self._current_a_title = None
            self._current_a_aria_label = None
            self._current_a_parts = []
        elif tag in content_tags:
            self._content_depth = max(0, self._content_depth - 1)
        elif tag == "p":
            if self._p_depth > 0:
                self._p_depth -= 1
            if self._p_count_stack:
                self._p_count_stack.pop()
        elif tag in skip_text_tags:
            if tag == "script" and self._in_ld_json:
                text = "".join(self._current_ld_json_parts).strip()
                if text:
                    self.ld_json_texts.append(text)
                self._in_ld_json = False
                self._current_ld_json_parts = []
            if self._skip_text_stack and self._skip_text_stack[-1] == tag:
                self._skip_text_stack.pop()

    def handle_data(self, data: str) -> None:
        if self._in_ld_json:
            self._current_ld_json_parts.append(data)
            return
        if self._in_title:
            self._current_title_parts.append(data)
        elif self._in_h1:
            self._current_h1_parts.append(data)
        elif self._in_h2:
            self._current_h2_parts.append(data)
        elif self._in_a:
            self._current_a_parts.append(data)
        if self._skip_text_stack:
            return
        cleaned = data.strip()
        if cleaned:
            self._text_chunks.append(cleaned)
            if self._content_depth > 0:
                self._text_chunks_content.append(cleaned)
            if self._p_depth > 0 and (not self._p_count_stack or self._p_count_stack[-1]):
                self._text_chunks_p_all.append(cleaned)
                if self._content_depth > 0:
                    self._text_chunks_p_content.append(cleaned)

    def get_text_word_count(self) -> int:
        # Prefer "main/article" extracted text (includes headings, list items, etc).
        # Fall back to all text when we couldn't detect a content container.
        chunks = self._text_chunks_content if self._seen_content_container and self._text_chunks_content else self._text_chunks
        text = " ".join(chunks)
        words = re.findall(r"[0-9A-Za-zÀ-ÖØ-öø-ÿ]+", text)
        return len(words)

    def get_title(self) -> str | None:
        for t in self.title_texts:
            t = re.sub(r"\s+", " ", (t or "")).strip()
            if t:
                return t
        return None


_thread_local = threading.local()


def _allow_private_hosts() -> bool:
    return str(os.environ.get("SEO_AUDIT_ALLOW_PRIVATE_HOSTS") or "").strip().lower() in {"1", "true", "yes"}


def _host_is_public(host: str) -> tuple[bool, str]:
    """
    Best-effort SSRF protection.

    Blocks localhost / .local / IP literals and hostnames resolving to non-public IP ranges.
    """
    h = (host or "").strip().lower().strip(".")
    if not h:
        return False, "missing_host"

    if _allow_private_hosts():
        return True, ""

    if h in {"localhost"} or h.endswith(".localhost"):
        return False, "localhost"
    if h.endswith(".local") or h.endswith(".localdomain"):
        return False, "local_tld"

    try:
        ipaddress.ip_address(h)
        return False, "ip_literal"
    except ValueError:
        pass

    cache: dict[str, tuple[bool, str]] | None = getattr(_thread_local, "host_public_cache", None)
    if cache is None:
        cache = {}
        setattr(_thread_local, "host_public_cache", cache)
    if h in cache:
        return cache[h]

    try:
        infos = socket.getaddrinfo(h, None)
    except Exception:
        # DNS failure => request will likely fail anyway; keep it non-blocking.
        cache[h] = (True, "")
        return True, ""

    ips: set[str] = set()
    for it in infos:
        try:
            sockaddr = it[4]
            if isinstance(sockaddr, tuple) and sockaddr:
                ips.add(str(sockaddr[0]))
        except Exception:
            continue

    for ip_s in sorted(ips):
        try:
            ip = ipaddress.ip_address(ip_s)
        except ValueError:
            continue
        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_multicast
            or ip.is_reserved
            or ip.is_unspecified
        ):
            cache[h] = (False, f"non_public_ip:{ip_s}")
            return cache[h]

    cache[h] = (True, "")
    return cache[h]


def _ssrf_guard(url: str) -> str | None:
    if _allow_private_hosts():
        return None
    parts = urlsplit(url or "")
    if parts.scheme not in {"http", "https"}:
        return None
    if parts.port and parts.port not in {80, 443}:
        return "blocked_port"
    host = (parts.hostname or "").strip()
    ok, reason = _host_is_public(host)
    return None if ok else reason


def _get_session(user_agent: str, *, connection_close: bool = True) -> requests.Session:
    key = f"{user_agent}||close={bool(connection_close)}"
    sessions: dict[str, requests.Session] | None = getattr(_thread_local, "sessions", None)
    if sessions is None:
        sessions = {}
        setattr(_thread_local, "sessions", sessions)
    session = sessions.get(key)
    if session is None:
        session = requests.Session()
        headers: dict[str, str] = {"User-Agent": user_agent, "Accept": "text/html,application/xhtml+xml"}
        if connection_close:
            # Some hosts intermittently reset keep-alive connections for bot-like traffic.
            headers["Connection"] = "close"
        session.headers.update(headers)
        sessions[key] = session
    return session


def _reset_session(user_agent: str, *, connection_close: bool = True) -> None:
    session = requests.Session()
    headers: dict[str, str] = {"User-Agent": user_agent, "Accept": "text/html,application/xhtml+xml"}
    if connection_close:
        headers["Connection"] = "close"
    session.headers.update(headers)
    key = f"{user_agent}||close={bool(connection_close)}"
    sessions: dict[str, requests.Session] | None = getattr(_thread_local, "sessions", None)
    if sessions is None:
        sessions = {}
        setattr(_thread_local, "sessions", sessions)
    sessions[key] = session


def _get_pagespeed_session(user_agent: str) -> requests.Session:
    session: requests.Session | None = getattr(_thread_local, "pagespeed_session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({"User-Agent": user_agent, "Accept": "application/json"})
        setattr(_thread_local, "pagespeed_session", session)
    return session


_PAGESPEED_ENDPOINT = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"


def _norm_self_url(url: str | None) -> str | None:
    if not url:
        return None
    u = str(url).strip()
    if not u:
        return None
    return _normalize_url(u, base=u) or u


def _pagespeed_extract_summary(payload: dict[str, Any], *, strategy: str) -> dict[str, Any]:
    def as_float(value: Any) -> float | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                return None
        return None

    out: dict[str, Any] = {"strategy": strategy}

    le = payload.get("loadingExperience") if isinstance(payload.get("loadingExperience"), dict) else None
    ole = payload.get("originLoadingExperience") if isinstance(payload.get("originLoadingExperience"), dict) else None

    overall = None
    metrics = None
    if isinstance(le, dict):
        overall = le.get("overall_category")
        metrics = le.get("metrics") if isinstance(le.get("metrics"), dict) else None
    if overall is None and isinstance(ole, dict):
        overall = ole.get("overall_category")
    out["overall_category"] = overall if isinstance(overall, str) and overall.strip() else None

    field_metrics: dict[str, Any] = {}
    source_metrics = metrics if isinstance(metrics, dict) else (ole.get("metrics") if isinstance(ole, dict) else None)
    if isinstance(source_metrics, dict):
        key_map: dict[str, tuple[str, str]] = {
            "lcp": ("LARGEST_CONTENTFUL_PAINT_MS", "ms"),
            "cls": ("CUMULATIVE_LAYOUT_SHIFT_SCORE", "score"),
            "inp": ("INTERACTION_TO_NEXT_PAINT_MS", "ms"),
        }
        # Some PSI payloads still expose INP under an experimental key.
        inp_alt = "EXPERIMENTAL_INTERACTION_TO_NEXT_PAINT_MS"
        if "INTERACTION_TO_NEXT_PAINT_MS" not in source_metrics and inp_alt in source_metrics:
            key_map["inp"] = (inp_alt, "ms")

        for short, (api_key, unit) in key_map.items():
            node = source_metrics.get(api_key)
            if not isinstance(node, dict):
                continue
            category = str(node.get("category") or "").strip().upper() or None
            p75_raw = as_float(node.get("percentile"))
            if p75_raw is None:
                continue
            if unit == "score" and p75_raw > 1.0:
                p75 = round(p75_raw / 100.0, 4)
            elif unit == "ms":
                p75 = int(round(p75_raw))
            else:
                p75 = p75_raw
            field_metrics[short] = {"p75": p75, "category": category, "unit": unit}

    out["field_metrics"] = field_metrics or None

    # Lighthouse (lab) fallback
    lhr = payload.get("lighthouseResult") if isinstance(payload.get("lighthouseResult"), dict) else None
    performance_score: int | None = None
    lab_metrics: dict[str, Any] = {}
    if isinstance(lhr, dict):
        cats = lhr.get("categories") if isinstance(lhr.get("categories"), dict) else None
        if isinstance(cats, dict):
            perf = cats.get("performance") if isinstance(cats.get("performance"), dict) else None
            if isinstance(perf, dict):
                score = as_float(perf.get("score"))
                if score is not None:
                    performance_score = int(round(score * 100))

        audits = lhr.get("audits") if isinstance(lhr.get("audits"), dict) else None
        if isinstance(audits, dict):
            def audit_numeric(key: str) -> float | None:
                node = audits.get(key)
                if not isinstance(node, dict):
                    return None
                return as_float(node.get("numericValue"))

            lcp = audit_numeric("largest-contentful-paint")
            if lcp is not None:
                lab_metrics["lcp"] = {"value": int(round(lcp)), "unit": "ms"}
            cls = audit_numeric("cumulative-layout-shift")
            if cls is not None:
                lab_metrics["cls"] = {"value": round(float(cls), 4), "unit": "score"}
            inp = audit_numeric("interaction-to-next-paint")
            if inp is None:
                inp = audit_numeric("experimental-interaction-to-next-paint")
            if inp is not None:
                lab_metrics["inp"] = {"value": int(round(inp)), "unit": "ms"}
            tbt = audit_numeric("total-blocking-time")
            if tbt is not None:
                lab_metrics["tbt"] = {"value": int(round(tbt)), "unit": "ms"}
            ttfb = audit_numeric("server-response-time")
            if ttfb is not None:
                lab_metrics["ttfb"] = {"value": int(round(ttfb)), "unit": "ms"}
            speed_index = audit_numeric("speed-index")
            if speed_index is not None:
                lab_metrics["speed_index"] = {"value": int(round(speed_index)), "unit": "ms"}

    out["performance_score"] = performance_score
    out["lab_metrics"] = lab_metrics or None
    return out


def _pagespeed_fetch(
    target_url: str,
    *,
    api_key: str,
    strategy: str,
    timeout_s: float,
    user_agent: str,
    attempts: int = 3,
) -> tuple[dict[str, Any] | None, str | None]:
    session = _get_pagespeed_session(user_agent)
    params: dict[str, Any] = {
        "url": target_url,
        "strategy": strategy,
        "category": "performance",
        "key": api_key,
    }

    last_err: str | None = None
    for attempt in range(1, max(1, int(attempts)) + 1):
        try:
            resp = session.get(_PAGESPEED_ENDPOINT, params=params, timeout=timeout_s)
        except requests.RequestException as e:
            last_err = f"{type(e).__name__}: {e}"
            if attempt < attempts:
                time.sleep(min(30.0, 0.8 * (2 ** (attempt - 1))))
                continue
            return None, last_err

        if resp.status_code == 200:
            try:
                data = resp.json()
            except Exception as e:
                return None, f"JSONDecodeError: {e}"
            return data if isinstance(data, dict) else None, None

        snippet = (resp.text or "").strip()
        if len(snippet) > 400:
            snippet = snippet[:400] + "…"
        last_err = f"HTTP {resp.status_code}: {snippet}"

        retryable = resp.status_code in {429, 500, 502, 503, 504}
        if retryable and attempt < attempts:
            retry_after = (resp.headers.get("Retry-After") or "").strip()
            sleep_s: float | None = None
            if retry_after:
                try:
                    sleep_s = float(retry_after)
                except ValueError:
                    sleep_s = None
            if sleep_s is None:
                sleep_s = min(30.0, 1.2 * (2 ** (attempt - 1)))
            time.sleep(sleep_s)
            continue

        return None, last_err

    return None, last_err or "unknown pagespeed error"


def _run_pagespeed(pages: list[PageData], config: CrawlConfig) -> dict[str, Any]:
    key = (config.pagespeed_api_key or "").strip()
    if not (config.pagespeed_enabled and key):
        reason = None
        if config.pagespeed_enabled and not key:
            reason = "missing_api_key"
            print("[PAGESPEED] Disabled (missing PAGESPEED_API_KEY)", flush=True)
        return {"enabled": False, "reason": reason}

    strategy = str(config.pagespeed_strategy or "mobile").strip().lower()
    if strategy not in {"mobile", "desktop"}:
        strategy = "mobile"

    timeout_s = max(5.0, float(config.pagespeed_timeout_s))
    max_urls = max(0, int(config.pagespeed_max_urls))
    workers = max(1, int(config.pagespeed_workers))

    # Pick a stable subset of HTML 200 pages (effective URLs, de-duplicated).
    pages_by_eff: dict[str, list[PageData]] = defaultdict(list)
    ordered_eff: list[str] = []
    seen_eff: set[str] = set()
    for p in sorted(pages, key=lambda x: x.url):
        if (p.content_type or "").lower().find("html") == -1:
            continue
        if not (isinstance(p.status_code, int) and p.status_code == 200) or p.error:
            continue
        eff = _norm_self_url(p.final_url or p.url)
        if not eff:
            continue
        pages_by_eff[eff].append(p)
        if eff not in seen_eff:
            seen_eff.add(eff)
            ordered_eff.append(eff)
        if max_urls and len(ordered_eff) >= max_urls:
            break

    targets = ordered_eff
    if not targets:
        print("[PAGESPEED] No eligible URLs (HTML 200) to test", flush=True)
        return {"enabled": True, "strategy": strategy, "requested": 0, "tested": 0, "errors": 0}

    print(f"[PAGESPEED] Start | urls={len(targets)} strategy={strategy} workers={workers}", flush=True)
    started = time.monotonic()
    last_progress = started

    ok = 0
    errors = 0

    def task(u: str) -> tuple[str, dict[str, Any] | None, str | None]:
        payload, err = _pagespeed_fetch(
            u,
            api_key=key,
            strategy=strategy,
            timeout_s=timeout_s,
            user_agent=str(config.user_agent),
            attempts=3,
        )
        if err or not payload:
            return u, None, err or "empty response"
        return u, _pagespeed_extract_summary(payload, strategy=strategy), None

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_url = {executor.submit(task, u): u for u in targets}
        done = 0
        for fut in concurrent.futures.as_completed(future_to_url):
            u = future_to_url[fut]
            try:
                eff, summary, err = fut.result()
            except Exception as e:
                eff, summary, err = u, None, f"{type(e).__name__}: {e}"
            if summary:
                ok += 1
                for p in pages_by_eff.get(eff, []):
                    p.pagespeed = summary
            else:
                errors += 1
                for p in pages_by_eff.get(eff, []):
                    p.pagespeed = {"strategy": strategy, "error": err}

            done += 1
            now = time.monotonic()
            if done == 1 or (done % 5 == 0) or (now - last_progress) >= 5.0 or done == len(targets):
                print(f"[PAGESPEED] {done}/{len(targets)} done | ok={ok} errors={errors}", flush=True)
                last_progress = now

    duration_s = time.monotonic() - started
    print(f"[PAGESPEED] Done | ok={ok} errors={errors} duration={duration_s:.1f}s", flush=True)
    return {"enabled": True, "strategy": strategy, "requested": len(targets), "tested": ok, "errors": errors, "duration_s": round(duration_s, 2)}


def _gsc_property_candidates(base_url: str, configured: str | None) -> list[str]:
    candidates: list[str] = []
    if isinstance(configured, str) and configured.strip():
        candidates.append(configured.strip())

    host = (urlsplit(base_url).hostname or "").strip().lower()
    host_no_www = host[4:] if host.startswith("www.") else host
    if host_no_www:
        candidates.append(f"sc-domain:{host_no_www}")

    root = _root_url(base_url).strip()
    if root:
        candidates.append(root if root.endswith("/") else f"{root}/")

    seen: set[str] = set()
    out: list[str] = []
    for c in candidates:
        if c in seen:
            continue
        seen.add(c)
        out.append(c)
    return out


def _gsc_summarize_rows(rows: list[dict[str, Any]], *, dim: str) -> dict[str, Any]:
    def as_int(value: Any) -> int:
        if isinstance(value, bool):
            return 0
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(round(value))
        if isinstance(value, str):
            try:
                return int(float(value))
            except ValueError:
                return 0
        return 0

    def as_float(value: Any) -> float:
        if isinstance(value, bool):
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                return 0.0
        return 0.0

    items: list[dict[str, Any]] = []
    total_clicks = 0
    total_impressions = 0
    weighted_pos = 0.0
    for r in rows:
        if not isinstance(r, dict):
            continue
        keys = r.get("keys") if isinstance(r.get("keys"), list) else []
        dimension = str(keys[0]) if keys else ""
        clicks = as_int(r.get("clicks"))
        impressions = as_int(r.get("impressions"))
        position = as_float(r.get("position"))
        ctr = as_float(r.get("ctr"))
        total_clicks += clicks
        total_impressions += impressions
        weighted_pos += position * impressions
        items.append(
            {
                dim: dimension,
                "clicks": clicks,
                "impressions": impressions,
                "ctr": ctr,
                "position": position,
            }
        )

    avg_ctr = (total_clicks / total_impressions) if total_impressions else 0.0
    avg_position = (weighted_pos / total_impressions) if total_impressions else 0.0
    items.sort(key=lambda d: (-int(d.get("impressions") or 0), -int(d.get("clicks") or 0), str(d.get(dim) or "")))

    return {
        "rows": len(items),
        "total_clicks": total_clicks,
        "total_impressions": total_impressions,
        "avg_ctr": round(avg_ctr, 6),
        "avg_position": round(avg_position, 4),
        "top": items[:10],
    }


def _gsc_daily_series(rows: list[dict[str, Any]], *, start_date: dt.date, end_date: dt.date) -> list[dict[str, Any]]:
    def as_int(value: Any) -> int:
        if isinstance(value, bool):
            return 0
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(round(value))
        if isinstance(value, str):
            try:
                return int(float(value))
            except ValueError:
                return 0
        return 0

    def as_float(value: Any) -> float:
        if isinstance(value, bool):
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                return 0.0
        return 0.0

    by_date: dict[str, dict[str, Any]] = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        keys = r.get("keys") if isinstance(r.get("keys"), list) else []
        key = str(keys[0]) if keys else ""
        if key:
            by_date[key] = r

    out: list[dict[str, Any]] = []
    d = start_date
    while d <= end_date:
        key = d.isoformat()
        r = by_date.get(key) or {}
        clicks = as_int(r.get("clicks"))
        impressions = as_int(r.get("impressions"))
        out.append(
            {
                "date": key,
                "clicks": clicks,
                "impressions": impressions,
                "ctr": as_float(r.get("ctr")),
                "position": as_float(r.get("position")),
            }
        )
        d = d + dt.timedelta(days=1)
    return out


def _run_gsc_api(config: CrawlConfig) -> dict[str, Any]:
    if not config.gsc_api_enabled:
        return {"enabled": False, "reason": "disabled_in_config"}

    credentials = (config.gsc_credentials or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or "").strip().strip('"').strip("'")
    if not credentials:
        return {"enabled": True, "ok": False, "reason": "missing_credentials"}

    cred_path = Path(credentials).expanduser()
    if not cred_path.is_absolute():
        cred_path = (Path.cwd() / cred_path).resolve()
    if not cred_path.exists():
        return {"enabled": True, "ok": False, "reason": "credentials_file_not_found", "credentials": str(cred_path)}

    try:
        import gsc_fetch  # type: ignore
    except Exception as e:
        return {"enabled": True, "ok": False, "reason": f"import_error: {type(e).__name__}: {e}"}

    output_dir = config.gsc_output_dir or os.path.join(config.output_dir, "gsc")
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    today = dt.datetime.now(dt.timezone.utc).date()
    end_date = today - dt.timedelta(days=3)
    days = max(1, int(config.gsc_days))
    start_date = end_date - dt.timedelta(days=days - 1)

    candidates = _gsc_property_candidates(config.base_url, config.gsc_property_url)

    last_err: str | None = None
    for prop in candidates:
        try:
            rows_q = gsc_fetch.fetch_gsc(
                credentials_path=cred_path.resolve(),
                property_url=prop,
                start_date=start_date,
                end_date=end_date,
                dimensions=["query"],
                search_type=str(config.gsc_search_type),
                row_limit=int(config.gsc_row_limit),
                timeout_s=float(config.gsc_timeout_s),
            )
            rows_p = gsc_fetch.fetch_gsc(
                credentials_path=cred_path.resolve(),
                property_url=prop,
                start_date=start_date,
                end_date=end_date,
                dimensions=["page"],
                search_type=str(config.gsc_search_type),
                row_limit=int(config.gsc_row_limit),
                timeout_s=float(config.gsc_timeout_s),
            )
            rows_d: list[dict[str, Any]] = []
            try:
                rows_d = gsc_fetch.fetch_gsc(
                    credentials_path=cred_path.resolve(),
                    property_url=prop,
                    start_date=start_date,
                    end_date=end_date,
                    dimensions=["date"],
                    search_type=str(config.gsc_search_type),
                    row_limit=max(500, int(days) + 10),
                    timeout_s=float(config.gsc_timeout_s),
                )
            except Exception:
                rows_d = []

            queries_csv = Path(output_dir) / "gsc-queries.csv"
            pages_csv = Path(output_dir) / "gsc-pages.csv"
            gsc_fetch.write_csv(queries_csv, rows_q, dimensions=["query"])
            gsc_fetch.write_csv(pages_csv, rows_p, dimensions=["page"])

            daily_csv = Path(output_dir) / "gsc-daily.csv"
            try:
                if rows_d:
                    gsc_fetch.write_csv(daily_csv, rows_d, dimensions=["date"])
            except Exception:
                pass
            daily_series = _gsc_daily_series(rows_d, start_date=start_date, end_date=end_date) if rows_d else []

            pages_items = _perf_items_from_api_rows(rows_p, dim="page")
            gsc_min_impr = max(0, int(getattr(config, "gsc_min_impressions", 0) or 0))
            opp_quick = _perf_opportunities(
                pages_items, dim="page", min_impressions=gsc_min_impr, pos_min=3.0, pos_max=10.0, limit=25
            )
            opp_push = _perf_opportunities(
                pages_items, dim="page", min_impressions=gsc_min_impr, pos_min=11.0, pos_max=20.0, limit=25
            )

            # Optional "Google issues" via URL Inspection API (sampled URLs).
            inspection_meta: dict[str, Any] = {"enabled": bool(config.gsc_inspection_enabled and config.gsc_inspection_max_urls > 0)}
            if inspection_meta["enabled"]:
                max_urls = max(1, int(config.gsc_inspection_max_urls))
                candidates_urls: list[str] = []
                seen_urls: set[str] = set()
                # Prioritize high-impression pages from GSC.
                pages_items_sorted = sorted(
                    [it for it in pages_items if isinstance(it, dict)],
                    key=lambda d: (-int(d.get("impressions") or 0), -int(d.get("clicks") or 0)),
                )
                for it in pages_items_sorted:
                    u = str(it.get("page") or "").strip()
                    if not u or not u.startswith("http"):
                        continue
                    if u in seen_urls:
                        continue
                    seen_urls.add(u)
                    candidates_urls.append(u)
                    if len(candidates_urls) >= max_urls:
                        break

                errors: list[dict[str, Any]] = []
                warnings: list[dict[str, Any]] = []
                notices: list[dict[str, Any]] = []
                raw: list[dict[str, Any]] = []

                def _classify_inspection(url: str, payload: dict[str, Any]) -> None:
                    node = payload.get("inspectionResult") if isinstance(payload.get("inspectionResult"), dict) else {}
                    idx = node.get("indexStatusResult") if isinstance(node.get("indexStatusResult"), dict) else {}
                    verdict = str(idx.get("verdict") or "").strip()
                    coverage = str(idx.get("coverageState") or "").strip()
                    robots = str(idx.get("robotsTxtState") or "").strip()
                    indexing = str(idx.get("indexingState") or "").strip()
                    fetch = str(idx.get("pageFetchState") or "").strip()
                    google_can = str(idx.get("googleCanonical") or "").strip()
                    user_can = str(idx.get("userCanonical") or "").strip()

                    entry = {
                        "url": url,
                        "verdict": verdict,
                        "coverage_state": coverage,
                        "robots_state": robots,
                        "indexing_state": indexing,
                        "fetch_state": fetch,
                        "google_canonical": google_can,
                        "user_canonical": user_can,
                    }

                    # Heuristic severity mapping: keep it stable and useful.
                    lc = f"{verdict} {coverage} {robots} {indexing} {fetch}".lower()
                    if "blocked" in lc and ("robot" in lc or "robots" in lc or "robotstxt" in lc):
                        errors.append(entry)
                        return
                    if verdict.upper() in {"FAIL", "FAILED"}:
                        errors.append(entry)
                        return
                    if "error" in lc or "server error" in lc or "5xx" in lc:
                        errors.append(entry)
                        return
                    if "redirect error" in lc:
                        warnings.append(entry)
                        return
                    if "not indexed" in lc or "currently not indexed" in lc:
                        notices.append(entry)
                        return
                    if "noindex" in lc or "blocked_by_meta_tag" in lc or "blocked by meta tag" in lc:
                        warnings.append(entry)
                        return
                    if google_can and user_can and google_can != user_can:
                        notices.append(entry)
                        return
                    # PASS / nothing notable -> do not create an issue.

                for u in candidates_urls:
                    try:
                        payload = gsc_fetch.inspect_url(
                            credentials_path=cred_path.resolve(),
                            property_url=prop,
                            inspection_url=u,
                            timeout_s=float(config.gsc_inspection_timeout_s),
                            language_code=config.gsc_inspection_language,
                        )
                        if isinstance(payload, dict):
                            raw.append({"url": u, "ok": True, "payload": payload})
                            _classify_inspection(u, payload)
                        else:
                            raw.append({"url": u, "ok": False, "error": "empty_response"})
                    except Exception as e:
                        raw.append({"url": u, "ok": False, "error": f"{type(e).__name__}: {e}"})

                inspection_json = Path(output_dir) / "gsc-url-inspection.json"
                try:
                    inspection_json.write_text(json.dumps(raw, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
                except Exception:
                    pass

                any_ok = any(isinstance(r, dict) and r.get("ok") for r in raw)
                inspection_meta.update(
                    {
                        "ok": bool(any_ok),
                        "reason": "" if any_ok else "all_requests_failed",
                        "checked": len(candidates_urls),
                        "errors": len(errors),
                        "warnings": len(warnings),
                        "notices": len(notices),
                        "json": str(inspection_json.resolve()) if inspection_json.exists() else "",
                    }
                )
                inspection_meta["issues"] = {
                    "indexing_errors": {"count": len(errors), "examples": errors[:10]},
                    "indexing_warnings": {"count": len(warnings), "examples": warnings[:10]},
                    "indexing_notices": {"count": len(notices), "examples": notices[:10]},
                }

            return {
                "enabled": True,
                "ok": True,
                "property": prop,
                "search_type": str(config.gsc_search_type),
                "days": days,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "queries_csv": str(queries_csv.resolve()),
                "pages_csv": str(pages_csv.resolve()),
                "daily_csv": str(daily_csv.resolve()) if daily_csv.exists() else "",
                "daily": daily_series,
                "queries": _gsc_summarize_rows(rows_q, dim="query"),
                "pages": _gsc_summarize_rows(rows_p, dim="page"),
                "min_impressions": gsc_min_impr,
                "issues": {
                    "pages_quick_wins": _issue_block_from_opps(opp_quick, dim="page", normalize_url=True),
                    "pages_push_page_1": _issue_block_from_opps(opp_push, dim="page", normalize_url=True),
                },
                "url_inspection": inspection_meta,
            }
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            continue

    return {
        "enabled": True,
        "ok": False,
        "reason": "no_accessible_property",
        "candidates": candidates,
        "last_error": last_err,
    }


def _run_bing_csv(config: CrawlConfig) -> dict[str, Any]:
    if not config.bing_enabled:
        return {"enabled": False, "reason": "disabled_in_config"}

    queries_csv = (config.bing_queries_csv or "").strip()
    pages_csv = (config.bing_pages_csv or "").strip()
    if not queries_csv and not pages_csv:
        return {"enabled": True, "ok": False, "reason": "missing_csv_paths"}

    out_dir = (config.bing_output_dir or "").strip() or os.path.join(config.output_dir, "bing")
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    out: dict[str, Any] = {"enabled": True, "ok": True, "reason": "csv"}
    out["min_impressions"] = max(0, int(config.bing_min_impressions))

    q_items: list[dict[str, Any]] = []
    p_items: list[dict[str, Any]] = []
    if queries_csv:
        src = Path(queries_csv).expanduser().resolve()
        if not src.exists():
            return {"enabled": True, "ok": False, "reason": "queries_csv_not_found", "queries_csv": str(src)}
        dst = Path(out_dir) / "bing-queries.csv"
        try:
            dst.write_bytes(src.read_bytes())
        except Exception:
            # If copy fails, fallback to reading directly.
            dst = src
        out["queries_csv"] = str(dst.resolve())
        q_items = _perf_items_from_csv(dst, dim="query")
        out["queries"] = _summarize_perf_items(q_items, dim="query")

    if pages_csv:
        src = Path(pages_csv).expanduser().resolve()
        if not src.exists():
            return {"enabled": True, "ok": False, "reason": "pages_csv_not_found", "pages_csv": str(src)}
        dst = Path(out_dir) / "bing-pages.csv"
        try:
            dst.write_bytes(src.read_bytes())
        except Exception:
            dst = src
        out["pages_csv"] = str(dst.resolve())
        p_items = _perf_items_from_csv(dst, dim="page")
        out["pages"] = _summarize_perf_items(p_items, dim="page")

    if p_items:
        opp_quick = _perf_opportunities(
            p_items,
            dim="page",
            min_impressions=int(out["min_impressions"]),
            pos_min=3.0,
            pos_max=10.0,
            limit=25,
        )
        opp_push = _perf_opportunities(
            p_items,
            dim="page",
            min_impressions=int(out["min_impressions"]),
            pos_min=11.0,
            pos_max=20.0,
            limit=25,
        )
        out["issues"] = {
            "pages_quick_wins": _issue_block_from_opps(opp_quick, dim="page", normalize_url=True),
            "pages_push_page_1": _issue_block_from_opps(opp_push, dim="page", normalize_url=True),
        }

    if not q_items and not p_items:
        out["ok"] = False
        out["reason"] = "empty_csv"
    return out


def _bing_site_candidates(base_url: str, configured: str | None) -> list[str]:
    candidates: list[str] = []
    if isinstance(configured, str) and configured.strip():
        candidates.append(configured.strip())
    root = _root_url(base_url).strip()
    if root:
        candidates.append(root if root.endswith("/") else f"{root}/")
    # Some accounts register without trailing slash.
    if root:
        candidates.append(root.rstrip("/"))
    seen: set[str] = set()
    out: list[str] = []
    for c in candidates:
        if c in seen:
            continue
        seen.add(c)
        out.append(c)
    return out


def _bing_extract_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if not isinstance(payload, dict):
        return []
    # Common wrappers.
    for key in ("d", "Data", "data", "Result", "result", "Results", "results"):
        node = payload.get(key)
        if isinstance(node, list):
            return [r for r in node if isinstance(r, dict)]
        if isinstance(node, dict):
            for v in node.values():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    return [r for r in v if isinstance(r, dict)]
    # Fallback: first list-like value.
    for v in payload.values():
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return [r for r in v if isinstance(r, dict)]
    return []


def _bing_date_iso(value: Any) -> str:
    if isinstance(value, str):
        v = value.strip()
        if not v:
            return ""
        # Common format: /Date(1399014000000-0700)/
        m = re.search(r"Date\\((\\d+)([+-]\\d+)?\\)", v)
        if m:
            try:
                ms = int(m.group(1))
                d = dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.timezone.utc).date()
                return d.isoformat()
            except Exception:
                return ""
        try:
            return dt.date.fromisoformat(v).isoformat()
        except Exception:
            return ""
    if isinstance(value, (int, float)) and float(value) > 0:
        # Heuristic: treat values > 10^12 as ms since epoch, otherwise seconds.
        try:
            ts = float(value)
            if ts > 1e12:
                ts = ts / 1000.0
            d = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).date()
            return d.isoformat()
        except Exception:
            return ""
    return ""


def _bing_rank_traffic_series(
    rows: list[dict[str, Any]], *, start_date: dt.date, end_date: dt.date
) -> list[dict[str, Any]]:
    def as_int(value: Any) -> int:
        if isinstance(value, bool):
            return 0
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(round(value))
        if isinstance(value, str):
            try:
                return int(float(value))
            except ValueError:
                return 0
        return 0

    by_date: dict[str, dict[str, Any]] = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        d = _bing_date_iso(r.get("Date") or r.get("date") or "")
        if not d:
            continue
        clicks = as_int(r.get("Clicks") if "Clicks" in r else r.get("clicks"))
        impressions = as_int(r.get("Impressions") if "Impressions" in r else r.get("impressions"))
        by_date[d] = {"clicks": clicks, "impressions": impressions}

    out: list[dict[str, Any]] = []
    available_dates: list[dt.date] = []
    for k in by_date.keys():
        try:
            available_dates.append(dt.date.fromisoformat(k))
        except Exception:
            continue

    effective_start = start_date
    effective_end = end_date
    if available_dates:
        effective_start = max(start_date, min(available_dates))
        effective_end = min(end_date, max(available_dates))
    if effective_end < effective_start:
        return []

    d = effective_start
    while d <= effective_end:
        key = d.isoformat()
        node = by_date.get(key) or {}
        clicks = int(node.get("clicks") or 0)
        impressions = int(node.get("impressions") or 0)
        out.append(
            {
                "date": key,
                "clicks": clicks,
                "impressions": impressions,
                "ctr": (clicks / impressions) if impressions else 0.0,
                "position": 0.0,
            }
        )
        d = d + dt.timedelta(days=1)
    return out


def _bing_call(
    method: str,
    *,
    params: dict[str, Any],
    timeout_s: float,
    api_key: str = "",
    access_token: str = "",
) -> Any:
    base = "https://www.bing.com/webmaster/api.svc/json"
    request_params = dict(params or {})
    headers: dict[str, str] = {}
    token = str(access_token or "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    else:
        key = str(api_key or "").strip()
        if not key:
            raise RuntimeError("bing_credentials_missing")
        request_params["apikey"] = key
    resp = requests.get(f"{base}/{method}", params=request_params, headers=headers, timeout=timeout_s)
    ct = resp.headers.get("content-type", "")
    if ct.startswith("application/json"):
        data = resp.json()
    else:
        raise RuntimeError(f"Non-JSON response for {method} (HTTP {resp.status_code})")

    if isinstance(data, dict) and isinstance(data.get("ErrorCode"), int) and int(data.get("ErrorCode")) != 0:
        raise RuntimeError(str(data.get("Message") or f"bing_api_error:{data.get('ErrorCode')}"))
    return data


def _bing_pick_site_url(
    *,
    base_url: str,
    timeout_s: float,
    api_key: str = "",
    access_token: str = "",
) -> tuple[str | None, list[str], str | None]:
    try:
        payload = _bing_call("GetUserSites", params={}, timeout_s=timeout_s, api_key=api_key, access_token=access_token)
    except Exception as e:
        return None, [], f"{type(e).__name__}: {e}"

    rows = _bing_extract_rows(payload)
    sites: list[str] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        for k in ("Url", "url", "SiteUrl", "siteUrl", "site_url"):
            v = r.get(k)
            if isinstance(v, str) and v.startswith(("http://", "https://")):
                sites.append(v.strip())
                break

    # Fallback: some payload shapes are strings.
    if not sites:
        blob = json.dumps(payload, ensure_ascii=False)
        sites = [s for s in re.findall(r"https?://[^\\s\"\\\\]+", blob) if s.startswith(("http://", "https://"))]

    cand = _bing_site_candidates(base_url, None)
    host = (urlsplit(base_url).hostname or "").strip().lower()
    host_no_www = host[4:] if host.startswith("www.") else host

    def score(u: str) -> tuple[int, int]:
        root = _root_url(u).rstrip("/").lower()
        s = 0
        if root in {c.rstrip('/').lower() for c in cand}:
            s += 3
        h = (urlsplit(u).hostname or "").lower()
        if h == host:
            s += 2
        if host_no_www and h == host_no_www:
            s += 2
        if u.endswith("/"):
            s += 1
        return s, len(u)

    best = None
    if sites:
        best = sorted(sites, key=lambda u: (-score(u)[0], score(u)[1]))[0]
    return best, sites, None


def _bing_normalize_items(rows: list[dict[str, Any]], *, dim: str) -> list[dict[str, Any]]:
    def pick(row: dict[str, Any], keys: list[str]) -> Any:
        for k in keys:
            if k in row:
                return row.get(k)
        # case-insensitive
        lk = {str(k).lower(): k for k in row.keys()}
        for k in keys:
            k2 = lk.get(k.lower())
            if k2 is not None:
                return row.get(k2)
        return None

    out: list[dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        if dim == "query":
            dimension = str(pick(r, ["Query", "query", "Keyword", "keyword"]) or "").strip()
        else:
            dimension = str(pick(r, ["Page", "page", "Url", "url", "URL"]) or "").strip()
        if not dimension:
            continue
        clicks = _to_int(pick(r, ["Clicks", "clicks"]))
        impressions = _to_int(pick(r, ["Impressions", "impressions"]))
        ctr_val = pick(r, ["Ctr", "ctr", "CTR"])
        ctr = _to_ctr(ctr_val) if ctr_val is not None else ((clicks / impressions) if impressions else 0.0)
        pos = _to_float(pick(r, ["AvgPosition", "AveragePosition", "Position", "position"]))
        out.append({dim: dimension, "clicks": clicks, "impressions": impressions, "ctr": ctr, "position": pos})
    return out


def _write_simple_csv(path: Path, items: list[dict[str, Any]], *, dim: str) -> None:
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    headers = [dim, "Clicks", "Impressions", "CTR", "Position"]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for it in items:
            if not isinstance(it, dict):
                continue
            w.writerow(
                {
                    dim: it.get(dim, ""),
                    "Clicks": int(it.get("clicks") or 0),
                    "Impressions": int(it.get("impressions") or 0),
                    "CTR": float(it.get("ctr") or 0.0),
                    "Position": float(it.get("position") or 0.0),
                }
            )


def _run_bing_api(config: CrawlConfig) -> dict[str, Any]:
    if not config.bing_enabled:
        return {"enabled": False, "reason": "disabled_in_config"}

    api_key = (config.bing_api_key or os.environ.get("BING_WEBMASTER_API_KEY") or "").strip().strip('"').strip("'")
    access_token = (config.bing_access_token or os.environ.get("BING_WEBMASTER_ACCESS_TOKEN") or "").strip().strip('"').strip("'")
    if not api_key and not access_token:
        return {"enabled": True, "ok": False, "reason": "missing_credentials"}

    out_dir = (config.bing_output_dir or "").strip() or os.path.join(config.output_dir, "bing")
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    today = dt.datetime.now(dt.timezone.utc).date()
    end_date = today - dt.timedelta(days=3)
    days = max(1, int(config.bing_days))
    start_date = end_date - dt.timedelta(days=days - 1)

    candidates = _bing_site_candidates(config.base_url, config.bing_site_url)
    last_err: str | None = None
    last_payload: dict[str, Any] | None = None

    user_sites: list[str] = []
    auto_site_url: str | None = None
    sites_err: str | None = None
    if not config.bing_site_url:
        auto_site_url, user_sites, sites_err = _bing_pick_site_url(
            base_url=config.base_url,
            timeout_s=float(config.bing_timeout_s),
            api_key=api_key,
            access_token=access_token,
        )
        if auto_site_url:
            candidates = [auto_site_url, *[c for c in candidates if c != auto_site_url]]

    for site_url in candidates:
        try:
            params = {
                "siteUrl": site_url,
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
            }
            payload_q = _bing_call(
                "GetQueryStats",
                params=params,
                timeout_s=float(config.bing_timeout_s),
                api_key=api_key,
                access_token=access_token,
            )
            payload_p = _bing_call(
                "GetPageStats",
                params=params,
                timeout_s=float(config.bing_timeout_s),
                api_key=api_key,
                access_token=access_token,
            )
            last_payload = {"queries": payload_q, "pages": payload_p}

            rows_q = _bing_extract_rows(payload_q)
            rows_p = _bing_extract_rows(payload_p)
            q_items = _bing_normalize_items(rows_q, dim="query")
            p_items = _bing_normalize_items(rows_p, dim="page")

            crawl_issues: list[dict[str, Any]] = []
            blocked_urls: list[dict[str, Any]] = []
            sitemaps_api: list[dict[str, Any]] = []
            url_info: list[dict[str, Any]] = []

            if config.bing_fetch_crawl_issues:
                try:
                    payload_ci = _bing_call(
                        "GetCrawlIssues",
                        params={"siteUrl": site_url},
                        timeout_s=float(config.bing_timeout_s),
                        api_key=api_key,
                        access_token=access_token,
                    )
                    crawl_issues = _bing_extract_rows(payload_ci)
                    (Path(out_dir) / "bing-crawl-issues.json").write_text(
                        json.dumps(payload_ci, ensure_ascii=False, indent=2), encoding="utf-8"
                    )
                except Exception as e:
                    crawl_issues = [{"error": f"{type(e).__name__}: {e}"}]

            if config.bing_fetch_blocked_urls:
                try:
                    payload_bu = _bing_call(
                        "GetBlockedUrls",
                        params={"siteUrl": site_url},
                        timeout_s=float(config.bing_timeout_s),
                        api_key=api_key,
                        access_token=access_token,
                    )
                    blocked_urls = _bing_extract_rows(payload_bu)
                    (Path(out_dir) / "bing-blocked-urls.json").write_text(
                        json.dumps(payload_bu, ensure_ascii=False, indent=2), encoding="utf-8"
                    )
                except Exception as e:
                    blocked_urls = [{"error": f"{type(e).__name__}: {e}"}]

            if config.bing_fetch_sitemaps:
                try:
                    payload_sm = _bing_call(
                        "GetSitemaps",
                        params={"siteUrl": site_url},
                        timeout_s=float(config.bing_timeout_s),
                        api_key=api_key,
                        access_token=access_token,
                    )
                    sitemaps_api = _bing_extract_rows(payload_sm)
                    (Path(out_dir) / "bing-sitemaps.json").write_text(
                        json.dumps(payload_sm, ensure_ascii=False, indent=2), encoding="utf-8"
                    )
                except Exception as e:
                    sitemaps_api = [{"error": f"{type(e).__name__}: {e}"}]

            quota_summary: dict[str, Any] | None = None
            try:
                payload_quota = _bing_call(
                    "GetUrlSubmissionQuota",
                    params={"siteUrl": site_url},
                    timeout_s=float(config.bing_timeout_s),
                    api_key=api_key,
                    access_token=access_token,
                )
                (Path(out_dir) / "bing-url-submission-quota.json").write_text(
                    json.dumps(payload_quota, ensure_ascii=False, indent=2), encoding="utf-8"
                )
                # Best-effort extraction: shapes vary (flat dict / wrapper / list of dict).
                quota_rows = _bing_extract_rows(payload_quota)
                node = quota_rows[0] if quota_rows else (payload_quota if isinstance(payload_quota, dict) else {})
                if isinstance(node, dict):
                    quota_summary = {
                        "remaining": node.get("RemainingQuota") or node.get("remainingQuota") or node.get("remaining"),
                        "daily_quota": node.get("DailyQuota") or node.get("dailyQuota") or node.get("quota"),
                        "reset_date": node.get("ResetDate") or node.get("resetDate") or node.get("reset_date"),
                    }
            except Exception:
                quota_summary = None

            urlinfo_max = max(0, int(config.bing_urlinfo_max))
            if urlinfo_max > 0 and p_items:
                # Only check a small subset: top pages by impressions.
                candidates_pages = sorted(
                    [p for p in p_items if isinstance(p, dict) and str(p.get('page') or '').startswith(('http://','https://'))],
                    key=lambda d: (-int(d.get('impressions') or 0), -int(d.get('clicks') or 0)),
                )
                for row in candidates_pages[:urlinfo_max]:
                    page_url = str(row.get("page") or "").strip()
                    if not page_url:
                        continue
                    try:
                        payload_ui = _bing_call(
                            "GetUrlInfo",
                            params={"siteUrl": site_url, "url": page_url},
                            timeout_s=float(config.bing_timeout_s),
                            api_key=api_key,
                            access_token=access_token,
                        )
                        # UrlInfo often wrapped in {d:{...}}
                        info_rows = _bing_extract_rows(payload_ui)
                        info = info_rows[0] if info_rows else (payload_ui.get("d") if isinstance(payload_ui, dict) else None)
                        if isinstance(info, dict):
                            url_info.append(
                                {
                                    "url": page_url,
                                    "http_status": info.get("HttpStatus"),
                                    "last_crawled": info.get("LastCrawledDate"),
                                    "discovery_date": info.get("DiscoveryDate"),
                                    "anchor_count": info.get("AnchorCount"),
                                }
                            )
                    except Exception as e:
                        url_info.append({"url": page_url, "error": f"{type(e).__name__}: {e}"})

                (Path(out_dir) / "bing-url-info.json").write_text(
                    json.dumps({"meta": {"checked": len(url_info)}, "rows": url_info}, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )

            # Persist raw payloads for debugging.
            (Path(out_dir) / "bing-query-stats.json").write_text(
                json.dumps(payload_q, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            (Path(out_dir) / "bing-page-stats.json").write_text(
                json.dumps(payload_p, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            daily_series: list[dict[str, Any]] = []
            daily_json_path = Path(out_dir) / "bing-rank-traffic-stats.json"
            try:
                payload_daily = _bing_call(
                    "GetRankAndTrafficStats",
                    params={"siteUrl": site_url},
                    timeout_s=float(config.bing_timeout_s),
                    api_key=api_key,
                    access_token=access_token,
                )
                daily_json_path.write_text(json.dumps(payload_daily, ensure_ascii=False, indent=2), encoding="utf-8")
                daily_rows = _bing_extract_rows(payload_daily)
                daily_series = _bing_rank_traffic_series(daily_rows, start_date=start_date, end_date=end_date) if daily_rows else []
            except Exception:
                daily_series = []
            queries_csv = Path(out_dir) / "bing-queries.csv"
            pages_csv = Path(out_dir) / "bing-pages.csv"
            _write_simple_csv(queries_csv, q_items, dim="query")
            _write_simple_csv(pages_csv, p_items, dim="page")

            out: dict[str, Any] = {
                "enabled": True,
                "ok": True,
                "reason": "api",
                "auth_mode": "oauth" if access_token else "api_key",
                "site_url": site_url,
                "user_sites": user_sites[:50],
                "user_sites_error": sites_err,
                "days": days,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "queries_csv": str(queries_csv.resolve()),
                "pages_csv": str(pages_csv.resolve()),
                "daily_json": str(daily_json_path.resolve()) if daily_json_path.exists() else "",
                "daily": daily_series,
                "queries": _summarize_perf_items(q_items, dim="query"),
                "pages": _summarize_perf_items(p_items, dim="page"),
                "min_impressions": max(0, int(config.bing_min_impressions)),
                "crawl_issues": {"rows": len([x for x in crawl_issues if isinstance(x, dict) and not x.get('error')]), "examples": crawl_issues[:10]},
                "blocked_urls": {"rows": len([x for x in blocked_urls if isinstance(x, dict) and not x.get('error')]), "examples": blocked_urls[:10]},
                "sitemaps": {"rows": len([x for x in sitemaps_api if isinstance(x, dict) and not x.get('error')]), "examples": sitemaps_api[:10]},
                "url_submission_quota": quota_summary or {},
                "url_info": {"checked": len(url_info), "rows": url_info[:10]},
            }

            if p_items:
                opp_quick = _perf_opportunities(
                    p_items,
                    dim="page",
                    min_impressions=int(out["min_impressions"]),
                    pos_min=3.0,
                    pos_max=10.0,
                    limit=25,
                )
                opp_push = _perf_opportunities(
                    p_items,
                    dim="page",
                    min_impressions=int(out["min_impressions"]),
                    pos_min=11.0,
                    pos_max=20.0,
                    limit=25,
                )
                out["issues"] = {
                    "pages_quick_wins": _issue_block_from_opps(opp_quick, dim="page", normalize_url=True),
                    "pages_push_page_1": _issue_block_from_opps(opp_push, dim="page", normalize_url=True),
                }
            # Additional Bing issues from API endpoints.
            if isinstance(out.get("crawl_issues"), dict):
                ci = out["crawl_issues"]
                out.setdefault("issues", {})
                out["issues"]["crawl_issues"] = {"count": int(ci.get("rows") or 0), "examples": ci.get("examples") or []}
            if isinstance(out.get("blocked_urls"), dict):
                bu = out["blocked_urls"]
                out.setdefault("issues", {})
                out["issues"]["blocked_urls"] = {"count": int(bu.get("rows") or 0), "examples": bu.get("examples") or []}
            if isinstance(out.get("sitemaps"), dict):
                sm = out["sitemaps"]
                out.setdefault("issues", {})
                out["issues"]["sitemaps"] = {"count": int(sm.get("rows") or 0), "examples": sm.get("examples") or []}
            if url_info:
                bad = [r for r in url_info if isinstance(r, dict) and isinstance(r.get("http_status"), int) and int(r.get("http_status")) not in (200, 204)]
                if bad:
                    out.setdefault("issues", {})
                    out["issues"]["url_info_non_200"] = {"count": len(bad), "examples": bad[:10]}
            return out
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            continue

    if last_payload:
        (Path(out_dir) / "bing-last-error-payload.json").write_text(
            json.dumps(last_payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    return {
        "enabled": True,
        "ok": False,
        "reason": "no_accessible_site",
        "candidates": candidates,
        "last_error": last_err,
    }


def _compute_cwv_summary(pages: list[PageData]) -> dict[str, Any] | None:
    ok_pages: list[PageData] = []
    for p in pages:
        ps = getattr(p, "pagespeed", None)
        if not isinstance(ps, dict) or ps.get("error"):
            continue
        ok_pages.append(p)

    if not ok_pages:
        return None

    field_only_metrics = {"cls", "inp"}

    def metric_value(ps: dict[str, Any], metric: str) -> tuple[float | int | None, str | None]:
        fm = ps.get("field_metrics")
        if isinstance(fm, dict):
            node = fm.get(metric)
            if isinstance(node, dict) and "p75" in node:
                v = node.get("p75")
                if isinstance(v, bool):
                    v = None
                if isinstance(v, (int, float)):
                    return v, "field"

        if metric in field_only_metrics:
            return None, None

        lm = ps.get("lab_metrics")
        if isinstance(lm, dict):
            node = lm.get(metric)
            if isinstance(node, dict) and "value" in node:
                v = node.get("value")
                if isinstance(v, bool):
                    v = None
                if isinstance(v, (int, float)):
                    return v, "lab"
        return None, None

    def category(metric: str, value: float | int | None) -> str:
        if value is None:
            return "na"
        v = float(value)
        if metric == "lcp":
            if v <= 2500:
                return "good"
            if v <= 4000:
                return "ni"
            return "poor"
        if metric == "cls":
            if v <= 0.1:
                return "good"
            if v <= 0.25:
                return "ni"
            return "poor"
        if metric == "inp":
            if v <= 200:
                return "good"
            if v <= 500:
                return "ni"
            return "poor"
        if metric == "tbt":
            if v <= 200:
                return "good"
            if v <= 600:
                return "ni"
            return "poor"
        return "na"

    def summarize_metric(metric: str) -> dict[str, Any]:
        counts: dict[str, int] = {"good": 0, "ni": 0, "poor": 0, "na": 0}
        worst: list[dict[str, Any]] = []
        for p in ok_pages:
            ps = p.pagespeed if isinstance(p.pagespeed, dict) else {}
            v, src = metric_value(ps, metric)
            cat = category(metric, v)
            counts[cat] = counts.get(cat, 0) + 1
            if v is not None:
                worst.append({"url": p.url, "value": v, "source": src})
        worst.sort(key=lambda d: float(d.get("value") or 0), reverse=True)
        return {"counts": counts, "worst": worst[:5]}

    # Most useful for UI/report: LCP / TBT / CLS (plus INP when available).
    sample_ps = ok_pages[0].pagespeed if isinstance(ok_pages[0].pagespeed, dict) else {}
    strategy = str(sample_ps.get("strategy") or "").strip() or None

    core_metrics = ["lcp", "tbt", "cls"]
    page_status: dict[str, int] = {"good": 0, "ni": 0, "poor": 0, "na": 0}
    for p in ok_pages:
        ps = p.pagespeed if isinstance(p.pagespeed, dict) else {}
        cats: list[str] = []
        for m in core_metrics:
            v, _ = metric_value(ps, m)
            cats.append(category(m, v))

        if all(c == "na" for c in cats):
            page_status["na"] += 1
        elif "poor" in cats:
            page_status["poor"] += 1
        elif "ni" in cats:
            page_status["ni"] += 1
        else:
            page_status["good"] += 1

    total_scored = int(page_status.get("good", 0) + page_status.get("ni", 0) + page_status.get("poor", 0))
    score = int(round((page_status.get("good", 0) / total_scored) * 100)) if total_scored else 0

    return {
        "score": score,
        "tested_pages": len(ok_pages),
        "strategy": strategy,
        "page_status": page_status,
        "core_metrics": core_metrics,
        "metrics": {
            "lcp": summarize_metric("lcp"),
            "tbt": summarize_metric("tbt"),
            "cls": summarize_metric("cls"),
            "inp": summarize_metric("inp"),
        },
        "note": "Field metrics (CrUX) preferred when available; CLS/INP require field data (otherwise N/A); Lighthouse lab metrics used as fallback for LCP/TBT.",
    }


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _root_url(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, "", "", ""))


def _root_probe_urls(base_url: str) -> list[str]:
    """
    Add a few Ahrefs-like probes to surface redirect issues (HTTP→HTTPS, www↔non-www, chains)
    without relying on internal links or sitemap entries.
    """
    parts = urlsplit(base_url)
    host = (parts.hostname or "").strip().lower()
    if not host:
        return []

    host_no_www = host[4:] if host.startswith("www.") else host
    host_www = host if host.startswith("www.") else f"www.{host}"

    probes: list[str] = []

    # Probe both http variants to detect forced HTTPS and chains.
    probes.append(urlunsplit(("http", host_no_www, "/", "", "")))
    if host_www != host_no_www:
        probes.append(urlunsplit(("http", host_www, "/", "", "")))
        # Probe the https www version too (www canonicalization without HTTP→HTTPS hop).
        probes.append(urlunsplit(("https", host_www, "/", "", "")))

    # De-duplicate while preserving order.
    return list(dict.fromkeys([p for p in probes if p]))


def _normalize_url(url: str, base: str) -> str | None:
    if not url:
        return None
    url = url.strip()
    if not url:
        return None
    url, _frag = urldefrag(url)
    lowered = url.lower()
    if lowered.startswith(("mailto:", "tel:", "javascript:", "data:")):
        return None
    absolute = urljoin(base, url)
    parts = urlsplit(absolute)
    if parts.scheme not in {"http", "https"}:
        return None
    host = parts.hostname or ""
    # Strip userinfo and normalize IPv6 bracket form.
    netloc = f"[{host}]" if ":" in host and not (host.startswith("[") and host.endswith("]")) else host
    if parts.port:
        netloc = f"{netloc}:{parts.port}"
    if netloc.endswith(":80") and parts.scheme == "http":
        netloc = netloc[:-3]
    if netloc.endswith(":443") and parts.scheme == "https":
        netloc = netloc[:-4]
    path = re.sub(r"/{2,}", "/", parts.path or "/")
    return urlunsplit((parts.scheme, netloc, path, parts.query, ""))


def _is_allowed_host(url: str, base_parts, allow_subdomains: bool) -> bool:
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"}:
        return False
    base_host = base_parts.hostname or ""
    host = parts.hostname or ""
    if allow_subdomains:
        return host == base_host or host.endswith("." + base_host)
    if host == base_host:
        return True
    # Allow common www redirect cases without enabling all subdomains.
    if base_host.startswith("www."):
        return host == base_host[4:]
    if host.startswith("www."):
        return host[4:] == base_host
    return False


def _compile_regex(pattern: str | None) -> re.Pattern[str] | None:
    if not pattern:
        return None
    return re.compile(pattern)


def _should_include(url: str, include_re: re.Pattern[str] | None, exclude_re: re.Pattern[str] | None) -> bool:
    if include_re and not include_re.search(url):
        return False
    if exclude_re and exclude_re.search(url):
        return False
    return True


def _fetch_text(
    url: str,
    timeout_s: float,
    user_agent: str,
    *,
    retries: int = 2,
    connection_close: bool = True,
) -> tuple[int | None, str | None, str | None, str | None, int]:
    guard = _ssrf_guard(url)
    if guard:
        return None, None, f"blocked_by_ssrf_guard:{guard}", None, 0
    session = _get_session(user_agent, connection_close=connection_close)
    last_err: str | None = None
    attempts = max(1, int(retries) + 1)
    for attempt in range(1, attempts + 1):
        try:
            resp = session.get(url, timeout=timeout_s, allow_redirects=True)
            resp.encoding = resp.encoding or "utf-8"
            return resp.status_code, resp.text, None, resp.url, len(resp.history or [])
        except requests.RequestException as e:
            last_err = f"{type(e).__name__}: {e}"
            _reset_session(user_agent, connection_close=connection_close)
            # Retry transient connection resets/timeouts.
            if attempt < attempts:
                time.sleep(min(2.0, 0.4 * (2 ** (attempt - 1))))
                continue
            break
    return None, None, last_err or "unknown_error", None, 0


def _fetch_bytes(
    url: str,
    *,
    timeout_s: float,
    user_agent: str,
    accept: str | None = None,
    retries: int = 2,
    connection_close: bool = True,
) -> tuple[int | None, bytes | None, str | None, str | None, int, str | None]:
    guard = _ssrf_guard(url)
    if guard:
        return None, None, f"blocked_by_ssrf_guard:{guard}", None, 0, None
    session = _get_session(user_agent, connection_close=connection_close)
    headers: dict[str, str] = {}
    if accept:
        headers["Accept"] = accept
    last_err: str | None = None
    attempts = max(1, int(retries) + 1)
    for attempt in range(1, attempts + 1):
        try:
            resp = session.get(url, timeout=timeout_s, allow_redirects=True, headers=headers or None)
            content_type = (resp.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower() or None
            return resp.status_code, resp.content, None, resp.url, len(resp.history or []), content_type
        except requests.RequestException as e:
            last_err = f"{type(e).__name__}: {e}"
            _reset_session(user_agent, connection_close=connection_close)
            if attempt < attempts:
                time.sleep(min(2.0, 0.4 * (2 ** (attempt - 1))))
                continue
            break
    return None, None, last_err or "unknown_error", None, 0, None


def _load_robots(
    root: str,
    timeout_s: float,
    user_agent: str,
    system_fetches: list[dict[str, Any]] | None = None,
    *,
    retries: int = 2,
    connection_close: bool = True,
) -> tuple[RobotsRules | None, list[str], str | None]:
    robots_url = urljoin(root, "/robots.txt")
    status, text, err, final_url, redirect_hops = _fetch_text(
        robots_url, timeout_s=timeout_s, user_agent=user_agent, retries=retries, connection_close=connection_close
    )
    if system_fetches is not None:
        system_fetches.append(
            {
                "type": "robots",
                "url": robots_url,
                "final_url": final_url,
                "status_code": int(status) if isinstance(status, int) else None,
                "redirect_hops": int(redirect_hops),
                "error": err,
            }
        )
    if err or not status or status >= 400 or text is None:
        return None, [], err or f"HTTP {status}"

    sitemaps: list[str] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.lower().startswith("sitemap:"):
            sitemap_url = line.split(":", 1)[1].strip()
            normalized = _normalize_url(sitemap_url, robots_url)
            if normalized:
                sitemaps.append(normalized)

    try:
        rules = _parse_robots_rules(text)
    except Exception as e:
        return None, sorted(set(sitemaps)), f"robots_parse_error: {type(e).__name__}: {e}"

    return rules, sorted(set(sitemaps)), None


def _check_llms_txt(
    root: str,
    timeout_s: float,
    user_agent: str,
    system_fetches: list[dict[str, Any]] | None = None,
    *,
    retries: int = 2,
    connection_close: bool = True,
) -> tuple[bool, str | None]:
    url = urljoin(root, "/llms.txt")
    status, text, err, final_url, redirect_hops = _fetch_text(
        url, timeout_s=timeout_s, user_agent=user_agent, retries=retries, connection_close=connection_close
    )
    if system_fetches is not None:
        system_fetches.append(
            {
                "type": "llms_txt",
                "url": url,
                "final_url": final_url,
                "status_code": int(status) if isinstance(status, int) else None,
                "redirect_hops": int(redirect_hops),
                "error": err,
            }
        )
    if err or not status or status >= 400 or text is None:
        return False, err or f"HTTP {status}"
    if not (text or "").strip():
        return False, "empty"
    return True, None


def _iter_sitemap_urls(
    sitemap_url: str,
    timeout_s: float,
    user_agent: str,
    max_urls: int,
    seen_sitemaps: set[str],
    retries: int = 2,
    connection_close: bool = True,
    hreflang_by_url: dict[str, dict[str, str]] | None = None,
    urlset_locs_by_sitemap: dict[str, list[str]] | None = None,
    system_fetches: list[dict[str, Any]] | None = None,
) -> list[str]:
    if sitemap_url in seen_sitemaps:
        return []
    if len(seen_sitemaps) > 25:
        return []
    seen_sitemaps.add(sitemap_url)

    status, body, err, final_url, redirect_hops, content_type = _fetch_bytes(
        sitemap_url,
        timeout_s=timeout_s,
        user_agent=user_agent,
        accept="application/xml,text/xml,*/*",
        retries=retries,
        connection_close=connection_close,
    )
    if system_fetches is not None:
        system_fetches.append(
            {
                "type": "sitemap",
                "url": sitemap_url,
                "final_url": final_url,
                "status_code": int(status) if isinstance(status, int) else None,
                "redirect_hops": int(redirect_hops),
                "content_type": content_type,
                "content_length": len(body) if isinstance(body, (bytes, bytearray)) else None,
                "error": err,
            }
        )
    if err or not status or status >= 400 or body is None:
        return []

    try:
        xml_bytes = bytes(body)
        # Semrush-like: sitemap file too large (best-effort).
        SITEMAP_MAX_BYTES = 50 * 1024 * 1024
        if len(xml_bytes) > SITEMAP_MAX_BYTES:
            if system_fetches is not None:
                system_fetches.append(
                    {
                        "type": "sitemap_parse",
                        "url": sitemap_url,
                        "final_url": final_url,
                        "status_code": int(status) if isinstance(status, int) else None,
                        "error": "sitemap_too_large",
                        "content_length": len(xml_bytes),
                    }
                )
            return []
        looks_gzip = (
            sitemap_url.lower().endswith(".gz")
            or (isinstance(content_type, str) and "gzip" in content_type)
            or (len(xml_bytes) >= 2 and xml_bytes[0] == 0x1F and xml_bytes[1] == 0x8B)
        )
        if looks_gzip:
            try:
                xml_bytes = gzip.decompress(xml_bytes)
            except Exception:
                # Fall back to raw bytes; some servers mislabel content-types.
                pass
        root = ElementTree.fromstring(xml_bytes)
    except ElementTree.ParseError:
        if system_fetches is not None:
            system_fetches.append(
                {
                    "type": "sitemap_parse",
                    "url": sitemap_url,
                    "final_url": final_url,
                    "status_code": int(status) if isinstance(status, int) else None,
                    "error": "sitemap_parse_error",
                }
            )
        return []

    def _strip_ns(tag: str) -> str:
        return tag.split("}", 1)[-1].lower()

    tag = _strip_ns(root.tag)
    urls: list[str] = []

    if tag == "sitemapindex":
        for sitemap in root.findall(".//{*}sitemap"):
            loc = sitemap.find("{*}loc")
            if loc is None or not (loc.text or "").strip():
                continue
            child = (loc.text or "").strip()
            child_norm = _normalize_url(child, sitemap_url)
            if not child_norm:
                continue
            urls.extend(
                _iter_sitemap_urls(
                    child_norm,
                    timeout_s=timeout_s,
                    user_agent=user_agent,
                    max_urls=max_urls - len(urls),
                    seen_sitemaps=seen_sitemaps,
                    hreflang_by_url=hreflang_by_url,
                    urlset_locs_by_sitemap=urlset_locs_by_sitemap,
                    system_fetches=system_fetches,
                )
            )
            if len(urls) >= max_urls:
                break
    elif tag == "urlset":
        for url_el in root.findall(".//{*}url"):
            loc = url_el.find("{*}loc")
            if loc is None or not (loc.text or "").strip():
                continue
            loc_text = (loc.text or "").strip()
            loc_norm = _normalize_url(loc_text, sitemap_url)
            if loc_norm:
                urls.append(loc_norm)
                if hreflang_by_url is not None:
                    lang_map: dict[str, str] = {}
                    for link_el in url_el.findall("{*}link"):
                        rel = (link_el.attrib.get("rel") or "").strip().lower()
                        if rel and rel != "alternate":
                            continue
                        code = (link_el.attrib.get("hreflang") or "").strip().lower()
                        href = (link_el.attrib.get("href") or "").strip()
                        if not code or not href:
                            continue
                        href_norm = _normalize_url(href, sitemap_url)
                        if href_norm:
                            lang_map[code] = href_norm
                    if lang_map:
                        existing = hreflang_by_url.get(loc_norm)
                        merged = dict(existing or {})
                        merged.update(lang_map)
                        hreflang_by_url[loc_norm] = merged
            if len(urls) >= max_urls:
                break

    if urlset_locs_by_sitemap is not None and tag == "urlset":
        # Best-effort mapping used later for Semrush-style sitemap-file issues.
        urlset_locs_by_sitemap[sitemap_url] = urls[:max_urls]

    return urls[:max_urls]


def _schema_org_validation_errors(ld_json_texts: list[str], *, page_url: str | None = None) -> list[str]:
    """
    Best-effort schema.org validation (Ahrefs-like).

    This is intentionally lightweight: we only flag a small set of common type errors
    that schema.org validators also report (e.g. required fields missing on common types).
    """

    def ctx_text_from(value: Any) -> str:
        if isinstance(value, str):
            return value
        if isinstance(value, list):
            return " ".join(str(x) for x in value)
        return ""

    def has_schema_org(ctx_text: str) -> bool:
        return "schema.org" in (ctx_text or "").lower()

    def iter_objs(node: Any, inherited_ctx: str) -> list[tuple[dict[str, Any], str]]:
        """
        Expand common JSON-LD containers (e.g. @graph) so we validate real schema objects,
        not wrapper nodes. Context can be inherited from parent containers.

        We also traverse nested nodes because many sites embed schema.org objects inside properties
        (e.g. Blog.blogPost → BlogPosting). To avoid flagging plain property dicts, we only yield
        dict nodes that declare an @type or an explicit @context.
        """
        out: list[tuple[dict[str, Any], str]] = []
        if isinstance(node, dict):
            node_ctx = ctx_text_from(node.get("@context")) or inherited_ctx
            graph = node.get("@graph")
            if isinstance(graph, list):
                for item in graph:
                    out.extend(iter_objs(item, node_ctx))
                return out
            if "@type" in node or "@context" in node:
                out.append((node, node_ctx))
            for v in node.values():
                out.extend(iter_objs(v, node_ctx))
            return out
        if isinstance(node, list):
            for item in node:
                out.extend(iter_objs(item, inherited_ctx))
        return out

    page_url_norm = _normalize_url((page_url or "").strip(), base=(page_url or "").strip()) if (page_url or "").strip() else None

    errors: set[str] = set()
    for raw in ld_json_texts:
        text = (raw or "").strip()
        if not text:
            continue
        try:
            data = json.loads(text)
        except Exception:
            errors.add("invalid_json")
            continue

        for obj, ctx_text in iter_objs(data, ""):
            if not has_schema_org(ctx_text):
                continue

            typ = obj.get("@type")
            types: set[str] = set()
            if isinstance(typ, str):
                if not typ.strip():
                    errors.add("missing_type")
                else:
                    types.add(typ.strip())
            elif isinstance(typ, list):
                if not any(isinstance(t, str) and t.strip() for t in typ):
                    errors.add("missing_type")
                else:
                    for t in typ:
                        if isinstance(t, str) and t.strip():
                            types.add(t.strip())
            elif typ is None:
                errors.add("missing_type")
            else:
                errors.add("missing_type")
            # Rich results required-field checks (best-effort). This is NOT a full Rich Results validator;
            # it only aims to catch common "missing required field" problems that tools like Ahrefs flag.
            if types & {"Article", "NewsArticle", "BlogPosting"}:
                headline = obj.get("headline")
                if not (isinstance(headline, str) and headline.strip()):
                    errors.add("headline_missing")
                dp_any = obj.get("datePublished")
                if not (isinstance(dp_any, str) and dp_any.strip()):
                    errors.add("datePublished_missing")

            if "BreadcrumbList" in types:
                # Ahrefs-like: be conservative. Only flag totally missing/empty breadcrumbs to avoid false positives
                # across varied breadcrumb schemas.
                items = obj.get("itemListElement")
                if not isinstance(items, list) or not items:
                    errors.add("breadcrumb_itemListElement_missing")

            if "FAQPage" in types:
                me = obj.get("mainEntity")
                if not isinstance(me, list) or not me:
                    errors.add("faq_mainEntity_missing")
                else:
                    for q in me:
                        if not isinstance(q, dict):
                            errors.add("faq_question_invalid")
                            continue
                        q_name = q.get("name")
                        if not (isinstance(q_name, str) and q_name.strip()):
                            errors.add("faq_question_name_missing")
                        ans = q.get("acceptedAnswer")
                        if not isinstance(ans, dict):
                            errors.add("faq_answer_missing")
                            continue
                        ans_text = ans.get("text")
                        if not (isinstance(ans_text, str) and ans_text.strip()):
                            errors.add("faq_answer_missing")

            if "Product" in types:
                # Ahrefs-like: avoid strict Product/Offer requirements (many valid Product schemas omit offers/prices).
                pass

            if "Organization" in types:
                # Ahrefs-like: only require a non-empty name. (URL / sameAs requirements vary widely.)
                org_name = obj.get("name")
                if not (isinstance(org_name, str) and org_name.strip()):
                    errors.add("organization_name_missing")

            # Ahrefs-like: BlogPosting entries on listing pages are often incomplete and trigger validation errors
            # (e.g. missing datePublished). Keep this check intentionally small and type-agnostic.
            if "BlogPosting" in types:
                dp = obj.get("datePublished")
                url_value = obj.get("url")
                url_norm = (
                    _normalize_url(str(url_value).strip(), base=str(url_value).strip())
                    if isinstance(url_value, str) and str(url_value).strip()
                    else None
                )
                # Ahrefs-like: only flag incomplete BlogPosting entries when they reference a *different* URL than the
                # current page (common on listing pages where blogPost items are partial). Avoid over-reporting on pages
                # that describe themselves but omit datePublished.
                if page_url_norm and url_norm and url_norm == page_url_norm:
                    pass
                else:
                    if not (isinstance(dp, str) and dp.strip()):
                        errors.add("datePublished_missing")

            # Semrush-like: some validators treat Offer.price as invalid when encoded as a string.
            if "SoftwareApplication" in types:
                offers = obj.get("offers")
                if isinstance(offers, dict):
                    offer_type = offers.get("@type")
                    if offer_type == "Offer":
                        if isinstance(offers.get("price"), str):
                            errors.add("offer_price_is_string")
                    elif offer_type == "AggregateOffer":
                        # Ahrefs-like: flag common invalid numeric fields encoded as strings.
                        if any(isinstance(offers.get(k), str) for k in ("lowPrice", "highPrice", "offerCount")):
                            errors.add("offer_price_is_string")
                        inner = offers.get("offers")
                        if isinstance(inner, list):
                            for off in inner:
                                if not isinstance(off, dict):
                                    continue
                                if off.get("@type") == "Offer" and isinstance(off.get("price"), str):
                                    errors.add("offer_price_is_string")
                elif isinstance(offers, list):
                    for off in offers:
                        if not isinstance(off, dict):
                            continue
                        if off.get("@type") == "Offer" and isinstance(off.get("price"), str):
                            errors.add("offer_price_is_string")

            if "position" in obj:
                pos = obj.get("position")
                if not isinstance(pos, int):
                    errors.add("position_not_integer")

    return sorted(errors)


def _schema_types_from_ld_json(ld_json_texts: list[str]) -> list[str]:
    types: set[str] = set()
    for raw in ld_json_texts:
        text = (raw or "").strip()
        if not text:
            continue
        try:
            data = json.loads(text)
        except Exception:
            continue
        stack: list[Any] = [data]
        while stack:
            node = stack.pop()
            if isinstance(node, dict):
                typ = node.get("@type")
                if isinstance(typ, str) and typ.strip():
                    types.add(typ.strip())
                elif isinstance(typ, list):
                    for t in typ:
                        if isinstance(t, str) and t.strip():
                            types.add(t.strip())
                for v in node.values():
                    if isinstance(v, (dict, list)):
                        stack.append(v)
            elif isinstance(node, list):
                for item in node:
                    if isinstance(item, (dict, list)):
                        stack.append(item)
    return sorted(types)


def _extract_page(url: str, config: CrawlConfig, rp: RobotsRules | None, base_parts) -> PageData:
    page = PageData(url=url, fetched_at=_now_iso())
    if rp and not config.ignore_robots and not rp.can_fetch(config.user_agent, url):
        page.error = "Blocked by robots.txt"
        return page

    session = _get_session(config.user_agent, connection_close=bool(config.connection_close))
    last_err: str | None = None
    attempts = max(1, int(getattr(config, "http_retries", 0) or 0) + 1)
    for attempt in range(1, attempts + 1):
        try:
            resp = session.get(url, timeout=config.timeout_s, allow_redirects=True)
            break
        except requests.RequestException as e:
            last_err = f"{type(e).__name__}: {e}"
            _reset_session(config.user_agent, connection_close=bool(config.connection_close))
            session = _get_session(config.user_agent, connection_close=bool(config.connection_close))
            if attempt < attempts:
                time.sleep(min(2.0, 0.4 * (2 ** (attempt - 1))))
                continue
            page.error = last_err
            return page

    page.status_code = resp.status_code
    page.final_url = resp.url
    try:
        page.elapsed_ms = int(round(float(resp.elapsed.total_seconds()) * 1000))
    except Exception:
        page.elapsed_ms = None
    page.redirect_chain = [r.url for r in resp.history] if resp.history else []
    page.redirect_statuses = [int(r.status_code) for r in resp.history if isinstance(getattr(r, "status_code", None), int)] if resp.history else []
    page.x_robots_tag = (resp.headers.get("X-Robots-Tag") or "").strip() or None
    page.content_encoding = (resp.headers.get("Content-Encoding") or "").strip() or None
    page.content_type = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower() or None
    page.response_bytes = len(resp.content) if resp.content is not None else None

    if not page.content_type or "html" not in page.content_type:
        return page

    resp.encoding = resp.encoding or "utf-8"
    html = resp.text or ""
    parser = PageHTMLExtractor()
    try:
        parser.feed(html)
    except Exception as e:
        page.error = f"HTMLParseError: {type(e).__name__}: {e}"
        return page

    page.title = parser.get_title()
    page.title_tag_count = int(parser.title_tag_count)
    page.meta_description = (parser.meta.get("description") or "").strip() or None
    page.meta_description_tag_count = int(parser.meta_description_tag_count)
    page.meta_robots = (parser.meta.get("robots") or "").strip() or None
    page.meta_robots_tag_count = int(parser.meta_robots_tag_count)
    page.meta_viewport = (parser.meta.get("viewport") or "").strip() or None
    page.meta_viewport_tag_count = int(parser.meta_viewport_tag_count)
    page.meta_refresh = (parser.meta_refresh or "").strip() or None
    page.meta_refresh_tag_count = int(parser.meta_refresh_tag_count)

    base_for_urls = page.final_url or url
    canonical_norm = _normalize_url((parser.canonical or "").strip(), base=base_for_urls) if (parser.canonical or "").strip() else None
    page.canonical = canonical_norm
    page.lang = parser.lang
    page.h1_tag_count = int(getattr(parser, "h1_tag_count", 0) or 0)
    page.h2_tag_count = int(getattr(parser, "h2_tag_count", 0) or 0)
    page.h1 = parser.h1
    page.h2 = parser.h2
    hreflang_norm: dict[str, str] = {}
    for code, href in parser.hreflang.items():
        href_norm = _normalize_url(str(href or "").strip(), base=base_for_urls)
        if href_norm:
            hreflang_norm[str(code or "").strip().lower()] = href_norm
    page.hreflang = hreflang_norm
    hreflang_raw: list[dict[str, str]] = []
    for code, href in getattr(parser, "hreflang_pairs", []) or []:
        href_norm = _normalize_url(str(href or "").strip(), base=base_for_urls)
        code_norm = str(code or "").strip().lower()
        if href_norm and code_norm:
            hreflang_raw.append({"hreflang": code_norm, "href": href_norm})
    page.hreflang_raw = hreflang_raw
    page.ld_json_blocks = parser.ld_json_blocks
    page.schema_types = _schema_types_from_ld_json(parser.ld_json_texts)
    try:
        page.article_like = bool(parser.is_article_like_page())
    except Exception:
        page.article_like = False
    page.schema_org_errors = _schema_org_validation_errors(parser.ld_json_texts, page_url=base_for_urls)
    page.text_word_count = parser.get_text_word_count()
    page.images_total = parser.images_total
    page.images_missing_alt = parser.images_missing_alt

    page.og_title = (parser.meta_property.get("og:title") or "").strip() or None
    page.og_description = (parser.meta_property.get("og:description") or "").strip() or None
    page.og_image = (parser.meta_property.get("og:image") or "").strip() or None
    page.og_url = (
        _normalize_url((parser.meta_property.get("og:url") or "").strip(), base=base_for_urls)
        if (parser.meta_property.get("og:url") or "").strip()
        else None
    )
    page.og_type = (parser.meta_property.get("og:type") or "").strip() or None

    def meta_any(key: str) -> str | None:
        return (parser.meta.get(key) or parser.meta_property.get(key) or "").strip() or None

    page.twitter_card = meta_any("twitter:card")
    page.twitter_title = meta_any("twitter:title")
    page.twitter_description = meta_any("twitter:description")
    page.twitter_image = meta_any("twitter:image") or meta_any("twitter:image:src")

    def norm_many(values: list[str]) -> list[str]:
        out: list[str] = []
        for v in values:
            n = _normalize_url(str(v or "").strip(), base=base_for_urls)
            if n:
                out.append(n)
        return sorted(set(out))

    page.image_urls = norm_many(parser.image_srcs)
    page.script_urls = norm_many(parser.script_srcs)
    page.css_urls = norm_many(parser.css_hrefs)

    internal: list[str] = []
    external: list[str] = []
    internal_df: list[str] = []
    internal_nf: list[str] = []
    external_df: list[str] = []
    external_nf: list[str] = []
    for raw_href, raw_rel in parser.links:
        norm = _normalize_url(raw_href, base=base_for_urls)
        if not norm:
            continue
        if not _should_include(norm, config.include_re, config.exclude_re):
            continue
        rel_tokens = {t for t in re.split(r"\s+", (raw_rel or "").strip().lower()) if t}
        nofollow = bool(rel_tokens & {"nofollow", "sponsored", "ugc"})
        if _is_allowed_host(norm, base_parts=base_parts, allow_subdomains=config.allow_subdomains):
            internal.append(norm)
            if nofollow:
                internal_nf.append(norm)
            else:
                internal_df.append(norm)
        else:
            external.append(norm)
            if nofollow:
                external_nf.append(norm)
            else:
                external_df.append(norm)
    page.internal_links = sorted(set(internal))
    page.external_links = sorted(set(external))
    page.internal_links_dofollow = sorted(set(internal_df))
    page.internal_links_nofollow = sorted(set(internal_nf))
    page.external_links_dofollow = sorted(set(external_df))
    page.external_links_nofollow = sorted(set(external_nf))

    # Preserve per-link occurrences for Ahrefs-like "* - links" exports.
    internal_link_items: list[dict[str, Any]] = []
    for it in getattr(parser, "link_items", []) or []:
        if not isinstance(it, dict):
            continue
        href = str(it.get("href") or "").strip()
        if not href:
            continue
        norm = _normalize_url(href, base=base_for_urls)
        if not norm:
            continue
        if not _should_include(norm, config.include_re, config.exclude_re):
            continue
        if not _is_allowed_host(norm, base_parts=base_parts, allow_subdomains=config.allow_subdomains):
            continue
        rel = str(it.get("rel") or "").strip()
        rel_tokens = {t for t in re.split(r"\s+", rel.lower()) if t}
        nofollow = bool(rel_tokens & {"nofollow", "sponsored", "ugc"})
        internal_link_items.append(
            {
                "target_url": norm,
                "nofollow": bool(nofollow),
                "anchor_text": str(it.get("text") or "").strip(),
                "rel": rel,
            }
        )
        if len(internal_link_items) >= 5000:
            break
    page.internal_link_items = internal_link_items

    # Links with no anchor text (Semrush-like): flag <a> elements that have no visible/semantic text.
    # Avoid false positives from empty overlay links when the same target is also linked with real text.
    normalized_links: list[tuple[str, str, str, str, str]] = []
    target_has_anchor_text: set[str] = set()
    for it in getattr(parser, "link_items", []) or []:
        if not isinstance(it, dict):
            continue
        href = str(it.get("href") or "").strip()
        if not href:
            continue
        if href.startswith(("javascript:", "mailto:", "tel:")):
            continue
        if href.startswith("#"):
            continue
        text = str(it.get("text") or "").strip()
        title = str(it.get("title") or "").strip()
        aria_label = str(it.get("aria_label") or "").strip()
        norm = _normalize_url(href, base=base_for_urls)
        if not norm:
            continue
        rel = str(it.get("rel") or "").strip()
        normalized_links.append((norm, rel, text, title, aria_label))
        is_urlish = bool(re.match(r"^(https?://|www\\.)", text.lower())) if text else False
        if (text and not is_urlish) or title or aria_label:
            target_has_anchor_text.add(norm)

    no_anchor_rows: list[dict[str, Any]] = []
    for norm, rel, text, title, aria_label in normalized_links:
        is_urlish = bool(re.match(r"^(https?://|www\\.)", text.lower())) if text else False
        if (text and not is_urlish) or title or aria_label:
            continue
        if norm in target_has_anchor_text:
            continue
        is_internal = _is_allowed_host(norm, base_parts=base_parts, allow_subdomains=config.allow_subdomains)
        no_anchor_rows.append(
            {
                "source_url": page.url,
                "target_url": norm,
                "rel": rel,
                "internal": bool(is_internal),
                "anchor_text": text,
                "title": title,
                "aria_label": aria_label,
            }
        )
        if len(no_anchor_rows) >= 2000:
            break
    page.links_without_anchor_text = no_anchor_rows
    return page


def _parse_int(value: str | None) -> int | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _fetch_resource(url: str, *, timeout_s: float, user_agent: str, connection_close: bool = True) -> dict[str, Any]:
    session = _get_session(user_agent, connection_close=connection_close)
    try:
        resp = session.get(url, timeout=timeout_s, allow_redirects=True, stream=True, headers={"Accept": "*/*"})
        try:
            content_type = (resp.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower() or None
            content_encoding = (resp.headers.get("Content-Encoding") or "").strip() or None
            content_length = _parse_int(resp.headers.get("Content-Length"))
            if content_length is None:
                # Some CDNs omit Content-Length; read a bounded amount to estimate size.
                max_read = 2 * 1024 * 1024
                total = 0
                try:
                    for chunk in resp.iter_content(chunk_size=64 * 1024):
                        if not chunk:
                            continue
                        total += len(chunk)
                        if total > max_read:
                            total = max_read + 1
                            break
                except Exception:
                    pass
                content_length = total if total > 0 else None
            return {
                "url": url,
                "final_url": resp.url,
                "status_code": int(resp.status_code) if isinstance(resp.status_code, int) else None,
                "redirect_hops": len(resp.history or []),
                "content_type": content_type,
                "content_encoding": content_encoding,
                "content_length": content_length,
                "error": None,
            }
        finally:
            resp.close()
    except requests.RequestException as e:
        return {
            "url": url,
            "final_url": None,
            "status_code": None,
            "redirect_hops": 0,
            "content_type": None,
            "content_encoding": None,
            "content_length": None,
            "error": f"{type(e).__name__}: {e}",
        }


def _fetch_internal_resources(pages: list[PageData], config: CrawlConfig, base_parts) -> tuple[list[dict[str, Any]], dict[str, int]]:
    def is_html_200(p: PageData) -> bool:
        return (p.content_type or "").find("html") != -1 and isinstance(p.status_code, int) and p.status_code == 200 and not p.error

    images: set[str] = set()
    scripts: set[str] = set()
    styles: set[str] = set()
    for p in pages:
        if not is_html_200(p):
            continue
        for u in p.image_urls:
            if _is_allowed_host(u, base_parts=base_parts, allow_subdomains=config.allow_subdomains):
                images.add(u)
        for u in p.script_urls:
            if _is_allowed_host(u, base_parts=base_parts, allow_subdomains=config.allow_subdomains):
                scripts.add(u)
        for u in p.css_urls:
            if _is_allowed_host(u, base_parts=base_parts, allow_subdomains=config.allow_subdomains):
                styles.add(u)

    def cap(values: set[str]) -> list[str]:
        max_n = max(0, int(config.max_resources))
        if max_n <= 0:
            return []
        out = sorted(values)
        return out[:max_n]

    image_list = cap(images)
    script_list = cap(scripts)
    css_list = cap(styles)

    tasks: list[tuple[str, str]] = []
    tasks.extend([(u, "image") for u in image_list])
    tasks.extend([(u, "javascript") for u in script_list])
    tasks.extend([(u, "css") for u in css_list])

    results: list[dict[str, Any]] = []
    max_workers = min(16, max(2, int(config.workers) * 2))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {
            executor.submit(
                _fetch_resource,
                u,
                timeout_s=float(config.timeout_s),
                user_agent=str(config.user_agent),
                connection_close=bool(config.connection_close),
            ): (u, t)
            for u, t in tasks
        }
        for fut in concurrent.futures.as_completed(future_to_task):
            u, rtype = future_to_task[fut]
            try:
                row = fut.result()
            except Exception as e:
                row = {
                    "url": u,
                    "final_url": None,
                    "status_code": None,
                    "redirect_hops": 0,
                    "content_type": None,
                    "content_encoding": None,
                    "content_length": None,
                    "error": f"{type(e).__name__}: {e}",
                }
            row["type"] = rtype
            results.append(row)

    counts = {"image": len(image_list), "javascript": len(script_list), "css": len(css_list)}
    return results, counts


def _fetch_external_resources(
    pages: list[PageData], config: CrawlConfig, base_parts
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    def is_html_200(p: PageData) -> bool:
        return (p.content_type or "").find("html") != -1 and isinstance(p.status_code, int) and p.status_code == 200 and not p.error

    images: set[str] = set()
    scripts: set[str] = set()
    styles: set[str] = set()
    for p in pages:
        if not is_html_200(p):
            continue
        for u in p.image_urls:
            if u.startswith(("http://", "https://")) and not _is_allowed_host(u, base_parts=base_parts, allow_subdomains=config.allow_subdomains):
                images.add(u)
        for u in p.script_urls:
            if u.startswith(("http://", "https://")) and not _is_allowed_host(u, base_parts=base_parts, allow_subdomains=config.allow_subdomains):
                scripts.add(u)
        for u in p.css_urls:
            if u.startswith(("http://", "https://")) and not _is_allowed_host(u, base_parts=base_parts, allow_subdomains=config.allow_subdomains):
                styles.add(u)

    def cap(values: set[str]) -> list[str]:
        max_n = max(0, int(config.max_resources))
        if max_n <= 0:
            return []
        out = sorted(values)
        return out[:max_n]

    image_list = cap(images)
    script_list = cap(scripts)
    css_list = cap(styles)

    tasks: list[tuple[str, str]] = []
    tasks.extend([(u, "image") for u in image_list])
    tasks.extend([(u, "javascript") for u in script_list])
    tasks.extend([(u, "css") for u in css_list])

    results: list[dict[str, Any]] = []
    max_workers = min(16, max(2, int(config.workers) * 2))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {
            executor.submit(
                _fetch_resource,
                u,
                timeout_s=float(config.timeout_s),
                user_agent=str(config.user_agent),
                connection_close=bool(config.connection_close),
            ): (u, t)
            for u, t in tasks
        }
        for fut in concurrent.futures.as_completed(future_to_task):
            u, rtype = future_to_task[fut]
            try:
                row = fut.result()
            except Exception as e:
                row = {
                    "url": u,
                    "final_url": None,
                    "status_code": None,
                    "redirect_hops": 0,
                    "content_type": None,
                    "content_encoding": None,
                    "content_length": None,
                    "error": f"{type(e).__name__}: {e}",
                }
            row["type"] = rtype
            results.append(row)

    counts = {"image": len(image_list), "javascript": len(script_list), "css": len(css_list)}
    return results, counts


def _score_external_resource_issues(
    pages: list[PageData],
    resources: list[dict[str, Any]],
    *,
    timeout_s: float,
    user_agent: str,
    output_dir: str | None = None,
) -> dict[str, dict[str, Any]]:
    issues_dir = Path(str(output_dir)).resolve() / "issues" if output_dir else None

    by_url: dict[str, dict[str, Any]] = {}
    for r in resources:
        if not isinstance(r, dict):
            continue
        url = str(r.get("url") or "").strip()
        if url:
            by_url[url] = r

    robots_cache: dict[str, RobotsRules | None] = {}

    def root_for(url: str) -> str | None:
        try:
            parts = urlsplit(url)
        except Exception:
            return None
        if not parts.scheme or not parts.hostname:
            return None
        return f"{parts.scheme}://{parts.hostname}/"

    disallowed_by_robots: set[str] = set()
    # Best-effort: check robots.txt on external hosts so we can match Semrush "disallowed external resources".
    for u in list(by_url.keys()):
        root = root_for(u)
        if not root:
            continue
        if root not in robots_cache:
            rp, _s, _err = _load_robots(root, timeout_s=float(timeout_s), user_agent=str(user_agent), system_fetches=None)
            robots_cache[root] = rp
        rp = robots_cache.get(root)
        if rp and not rp.can_fetch(str(user_agent), u):
            disallowed_by_robots.add(u)

    def is_broken_image(row: dict[str, Any]) -> bool:
        if str(row.get("type") or "").lower() != "image":
            return False
        if str(row.get("url") or "") in disallowed_by_robots:
            return False
        sc = row.get("status_code")
        if isinstance(sc, int) and sc >= 400:
            return True
        return bool(row.get("error"))

    def is_disallowed(row: dict[str, Any]) -> bool:
        if str(row.get("url") or "") in disallowed_by_robots:
            return True
        sc = row.get("status_code")
        if isinstance(sc, int) and sc in (401, 403):
            return True
        return False

    def is_broken_js_css(row: dict[str, Any]) -> bool:
        rtype = str(row.get("type") or "").lower()
        if rtype not in {"javascript", "css"}:
            return False
        if str(row.get("url") or "") in disallowed_by_robots:
            return False
        sc = row.get("status_code")
        if isinstance(sc, int) and sc >= 400 and sc not in (401, 403):
            return True
        return bool(row.get("error"))

    broken_by_page: dict[str, list[dict[str, Any]]] = defaultdict(list)
    broken_js_css_by_page: dict[str, list[dict[str, Any]]] = defaultdict(list)
    disallowed_by_page: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for p in pages:
        if (p.content_type or "").find("html") == -1:
            continue
        page_url = _normalize_url(p.final_url or p.url, base=p.final_url or p.url) or (p.final_url or p.url)

        broken: list[dict[str, Any]] = []
        disallowed: list[dict[str, Any]] = []

        for u in p.image_urls:
            row = by_url.get(u)
            if not row:
                continue
            if is_broken_image(row):
                broken.append({"resource_url": u, "status_code": row.get("status_code"), "error": row.get("error")})
            if is_disallowed(row):
                disallowed.append({"resource_url": u, "type": "image", "status_code": row.get("status_code")})
        for u in p.script_urls:
            row = by_url.get(u)
            if not row:
                continue
            if is_broken_js_css(row):
                broken_js_css_by_page[page_url].append(
                    {"resource_url": u, "type": "javascript", "status_code": row.get("status_code"), "error": row.get("error")}
                )
            if is_disallowed(row):
                disallowed.append({"resource_url": u, "type": "javascript", "status_code": row.get("status_code")})
        for u in p.css_urls:
            row = by_url.get(u)
            if not row:
                continue
            if is_broken_js_css(row):
                broken_js_css_by_page[page_url].append(
                    {"resource_url": u, "type": "css", "status_code": row.get("status_code"), "error": row.get("error")}
                )
            if is_disallowed(row):
                disallowed.append({"resource_url": u, "type": "css", "status_code": row.get("status_code")})

        if broken:
            broken_by_page[page_url].extend(broken)
        if disallowed:
            disallowed_by_page[page_url].extend(disallowed)

    def issue(issue_key: str, rows: list[Any], limit: int = 25) -> dict[str, Any]:
        _write_issue_rows(issues_dir, issue_key, rows)
        return {"count": len(rows), "examples": rows[:limit]}

    broken_external_images = [
        {"url": url, "broken_images": rows[:100], "count": len(rows)} for url, rows in sorted(broken_by_page.items())
    ]
    broken_external_js_css = [
        {"url": url, "resources": rows[:200], "count": len(rows)}
        for url, rows in sorted(broken_js_css_by_page.items())
        if rows
    ]
    disallowed_external_resources = [
        {"url": url, "resources": rows[:200], "count": len(rows)} for url, rows in sorted(disallowed_by_page.items())
    ]

    return {
        "broken_external_images": issue("broken_external_images", broken_external_images),
        "broken_external_js_css": issue("broken_external_js_css", broken_external_js_css),
        "disallowed_external_resources": issue("disallowed_external_resources", disallowed_external_resources),
    }


def _score_resource_issues(
    pages: list[PageData],
    resources: list[dict[str, Any]],
    *,
    output_dir: str | None = None,
    strict_link_counts: bool = False,
) -> dict[str, dict[str, Any]]:
    issues_dir = Path(str(output_dir)).resolve() / "issues" if output_dir else None

    def issue(issue_key: str, rows: list[Any], limit: int = 25) -> dict[str, Any]:
        _write_issue_rows(issues_dir, issue_key, rows)
        return {"count": len(rows), "examples": rows[:limit]}

    def final_url(p: PageData) -> str:
        return _normalize_url(p.final_url or p.url, base=p.final_url or p.url) or (p.final_url or p.url)

    by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_url: dict[str, dict[str, Any]] = {}
    for r in resources:
        if not isinstance(r, dict):
            continue
        url = str(r.get("url") or "").strip()
        rtype = str(r.get("type") or "").strip().lower()
        if not url or not rtype:
            continue
        by_type[rtype].append(r)
        by_url[url] = r

    def is_broken(row: dict[str, Any]) -> bool:
        sc = row.get("status_code")
        if isinstance(sc, int) and sc >= 400:
            return True
        return bool(row.get("error"))

    def is_redirect(row: dict[str, Any]) -> bool:
        hops = row.get("redirect_hops")
        return isinstance(hops, int) and hops > 0

    def is_large(row: dict[str, Any], threshold_bytes: int) -> bool:
        length = row.get("content_length")
        return isinstance(length, int) and length > threshold_bytes

    def _looks_minified_url(url: str | None) -> bool:
        if not url:
            return False
        try:
            path = urlsplit(str(url)).path.lower()
        except Exception:
            path = str(url).lower()
        if ".min." in path:
            return True
        return path.endswith(".min.css") or path.endswith(".min.js")

    # Ahrefs-like threshold tuning (avoid flagging moderately sized images as "too large").
    # Ahrefs-like: "Image file size too large" is very conservative (often only multi-MB images).
    IMG_TOO_LARGE = 1024 * 1024
    JS_TOO_LARGE = 200 * 1024
    CSS_TOO_LARGE = 100 * 1024
    NOT_MINIFIED_TOO_LARGE = 1 * 1024

    broken_images = sorted({r["url"] for r in by_type.get("image", []) if is_broken(r)})
    broken_js = sorted({r["url"] for r in by_type.get("javascript", []) if is_broken(r)})
    broken_css = sorted({r["url"] for r in by_type.get("css", []) if is_broken(r)})

    redirected_images = sorted({r["url"] for r in by_type.get("image", []) if is_redirect(r)})
    redirected_js = sorted({r["url"] for r in by_type.get("javascript", []) if is_redirect(r)})
    redirected_css = sorted({r["url"] for r in by_type.get("css", []) if is_redirect(r)})

    large_images = sorted({r["url"] for r in by_type.get("image", []) if is_large(r, IMG_TOO_LARGE)})
    large_js = sorted({r["url"] for r in by_type.get("javascript", []) if is_large(r, JS_TOO_LARGE)})
    large_css = sorted({r["url"] for r in by_type.get("css", []) if is_large(r, CSS_TOO_LARGE)})

    not_minified_css = sorted(
        {
            r["url"]
            for r in by_type.get("css", [])
            if is_large(r, NOT_MINIFIED_TOO_LARGE) and not _looks_minified_url(str(r.get("final_url") or r.get("url") or ""))
        }
    )
    not_minified_js = sorted(
        {
            r["url"]
            for r in by_type.get("javascript", [])
            if is_large(r, NOT_MINIFIED_TOO_LARGE) and not _looks_minified_url(str(r.get("final_url") or r.get("url") or ""))
        }
    )

    broken_images_set = set(broken_images)
    broken_js_set = set(broken_js)
    broken_css_set = set(broken_css)
    redirected_images_set = set(redirected_images)
    redirected_js_set = set(redirected_js)
    redirected_css_set = set(redirected_css)

    pages_with_broken_image: list[str] = []
    pages_with_broken_js: list[str] = []
    pages_with_broken_css: list[str] = []
    pages_with_redirected_image: list[str] = []
    pages_with_redirected_js: list[str] = []
    pages_with_redirected_css: list[str] = []
    pages_with_not_minified_css: list[str] = []
    pages_with_not_minified_js: list[str] = []

    not_minified_css_set = set(not_minified_css)
    not_minified_js_set = set(not_minified_js)

    for p in pages:
        if (p.content_type or "").find("html") == -1:
            continue
        if not isinstance(p.status_code, int) or p.status_code != 200 or p.error:
            continue
        pid = final_url(p)
        if broken_images_set and any(u in broken_images_set for u in p.image_urls):
            pages_with_broken_image.append(pid)
        if broken_js_set and any(u in broken_js_set for u in p.script_urls):
            pages_with_broken_js.append(pid)
        if broken_css_set and any(u in broken_css_set for u in p.css_urls):
            pages_with_broken_css.append(pid)
        if redirected_images_set and any(u in redirected_images_set for u in p.image_urls):
            pages_with_redirected_image.append(pid)
        if redirected_js_set and any(u in redirected_js_set for u in p.script_urls):
            pages_with_redirected_js.append(pid)
        if redirected_css_set and any(u in redirected_css_set for u in p.css_urls):
            pages_with_redirected_css.append(pid)
        if not_minified_css_set:
            css_match = next((u for u in p.css_urls if u in not_minified_css_set), None)
            if css_match:
                pages_with_not_minified_css.append(f"{pid} -> {css_match}")
        if not_minified_js_set:
            js_match = next((u for u in p.script_urls if u in not_minified_js_set), None)
            if js_match:
                pages_with_not_minified_js.append(f"{pid} -> {js_match}")

    issues: dict[str, dict[str, Any]] = {}
    issues["image_broken"] = issue("image_broken", broken_images)
    issues["page_has_broken_image"] = issue("page_has_broken_image", sorted(set(pages_with_broken_image)))
    issues["image_redirects"] = issue("image_redirects", redirected_images)
    issues["page_has_redirected_image"] = issue("page_has_redirected_image", sorted(set(pages_with_redirected_image)))
    issues["image_file_size_too_large"] = issue("image_file_size_too_large", large_images)
    # Ahrefs-like: per-link export for large images ("... - links").
    large_image_set = set(large_images)
    image_file_size_too_large_links: list[dict[str, Any]] = []
    seen_large_img_links: set[tuple[str, str]] = set()
    if large_image_set:
        for p in pages:
            if (p.content_type or "").find("html") == -1:
                continue
            if not isinstance(p.status_code, int) or p.status_code != 200 or p.error:
                continue
            src = final_url(p)
            for img in p.image_urls:
                if img in large_image_set:
                    key = (src, img)
                    if key not in seen_large_img_links:
                        seen_large_img_links.add(key)
                        image_file_size_too_large_links.append({"source_url": src, "target_url": img})
    issues["image_file_size_too_large_links"] = issue("image_file_size_too_large_links", image_file_size_too_large_links)

    issues["javascript_broken"] = issue("javascript_broken", broken_js)
    # Ahrefs-like: per-link export for broken JS ("... - links").
    broken_js_set = set(broken_js)
    javascript_broken_links: list[dict[str, Any]] = []
    seen_js_links: set[tuple[str, str]] = set()
    if broken_js_set:
        for p in pages:
            if (p.content_type or "").find("html") == -1:
                continue
            if not isinstance(p.status_code, int) or p.status_code != 200 or p.error:
                continue
            src = final_url(p)
            for js in p.script_urls:
                if js in broken_js_set:
                    if strict_link_counts:
                        javascript_broken_links.append({"source_url": src, "target_url": js, "nofollow": False})
                    else:
                        key = (src, js)
                        if key not in seen_js_links:
                            seen_js_links.add(key)
                            javascript_broken_links.append({"source_url": src, "target_url": js, "nofollow": False})
    issues["javascript_broken_links"] = issue("javascript_broken_links", javascript_broken_links)
    issues["page_has_broken_javascript"] = issue("page_has_broken_javascript", sorted(set(pages_with_broken_js)))
    issues["javascript_redirects"] = issue("javascript_redirects", redirected_js)
    issues["page_has_redirected_javascript"] = issue(
        "page_has_redirected_javascript", sorted(set(pages_with_redirected_js))
    )
    issues["javascript_file_size_too_large"] = issue("javascript_file_size_too_large", large_js)

    issues["css_broken"] = issue("css_broken", broken_css)
    issues["page_has_broken_css"] = issue("page_has_broken_css", sorted(set(pages_with_broken_css)))
    issues["css_redirects"] = issue("css_redirects", redirected_css)
    issues["page_has_redirected_css"] = issue("page_has_redirected_css", sorted(set(pages_with_redirected_css)))
    issues["css_file_size_too_large"] = issue("css_file_size_too_large", large_css)
    issues["css_not_minified"] = issue("css_not_minified", sorted(set(pages_with_not_minified_css)))

    issues["javascript_not_minified"] = issue("javascript_not_minified", sorted(set(pages_with_not_minified_js)))
    # Semrush mega export uses a combined check for unminified JS+CSS.
    unminified_pages = sorted(
        {
            str(row).split(" -> ", 1)[0].strip()
            for row in (pages_with_not_minified_css + pages_with_not_minified_js)
            if isinstance(row, str) and " -> " in row
        }
    )
    issues["unminified_javascript_and_css_files"] = issue("unminified_javascript_and_css_files", unminified_pages)

    # Semrush mega export: broken internal JavaScript and CSS files (count affected pages).
    broken_internal_js_css_pages = sorted(set(pages_with_broken_js + pages_with_broken_css))
    issues["broken_internal_javascript_and_css_files"] = issue(
        "broken_internal_javascript_and_css_files", broken_internal_js_css_pages
    )

    return issues


def _score_issues(
    pages: list[PageData],
    sitemap_urls: set[str] | None = None,
    sitemap_urlsets: dict[str, list[str]] | None = None,
    sitemap_hreflang: dict[str, dict[str, str]] | None = None,
    previous_pages: list[PageData] | None = None,
    *,
    output_dir: str | None = None,
    strict_link_counts: bool = False,
) -> dict[str, dict[str, Any]]:
    ISSUE_EXAMPLES_LIMIT = 200
    issues_dir = Path(str(output_dir)).resolve() / "issues" if output_dir else None
    sitemap_urls_norm: set[str] = set()
    if sitemap_urls:
        for u in sitemap_urls:
            if not isinstance(u, str) or not u.strip():
                continue
            sitemap_urls_norm.add(_normalize_url(u.strip(), base=u.strip()) or u.strip())

    def _non_empty(value: str | None) -> bool:
        return bool(value and value.strip())

    def _scheme(url: str | None) -> str:
        return (urlsplit(url or "").scheme or "").lower()

    def _norm_self(url: str | None) -> str | None:
        if not url:
            return None
        u = url.strip()
        if not u:
            return None
        return _normalize_url(u, base=u) or u

    def _looks_noindex(value: str | None) -> bool:
        v = (value or "").lower()
        return "noindex" in v

    def _looks_nofollow(value: str | None) -> bool:
        v = (value or "").lower()
        return "nofollow" in v

    def _is_html(p: PageData) -> bool:
        return (p.content_type or "").find("html") != -1

    def _final_url(p: PageData) -> str:
        return _norm_self(p.final_url) or _norm_self(p.url) or p.url

    def _is_redirect(p: PageData) -> bool:
        return bool(p.redirect_statuses)

    def _is_timeout(p: PageData) -> bool:
        if not p.error:
            return False
        err = str(p.error or "").lower()
        # Ahrefs-like: many network/connection failures are reported as "Timed out" with HTTP status 0.
        net_error = any(
            tok in err
            for tok in (
                "timeout",
                "timed out",
                "forcedtimeout",
                "connection reset",
                "connection aborted",
                "remote disconnected",
                "read timed out",
                "max retries exceeded",
            )
        )
        if not net_error:
            return False
        sc = getattr(p, "status_code", None)
        if sc in (None, 0):
            return True
        return "timeout" in err

    def _is_indexable(p: PageData) -> bool:
        if not _is_html(p):
            return False
        if not isinstance(p.status_code, int) or p.status_code != 200:
            return False
        if p.error:
            return False
        if _looks_noindex(p.meta_robots) or _looks_noindex(p.x_robots_tag):
            return False
        # Ahrefs-like: treat non-canonical URLs as not indexable (canonicalized duplicates).
        if _is_non_canonical(p):
            return False
        return True

    def _is_non_canonical(p: PageData) -> bool:
        if not _is_html(p):
            return False
        if not p.canonical:
            return False
        return _norm_self(p.canonical) != _final_url(p)

    def _issue_block(issue_key: str, rows: list[Any], *, limit: int = ISSUE_EXAMPLES_LIMIT) -> dict[str, Any]:
        _write_issue_rows(issues_dir, issue_key, rows)
        return {"count": len(rows), "examples": rows[:limit]}

    def _pick_better(existing: PageData, candidate: PageData, *, url_key: str | None) -> PageData:
        """
        Choose the "best" representative when multiple PageData objects map to the same normalized URL.
        Prefer direct requests to that URL, then non-redirecting responses (canonical), then non-errors.
        """

        def score(p: PageData) -> tuple[int, int, int, int, int, int]:
            key = (url_key or "").strip()
            req = (_norm_self(p.url) or "").strip()
            final = (_norm_self(p.final_url) or "").strip()
            direct = 0 if key and req == key else 1
            redirect_penalty = 0 if not _is_redirect(p) else 1
            error_penalty = 0 if not p.error else 1
            status_penalty = 0 if (isinstance(p.status_code, int) and p.status_code == 200) else 1
            scheme = _scheme(key or final or req)
            https_penalty = 0 if scheme == "https" else 1
            length_penalty = len(req or key or final)
            return (direct, redirect_penalty, error_penalty, status_penalty, https_penalty, length_penalty)

        return candidate if score(candidate) < score(existing) else existing

    requested_html_pages = [p for p in pages if _is_html(p)]

    # Index pages by any known URL (requested + final). Normalize keys so comparisons are stable.
    page_by_requested: dict[str, PageData] = {}
    page_by_any: dict[str, PageData] = {}
    effective_url_for_requested: dict[str, str] = {}
    effective_pages: dict[str, PageData] = {}
    for p in pages:
        req = _norm_self(p.url)
        if req and req not in page_by_requested:
            page_by_requested[req] = p
        for u in [p.url, p.final_url]:
            u_norm = _norm_self(u)
            if not u_norm:
                continue
            existing_any = page_by_any.get(u_norm)
            page_by_any[u_norm] = p if existing_any is None else _pick_better(existing_any, p, url_key=u_norm)
        eff = _final_url(p)
        effective_url_for_requested[p.url] = eff
        existing_eff = effective_pages.get(eff)
        effective_pages[eff] = p if existing_eff is None else _pick_better(existing_eff, p, url_key=eff)

    prev_effective_pages: dict[str, PageData] | None = None
    if previous_pages:
        prev_page_by_any: dict[str, PageData] = {}
        prev_effective_pages = {}
        for p in previous_pages:
            for u in [p.url, p.final_url]:
                u_norm = _norm_self(u)
                if not u_norm:
                    continue
                existing_any = prev_page_by_any.get(u_norm)
                prev_page_by_any[u_norm] = p if existing_any is None else _pick_better(existing_any, p, url_key=u_norm)
            eff = _final_url(p)
            existing_eff = prev_effective_pages.get(eff)
            prev_effective_pages[eff] = p if existing_eff is None else _pick_better(existing_eff, p, url_key=eff)

    # Prefer "effective" (final) pages for on-page analyses so redirect probes don't inflate counts.
    html_pages = [p for p in effective_pages.values() if _is_html(p)]
    ok_html_pages = [
        p for p in html_pages if isinstance(p.status_code, int) and p.status_code == 200 and not p.error
    ]

    # --- On-page basics (existing keys) ---
    missing_title = [p.url for p in ok_html_pages if not _non_empty(p.title)]
    missing_description = [p.url for p in ok_html_pages if not _non_empty(p.meta_description)]
    def _is_missing_h1(p: PageData) -> bool:
        return (p.h1_tag_count or 0) == 0 or ((p.h1_tag_count or 0) > 0 and not p.h1)

    missing_h1_indexable = [p.url for p in ok_html_pages if _is_indexable(p) and _is_missing_h1(p)]
    missing_h1_not_indexable = [p.url for p in ok_html_pages if (not _is_indexable(p)) and _is_missing_h1(p)]
    missing_h1 = list(dict.fromkeys([*missing_h1_indexable, *missing_h1_not_indexable]))
    multiple_h1 = [p.url for p in ok_html_pages if (p.h1_tag_count or 0) > 1]
    missing_canonical = [p.url for p in ok_html_pages if not _non_empty(p.canonical)]

    bad_status = [p.url for p in pages if isinstance(p.status_code, int) and p.status_code >= 400]
    blocked = [p.url for p in pages if p.error == "Blocked by robots.txt"]

    # Ahrefs-like: consider duplicates on indexable pages only (canonical + indexable).
    _dupe_pool = [p for p in ok_html_pages if _is_indexable(p)]
    title_counts = Counter([p.title.strip() for p in _dupe_pool if _non_empty(p.title)])
    duplicate_titles = {t: c for t, c in title_counts.items() if c > 1}
    description_counts = Counter([p.meta_description.strip() for p in _dupe_pool if _non_empty(p.meta_description)])
    duplicate_descriptions = {d: c for d, c in description_counts.items() if c > 1}

    def _short_text(value: str | None, max_len: int = 160) -> str | None:
        if not _non_empty(value):
            return None
        text = re.sub(r"\s+", " ", str(value)).strip()
        if len(text) <= max_len:
            return text
        return text[: max(0, max_len - 1)] + "…"

    duplicate_title_examples: list[str] = []
    if duplicate_titles:
        for p in _dupe_pool:
            if not _non_empty(p.title):
                continue
            title = p.title.strip()
            if title not in duplicate_titles:
                continue
            duplicate_title_examples.append(f"{p.url} — {title}")
    duplicate_title_examples = sorted(set(duplicate_title_examples))

    duplicate_description_examples: list[str] = []
    if duplicate_descriptions:
        for p in _dupe_pool:
            if not _non_empty(p.meta_description):
                continue
            desc = p.meta_description.strip()
            if desc not in duplicate_descriptions:
                continue
            duplicate_description_examples.append(f"{p.url} — {_short_text(desc, 160) or desc}")
    duplicate_description_examples = sorted(set(duplicate_description_examples))

    # Full rows for duplicates (real affected URLs + value), used by the UI.
    duplicate_title_rows: list[dict[str, Any]] = []
    if duplicate_titles:
        for p in _dupe_pool:
            if not _non_empty(p.title):
                continue
            t = p.title.strip()
            if t not in duplicate_titles:
                continue
            duplicate_title_rows.append(
                {"url": _final_url(p), "title": _short_text(t, 200) or t[:200], "group_count": int(duplicate_titles.get(t) or 0)}
            )

    duplicate_description_rows: list[dict[str, Any]] = []
    if duplicate_descriptions:
        for p in _dupe_pool:
            if not _non_empty(p.meta_description):
                continue
            d = p.meta_description.strip()
            if d not in duplicate_descriptions:
                continue
            duplicate_description_rows.append(
                {
                    "url": _final_url(p),
                    "meta_description": _short_text(d, 260) or d[:260],
                    "group_count": int(duplicate_descriptions.get(d) or 0),
                }
            )

    # --- HTTP status (Ahrefs-like) ---
    http_404 = [p.url for p in pages if p.status_code == 404]
    http_4xx = [p.url for p in pages if isinstance(p.status_code, int) and 400 <= p.status_code < 500]
    http_500 = [p.url for p in pages if p.status_code == 500]
    http_5xx = [p.url for p in pages if isinstance(p.status_code, int) and 500 <= p.status_code < 600]
    timeouts = [p.url for p in pages if _is_timeout(p)]

    # --- Redirects (Ahrefs-like) ---
    redirect_loop = [p.url for p in pages if p.error and "toomanyredirects" in p.error.lower()]
    def _is_lang_root_trailing_slash_redirect(p: PageData) -> bool:
        if not _is_redirect(p):
            return False
        a = urlsplit(p.url or "")
        b = urlsplit(p.final_url or "")
        if not a.path or not b.path:
            return False
        if not re.fullmatch(r"/[a-z]{2}", a.path):
            return False
        if b.path != f"{a.path}/":
            return False
        return True

    # Don't count language-root slash normalizations here (they are covered by Semrush-like "permanent redirects").
    redirect_3xx = [p.url for p in pages if _is_redirect(p) and not _is_lang_root_trailing_slash_redirect(p)]
    redirect_302 = [p.url for p in pages if 302 in (p.redirect_statuses or [])]
    broken_redirect = [p.url for p in pages if _is_redirect(p) and ((isinstance(p.status_code, int) and p.status_code >= 400) or p.error)]
    redirect_chain = [p.url for p in pages if len(p.redirect_statuses or []) > 1]
    redirect_chain_too_long = [p.url for p in pages if len(p.redirect_statuses or []) > 3]
    http_to_https_redirect = [
        p.url
        for p in pages
        if _is_redirect(p) and _scheme(p.url) == "http" and _scheme(p.final_url) == "https"
    ]
    https_to_http_redirect = [
        p.url
        for p in pages
        if _is_redirect(p) and _scheme(p.url) == "https" and _scheme(p.final_url) == "http"
    ]
    meta_refresh_redirect = [p.url for p in ok_html_pages if (p.meta_refresh_tag_count or 0) > 0]

    # --- Indexability / robots directives ---
    # Ahrefs-like: don't flag meta robots issues on error pages (4xx/5xx) because many sites noindex their 404s.
    noindex_pages_all = [
        p.url for p in ok_html_pages if _looks_noindex(p.meta_robots) or _looks_noindex(p.x_robots_tag)
    ]
    nofollow_page = [
        p.url for p in ok_html_pages if _looks_nofollow(p.meta_robots) or _looks_nofollow(p.x_robots_tag)
    ]
    noindex_in_html_and_http_header = [
        p.url for p in ok_html_pages if _looks_noindex(p.meta_robots) and _looks_noindex(p.x_robots_tag)
    ]
    nofollow_in_html_and_http_header = [
        p.url for p in ok_html_pages if _looks_nofollow(p.meta_robots) and _looks_nofollow(p.x_robots_tag)
    ]
    noindex_and_nofollow_page = [
        p.url
        for p in ok_html_pages
        if (_looks_noindex(p.meta_robots) or _looks_noindex(p.x_robots_tag))
        and (_looks_nofollow(p.meta_robots) or _looks_nofollow(p.x_robots_tag))
    ]
    noindex_follow_page = [
        p.url
        for p in ok_html_pages
        if (_looks_noindex(p.meta_robots) or _looks_noindex(p.x_robots_tag))
        and not (_looks_nofollow(p.meta_robots) or _looks_nofollow(p.x_robots_tag))
    ]

    # Canonical target checks
    canonical_points_to_4xx: list[str] = []
    canonical_points_to_5xx: list[str] = []
    canonical_points_to_redirect: list[dict[str, Any]] = []
    non_canonical_specified_as_canonical: list[str] = []
    canonical_from_http_to_https: list[str] = []
    canonical_from_https_to_http: list[str] = []

    for p in ok_html_pages:
        if not p.canonical:
            continue
        canon = _norm_self(p.canonical)
        if not canon:
            continue

        # Scheme cross-check
        page_scheme = _scheme(_final_url(p))
        canon_scheme = _scheme(canon)
        if page_scheme == "http" and canon_scheme == "https":
            canonical_from_http_to_https.append(p.url)
        if page_scheme == "https" and canon_scheme == "http":
            canonical_from_https_to_http.append(p.url)

        target = page_by_any.get(canon)
        if target and isinstance(target.status_code, int):
            if 400 <= target.status_code < 500:
                canonical_points_to_4xx.append(f"{p.url} -> {canon}")
            elif 500 <= target.status_code < 600:
                canonical_points_to_5xx.append(f"{p.url} -> {canon}")

        target_req = page_by_requested.get(canon)
        target_is_redirect = bool(target_req and _is_redirect(target_req))
        if target_is_redirect:
            canonical_points_to_redirect.append(
                {
                    "url": _final_url(p),
                    "canonical_url": canon,
                }
            )

        # Avoid double-reporting: if the canonical target is a redirect (or a hard error),
        # report it as such rather than also "non-canonical specified as canonical".
        if (
            target
            and _is_non_canonical(target)
            and not target_is_redirect
            and not (isinstance(target.status_code, int) and target.status_code >= 400)
        ):
            non_canonical_specified_as_canonical.append(f"{p.url} -> {canon}")

    orphaned_sitemap_pages: list[str] = []
    incorrect_pages_found_in_sitemap_xml: list[str] = []

    # --- Links graph (incoming/outgoing) ---
    incoming_df: dict[str, set[str]] = defaultdict(set)
    incoming_nf: dict[str, set[str]] = defaultdict(set)
    incoming_df_raw: dict[str, set[str]] = defaultdict(set)
    incoming_nf_raw: dict[str, set[str]] = defaultdict(set)

    def _resolve_incoming_target(url: str) -> str:
        """
        Ahrefs-like: for incoming internal link calculations, attribute links that
        point to a redirecting URL to the final destination URL (while keeping the
        "links to redirect" issue separate).
        """

        u = _norm_self(url) or url
        req = page_by_requested.get(u)
        if req and _is_redirect(req):
            u = _norm_self(req.final_url) or u
        return u

    # Ahrefs-like: build link graph only from OK HTML pages.
    # Error pages (4xx/5xx) often contain navigations/menus that can inflate "links" issues and orphans.
    for p in ok_html_pages:
        # Ahrefs-like: de-duplicate sources by canonical URL when available.
        # This prevents counting the same page twice when both a canonical and a non-canonical URL variant
        # are crawled (e.g. `/page` + `/page.html`).
        source = _final_url(p)
        if p.canonical:
            canon_src = _norm_self(p.canonical)
            if canon_src:
                source = canon_src
        for t in p.internal_links_dofollow:
            target_raw = _norm_self(t) or t
            if target_raw != source:
                incoming_df_raw[target_raw].add(source)
            target = _resolve_incoming_target(t)
            if target != source:
                incoming_df[target].add(source)
        for t in p.internal_links_nofollow:
            target_raw = _norm_self(t) or t
            if target_raw != source:
                incoming_nf_raw[target_raw].add(source)
            target = _resolve_incoming_target(t)
            if target != source:
                incoming_nf[target].add(source)

    def incoming_counts(url: str) -> tuple[int, int, int]:
        df = len(incoming_df.get(url, set()))
        nf = len(incoming_nf.get(url, set()))
        return df, nf, df + nf

    def incoming_counts_raw(url: str) -> tuple[int, int, int]:
        df = len(incoming_df_raw.get(url, set()))
        nf = len(incoming_nf_raw.get(url, set()))
        return df, nf, df + nf

    # Now that the incoming link graph is built, we can compute "noindex pages with dofollow incoming links".
    noindex_with_dofollow_links: list[str] = []
    for u in noindex_pages_all:
        df, _nf, _total = incoming_counts(u)
        if df > 0:
            noindex_with_dofollow_links.append(u)
    noindex_page = noindex_with_dofollow_links

    orphan_pages: list[str] = []
    only_one_dofollow_incoming: list[str] = []
    nofollow_incoming_only: list[str] = []
    nofollow_and_dofollow_incoming: list[str] = []
    ok_html_by_eff: dict[str, PageData] = {_final_url(p): p for p in ok_html_pages}
    for eff_url, p in ok_html_by_eff.items():
        df, nf, total = incoming_counts(eff_url)
        if total == 0:
            orphan_pages.append(eff_url)
        if df == 1:
            only_one_dofollow_incoming.append(eff_url)
        if nf > 0 and df == 0:
            nofollow_incoming_only.append(eff_url)
        if nf > 0 and df > 0:
            nofollow_and_dofollow_incoming.append(eff_url)

    # --- Semrush mega export: sitemap-file issues (best-effort) ---
    # Semrush sometimes reports "Incorrect pages found in sitemap.xml" / "Orphaned sitemap pages" on
    # the sitemap file URLs themselves. We emulate that by analyzing the URLsets we parsed.
    if sitemap_urlsets:
        for sitemap_url, locs in sitemap_urlsets.items():
            if not isinstance(sitemap_url, str) or not sitemap_url.strip():
                continue
            if not isinstance(locs, list) or not locs:
                continue
            any_incorrect = False
            # Semrush mega export can report sitemap files as "orphaned pages" (0 internal incoming links).
            # Since sitemap files are generally not linked from HTML pages, treat URLset sitemap files as orphaned.
            any_orphan = True
            for loc in locs:
                if not isinstance(loc, str) or not loc.startswith(("http://", "https://")):
                    continue
                loc_norm = _norm_self(loc) or loc
                p = page_by_any.get(loc_norm)
                if p:
                    is_4xx = bool(isinstance(p.status_code, int) and 400 <= p.status_code < 500)
                    is_5xx = bool(isinstance(p.status_code, int) and 500 <= p.status_code < 600)
                    is_redirect = bool(_is_redirect(p) and (loc_norm in page_by_requested))
                    is_timeout = _is_timeout(p)
                    is_noindex = bool(_looks_noindex(p.meta_robots) or _looks_noindex(p.x_robots_tag))
                    is_non_canonical = _is_non_canonical(p)
                    if is_redirect or is_timeout or is_4xx or is_5xx or is_noindex or is_non_canonical:
                        any_incorrect = True
                if any_incorrect:
                    break

            if any_incorrect:
                incorrect_pages_found_in_sitemap_xml.append(sitemap_url)
            if any_orphan:
                orphaned_sitemap_pages.append(sitemap_url)

    # Canonical URL has no incoming internal links
    canonical_no_incoming: list[str] = []
    canonical_urls: set[str] = set()
    for p in ok_html_pages:
        # Ahrefs-like: only consider explicitly set canonicals that point to a *different* URL.
        # Self-canonicals are covered by orphan/link issues directly on the page URL.
        if not p.canonical:
            continue
        canon = _norm_self(p.canonical)
        if not canon:
            continue
        if canon == _final_url(p):
            continue
        # Treat incoming links to redirecting canonicals as incoming to their final destination.
        canon_req = page_by_requested.get(canon)
        if canon_req and _is_redirect(canon_req):
            canon = _norm_self(canon_req.final_url) or canon
        canonical_urls.add(canon)

    # Avoid duplicating Ahrefs-like reporting: canonical URLs with 0 incoming links are tracked separately.
    orphan_pages = [u for u in orphan_pages if u not in canonical_urls]
    for canon in sorted(canonical_urls):
        df, _nf, _total = incoming_counts(canon)
        if df > 0:
            continue

        # Ahrefs reports this for INDEXABLE pages.
        # Check if the canonical target URL itself is known and indexable.
        target_page = effective_pages.get(canon)
        if target_page and _is_indexable(target_page):
            canonical_no_incoming.append(canon)

    # Outgoing link issues
    links_to_broken: list[dict[str, Any]] = []
    links_to_redirect: list[dict[str, Any]] = []
    pages_no_outgoing: list[str] = []
    pages_with_nofollow_outgoing_internal: list[str] = []

    https_page_internal_to_http: list[str] = []
    http_page_internal_to_https: list[str] = []
    https_page_http_images: list[str] = []
    https_page_http_js: list[str] = []
    https_page_http_css: list[str] = []
    https_mixed_content: list[str] = []

    for p in ok_html_pages:
        source = _final_url(p)

        if not p.internal_links and not p.external_links:
            pages_no_outgoing.append(source)

        if p.internal_links_nofollow:
            pages_with_nofollow_outgoing_internal.append(source)

        page_scheme = _scheme(source)
        if page_scheme == "https":
            if any(_scheme(t) == "http" for t in p.internal_links):
                https_page_internal_to_http.append(source)
            if any(_scheme(t) == "http" for t in p.image_urls):
                https_page_http_images.append(source)
            if any(_scheme(t) == "http" for t in p.script_urls):
                https_page_http_js.append(source)
            if any(_scheme(t) == "http" for t in p.css_urls):
                https_page_http_css.append(source)
            if (
                source in https_page_internal_to_http
                or source in https_page_http_images
                or source in https_page_http_js
                or source in https_page_http_css
            ):
                https_mixed_content.append(source)
        elif page_scheme == "http":
            if any(_scheme(t) == "https" for t in p.internal_links):
                http_page_internal_to_https.append(source)

        broken_targets: list[str] = []
        redirect_targets: list[str] = []
        for t in p.internal_links:
            target = page_by_any.get(_norm_self(t) or t)
            if not target:
                continue
            if isinstance(target.status_code, int) and target.status_code >= 400:
                broken_targets.append(t)
            # Link points to a redirecting URL only if that exact URL is known as a requested URL that redirects.
            req_target = page_by_requested.get(_norm_self(t) or t)
            if req_target and _is_redirect(req_target):
                redirect_targets.append(t)

        if broken_targets:
            links_to_broken.append({"source_url": source, "targets": sorted(set(broken_targets))[:10]})
        if redirect_targets:
            links_to_redirect.append({"source_url": source, "targets": sorted(set(redirect_targets))[:10]})

    redirected_page_no_incoming: list[str] = []

    # Ahrefs-like: "Redirected page has no incoming internal links" refers to FINAL (200) pages that
    # receive internal navigation only via redirecting URL variants:
    # - No. of href inlinks == 0 (no direct internal links to the final URL, excluding self-links)
    # - No. of redirect inlinks > 0 (at least one redirecting URL pointing to this final URL has inlinks)
    redirecting_urls_by_final: dict[str, set[str]] = defaultdict(set)
    hreflang_target_urls: set[str] = set()
    incoming_sources_by_target: dict[str, set[str]] = defaultdict(set)

    for p in ok_html_pages:
        src = _norm_self(_final_url(p)) or _final_url(p)
        for t in (p.internal_links_dofollow or []) + (p.internal_links_nofollow or []):
            t_norm = _norm_self(t) or t
            if t_norm and src and t_norm != src:
                incoming_sources_by_target[t_norm].add(src)
        if isinstance(p.hreflang, dict):
            for href in p.hreflang.values():
                if isinstance(href, str) and href.strip():
                    hreflang_target_urls.add(_norm_self(href) or href.strip())

    if isinstance(sitemap_hreflang, dict):
        for lang_map in sitemap_hreflang.values():
            if not isinstance(lang_map, dict):
                continue
            for href in lang_map.values():
                if isinstance(href, str) and href.strip():
                    hreflang_target_urls.add(_norm_self(href) or href.strip())

    def _strip_www(host: str) -> str:
        h = (host or "").strip().lower()
        return h[4:] if h.startswith("www.") else h

    for req_url, req_page in page_by_requested.items():
        if not _is_redirect(req_page):
            continue
        # Ahrefs-like: language root slash normalization (e.g. /fr -> /fr/) is expected and not a link issue.
        if _is_lang_root_trailing_slash_redirect(req_page):
            continue
        final = _norm_self(req_page.final_url)
        if not final:
            continue
        a = urlsplit(req_url)
        b = urlsplit(final)
        if (
            _strip_www(a.hostname or "") == _strip_www(b.hostname or "") == _strip_www(b.hostname or "")
            and (a.path or "/") == (b.path or "/")
            and (a.query or "") == (b.query or "")
            and ((a.scheme or "") != (b.scheme or "") or (a.hostname or "") != (b.hostname or ""))
        ):
            # Canonicalization redirects (http↔https, www↔non-www) are expected; don't report as link issue.
            continue
        redirecting_urls_by_final[final].add(req_url)

    # Determine which FINAL URLs are only getting inlinks via redirects (Ahrefs "redirect inlinks").
    for final, redirecting_urls in redirecting_urls_by_final.items():
        target_page = ok_html_by_eff.get(final)
        if not target_page:
            continue

        # Direct internal href inlinks to the FINAL URL (exclude self-links).
        href_sources = {s for s in incoming_sources_by_target.get(final, set()) if s != final}
        if href_sources:
            continue

        redirect_inlinks = 0
        for r in redirecting_urls:
            r_norm = _norm_self(r) or r
            if incoming_sources_by_target.get(r_norm, set()):
                redirect_inlinks += 1
                continue
            if sitemap_urls_norm and (r_norm in sitemap_urls_norm):
                redirect_inlinks += 1
                continue
            if r_norm in hreflang_target_urls:
                redirect_inlinks += 1
                continue

        if redirect_inlinks > 0:
            redirected_page_no_incoming.append(final)

    # --- Content thresholds (Ahrefs-like approximations) ---
    TITLE_TOO_LONG = 70
    TITLE_TOO_SHORT = 20
    DESC_TOO_LONG = 160
    DESC_TOO_SHORT = 100
    LOW_WORD_COUNT = 200
    # Ahrefs "AI content detection" is proprietary; we approximate it with a deterministic heuristic.
    # Keep this conservative to avoid false positives.
    AI_HIGH_CONTENT_WORD_COUNT = 2000
    AI_HIGH_CONTENT_PRIVACY_WORD_COUNT = 2000
    # "Slow page" in Ahrefs is a crawl-based signal (not CWV). We approximate with PSI overall category
    # (when field data exists) and a conservative Lighthouse LCP threshold.
    SLOW_PAGE_MS = 2000
    SLOW_PAGE_LCP_MS = 3500
    SLOW_PAGE_SPEED_INDEX_MS = 2500
    CWV_LCP_GOOD_MS = 2500
    CWV_LCP_POOR_MS = 4000
    CWV_CLS_GOOD = 0.1
    CWV_CLS_POOR = 0.25
    CWV_INP_GOOD_MS = 200
    CWV_INP_POOR_MS = 500
    CWV_TBT_GOOD_MS = 200
    CWV_TBT_POOR_MS = 600
    CWV_FIELD_ONLY_METRICS = {"cls", "inp"}

    # --- Semrush-like: permanent redirects (301) ---
    permanent_redirects: list[str] = []
    for p in pages:
        req = (p.url or "").strip()
        fin = (p.final_url or "").strip()
        if not req or not fin or req == fin:
            continue
        statuses = getattr(p, "redirect_statuses", None)
        if not isinstance(statuses, list) or not any(isinstance(s, int) and s in {301, 308} for s in statuses):
            continue
        a = urlsplit(req)
        b = urlsplit(fin)
        # Semrush mega export seems to attribute canonicalization 301s (http↔https, www↔non-www, /fr→/fr/)
        # to the destination URL.
        host_changed = (a.hostname or "").lower() != (b.hostname or "").lower()
        scheme_changed = (a.scheme or "").lower() != (b.scheme or "").lower()
        added_trailing_slash_lang_root = bool(
            not (a.path or "").endswith("/")
            and (b.path or "").endswith("/")
            and (b.path or "") == f"{a.path or ''}/"
            and re.fullmatch(r"/[a-z]{2}/", b.path or "")
        )
        if host_changed or scheme_changed or added_trailing_slash_lang_root:
            permanent_redirects.append(_norm_self(fin) or fin)

    # --- Semrush-like: low text to HTML ratio ---
    low_text_to_html_ratio: list[str] = []
    for p in ok_html_pages:
        wc = int(p.text_word_count or 0) if isinstance(p.text_word_count, int) else 0
        size = int(p.response_bytes or 0) if isinstance(p.response_bytes, int) else 0
        if wc <= 0 or size <= 0:
            continue
        # Approximate Semrush "Low text to HTML ratio" on several reference sites.
        # Threshold expressed as "minimum words per KB of HTML".
        if (wc / max(1.0, (size / 1024.0))) < 8.0:
            low_text_to_html_ratio.append(p.url)

    indexable_html_pages = [p for p in ok_html_pages if _is_indexable(p)]
    not_indexable_html_pages = [p for p in ok_html_pages if not _is_indexable(p)]

    multiple_title_tags = [p.url for p in ok_html_pages if (p.title_tag_count or 0) > 1]
    multiple_meta_description_tags = [p.url for p in ok_html_pages if (p.meta_description_tag_count or 0) > 1]
    def _is_semrush_indexable(p: PageData) -> bool:
        # Semrush often reports content issues on canonicalized duplicates (e.g. query pages),
        # as long as they're HTML 200 and not explicitly noindex.
        if not _is_html(p):
            return False
        if not isinstance(p.status_code, int) or p.status_code != 200:
            return False
        if p.error:
            return False
        if _looks_noindex(p.meta_robots) or _looks_noindex(p.x_robots_tag):
            return False
        return True

    def _primary_lang_hint() -> str | None:
        # Best-effort: use the language of the root homepage as the crawl's primary language.
        # This helps match Semrush behavior on multi-language sites where some checks (e.g. low word count)
        # appear to be computed mainly for the primary language.
        for p in ok_html_pages:
            eff = _final_url(p)
            parts = urlsplit(eff)
            if (parts.path or "/") == "/":
                lang = (p.lang or "").strip().lower()
                return lang.split("-", 1)[0] if lang else None
        langs = [(p.lang or "").strip().lower().split("-", 1)[0] for p in ok_html_pages if (p.lang or "").strip()]
        if not langs:
            return None
        return Counter(langs).most_common(1)[0][0]

    primary_lang = _primary_lang_hint()

    low_word_count = [
        p.url
        for p in ok_html_pages
        if _is_semrush_indexable(p)
        and isinstance(p.text_word_count, int)
        and p.text_word_count < LOW_WORD_COUNT
        and (
            not primary_lang
            or not (p.lang or "").strip()
            or (p.lang or "").strip().lower().split("-", 1)[0] == primary_lang
        )
    ]
    title_too_long_indexable = [
        p.url for p in indexable_html_pages if _non_empty(p.title) and len(p.title.strip()) > TITLE_TOO_LONG
    ]
    title_too_long_not_indexable = [
        p.url for p in not_indexable_html_pages if _non_empty(p.title) and len(p.title.strip()) > TITLE_TOO_LONG
    ]
    title_too_short = [p.url for p in ok_html_pages if _non_empty(p.title) and len(p.title.strip()) < TITLE_TOO_SHORT]
    meta_description_too_long_indexable = [
        p.url for p in indexable_html_pages if _non_empty(p.meta_description) and len(p.meta_description.strip()) > DESC_TOO_LONG
    ]
    meta_description_too_long_not_indexable = [
        p.url for p in not_indexable_html_pages if _non_empty(p.meta_description) and len(p.meta_description.strip()) > DESC_TOO_LONG
    ]
    meta_description_too_short_indexable = [
        p.url for p in indexable_html_pages if _non_empty(p.meta_description) and len(p.meta_description.strip()) < DESC_TOO_SHORT
    ]
    meta_description_too_short_not_indexable = [
        p.url
        for p in not_indexable_html_pages
        if _non_empty(p.meta_description) and len(p.meta_description.strip()) < DESC_TOO_SHORT
    ]

    pages_have_high_ai_content_levels_set: set[str] = set()
    privacy_path_re = re.compile(r"(privacy|confidentialit|rgpd|gdpr)", re.IGNORECASE)
    privacy_rgpd_re = re.compile(r"(rgpd|gdpr)", re.IGNORECASE)
    for p in indexable_html_pages:
        if not isinstance(p.text_word_count, int):
            continue
        eff = _final_url(p)
        path = urlsplit(eff).path or "/"
        lang = (p.lang or "").strip().lower()
        if primary_lang and lang and lang.split("-", 1)[0] != primary_lang:
            continue

        article_like = bool(getattr(p, "article_like", False)) or ("/blog/" in eff)
        is_privacy_like = bool(privacy_path_re.search(path))
        if article_like and p.text_word_count >= AI_HIGH_CONTENT_WORD_COUNT:
            pages_have_high_ai_content_levels_set.add(eff)
        elif is_privacy_like and p.text_word_count >= AI_HIGH_CONTENT_PRIVACY_WORD_COUNT:
            # Avoid flagging generic privacy pages; require an explicit RGPD/GDPR mention in prominent text.
            title = (p.title or "").strip()
            h1 = (p.h1[0] if p.h1 else "") if isinstance(p.h1, list) else ""
            if privacy_rgpd_re.search(title) or privacy_rgpd_re.search(h1):
                pages_have_high_ai_content_levels_set.add(eff)

    pages_have_high_ai_content_levels = sorted(pages_have_high_ai_content_levels_set)

    def _ps_dict(p: PageData) -> dict[str, Any] | None:
        ps = getattr(p, "pagespeed", None)
        if not isinstance(ps, dict):
            return None
        if ps.get("error"):
            return None
        return ps

    def _ps_cat_is_poor(value: str | None) -> bool:
        v = (value or "").strip().upper()
        return v in {"SLOW", "POOR"}

    def _ps_field_metric(p: PageData, metric: str) -> dict[str, Any] | None:
        ps = _ps_dict(p)
        if not ps:
            return None
        fm = ps.get("field_metrics")
        if not isinstance(fm, dict):
            return None
        node = fm.get(metric)
        return node if isinstance(node, dict) else None

    def _ps_lab_value(p: PageData, metric: str) -> float | None:
        ps = _ps_dict(p)
        if not ps:
            return None
        lm = ps.get("lab_metrics")
        if not isinstance(lm, dict):
            return None
        node = lm.get(metric)
        if not isinstance(node, dict):
            return None
        value = node.get("value")
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        return None

    def _ps_metric_is_poor(p: PageData, metric: str) -> bool:
        field = _ps_field_metric(p, metric)
        if field:
            cat = str(field.get("category") or "").strip()
            if _ps_cat_is_poor(cat):
                return True
            p75 = field.get("p75")
            if metric == "lcp" and isinstance(p75, int) and p75 > CWV_LCP_POOR_MS:
                return True
            if metric == "inp" and isinstance(p75, int) and p75 > CWV_INP_POOR_MS:
                return True
            if metric == "cls" and isinstance(p75, (int, float)) and float(p75) > CWV_CLS_POOR:
                return True

        if metric in CWV_FIELD_ONLY_METRICS:
            return False

        lab_val = _ps_lab_value(p, metric)
        if lab_val is None:
            return False
        if metric == "lcp" and lab_val > CWV_LCP_POOR_MS:
            return True
        if metric == "inp" and lab_val > CWV_INP_POOR_MS:
            return True
        if metric == "tbt" and lab_val > CWV_TBT_POOR_MS:
            return True
        if metric == "cls" and lab_val > CWV_CLS_POOR:
            return True
        return False

    def _ps_metric_is_ni(p: PageData, metric: str) -> bool:
        if _ps_metric_is_poor(p, metric):
            return False

        field = _ps_field_metric(p, metric)
        if field:
            p75 = field.get("p75")
            if metric == "lcp" and isinstance(p75, int) and CWV_LCP_GOOD_MS < p75 <= CWV_LCP_POOR_MS:
                return True
            if metric == "inp" and isinstance(p75, int) and CWV_INP_GOOD_MS < p75 <= CWV_INP_POOR_MS:
                return True
            if metric == "cls" and isinstance(p75, (int, float)) and CWV_CLS_GOOD < float(p75) <= CWV_CLS_POOR:
                return True
            cat = str(field.get("category") or "").strip().upper()
            if cat in {"AVERAGE", "NI", "NEEDS_IMPROVEMENT"}:
                return True

        if metric in CWV_FIELD_ONLY_METRICS:
            return False

        lab_val = _ps_lab_value(p, metric)
        if lab_val is None:
            return False
        if metric == "lcp" and CWV_LCP_GOOD_MS < lab_val <= CWV_LCP_POOR_MS:
            return True
        if metric == "inp" and CWV_INP_GOOD_MS < lab_val <= CWV_INP_POOR_MS:
            return True
        if metric == "tbt" and CWV_TBT_GOOD_MS < lab_val <= CWV_TBT_POOR_MS:
            return True
        if metric == "cls" and CWV_CLS_GOOD < lab_val <= CWV_CLS_POOR:
            return True
        return False

    pages_with_poor_lcp = [p.url for p in ok_html_pages if _ps_metric_is_poor(p, "lcp")]
    pages_with_poor_cls = [p.url for p in ok_html_pages if _ps_metric_is_poor(p, "cls")]
    pages_with_poor_inp = [p.url for p in ok_html_pages if _ps_metric_is_poor(p, "inp")]
    pages_with_poor_tbt = [p.url for p in ok_html_pages if _ps_metric_is_poor(p, "tbt")]
    pages_with_ni_lcp = [p.url for p in ok_html_pages if _ps_metric_is_ni(p, "lcp")]
    pages_with_ni_cls = [p.url for p in ok_html_pages if _ps_metric_is_ni(p, "cls")]
    pages_with_ni_tbt = [p.url for p in ok_html_pages if _ps_metric_is_ni(p, "tbt")]

    cwv_lcp_pages_to_fix = list(dict.fromkeys([*pages_with_poor_lcp, *pages_with_ni_lcp]))
    cwv_tbt_pages_to_fix = list(dict.fromkeys([*pages_with_poor_tbt, *pages_with_ni_tbt]))
    cwv_cls_pages_to_fix = list(dict.fromkeys([*pages_with_poor_cls, *pages_with_ni_cls]))

    slow_page_set: set[str] = set()
    speed_index_by_url: dict[str, float] = {}
    perf_score_by_url: dict[str, float] = {}
    for p in ok_html_pages:
        eff = _final_url(p)
        # Ahrefs-like: consider only canonical pages for performance issues.
        if _is_non_canonical(p):
            continue
        parts_eff = urlsplit(eff)
        # Ahrefs-like: ignore query-string variations for performance issues.
        if parts_eff.query:
            continue
        ps = _ps_dict(p)
        if ps:
            overall = str(ps.get("overall_category") or "").strip()
            perf_score = ps.get("performance_score")
            if isinstance(perf_score, (int, float)):
                perf_score_by_url[eff] = float(perf_score)
            lab_ttfb = _ps_lab_value(p, "ttfb")
            lab_speed_index = _ps_lab_value(p, "speed_index")
            if isinstance(lab_speed_index, (int, float)):
                speed_index_by_url[eff] = float(lab_speed_index)
            if _ps_metric_is_poor(p, "inp"):
                slow_page_set.add(eff)
            elif _ps_cat_is_poor(overall):
                slow_page_set.add(eff)
            elif isinstance(lab_ttfb, (int, float)) and float(lab_ttfb) > SLOW_PAGE_MS:
                slow_page_set.add(eff)
            continue
        if isinstance(p.elapsed_ms, int) and p.elapsed_ms > SLOW_PAGE_MS:
            slow_page_set.add(eff)

    # Ahrefs-like fallback: even when no page crosses our absolute thresholds (often due to CDN / location),
    # still flag the slowest page by Lighthouse Speed Index when it is meaningfully high.
    if not slow_page_set and speed_index_by_url:
        # Fallback (best-effort): if nothing is flagged by thresholds, pick the slowest *non-root* canonical page
        # by Speed Index *only when it is a clear outlier* (helps match Ahrefs when crawl timings differ).
        non_root: dict[str, float] = {}
        for u, si in speed_index_by_url.items():
            # If Lighthouse thinks the page is fast (high perf score), don't flag it as slow.
            if perf_score_by_url.get(u, 100.0) > 80.0:
                continue
            parts = urlsplit(u)
            if parts.query:
                continue
            if (parts.path or "/") in {"/", ""}:
                continue
            if re.fullmatch(r"/[a-z]{2}(-[a-z0-9]{2,8})?/?", parts.path or "", re.IGNORECASE):
                continue
            non_root[u] = si
        if non_root:
            values = sorted(non_root.values())
            n = len(values)
            if n % 2 == 1:
                median_si = values[n // 2]
            else:
                median_si = (values[n // 2 - 1] + values[n // 2]) / 2.0
            max_si = values[-1]
            if median_si > 0 and max_si >= 2000 and (max_si / median_si) >= 1.8:
                for u, si in non_root.items():
                    if si == max_si:
                        slow_page_set.add(u)

    slow_page = sorted(slow_page_set)

    # --- SERP heuristics (best-effort) ---
    # Ahrefs uses SERP titles from their own data sources. Locally we approximate:
    # - sometimes, long titles containing competitor lists are rewritten to a shorter variant
    # - home page titles are sometimes rewritten based on prominent on-page headings (H1)
    page_and_serp_titles_do_not_match_set: set[str] = set()

    base_host = ""
    for p in ok_html_pages:
        host = urlsplit(_final_url(p)).hostname or ""
        if host:
            base_host = host.lower()
            break
    if base_host.startswith("www."):
        base_host = base_host[4:]
    brand_root = (base_host.split(".", 1)[0] if base_host else "").strip().lower()
    brand_key = re.sub(r"[^0-9a-z]+", "", brand_root)
    host_tokens = [t for t in re.split(r"[^0-9a-z]+", brand_root) if t]
    host_tokens = [t for t in host_tokens if len(t) >= 3 and t not in {"www", "site", "app"}]

    def _seg_has_brand(seg: str) -> bool:
        if not brand_key:
            return False
        s = re.sub(r"[^0-9A-Za-zÀ-ÖØ-öø-ÿ]+", "", (seg or "").lower())
        return brand_key in s

    def _strip_brand(value: str | None) -> str:
        t = re.sub(r"\s+", " ", (value or "")).strip()
        if not t:
            return ""
        parts = [p.strip() for p in re.split(r"\s*[|—–]\s*", t) if p and p.strip()]
        if len(parts) < 2 and " - " in t:
            parts = [p.strip() for p in t.split(" - ") if p and p.strip()]
        if len(parts) >= 2:
            if _seg_has_brand(parts[0]) and len(parts) > 1:
                parts = parts[1:]
            if _seg_has_brand(parts[-1]) and len(parts) > 1:
                parts = parts[:-1]
        return " | ".join(parts).strip() if parts else t

    def _cmp_key(value: str | None) -> str:
        return re.sub(r"[^0-9A-Za-zÀ-ÖØ-öø-ÿ]+", "", (value or "").lower())

    WORD_RE = re.compile(r"[0-9A-Za-zÀ-ÖØ-öø-ÿ]+")

    def _token_set(value: str | None) -> set[str]:
        return {w.lower() for w in WORD_RE.findall(value or "")}

    def _jaccard(a: set[str], b: set[str]) -> float:
        if not a or not b:
            return 0.0
        return len(a & b) / len(a | b)

    def _looks_competitor_list(title: str | None) -> bool:
        t = (title or "").strip()
        if not t:
            return False
        # Typical pattern on review pages: "(InVideo, Pictory)" etc.
        for seg in re.findall(r"\(([^)]{0,120})\)", t):
            if "," not in seg:
                continue
            items = [s.strip() for s in seg.split(",") if s.strip()]
            if len(items) < 2:
                continue
            brandish = sum(1 for s in items if re.match(r"^[A-Z0-9]", s or ""))
            if brandish >= 2:
                return True
        return False

    for p in indexable_html_pages:
        title = _strip_brand(p.title)
        og_title = _strip_brand(p.og_title)
        h1 = (p.h1[0] if p.h1 else None) if isinstance(p.h1, list) else None

        parts = urlsplit(_final_url(p))
        is_root = parts.path in {"", "/"}
        is_locale_root = bool(re.fullmatch(r"/[a-z]{2}(-[a-z0-9]{2,8})?/?", parts.path or "", re.IGNORECASE))
        if (is_root or is_locale_root) and _non_empty(title) and _non_empty(h1):
            # Ahrefs-like: for home/locale-root pages, SERP titles are often rewritten to a shorter variant.
            # A strong signal is when OG title is a trimmed subset of <title> that drops a trailing update marker.
            if _non_empty(og_title) and _cmp_key(title) != _cmp_key(og_title):
                raw_title = str(p.title or "").strip()
                raw_og = str(p.og_title or "").strip()
                raw_title_cmp = _cmp_key(raw_title)
                raw_og_cmp = _cmp_key(raw_og)
                if raw_og_cmp and raw_og_cmp in raw_title_cmp:
                    has_update_marker = bool(
                        re.search(r"\b(maj|mise\s*à\s*jour|updated|update)\b", raw_title, re.IGNORECASE)
                    )
                    has_trailing_parens = bool(re.search(r"\([^)]{4,}\)\s*$", raw_title))
                    if has_update_marker or (has_trailing_parens and (len(raw_title) - len(raw_og)) >= 10):
                        page_and_serp_titles_do_not_match_set.add(_final_url(p))
                        continue

            # Locale roots (e.g. /en/) often have SERP titles rewritten with a site name derived from the hostname.
            # If the title is missing an obvious hostname token, assume it won't match the SERP title.
            lang = (p.lang or "").strip().lower()
            if is_locale_root and host_tokens and (lang.startswith("en") or (parts.path or "").startswith("/en/")):
                raw_tset = _token_set(p.title)
                missing = [tok for tok in host_tokens if tok not in raw_tset]
                if missing:
                    # Reduce false positives on domains like "*-avis.com": missing "avis" alone doesn't reliably imply a SERP rewrite.
                    if missing == ["avis"] and lang.startswith("en"):
                        if re.search(r"\breviews\b", title, re.IGNORECASE):
                            page_and_serp_titles_do_not_match_set.add(_final_url(p))
                            continue
                    else:
                        page_and_serp_titles_do_not_match_set.add(_final_url(p))
                        continue
            # If neither contains the other, assume SERP likely differs from the page title.
            if _cmp_key(h1) not in _cmp_key(title) and _cmp_key(title) not in _cmp_key(h1):
                # Reduce false positives: require low token overlap between title and H1.
                overlap = _jaccard(_token_set(title), _token_set(h1))
                # Heuristic: if overlap is *too* low, it's likely just different copy, not a SERP rewrite.
                if 0.26 <= overlap < 0.33:
                    page_and_serp_titles_do_not_match_set.add(_final_url(p))
            continue

        # For non-home pages, only flag when the title likely contains a competitor list that gets dropped in SERPs.
        # In practice this rewrite signal is much more reliable for English SERPs; keep it conservative to match Ahrefs.
        if _non_empty(title) and _non_empty(og_title) and _cmp_key(title) != _cmp_key(og_title):
            lang = (p.lang or "").strip().lower()
            if (lang.startswith("en") or (urlsplit(_final_url(p)).path or "").startswith("/en/")) and _looks_competitor_list(
                title
            ):
                page_and_serp_titles_do_not_match_set.add(_final_url(p))

    page_and_serp_titles_do_not_match = sorted(page_and_serp_titles_do_not_match_set)

    duplicate_pages_without_canonical = [
        p.url
        for p in ok_html_pages
        if not _non_empty(p.canonical)
        and (
            (_non_empty(p.title) and title_counts.get(p.title.strip(), 0) > 1)
            or (_non_empty(p.meta_description) and description_counts.get(p.meta_description.strip(), 0) > 1)
        )
    ]

    # --- Social tags ---
    open_graph_missing: list[str] = []
    open_graph_incomplete: list[str] = []
    open_graph_url_mismatch: list[str] = []
    twitter_missing: list[str] = []
    twitter_incomplete: list[str] = []

    # Use final URL de-duplication to avoid counting redirect probes (http↔https, www↔non-www) as separate pages.
    for p in ok_html_by_eff.values():
        eff = _final_url(p)
        # Ahrefs-like: Open Graph is "missing" when no OG tags are present at all.
        og_any = [p.og_title, p.og_description, p.og_image, p.og_url, p.og_type]
        og_any_present = any(_non_empty(v) for v in og_any)
        if not og_any_present:
            open_graph_missing.append(eff)
        else:
            # Ahrefs-like: treat OG as incomplete/invalid when any required attribute is missing.
            # Ahrefs seems to expect og:type in addition to title/description/image/url.
            og_required = [p.og_title, p.og_description, p.og_image, p.og_url, p.og_type]
            if any(not _non_empty(v) for v in og_required):
                open_graph_incomplete.append(eff)

        if p.og_url and p.canonical and _norm_self(p.og_url) and _norm_self(p.canonical):
            if _norm_self(p.og_url) != _norm_self(p.canonical):
                open_graph_url_mismatch.append(eff)

        # Ahrefs-like: be conservative about Twitter Card. If Open Graph is present, don't require Twitter tags.
        has_any_twitter = bool(_non_empty(p.twitter_title) or _non_empty(p.twitter_description) or _non_empty(p.twitter_image))
        if not _non_empty(p.twitter_card):
            if has_any_twitter or not og_any_present:
                twitter_missing.append(eff)
        else:
            # Ahrefs-like heuristic: allow Open Graph + standard meta fallbacks.
            tw_title = p.twitter_title or p.og_title or p.title
            tw_desc = p.twitter_description or p.og_description or p.meta_description
            tw_img = p.twitter_image or p.og_image
            if not (_non_empty(tw_title) and _non_empty(tw_desc) and _non_empty(tw_img)):
                twitter_incomplete.append(eff)

    # --- Localization (hreflang + lang) ---
    LANG_RE = re.compile(r"^[a-z]{2}(-[a-z0-9]{2,8})*$", re.IGNORECASE)
    HREFLANG_RE = re.compile(r"^(x-default|[a-z]{2}(-[a-z0-9]{2,8})*)$", re.IGNORECASE)

    html_lang_missing = [p.url for p in ok_html_pages if not _non_empty(p.lang)]
    html_lang_invalid = [p.url for p in ok_html_pages if _non_empty(p.lang) and not LANG_RE.match(p.lang.strip())]

    def _hreflang_map_for(p: PageData) -> dict[str, str]:
        if p.hreflang:
            return p.hreflang
        if not sitemap_hreflang:
            return {}
        candidates = [
            _final_url(p),
            _norm_self(p.url) or p.url,
            _norm_self(p.final_url) if p.final_url else None,
        ]
        for key in candidates:
            if not key:
                continue
            key_norm = _norm_self(key) or key
            if key_norm in sitemap_hreflang:
                return sitemap_hreflang[key_norm]
        return {}

    # Ahrefs-like: hreflang issues can be derived from sitemap hreflang tags even when a page timed out.
    hreflang_source_pages: list[PageData] = []
    for p in pages:
        try:
            if _hreflang_map_for(p):
                hreflang_source_pages.append(p)
        except Exception:
            continue

    hreflang_defined_but_html_lang_missing = [
        p.url for p in ok_html_pages if _hreflang_map_for(p) and not _non_empty(p.lang)
    ]

    hreflang_annotation_invalid: list[str] = []
    hreflang_x_default_missing_set: set[str] = set()
    hreflang_url_to_redirect_or_broken: list[str] = []
    hreflang_to_non_canonical: list[str] = []
    hreflang_referenced_multi_lang: list[str] = []
    missing_reciprocal_hreflang_set: set[str] = set()

    for p in hreflang_source_pages:
        hreflang = _hreflang_map_for(p)
        if not hreflang:
            continue

        # Ahrefs-like: avoid flagging hreflang target quality issues on non-canonical duplicates.
        # The canonical URL should be the one carrying the hreflang annotations.
        if _is_non_canonical(p):
            continue

        # Ahrefs-like: treat x-default missing as relevant only for indexable pages.
        if "x-default" not in hreflang and _is_indexable(p):
            hreflang_x_default_missing_set.add(_final_url(p))

        # Same URL referenced for multiple languages within one page.
        #
        # Ahrefs-like: ignore region variants that share the same primary language (e.g. fr + fr-FR → same URL),
        # but flag when the *same* URL is referenced for multiple different primary languages (e.g. fr + en → same URL).
        url_to_primary_langs: dict[str, set[str]] = defaultdict(set)
        for code, href in hreflang.items():
            code_norm = str(code or "").strip().lower()
            if code_norm == "x-default":
                continue
            href_norm = _norm_self(href) or str(href or "").strip()
            primary = code_norm.split("-", 1)[0] if code_norm else ""
            if href_norm and primary:
                url_to_primary_langs[href_norm].add(primary)
        if any(len(codes) > 1 for codes in url_to_primary_langs.values()):
            hreflang_referenced_multi_lang.append(p.url)

        invalid = False
        any_redirect_or_broken = False
        any_non_canonical = False
        for code, href in hreflang.items():
            if not HREFLANG_RE.match(code):
                invalid = True
                continue
            if not _non_empty(href):
                invalid = True
                continue
            t = page_by_any.get(_norm_self(href) or href)
            if t:
                if _is_redirect(t) or _is_timeout(t) or (isinstance(t.status_code, int) and t.status_code >= 400):
                    any_redirect_or_broken = True
                if _is_non_canonical(t):
                    any_non_canonical = True
        # Avoid double-counting: redirect/broken is a more specific condition than non-canonical.
        if any_redirect_or_broken:
            hreflang_url_to_redirect_or_broken.append(p.url)
        elif any_non_canonical:
            hreflang_to_non_canonical.append(p.url)
        if invalid:
            hreflang_annotation_invalid.append(p.url)

        # Reciprocal check (heuristic): target should reference this page back in its hreflang set.
        #
        # Ahrefs-like: ignore non-canonical duplicates as sources, otherwise the canonical URL gets falsely flagged
        # (e.g. /blog canonicalizes to /blog.html, but /blog.html doesn't reference /blog back).
        if _is_non_canonical(p):
            continue
        this_eff = _final_url(p)
        this_key = _norm_self(p.canonical) or this_eff
        for href in hreflang.values():
            t = page_by_any.get(_norm_self(href) or href)
            if not t:
                continue
            t_hreflang = _hreflang_map_for(t)
            if not t_hreflang:
                missing_reciprocal_hreflang_set.add(this_key)
                break
            back_refs = {_norm_self(u) or u for u in t_hreflang.values()}
            if (_norm_self(this_eff) or this_eff) not in back_refs:
                missing_reciprocal_hreflang_set.add(this_key)
                break

        # Ahrefs-like: x-default should generally be consistent across alternates in a hreflang group.
        # When alternates disagree on the x-default target (including "missing on one side"), flag as missing reciprocal
        # (matches Ahrefs UI behavior across multiple projects).
        if this_key not in missing_reciprocal_hreflang_set:
            x_default = _norm_self(hreflang.get("x-default"))
            for href in hreflang.values():
                t = page_by_any.get(_norm_self(href) or href)
                if not t:
                    continue
                t_hreflang = _hreflang_map_for(t)
                if not t_hreflang:
                    continue
                t_x_default = _norm_self(t_hreflang.get("x-default"))
                if (x_default or t_x_default) and t_x_default != x_default:
                    missing_reciprocal_hreflang_set.add(this_key)
                    break

    missing_reciprocal_hreflang = sorted(missing_reciprocal_hreflang_set)

    # Semrush-like: hreflang conflicts within page source code.
    hreflang_conflicts_within_page_source_code: list[dict[str, Any]] = []
    for p in ok_html_pages:
        raw = getattr(p, "hreflang_raw", None)
        if not isinstance(raw, list) or not raw:
            continue
        m: dict[str, set[str]] = defaultdict(set)
        for it in raw:
            if not isinstance(it, dict):
                continue
            code = str(it.get("hreflang") or "").strip().lower()
            href = str(it.get("href") or "").strip()
            if not code or not href:
                continue
            m[code].add(href)
        conflicts: list[dict[str, Any]] = []
        # 1) Duplicate hreflang values pointing to different URLs.
        conflicts.extend(
            [{"hreflang": code, "hrefs": sorted(list(hrefs)), "reason": "duplicate_hreflang_value"} for code, hrefs in m.items() if len(hrefs) > 1]
        )
        # 2) Canonical mismatch (common on filtered/query pages): hreflang points to canonical but not to the page URL.
        if _is_non_canonical(p):
            hrefs_all = {h for hs in m.values() for h in hs}
            self_u = _final_url(p)
            canon = _norm_self(p.canonical)
            if canon and canon in hrefs_all and self_u not in hrefs_all:
                conflicts.append({"hreflang": "*", "hrefs": sorted(list(hrefs_all))[:10], "reason": "hreflang_points_to_canonical_not_self"})
        if conflicts:
            hreflang_conflicts_within_page_source_code.append({"url": _final_url(p), "conflicts": conflicts})

    # Ahrefs-like: more than one page for same language in hreflang.
    #
    # Model: build a graph of *canonical* pages connected by their hreflang alternates.
    # Include x-default targets because Ahrefs appears to treat it as part of the hreflang group.
    #
    # This avoids inflating the issue with non-canonical duplicates (e.g. URL variants that canonicalize to a page),
    # while still catching cases where many canonical pages are incorrectly connected via a shared x-default.
    more_than_one_page_same_lang: set[str] = set()

    canonical_pages: dict[str, PageData] = {}
    for p in ok_html_pages:
        if _is_non_canonical(p):
            continue
        canon = _norm_self(p.canonical) if p.canonical else _final_url(p)
        if canon:
            canonical_pages[canon] = p

    nodes = set(canonical_pages.keys())
    graph: dict[str, set[str]] = {n: set() for n in nodes}

    def _canonical_node_for_url(u: str) -> str | None:
        u_norm = _norm_self(u) or u
        if u_norm in nodes:
            return u_norm
        t = page_by_any.get(u_norm)
        if not t or _is_non_canonical(t):
            return None
        canon = _norm_self(t.canonical) if t.canonical else _final_url(t)
        if canon in nodes:
            return canon
        return None

    for canon_url, p in canonical_pages.items():
        hreflang = _hreflang_map_for(p)
        if not hreflang:
            continue
        for href in hreflang.values():
            if not href:
                continue
            dst = _canonical_node_for_url(href)
            if not dst or dst == canon_url:
                continue
            graph[canon_url].add(dst)
            graph[dst].add(canon_url)

    visited: set[str] = set()
    for start in nodes:
        if start in visited or not graph.get(start):
            continue
        component: list[str] = []
        stack = [start]
        visited.add(start)
        while stack:
            cur = stack.pop()
            component.append(cur)
            for nb in graph.get(cur, set()):
                if nb not in visited:
                    visited.add(nb)
                    stack.append(nb)

        lang_counts: Counter[str] = Counter()
        for u in component:
            pg = canonical_pages.get(u)
            lang = (pg.lang or "").strip().lower() if pg else ""
            primary = lang.split("-", 1)[0] if lang else ""
            if primary:
                lang_counts[primary] += 1

        if any(c > 1 for c in lang_counts.values()):
            more_than_one_page_same_lang.update(component)

    # Ahrefs-like: don't count root / locale-root pages for this issue (prevents false positives on x-default hubs).
    more_than_one_page_same_lang_filtered: set[str] = set()
    for u in more_than_one_page_same_lang:
        parts = urlsplit(u)
        path = parts.path or "/"
        if path in {"", "/"}:
            continue
        if re.fullmatch(r"/[a-z]{2}(-[a-z0-9]{2,8})?/?", path, re.IGNORECASE):
            continue
        more_than_one_page_same_lang_filtered.add(u)

    more_than_one_page_for_same_language_in_hreflang = sorted(more_than_one_page_same_lang_filtered)

    # --- Structured data (schema.org) ---
    # Ahrefs-like: be conservative to avoid false positives.
    # - schema.org validation errors: only hard parse/type errors
    # - Google rich results errors: only a small subset of rich-results-like required-field issues
    SCHEMA_ORG_HARD_ERRORS = {"invalid_json", "missing_type"}
    RICH_RESULTS_ERRORS = {
        "faq_mainEntity_missing",
        "faq_question_invalid",
        "faq_question_name_missing",
        "faq_answer_missing",
        "offer_price_is_string",
    }

    structured_data_schema_org_errors_set: set[str] = set()
    structured_data_google_rich_results_errors_set: set[str] = set()
    for p in ok_html_pages:
        errs = getattr(p, "schema_org_errors", None)
        if not isinstance(errs, list) or not errs:
            continue
        err_set = {str(e).strip() for e in errs if isinstance(e, str) and str(e).strip()}
        if err_set & SCHEMA_ORG_HARD_ERRORS:
            structured_data_schema_org_errors_set.add(p.url)
        if err_set & RICH_RESULTS_ERRORS:
            structured_data_google_rich_results_errors_set.add(p.url)

    structured_data_schema_org_errors = sorted(structured_data_schema_org_errors_set)
    structured_data_google_rich_results_errors = sorted(structured_data_google_rich_results_errors_set)

    # --- Sitemaps (best-effort based on loaded URLs) ---
    sitemap_urls = sitemap_urls or set()

    # Ahrefs-like: "Page in multiple sitemaps" (best-effort). A page is flagged if it appears in more than one
    # URLset sitemap file that we successfully parsed.
    page_in_multiple_sitemaps: list[str] = []
    if sitemap_urlsets:
        url_to_sitemaps: dict[str, set[str]] = {}
        url_to_occurrences: Counter[str] = Counter()
        for _sitemap_url, _locs in sitemap_urlsets.items():
            if not isinstance(_sitemap_url, str) or not _sitemap_url.strip():
                continue
            if not isinstance(_locs, list):
                continue
            for _loc in _locs:
                if isinstance(_loc, str) and _loc.strip():
                    url_to_sitemaps.setdefault(_loc, set()).add(_sitemap_url)
                    url_to_occurrences[_loc] += 1
        # Ahrefs export sometimes flags duplicates when a URL appears twice in the *same* sitemap file.
        page_in_multiple_sitemaps = sorted(
            [u for u, sms in url_to_sitemaps.items() if (len(sms) > 1) or (url_to_occurrences.get(u, 0) > 1)]
        )

    sitemap_3xx: list[str] = []
    sitemap_4xx: list[str] = []
    sitemap_5xx: list[str] = []
    sitemap_noindex: list[str] = []
    sitemap_non_canonical: list[str] = []
    sitemap_timed_out: list[str] = []

    for u in sitemap_urls:
        u_norm = _norm_self(u) or u
        p = page_by_any.get(u_norm)
        if not p:
            continue

        is_timeout = _is_timeout(p)
        is_redirect = bool(_is_redirect(p) and (u_norm in page_by_requested))
        is_4xx = bool(isinstance(p.status_code, int) and 400 <= p.status_code < 500)
        is_5xx = bool(isinstance(p.status_code, int) and 500 <= p.status_code < 600)
        is_noindex = bool(_looks_noindex(p.meta_robots) or _looks_noindex(p.x_robots_tag))

        if is_timeout:
            sitemap_timed_out.append(u_norm)
        if is_redirect:
            sitemap_3xx.append(u_norm)
        if is_4xx:
            sitemap_4xx.append(u_norm)
        if is_5xx:
            sitemap_5xx.append(u_norm)
        if is_noindex:
            sitemap_noindex.append(u_norm)
        # Avoid double-counting: only report "non-canonical in sitemap" for otherwise OK URLs.
        if _is_non_canonical(p) and not (is_timeout or is_redirect or is_4xx or is_5xx or is_noindex):
            sitemap_non_canonical.append(u_norm)

    indexable_not_in_sitemap: list[str] = []
    sitemap_set_norm = {_norm_self(u) or u for u in sitemap_urls}
    for p in effective_pages.values():
        if not _is_indexable(p):
            continue
        eff = _final_url(p)
        if sitemap_set_norm and eff not in sitemap_set_norm:
            indexable_not_in_sitemap.append(eff)

    issues: dict[str, dict[str, Any]] = {}

    # Existing keys (keep stable)
    issues["bad_status"] = _issue_block("bad_status", bad_status)
    issues["blocked_by_robots"] = _issue_block("blocked_by_robots", blocked)
    issues["missing_title"] = _issue_block("missing_title", missing_title)
    issues["missing_meta_description"] = _issue_block("missing_meta_description", missing_description)
    issues["missing_h1_indexable"] = _issue_block("missing_h1_indexable", missing_h1_indexable)
    issues["missing_h1_not_indexable"] = _issue_block("missing_h1_not_indexable", missing_h1_not_indexable)
    issues["multiple_h1"] = _issue_block("multiple_h1", multiple_h1)
    issues["missing_canonical"] = _issue_block("missing_canonical", missing_canonical)
    # Ahrefs-like: count = number of affected URLs, not number of duplicated values.
    issues["duplicate_titles"] = {
        "count": int(sum(int(c) for c in duplicate_titles.values())),
        "top": sorted(duplicate_titles.items(), key=lambda kv: (-kv[1], kv[0]))[:20],
        "examples": duplicate_title_examples[:ISSUE_EXAMPLES_LIMIT],
    }
    issues["duplicate_meta_descriptions"] = {
        "count": int(sum(int(c) for c in duplicate_descriptions.values())),
        "top": sorted(duplicate_descriptions.items(), key=lambda kv: (-kv[1], kv[0]))[:20],
        "examples": duplicate_description_examples[:ISSUE_EXAMPLES_LIMIT],
    }
    _write_issue_rows(issues_dir, "duplicate_titles", duplicate_title_rows)
    _write_issue_rows(issues_dir, "duplicate_meta_descriptions", duplicate_description_rows)

    # Semrush-like issues (mega export)
    issues["permanent_redirects"] = _issue_block("permanent_redirects", sorted(set(permanent_redirects)))
    issues["low_text_to_html_ratio"] = _issue_block("low_text_to_html_ratio", sorted(set(low_text_to_html_ratio)))
    issues["incorrect_pages_found_in_sitemap_xml"] = _issue_block(
        "incorrect_pages_found_in_sitemap_xml", sorted(set(incorrect_pages_found_in_sitemap_xml))
    )
    issues["orphaned_sitemap_pages"] = _issue_block("orphaned_sitemap_pages", sorted(set(orphaned_sitemap_pages)))

    # Ahrefs: "Pages to submit to IndexNow" appears to focus on recently changed, high-level pages.
    # We'll compute it during change tracking when a previous crawl is available; default to empty otherwise.
    issues["pages_to_submit_to_indexnow"] = _issue_block("pages_to_submit_to_indexnow", [])

    # Ahrefs-like categories (subset implemented)
    issues["internal_pages"] = {"count": len(requested_html_pages), "examples": []}
    issues["http_404"] = _issue_block("http_404", http_404)
    issues["http_4xx"] = _issue_block("http_4xx", http_4xx)
    issues["http_500"] = _issue_block("http_500", http_500)
    issues["http_5xx"] = _issue_block("http_5xx", http_5xx)
    issues["timed_out"] = _issue_block("timed_out", timeouts)

    issues["redirect_loop"] = _issue_block("redirect_loop", redirect_loop)
    issues["redirect_3xx"] = _issue_block("redirect_3xx", redirect_3xx)
    issues["redirect_302"] = _issue_block("redirect_302", redirect_302)
    issues["broken_redirect"] = _issue_block("broken_redirect", broken_redirect)
    issues["redirect_chain"] = _issue_block("redirect_chain", redirect_chain)
    issues["redirect_chain_too_long"] = _issue_block("redirect_chain_too_long", redirect_chain_too_long)
    issues["http_to_https_redirect"] = _issue_block("http_to_https_redirect", http_to_https_redirect)
    issues["https_to_http_redirect"] = _issue_block("https_to_http_redirect", https_to_http_redirect)
    issues["meta_refresh_redirect"] = _issue_block("meta_refresh_redirect", meta_refresh_redirect)

    issues["noindex_page"] = _issue_block("noindex_page", noindex_page)
    issues["nofollow_page"] = _issue_block("nofollow_page", nofollow_page)
    issues["noindex_in_html_and_http_header"] = _issue_block("noindex_in_html_and_http_header", noindex_in_html_and_http_header)
    issues["nofollow_in_html_and_http_header"] = _issue_block("nofollow_in_html_and_http_header", nofollow_in_html_and_http_header)
    issues["noindex_and_nofollow_page"] = _issue_block("noindex_and_nofollow_page", noindex_and_nofollow_page)
    issues["noindex_follow_page"] = _issue_block("noindex_follow_page", noindex_follow_page)
    issues["canonical_points_to_4xx"] = _issue_block("canonical_points_to_4xx", canonical_points_to_4xx)
    issues["canonical_points_to_5xx"] = _issue_block("canonical_points_to_5xx", canonical_points_to_5xx)
    issues["canonical_points_to_redirect"] = _issue_block("canonical_points_to_redirect", canonical_points_to_redirect)
    issues["non_canonical_page_specified_as_canonical_one"] = _issue_block(
        "non_canonical_page_specified_as_canonical_one", non_canonical_specified_as_canonical
    )
    issues["canonical_from_http_to_https"] = _issue_block("canonical_from_http_to_https", canonical_from_http_to_https)
    issues["canonical_from_https_to_http"] = _issue_block("canonical_from_https_to_http", canonical_from_https_to_http)

    def _is_indexable_url(u: str) -> bool:
        u_norm = _norm_self(u) or u
        p = page_by_any.get(u_norm) or effective_pages.get(u_norm)
        return bool(p and _is_indexable(p))

    def _split_url_list(urls: list[str]) -> tuple[list[str], list[str]]:
        indexable: list[str] = []
        not_indexable: list[str] = []
        for u in urls:
            (indexable if _is_indexable_url(u) else not_indexable).append(u)
        return indexable, not_indexable

    def _split_source_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        indexable: list[dict[str, Any]] = []
        not_indexable: list[dict[str, Any]] = []
        for row in rows:
            src = row.get("source_url")
            if isinstance(src, str) and src:
                (indexable if _is_indexable_url(src) else not_indexable).append(row)
            else:
                not_indexable.append(row)
        return indexable, not_indexable

    issues["canonical_url_has_no_incoming_internal_links"] = _issue_block(
        "canonical_url_has_no_incoming_internal_links", canonical_no_incoming
    )

    # Ahrefs-like: report link-related issues separately for indexable vs not indexable pages.
    orphan_indexable, orphan_not_indexable = _split_url_list(orphan_pages)
    issues["orphan_page_indexable"] = _issue_block("orphan_page_indexable", orphan_indexable)
    issues["orphan_page_not_indexable"] = _issue_block("orphan_page_not_indexable", orphan_not_indexable)

    one_df_indexable, one_df_not_indexable = _split_url_list(only_one_dofollow_incoming)
    issues["page_has_only_one_dofollow_incoming_internal_link"] = _issue_block(
        "page_has_only_one_dofollow_incoming_internal_link", sorted(set(only_one_dofollow_incoming))
    )
    issues["page_has_only_one_dofollow_incoming_internal_link_indexable"] = _issue_block(
        "page_has_only_one_dofollow_incoming_internal_link_indexable", one_df_indexable
    )
    issues["page_has_only_one_dofollow_incoming_internal_link_not_indexable"] = _issue_block(
        "page_has_only_one_dofollow_incoming_internal_link_not_indexable", one_df_not_indexable
    )

    nf_only_indexable, nf_only_not_indexable = _split_url_list(nofollow_incoming_only)
    issues["page_has_nofollow_incoming_internal_links_only_indexable"] = _issue_block(
        "page_has_nofollow_incoming_internal_links_only_indexable", nf_only_indexable
    )
    issues["page_has_nofollow_incoming_internal_links_only_not_indexable"] = _issue_block(
        "page_has_nofollow_incoming_internal_links_only_not_indexable", nf_only_not_indexable
    )

    nf_mix_indexable, nf_mix_not_indexable = _split_url_list(nofollow_and_dofollow_incoming)
    issues["page_has_nofollow_and_dofollow_incoming_internal_links_indexable"] = _issue_block(
        "page_has_nofollow_and_dofollow_incoming_internal_links_indexable", nf_mix_indexable
    )
    issues["page_has_nofollow_and_dofollow_incoming_internal_links_not_indexable"] = _issue_block(
        "page_has_nofollow_and_dofollow_incoming_internal_links_not_indexable", nf_mix_not_indexable
    )

    issues["redirected_page_has_no_incoming_internal_links"] = _issue_block(
        "redirected_page_has_no_incoming_internal_links", redirected_page_no_incoming
    )

    broken_indexable, broken_not_indexable = _split_source_rows(links_to_broken)
    issues["page_has_links_to_broken_page_indexable"] = _issue_block(
        "page_has_links_to_broken_page_indexable", broken_indexable
    )
    issues["page_has_links_to_broken_page_not_indexable"] = _issue_block(
        "page_has_links_to_broken_page_not_indexable", broken_not_indexable
    )
    # Semrush-like: group all broken internal links regardless of indexability.
    broken_union: dict[str, set[str]] = defaultdict(set)
    for row in (broken_indexable + broken_not_indexable):
        if not isinstance(row, dict):
            continue
        src = str(row.get("source_url") or "").strip()
        tgts = row.get("targets")
        if not src or not isinstance(tgts, list):
            continue
        for t in tgts:
            if isinstance(t, str) and t.strip():
                broken_union[src].add(t.strip())
    broken_union_rows = [
        {"source_url": src, "targets": sorted(tgts)[:50]} for src, tgts in sorted(broken_union.items()) if tgts
    ]
    issues["broken_internal_links"] = _issue_block("broken_internal_links", broken_union_rows)

    # Ahrefs-like: per-link exports ("... - links") for broken targets.
    broken_link_rows_indexable: list[dict[str, Any]] = []
    broken_link_rows_not_indexable: list[dict[str, Any]] = []
    seen_broken_link_idx: set[tuple[str, str, bool]] = set()
    seen_broken_link_no: set[tuple[str, str, bool]] = set()
    for p in ok_html_pages:
        source = _final_url(p)
        is_idx = _is_indexable(p)
        for it in getattr(p, "internal_link_items", []) or []:
            if not isinstance(it, dict):
                continue
            target = str(it.get("target_url") or "").strip()
            if not target:
                continue
            tgt = page_by_any.get(_norm_self(target) or target)
            if not tgt:
                continue
            if not (isinstance(tgt.status_code, int) and tgt.status_code >= 400):
                continue
            target_norm = _norm_self(target) or target
            nofollow = bool(it.get("nofollow"))
            row = {
                "source_url": source,
                "target_url": target_norm,
                "nofollow": nofollow,
                "anchor_text": str(it.get("anchor_text") or "").strip(),
            }
            if is_idx:
                key = (source, target_norm, nofollow)
                if key not in seen_broken_link_idx:
                    seen_broken_link_idx.add(key)
                    broken_link_rows_indexable.append(row)
            else:
                key = (source, target_norm, nofollow)
                if key not in seen_broken_link_no:
                    seen_broken_link_no.add(key)
                    broken_link_rows_not_indexable.append(row)

    issues["page_has_links_to_broken_page_links_indexable"] = _issue_block(
        "page_has_links_to_broken_page_links_indexable", broken_link_rows_indexable
    )
    issues["page_has_links_to_broken_page_links_not_indexable"] = _issue_block(
        "page_has_links_to_broken_page_links_not_indexable", broken_link_rows_not_indexable
    )

    # Ahrefs-like: "Page has links to redirect" counts *pages* (unique source URLs), not individual links.
    pages_linking_to_redirect = sorted(
        {
            str(r.get("source_url") or "").strip()
            for r in (links_to_redirect or [])
            if isinstance(r, dict) and isinstance(r.get("source_url"), str) and str(r.get("source_url") or "").strip()
        }
    )
    redir_indexable, redir_not_indexable = _split_url_list(pages_linking_to_redirect)
    issues["page_has_links_to_redirect_indexable"] = _issue_block("page_has_links_to_redirect_indexable", redir_indexable)
    issues["page_has_links_to_redirect_not_indexable"] = _issue_block("page_has_links_to_redirect_not_indexable", redir_not_indexable)

    # Ahrefs-like: per-link exports for redirect targets (split by indexable vs not indexable source pages).
    redirect_link_rows_indexable: list[dict[str, Any]] = []
    redirect_link_rows_not_indexable: list[dict[str, Any]] = []
    seen_redirect_link_idx: set[tuple[str, str, bool]] = set()
    seen_redirect_link_no: set[tuple[str, str, bool]] = set()
    for p in ok_html_pages:
        is_idx = _is_indexable(p)
        source = _final_url(p)
        for it in getattr(p, "internal_link_items", []) or []:
            if not isinstance(it, dict):
                continue
            target = str(it.get("target_url") or "").strip()
            if not target:
                continue
            tgt_req = page_by_requested.get(_norm_self(target) or target)
            if not (tgt_req and _is_redirect(tgt_req)):
                continue
            target_norm = _norm_self(target) or target
            nofollow = bool(it.get("nofollow"))
            row = {
                "source_url": source,
                "target_url": target_norm,
                "nofollow": nofollow,
                "anchor_text": str(it.get("anchor_text") or "").strip(),
            }
            key = (source, target_norm, nofollow)
            if is_idx:
                if key in seen_redirect_link_idx:
                    continue
                seen_redirect_link_idx.add(key)
                redirect_link_rows_indexable.append(row)
            else:
                if key in seen_redirect_link_no:
                    continue
                seen_redirect_link_no.add(key)
                redirect_link_rows_not_indexable.append(row)
    issues["page_has_links_to_redirect_links_indexable"] = _issue_block(
        "page_has_links_to_redirect_links_indexable", redirect_link_rows_indexable
    )
    issues["page_has_links_to_redirect_links_not_indexable"] = _issue_block(
        "page_has_links_to_redirect_links_not_indexable", redirect_link_rows_not_indexable
    )

    # Ahrefs-like: per-link exports for broken/redirect targets (404/4xx/3xx links).
    links_to_404: list[dict[str, Any]] = []
    links_to_4xx: list[dict[str, Any]] = []
    seen_404: set[tuple[str, str]] = set()
    seen_4xx: set[tuple[str, str]] = set()
    for p in ok_html_pages:
        source = _final_url(p)
        for it in getattr(p, "internal_link_items", []) or []:
            if not isinstance(it, dict):
                continue
            target = str(it.get("target_url") or "").strip()
            if not target:
                continue
            tgt = page_by_any.get(_norm_self(target) or target)
            if not tgt or not isinstance(tgt.status_code, int):
                continue
            target_norm = _norm_self(target) or target
            if tgt.status_code == 404:
                key = (source, target_norm)
                if key not in seen_404:
                    seen_404.add(key)
                    links_to_404.append({"source_url": source, "target_url": target_norm})
            if 400 <= tgt.status_code < 500:
                key = (source, target_norm)
                if key not in seen_4xx:
                    seen_4xx.add(key)
                    links_to_4xx.append({"source_url": source, "target_url": target_norm})

    issues["links_to_404_page"] = _issue_block("links_to_404_page", links_to_404)
    issues["links_to_4xx_page"] = _issue_block("links_to_4xx_page", links_to_4xx)

    redirect_3xx_link_rows: list[dict[str, Any]] = []
    seen_redirect_links: set[tuple[str, str, str, bool, str]] = set()
    # HTML href links
    for p in pages:
        if not (_is_html(p) and not p.error and isinstance(p.status_code, int) and p.status_code == 200):
            continue
        source = _final_url(p)
        for it in getattr(p, "internal_link_items", []) or []:
            if not isinstance(it, dict):
                continue
            target = str(it.get("target_url") or "").strip()
            if not target:
                continue
            tgt_req = page_by_requested.get(_norm_self(target) or target)
            if tgt_req and _is_redirect(tgt_req):
                target_norm = _norm_self(target) or target
                nofollow = bool(it.get("nofollow"))
                key = (source, target_norm, "href", nofollow, "")
                if key in seen_redirect_links:
                    continue
                seen_redirect_links.add(key)
                redirect_3xx_link_rows.append(
                    {
                        "source_url": source,
                        "target_url": target_norm,
                        "nofollow": nofollow,
                        "anchor_text": str(it.get("anchor_text") or "").strip(),
                        "link_type": "href",
                    }
                )

    # Sitemap URL links (sitemap file -> loc)
    if sitemap_urlsets:
        for sitemap_url, locs in sitemap_urlsets.items():
            if not isinstance(sitemap_url, str) or not sitemap_url.strip() or not isinstance(locs, list):
                continue
            for loc in locs:
                if not isinstance(loc, str) or not loc.startswith(("http://", "https://")):
                    continue
                tgt_req = page_by_requested.get(_norm_self(loc) or loc)
                if tgt_req and _is_redirect(tgt_req):
                    loc_norm = _norm_self(loc) or loc
                    key = (sitemap_url, loc_norm, "sitemap", False, "")
                    if key in seen_redirect_links:
                        continue
                    seen_redirect_links.add(key)
                    redirect_3xx_link_rows.append({"source_url": sitemap_url, "target_url": loc_norm, "nofollow": False, "link_type": "sitemap"})

    # HTML hreflang links (page -> alternate)
    for p in ok_html_pages:
        hreflang = _hreflang_map_for(p)
        if not hreflang:
            continue
        source = _final_url(p)
        for code, href in hreflang.items():
            if not isinstance(code, str) or not isinstance(href, str):
                continue
            tgt_req = page_by_requested.get(_norm_self(href) or href)
            if tgt_req and _is_redirect(tgt_req):
                href_norm = _norm_self(href) or href
                key = (source, href_norm, "hreflang", False, code.strip().lower())
                if key in seen_redirect_links:
                    continue
                seen_redirect_links.add(key)
                redirect_3xx_link_rows.append(
                    {"source_url": source, "target_url": href_norm, "nofollow": False, "link_type": "hreflang", "hreflang": code}
                )

    # Canonical links (page -> canonical URL)
    for p in ok_html_pages:
        if not p.canonical:
            continue
        canon = _norm_self(p.canonical)
        if not canon:
            continue
        tgt_req = page_by_requested.get(canon)
        if not (tgt_req and _is_redirect(tgt_req)):
            continue
        source = _final_url(p)
        key = (source, canon, "canonical", False, "")
        if key in seen_redirect_links:
            continue
        seen_redirect_links.add(key)
        redirect_3xx_link_rows.append({"source_url": source, "target_url": canon, "nofollow": False, "link_type": "canonical"})

    # Sitemap hreflang links (loc -> href)
    if sitemap_hreflang:
        for loc, lang_map in sitemap_hreflang.items():
            if not isinstance(loc, str) or not isinstance(lang_map, dict):
                continue
            for href in lang_map.values():
                if not isinstance(href, str):
                    continue
                tgt_req = page_by_requested.get(_norm_self(href) or href)
                if tgt_req and _is_redirect(tgt_req):
                    loc_norm = _norm_self(loc) or loc
                    href_norm = _norm_self(href) or href
                    key = (loc_norm, href_norm, "hreflang_sitemap", False, "")
                    if key in seen_redirect_links:
                        continue
                    seen_redirect_links.add(key)
                    redirect_3xx_link_rows.append({"source_url": loc_norm, "target_url": href_norm, "nofollow": False, "link_type": "hreflang_sitemap"})

    issues["redirect_3xx_links"] = _issue_block("redirect_3xx_links", redirect_3xx_link_rows)

    # Ahrefs-like: per-link export for "Timed out - links".
    # Ahrefs exports include multiple link sources (href/hreflang/canonical/sitemaps/redirects).
    timed_out_target_set = {_norm_self(u) or u for u in timeouts if isinstance(u, str) and u.strip()}
    timed_out_links: list[dict[str, Any]] = []
    seen_timed_out_links: set[tuple[str, str, bool]] = set()
    if timed_out_target_set:
        def _add_timeout_link(
            *,
            source_url: str,
            target_url: str,
            nofollow: bool = False,
            link_type: str,
            anchor_text: str = "",
            hreflang: str = "",
        ) -> None:
            if strict_link_counts:
                timed_out_links.append(
                    {
                        "source_url": source_url,
                        "target_url": target_url,
                        "nofollow": bool(nofollow),
                        "anchor_text": anchor_text,
                        "link_type": link_type,
                        "hreflang": hreflang,
                    }
                )
                return
            key = (source_url, target_url, bool(nofollow))
            if key in seen_timed_out_links:
                return
            seen_timed_out_links.add(key)
            timed_out_links.append(
                {
                    "source_url": source_url,
                    "target_url": target_url,
                    "nofollow": bool(nofollow),
                    "anchor_text": anchor_text,
                    "link_type": link_type,
                    "hreflang": hreflang,
                }
            )

        # 1) Href links (HTML <a>)
        for p in ok_html_pages:
            source = _final_url(p)
            for it in getattr(p, "internal_link_items", []) or []:
                if not isinstance(it, dict):
                    continue
                target = str(it.get("target_url") or "").strip()
                if not target:
                    continue
                target_norm = _norm_self(target) or target
                if target_norm not in timed_out_target_set:
                    continue
                _add_timeout_link(
                    source_url=source,
                    target_url=target_norm,
                    nofollow=bool(it.get("nofollow")),
                    anchor_text=str(it.get("anchor_text") or "").strip(),
                    link_type="Href link",
                )

        # 2) Canonical links
        for p in pages:
            if not p.canonical:
                continue
            source = _final_url(p)
            canon = _norm_self(p.canonical) or str(p.canonical)
            if canon in timed_out_target_set:
                _add_timeout_link(source_url=source, target_url=canon, nofollow=False, link_type="Canonical")

        # 3) Hreflang links from HTML or sitemap tags
        for p in pages:
            hreflang_map = _hreflang_map_for(p)
            if not hreflang_map:
                continue
            source = _final_url(p)
            for code, href in hreflang_map.items():
                href_norm = _norm_self(href) or str(href)
                if href_norm in timed_out_target_set:
                    _add_timeout_link(
                        source_url=source,
                        target_url=href_norm,
                        nofollow=False,
                        link_type="Hreflang",
                        hreflang=str(code),
                    )

        # 4) Sitemap URL links (sitemap file -> loc)
        if sitemap_urlsets:
            for sitemap_url, locs in sitemap_urlsets.items():
                if not isinstance(sitemap_url, str) or not sitemap_url.strip() or not isinstance(locs, list):
                    continue
                for loc in locs:
                    if not isinstance(loc, str) or not loc.startswith(("http://", "https://")):
                        continue
                    loc_norm = _norm_self(loc) or loc
                    if loc_norm in timed_out_target_set:
                        _add_timeout_link(source_url=sitemap_url, target_url=loc_norm, nofollow=False, link_type="Sitemap URL")

        # 5) Redirect chains: attribute a "Redirect" link to a timed-out final URL.
        for p in pages:
            if not _is_redirect(p):
                continue
            target = _final_url(p)
            if target in timed_out_target_set:
                _add_timeout_link(source_url=_norm_self(p.url) or p.url, target_url=target, nofollow=False, link_type="Redirect")
    issues["timed_out_links"] = _issue_block("timed_out_links", timed_out_links)

    # Ahrefs-like: per-link export for "Page has only one dofollow incoming internal link - links".
    one_df_targets = {_norm_self(u) or u for u in only_one_dofollow_incoming if isinstance(u, str) and u.strip()}
    one_df_links: list[dict[str, Any]] = []
    seen_one_df: set[tuple[str, str]] = set()
    if one_df_targets:
        for p in ok_html_pages:
            source = _final_url(p)
            for it in getattr(p, "internal_link_items", []) or []:
                if not isinstance(it, dict):
                    continue
                if bool(it.get("nofollow")):
                    continue
                target = str(it.get("target_url") or "").strip()
                if not target:
                    continue
                target_norm = _norm_self(target) or target
                if target_norm not in one_df_targets:
                    continue
                if not strict_link_counts:
                    key = (source, target_norm)
                    if key in seen_one_df:
                        continue
                    seen_one_df.add(key)
                one_df_links.append(
                    {
                        "source_url": source,
                        "target_url": target_norm,
                        "nofollow": False,
                        "anchor_text": str(it.get("anchor_text") or "").strip(),
                    }
                )
    issues["page_has_only_one_dofollow_incoming_internal_link_links"] = _issue_block(
        "page_has_only_one_dofollow_incoming_internal_link_links", one_df_links
    )

    no_out_indexable, no_out_not_indexable = _split_url_list(pages_no_outgoing)
    issues["page_has_no_outgoing_links_indexable"] = _issue_block("page_has_no_outgoing_links_indexable", no_out_indexable)
    issues["page_has_no_outgoing_links_not_indexable"] = _issue_block(
        "page_has_no_outgoing_links_not_indexable", no_out_not_indexable
    )

    nf_out_indexable, nf_out_not_indexable = _split_url_list(pages_with_nofollow_outgoing_internal)
    issues["page_has_nofollow_outgoing_internal_links_indexable"] = _issue_block(
        "page_has_nofollow_outgoing_internal_links_indexable", nf_out_indexable
    )
    issues["page_has_nofollow_outgoing_internal_links_not_indexable"] = _issue_block(
        "page_has_nofollow_outgoing_internal_links_not_indexable", nf_out_not_indexable
    )

    issues["https_page_has_internal_links_to_http"] = _issue_block(
        "https_page_has_internal_links_to_http", https_page_internal_to_http
    )
    issues["http_page_has_internal_links_to_https"] = _issue_block(
        "http_page_has_internal_links_to_https", http_page_internal_to_https
    )
    issues["https_page_links_to_http_image"] = _issue_block("https_page_links_to_http_image", https_page_http_images)
    issues["https_page_links_to_http_javascript"] = _issue_block(
        "https_page_links_to_http_javascript", https_page_http_js
    )
    issues["https_page_links_to_http_css"] = _issue_block("https_page_links_to_http_css", https_page_http_css)
    issues["https_http_mixed_content"] = _issue_block("https_http_mixed_content", https_mixed_content)

    # Semrush-like: links with no anchor text (count per affected page, not per link).
    links_no_anchor_by_page: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for p in ok_html_pages:
        rows = getattr(p, "links_without_anchor_text", None)
        if not isinstance(rows, list) or not rows:
            continue
        pid = _final_url(p)
        for r in rows:
            if isinstance(r, dict) and r.get("target_url"):
                links_no_anchor_by_page[pid].append(r)
    links_no_anchor_pages = [
        {"url": url, "links": rows[:50], "count": len(rows)} for url, rows in sorted(links_no_anchor_by_page.items())
    ]
    _write_issue_rows(issues_dir, "links_with_no_anchor_text", links_no_anchor_pages)
    issues["links_with_no_anchor_text"] = {
        "count": len(links_no_anchor_pages),
        "examples": [r["url"] for r in links_no_anchor_pages[:ISSUE_EXAMPLES_LIMIT]],
    }

    # Semrush-like: external links marked nofollow (count per affected page, not per link).
    nofollow_external_by_page: dict[str, list[str]] = defaultdict(list)
    for p in ok_html_pages:
        pid = _final_url(p)
        for u in getattr(p, "external_links_nofollow", []) or []:
            if isinstance(u, str) and u.startswith(("http://", "https://")):
                nofollow_external_by_page[pid].append(u)
    nofollow_external_pages = [
        {"url": url, "targets": sorted(set(tgts))[:100], "count": len(set(tgts))}
        for url, tgts in sorted(nofollow_external_by_page.items())
        if tgts
    ]
    _write_issue_rows(issues_dir, "nofollow_external_links", nofollow_external_pages)
    issues["nofollow_external_links"] = {
        "count": len(nofollow_external_pages),
        "examples": [r["url"] for r in nofollow_external_pages[:ISSUE_EXAMPLES_LIMIT]],
    }

    issues["multiple_title_tags"] = _issue_block("multiple_title_tags", multiple_title_tags)
    issues["multiple_meta_description_tags"] = _issue_block(
        "multiple_meta_description_tags", multiple_meta_description_tags
    )
    issues["low_word_count"] = _issue_block("low_word_count", low_word_count)
    issues["pages_have_high_ai_content_levels"] = _issue_block(
        "pages_have_high_ai_content_levels", pages_have_high_ai_content_levels
    )
    issues["title_too_long_indexable"] = _issue_block("title_too_long_indexable", title_too_long_indexable)
    issues["title_too_long_not_indexable"] = _issue_block("title_too_long_not_indexable", title_too_long_not_indexable)
    issues["title_too_long"] = _issue_block(
        "title_too_long", sorted(set(title_too_long_indexable + title_too_long_not_indexable))
    )
    issues["title_too_short"] = _issue_block("title_too_short", title_too_short)
    issues["meta_description_too_long_indexable"] = _issue_block(
        "meta_description_too_long_indexable", meta_description_too_long_indexable
    )
    issues["meta_description_too_long_not_indexable"] = _issue_block(
        "meta_description_too_long_not_indexable", meta_description_too_long_not_indexable
    )
    issues["meta_description_too_long"] = _issue_block(
        "meta_description_too_long",
        sorted(set(meta_description_too_long_indexable + meta_description_too_long_not_indexable)),
    )
    issues["meta_description_too_short_indexable"] = _issue_block(
        "meta_description_too_short_indexable", meta_description_too_short_indexable
    )
    issues["meta_description_too_short_not_indexable"] = _issue_block(
        "meta_description_too_short_not_indexable", meta_description_too_short_not_indexable
    )
    issues["meta_description_too_short"] = _issue_block(
        "meta_description_too_short",
        sorted(set(meta_description_too_short_indexable + meta_description_too_short_not_indexable)),
    )
    issues["pages_with_poor_lcp"] = _issue_block("pages_with_poor_lcp", pages_with_poor_lcp)
    issues["pages_with_poor_cls"] = _issue_block("pages_with_poor_cls", pages_with_poor_cls)
    issues["pages_with_poor_inp"] = _issue_block("pages_with_poor_inp", pages_with_poor_inp)
    issues["pages_with_poor_tbt"] = _issue_block("pages_with_poor_tbt", pages_with_poor_tbt)
    issues["cwv_lcp_pages_to_fix"] = _issue_block("cwv_lcp_pages_to_fix", cwv_lcp_pages_to_fix)
    issues["cwv_tbt_pages_to_fix"] = _issue_block("cwv_tbt_pages_to_fix", cwv_tbt_pages_to_fix)
    issues["cwv_cls_pages_to_fix"] = _issue_block("cwv_cls_pages_to_fix", cwv_cls_pages_to_fix)
    issues["slow_page"] = _issue_block("slow_page", slow_page)
    issues["page_and_serp_titles_do_not_match"] = _issue_block(
        "page_and_serp_titles_do_not_match", page_and_serp_titles_do_not_match
    )
    issues["duplicate_pages_without_canonical"] = _issue_block(
        "duplicate_pages_without_canonical", duplicate_pages_without_canonical
    )

    issues["open_graph_tags_missing"] = _issue_block("open_graph_tags_missing", open_graph_missing)
    issues["open_graph_tags_incomplete"] = _issue_block("open_graph_tags_incomplete", open_graph_incomplete)
    issues["open_graph_url_not_matching_canonical"] = _issue_block(
        "open_graph_url_not_matching_canonical", open_graph_url_mismatch
    )
    issues["twitter_card_missing"] = _issue_block("twitter_card_missing", twitter_missing)
    issues["twitter_card_incomplete"] = _issue_block("twitter_card_incomplete", twitter_incomplete)

    issues["html_lang_attribute_missing"] = _issue_block("html_lang_attribute_missing", html_lang_missing)
    issues["html_lang_attribute_invalid"] = _issue_block("html_lang_attribute_invalid", html_lang_invalid)
    issues["hreflang_defined_but_html_lang_missing"] = _issue_block(
        "hreflang_defined_but_html_lang_missing", hreflang_defined_but_html_lang_missing
    )
    issues["hreflang_annotation_invalid"] = _issue_block("hreflang_annotation_invalid", hreflang_annotation_invalid)
    _write_issue_rows(issues_dir, "hreflang_conflicts_within_page_source_code", hreflang_conflicts_within_page_source_code)
    issues["hreflang_conflicts_within_page_source_code"] = {
        "count": len(hreflang_conflicts_within_page_source_code),
        "examples": [str(r.get("url") or "") for r in hreflang_conflicts_within_page_source_code[:ISSUE_EXAMPLES_LIMIT]],
    }
    issues["more_than_one_page_for_same_language_in_hreflang"] = _issue_block(
        "more_than_one_page_for_same_language_in_hreflang", more_than_one_page_for_same_language_in_hreflang
    )
    issues["missing_reciprocal_hreflang"] = _issue_block("missing_reciprocal_hreflang", missing_reciprocal_hreflang)
    issues["structured_data_schema_org_validation_error"] = _issue_block(
        "structured_data_schema_org_validation_error", structured_data_schema_org_errors
    )
    issues["structured_data_google_rich_results_validation_error"] = _issue_block(
        "structured_data_google_rich_results_validation_error", structured_data_google_rich_results_errors
    )
    issues["page_referenced_for_more_than_one_language_in_hreflang"] = _issue_block(
        "page_referenced_for_more_than_one_language_in_hreflang", hreflang_referenced_multi_lang
    )
    issues["hreflang_to_redirect_or_broken_page"] = _issue_block(
        "hreflang_to_redirect_or_broken_page", hreflang_url_to_redirect_or_broken
    )
    # Ahrefs-like: per-link export for hreflang targets that redirect or are broken.
    hreflang_to_redirect_or_broken_page_links: list[dict[str, Any]] = []
    seen_hreflang_links: set[tuple[str, str, str]] = set()
    for p in hreflang_source_pages:
        hreflang = _hreflang_map_for(p)
        if not hreflang:
            continue
        if _is_non_canonical(p):
            continue
        source = _final_url(p)
        found_in = "HTML source" if p.hreflang else "Sitemap"
        for code, href in hreflang.items():
            if not isinstance(code, str) or not isinstance(href, str):
                continue
            href_norm = _norm_self(href) or href
            t = page_by_any.get(href_norm)
            if not t:
                continue
            if not (_is_redirect(t) or _is_timeout(t) or (isinstance(t.status_code, int) and t.status_code >= 400)):
                continue
            if not strict_link_counts:
                key = (source, href_norm, code.strip().lower())
                if key in seen_hreflang_links:
                    continue
                seen_hreflang_links.add(key)
            hreflang_to_redirect_or_broken_page_links.append(
                {"source_url": source, "target_url": href_norm, "hreflang": code, "found_in": found_in}
            )
    issues["hreflang_to_redirect_or_broken_page_links"] = _issue_block(
        "hreflang_to_redirect_or_broken_page_links", hreflang_to_redirect_or_broken_page_links
    )
    issues["hreflang_to_non_canonical"] = _issue_block("hreflang_to_non_canonical", hreflang_to_non_canonical)
    issues["x_default_hreflang_missing"] = _issue_block(
        "x_default_hreflang_missing", sorted(hreflang_x_default_missing_set)
    )

    issues["page_in_multiple_sitemaps"] = _issue_block("page_in_multiple_sitemaps", page_in_multiple_sitemaps)

    issues["sitemap_3xx_redirect"] = _issue_block("sitemap_3xx_redirect", sitemap_3xx)
    issues["sitemap_4xx_page"] = _issue_block("sitemap_4xx_page", sitemap_4xx)
    issues["sitemap_5xx_page"] = _issue_block("sitemap_5xx_page", sitemap_5xx)
    issues["sitemap_noindex_page"] = _issue_block("sitemap_noindex_page", sitemap_noindex)
    issues["sitemap_non_canonical_page"] = _issue_block("sitemap_non_canonical_page", sitemap_non_canonical)
    issues["sitemap_page_timed_out"] = _issue_block("sitemap_page_timed_out", sitemap_timed_out)
    issues["indexable_page_not_in_sitemap"] = _issue_block("indexable_page_not_in_sitemap", indexable_not_in_sitemap)

    # --- Change tracking (Ahrefs-like, minimal subset) ---
    if prev_effective_pages:
        canonical_url_changed: list[str] = []
        indexable_page_became_non_indexable: list[str] = []
        noindex_page_became_indexable: list[str] = []
        pages_to_submit_to_indexnow: list[str] = []
        title_tag_changed: list[str] = []
        meta_description_changed: list[str] = []
        h1_tag_changed: list[str] = []
        word_count_changed: list[str] = []

        def _is_ok_html(p: PageData) -> bool:
            return _is_html(p) and isinstance(p.status_code, int) and p.status_code == 200 and not p.error

        for eff, cur in effective_pages.items():
            prev = prev_effective_pages.get(eff)
            if not prev:
                continue
            prev_ok = _is_ok_html(prev)
            cur_ok = _is_ok_html(cur)

            # Ahrefs-like: "Indexable page became non-indexable" can be triggered by timeouts/4xx/robots,
            # so we only require the *previous* version to be OK+indexable.
            if prev_ok and _is_indexable(prev) and not _is_indexable(cur):
                indexable_page_became_non_indexable.append(eff)

            # For the remaining "changed" checks, require both versions to be OK HTML.
            if not prev_ok or not cur_ok:
                continue

            prev_can = _norm_self(prev.canonical) if prev.canonical else None
            cur_can = _norm_self(cur.canonical) if cur.canonical else None
            if prev_can != cur_can and (prev_can or cur_can):
                canonical_url_changed.append(eff)

            cur_indexable = _is_indexable(cur)

            prev_noindex = _looks_noindex(prev.meta_robots) or _looks_noindex(prev.x_robots_tag)
            cur_noindex = _looks_noindex(cur.meta_robots) or _looks_noindex(cur.x_robots_tag)
            if prev_noindex and not cur_noindex and cur_indexable:
                noindex_page_became_indexable.append(eff)

            parts = urlsplit(eff)
            is_root = (parts.path or "/") in {"", "/"}
            is_locale_root = bool(re.fullmatch(r"/[a-z]{2}(-[a-z0-9]{2,8})?/?", parts.path or "", re.IGNORECASE))

            prev_title = (prev.title or "").strip()
            cur_title = (cur.title or "").strip()
            if prev_title and cur_title and prev_title != cur_title:
                # Ahrefs-like: change tracking is noisier on non-root pages; keep it conservative for parity.
                if (not strict_link_counts) or is_root or is_locale_root:
                    title_tag_changed.append(eff)

            prev_desc = (prev.meta_description or "").strip()
            cur_desc = (cur.meta_description or "").strip()
            if prev_desc and cur_desc and prev_desc != cur_desc:
                meta_description_changed.append(eff)

            prev_h1 = (prev.h1[0] if prev.h1 else "").strip()
            cur_h1 = (cur.h1[0] if cur.h1 else "").strip()
            if prev_h1 and cur_h1 and prev_h1 != cur_h1:
                h1_tag_changed.append(eff)

            if (
                isinstance(prev.text_word_count, int)
                and isinstance(cur.text_word_count, int)
                and prev.text_word_count != cur.text_word_count
            ):
                word_count_changed.append(eff)

            # IndexNow: only suggest high-level pages (home / locale roots) when significant content changed.
            # This matches Ahrefs better than suggesting the entire indexable set.
            if (is_root or is_locale_root) and cur_indexable:
                if prev_title and cur_title and prev_title != cur_title:
                    pages_to_submit_to_indexnow.append(eff)
                elif prev_desc and cur_desc and prev_desc != cur_desc:
                    pages_to_submit_to_indexnow.append(eff)
                elif prev_h1 and cur_h1 and prev_h1 != cur_h1:
                    pages_to_submit_to_indexnow.append(eff)
                elif prev_can != cur_can and (prev_can or cur_can):
                    pages_to_submit_to_indexnow.append(eff)

        issues["canonical_url_changed"] = _issue_block("canonical_url_changed", sorted(set(canonical_url_changed)))
        issues["indexable_page_became_non_indexable"] = _issue_block(
            "indexable_page_became_non_indexable", sorted(set(indexable_page_became_non_indexable))
        )
        issues["noindex_page_became_indexable"] = _issue_block(
            "noindex_page_became_indexable", sorted(set(noindex_page_became_indexable))
        )
        issues["title_tag_changed"] = _issue_block("title_tag_changed", sorted(set(title_tag_changed)))
        issues["meta_description_changed"] = _issue_block("meta_description_changed", sorted(set(meta_description_changed)))
        issues["h1_tag_changed"] = _issue_block("h1_tag_changed", sorted(set(h1_tag_changed)))
        issues["word_count_changed"] = _issue_block("word_count_changed", sorted(set(word_count_changed)))
        issues["pages_to_submit_to_indexnow"] = _issue_block(
            "pages_to_submit_to_indexnow", sorted(set(pages_to_submit_to_indexnow))
        )

    return issues


def _write_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _write_text(path: str, text: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def _render_md_report(base_url: str, pages: list[PageData], issues: dict[str, dict[str, Any]], meta: dict[str, Any]) -> str:
    html_pages = [p for p in pages if (p.content_type or "").find("html") != -1]
    ok_pages = [p for p in pages if isinstance(p.status_code, int) and 200 <= p.status_code < 300]
    pagespeed_meta = meta.get("pagespeed") if isinstance(meta.get("pagespeed"), dict) else {}
    cwv_meta = meta.get("cwv") if isinstance(meta.get("cwv"), dict) else None
    gsc_meta = meta.get("gsc_api") if isinstance(meta.get("gsc_api"), dict) else {}

    def issue_count(key: str) -> int:
        block = issues.get(key)
        if not isinstance(block, dict):
            return 0
        c = block.get("count")
        return int(c) if isinstance(c, int) else 0

    def issue_examples(key: str) -> list[Any]:
        block = issues.get(key)
        if not isinstance(block, dict):
            return []
        ex = block.get("examples")
        return ex if isinstance(ex, list) else []

    def issue_count_sum(*keys: str) -> int:
        return sum(issue_count(k) for k in keys)

    def top_items(key: str) -> list[tuple[str, int]]:
        block = issues.get(key)
        if not isinstance(block, dict):
            return []
        top = block.get("top")
        if not isinstance(top, list):
            return []
        out: list[tuple[str, int]] = []
        for item in top:
            if isinstance(item, (list, tuple)) and len(item) == 2 and isinstance(item[1], int):
                out.append((str(item[0]), int(item[1])))
        return out

    def is_indexable(p: PageData) -> bool:
        if (p.content_type or "").find("html") == -1:
            return False
        if not isinstance(p.status_code, int) or p.status_code != 200:
            return False
        if p.error:
            return False
        robots = (p.meta_robots or "").lower()
        xrobots = (p.x_robots_tag or "").lower()
        return "noindex" not in robots and "noindex" not in xrobots

    indexable_pages = [p for p in pages if is_indexable(p)]

    def _pct(part: int, total: int) -> str:
        if total <= 0:
            return "0%"
        return f"{(part / total) * 100:.1f}%"

    def _fmt_num(value: Any) -> str:
        if isinstance(value, bool) or value is None:
            return "—"
        if isinstance(value, int):
            return f"{value}"
        if isinstance(value, float):
            return f"{value:.4f}".rstrip("0").rstrip(".")
        return str(value)

    def _fmt_pct(value: Any) -> str:
        if isinstance(value, bool) or value is None:
            return "—"
        if isinstance(value, (int, float)):
            return f"{float(value) * 100:.2f}%"
        return str(value)

    def _fmt_ms(value: Any) -> str:
        if isinstance(value, bool) or value is None:
            return "—"
        if isinstance(value, (int, float)):
            v = float(value)
            if v >= 1000:
                return f"{v:.0f} ms"
            return f"{v:.0f} ms"
        return str(value)

    lines: list[str] = []
    lines.append(f"# SEO Audit - {base_url}")
    lines.append("")

    lines.append("## Résumé")
    lines.append(f"- Pages analysées: **{len(pages)}** (HTML: **{len(html_pages)}**)")
    lines.append(f"- Pages OK (2xx): **{len(ok_pages)}** | Indexables: **{len(indexable_pages)}**")
    lines.append(f"- Erreurs (>=400): **{issue_count('bad_status')}** | Timeouts: **{issue_count('timed_out')}**")
    lines.append(f"- Bloquées robots.txt: **{issue_count('blocked_by_robots')}**")
    if isinstance(pagespeed_meta, dict) and pagespeed_meta.get("enabled"):
        tested = pagespeed_meta.get("tested")
        requested = pagespeed_meta.get("requested")
        strategy = pagespeed_meta.get("strategy")
        errors = pagespeed_meta.get("errors")
        lines.append(
            f"- PageSpeed: **{_fmt_num(tested)}/{_fmt_num(requested)}** (errors: {_fmt_num(errors)}) | strategy: **{strategy}**"
        )
    if isinstance(gsc_meta, dict) and gsc_meta.get("enabled"):
        if gsc_meta.get("ok"):
            qs = gsc_meta.get("queries") if isinstance(gsc_meta.get("queries"), dict) else {}
            lines.append(
                f"- GSC: **OK** | property: **{gsc_meta.get('property')}** | clicks: **{_fmt_num(qs.get('total_clicks'))}** | impressions: **{_fmt_num(qs.get('total_impressions'))}**"
            )
        else:
            lines.append(f"- GSC: activé mais indisponible (**{gsc_meta.get('reason')}**)")
    lines.append("")

    lines.append("## Audit type Ahrefs (résumé)")
    lines.append(f"- Internal pages (HTML): **{issue_count('internal_pages')}**")
    lines.append(
        f"- Statuts: 404 **{issue_count('http_404')}** | 4xx **{issue_count('http_4xx')}** | 5xx **{issue_count('http_5xx')}** | redirect (URLs) **{issue_count('redirect_3xx')}**"
    )
    lines.append(
        f"- Redirects: broken **{issue_count('broken_redirect')}** | loop **{issue_count('redirect_loop')}** | chain>3 **{issue_count('redirect_chain_too_long')}**"
    )
    lines.append(
        f"- Mixed content (HTTPS→HTTP): **{issue_count('https_http_mixed_content')}** (links: {issue_count('https_page_has_internal_links_to_http')}, img: {issue_count('https_page_links_to_http_image')}, js: {issue_count('https_page_links_to_http_javascript')}, css: {issue_count('https_page_links_to_http_css')})"
    )
    resources_checked = bool(meta.get("resources_checked"))
    if resources_checked:
        lines.append(
            f"- Assets internes (check activé): img broken **{issue_count('image_broken')}** (pages: {issue_count('page_has_broken_image')}), js broken **{issue_count('javascript_broken')}**, css broken **{issue_count('css_broken')}**"
        )
        lines.append(
            f"  - Redirects: img **{issue_count('image_redirects')}**, js **{issue_count('javascript_redirects')}**, css **{issue_count('css_redirects')}**"
        )
        lines.append(
            f"  - Tailles: img **{issue_count('image_file_size_too_large')}**, js **{issue_count('javascript_file_size_too_large')}**, css **{issue_count('css_file_size_too_large')}**"
        )
    else:
        lines.append("- Assets internes: vérification désactivée (activer `--check-resources`).")
    lines.append("")

    lines.append("## Indexabilité")
    lines.append(
        f"- Noindex: **{issue_count('noindex_page')}** | Nofollow: **{issue_count('nofollow_page')}** | Noindex+Nofollow: **{issue_count('noindex_and_nofollow_page')}**"
    )
    lines.append(
        f"- Canonicals: manquants **{issue_count('missing_canonical')}** | →4xx **{issue_count('canonical_points_to_4xx')}** | →5xx **{issue_count('canonical_points_to_5xx')}** | →redirect **{issue_count('canonical_points_to_redirect')}**"
    )
    lines.append(
        f"- Canonical non canonique: **{issue_count('non_canonical_page_specified_as_canonical_one')}** | HTTP→HTTPS: **{issue_count('canonical_from_http_to_https')}** | HTTPS→HTTP: **{issue_count('canonical_from_https_to_http')}**"
    )
    lines.append("")

    lines.append("## Liens internes")
    lines.append(
        f"- Orphan pages (0 lien entrant): **{issue_count_sum('orphan_page_indexable', 'orphan_page_not_indexable')}** | Canonical sans lien entrant: **{issue_count('canonical_url_has_no_incoming_internal_links')}**"
    )
    lines.append(
        f"- Pages qui pointent vers 4xx/5xx: **{issue_count_sum('page_has_links_to_broken_page_indexable', 'page_has_links_to_broken_page_not_indexable')}** | vers redirect: **{issue_count_sum('page_has_links_to_redirect_indexable', 'page_has_links_to_redirect_not_indexable')}**"
    )
    lines.append(
        f"- Sans outgoing links: **{issue_count_sum('page_has_no_outgoing_links_indexable', 'page_has_no_outgoing_links_not_indexable')}** | Outgoing internal nofollow: **{issue_count_sum('page_has_nofollow_outgoing_internal_links_indexable', 'page_has_nofollow_outgoing_internal_links_not_indexable')}**"
    )
    lines.append("")

    lines.append("## Contenu (heuristiques)")
    lines.append(
        f"- Titles: manquants **{issue_count('missing_title')}** | multiples **{issue_count('multiple_title_tags')}** | trop longs **{issue_count('title_too_long_indexable') + issue_count('title_too_long_not_indexable')}** | trop courts **{issue_count('title_too_short')}**"
    )
    lines.append(
        f"- Meta desc: manquantes **{issue_count('missing_meta_description')}** | multiples **{issue_count('multiple_meta_description_tags')}** | trop longues **{issue_count('meta_description_too_long_indexable') + issue_count('meta_description_too_long_not_indexable')}** | trop courtes **{issue_count('meta_description_too_short_indexable') + issue_count('meta_description_too_short_not_indexable')}**"
    )
    lines.append(
        f"- H1: manquants **{issue_count('missing_h1_indexable') + issue_count('missing_h1_not_indexable')}** | multiples **{issue_count('multiple_h1')}** | low word count **{issue_count('low_word_count')}**"
    )
    lines.append(f"- Duplicates sans canonical (heuristique): **{issue_count('duplicate_pages_without_canonical')}**")
    lines.append("")

    lines.append("## Social / OpenGraph")
    lines.append(
        f"- OpenGraph: missing **{issue_count('open_graph_tags_missing')}** | incomplete **{issue_count('open_graph_tags_incomplete')}** | og:url != canonical **{issue_count('open_graph_url_not_matching_canonical')}**"
    )
    lines.append(
        f"- Twitter card: missing **{issue_count('twitter_card_missing')}** | incomplete **{issue_count('twitter_card_incomplete')}**"
    )
    lines.append("")

    lines.append("## Localisation (lang/hreflang)")
    lines.append(
        f"- HTML lang: missing **{issue_count('html_lang_attribute_missing')}** | invalid **{issue_count('html_lang_attribute_invalid')}**"
    )
    lines.append(
        f"- Hreflang: invalid **{issue_count('hreflang_annotation_invalid')}** | x-default missing **{issue_count('x_default_hreflang_missing')}** | reciprocal missing **{issue_count('missing_reciprocal_hreflang')}**"
    )
    lines.append(
        f"- Hreflang targets: redirect/broken **{issue_count('hreflang_to_redirect_or_broken_page')}** | non canonical **{issue_count('hreflang_to_non_canonical')}**"
    )
    lines.append("")

    lines.append("## Sitemaps (best-effort)")
    lines.append(
        f"- URLs sitemap: 3xx **{issue_count('sitemap_3xx_redirect')}** | 4xx **{issue_count('sitemap_4xx_page')}** | 5xx **{issue_count('sitemap_5xx_page')}** | noindex **{issue_count('sitemap_noindex_page')}** | non canonical **{issue_count('sitemap_non_canonical_page')}**"
    )
    lines.append(f"- Indexable pages not in sitemap: **{issue_count('indexable_page_not_in_sitemap')}**")
    lines.append("")

    lines.append("## Données Google")
    # --- GSC ---
    lines.append("### Search Console (GSC)")
    if isinstance(gsc_meta, dict) and gsc_meta.get("enabled"):
        if gsc_meta.get("ok"):
            prop = gsc_meta.get("property")
            days = gsc_meta.get("days")
            start_date = gsc_meta.get("start_date")
            end_date = gsc_meta.get("end_date")
            search_type = gsc_meta.get("search_type")
            q = gsc_meta.get("queries") if isinstance(gsc_meta.get("queries"), dict) else {}
            p = gsc_meta.get("pages") if isinstance(gsc_meta.get("pages"), dict) else {}
            lines.append(f"- Property: **{prop}**")
            lines.append(f"- Période: **{start_date} → {end_date}** ({days} jours) | search_type: **{search_type}**")
            lines.append(
                f"- Totaux (queries): clicks **{_fmt_num(q.get('total_clicks'))}**, impressions **{_fmt_num(q.get('total_impressions'))}**, CTR **{_fmt_pct(q.get('avg_ctr'))}**, position **{_fmt_num(q.get('avg_position'))}**"
            )
            lines.append(
                f"- Totaux (pages): clicks **{_fmt_num(p.get('total_clicks'))}**, impressions **{_fmt_num(p.get('total_impressions'))}**, CTR **{_fmt_pct(p.get('avg_ctr'))}**, position **{_fmt_num(p.get('avg_position'))}**"
            )
            queries_csv = gsc_meta.get("queries_csv")
            pages_csv = gsc_meta.get("pages_csv")
            if queries_csv or pages_csv:
                lines.append(f"- Fichiers: {queries_csv or '—'} | {pages_csv or '—'}")
            top_q = q.get("top") if isinstance(q.get("top"), list) else []
            if top_q:
                lines.append("")
                lines.append("#### Top queries (impressions)")
                lines.append("| Query | Clicks | Impr. | CTR | Pos |")
                lines.append("|---|---:|---:|---:|---:|")
                for row in top_q[:10]:
                    if not isinstance(row, dict):
                        continue
                    query = str(row.get("query") or "")
                    lines.append(
                        f"| {query[:80]} | {_fmt_num(row.get('clicks'))} | {_fmt_num(row.get('impressions'))} | {_fmt_pct(row.get('ctr'))} | {_fmt_num(row.get('position'))} |"
                    )
            top_p = p.get("top") if isinstance(p.get("top"), list) else []
            if top_p:
                lines.append("")
                lines.append("#### Top pages (impressions)")
                lines.append("| Page | Clicks | Impr. | CTR | Pos |")
                lines.append("|---|---:|---:|---:|---:|")
                for row in top_p[:10]:
                    if not isinstance(row, dict):
                        continue
                    page_url = str(row.get("page") or "")
                    lines.append(
                        f"| {page_url[:80]} | {_fmt_num(row.get('clicks'))} | {_fmt_num(row.get('impressions'))} | {_fmt_pct(row.get('ctr'))} | {_fmt_num(row.get('position'))} |"
                    )

            gsc_issues = gsc_meta.get("issues") if isinstance(gsc_meta.get("issues"), dict) else {}
            qw = gsc_issues.get("pages_quick_wins") if isinstance(gsc_issues.get("pages_quick_wins"), dict) else {}
            pp1 = gsc_issues.get("pages_push_page_1") if isinstance(gsc_issues.get("pages_push_page_1"), dict) else {}
            if qw or pp1:
                lines.append("")
                lines.append("#### Opportunités (pages)")
                lines.append(
                    f"- Quick wins CTR (pos 3–10, impr ≥ {gsc_meta.get('min_impressions')}): **{_fmt_num(qw.get('count'))}**"
                )
                lines.append(
                    f"- À pousser page 1 (pos 11–20, impr ≥ {gsc_meta.get('min_impressions')}): **{_fmt_num(pp1.get('count'))}**"
                )
        else:
            lines.append(f"- Échec/skip: **{gsc_meta.get('reason')}**")
            if gsc_meta.get("last_error"):
                lines.append(f"- Dernière erreur: {gsc_meta.get('last_error')}")
            candidates = gsc_meta.get("candidates") if isinstance(gsc_meta.get("candidates"), list) else []
            if candidates:
                lines.append(f"- Candidates: {', '.join(str(c) for c in candidates[:5])}")
    else:
        lines.append("- Désactivé (activer `gsc_api.enabled: true` + fournir `GOOGLE_APPLICATION_CREDENTIALS`).")
    lines.append("")

    # --- Bing ---
    lines.append("### Bing Webmaster Tools")
    if isinstance(meta.get("bing"), dict):
        bing_meta = meta.get("bing") if isinstance(meta.get("bing"), dict) else {}
        if bing_meta.get("enabled") and bing_meta.get("ok"):
            lines.append(f"- Min impressions (issues): **{bing_meta.get('min_impressions')}**")
            lines.append(f"- Mode: **{bing_meta.get('reason')}**")
            if bing_meta.get("site_url"):
                lines.append(f"- siteUrl: **{bing_meta.get('site_url')}**")
            quota = bing_meta.get("url_submission_quota") if isinstance(bing_meta.get("url_submission_quota"), dict) else {}
            if quota:
                remaining = quota.get("remaining")
                daily = quota.get("daily_quota")
                reset = quota.get("reset_date")
                parts = []
                if daily is not None or remaining is not None:
                    parts.append(f"{remaining}/{daily}" if daily is not None else str(remaining))
                if reset:
                    parts.append(f"reset={reset}")
                if parts:
                    lines.append(f"- Quota soumission d’URL: **{' · '.join(parts)}**")
            if bing_meta.get("queries_csv") or bing_meta.get("pages_csv"):
                lines.append(f"- Fichiers: {bing_meta.get('queries_csv') or '—'} | {bing_meta.get('pages_csv') or '—'}")
            bq = bing_meta.get("queries") if isinstance(bing_meta.get("queries"), dict) else {}
            bp = bing_meta.get("pages") if isinstance(bing_meta.get("pages"), dict) else {}
            if bp:
                lines.append(
                    f"- Totaux (pages): clicks **{_fmt_num(bp.get('total_clicks'))}**, impressions **{_fmt_num(bp.get('total_impressions'))}**, CTR **{_fmt_pct(bp.get('avg_ctr'))}**, position **{_fmt_num(bp.get('avg_position'))}**"
                )
            if bq:
                lines.append(
                    f"- Totaux (queries): clicks **{_fmt_num(bq.get('total_clicks'))}**, impressions **{_fmt_num(bq.get('total_impressions'))}**, CTR **{_fmt_pct(bq.get('avg_ctr'))}**, position **{_fmt_num(bq.get('avg_position'))}**"
                )
            b_issues = bing_meta.get("issues") if isinstance(bing_meta.get("issues"), dict) else {}
            qw = b_issues.get("pages_quick_wins") if isinstance(b_issues.get("pages_quick_wins"), dict) else {}
            pp1 = b_issues.get("pages_push_page_1") if isinstance(b_issues.get("pages_push_page_1"), dict) else {}
            if qw or pp1:
                lines.append(
                    f"- Opportunités pages: quick wins **{_fmt_num(qw.get('count'))}**, page 1 **{_fmt_num(pp1.get('count'))}**"
                )
            ci = b_issues.get("crawl_issues") if isinstance(b_issues.get("crawl_issues"), dict) else {}
            bu = b_issues.get("blocked_urls") if isinstance(b_issues.get("blocked_urls"), dict) else {}
            ui = b_issues.get("url_info_non_200") if isinstance(b_issues.get("url_info_non_200"), dict) else {}
            if ci:
                lines.append(f"- Crawl issues (API): **{_fmt_num(ci.get('count'))}**")
            if bu:
                lines.append(f"- Blocked URLs (API): **{_fmt_num(bu.get('count'))}**")
            if ui:
                lines.append(f"- UrlInfo non-200 (API): **{_fmt_num(ui.get('count'))}**")
            sm = bing_meta.get("sitemaps") if isinstance(bing_meta.get("sitemaps"), dict) else {}
            if sm:
                lines.append(f"- Sitemaps (API): **{_fmt_num(sm.get('rows'))}**")
        elif bing_meta.get("enabled"):
            lines.append(f"- Échec/skip: **{bing_meta.get('reason')}**")
        else:
            lines.append("- Désactivé (module non activé).")
    else:
        lines.append("- Désactivé (module non activé).")
    lines.append("")

    # --- CWV / PageSpeed ---
    lines.append("### Core Web Vitals (PageSpeed Insights)")
    if isinstance(pagespeed_meta, dict) and pagespeed_meta.get("enabled"):
        lines.append(
            f"- PageSpeed: strategy **{pagespeed_meta.get('strategy')}** | URLs testées **{_fmt_num(pagespeed_meta.get('tested'))}/{_fmt_num(pagespeed_meta.get('requested'))}** | erreurs **{_fmt_num(pagespeed_meta.get('errors'))}** | durée **{_fmt_num(pagespeed_meta.get('duration_s'))}s**"
        )
    else:
        reason = pagespeed_meta.get("reason") if isinstance(pagespeed_meta, dict) else None
        lines.append(f"- PageSpeed: désactivé ({reason or 'disabled_in_config'})")

    if isinstance(cwv_meta, dict):
        lines.append(f"- Pages testées: **{_fmt_num(cwv_meta.get('tested_pages'))}** | strategy: **{cwv_meta.get('strategy')}**")
        metrics = cwv_meta.get("metrics") if isinstance(cwv_meta.get("metrics"), dict) else {}

        def _metric_counts(metric: str) -> dict[str, int]:
            node = metrics.get(metric) if isinstance(metrics.get(metric), dict) else {}
            counts = node.get("counts") if isinstance(node.get("counts"), dict) else {}
            return {
                "good": int(counts.get("good") or 0),
                "ni": int(counts.get("ni") or 0),
                "poor": int(counts.get("poor") or 0),
                "na": int(counts.get("na") or 0),
            }

        lcp_c = _metric_counts("lcp")
        tbt_c = _metric_counts("tbt")
        cls_c = _metric_counts("cls")
        inp_c = _metric_counts("inp")
        lines.append("")
        lines.append("| Metric | Good | NI | Poor | N/A |")
        lines.append("|---|---:|---:|---:|---:|")
        lines.append(f"| LCP (ms) | {lcp_c['good']} | {lcp_c['ni']} | {lcp_c['poor']} | {lcp_c['na']} |")
        lines.append(f"| TBT (ms) | {tbt_c['good']} | {tbt_c['ni']} | {tbt_c['poor']} | {tbt_c['na']} |")
        lines.append(f"| CLS | {cls_c['good']} | {cls_c['ni']} | {cls_c['poor']} | {cls_c['na']} |")
        lines.append(f"| INP (ms) | {inp_c['good']} | {inp_c['ni']} | {inp_c['poor']} | {inp_c['na']} |")

        def _worst(metric: str) -> list[dict[str, Any]]:
            node = metrics.get(metric) if isinstance(metrics.get(metric), dict) else {}
            worst = node.get("worst") if isinstance(node.get("worst"), list) else []
            return [w for w in worst if isinstance(w, dict)]

        worst_lcp = _worst("lcp")
        worst_tbt = _worst("tbt")
        worst_cls = _worst("cls")
        if worst_lcp:
            lines.append("")
            lines.append("#### Worst pages (LCP)")
            for row in worst_lcp[:5]:
                lines.append(f"- {_fmt_ms(row.get('value'))} ({row.get('source')}) — {row.get('url')}")
        if worst_tbt:
            lines.append("")
            lines.append("#### Worst pages (TBT)")
            for row in worst_tbt[:5]:
                lines.append(f"- {_fmt_ms(row.get('value'))} ({row.get('source')}) — {row.get('url')}")
        if worst_cls:
            lines.append("")
            lines.append("#### Worst pages (CLS)")
            for row in worst_cls[:5]:
                lines.append(f"- {_fmt_num(row.get('value'))} ({row.get('source')}) — {row.get('url')}")
        note = cwv_meta.get("note")
        if note:
            lines.append("")
            lines.append(f"- Note: {note}")
    else:
        lines.append("- Aucune donnée CWV (activer `--pagespeed` + clé `PAGESPEED_API_KEY`).")
    lines.append("")

    lines.append("## Détails (exemples)")
    for k, title in [
        ("http_404", "404"),
        ("timed_out", "Timeouts"),
        ("https_http_mixed_content", "Mixed content (HTTPS→HTTP)"),
        ("orphan_page_indexable", "Orphan pages (indexables)"),
        ("orphan_page_not_indexable", "Orphan pages (non indexables)"),
        ("canonical_points_to_4xx", "Canonical → 4xx"),
        ("canonical_points_to_redirect", "Canonical → redirect"),
    ]:
        ex = issue_examples(k)
        if not ex:
            continue
        lines.append(f"### {title} (exemples)")
        for item in ex[:10]:
            lines.append(f"- {item}")
        lines.append("")

    broken_examples = [
        *issue_examples("page_has_links_to_broken_page_indexable"),
        *issue_examples("page_has_links_to_broken_page_not_indexable"),
    ]
    if broken_examples:
        lines.append("### Pages avec liens vers pages cassées (exemples)")
        for row in broken_examples[:10]:
            if isinstance(row, dict):
                src = row.get("source_url")
                targets = row.get("targets")
                if src:
                    lines.append(f"- Source: {src}")
                if isinstance(targets, list) and targets:
                    lines.append(f"  - Cibles: {', '.join(str(t) for t in targets[:5])}")
            else:
                lines.append(f"- {row}")
        lines.append("")

    if top_items("duplicate_titles"):
        lines.append("### Titres dupliqués (top)")
        for title, count in top_items("duplicate_titles")[:10]:
            lines.append(f"- ({count}x) {title[:160]}")
        lines.append("")

    if top_items("duplicate_meta_descriptions"):
        lines.append("### Meta descriptions dupliquées (top)")
        for desc, count in top_items("duplicate_meta_descriptions")[:10]:
            cleaned = re.sub(r"\s+", " ", str(desc)).strip()
            lines.append(f"- ({count}x) {cleaned[:160]}")
        lines.append("")

    lines.append("## Recommandations (priorisées)")
    lines.append("1. Corriger d’abord les pages en erreur (4xx/5xx) + les redirects cassés.")
    lines.append("2. Corriger mixed content (HTTPS→HTTP) et normaliser HTTP/HTTPS (links + canonicals).")
    lines.append("3. Fixer indexabilité (noindex/nofollow involontaires, canonicals incohérents).")
    lines.append("4. Corriger maillage interne: pages orphelines + liens internes cassés/redirect.")
    lines.append("5. Optimiser contenu: titles/meta uniques, longueurs, low word count, social tags, hreflang.")
    lines.append("")

    lines.append("## Détails exécution")
    lines.append(f"- started_at: {meta.get('started_at')}")
    lines.append(f"- base_url: {meta.get('base_url')}")
    lines.append(f"- pages_crawled: {meta.get('pages_crawled')}/{meta.get('max_pages')}")
    lines.append(f"- robots: {meta.get('robots')}")
    sitemaps = meta.get("sitemaps") if isinstance(meta.get("sitemaps"), list) else []
    if sitemaps:
        lines.append(f"- sitemaps: {len(sitemaps)}")
        for u in sitemaps[:3]:
            lines.append(f"  - {u}")
    else:
        lines.append("- sitemaps: 0")
    lines.append(f"- sitemap_seed_urls: {meta.get('sitemap_seed_urls')}")
    lines.append(f"- user_agent: {meta.get('user_agent')}")
    lines.append(f"- include: {meta.get('include')}")
    lines.append(f"- exclude: {meta.get('exclude')}")
    lines.append(f"- resources_checked: {meta.get('resources_checked')}")
    lines.append(f"- resources_counts: {meta.get('resources_counts')}")
    if isinstance(pagespeed_meta, dict) and pagespeed_meta.get("enabled"):
        lines.append(
            f"- pagespeed: strategy={pagespeed_meta.get('strategy')} tested={_fmt_num(pagespeed_meta.get('tested'))}/{_fmt_num(pagespeed_meta.get('requested'))} errors={_fmt_num(pagespeed_meta.get('errors'))} duration_s={_fmt_num(pagespeed_meta.get('duration_s'))}"
        )
    else:
        lines.append(f"- pagespeed: disabled ({pagespeed_meta.get('reason') if isinstance(pagespeed_meta, dict) else 'disabled_in_config'})")
    if isinstance(gsc_meta, dict) and gsc_meta.get("enabled"):
        if gsc_meta.get("ok"):
            lines.append(f"- gsc_api: ok property={gsc_meta.get('property')} days={gsc_meta.get('days')}")
        else:
            lines.append(f"- gsc_api: failed reason={gsc_meta.get('reason')}")
    else:
        lines.append("- gsc_api: disabled")
    lines.append("")
    return "\n".join(lines)


def _parse_args(argv: list[str]) -> CrawlConfig:
    parser = argparse.ArgumentParser(description="Crawl a website and generate an SEO audit report (JSON + Markdown).")
    parser.add_argument(
        "--profile",
        choices=["default", "ahrefs"],
        default="default",
        help="Crawl profile preset (default: default).",
    )
    parser.add_argument(
        "--align-ahrefs-zip",
        help=argparse.SUPPRESS,
    )
    parser.add_argument("base_url", help="Base URL to crawl, e.g. https://example.com/")
    parser.add_argument("--sitemap", action="append", default=[], help="Sitemap URL(s) to seed crawl (repeatable).")
    parser.add_argument("--max-pages", type=int, default=200, help="Maximum number of pages to fetch (default: 200).")
    parser.add_argument(
        "--max-sitemap-urls",
        type=int,
        default=2000,
        help="Maximum number of URLs to read from sitemaps in total (default: 2000).",
    )
    parser.add_argument("--timeout", type=float, default=15.0, help="HTTP timeout seconds (default: 15).")
    parser.add_argument("--workers", type=int, default=4, help="Concurrent workers (default: 4).")
    parser.add_argument("--user-agent", default=None, help="User-Agent header (default: SEOAutopilot/1.0).")
    parser.add_argument("--ignore-robots", action="store_true", help="Ignore robots.txt rules.")
    parser.add_argument("--allow-subdomains", action="store_true", help="Allow crawling subdomains of the base host.")
    parser.add_argument(
        "--http-retries",
        type=int,
        default=2,
        help="HTTP retries after first attempt for robots/sitemaps/aux fetches (default: 2).",
    )
    parser.add_argument(
        "--keep-alive",
        action="store_true",
        help='Use keep-alive connections (disables "Connection: close").',
    )
    parser.add_argument(
        "--no-discover-canonicals",
        action="store_true",
        help="Do not enqueue canonical URLs as new crawl targets.",
    )
    parser.add_argument(
        "--no-discover-hreflang",
        action="store_true",
        help="Do not enqueue hreflang URLs as new crawl targets.",
    )
    parser.add_argument(
        "--strict-link-counts",
        action="store_true",
        help="Count '* - links' issues per occurrence (Ahrefs-like).",
    )
    parser.add_argument("--include", help="Regex: only include URLs matching this pattern.")
    parser.add_argument("--exclude", help="Regex: exclude URLs matching this pattern.")
    parser.add_argument("--output-dir", help="Output directory (default: seo-audit-<host>-<timestamp>).")
    parser.add_argument(
        "--check-resources",
        action="store_true",
        help="Also fetch internal image/js/css resources to detect broken/redirect/size issues (slower).",
    )
    parser.add_argument(
        "--max-resources",
        type=int,
        default=250,
        help="Max unique internal resources to check per type (default: 250).",
    )
    parser.add_argument(
        "--pagespeed",
        action="store_true",
        help="Run Google PageSpeed Insights (Core Web Vitals) on a subset of pages (requires PAGESPEED_API_KEY env var).",
    )
    parser.add_argument(
        "--pagespeed-strategy",
        default="mobile",
        help="PageSpeed strategy: mobile or desktop (default: mobile).",
    )
    parser.add_argument(
        "--pagespeed-max-urls",
        type=int,
        default=50,
        help="Max unique URLs to test via PageSpeed (default: 50).",
    )
    parser.add_argument(
        "--pagespeed-timeout",
        type=float,
        default=60.0,
        help="PageSpeed API timeout seconds (default: 60).",
    )
    parser.add_argument(
        "--pagespeed-workers",
        type=int,
        default=2,
        help="Concurrent PageSpeed requests (default: 2).",
    )
    parser.add_argument(
        "--gsc-api",
        action="store_true",
        help="Fetch Google Search Console data (queries + pages) and attach it to the report (requires GOOGLE_APPLICATION_CREDENTIALS service account JSON).",
    )
    parser.add_argument(
        "--gsc-property",
        help="Optional GSC property to use (e.g. sc-domain:example.com or https://example.com/). If omitted, auto-detect candidates.",
    )
    parser.add_argument(
        "--gsc-days",
        type=int,
        default=28,
        help="GSC date range in days (default: 28).",
    )
    parser.add_argument(
        "--gsc-search-type",
        default="web",
        help="GSC search type: web|image|video|news|discover (default: web).",
    )
    parser.add_argument(
        "--gsc-row-limit",
        type=int,
        default=25000,
        help="GSC row limit (default: 25000).",
    )
    parser.add_argument(
        "--gsc-timeout",
        type=float,
        default=30.0,
        help="GSC API timeout seconds (default: 30).",
    )
    parser.add_argument(
        "--gsc-output-dir",
        help="Directory to write fetched GSC CSV files (default: <output_dir>/gsc).",
    )
    parser.add_argument(
        "--gsc-credentials",
        help="Path to service account JSON key (default: env GOOGLE_APPLICATION_CREDENTIALS).",
    )
    parser.add_argument(
        "--gsc-min-impressions",
        type=int,
        default=200,
        help="GSC: minimum impressions threshold used to compute opportunity issues (default: 200).",
    )
    parser.add_argument(
        "--gsc-inspection",
        action="store_true",
        help="GSC: run URL Inspection API on a small sample to surface Google indexing issues (requires webmasters scope).",
    )
    parser.add_argument(
        "--gsc-inspection-max-urls",
        type=int,
        default=0,
        help="GSC URL Inspection: max URLs to check (default: 0 = disabled).",
    )
    parser.add_argument(
        "--gsc-inspection-timeout",
        type=float,
        default=30.0,
        help="GSC URL Inspection timeout seconds (default: 30).",
    )
    parser.add_argument(
        "--gsc-inspection-language",
        help="Optional languageCode for URL Inspection API (e.g. fr, en).",
    )
    parser.add_argument(
        "--bing",
        action="store_true",
        help="Attach Bing performance CSV exports to the report and compute opportunity issues.",
    )
    parser.add_argument("--bing-queries-csv", help="Path to Bing performance export (queries).")
    parser.add_argument("--bing-pages-csv", help="Path to Bing performance export (pages).")
    parser.add_argument("--bing-site-url", help="Optional Bing siteUrl override (default: derived from base_url).")
    parser.add_argument("--bing-days", type=int, default=28, help="Bing date range in days (default: 28).")
    parser.add_argument("--bing-timeout", type=float, default=30.0, help="Bing API timeout seconds (default: 30).")
    parser.add_argument("--bing-api-key", help="Bing Webmaster Tools API key (default: env BING_WEBMASTER_API_KEY).")
    parser.add_argument("--bing-no-crawl-issues", action="store_true", help="Disable Bing GetCrawlIssues fetch.")
    parser.add_argument("--bing-no-blocked-urls", action="store_true", help="Disable Bing GetBlockedUrls fetch.")
    parser.add_argument("--bing-no-sitemaps", action="store_true", help="Disable Bing GetSitemaps fetch.")
    parser.add_argument("--bing-urlinfo-max", type=int, default=0, help="Bing: fetch UrlInfo for up to N top pages (default: 0).")
    parser.add_argument(
        "--bing-min-impressions",
        type=int,
        default=200,
        help="Bing: minimum impressions threshold used to compute opportunity issues (default: 200).",
    )
    parser.add_argument(
        "--bing-output-dir",
        help="Directory to write/copy Bing CSV files (default: <output_dir>/bing).",
    )
    args = parser.parse_args(argv)

    base_url = _normalize_url(args.base_url, args.base_url) or args.base_url
    base_parts = urlsplit(base_url)
    timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    output_dir = args.output_dir or f"seo-audit-{(base_parts.hostname or 'site')}-{timestamp}"

    pagespeed_api_key = (os.environ.get("PAGESPEED_API_KEY") or "").strip() or None

    # Profile presets (can be overridden by explicit flags).
    profile = str(getattr(args, "profile", "default") or "default").strip().lower()
    user_agent = str(args.user_agent).strip() if isinstance(args.user_agent, str) and str(args.user_agent).strip() else ""
    allow_subdomains = bool(args.allow_subdomains)
    http_retries = int(args.http_retries) if isinstance(args.http_retries, int) else 2
    connection_close = not bool(args.keep_alive)
    discover_canonicals = not bool(args.no_discover_canonicals)
    discover_hreflang = not bool(args.no_discover_hreflang)
    strict_link_counts = bool(args.strict_link_counts)
    check_resources = bool(args.check_resources)

    if profile == "ahrefs":
        if not user_agent:
            user_agent = "AhrefsSiteAudit/6.1 (+http://ahrefs.com/robot/)"
        # Ahrefs often crawls subdomains (incl. www) and uses keep-alive connections.
        if not allow_subdomains:
            allow_subdomains = True
        # For strict parity testing, disable retries so transient failures show up as timeouts.
        http_retries = min(http_retries, 0)
        connection_close = False
        # Keep canonical/hreflang discovery on by default (Ahrefs crawls multiple URL sources).
        strict_link_counts = True
        # Ahrefs defaults: check image/CSS/JS resources.
        check_resources = True
    if not user_agent:
        user_agent = "SEOAutopilot/1.0"

    return CrawlConfig(
        base_url=base_url,
        max_pages=max(1, args.max_pages),
        max_sitemap_urls=max(1, args.max_sitemap_urls),
        timeout_s=max(1.0, args.timeout),
        workers=max(1, args.workers),
        user_agent=user_agent,
        ignore_robots=args.ignore_robots,
        allow_subdomains=allow_subdomains,
        include_re=_compile_regex(args.include),
        exclude_re=_compile_regex(args.exclude),
        sitemap_urls=[s for s in args.sitemap if s],
        output_dir=output_dir,
        check_resources=bool(check_resources),
        max_resources=max(0, int(args.max_resources)),
        http_retries=max(0, int(http_retries)),
        connection_close=bool(connection_close),
        discover_canonicals=bool(discover_canonicals),
        discover_hreflang=bool(discover_hreflang),
        strict_link_counts=bool(strict_link_counts),
        profile=profile,
        pagespeed_enabled=bool(args.pagespeed),
        pagespeed_strategy=str(args.pagespeed_strategy or "mobile").strip().lower(),
        pagespeed_max_urls=max(0, int(args.pagespeed_max_urls)),
        pagespeed_timeout_s=max(1.0, float(args.pagespeed_timeout)),
        pagespeed_workers=max(1, int(args.pagespeed_workers)),
        pagespeed_api_key=pagespeed_api_key,
        gsc_api_enabled=bool(args.gsc_api),
        gsc_property_url=(str(args.gsc_property).strip() if isinstance(args.gsc_property, str) and str(args.gsc_property).strip() else None),
        gsc_days=max(1, int(args.gsc_days)),
        gsc_search_type=str(args.gsc_search_type or "web").strip().lower(),
        gsc_row_limit=max(1, int(args.gsc_row_limit)),
        gsc_timeout_s=max(1.0, float(args.gsc_timeout)),
        gsc_output_dir=(str(args.gsc_output_dir).strip() if isinstance(args.gsc_output_dir, str) and str(args.gsc_output_dir).strip() else None),
        gsc_credentials=(str(args.gsc_credentials).strip() if isinstance(args.gsc_credentials, str) and str(args.gsc_credentials).strip() else None),
        gsc_min_impressions=max(0, int(args.gsc_min_impressions)),
        gsc_inspection_enabled=bool(args.gsc_inspection),
        gsc_inspection_max_urls=max(0, int(args.gsc_inspection_max_urls)),
        gsc_inspection_timeout_s=max(1.0, float(args.gsc_inspection_timeout)),
        gsc_inspection_language=(str(args.gsc_inspection_language).strip() if isinstance(args.gsc_inspection_language, str) and str(args.gsc_inspection_language).strip() else None),
        bing_enabled=bool(args.bing),
        bing_queries_csv=(str(args.bing_queries_csv).strip() if isinstance(args.bing_queries_csv, str) and str(args.bing_queries_csv).strip() else None),
        bing_pages_csv=(str(args.bing_pages_csv).strip() if isinstance(args.bing_pages_csv, str) and str(args.bing_pages_csv).strip() else None),
        bing_min_impressions=max(0, int(args.bing_min_impressions)),
        bing_output_dir=(str(args.bing_output_dir).strip() if isinstance(args.bing_output_dir, str) and str(args.bing_output_dir).strip() else None),
        bing_site_url=(str(args.bing_site_url).strip() if isinstance(args.bing_site_url, str) and str(args.bing_site_url).strip() else None),
        bing_days=max(1, int(args.bing_days)),
        bing_timeout_s=max(1.0, float(args.bing_timeout)),
        bing_api_key=(str(args.bing_api_key).strip() if isinstance(args.bing_api_key, str) and str(args.bing_api_key).strip() else None),
        bing_access_token=((os.environ.get("BING_WEBMASTER_ACCESS_TOKEN") or "").strip() or None),
        bing_fetch_crawl_issues=(not bool(args.bing_no_crawl_issues)),
        bing_fetch_blocked_urls=(not bool(args.bing_no_blocked_urls)),
        bing_fetch_sitemaps=(not bool(args.bing_no_sitemaps)),
        bing_urlinfo_max=max(0, int(args.bing_urlinfo_max)),
    )


def _load_json_dict(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _find_previous_report(output_dir: str) -> tuple[str | None, Path | None]:
    """
    Best-effort previous crawl detection (Ahrefs-like change tracking).

    When the crawler is run via the UI, output_dir is typically:
      seo-runs/<slug>/<timestamp>/audit
    We look for the closest earlier sibling <timestamp>/audit/report.json.
    """
    out = Path(output_dir).expanduser().resolve()
    if out.name != "audit":
        return None, None
    ts_dir = out.parent
    if not re.fullmatch(r"\d{8}-\d{6}", ts_dir.name):
        return None, None
    slug_dir = ts_dir.parent
    if not slug_dir.exists() or not slug_dir.is_dir():
        return None, None

    siblings = sorted(
        [p.name for p in slug_dir.iterdir() if p.is_dir() and re.fullmatch(r"\d{8}-\d{6}", p.name)]
    )
    if ts_dir.name not in siblings:
        return None, None
    idx = siblings.index(ts_dir.name)
    for prev_ts in reversed(siblings[:idx]):
        report_path = slug_dir / prev_ts / "audit" / "report.json"
        if report_path.exists() and report_path.is_file():
            return prev_ts, report_path
    return None, None


def _pages_from_report(report: dict[str, Any]) -> list[PageData]:
    raw_pages = report.get("pages") if isinstance(report.get("pages"), list) else []
    fields = set(PageData.__dataclass_fields__.keys())
    out: list[PageData] = []
    for row in raw_pages:
        if not isinstance(row, dict):
            continue
        url = row.get("url")
        if not isinstance(url, str) or not url.strip():
            continue
        kwargs = {k: row.get(k) for k in fields if k in row}
        try:
            out.append(PageData(**kwargs))
        except Exception:
            continue
    return out


def main(argv: list[str]) -> int:
    config = _parse_args(argv)

    os.makedirs(config.output_dir, exist_ok=True)

    base_parts = urlsplit(config.base_url)
    root = _root_url(config.base_url)
    print(
        f"[CRAWL] 0/{config.max_pages} Start {config.base_url} | workers={config.workers} timeout={config.timeout_s}s",
        flush=True,
    )

    robots_error: str | None = None
    rp: RobotsRules | None = None
    discovered_sitemaps: list[str] = []
    system_fetches: list[dict[str, Any]] = []
    llms_ok, llms_err = _check_llms_txt(
        root,
        timeout_s=config.timeout_s,
        user_agent=config.user_agent,
        system_fetches=system_fetches,
        retries=int(config.http_retries),
        connection_close=bool(config.connection_close),
    )
    if not config.ignore_robots:
        rp, discovered_sitemaps, robots_error = _load_robots(
            root,
            timeout_s=config.timeout_s,
            user_agent=config.user_agent,
            system_fetches=system_fetches,
            retries=int(config.http_retries),
            connection_close=bool(config.connection_close),
        )

    sitemap_urls = list(dict.fromkeys([*config.sitemap_urls, *discovered_sitemaps]))
    if not sitemap_urls:
        default_sitemap = _normalize_url("/sitemap.xml", base=root)
        if default_sitemap:
            sitemap_urls = [default_sitemap]

    sitemap_seed_urls: list[str] = []
    sitemap_hreflang_by_url: dict[str, dict[str, str]] = {}
    sitemap_urlsets: dict[str, list[str]] = {}
    seen_sitemaps: set[str] = set()
    for sitemap_url in sitemap_urls:
        for u in _iter_sitemap_urls(
            sitemap_url,
            timeout_s=config.timeout_s,
            user_agent=config.user_agent,
            max_urls=config.max_sitemap_urls - len(sitemap_seed_urls),
            seen_sitemaps=seen_sitemaps,
            retries=int(config.http_retries),
            connection_close=bool(config.connection_close),
            hreflang_by_url=sitemap_hreflang_by_url,
            urlset_locs_by_sitemap=sitemap_urlsets,
            system_fetches=system_fetches,
        ):
            if len(sitemap_seed_urls) >= config.max_sitemap_urls:
                break
            if not _is_allowed_host(u, base_parts, allow_subdomains=config.allow_subdomains):
                continue
            if rp and not config.ignore_robots and not rp.can_fetch(config.user_agent, u):
                continue
            if not _should_include(u, config.include_re, config.exclude_re):
                continue
            sitemap_seed_urls.append(u)
    print(f"[SITEMAP] sitemaps={len(sitemap_urls)} seed_urls={len(sitemap_seed_urls)}", flush=True)

    # Probe a small set of "language root" variants without trailing slash (e.g. /fr → /fr/)
    # to surface canonicalization redirects similarly to Semrush.
    lang_root_probes: list[str] = []
    for locs in sitemap_urlsets.values():
        if not isinstance(locs, list):
            continue
        for u in locs:
            if not isinstance(u, str) or not u.startswith(("http://", "https://")):
                continue
            parts = urlsplit(u)
            if re.fullmatch(r"/[a-z]{2}/", parts.path or ""):
                no_slash_path = (parts.path or "").rstrip("/")
                if no_slash_path:
                    lang_root_probes.append(urlunsplit((parts.scheme, parts.netloc, no_slash_path, "", "")))

    probe_urls = _root_probe_urls(config.base_url)
    # Ahrefs-like: include sitemap file URLs as crawl targets (they appear as timed out / 3XX in Ahrefs exports).
    extra_sitemap_targets: list[str] = []  # Do not crawl sitemap XML files as pages (keeps parity with Ahrefs issues)
    start_urls = list(
        dict.fromkeys([config.base_url, *probe_urls, *lang_root_probes, *extra_sitemap_targets, *sitemap_seed_urls])
    )
    queue = deque(start_urls)
    # `seen` means "discovered/enqueued" (not "already crawled").
    # This makes the crawl frontier deterministic and avoids queue blow-ups due to duplicates.
    seen: set[str] = set(start_urls)
    pages: dict[str, PageData] = {}

    def _enqueue(urls: list[str]) -> None:
        for u in urls:
            if len(pages) + len(queue) >= config.max_pages * 5:
                continue
            if u in seen:
                continue
            if not _is_allowed_host(u, base_parts, allow_subdomains=config.allow_subdomains):
                continue
            if not _should_include(u, config.include_re, config.exclude_re):
                continue
            if rp and not config.ignore_robots and not rp.can_fetch(config.user_agent, u):
                continue
            seen.add(u)
            queue.append(u)

    with concurrent.futures.ThreadPoolExecutor(max_workers=config.workers) as executor:
        last_progress = time.monotonic()
        while queue and len(pages) < config.max_pages:
            batch: list[str] = []
            while queue and len(batch) < config.workers and (len(pages) + len(batch)) < config.max_pages:
                u = queue.popleft()
                if u in pages:
                    continue
                batch.append(u)

            if not batch:
                continue

            futures = [executor.submit(_extract_page, u, config, rp, base_parts) for u in batch]
            # Deterministic processing: wait for the batch, then process results in submission order.
            concurrent.futures.wait(futures)
            for fut in futures:
                page = fut.result()
                pages[page.url] = page
                # Ahrefs-like: only follow links from successfully fetched HTML pages.
                # This prevents error pages (4xx/5xx) from expanding the crawl frontier and inflating issues.
                if (
                    isinstance(page.status_code, int)
                    and page.status_code == 200
                    and not page.error
                    and (page.content_type or "").find("html") != -1
                ):
                    # Ahrefs-like: discover URLs from multiple sources on a page:
                    # - HTML links (<a href>)
                    # - Canonical URL
                    # - Hreflang alternates
                    to_enqueue = list(page.internal_links)
                    if config.discover_canonicals and page.canonical:
                        to_enqueue.append(page.canonical)
                    if config.discover_hreflang and page.hreflang:
                        to_enqueue.extend(page.hreflang.values())
                    if to_enqueue:
                        _enqueue(to_enqueue)
                now = time.monotonic()
                if len(pages) == 1 or (len(pages) % 10 == 0) or (now - last_progress) >= 2.0:
                    print(
                        f"[CRAWL] {len(pages)}/{config.max_pages} pages crawled | queue={len(queue)} seen={len(seen)}",
                        flush=True,
                    )
                    last_progress = now

    page_list = list(pages.values())
    page_list.sort(key=lambda p: p.url)
    print(f"[CRAWL] Done | pages={len(page_list)}", flush=True)

    pagespeed_meta: dict[str, Any] = {"enabled": False, "reason": "disabled_in_config"}
    if config.pagespeed_enabled:
        pagespeed_meta = _run_pagespeed(page_list, config)

    cwv_summary = _compute_cwv_summary(page_list)

    gsc_meta: dict[str, Any] = {"enabled": False, "reason": "disabled_in_config"}
    if config.gsc_api_enabled:
        print("[GSC] Fetching Search Console data…", flush=True)
        gsc_meta = _run_gsc_api(config)
        if gsc_meta.get("ok"):
            print(f"[GSC] OK | property={gsc_meta.get('property')} days={gsc_meta.get('days')}", flush=True)
        else:
            print(f"[GSC] Skipped/failed | reason={gsc_meta.get('reason')}", flush=True)

    bing_meta: dict[str, Any] = {"enabled": False, "reason": "disabled_in_config"}
    if config.bing_enabled:
        print("[BING] Fetching Bing data…", flush=True)
        # Prefer API when an API key is available; otherwise fallback to CSV mode.
        if (config.bing_api_key or os.environ.get("BING_WEBMASTER_API_KEY") or "").strip() or (
            config.bing_access_token or os.environ.get("BING_WEBMASTER_ACCESS_TOKEN") or ""
        ).strip():
            bing_meta = _run_bing_api(config)
        else:
            bing_meta = _run_bing_csv(config)
        if bing_meta.get("ok"):
            print("[BING] OK", flush=True)
        else:
            print(f"[BING] Skipped/failed | reason={bing_meta.get('reason')}", flush=True)

    prev_ts, prev_report_path = _find_previous_report(config.output_dir)
    prev_pages: list[PageData] | None = None
    if prev_report_path and prev_ts:
        prev_report = _load_json_dict(prev_report_path)
        if isinstance(prev_report, dict):
            prev_pages = _pages_from_report(prev_report)
            print(f"[COMPARE] previous={prev_ts} pages={len(prev_pages)}", flush=True)

    issues = _score_issues(
        page_list,
        sitemap_urls=set(sitemap_seed_urls),
        sitemap_urlsets=sitemap_urlsets,
        sitemap_hreflang=sitemap_hreflang_by_url,
        previous_pages=prev_pages,
        output_dir=config.output_dir,
        strict_link_counts=bool(config.strict_link_counts),
    )

    # --- Semrush-like robots/sitemap issues (system-level) ---
    robots_url = urljoin(root, "/robots.txt")
    if not config.ignore_robots:
        if not rp:
            if isinstance(robots_error, str) and "robots_parse_error" in robots_error:
                _write_issue_rows(Path(str(config.output_dir)).resolve() / "issues", "robots_invalid_format", [robots_url])
                issues["robots_invalid_format"] = {"count": 1, "examples": [robots_url]}
            else:
                _write_issue_rows(Path(str(config.output_dir)).resolve() / "issues", "robots_txt_not_found", [robots_url])
                issues["robots_txt_not_found"] = {"count": 1, "examples": [robots_url]}

    sitemap_fetches = [f for f in system_fetches if isinstance(f, dict) and f.get("type") == "sitemap"]
    sitemap_parse_fetches = [f for f in system_fetches if isinstance(f, dict) and f.get("type") == "sitemap_parse"]
    sitemap_not_found = sorted(
        {
            str(f.get("url") or "")
            for f in sitemap_fetches
            if isinstance(f.get("status_code"), int) and int(f.get("status_code")) >= 400 and str(f.get("url") or "")
        }
    )
    if sitemap_not_found:
        _write_issue_rows(Path(str(config.output_dir)).resolve() / "issues", "sitemap_xml_not_found", sitemap_not_found)
        issues["sitemap_xml_not_found"] = {"count": len(sitemap_not_found), "examples": sitemap_not_found[:200]}

    sitemap_invalid = sorted({str(f.get("url") or "") for f in sitemap_parse_fetches if f.get("error") == "sitemap_parse_error"})
    if sitemap_invalid:
        _write_issue_rows(Path(str(config.output_dir)).resolve() / "issues", "sitemap_invalid_format", sitemap_invalid)
        issues["sitemap_invalid_format"] = {"count": len(sitemap_invalid), "examples": sitemap_invalid[:200]}

    sitemap_too_large = sorted({str(f.get("url") or "") for f in sitemap_parse_fetches if f.get("error") == "sitemap_too_large"})
    if sitemap_too_large:
        _write_issue_rows(Path(str(config.output_dir)).resolve() / "issues", "sitemap_file_too_large", sitemap_too_large)
        issues["sitemap_file_too_large"] = {"count": len(sitemap_too_large), "examples": sitemap_too_large[:200]}

    if rp and not discovered_sitemaps and sitemap_urls:
        # Robots exists but has no Sitemap: directive.
        _write_issue_rows(Path(str(config.output_dir)).resolve() / "issues", "sitemap_not_in_robots", [robots_url])
        issues["sitemap_not_in_robots"] = {"count": 1, "examples": [robots_url]}

    if urlsplit(config.base_url).scheme.lower() == "https":
        http_in_sitemap = sorted({u for u in sitemap_seed_urls if isinstance(u, str) and u.startswith("http://")})
        if http_in_sitemap:
            _write_issue_rows(Path(str(config.output_dir)).resolve() / "issues", "sitemap_http_urls_for_https", http_in_sitemap)
            issues["sitemap_http_urls_for_https"] = {"count": len(http_in_sitemap), "examples": http_in_sitemap[:200]}

    # Semrush mega export can flag "Incorrect pages found in sitemap.xml" / "Orphaned sitemap pages" on sitemap files.
    # We surface those as system-level issues:
    # - incorrect_pages_found_in_sitemap_xml: sitemap fetch/parse anomalies or sitemapindex recursion limits.
    # - orphaned_sitemap_pages: sitemap URLs that have no incoming internal links (based on crawl graph).
    sitemap_incorrect = sorted(
        {
            str(f.get("url") or "")
            for f in (sitemap_fetches + sitemap_parse_fetches)
            if str(f.get("error") or "") in {"sitemap_parse_error", "sitemap_too_large"} and str(f.get("url") or "")
        }
    )
    if sitemap_incorrect:
        issues_dir = Path(str(config.output_dir)).resolve() / "issues"
        existing_rows: list[str] = []
        existing_path = issues_dir / "incorrect_pages_found_in_sitemap_xml.json"
        try:
            if existing_path.exists():
                existing_data = json.loads(existing_path.read_text(encoding="utf-8"))
                if isinstance(existing_data, list):
                    existing_rows = [str(x) for x in existing_data if isinstance(x, str)]
        except Exception:
            existing_rows = []
        merged = sorted(set(existing_rows + sitemap_incorrect))
        _write_issue_rows(issues_dir, "incorrect_pages_found_in_sitemap_xml", merged)
        issues["incorrect_pages_found_in_sitemap_xml"] = {"count": len(merged), "examples": merged[:200]}

    # --- Semrush-like security checks (site-level) ---
    def _tls_audit(host: str, port: int = 443) -> dict[str, Any]:
        out: dict[str, Any] = {"host": host, "port": port, "ok": False}
        try:
            ctx = ssl.create_default_context()
            with socket.create_connection((host, port), timeout=float(config.timeout_s)) as sock:
                with ctx.wrap_socket(sock, server_hostname=host) as ssock:
                    out["ok"] = True
                    out["tls_version"] = ssock.version()
                    out["cipher"] = (ssock.cipher() or (None, None, None))[0]
                    cert = ssock.getpeercert() or {}
                    out["cert"] = cert
                    try:
                        ssl.match_hostname(cert, host)
                        out["hostname_ok"] = True
                    except Exception as e:
                        out["hostname_ok"] = False
                        out["hostname_error"] = f"{type(e).__name__}: {e}"
                    not_after = cert.get("notAfter")
                    out["not_after"] = not_after
                    if isinstance(not_after, str) and not_after.strip():
                        try:
                            dt_na = dt.datetime.strptime(not_after, "%b %d %H:%M:%S %Y %Z").replace(tzinfo=dt.timezone.utc)
                            out["days_left"] = int((dt_na - dt.datetime.now(dt.timezone.utc)).total_seconds() // 86400)
                        except Exception:
                            pass
        except Exception as e:
            out["error"] = f"{type(e).__name__}: {e}"
        return out

    host = (base_parts.hostname or "").strip()
    if host:
        tls = _tls_audit(host)
        system_fetches.append({"type": "tls", **tls})
        issues_dir = Path(str(config.output_dir)).resolve() / "issues"
        if tls.get("ok") and isinstance(tls.get("days_left"), int) and int(tls["days_left"]) <= 30:
            row = {"host": host, "not_after": tls.get("not_after"), "days_left": int(tls.get("days_left") or 0)}
            _write_issue_rows(issues_dir, "certificate_expiration", [row])
            issues["certificate_expiration"] = {"count": 1, "examples": [host]}
        if tls.get("ok") and tls.get("hostname_ok") is False:
            row = {"host": host, "error": tls.get("hostname_error")}
            _write_issue_rows(issues_dir, "certificate_name_mismatch", [row])
            issues["certificate_name_mismatch"] = {"count": 1, "examples": [host]}
        ver = str(tls.get("tls_version") or "")
        if tls.get("ok") and ver in {"TLSv1", "TLSv1.1"}:
            _write_issue_rows(issues_dir, "old_tls_version", [{"host": host, "tls_version": ver}])
            issues["old_tls_version"] = {"count": 1, "examples": [host]}
        cipher = str(tls.get("cipher") or "").upper()
        if tls.get("ok") and any(tok in cipher for tok in ("RC4", "3DES", "NULL", "MD5")):
            _write_issue_rows(issues_dir, "insecure_cipher", [{"host": host, "cipher": tls.get("cipher")}])
            issues["insecure_cipher"] = {"count": 1, "examples": [host]}

        # HSTS (only meaningful for https)
        if urlsplit(config.base_url).scheme.lower() == "https":
            try:
                resp = _get_session(config.user_agent).get(root, timeout=float(config.timeout_s), allow_redirects=True)
                sts = (resp.headers.get("Strict-Transport-Security") or "").strip()
                system_fetches.append({"type": "hsts", "url": root, "present": bool(sts)})
                if not sts:
                    _write_issue_rows(issues_dir, "no_hsts", [root])
                    issues["no_hsts"] = {"count": 1, "examples": [root]}
            except Exception:
                pass

    # DNS resolution issue (best-effort based on request errors).
    dns_bad: list[str] = []
    for p in page_list:
        err = str(p.error or "")
        if not err:
            continue
        if any(tok in err for tok in ("NameResolutionError", "Temporary failure in name resolution", "gaierror", "nodename nor servname")):
            dns_bad.append(p.url)
    dns_bad = sorted(set(dns_bad))
    if dns_bad:
        _write_issue_rows(Path(str(config.output_dir)).resolve() / "issues", "dns_resolution_issue", dns_bad)
        issues["dns_resolution_issue"] = {"count": len(dns_bad), "examples": dns_bad[:200]}

    # Attach search performance opportunities as issues (GSC/Bing).
    if isinstance(gsc_meta, dict) and gsc_meta.get("enabled") and gsc_meta.get("ok"):
        gsc_issues = gsc_meta.get("issues") if isinstance(gsc_meta.get("issues"), dict) else {}
        if isinstance(gsc_issues.get("pages_quick_wins"), dict):
            issues["gsc_pages_quick_wins"] = gsc_issues["pages_quick_wins"]
        if isinstance(gsc_issues.get("pages_push_page_1"), dict):
            issues["gsc_pages_push_page_1"] = gsc_issues["pages_push_page_1"]
        inspection = gsc_meta.get("url_inspection") if isinstance(gsc_meta.get("url_inspection"), dict) else {}
        insp_issues = inspection.get("issues") if isinstance(inspection.get("issues"), dict) else {}
        if isinstance(insp_issues.get("indexing_errors"), dict):
            issues["gsc_indexing_errors"] = insp_issues["indexing_errors"]
        if isinstance(insp_issues.get("indexing_warnings"), dict):
            issues["gsc_indexing_warnings"] = insp_issues["indexing_warnings"]
        if isinstance(insp_issues.get("indexing_notices"), dict):
            issues["gsc_indexing_notices"] = insp_issues["indexing_notices"]

    if isinstance(bing_meta, dict) and bing_meta.get("enabled") and bing_meta.get("ok"):
        bing_issues = bing_meta.get("issues") if isinstance(bing_meta.get("issues"), dict) else {}
        if isinstance(bing_issues.get("pages_quick_wins"), dict):
            issues["bing_pages_quick_wins"] = bing_issues["pages_quick_wins"]
        if isinstance(bing_issues.get("pages_push_page_1"), dict):
            issues["bing_pages_push_page_1"] = bing_issues["pages_push_page_1"]
        if isinstance(bing_issues.get("crawl_issues"), dict):
            issues["bing_crawl_issues"] = bing_issues["crawl_issues"]
        if isinstance(bing_issues.get("blocked_urls"), dict):
            issues["bing_blocked_urls"] = bing_issues["blocked_urls"]
        if isinstance(bing_issues.get("sitemaps"), dict):
            issues["bing_sitemaps"] = bing_issues["sitemaps"]
        if isinstance(bing_issues.get("url_info_non_200"), dict):
            issues["bing_urlinfo_non_200"] = bing_issues["url_info_non_200"]
    resources: list[dict[str, Any]] = []
    resources_counts: dict[str, int] = {"image": 0, "javascript": 0, "css": 0}
    resources_external_counts: dict[str, int] = {"image": 0, "javascript": 0, "css": 0}
    resources_checked = bool(config.check_resources and config.max_resources > 0)
    if resources_checked:
        print(f"[RESOURCES] Checking internal resources | max_per_type={int(config.max_resources)}", flush=True)
        resources, resources_counts = _fetch_internal_resources(page_list, config, base_parts)
        issues.update(
            _score_resource_issues(
                page_list, resources, output_dir=config.output_dir, strict_link_counts=bool(config.strict_link_counts)
            )
        )
        ext_resources, ext_counts = _fetch_external_resources(page_list, config, base_parts)
        resources_external_counts = dict(ext_counts)
        issues.update(
            _score_external_resource_issues(
                page_list,
                ext_resources,
                timeout_s=float(config.timeout_s),
                user_agent=str(config.user_agent),
                output_dir=config.output_dir,
            )
        )
        print(
            f"[RESOURCES] Done | image={resources_counts.get('image', 0)} javascript={resources_counts.get('javascript', 0)} css={resources_counts.get('css', 0)}",
            flush=True,
        )

    meta = {
        "started_at": _now_iso(),
        "profile": str(config.profile or "default"),
        "base_url": config.base_url,
        "max_pages": config.max_pages,
        "pages_crawled": len(page_list),
        # Ahrefs-like: links found vs crawled (uncrawled links)
        "urls_discovered": int(len(seen)),
        "urls_uncrawled": int(max(0, len(seen) - len(page_list))),
        "previous_crawl": {"timestamp": prev_ts} if prev_ts else {"timestamp": None},
        "robots": "ignored" if config.ignore_robots else ("ok" if rp else f"unavailable ({robots_error})"),
        "sitemaps": sitemap_urls,
        "sitemap_seed_urls": len(sitemap_seed_urls),
        "user_agent": config.user_agent,
        "strict_link_counts": bool(config.strict_link_counts),
        "include": config.include_re.pattern if config.include_re else None,
        "exclude": config.exclude_re.pattern if config.exclude_re else None,
        "resources_checked": resources_checked,
        "resources_max_per_type": int(config.max_resources),
        "resources_counts": resources_counts,
        "resources_external_counts": resources_external_counts if resources_checked else {},
        "pagespeed": pagespeed_meta,
        "cwv": cwv_summary,
        "gsc_api": gsc_meta,
        "bing": bing_meta,
        "llms_txt": {"ok": bool(llms_ok), "reason": str(llms_err or "")},
        "thresholds": {
            "title_too_long_chars": 70,
            "title_too_short_chars": 20,
            "meta_description_too_long_chars": 160,
            "meta_description_too_short_chars": 100,
            "low_word_count": 200,
            "redirect_chain_too_long_hops": 3,
            "image_file_size_too_large_bytes": 400 * 1024,
            "javascript_file_size_too_large_bytes": 200 * 1024,
            "css_file_size_too_large_bytes": 100 * 1024,
            "cwv_lcp_poor_ms": 4000,
            "cwv_cls_poor": 0.25,
            "cwv_inp_poor_ms": 500,
            "cwv_tbt_poor_ms": 600,
        },
    }

    if not llms_ok:
        llms_url = urljoin(root, "/llms.txt")
        issues_dir = Path(str(config.output_dir)).resolve() / "issues"
        _write_issue_rows(issues_dir, "llms_txt_not_found", [llms_url])
        issues["llms_txt_not_found"] = {"count": 1, "examples": [llms_url]}

    report = {
        "meta": meta,
        "issues": issues,
        "pages": [dataclasses.asdict(p) for p in page_list],
    }
    if system_fetches:
        report["system_fetches"] = system_fetches
    if resources:
        report["resources"] = resources

    json_path = os.path.join(config.output_dir, "report.json")
    md_path = os.path.join(config.output_dir, "report.md")
    _write_json(json_path, report)
    _write_text(md_path, _render_md_report(config.base_url, page_list, issues, meta))

    print(f"[OK] Wrote {json_path}")
    print(f"[OK] Wrote {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
