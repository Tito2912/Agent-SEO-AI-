"""Proof of direct indexable HTTPS pages and exact sitemap scheme proposals."""

import re
from html.parser import HTMLParser
from urllib.parse import urlsplit


def excluded(value) -> bool:
    return bool({"noindex", "none"} & set(re.split(r"[,;\s:]+", str(value or "").lower())))


def verified_urls(urls: list[str], pages: list[dict] | None, identify, *, host: str | None = None) -> tuple[list[str], list[str]]:
    """Prove coherent independently requested, direct, indexable HTTPS documents."""
    requested = {}
    for row in pages or []:
        if isinstance(row, dict):
            requested.setdefault(identify(row.get("url")), []).append(row)
    accepted, refused = [], []
    for value in urls:
        target = identify(value)
        try:
            parsed = urlsplit(str(value or ""))
            valid = (parsed.scheme == "https" and parsed.netloc and not parsed.username and not parsed.password
                     and not parsed.fragment and parsed.port is None and (host is None or parsed.netloc.lower() == host))
        except ValueError:
            valid = False
        rows = requested.get(target, [])
        valid = valid and rows and all(
            type(row.get("status_code")) is int and row["status_code"] == 200
            and not row.get("error") and not row.get("blocked_by_host")
            and identify(row.get("final_url") or row.get("url")) == target
            and not row.get("redirect_chain") and not row.get("redirect_statuses")
            and str(row.get("content_type") or "").split(";", 1)[0].strip().lower() in {"text/html", "application/xhtml+xml"}
            and identify(row.get("canonical")) in {"", target}
            and not any(excluded(row.get(k)) for k in ("meta_robots", "x_robots_tag")) for row in rows)
        if valid and len({identify(row.get("canonical")) for row in rows}) == 1:
            if target not in accepted:
                accepted.append(target)
        else:
            refused.append(target or "URL inconnue")
    return accepted, sorted(set(refused))


def verified_pairs(pairs: list[dict], pages: list[dict] | None, identify) -> tuple[list[dict], list[str]]:
    targets, _ = verified_urls([pair.get('to') for pair in pairs], pages, identify)
    accepted, refused = [], []
    for pair in pairs:
        source, target = (identify(pair.get(k)) for k in ('from', 'to'))
        try:
            parsed = urlsplit(str(pair.get('from') or ''))
            valid = (parsed.scheme == 'http' and parsed.netloc and not parsed.username and not parsed.password
                     and not parsed.fragment and parsed.port is None and target == 'https:' + source[5:])
        except ValueError:
            valid = False
        normalized = {'from': source, 'to': target}
        if valid and target in targets:
            if normalized not in accepted:
                accepted.append(normalized)
        else:
            refused.append(source or 'URL inconnue')
    return accepted, sorted(set(refused))


def literal_destination(raw, canonical, inspect, identify) -> bool:
    document = inspect(raw, allow_noindex=True) if len(raw) <= 80_000 else None
    if (not document or any(excluded(value) for _, value in document["robots"])
            or [identify(value) for value in document["canonicals"]] != ([canonical] if canonical else [])):
        return False

    class Refresh(HTMLParser):
        found = False

        def handle_starttag(self, tag, attrs):
            if tag == "meta" and str(dict(attrs).get("http-equiv") or "").strip().lower() == "refresh":
                self.found = True

    parser = Refresh()
    parser.feed(raw)
    parser.close()
    return not parser.found
