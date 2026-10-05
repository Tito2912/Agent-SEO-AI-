"""Proof of a literal meta-noindex instruction, distinct from an injected preview header."""

import re
from urllib.parse import urlsplit

try:
    from .sitemap_redirects import FOLLOWABLE
except ImportError:
    from sitemap_redirects import FOLLOWABLE


def robots_tokens(value) -> frozenset[str]:
    return frozenset(re.split(r"[,;\s]+", value.strip().lower())) - {""} if isinstance(value, str) else frozenset()


def verified_urls(urls: list[str], pages: list[dict] | None, identify) -> tuple[list[str], list[str]]:
    requested = {}
    for row in pages or []:
        if isinstance(row, dict):
            requested.setdefault(identify(row.get("url")), []).append(row)

    def healthy(row, target):
        return (type(row.get("status_code")) is int and row["status_code"] == 200
            and not row.get("error") and not row.get("blocked_by_host")
            and identify(row.get("final_url") or row.get("url")) == target
            and str(row.get("content_type") or "").split(";", 1)[0].strip().lower() in {"text/html", "application/xhtml+xml"}
            and type(row.get("meta_robots_tag_count")) is int and row["meta_robots_tag_count"] == 1
            and "noindex" in robots_tokens(row.get("meta_robots")))

    accepted, refused = [], []
    for value in urls:
        source = identify(value)
        rows = requested.get(source, [])
        finals = {identify(row.get("final_url") or row.get("url")) for row in rows}
        if not source or len(finals) != 1:
            refused.append(source or "URL inconnue")
            continue
        target = finals.pop()
        direct = requested.get(target, [])
        valid = (target and direct and all(healthy(row, target) for row in rows + direct)
            and all(not row.get("redirect_chain") and not row.get("redirect_statuses") for row in direct)
            and len({robots_tokens(row.get("meta_robots")) for row in rows + direct}) == 1)
        witnesses = set()
        for row in rows:
            chain, codes = row.get("redirect_chain") or [], row.get("redirect_statuses") or []
            if source == target:
                valid = valid and not chain and not codes
                continue
            if (not isinstance(chain, list) or not isinstance(codes, list) or not chain or len(chain) != len(codes)
                    or any(type(code) is not int or code not in FOLLOWABLE for code in codes)):
                valid = False
                continue
            hops = [identify(url) for url in chain] + [target]
            if (hops[0] != source or not all(hops) or len(set(hops)) != len(hops)
                    or any(urlsplit(url).netloc != urlsplit(source).netloc or urlsplit(url).username or urlsplit(url).password for url in hops)
                    or source.startswith("https://") and any(url.startswith("http://") for url in hops)):
                valid = False
            witnesses.add((tuple(hops), tuple(codes)))
        if valid and (source == target or len(witnesses) == 1):
            if source not in accepted:
                accepted.append(source)
        else:
            refused.append(source)
    return accepted, sorted(set(refused))
