"""Evidence checks for the bounded literal-Netlify sitemap redirect path."""

from pathlib import PurePosixPath
import re
import shlex
from urllib.parse import urljoin, urlsplit

FOLLOWABLE = {301, 302, 303, 307, 308}


def config_path(paths: list[str], sitemap: str) -> str:
    candidates = [path for path in paths if PurePosixPath(path).name == "_redirects"]
    expected = str(PurePosixPath(sitemap).with_name("_redirects"))
    return expected if candidates == [expected] else ""


def verified_pairs(pairs: list[dict], pages: list[dict] | None, identify) -> tuple[list[dict], list[str]]:
    requested = {}
    for row in pages or []:
        if isinstance(row, dict):
            requested.setdefault(identify(row.get("url")), []).append(row)

    def healthy(row, target):
        canonical = identify(row.get("canonical"))
        return (type(row.get("status_code")) is int and row["status_code"] == 200
            and not row.get("error") and not row.get("blocked_by_host")
            and identify(row.get("final_url") or row.get("url")) == target
            and str(row.get("content_type") or "").split(";", 1)[0].strip().lower() in {"text/html", "application/xhtml+xml"}
            and (not row.get("canonical") or canonical == target)
            and not any("noindex" in re.split(r"[,;\s:]+", str(row.get(key) or "").lower())
                        for key in ("meta_robots", "x_robots_tag")))

    intentions = {}
    for pair in pairs:
        intentions.setdefault(identify(pair.get("from")), set()).add(identify(pair.get("to")))
    accepted, refused = [], []
    for pair in pairs:
        source, target = (identify(pair.get(key)) for key in ("from", "to"))
        sources, masters = requested.get(source, []), requested.get(target, [])
        valid = (source and target and source != target and identify(pair.get("page")) == source
            and len(intentions[source]) == 1 and sources and masters
            and all(healthy(row, target) for row in sources + masters)
            and all(not row.get("redirect_statuses") and not row.get("redirect_chain") for row in masters)
            and len({identify(row.get("canonical")) for row in sources + masters}) == 1)
        witnesses = set()
        for row in sources:
            chain, statuses = row.get("redirect_chain"), row.get("redirect_statuses")
            if (not isinstance(chain, list) or not isinstance(statuses, list) or not chain
                    or len(chain) != len(statuses)
                    or any(type(code) is not int or code not in FOLLOWABLE for code in statuses)):
                valid = False
                continue
            urls = [identify(url) for url in chain] + [target]
            if (urls[0] != source or not all(urls) or len(set(urls)) != len(urls)
                    or any(urlsplit(url).netloc != urlsplit(source).netloc or urlsplit(url).username
                           or urlsplit(url).password for url in urls)
                    or source.startswith("https://") and any(url.startswith("http://") for url in urls)):
                valid = False
            witnesses.add((tuple(urls), tuple(statuses)))
        if valid and len(witnesses) == 1:
            value = {"page": source, "from": source, "to": target}
            if value not in accepted:
                accepted.append(value)
        else:
            refused.append(source or "URL inconnue")
    return accepted, sorted(set(refused))


def rules_match(content: str, pairs: list[dict], pages: list[dict], identify, shadowed: set[str] | None = None,
                *, direct: set[str] | None = None) -> bool:
    """Require exact flat rules, never infer wildcard precedence or condition semantics."""
    if (not pairs and not direct) or len(content) > 80_000:
        return False
    origin = pairs[0]["from"] if pairs else min(direct)
    rules = {}
    try:
        for line in content.splitlines():
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            if "'" in line or '"' in line:
                return False
            tokens = shlex.split(line, comments=True)
            if len(tokens) != 3 or not re.fullmatch(r"\d{3}!?", tokens[2]):
                return False
            if any(not token.startswith(("/", "https://", "http://")) or token.startswith("//") for token in tokens[:2]):
                return False
            source, target = (identify(urljoin(origin, token)) for token in tokens[:2])
            if any(not value or re.search(r"[:*]", urlsplit(value).path) or urlsplit(value).query
                   or urlsplit(value).fragment for value in (source, target)) or source in rules:
                return False
            rules[source] = (target, int(tokens[2].removesuffix("!")), tokens[2].endswith("!"))
        if set(direct or []) & rules.keys():
            return False
        requested = {identify(row.get("url")): row for row in pages if isinstance(row, dict)}
        for pair in pairs:
            if pair["to"] in rules:
                return False
            row = requested[pair["from"]]
            chain = [identify(url) for url in row["redirect_chain"]] + [pair["to"]]
            if any(urlsplit(url).query for url in chain):
                return False
            for source, target, code in zip(chain, chain[1:], row["redirect_statuses"], strict=False):
                rule = rules.get(source)
                if not rule or rule[:2] != (target, code) or source in (shadowed or set()) and not rule[2]:
                    return False
    except (KeyError, TypeError, ValueError):
        return False
    return True
