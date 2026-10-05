"""Surgical verified sitemap cleanup using validated XML nodes and byte offsets."""

from collections.abc import Callable
from html import escape
from xml.parsers import expat
from urllib.parse import urlsplit

from defusedxml import ElementTree as ET
from defusedxml.common import DefusedXmlException

NS = "http://www.sitemaps.org/schemas/sitemap/0.9"


def rewrite_canonicals(content: str, pairs: list[dict[str, str]],
                       identify: Callable[[str], str]) -> tuple[str, int]:
    """Keep an existing master's entire entry; new masters never inherit alias metadata."""
    return _rewrite(content, pairs, identify, remove=False)


def remove_urls(content: str, urls: list[str], identify: Callable[[str], str]) -> tuple[str, int]:
    """Delete only exact URL entries, keeping every byte outside their nodes unchanged."""
    return _rewrite(content, [{"from": url, "to": ""} for url in urls], identify, remove=True)


def upgrade_schemes(content: str, pairs: list[dict[str, str]], identify: Callable[[str], str]) -> tuple[str, int]:
    """Change only loc text; prefer an existing HTTPS entry over converted metadata."""
    if any(not identify(p.get("from", "")).startswith("http://")
           or identify(p.get("to", "")) != "https:" + identify(p.get("from", ""))[5:] for p in pairs):
        return content, 0
    def exact(value):
        identity = identify(value)
        fragment = urlsplit(value).fragment
        return identity + '#' + fragment if identity and fragment else identity
    return _rewrite(content, pairs, exact, remove=False, scheme_only=True)


def _rewrite(content, pairs, identify, *, remove, scheme_only=False):
    mapping = {}
    for pair in pairs:
        source, target = (identify(pair.get(k, "")) for k in ("from", "to"))
        if not source or (not remove and not target) or source == target or mapping.get(source, target) != target:
            return content, 0
        mapping[source] = target
    if not mapping or set(mapping) & set(mapping.values()):
        return content, 0
    try:
        ET.fromstring(content, forbid_dtd=True)
        data = content.encode("utf-8")
        parser = expat.ParserCreate(encoding="UTF-8", namespace_separator="}")
        stack, roots = [], []

        def start(name, attrs):
            frame = {"name": name, "attrs": attrs, "start": parser.CurrentByteIndex, "children": [], "text": []}
            (stack[-1]["children"] if stack else roots).append(frame)
            stack.append(frame)

        def end(name):
            frame = stack.pop()
            frame["close"] = parser.CurrentByteIndex
            frame["end"] = data.find(b">", frame["close"]) + 1

        def text(value):
            if stack:
                stack[-1]["text"].append(value)

        parser.StartElementHandler, parser.EndElementHandler, parser.CharacterDataHandler = start, end, text
        parser.Parse(data, True)
        root = roots[0]
        namespace = root["name"].rsplit("}", 1)[0] + "}" if "}" in root["name"] else ""
        if len(roots) != 1 or namespace not in {"", NS + "}"} or root["name"] != namespace + "urlset":
            return content, 0
        entries = []
        for frame in root["children"]:
            locs = [node for node in frame["children"] if node["name"] == namespace + "loc"]
            if frame["name"] != namespace + "url" or frame["attrs"] or len(locs) != 1:
                return content, 0
            loc = locs[0]
            inner = data[data.find(b">", loc["start"]) + 1:loc["close"]]
            value = identify("".join(loc["text"]).strip())
            if not value or loc["attrs"] or loc["children"] or b"<" in inner:
                return content, 0
            entries.append((frame, loc, value))
        present = {value for _, _, value in entries if value not in mapping}
        edits, count = [], 0
        for frame, loc, value in entries:
            if value not in mapping:
                continue
            target = mapping[value]
            replacement = b""
            if not remove and target not in present:
                present.add(target)
                if scheme_only:
                    # A moved entry must not introduce unverified language annotations.
                    if any(node["name"] == "http://www.w3.org/1999/xhtml}link" for node in frame["children"]):
                        return content, 0
                    start = data.find(b">", loc["start"]) + 1
                    edits.append((start, loc["close"], escape(target, quote=False).encode("utf-8")))
                    count += 1
                    continue
                # Reuse only in-scope qualified names, not lastmod/images from a different URL.
                url_open = data[frame["start"]:data.find(b">", frame["start"]) + 1]
                loc_open = data[loc["start"]:data.find(b">", loc["start"]) + 1]
                loc_close = data[loc["close"]:loc["end"]]
                url_close = data[frame["close"]:frame["end"]]
                replacement = url_open + loc_open + escape(target, quote=False).encode("utf-8") + loc_close + url_close
            edits.append((frame["start"], frame["end"], replacement))
            count += 1
        output = data
        for begin, finish, replacement in reversed(edits):
            output = output[:begin] + replacement + output[finish:]
        ET.fromstring(output, forbid_dtd=True)
        return output.decode("utf-8"), count
    except (ET.ParseError, DefusedXmlException, ValueError, LookupError, expat.ExpatError):
        return content, 0
