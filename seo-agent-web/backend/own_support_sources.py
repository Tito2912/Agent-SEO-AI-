"""Select public support evidence and preserve table labels within bounded excerpts."""

import re

from bs4 import BeautifulSoup


MAX_EXCERPT = 1800
PRICING = re.compile(
    r"\b(?:forfaits?|tarifs?|prix|co[uû]t(?:e(?:nt)?|er|s)?|pric(?:e|es|ing)|"
    r"costs?|cheap(?:er|est)?|payant(?:e|s|es)?|gratuit(?:e|s|es)?|paid|free|"
    r"plans?|quotas?|abonn\w*|subscri\w*)\b|\bhow\s+much\b|\bmoins\s+ch[eè]r(?:e|s|es)?\b",
    re.I,
)


def select_docs(question):
    # Prices must not disappear behind three other topics in a mixed question.
    selected = ["plans-et-quotas"] if PRICING.search(question) else []
    choices = [
        (r"google|search console", "connecter-search-console"),
        (r"github", "connecter-github"),
        (r"analys|crawl", "lancer-un-crawl"),
        (r"rapport|report|score", "lire-le-rapport"),
        (r"corr|fix|vérif|verif", "verifier-une-correction"),
    ]
    selected.extend(
        slug for pattern, slug in choices if re.search(pattern, question, re.I)
    )
    return list(dict.fromkeys(selected + ["depannage", "demarrer"]))[:3]


def _text(node):
    return " ".join(node.get_text(" ", strip=True).split())


def _blocks(parsed):
    for node in parsed.select("h2,h3,p,li,table"):
        if node.find_parent(["table", "li"]):
            continue
        if node.name != "table":
            if text := _text(node):
                yield text
            continue
        headers = None
        for row in node.select("tr"):
            cells = row.find_all(["th", "td"], recursive=False)
            values = [_text(cell) for cell in cells]
            if not values:
                continue
            if headers is None and all(cell.name == "th" for cell in cells):
                headers = values
                continue
            if headers and len(headers) == len(values) and all(headers[1:]):
                # A price without its plan name is not sufficient source evidence.
                pairs = [
                    header + ": " + value
                    for header, value in zip(headers[1:], values[1:])
                ]
                yield values[0] + " — " + "; ".join(pairs)
            else:
                yield " | ".join(values)


def excerpt(body_html):
    """Return complete text blocks; never cut a price away from its table headers."""
    parsed = BeautifulSoup(body_html, "html.parser")
    kept = []
    length = 0
    for block in _blocks(parsed):
        size = len(block) + int(bool(kept))
        if length + size > MAX_EXCERPT:
            return "\n".join(kept), True
        kept.append(block)
        length += size
    return "\n".join(kept), False
