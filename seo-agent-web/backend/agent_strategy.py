"""Three explained actions from existing crawl/GSC evidence; no network or writes.

The ordering is a product rule, not a prediction of traffic or revenue. Search Console
rows are measurements; low CTR does not diagnose its cause. Competitor topics are
untrusted observations and never sufficient on their own to commission a page.
"""
from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Iterable, Mapping
from typing import Any
from urllib.parse import quote, unquote, urljoin, urlsplit, urlunsplit

try:
    from . import competitors, keywords
except ImportError:
    import competitors
    import keywords


INDEXABILITY = frozenset({
    "http_404", "http_4xx", "http_500", "http_5xx", "blocked_by_robots",
    "noindex_page", "noindex_follow_page", "sitemap_noindex_page",
    "sitemap_4xx_page", "sitemap_5xx_page",
})
METADATA = frozenset({"missing_title", "empty_title", "missing_meta_description"})
INACTIVE_DECISIONS = frozenset({"refused", "dismissed", "ignored", "done", "completed", "in_progress", "accepted"})
_UTILITY = re.compile(r"(?:privacy|confidentialit|mentions|legal|cgu|cgv|cookie|connexion|login)", re.I)


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) and parsed >= 0 else None


def project_url(value: Any, base_url: str) -> str:
    """Return a same-origin page within the project's base path, or an empty string.

    Fragments identify the same page; queries and meaningful trailing slashes remain.
    Relative crawl URLs are accepted, protocol-relative or credentialed URLs are not.
    """
    if not isinstance(value, str) or not isinstance(base_url, str):
        return ""
    value = value.strip()
    if not value or value.startswith("//") or any(c.isspace() or ord(c) < 32 or c == "\\" for c in unquote(value)):
        return ""
    try:
        base = urlsplit(base_url)
        target = urlsplit(urljoin(base_url.rstrip("/") + "/", value))
        if any(p.scheme not in {"https", "http"} or not p.hostname or p.username or p.password for p in (base, target)):
            return ""
        def origin(parts):
            return parts.scheme, parts.hostname.lower(), parts.port or (443 if parts.scheme == "https" else 80)
        if origin(base) != origin(target):
            return ""
        decoded = unquote(target.path)
        if any(segment in {".", ".."} for segment in decoded.split("/")):
            return ""
        prefix = base.path.rstrip("/")
        if prefix and not (target.path == prefix or target.path.startswith(prefix + "/")):
            return ""
        return urlunsplit((target.scheme, target.netloc.lower(), target.path or "/", target.query, ""))
    except (ValueError, UnicodeError):
        return ""


def _identity(kind: str, target: str) -> str:
    return hashlib.sha256((kind + "\n" + target).encode("utf-8")).hexdigest()[:32]


def _rows(values: Any) -> list[dict]:
    if not isinstance(values, Iterable) or isinstance(values, (str, bytes, Mapping)):
        return []
    return [dict(row) for row in values if isinstance(row, Mapping)]


def _text_values(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value.strip() else []
    return [v for v in value if isinstance(v, str) and v.strip()] if isinstance(value, (list, tuple)) else []


def _suppressed(decisions: Any) -> set[str]:
    if isinstance(decisions, Mapping):
        decisions = [{"action_key": key, "status": value} for key, value in decisions.items()]
    return {str(row.get("action_key")) for row in _rows(decisions)
            if isinstance(row.get("status", row.get("state")), str)
            and row.get("status", row.get("state")) in INACTIVE_DECISIONS}


def recommend_actions(*, base_url: str, slug: str, report: Mapping | None = None,
                      query_rows: Iterable[dict] = (), competitor_topics: Iterable[dict] = (),
                      existing_pages: Iterable[dict] = (), business_context: Mapping | None = None,
                      decisions: Any = (), limit: int = 3) -> list[dict[str, Any]]:
    """Return at most three actionable proposals; never fill a quota with invented work.

    ``report`` is the existing dashboard report (pages, issues with count/examples).
    ``query_rows`` accepts normalized keyword/page rows or native keys=[query,page].
    ``competitor_topics`` is the output of competitors.compare, with covered/terms.
    ``business_context`` optionally supplies activity/services/topics, important_pages/priority_urls,
    expected_indexable_urls and excluded_topics. Only explicit business terms may
    justify a new subject; generic site vocabulary is not a business decision.
    """
    if not project_url(base_url, base_url):
        return []
    try:
        limit = max(0, min(3, int(limit)))
    except (TypeError, ValueError, OverflowError):
        return []
    if not limit:
        return []
    context = dict(business_context) if isinstance(business_context, Mapping) else {}
    report = dict(report) if isinstance(report, Mapping) else {}
    pages = _rows(existing_pages) + _rows(report.get("pages"))
    pages = [p for p in pages if project_url(p.get("url"), base_url)]
    own_urls = {project_url(p["url"], base_url) for p in pages}
    gsc = []
    for row in _rows(query_rows):
        keys = row.get("keys") if isinstance(row.get("keys"), list) else []
        url = project_url(row.get("page") or (keys[1] if len(keys) > 1 else ""), base_url)
        impressions, clicks, position = (_number(row.get(key)) for key in ("impressions", "clicks", "position"))
        if (not url or any(v is None for v in (impressions, clicks, position)) or not position
                or clicks > impressions or impressions != int(impressions) or clicks != int(clicks)):
            continue
        row.update(page=url, impressions=impressions, clicks=clicks, position=position,
                   ctr=clicks / impressions if impressions else 0)
        gsc.append(row)
        own_urls.add(url)
    observed_impressions = {}
    for row in gsc:
        # Query/page rows can overlap. Max is enough to establish a measured signal
        # without advertising their sum as an exhaustive page total.
        observed_impressions[row["page"]] = max(observed_impressions.get(row["page"], 0), row["impressions"])
    priority = {project_url(u, base_url) for u in
                _text_values(context.get("priority_urls")) + _text_values(context.get("important_pages"))}
    expected = {project_url(u, base_url) for u in _text_values(context.get("expected_indexable_urls"))}
    declared_priority = priority | expected
    priority |= expected | {project_url(base_url, base_url)}
    priority.discard("")
    suppressed = _suppressed(decisions)
    root = "/projects/" + quote(str(slug), safe="")
    candidates = []

    def add(kind, target, title, reason, evidence, effort, uncertainty, href, ordering):
        key = _identity(kind, target)
        if key not in suppressed:
            candidates.append((ordering, {
                "action_key": key, "kind": kind, "title": title, "page_url": target if kind != "new_content" else "",
                "reason": reason, "evidence": evidence, "effort": effort,
                "uncertainty": uncertainty, "next_href": href,
            }))

    issues = report.get("issues")
    for issue_key, block in issues.items() if isinstance(issues, Mapping) else []:
        if issue_key not in INDEXABILITY | METADATA or not isinstance(block, Mapping):
            continue
        count = _number(block.get("count"))
        if count is None or count <= 0:
            continue
        examples = block.get("examples")
        for raw in examples if isinstance(examples, list) else []:
            url = project_url(raw.get("url") if isinstance(raw, Mapping) else raw, base_url)
            if not url or url not in own_urls or _UTILITY.search(url):
                continue
            important = url in priority or observed_impressions.get(url, 0) >= keywords.MIN_IMPRESSIONS
            if issue_key in INDEXABILITY and not important:
                continue
            if issue_key in METADATA:
                add("technical_metadata", url, "Préparer les métadonnées manquantes",
                    "L’analyse signale un titre ou une description manquants sur cette page.",
                    {"source": "crawl", "issue_key": issue_key, "reported_count": count,
                     "page_url": url, "gsc_available": bool(gsc)},
                    "court", "Le crawl constate l’absence ; il ne prédit pas un gain de trafic.",
                    root + "/issues", (2, -observed_impressions.get(url, 0), url, str(issue_key)))
            else:
                add("technical_indexability", url, "Vérifier l’accès et l’indexabilité de cette page",
                    "Une page importante présente un signal d’accès ou d’indexabilité à examiner avant de développer son contenu.",
                    {"source": "crawl", "issue_key": issue_key, "reported_count": count,
                     "page_url": url, "priority_declared": url in declared_priority,
                     "importance_basis": "customer" if url in declared_priority else "homepage"
                     if url == project_url(base_url, base_url) else "search_console",
                     "gsc_impressions_observed": observed_impressions.get(url), "gsc_available": bool(gsc)},
                    "à évaluer", "Un noindex ou un blocage peut être intentionnel : confirmer le rôle de la page avant de le retirer.",
                    root + "/issues", (0, -observed_impressions.get(url, 0), url, str(issue_key)))

    for opportunity in keywords.find_opportunities(gsc, limit=5000):
        url = opportunity["page"]
        if _UTILITY.search(url):
            continue
        near = opportunity["kind"] == keywords.KIND_NEAR_PAGE_ONE
        add("improve_existing", url, "Renforcer une page déjà visible" if near else "Examiner une page peu cliquée",
            "La page apparaît sur une recherche pertinente à examiner, avec une position moyenne entre 11 et 20." if near
            else "La page reçoit des impressions mais peu de clics sur cette recherche.",
            {"source": "search_console", "query": opportunity["query"], "page_url": url,
             "clicks": opportunity["clicks"], "impressions": opportunity["impressions"],
             "ctr": opportunity["ctr"], "position": opportunity["position"],
             "signal": opportunity["kind"]},
            "moyen", "L’intention, la concurrence et la présentation des résultats sont à vérifier ; le signal ne prouve pas que le titre est en cause.",
            root + "/keywords/opportunities", (1, -opportunity["impressions"], url, opportunity["query"]))

    business_terms = competitors.page_terms({"title": " ".join(_text_values(context.get("activity"))
                                           + _text_values(context.get("services")) + _text_values(context.get("topics")))})
    excluded_terms = competitors.page_terms({"title": " ".join(_text_values(context.get("excluded_topics")))})
    own_terms = [(p, competitors.page_terms(p)) for p in pages]
    topics_seen = []
    for topic in _rows(competitor_topics):
        if topic.get("covered") is not False or not business_terms:
            continue
        title = str(topic.get("competitor_title") or topic.get("title") or "").strip()
        terms = competitors.page_terms({"title": title})
        supplied = topic.get("terms")
        if isinstance(supplied, list):
            terms |= competitors.page_terms({"title": " ".join(v for v in supplied if isinstance(v, str))})
        matched = terms & business_terms
        if not title or len(matched) < 2 or terms & excluded_terms:
            continue
        # A competitor comparison may be stale or partial. Recheck observed own pages
        # and previously selected topic vocabulary before commissioning more work.
        if any(len(terms & t) >= min(3, len(terms)) and competitors.overlap(terms, t) >= 0.5
               for _, t in own_terms if t):
            continue
        if any(competitors.overlap(terms, t) >= 0.6 and len(terms & t) >= 2 for t in topics_seen):
            continue
        normalized = " ".join(sorted(terms))
        topics_seen.append(terms)
        add("new_content", normalized, "Évaluer un nouveau contenu utile à votre activité",
            "Un sujet observé chez un concurrent correspond à votre activité déclarée et n’a pas été repéré dans les pages analysées.",
            {"source": "competitor_crawl", "topic": title, "business_terms": sorted(matched),
             "competitor_url": project_url(topic.get("competitor_url"), topic.get("competitor_url")),
             "gsc_available": bool(gsc)},
            "important", "L’analyse est un échantillon : vérifier l’absence de page équivalente et la demande avant de rédiger. Aucun volume de recherche n’est déduit.",
            root + "/content", (3, -len(matched), normalized, ""))

    candidates.sort(key=lambda item: item[0])
    selected, destinations = [], set()
    for _, action in candidates:
        destination = action["page_url"] or action["action_key"]
        if destination in destinations:
            continue
        destinations.add(destination)
        selected.append(action)
        if len(selected) == limit:
            break
    return selected
