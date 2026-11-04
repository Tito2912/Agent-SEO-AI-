"""Bounded navigation between public help pages and authorized project screens."""
from __future__ import annotations

import html
import re
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit


_HELP_PATH = re.compile(r"/(?:en/)?(?:docs|ressources-seo)(?:/[a-z0-9-]+)?/?")
_DOC_SCREENS = {
    "demarrer": "", "lire-le-rapport": "", "depannage": "",
    "exports-et-rapports": "", "fix-pack-sans-depot": "",
    "anomalies-et-priorites": "/issues", "lancer-un-crawl": "/settings/crawl",
    "automatiser-les-audits": "/settings/crawl",
    "corrections-automatiques": "/corrections", "cms-compatibles": "/cms",
    "corriger-dans-wordpress": "/cms", "corriger-dans-ghost": "/cms", "corriger-dans-prestashop": "/cms", "corriger-dans-wix": "/cms",
    "corriger-dans-webflow": "/cms",
    "verifier-une-correction": "/first-correction", "rediger-du-contenu": "/content",
    "analyser-les-concurrents": "/competitors", "backlinks": "/backlinks",
    "opportunites-de-mots-cles": "/keywords/opportunities",
    "suivre-la-performance": "/performance",
}
_GUIDE_SCREENS = {
    "audit-seo-technique-checklist-priorites": "/issues",
    "corriger-title-meta-description-grande-echelle": "/corrections",
    "corriger-le-seo-dans-le-code-pas-dans-un-rapport": "/corrections",
    "regression-seo-apres-mise-en-production": "/crawls",
    "frequence-crawl-seo-site-vitrine-ecommerce-blog": "/settings/crawl",
    "connecter-google-search-console-audit-mensuel": "/settings/search",
    "core-web-vitals-lire-signaux-seo": "",
    "ce-que-vos-concurrents-publient-et-pas-vous": "/competitors",
    "netlinking-opportunites-backlinks-sans-spam": "/backlinks/opportunities",
}


def is_help_path(path: str) -> bool:
    return bool(_HELP_PATH.fullmatch(path))


def link(path: str, context: dict | None) -> str:
    """Carry only a context already authorized by the caller, inside the help collections."""
    if not context:
        return path
    parts = urlsplit(path)
    if parts.scheme or parts.netloc or not is_help_path(parts.path):
        return path
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k not in {"project", "crawl"}]
    query.append(("project", context["slug"]))
    if context.get("crawl"):
        query.append(("crawl", context["crawl"]))
    return urlunsplit(("", "", parts.path, urlencode(query), parts.fragment))


def body_links(body: str, context: dict | None) -> str:
    """Adapt rendered links on a request copy, never the shared article cache or its source."""
    if not context:
        return body

    def replace(match: re.Match) -> str:
        original = match.group(2)
        target = link(html.unescape(original), context)
        return match.group(0) if target == html.unescape(original) else f'{match.group(1)}{html.escape(target, quote=True)}"'

    return re.sub(r'(<a\b[^>]*?\bhref=")([^"]*)"', replace, body)


def app_path(collection: str, slug: str, context: dict | None, fallback: str = "/") -> str:
    """Only fixed, read-only screens; account settings retain their existing destination."""
    screens = _DOC_SCREENS if collection == "docs" else _GUIDE_SCREENS
    if not context or slug not in screens:
        return fallback
    screen = screens[slug]
    path = "/projects/" + quote(context["slug"], safe="") + screen
    if context.get("crawl") and screen in {"", "/issues"}:
        path += "?" + urlencode({"crawl": context["crawl"]})
    return path
