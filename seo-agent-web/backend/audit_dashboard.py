from __future__ import annotations

import json
import math
import os
import re
from functools import lru_cache
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


Severity = str  # "error" | "warning" | "notice"


SEVERITY_ORDER: dict[Severity, int] = {"error": 0, "warning": 1, "notice": 2}

REPO_ROOT = Path(__file__).resolve().parents[2]

# Keys that are present in report.json but are not shown as "issues" in the UI.
# (Some are legacy catch-alls or noisy counters; keep them in report.json for debugging.)
NON_ISSUE_KEYS: set[str] = {
    "internal_pages",
    "bad_status",
    "missing_canonical",
    # CWV helper issues (used for drilldowns from the CWV card).
    "cwv_lcp_pages_to_fix",
    "cwv_tbt_pages_to_fix",
    "cwv_cls_pages_to_fix",
}


def _comparable_meta(cur: dict[str, Any], prev: dict[str, Any]) -> bool:
    """
    Only compute issue deltas when the crawl configuration is compatible.

    This avoids confusing "+XX" changes when users switched profile (default vs ahrefs),
    toggled resources, or aligned to an Ahrefs ZIP (strict parity mode).
    """
    keys = [
        "profile",
        "user_agent",
        "include",
        "exclude",
        "resources_checked",
        "strict_link_counts",
    ]
    for k in keys:
        if (cur.get(k) or "") != (prev.get(k) or ""):
            return False
    # Thresholds influence many content issues.
    if cur.get("thresholds") != prev.get("thresholds"):
        return False
    return True


@dataclass(frozen=True)
class IssueMeta:
    key: str
    label: str
    category: str
    severity: Severity
    description: str | None = None


@lru_cache(maxsize=1)
def _parity_mapping_info() -> dict[str, dict[str, set[str]]]:
    """
    Parse tools/parity/mapping.yml (lightweight, no YAML dependency).

    Returns:
      - keys_by_tool: issue keys that are mapped for the tool (used for "Ahrefs parity" view)
      - link_exports_by_tool: issue keys that correspond to "* - links" exports (hidden from Ahrefs "All issues" table)
    """
    path = (REPO_ROOT / "tools" / "parity" / "mapping.yml").resolve()
    keys_by_tool: dict[str, set[str]] = {"ahrefs": set(), "semrush": set()}
    link_exports_by_tool: dict[str, set[str]] = {"ahrefs": set(), "semrush": set()}
    if not path.exists():
        return {"keys_by_tool": keys_by_tool, "link_exports_by_tool": link_exports_by_tool}

    current_tool: str | None = None
    current_match: str | None = None
    try:
        for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("- tool:"):
                current_tool = line.split(":", 1)[1].strip().lower()
                if current_tool not in keys_by_tool:
                    current_tool = None
                current_match = None
                continue
            if current_tool and line.startswith("match:"):
                current_match = line.split(":", 1)[1].strip()
                continue
            if current_tool and line.startswith("issue_key:"):
                issue_key = line.split(":", 1)[1].strip().strip('"').strip("'")
                if not issue_key:
                    continue
                keys_by_tool[current_tool].add(issue_key)
                if isinstance(current_match, str) and ("- links" in current_match or "-links" in current_match):
                    link_exports_by_tool[current_tool].add(issue_key)
    except Exception:
        return {"keys_by_tool": keys_by_tool, "link_exports_by_tool": link_exports_by_tool}
    return {"keys_by_tool": keys_by_tool, "link_exports_by_tool": link_exports_by_tool}


def _allowed_issue_keys_for_profile(profile: str | None) -> set[str] | None:
    prof = str(profile or "").strip().lower()
    if prof == "ahrefs":
        keys = _parity_mapping_info().get("keys_by_tool", {}).get("ahrefs") or set()
        return set(keys) if keys else None
    return None


ISSUE_CATALOG: dict[str, IssueMeta] = {
    # Internal / HTTP status
    "bad_status": IssueMeta("bad_status", "Statut HTTP anormal", "Internal pages", "error"),
    "http_404": IssueMeta("http_404", "Page 404", "Internal pages", "error"),
    "http_4xx": IssueMeta("http_4xx", "Page 4XX", "Internal pages", "error"),
    "http_500": IssueMeta("http_500", "Page 500", "Internal pages", "error"),
    "http_5xx": IssueMeta("http_5xx", "Page 5XX", "Internal pages", "error"),
    "timed_out": IssueMeta("timed_out", "Timed out", "Internal pages", "error"),
    "timed_out_links": IssueMeta("timed_out_links", "Timed out — liens", "Internal pages", "error"),
    "https_http_mixed_content": IssueMeta(
        "https_http_mixed_content", "Mixed content (HTTPS → HTTP)", "Internal pages", "warning"
    ),
    "blocked_by_robots": IssueMeta("blocked_by_robots", "Bloqué par robots.txt", "Other", "warning"),
    "slow_page": IssueMeta("slow_page", "Slow page", "Usability and performance", "warning"),
    "pages_with_poor_lcp": IssueMeta(
        "pages_with_poor_lcp", "Pages with poor LCP", "Usability and performance", "warning"
    ),
    "pages_with_poor_cls": IssueMeta(
        "pages_with_poor_cls", "Pages with poor CLS", "Usability and performance", "warning"
    ),
    "pages_with_poor_inp": IssueMeta(
        "pages_with_poor_inp", "Pages with poor INP", "Usability and performance", "warning"
    ),
    "pages_with_poor_tbt": IssueMeta(
        "pages_with_poor_tbt", "Pages with poor TBT", "Usability and performance", "warning"
    ),
    "cwv_lcp_pages_to_fix": IssueMeta(
        "cwv_lcp_pages_to_fix", "CWV: LCP — pages à corriger", "Usability and performance", "warning"
    ),
    "cwv_tbt_pages_to_fix": IssueMeta(
        "cwv_tbt_pages_to_fix", "CWV: TBT — pages à corriger", "Usability and performance", "warning"
    ),
    "cwv_cls_pages_to_fix": IssueMeta(
        "cwv_cls_pages_to_fix", "CWV: CLS — pages à corriger", "Usability and performance", "warning"
    ),
    "structured_data_schema_org_validation_error": IssueMeta(
        "structured_data_schema_org_validation_error",
        "Structured data has schema.org validation error",
        "Other",
        "notice",
    ),
    "structured_data_google_rich_results_validation_error": IssueMeta(
        "structured_data_google_rich_results_validation_error",
        "Structured data has Google rich results validation error",
        "Other",
        "notice",
    ),
    "page_has_links_to_redirect_links_not_indexable": IssueMeta(
        "page_has_links_to_redirect_links_not_indexable",
        "Liens vers redirections (non indexable) — détails",
        "Links",
        "warning",
    ),
    "internal_pages": IssueMeta("internal_pages", "Pages internes (métrique)", "Other", "notice"),
    # Search performance (GSC / Bing)
    "gsc_pages_quick_wins": IssueMeta(
        "gsc_pages_quick_wins",
        "GSC: quick wins CTR (pages)",
        "Search performance",
        "notice",
        description="Pages en positions 3–10 avec CTR sous l’attendu (opportunités).",
    ),
    "gsc_pages_push_page_1": IssueMeta(
        "gsc_pages_push_page_1",
        "GSC: à pousser en page 1 (pages)",
        "Search performance",
        "notice",
        description="Pages en positions 11–20 avec potentiel de clicks (opportunités).",
    ),
    "gsc_indexing_errors": IssueMeta(
        "gsc_indexing_errors",
        "Google (GSC URL Inspection): erreurs",
        "Google Search Console",
        "error",
        description="Échantillon de pages inspectées via l’API URL Inspection (indexation/crawl/robots).",
    ),
    "gsc_indexing_warnings": IssueMeta(
        "gsc_indexing_warnings",
        "Google (GSC URL Inspection): avertissements",
        "Google Search Console",
        "warning",
        description="Échantillon de pages inspectées via l’API URL Inspection (indexation/crawl/robots).",
    ),
    "gsc_indexing_notices": IssueMeta(
        "gsc_indexing_notices",
        "Google (GSC URL Inspection): avis",
        "Google Search Console",
        "notice",
        description="Échantillon de pages inspectées via l’API URL Inspection (indexation/crawl/robots).",
    ),
    "bing_pages_quick_wins": IssueMeta(
        "bing_pages_quick_wins",
        "Bing: quick wins CTR (pages)",
        "Search performance",
        "notice",
        description="Pages en positions 3–10 avec CTR sous l’attendu (export Bing).",
    ),
    "bing_pages_push_page_1": IssueMeta(
        "bing_pages_push_page_1",
        "Bing: à pousser en page 1 (pages)",
        "Search performance",
        "notice",
        description="Pages en positions 11–20 avec potentiel de clicks (export Bing).",
    ),
    "bing_crawl_issues": IssueMeta(
        "bing_crawl_issues",
        "Bing: crawl issues",
        "Search performance",
        "warning",
        description="Issues de crawl remontées par Bing Webmaster Tools (API).",
    ),
    "bing_blocked_urls": IssueMeta(
        "bing_blocked_urls",
        "Bing: URLs bloquées",
        "Search performance",
        "notice",
        description="URLs bloquées côté Bing Webmaster Tools (API).",
    ),
    "bing_sitemaps": IssueMeta(
        "bing_sitemaps",
        "Bing: sitemaps (API)",
        "Sitemaps",
        "notice",
        description="Sitemaps connus par Bing Webmaster Tools (API).",
    ),
    "llms_txt_not_found": IssueMeta(
        "llms_txt_not_found",
        "Llms.txt not found",
        "Sitemaps",
        "notice",
        description="Le fichier /llms.txt est absent (référence Semrush).",
    ),
    "hreflang_conflicts_within_page_source_code": IssueMeta(
        "hreflang_conflicts_within_page_source_code",
        "Hreflang conflicts within page source code",
        "Localization",
        "warning",
        description="Même hreflang déclaré plusieurs fois avec des URLs différentes sur une même page.",
    ),
    "broken_external_images": IssueMeta(
        "broken_external_images",
        "Images externes cassées",
        "Resources",
        "warning",
        description="Pages qui chargent des images externes en erreur (4XX/5XX/timeout).",
    ),
    "disallowed_external_resources": IssueMeta(
        "disallowed_external_resources",
        "Ressources externes refusées",
        "Resources",
        "notice",
        description="Ressources externes refusées (HTTP 401/403) sur les pages crawled.",
    ),
    "broken_external_js_css": IssueMeta(
        "broken_external_js_css",
        "JS/CSS externes cassés",
        "Resources",
        "warning",
        description="Pages qui chargent des ressources JS/CSS externes en erreur (4XX/5XX/timeout).",
    ),
    "robots_txt_not_found": IssueMeta(
        "robots_txt_not_found",
        "Robots.txt introuvable",
        "Robots",
        "warning",
        description="Le fichier /robots.txt est introuvable ou inaccessible.",
    ),
    "robots_invalid_format": IssueMeta(
        "robots_invalid_format",
        "Format robots.txt invalide",
        "Robots",
        "warning",
        description="Robots.txt présent mais non parseable.",
    ),
    "sitemap_xml_not_found": IssueMeta(
        "sitemap_xml_not_found",
        "Sitemap.xml introuvable",
        "Sitemaps",
        "warning",
        description="Le(s) sitemap(s) configuré(s) est/sont introuvable(s) ou inaccessible(s).",
    ),
    "sitemap_invalid_format": IssueMeta(
        "sitemap_invalid_format",
        "Format sitemap.xml invalide",
        "Sitemaps",
        "warning",
        description="Le(s) sitemap(s) existe(nt) mais ne sont pas des XML valides.",
    ),
    "sitemap_not_in_robots": IssueMeta(
        "sitemap_not_in_robots",
        "Sitemap.xml non déclaré dans robots.txt",
        "Sitemaps",
        "notice",
        description="Robots.txt ne contient pas de directive Sitemap.",
    ),
    "sitemap_http_urls_for_https": IssueMeta(
        "sitemap_http_urls_for_https",
        "URLs HTTP dans un sitemap de site HTTPS",
        "Sitemaps",
        "warning",
        description="URLs HTTP trouvées dans le sitemap alors que le site est en HTTPS.",
    ),
    "sitemap_file_too_large": IssueMeta(
        "sitemap_file_too_large",
        "Sitemap trop volumineux",
        "Sitemaps",
        "notice",
        description="Sitemap trop volumineux (seuil 50MB, best-effort).",
    ),
    "certificate_expiration": IssueMeta(
        "certificate_expiration",
        "Expiration du certificat",
        "Security",
        "warning",
        description="Certificat TLS expiré ou proche d’expiration (≤ 30 jours).",
    ),
    "certificate_name_mismatch": IssueMeta(
        "certificate_name_mismatch",
        "Certificat TLS non conforme au nom de domaine",
        "Security",
        "error",
        description="Le certificat TLS ne correspond pas au nom de domaine.",
    ),
    "old_tls_version": IssueMeta(
        "old_tls_version",
        "Version TLS obsolète",
        "Security",
        "warning",
        description="Négociation TLSv1/TLSv1.1 détectée.",
    ),
    "insecure_cipher": IssueMeta(
        "insecure_cipher",
        "Algorithmes de chiffrement faibles",
        "Security",
        "warning",
        description="Cipher potentiellement faible détecté (RC4/3DES/NULL/MD5).",
    ),
    "no_hsts": IssueMeta(
        "no_hsts",
        "HSTS absent",
        "Security",
        "notice",
        description="Header Strict-Transport-Security absent.",
    ),
    "dns_resolution_issue": IssueMeta(
        "dns_resolution_issue",
        "Problème de résolution DNS",
        "Security",
        "error",
        description="Erreur de résolution DNS détectée pendant le crawl.",
    ),
    "permanent_redirects": IssueMeta(
        "permanent_redirects",
        "Permanent redirects",
        "Redirects",
        "notice",
        description="URLs qui redirigent en 301 (Semrush).",
    ),
    "low_text_to_html_ratio": IssueMeta(
        "low_text_to_html_ratio",
        "Low text to HTML ratio",
        "Content",
        "notice",
        description="Pages avec peu de texte comparé à la taille HTML.",
    ),
    "incorrect_pages_found_in_sitemap_xml": IssueMeta(
        "incorrect_pages_found_in_sitemap_xml",
        "Incorrect pages found in sitemap.xml",
        "Sitemaps",
        "warning",
        description="Sitemap(s) détecté(s) comme incorrects (Semrush).",
    ),
    "orphaned_sitemap_pages": IssueMeta(
        "orphaned_sitemap_pages",
        "Orphaned sitemap pages",
        "Sitemaps",
        "notice",
        description="Pages sitemap sans liens internes entrants (Semrush).",
    ),
    "bing_urlinfo_non_200": IssueMeta(
        "bing_urlinfo_non_200",
        "Bing: UrlInfo non-200",
        "Search performance",
        "notice",
        description="URLs (échantillon) avec HttpStatus non-200 selon Bing UrlInfo (API).",
    ),
    # Indexability / Canonical / Robots
    "canonical_points_to_4xx": IssueMeta("canonical_points_to_4xx", "Canonical → 4XX", "Indexability", "error"),
    "canonical_points_to_5xx": IssueMeta("canonical_points_to_5xx", "Canonical → 5XX", "Indexability", "error"),
    "canonical_points_to_redirect": IssueMeta(
        "canonical_points_to_redirect", "Canonical → redirection", "Indexability", "error"
    ),
    "non_canonical_page_specified_as_canonical_one": IssueMeta(
        "non_canonical_page_specified_as_canonical_one",
        "Page non canonique définie comme canonique",
        "Indexability",
        "warning",
    ),
    "canonical_from_http_to_https": IssueMeta(
        "canonical_from_http_to_https", "Canonical de HTTP vers HTTPS", "Indexability", "notice"
    ),
    "canonical_from_https_to_http": IssueMeta(
        "canonical_from_https_to_http", "Canonical de HTTPS vers HTTP", "Indexability", "warning"
    ),
    "canonical_url_changed": IssueMeta(
        "canonical_url_changed", "Canonical URL changed", "Indexability", "notice"
    ),
    "indexable_page_became_non_indexable": IssueMeta(
        "indexable_page_became_non_indexable", "Indexable page became non-indexable", "Indexability", "notice"
    ),
    "noindex_page_became_indexable": IssueMeta(
        "noindex_page_became_indexable", "Noindex page became indexable", "Indexability", "notice"
    ),
    "missing_canonical": IssueMeta("missing_canonical", "Canonical manquant", "Indexability", "warning"),
    "nofollow_in_html_and_http_header": IssueMeta(
        "nofollow_in_html_and_http_header", "Nofollow (HTML + header)", "Indexability", "notice"
    ),
    "nofollow_page": IssueMeta("nofollow_page", "Page en nofollow", "Indexability", "notice"),
    "noindex_in_html_and_http_header": IssueMeta(
        "noindex_in_html_and_http_header", "Noindex (HTML + header)", "Indexability", "notice"
    ),
    "noindex_page": IssueMeta("noindex_page", "Page en noindex", "Indexability", "warning"),
    "noindex_and_nofollow_page": IssueMeta(
        "noindex_and_nofollow_page", "Page noindex + nofollow", "Indexability", "notice"
    ),
    "noindex_follow_page": IssueMeta("noindex_follow_page", "Page noindex, follow", "Indexability", "notice"),
    # Content changes (diff vs previous crawl)
    "title_tag_changed": IssueMeta("title_tag_changed", "Balise title modifiée", "Content", "notice"),
    "meta_description_changed": IssueMeta("meta_description_changed", "Meta description changed", "Content", "notice"),
    "h1_tag_changed": IssueMeta("h1_tag_changed", "H1 tag changed", "Content", "notice"),
    "word_count_changed": IssueMeta("word_count_changed", "Word count changed", "Content", "notice"),
    # Links (internal)
    "canonical_url_has_no_incoming_internal_links": IssueMeta(
        "canonical_url_has_no_incoming_internal_links",
        "Canonical sans liens internes entrants",
        "Links",
        "error",
    ),
    "https_page_has_internal_links_to_http": IssueMeta(
        "https_page_has_internal_links_to_http", "Page HTTPS → liens internes HTTP", "Links", "warning"
    ),
    "http_page_has_internal_links_to_https": IssueMeta(
        "http_page_has_internal_links_to_https", "Page HTTP → liens internes HTTPS", "Links", "notice"
    ),
    "orphan_page": IssueMeta("orphan_page", "Page orpheline (0 lien entrant)", "Links", "warning"),
    "orphan_page_indexable": IssueMeta(
        "orphan_page_indexable", "Page orpheline (indexable)", "Links", "warning"
    ),
    "orphan_page_not_indexable": IssueMeta(
        "orphan_page_not_indexable", "Page orpheline (non indexable)", "Links", "warning"
    ),
    "page_has_links_to_broken_page": IssueMeta(
        "page_has_links_to_broken_page", "Page → lien cassé (4XX/5XX)", "Links", "warning"
    ),
    "page_has_links_to_broken_page_indexable": IssueMeta(
        "page_has_links_to_broken_page_indexable",
        "Page → lien cassé (indexable)",
        "Links",
        "error",
    ),
    "page_has_links_to_broken_page_not_indexable": IssueMeta(
        "page_has_links_to_broken_page_not_indexable",
        "Page → lien cassé (non indexable)",
        "Links",
        "warning",
    ),
    "page_has_links_to_broken_page_links_indexable": IssueMeta(
        "page_has_links_to_broken_page_links_indexable",
        "Liens vers pages cassées (indexable) — détails",
        "Links",
        "error",
    ),
    "page_has_links_to_broken_page_links_not_indexable": IssueMeta(
        "page_has_links_to_broken_page_links_not_indexable",
        "Liens vers pages cassées (non indexable) — détails",
        "Links",
        "warning",
    ),
    "broken_internal_links": IssueMeta(
        "broken_internal_links",
        "Liens internes cassés (toutes pages)",
        "Links",
        "warning",
    ),
    "page_has_no_outgoing_links": IssueMeta("page_has_no_outgoing_links", "Page sans liens sortants", "Links", "error"),
    "page_has_no_outgoing_links_indexable": IssueMeta(
        "page_has_no_outgoing_links_indexable", "Page sans liens sortants (indexable)", "Links", "error"
    ),
    "page_has_no_outgoing_links_not_indexable": IssueMeta(
        "page_has_no_outgoing_links_not_indexable", "Page sans liens sortants (non indexable)", "Links", "error"
    ),
    "page_has_links_to_redirect": IssueMeta(
        "page_has_links_to_redirect", "Page → lien vers redirection", "Links", "warning"
    ),
    "page_has_links_to_redirect_indexable": IssueMeta(
        "page_has_links_to_redirect_indexable",
        "Page → lien vers redirection (indexable)",
        "Links",
        "warning",
    ),
    "page_has_links_to_redirect_not_indexable": IssueMeta(
        "page_has_links_to_redirect_not_indexable",
        "Page → lien vers redirection (non indexable)",
        "Links",
        "notice",
    ),
    "page_has_links_to_redirect_links_indexable": IssueMeta(
        "page_has_links_to_redirect_links_indexable",
        "Liens vers redirections (indexable) — détails",
        "Links",
        "warning",
    ),
    "redirect_3xx_links": IssueMeta(
        "redirect_3xx_links",
        "Liens pointant vers des redirections 3XX",
        "Links",
        "warning",
    ),
    "links_to_404_page": IssueMeta("links_to_404_page", "Liens vers pages 404", "Links", "error"),
    "links_to_4xx_page": IssueMeta("links_to_4xx_page", "Liens vers pages 4XX", "Links", "error"),
    "page_has_nofollow_incoming_internal_links_only": IssueMeta(
        "page_has_nofollow_incoming_internal_links_only", "Liens entrants internes : nofollow uniquement", "Links", "notice"
    ),
    "page_has_nofollow_incoming_internal_links_only_indexable": IssueMeta(
        "page_has_nofollow_incoming_internal_links_only_indexable",
        "Liens entrants internes : nofollow uniquement (indexable)",
        "Links",
        "notice",
    ),
    "page_has_nofollow_incoming_internal_links_only_not_indexable": IssueMeta(
        "page_has_nofollow_incoming_internal_links_only_not_indexable",
        "Liens entrants internes : nofollow uniquement (non indexable)",
        "Links",
        "notice",
    ),
    "redirected_page_has_no_incoming_internal_links": IssueMeta(
        "redirected_page_has_no_incoming_internal_links",
        "Page redirigée sans lien interne entrant",
        "Links",
        "notice",
    ),
    "page_has_nofollow_and_dofollow_incoming_internal_links": IssueMeta(
        "page_has_nofollow_and_dofollow_incoming_internal_links",
        "Liens entrants internes : nofollow + dofollow",
        "Links",
        "notice",
    ),
    "page_has_nofollow_and_dofollow_incoming_internal_links_indexable": IssueMeta(
        "page_has_nofollow_and_dofollow_incoming_internal_links_indexable",
        "Liens entrants internes : nofollow + dofollow (indexable)",
        "Links",
        "notice",
    ),
    "page_has_nofollow_and_dofollow_incoming_internal_links_not_indexable": IssueMeta(
        "page_has_nofollow_and_dofollow_incoming_internal_links_not_indexable",
        "Liens entrants internes : nofollow + dofollow (non indexable)",
        "Links",
        "notice",
    ),
    "page_has_nofollow_outgoing_internal_links": IssueMeta(
        "page_has_nofollow_outgoing_internal_links", "Liens internes sortants en nofollow", "Links", "notice"
    ),
    "page_has_nofollow_outgoing_internal_links_indexable": IssueMeta(
        "page_has_nofollow_outgoing_internal_links_indexable",
        "Liens internes sortants en nofollow (indexable)",
        "Links",
        "notice",
    ),
    "page_has_nofollow_outgoing_internal_links_not_indexable": IssueMeta(
        "page_has_nofollow_outgoing_internal_links_not_indexable",
        "Liens internes sortants en nofollow (non indexable)",
        "Links",
        "notice",
    ),
    "page_has_only_one_dofollow_incoming_internal_link": IssueMeta(
        "page_has_only_one_dofollow_incoming_internal_link", "Un seul lien interne entrant dofollow", "Links", "notice"
    ),
    "page_has_only_one_dofollow_incoming_internal_link_links": IssueMeta(
        "page_has_only_one_dofollow_incoming_internal_link_links",
        "Un seul lien interne entrant dofollow — liens",
        "Links",
        "notice",
    ),
    "page_has_only_one_dofollow_incoming_internal_link_indexable": IssueMeta(
        "page_has_only_one_dofollow_incoming_internal_link_indexable",
        "Un seul lien interne entrant dofollow (indexable)",
        "Links",
        "notice",
    ),
    "page_has_only_one_dofollow_incoming_internal_link_not_indexable": IssueMeta(
        "page_has_only_one_dofollow_incoming_internal_link_not_indexable",
        "Un seul lien interne entrant dofollow (non indexable)",
        "Links",
        "notice",
    ),
    "links_with_no_anchor_text": IssueMeta(
        "links_with_no_anchor_text",
        "Liens sans texte d’ancrage",
        "Links",
        "warning",
        description="Liens <a> avec texte vide et sans attributs de label (title/aria-label).",
    ),
    "nofollow_external_links": IssueMeta(
        "nofollow_external_links",
        "Liens externes en nofollow",
        "Links",
        "notice",
        description="Liens externes avec rel=nofollow (source/target).",
    ),
    # Redirects
    "broken_redirect": IssueMeta("broken_redirect", "Redirection cassée", "Redirects", "error"),
    "redirect_chain_too_long": IssueMeta("redirect_chain_too_long", "Chaîne de redirection trop longue", "Redirects", "warning"),
    "redirect_loop": IssueMeta("redirect_loop", "Boucle de redirection", "Redirects", "error"),
    "redirect_3xx": IssueMeta("redirect_3xx", "Redirection 3XX", "Redirects", "warning"),
    "redirect_302": IssueMeta("redirect_302", "Redirection 302", "Redirects", "notice"),
    "https_to_http_redirect": IssueMeta("https_to_http_redirect", "Redirection HTTPS → HTTP", "Redirects", "warning"),
    "http_to_https_redirect": IssueMeta("http_to_https_redirect", "Redirection HTTP → HTTPS", "Redirects", "notice"),
    "meta_refresh_redirect": IssueMeta("meta_refresh_redirect", "Meta refresh redirect", "Redirects", "warning"),
    "redirect_chain": IssueMeta("redirect_chain", "Chaîne de redirection", "Redirects", "notice"),
    # Content (on-page)
    "multiple_meta_description_tags": IssueMeta(
        "multiple_meta_description_tags", "Plusieurs meta descriptions", "Content", "warning"
    ),
    "multiple_title_tags": IssueMeta("multiple_title_tags", "Plusieurs balises title", "Content", "warning"),
    "missing_title": IssueMeta("missing_title", "Balise title manquante / vide", "Content", "warning"),
    "missing_h1": IssueMeta("missing_h1", "H1 manquant / vide", "Content", "warning"),
    "missing_h1_indexable": IssueMeta("missing_h1_indexable", "H1 manquant / vide", "Content", "warning"),
    "missing_h1_not_indexable": IssueMeta("missing_h1_not_indexable", "H1 manquant / vide", "Content", "notice"),
    "low_word_count": IssueMeta("low_word_count", "Faible nombre de mots", "Content", "notice"),
    "missing_meta_description": IssueMeta("missing_meta_description", "Meta description manquante / vide", "Content", "warning"),
    "meta_description_too_long": IssueMeta("meta_description_too_long", "Meta description trop longue", "Content", "warning"),
    "meta_description_too_long_indexable": IssueMeta(
        "meta_description_too_long_indexable", "Meta description trop longue (indexable)", "Content", "warning"
    ),
    "meta_description_too_long_not_indexable": IssueMeta(
        "meta_description_too_long_not_indexable",
        "Meta description trop longue (non indexable)",
        "Content",
        "notice",
    ),
    "meta_description_too_short_indexable": IssueMeta(
        "meta_description_too_short_indexable", "Meta description trop courte", "Content", "warning"
    ),
    "meta_description_too_short_not_indexable": IssueMeta(
        "meta_description_too_short_not_indexable", "Meta description trop courte", "Content", "notice"
    ),
    "meta_description_too_short": IssueMeta(
        "meta_description_too_short", "Meta description trop courte", "Content", "warning"
    ),
    "page_and_serp_titles_do_not_match": IssueMeta(
        "page_and_serp_titles_do_not_match", "Titre de la page ≠ titre en SERP", "Content", "notice"
    ),
    "title_too_long": IssueMeta("title_too_long", "Balise title trop longue", "Content", "notice"),
    "title_too_long_indexable": IssueMeta("title_too_long_indexable", "Balise title trop longue (indexable)", "Content", "warning"),
    "title_too_long_not_indexable": IssueMeta(
        "title_too_long_not_indexable", "Balise title trop longue (non indexable)", "Content", "notice"
    ),
    "title_too_short": IssueMeta("title_too_short", "Balise title trop courte", "Content", "notice"),
    "multiple_h1": IssueMeta("multiple_h1", "Plusieurs H1", "Content", "warning"),
    # Social tags
    "open_graph_tags_incomplete": IssueMeta("open_graph_tags_incomplete", "Open Graph incomplet", "Social tags", "warning"),
    "open_graph_tags_missing": IssueMeta("open_graph_tags_missing", "Open Graph manquant", "Social tags", "notice"),
    "open_graph_url_not_matching_canonical": IssueMeta(
        "open_graph_url_not_matching_canonical", "OG URL ≠ canonical", "Social tags", "notice"
    ),
    "twitter_card_incomplete": IssueMeta("twitter_card_incomplete", "Twitter card incomplète", "Social tags", "notice"),
    "twitter_card_missing": IssueMeta("twitter_card_missing", "Twitter card manquante", "Social tags", "notice"),
    # Duplicates
    "duplicate_pages_without_canonical": IssueMeta(
        "duplicate_pages_without_canonical", "Pages dupliquées sans canonical", "Duplicates", "warning"
    ),
    "duplicate_titles": IssueMeta("duplicate_titles", "Titres dupliqués", "Duplicates", "warning"),
    "duplicate_meta_descriptions": IssueMeta("duplicate_meta_descriptions", "Meta descriptions dupliquées", "Duplicates", "warning"),
    # Localization / hreflang / lang
    "hreflang_annotation_invalid": IssueMeta("hreflang_annotation_invalid", "Hreflang invalide", "Localization", "warning"),
    "hreflang_to_non_canonical": IssueMeta("hreflang_to_non_canonical", "Hreflang vers non-canonical", "Localization", "warning"),
    "hreflang_to_redirect_or_broken_page": IssueMeta(
        "hreflang_to_redirect_or_broken_page", "Hreflang vers redirect / URL cassée", "Localization", "warning"
    ),
    "hreflang_to_redirect_or_broken_page_links": IssueMeta(
        "hreflang_to_redirect_or_broken_page_links",
        "Hreflang vers redirect / URL cassée — liens",
        "Localization",
        "warning",
    ),
    "more_than_one_page_for_same_language_in_hreflang": IssueMeta(
        "more_than_one_page_for_same_language_in_hreflang",
        "Plusieurs pages pour une même langue dans hreflang",
        "Localization",
        "error",
    ),
    "html_lang_attribute_invalid": IssueMeta("html_lang_attribute_invalid", "Attribut HTML lang invalide", "Localization", "notice"),
    "missing_reciprocal_hreflang": IssueMeta(
        "missing_reciprocal_hreflang", "Hreflang réciproque manquant", "Localization", "error"
    ),
    "hreflang_defined_but_html_lang_missing": IssueMeta(
        "hreflang_defined_but_html_lang_missing", "Hreflang défini mais HTML lang manquant", "Localization", "notice"
    ),
    "html_lang_attribute_missing": IssueMeta("html_lang_attribute_missing", "Attribut HTML lang manquant", "Localization", "notice"),
    "page_referenced_for_more_than_one_language_in_hreflang": IssueMeta(
        "page_referenced_for_more_than_one_language_in_hreflang",
        "Page référencée pour plusieurs langues",
        "Localization",
        "warning",
    ),
    "x_default_hreflang_missing": IssueMeta("x_default_hreflang_missing", "Hreflang x-default manquant", "Localization", "notice"),
    # Asset optimization
    "css_not_minified": IssueMeta("css_not_minified", "CSS non minifié", "CSS", "warning"),
    "javascript_not_minified": IssueMeta("javascript_not_minified", "JavaScript non minifié", "JavaScript", "warning"),
    "javascript_broken": IssueMeta("javascript_broken", "JavaScript cassé", "JavaScript", "error"),
    "javascript_broken_links": IssueMeta("javascript_broken_links", "JavaScript cassé — liens", "JavaScript", "error"),
    "page_has_broken_javascript": IssueMeta("page_has_broken_javascript", "Page avec JavaScript cassé", "JavaScript", "error"),
    "page_has_broken_javascript_links": IssueMeta(
        "page_has_broken_javascript_links", "Page avec JavaScript cassé — liens", "JavaScript", "error"
    ),
    "unminified_javascript_and_css_files": IssueMeta(
        "unminified_javascript_and_css_files", "JS/CSS non minifiés", "JavaScript", "warning"
    ),
    "broken_internal_javascript_and_css_files": IssueMeta(
        "broken_internal_javascript_and_css_files", "JS/CSS internes cassés", "JavaScript", "error"
    ),
    "image_file_size_too_large_links": IssueMeta(
        "image_file_size_too_large_links", "Images trop lourdes — liens", "Images", "error"
    ),
    "image_file_size_too_large": IssueMeta(
        "image_file_size_too_large",
        "Image trop lourde",
        "Images",
        "error",
    ),
    "page_has_broken_image": IssueMeta(
        "page_has_broken_image",
        "Page avec image cassée",
        "Images",
        "error",
    ),
    "image_broken": IssueMeta(
        "image_broken",
        "Image cassée",
        "Images",
        "error",
    ),
    # Assets (HTTPS links to HTTP)
    "https_page_links_to_http_image": IssueMeta(
        "https_page_links_to_http_image", "Page HTTPS → image HTTP", "Images", "warning"
    ),
    "https_page_links_to_http_javascript": IssueMeta(
        "https_page_links_to_http_javascript", "Page HTTPS → JavaScript HTTP", "JavaScript", "warning"
    ),
    "https_page_links_to_http_css": IssueMeta("https_page_links_to_http_css", "Page HTTPS → CSS HTTP", "CSS", "warning"),
    # Sitemaps
    "sitemap_3xx_redirect": IssueMeta("sitemap_3xx_redirect", "Sitemap : URL en 3XX", "Sitemaps", "error"),
    "sitemap_4xx_page": IssueMeta("sitemap_4xx_page", "Sitemap : URL en 4XX", "Sitemaps", "error"),
    "sitemap_5xx_page": IssueMeta("sitemap_5xx_page", "Sitemap : URL en 5XX", "Sitemaps", "error"),
    "sitemap_noindex_page": IssueMeta("sitemap_noindex_page", "Sitemap : URL noindex", "Sitemaps", "notice"),
    "sitemap_non_canonical_page": IssueMeta(
        "sitemap_non_canonical_page", "Sitemap : URL non-canonique", "Sitemaps", "notice"
    ),
    "sitemap_page_timed_out": IssueMeta("sitemap_page_timed_out", "Sitemap : URL timed out", "Sitemaps", "warning"),
    "page_in_multiple_sitemaps": IssueMeta("page_in_multiple_sitemaps", "Page dans plusieurs sitemaps", "Sitemaps", "notice"),

    "indexable_page_not_in_sitemap": IssueMeta("indexable_page_not_in_sitemap", "Page indexable absente du sitemap", "Sitemaps", "notice"),
    "pages_to_submit_to_indexnow": IssueMeta("pages_to_submit_to_indexnow", "Pages à soumettre à IndexNow", "Other", "notice"),
    "pages_have_high_ai_content_levels": IssueMeta(
        "pages_have_high_ai_content_levels",
        "Pages have high AI content levels",
        "Content",
        "notice",
    ),
}


_URL_RE = re.compile(r"https?://[^\s)>,]+", re.IGNORECASE)


def parse_timestamp(value: str) -> datetime | None:
    value = (value or "").strip()
    if not value:
        return None
    for fmt in ("%Y%m%d-%H%M%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def format_timestamp(value: str) -> str:
    dt = parse_timestamp(value)
    if not dt:
        return value
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        import json

        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def list_project_slugs(runs_dir: Path) -> list[str]:
    if not runs_dir.exists():
        return []
    slugs: list[str] = []
    for p in runs_dir.iterdir():
        if not p.is_dir():
            continue
        if p.name.startswith("_"):
            continue
        slugs.append(p.name)
    slugs.sort()
    return slugs


def list_project_crawls(runs_dir: Path, slug: str) -> list[str]:
    base = runs_dir / slug
    if not base.exists():
        return []
    items: list[str] = []
    for p in base.iterdir():
        if not p.is_dir():
            continue
        # expected: YYYYMMDD-HHMMSS
        if re.fullmatch(r"\d{8}-\d{6}", p.name):
            items.append(p.name)
    items.sort()
    return items


def _select_crawl_timestamp_with_report(
    runs_dir: Path, slug: str, requested: str | None
) -> tuple[str | None, bool]:
    """
    Returns (timestamp, used_fallback).

    We keep timestamp directories even while a crawl is running; during that window `audit/report.json`
    does not exist yet. Without this guard, the project can "disappear" from the UI (404 / missing row)
    until the report is written.
    """
    crawls = list_project_crawls(runs_dir, slug)
    if not crawls:
        return None, False

    if requested and requested in crawls:
        if load_report_json(runs_dir, slug, requested):
            return requested, False
        # Requested crawl exists but has no report yet (or invalid JSON): fallback to latest completed.
        for ts in reversed(crawls):
            if load_report_json(runs_dir, slug, ts):
                return ts, True
        # No report anywhere (first crawl still running / failed early).
        return requested, True

    # Default: latest completed crawl (if any), otherwise latest directory.
    for ts in reversed(crawls):
        if load_report_json(runs_dir, slug, ts):
            return ts, False
    return crawls[-1], False


def load_run_json(runs_dir: Path, slug: str, timestamp: str) -> dict[str, Any]:
    run_path = runs_dir / slug / timestamp / "run.json"
    data = _read_json(run_path)
    return data or {}


def load_report_json(runs_dir: Path, slug: str, timestamp: str) -> dict[str, Any] | None:
    report_path = runs_dir / slug / timestamp / "audit" / "report.json"
    return _read_json(report_path)


def report_path(runs_dir: Path, slug: str, timestamp: str) -> Path:
    return runs_dir / slug / timestamp / "audit" / "report.json"


def report_md_path(runs_dir: Path, slug: str, timestamp: str) -> Path:
    return runs_dir / slug / timestamp / "audit" / "report.md"


def issue_meta(key: str) -> IssueMeta:
    if key in ISSUE_CATALOG:
        return ISSUE_CATALOG[key]

    label = key.replace("_", " ").strip()
    label = label.replace("http", "HTTP").replace("https", "HTTPS")

    # Basic heuristics for unseen keys.
    severity: Severity = "notice"
    if any(tok in key for tok in ("404", "4xx", "500", "5xx", "timed_out", "broken", "loop")) or key.startswith("http_"):
        severity = "error"
    elif any(tok in key for tok in ("missing", "invalid", "too_long", "too_short", "nofollow", "noindex", "redirect", "duplicate")):
        severity = "warning"

    category = "Other"
    if any(tok in key for tok in ("canonical", "noindex", "nofollow")):
        category = "Indexability"
    elif any(tok in key for tok in ("redirect", "3xx", "302")):
        category = "Redirects"
    elif any(tok in key for tok in ("title", "meta_description", "h1", "word_count")):
        category = "Content"
    elif any(tok in key for tok in ("hreflang", "lang")):
        category = "Localization"
    elif key.startswith("sitemap") or "sitemap" in key:
        category = "Sitemaps"
    elif any(tok in key for tok in ("image", "img")):
        category = "Images"
    elif any(tok in key for tok in ("javascript", "script", "js")):
        category = "JavaScript"
    elif "css" in key:
        category = "CSS"
    elif "link" in key or "orphan" in key:
        category = "Links"

    return IssueMeta(key=key, label=label, category=category, severity=severity)


def issue_count(issue_block: Any) -> int:
    if isinstance(issue_block, dict) and isinstance(issue_block.get("count"), int):
        return int(issue_block.get("count") or 0)
    return 0


def issue_examples(issue_block: Any, limit: int = 20) -> list[str]:
    if not isinstance(issue_block, dict):
        return []
    examples = issue_block.get("examples")
    if isinstance(examples, list):
        out: list[str] = []
        for ex in examples[: max(0, limit)]:
            if isinstance(ex, str):
                out.append(ex)
            elif isinstance(ex, dict):
                # Try to show something readable while keeping structure.
                src = ex.get("source_url") or ex.get("source") or ex.get("url")
                tgt = ex.get("target_url") or ex.get("target")
                if isinstance(src, str) and isinstance(tgt, str) and src and tgt:
                    out.append(f"{src} -> {tgt}")
                else:
                    out.append(str(ex))
            else:
                out.append(str(ex))
        return out

    top = issue_block.get("top")
    if isinstance(top, list):
        out = []
        for item in top[: max(0, limit)]:
            if (
                isinstance(item, (list, tuple))
                and len(item) == 2
                and isinstance(item[0], str)
                and isinstance(item[1], int)
            ):
                out.append(f"{item[0]} ({item[1]})")
            else:
                out.append(str(item))
        return out

    return []


def extract_impacted_pages(issue_key: str, issue_block: Any, limit: int = 500) -> set[str]:
    if not isinstance(issue_block, dict):
        return set()

    impacted: set[str] = set()

    # Prefer the "examples" list (it usually contains URLs).
    examples = issue_block.get("examples")
    if isinstance(examples, list):
        for ex in examples[: max(0, limit)]:
            if isinstance(ex, str):
                # Common format: "source -> target"
                if "->" in ex:
                    left = ex.split("->", 1)[0].strip()
                    if left.startswith("http://") or left.startswith("https://"):
                        impacted.add(left)
                        continue
                urls = _URL_RE.findall(ex)
                if urls:
                    impacted.add(urls[0])
                continue

            if isinstance(ex, dict):
                src = ex.get("source_url") or ex.get("source") or ex.get("url")
                if isinstance(src, str) and src.startswith(("http://", "https://")):
                    impacted.add(src)
                    continue
                # Otherwise, fallback to first URL-like value.
                for v in ex.values():
                    if isinstance(v, str) and v.startswith(("http://", "https://")):
                        impacted.add(v)
                        break
                continue

    # Some issues can be dict maps: {url: ...}
    for k in issue_block.keys():
        if isinstance(k, str) and k.startswith(("http://", "https://")):
            impacted.add(k)

    return impacted


def summarize_report(report: dict[str, Any], previous: dict[str, Any] | None = None) -> dict[str, Any]:
    meta = report.get("meta") if isinstance(report.get("meta"), dict) else {}
    issues = report.get("issues") if isinstance(report.get("issues"), dict) else {}
    pages = report.get("pages") if isinstance(report.get("pages"), list) else []
    resources = report.get("resources") if isinstance(report.get("resources"), list) else []
    system_fetches = report.get("system_fetches") if isinstance(report.get("system_fetches"), list) else []

    pages_crawled = meta.get("pages_crawled") if isinstance(meta.get("pages_crawled"), int) else len(pages)
    resources_crawled = len([r for r in resources if isinstance(r, dict) and str(r.get("type") or "").strip().lower() in {"image", "javascript", "css"}])
    system_urls_crawled = len([r for r in system_fetches if isinstance(r, dict)])
    # Ahrefs-like: health score is based on internal URLs (not assets/system fetches).
    internal_urls_crawled = int(pages_crawled)
    urls_crawled = int(pages_crawled) + int(resources_crawled) + int(system_urls_crawled)
    urls_crawled_distribution = int(pages_crawled) + int(resources_crawled)

    # Ahrefs-like: "Crawl status of links found" (discovered vs crawled)
    urls_discovered = meta.get("urls_discovered") if isinstance(meta.get("urls_discovered"), int) else int(pages_crawled)
    urls_discovered = max(int(urls_discovered), int(pages_crawled))
    urls_uncrawled = meta.get("urls_uncrawled") if isinstance(meta.get("urls_uncrawled"), int) else max(0, urls_discovered - int(pages_crawled))
    urls_uncrawled = max(0, int(urls_uncrawled))

    prev_issues = None
    if previous and isinstance(previous.get("issues"), dict):
        prev_meta = previous.get("meta") if isinstance(previous.get("meta"), dict) else {}
        if _comparable_meta(meta, prev_meta):
            prev_issues = previous.get("issues")

    issue_rows: list[dict[str, Any]] = []
    by_severity: dict[str, int] = {"error": 0, "warning": 0, "notice": 0}
    by_category: dict[str, int] = {}

    internal_error_urls: set[str] = set()

    # Ground truth for crawl failures: status>=400 or request error.
    for p in pages:
        if not isinstance(p, dict):
            continue
        url = p.get("url")
        if not isinstance(url, str) or not url:
            continue
        status = p.get("status_code")
        err = p.get("error")
        if isinstance(status, int) and status >= 400:
            internal_error_urls.add(url)
        if isinstance(err, str) and err.strip():
            internal_error_urls.add(url)

    # Hide aggregate keys when split variants exist (avoids confusing double-counts).
    skip_keys: set[str] = set()
    if any(k in issues for k in ("title_too_long_indexable", "title_too_long_not_indexable")):
        skip_keys.add("title_too_long")
    if any(k in issues for k in ("meta_description_too_long_indexable", "meta_description_too_long_not_indexable")):
        skip_keys.add("meta_description_too_long")
    if any(k in issues for k in ("meta_description_too_short_indexable", "meta_description_too_short_not_indexable")):
        skip_keys.add("meta_description_too_short")
    if any(
        k in issues
        for k in ("page_has_only_one_dofollow_incoming_internal_link_indexable", "page_has_only_one_dofollow_incoming_internal_link_not_indexable")
    ):
        skip_keys.add("page_has_only_one_dofollow_incoming_internal_link")

    allowed_keys = _allowed_issue_keys_for_profile(meta.get("profile"))
    ahrefs_profile = str(meta.get("profile") or "").strip().lower() == "ahrefs"

    def _is_link_export_issue_key(issue_key: str) -> bool:
        exports = _parity_mapping_info().get("link_exports_by_tool", {}).get("ahrefs") or set()
        return str(issue_key or "") in exports

    for key, block in issues.items():
        if key in NON_ISSUE_KEYS:
            continue
        if key in skip_keys:
            continue
        if allowed_keys is not None and str(key) not in allowed_keys:
            continue
        # Ahrefs "All issues" table does not list per-link exports as separate issues.
        if ahrefs_profile and _is_link_export_issue_key(str(key)):
            continue
        count = issue_count(block)
        if count <= 0:
            continue
        meta_info = issue_meta(str(key))
        change = None
        if prev_issues is not None and isinstance(prev_issues.get(key), dict):
            change = count - issue_count(prev_issues.get(key))
        elif prev_issues is not None:
            change = count

        by_severity[meta_info.severity] = by_severity.get(meta_info.severity, 0) + count
        by_category[meta_info.category] = by_category.get(meta_info.category, 0) + count

        if meta_info.severity == "error" and str(key) != "bad_status":
            # `bad_status` is a legacy, catch-all counter; rely on per-page status_code/error instead.
            internal_error_urls |= extract_impacted_pages(str(key), block)

        issue_rows.append(
            {
                "key": str(key),
                "label": meta_info.label,
                "category": meta_info.category,
                "severity": meta_info.severity,
                "count": count,
                "change": change,
            }
        )

    urls_with_errors = len(internal_error_urls)
    urls_without_errors = max(0, int(internal_urls_crawled) - urls_with_errors) if internal_urls_crawled else 0
    health_score = (
        int(round((urls_without_errors / internal_urls_crawled) * 100)) if internal_urls_crawled else 0
    )

    issue_rows.sort(key=lambda r: (SEVERITY_ORDER.get(r["severity"], 99), -int(r["count"]), r["label"]))

    top_issues = issue_rows[:10]

    issues_total = int(sum(by_severity.values()))

    pagespeed_meta = meta.get("pagespeed") if isinstance(meta.get("pagespeed"), dict) else {}
    pagespeed_summary = {
        "enabled": bool(pagespeed_meta.get("enabled") or False),
        "strategy": str(pagespeed_meta.get("strategy") or "") if pagespeed_meta else "",
        "requested": int(pagespeed_meta.get("requested") or 0) if isinstance(pagespeed_meta, dict) else 0,
        "tested": int(pagespeed_meta.get("tested") or 0) if isinstance(pagespeed_meta, dict) else 0,
        "errors": int(pagespeed_meta.get("errors") or 0) if isinstance(pagespeed_meta, dict) else 0,
        "duration_s": float(pagespeed_meta.get("duration_s") or 0.0) if isinstance(pagespeed_meta, dict) else 0.0,
        "reason": str(pagespeed_meta.get("reason") or "") if pagespeed_meta else "",
    }

    cwv_meta = meta.get("cwv") if isinstance(meta.get("cwv"), dict) else None
    cwv_summary: dict[str, Any] | None = None
    if isinstance(cwv_meta, dict):
        metrics = cwv_meta.get("metrics") if isinstance(cwv_meta.get("metrics"), dict) else {}

        def metric_counts(metric: str) -> dict[str, int]:
            node = metrics.get(metric) if isinstance(metrics.get(metric), dict) else {}
            counts = node.get("counts") if isinstance(node.get("counts"), dict) else {}
            return {
                "good": int(counts.get("good") or 0),
                "ni": int(counts.get("ni") or 0),
                "poor": int(counts.get("poor") or 0),
                "na": int(counts.get("na") or 0),
            }

        score: int | None = int(cwv_meta.get("score")) if isinstance(cwv_meta.get("score"), int) else None
        page_status: dict[str, int] | None = (
            cwv_meta.get("page_status") if isinstance(cwv_meta.get("page_status"), dict) else None
        )

        # Backward compatibility: older reports have `meta.cwv.metrics` but not `score` / `page_status`.
        if score is None or page_status is None:
            def _metric_value(ps: dict[str, Any], metric: str) -> float | int | None:
                fm = ps.get("field_metrics")
                if isinstance(fm, dict):
                    node = fm.get(metric)
                    if isinstance(node, dict) and "p75" in node:
                        v = node.get("p75")
                        if isinstance(v, bool):
                            v = None
                        if isinstance(v, (int, float)):
                            return v
                lm = ps.get("lab_metrics")
                if isinstance(lm, dict):
                    node = lm.get(metric)
                    if isinstance(node, dict) and "value" in node:
                        v = node.get("value")
                        if isinstance(v, bool):
                            v = None
                        if isinstance(v, (int, float)):
                            return v
                return None

            def _cat(metric: str, value: float | int | None) -> str:
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
                if metric == "tbt":
                    if v <= 200:
                        return "good"
                    if v <= 600:
                        return "ni"
                    return "poor"
                return "na"

            computed_status: dict[str, int] = {"good": 0, "ni": 0, "poor": 0, "na": 0}
            for row in pages:
                if not isinstance(row, dict):
                    continue
                ps = row.get("pagespeed")
                if not isinstance(ps, dict) or ps.get("error"):
                    continue
                cats = [_cat("lcp", _metric_value(ps, "lcp")), _cat("tbt", _metric_value(ps, "tbt")), _cat("cls", _metric_value(ps, "cls"))]
                if "poor" in cats:
                    computed_status["poor"] += 1
                elif "na" in cats:
                    computed_status["na"] += 1
                elif "ni" in cats:
                    computed_status["ni"] += 1
                else:
                    computed_status["good"] += 1

            total = sum(computed_status.values())
            computed_score = int(round((computed_status["good"] / total) * 100)) if total else 0
            page_status = computed_status if page_status is None else page_status
            score = computed_score if score is None else score

        cwv_summary = {
            "score": score,
            "tested_pages": int(cwv_meta.get("tested_pages") or 0),
            "strategy": str(cwv_meta.get("strategy") or ""),
            "page_status": page_status,
            "metrics": {
                "lcp": metric_counts("lcp"),
                "tbt": metric_counts("tbt"),
                "cls": metric_counts("cls"),
                "inp": metric_counts("inp"),
            },
        }

    gsc_meta = meta.get("gsc_api") if isinstance(meta.get("gsc_api"), dict) else {}
    gsc_summary = {
        "enabled": bool(gsc_meta.get("enabled") or False),
        "ok": bool(gsc_meta.get("ok") or False),
        "reason": str(gsc_meta.get("reason") or ""),
        "property": str(gsc_meta.get("property") or ""),
        "days": int(gsc_meta.get("days") or 0) if isinstance(gsc_meta, dict) else 0,
        "search_type": str(gsc_meta.get("search_type") or ""),
        "start_date": str(gsc_meta.get("start_date") or ""),
        "end_date": str(gsc_meta.get("end_date") or ""),
        "queries_csv": str(gsc_meta.get("queries_csv") or ""),
        "pages_csv": str(gsc_meta.get("pages_csv") or ""),
        "daily_csv": str(gsc_meta.get("daily_csv") or ""),
        "daily": [],
        "url_inspection": {},
        "totals": {},
    }
    if gsc_summary["enabled"] and gsc_summary["ok"]:
        queries = gsc_meta.get("queries") if isinstance(gsc_meta.get("queries"), dict) else {}
        pages_node = gsc_meta.get("pages") if isinstance(gsc_meta.get("pages"), dict) else {}
        gsc_summary["totals"] = {
            "clicks": int(queries.get("total_clicks") or 0),
            "impressions": int(queries.get("total_impressions") or 0),
            "avg_ctr": float(queries.get("avg_ctr") or 0.0),
            "avg_position": float(queries.get("avg_position") or 0.0),
            "pages_clicks": int(pages_node.get("total_clicks") or 0),
            "pages_impressions": int(pages_node.get("total_impressions") or 0),
        }
        daily_rows = gsc_meta.get("daily") if isinstance(gsc_meta.get("daily"), list) else []
        normalized_daily: list[dict[str, Any]] = []
        for r in daily_rows[:400]:
            if not isinstance(r, dict):
                continue
            date = str(r.get("date") or "").strip()
            if not date:
                continue
            try:
                clicks = int(r.get("clicks") or 0)
            except Exception:
                clicks = 0
            try:
                impressions = int(r.get("impressions") or 0)
            except Exception:
                impressions = 0
            try:
                ctr = float(r.get("ctr") or 0.0)
            except Exception:
                ctr = 0.0
            try:
                position = float(r.get("position") or 0.0)
            except Exception:
                position = 0.0
            normalized_daily.append(
                {
                    "date": date,
                    "clicks": clicks,
                    "impressions": impressions,
                    "ctr": ctr,
                    "position": position,
                }
            )
        gsc_summary["daily"] = normalized_daily
        insp = gsc_meta.get("url_inspection") if isinstance(gsc_meta.get("url_inspection"), dict) else {}
        if isinstance(insp, dict) and insp.get("enabled"):
            gsc_summary["url_inspection"] = {
                "enabled": bool(insp.get("enabled") or False),
                "ok": bool(insp.get("ok") or False),
                "reason": str(insp.get("reason") or ""),
                "checked": int(insp.get("checked") or 0),
                "errors": int(insp.get("errors") or 0),
                "warnings": int(insp.get("warnings") or 0),
                "notices": int(insp.get("notices") or 0),
                "json": str(insp.get("json") or ""),
            }

    bing_meta = meta.get("bing") if isinstance(meta.get("bing"), dict) else {}
    bing_summary = {
        "enabled": bool(bing_meta.get("enabled") or False),
        "ok": bool(bing_meta.get("ok") or False),
        "reason": str(bing_meta.get("reason") or ""),
        "site_url": str(bing_meta.get("site_url") or ""),
        "days": int(bing_meta.get("days") or 0) if isinstance(bing_meta, dict) else 0,
        "start_date": str(bing_meta.get("start_date") or ""),
        "end_date": str(bing_meta.get("end_date") or ""),
        "queries_csv": str(bing_meta.get("queries_csv") or ""),
        "pages_csv": str(bing_meta.get("pages_csv") or ""),
        "daily_json": str(bing_meta.get("daily_json") or ""),
        "daily": [],
        "totals": {},
        "crawl_issues": {},
        "blocked_urls": {},
        "sitemaps": {},
        "url_info": {},
    }
    if bing_summary["enabled"] and bing_summary["ok"]:
        queries = bing_meta.get("queries") if isinstance(bing_meta.get("queries"), dict) else {}
        pages_node = bing_meta.get("pages") if isinstance(bing_meta.get("pages"), dict) else {}
        bing_summary["totals"] = {
            "clicks": int(queries.get("total_clicks") or 0),
            "impressions": int(queries.get("total_impressions") or 0),
            "avg_ctr": float(queries.get("avg_ctr") or 0.0),
            "avg_position": float(queries.get("avg_position") or 0.0),
            "pages_clicks": int(pages_node.get("total_clicks") or 0),
            "pages_impressions": int(pages_node.get("total_impressions") or 0),
        }
        daily_rows = bing_meta.get("daily") if isinstance(bing_meta.get("daily"), list) else []
        normalized_daily: list[dict[str, Any]] = []
        for r in daily_rows[:400]:
            if not isinstance(r, dict):
                continue
            date = str(r.get("date") or "").strip()
            if not date:
                continue
            try:
                clicks = int(r.get("clicks") or 0)
            except Exception:
                clicks = 0
            try:
                impressions = int(r.get("impressions") or 0)
            except Exception:
                impressions = 0
            try:
                ctr = float(r.get("ctr") or 0.0)
            except Exception:
                ctr = 0.0
            try:
                position = float(r.get("position") or 0.0)
            except Exception:
                position = 0.0
            normalized_daily.append(
                {
                    "date": date,
                    "clicks": clicks,
                    "impressions": impressions,
                    "ctr": ctr,
                    "position": position,
                }
            )
        bing_summary["daily"] = normalized_daily
        ci = bing_meta.get("crawl_issues") if isinstance(bing_meta.get("crawl_issues"), dict) else {}
        bu = bing_meta.get("blocked_urls") if isinstance(bing_meta.get("blocked_urls"), dict) else {}
        sm = bing_meta.get("sitemaps") if isinstance(bing_meta.get("sitemaps"), dict) else {}
        ui = bing_meta.get("url_info") if isinstance(bing_meta.get("url_info"), dict) else {}
        bing_summary["crawl_issues"] = {"rows": int(ci.get("rows") or 0)}
        bing_summary["blocked_urls"] = {"rows": int(bu.get("rows") or 0)}
        bing_summary["sitemaps"] = {"rows": int(sm.get("rows") or 0)}
        bing_summary["url_info"] = {"checked": int(ui.get("checked") or 0)}

    return {
        "base_url": str(meta.get("base_url") or ""),
        "pages_crawled": int(pages_crawled),
        "resources_crawled": int(resources_crawled),
        "system_urls_crawled": int(system_urls_crawled),
        "urls_crawled": int(urls_crawled),
        "urls_crawled_distribution": int(urls_crawled_distribution),
        "urls_discovered": int(urls_discovered),
        "urls_uncrawled": int(urls_uncrawled),
        "health_score": int(max(0, min(100, health_score))),
        "urls_with_errors": int(urls_with_errors),
        "urls_without_errors": int(urls_without_errors),
        "issues_total": int(issues_total),
        "issues_distribution": by_severity,
        "issues_by_category": by_category,
        "issues": issue_rows,
        "top_issues": top_issues,
        "pagespeed": pagespeed_summary,
        "cwv": cwv_summary,
        "gsc": gsc_summary,
        "bing": bing_summary,
    }


def project_latest_summary(runs_dir: Path, slug: str) -> dict[str, Any] | None:
    ts, _ = _select_crawl_timestamp_with_report(runs_dir, slug, requested=None)
    if not ts:
        return None

    run = load_run_json(runs_dir, slug, ts)
    report = load_report_json(runs_dir, slug, ts)
    if report:
        summary = summarize_report(report)
        site_name = str(run.get("site_name") or slug)
        base_url = str(run.get("base_url") or summary.get("base_url") or "")
        return {
            "slug": slug,
            "site_name": site_name,
            "base_url": base_url,
            "timestamp": ts,
            "timestamp_label": format_timestamp(ts),
            "pages_crawled": summary["pages_crawled"],
            "urls_crawled": summary.get("urls_crawled", summary["pages_crawled"]),
            "health_score": summary["health_score"],
            "urls_with_errors": summary["urls_with_errors"],
            "issues_distribution": summary["issues_distribution"],
        }

    # No report yet: still return a row so the project does not "disappear".
    site_name = str(run.get("site_name") or slug)
    base_url = str(run.get("base_url") or "")
    return {
        "slug": slug,
        "site_name": site_name,
        "base_url": base_url,
        "timestamp": ts,
        "timestamp_label": format_timestamp(ts),
        "pages_crawled": 0,
        "urls_crawled": 0,
        "health_score": 0,
        "urls_with_errors": 0,
        "issues_distribution": {"error": 0, "warning": 0, "notice": 0},
    }


def project_overview(runs_dir: Path, slug: str, timestamp: str | None, compare_to: str | None) -> dict[str, Any] | None:
    crawls = list_project_crawls(runs_dir, slug)
    if not crawls:
        return None

    requested_ts = timestamp if timestamp and timestamp in crawls else crawls[-1]
    current_ts, used_fallback = _select_crawl_timestamp_with_report(runs_dir, slug, requested=requested_ts)
    if not current_ts:
        return None

    compare_ts: str | None = None
    if compare_to and compare_to in crawls and compare_to != current_ts:
        compare_ts = compare_to
    elif len(crawls) >= 2:
        # default compare with previous crawl
        idx = crawls.index(current_ts)
        if idx > 0:
            compare_ts = crawls[idx - 1]

    current_report = load_report_json(runs_dir, slug, current_ts)
    if not current_report:
        return None

    previous_report = load_report_json(runs_dir, slug, compare_ts) if compare_ts else None

    run = load_run_json(runs_dir, slug, current_ts)

    summary = summarize_report(current_report, previous=previous_report)
    previous_summary = summarize_report(previous_report) if previous_report else None

    history: list[dict[str, Any]] = []
    for ts in crawls[-12:]:
        r = load_report_json(runs_dir, slug, ts)
        if not r:
            continue
        s = summarize_report(r)
        history.append(
            {
                "timestamp": ts,
                "label": format_timestamp(ts),
                "health_score": s["health_score"],
                "pages_crawled": s["pages_crawled"],
                "urls_crawled": s.get("urls_crawled", s["pages_crawled"]),
                "urls_with_errors": s["urls_with_errors"],
            }
        )

    site_name = str(run.get("site_name") or slug)
    base_url = str(run.get("base_url") or summary.get("base_url") or "")

    return {
        "slug": slug,
        "site_name": site_name,
        "base_url": base_url,
        "crawls": list(reversed(crawls)),
        "current": {
            "timestamp": current_ts,
            "label": format_timestamp(current_ts),
            "dir": str((runs_dir / slug / current_ts).resolve()),
            "report_json": str(report_path(runs_dir, slug, current_ts)),
            "report_md": str(report_md_path(runs_dir, slug, current_ts)),
            "summary": summary,
            "requested_timestamp": requested_ts,
            "used_fallback": bool(used_fallback),
        },
        "compare": {
            "timestamp": compare_ts,
            "label": format_timestamp(compare_ts) if compare_ts else None,
            "summary": previous_summary,
        }
        if compare_ts
        else None,
        "history": history,
    }


def issue_detail(
    runs_dir: Path,
    slug: str,
    timestamp: str | None,
    issue_key: str,
    *,
    page: int = 1,
    per_page: int = 200,
    q: str | None = None,
) -> dict[str, Any] | None:
    crawls = list_project_crawls(runs_dir, slug)
    if not crawls:
        return None
    requested_ts = timestamp if timestamp in crawls else crawls[-1]
    current_ts, _ = _select_crawl_timestamp_with_report(runs_dir, slug, requested=requested_ts)
    if not current_ts:
        return None

    report = load_report_json(runs_dir, slug, current_ts)
    if not report:
        return None
    issues = report.get("issues") if isinstance(report.get("issues"), dict) else {}
    block = issues.get(issue_key)

    pages = report.get("pages") if isinstance(report.get("pages"), list) else []

    def _path_from_any_os(value: str) -> Path:
        raw = (value or "").strip().strip('"')
        m = re.match(r"^([a-zA-Z]):[\\\\/](.*)$", raw)
        if m:
            drive = (m.group(1) or "").lower()
            rest = (m.group(2) or "").replace("\\", "/")
            return Path(f"/mnt/{drive}/{rest}")
        return Path(raw)

    def _load_json_list_any(path_str: str) -> list[dict[str, Any]]:
        p = _path_from_any_os(path_str).expanduser()
        try:
            if not p.is_absolute():
                p = (Path.cwd() / p).resolve()
            else:
                p = p.resolve()
        except Exception:
            pass
        if not p.exists() or not p.is_file():
            return []
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return []
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        return []

    def _paginate(rows: list[dict[str, Any]], page: int, per_page: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        per_page = max(10, min(500, int(per_page)))
        total = len(rows)
        pages_total = max(1, int(math.ceil(total / per_page))) if total else 1
        page = max(1, min(int(page), pages_total))
        start = (page - 1) * per_page
        end = start + per_page
        return rows[start:end], {"page": page, "per_page": per_page, "total": total, "pages": pages_total}

    def _gsc_inspection_severity(payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
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
            "verdict": verdict,
            "coverage_state": coverage,
            "robots_state": robots,
            "indexing_state": indexing,
            "fetch_state": fetch,
            "google_canonical": google_can,
            "user_canonical": user_can,
        }

        lc = f"{verdict} {coverage} {robots} {indexing} {fetch}".lower()
        if "blocked" in lc and ("robot" in lc or "robots" in lc or "robotstxt" in lc):
            return "error", entry
        if verdict.upper() in {"FAIL", "FAILED"}:
            return "error", entry
        if "error" in lc or "server error" in lc or "5xx" in lc:
            return "error", entry
        if "redirect error" in lc:
            return "warning", entry
        if "noindex" in lc or "blocked_by_meta_tag" in lc or "blocked by meta tag" in lc:
            return "warning", entry
        if "not indexed" in lc or "currently not indexed" in lc:
            return "notice", entry
        if google_can and user_can and google_can != user_can:
            return "notice", entry
        return "ok", entry

    def cwv_metric_value(ps: dict[str, Any], m: str) -> tuple[float | int | None, str | None]:
        fm = ps.get("field_metrics")
        if isinstance(fm, dict):
            node = fm.get(m)
            if isinstance(node, dict) and "p75" in node:
                v = node.get("p75")
                if isinstance(v, bool):
                    v = None
                if isinstance(v, (int, float)):
                    return v, "field"
        lm = ps.get("lab_metrics")
        if isinstance(lm, dict):
            node = lm.get(m)
            if isinstance(node, dict) and "value" in node:
                v = node.get("value")
                if isinstance(v, bool):
                    v = None
                if isinstance(v, (int, float)):
                    return v, "lab"
        return None, None

    def cwv_category(m: str, value: float | int | None) -> str:
        if value is None:
            return "na"
        v = float(value)
        if m == "lcp":
            if v <= 2500:
                return "good"
            if v <= 4000:
                return "ni"
            return "poor"
        if m == "cls":
            if v <= 0.1:
                return "good"
            if v <= 0.25:
                return "ni"
            return "poor"
        if m == "tbt":
            if v <= 200:
                return "good"
            if v <= 600:
                return "ni"
            return "poor"
        return "na"

    cwv_rows: list[dict[str, Any]] | None = None
    if issue_key in {"cwv_lcp_pages_to_fix", "cwv_tbt_pages_to_fix", "cwv_cls_pages_to_fix"}:
        metric = issue_key.split("_", 2)[1]  # lcp/tbt/cls
        rows: list[dict[str, Any]] = []
        for p in pages:
            if not isinstance(p, dict):
                continue
            url = p.get("url")
            if not isinstance(url, str) or not url.startswith(("http://", "https://")):
                continue
            ps = p.get("pagespeed")
            if not isinstance(ps, dict) or ps.get("error"):
                continue
            v, src = cwv_metric_value(ps, metric)
            cat = cwv_category(metric, v)
            if cat in {"ni", "poor"}:
                rows.append({"url": url, "value": v, "unit": "ms" if metric in {"lcp", "tbt"} else "", "source": src, "category": cat})

        rows.sort(key=lambda r: float(r.get("value") or 0), reverse=True)
        cwv_rows = rows[:500]

        # Backward-compatible CWV drilldown for older reports that don't contain `cwv_*_pages_to_fix`.
        if not isinstance(block, dict):
            examples = [r["url"] for r in cwv_rows]
            block = {"count": len(examples), "examples": examples[:200]}

    if not isinstance(block, dict):
        return None

    meta_info = issue_meta(issue_key)
    count = issue_count(block)
    examples = issue_examples(block, limit=200)

    q_norm = (q or "").strip().lower()
    special_kind: str | None = None
    special_rows: list[dict[str, Any]] | None = None
    special_pagination: dict[str, Any] | None = None
    special_source: str = ""

    # Generic "real rows" for crawl issues: if the crawler wrote `audit/issues/<issue_key>.json`,
    # prefer it over the summarized `examples` list stored in report.json.
    if issue_key not in {"cwv_lcp_pages_to_fix", "cwv_tbt_pages_to_fix", "cwv_cls_pages_to_fix"}:
        issue_rows_path = report_path(runs_dir, slug, current_ts).parent / "issues" / f"{re.sub(r'[^a-zA-Z0-9_.-]+','_', issue_key)}.json"
    else:
        issue_rows_path = None
    if issue_rows_path and issue_rows_path.exists() and issue_rows_path.is_file():
        try:
            raw = json.loads(issue_rows_path.read_text(encoding="utf-8"))
        except Exception:
            raw = None
        rows: list[dict[str, Any]] = []
        if isinstance(raw, list):
            for it in raw:
                if isinstance(it, dict):
                    rows.append(it)
                else:
                    s = str(it)
                    row = {"value": s}
                    if s.startswith(("http://", "https://")):
                        row["url"] = s
                    rows.append(row)
        if rows:
            special_kind = "crawl_issue_rows"
            if q_norm:
                rows = [r for r in rows if q_norm in json.dumps(r, ensure_ascii=False).lower()]
            special_rows = rows
            special_source = str(issue_rows_path)

    if issue_key in {"gsc_indexing_errors", "gsc_indexing_warnings", "gsc_indexing_notices"}:
        meta = report.get("meta") if isinstance(report.get("meta"), dict) else {}
        gsc_meta = meta.get("gsc_api") if isinstance(meta.get("gsc_api"), dict) else {}
        insp = gsc_meta.get("url_inspection") if isinstance(gsc_meta.get("url_inspection"), dict) else {}
        insp_path = str(insp.get("json") or "").strip()
        if not insp_path:
            insp_path = str((runs_dir / slug / current_ts / "gsc" / "gsc-url-inspection.json").resolve())
        raw = _load_json_list_any(insp_path)
        sev = "error" if issue_key.endswith("_errors") else ("warning" if issue_key.endswith("_warnings") else "notice")
        rows: list[dict[str, Any]] = []
        for it in raw:
            url = str(it.get("url") or "").strip()
            payload = it.get("payload") if isinstance(it.get("payload"), dict) else {}
            if not url or not payload:
                continue
            s, entry = _gsc_inspection_severity(payload)
            if s != sev:
                continue
            rows.append({"url": url, **entry})
        special_kind = "gsc_url_inspection"
        if q_norm:
            rows = [r for r in rows if q_norm in str(r.get("url") or "").lower() or q_norm in json.dumps(r, ensure_ascii=False).lower()]
        special_rows = rows
        special_source = insp_path

    if issue_key in {"bing_crawl_issues", "bing_blocked_urls", "bing_sitemaps"}:
        key_to_file = {
            "bing_crawl_issues": "bing-crawl-issues.json",
            "bing_blocked_urls": "bing-blocked-urls.json",
            "bing_sitemaps": "bing-sitemaps.json",
        }
        fp = str((runs_dir / slug / current_ts / "bing" / key_to_file[issue_key]).resolve())
        raw = _load_json_list_any(fp)
        rows = [r for r in raw if isinstance(r, dict) and not r.get("error")]
        special_kind = "bing_api_rows"
        if q_norm:
            rows = [r for r in rows if q_norm in json.dumps(r, ensure_ascii=False).lower()]
        special_rows = rows
        special_source = fp

    if special_rows is not None:
        count = len(special_rows)
        examples = []
        paged, pagination = _paginate(special_rows, page=int(page or 1), per_page=int(per_page))
        special_rows = paged
        special_pagination = pagination

    issue_payload: dict[str, Any] = {
        "slug": slug,
        "timestamp": current_ts,
        "timestamp_label": format_timestamp(current_ts),
        "issue": {
            "key": issue_key,
            "label": meta_info.label,
            "category": meta_info.category,
            "severity": meta_info.severity,
            "count": count,
            "examples": examples,
            "raw": block,
        },
        "report_json": str(report_path(runs_dir, slug, current_ts)),
        "report_md": str(report_md_path(runs_dir, slug, current_ts)),
    }

    if cwv_rows is not None:
        metric = issue_key.split("_", 2)[1]
        issue_payload["issue"]["cwv"] = {
            "metric": metric,
            "rows": cwv_rows,
            "thresholds": {
                "lcp": {"good_max": 2500, "ni_max": 4000},
                "tbt": {"good_max": 200, "ni_max": 600},
                "cls": {"good_max": 0.1, "ni_max": 0.25},
            }.get(metric, {}),
        }

    if special_kind and special_rows is not None and special_pagination is not None:
        issue_payload["issue"]["kind"] = special_kind
        issue_payload["issue"]["rows"] = special_rows
        issue_payload["issue"]["pagination"] = special_pagination
        issue_payload["issue"]["source"] = special_source

    return issue_payload


def filter_issues(
    issues: Iterable[dict[str, Any]],
    severity: str | None = None,
    category: str | None = None,
    query: str | None = None,
) -> list[dict[str, Any]]:
    sev = (severity or "").strip().lower()
    cat = (category or "").strip()
    q = (query or "").strip().lower()

    out: list[dict[str, Any]] = []
    for it in issues:
        if sev and str(it.get("severity")).lower() != sev:
            continue
        if cat and str(it.get("category")) != cat:
            continue
        if q and q not in str(it.get("label", "")).lower() and q not in str(it.get("key", "")).lower():
            continue
        out.append(it)
    return out
