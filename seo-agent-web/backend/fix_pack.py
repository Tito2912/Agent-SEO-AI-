from __future__ import annotations

import csv
import io
import json
import re
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

try:
    from . import audit_dashboard as dash  # type: ignore
    from . import fix_suggestions as fix_suggestions  # type: ignore
except ImportError:  # pragma: no cover
    import audit_dashboard as dash  # type: ignore
    import fix_suggestions as fix_suggestions  # type: ignore


_ISSUE_KEY_SAFE_RE = re.compile(r"[^a-zA-Z0-9_.-]+")


def _safe_issue_key(issue_key: str) -> str:
    key = _ISSUE_KEY_SAFE_RE.sub("_", (issue_key or "").strip()) or "issue"
    return key[:180]


def _csv_bytes(rows: list[dict[str, Any]], *, fieldnames: list[str]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def _read_json_list(path: Path) -> list[Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return []
    return data if isinstance(data, list) else []


def _issue_rows(issues_dir: Path, issue_key: str, *, limit: int = 100000) -> list[Any]:
    key = _safe_issue_key(issue_key)
    path = (issues_dir / f"{key}.json").resolve()
    if not path.exists() or not path.is_file():
        return []
    rows = _read_json_list(path)
    return rows[: max(0, int(limit))]


def _domain_from_base_url(base_url: str) -> str:
    try:
        host = (urlsplit(base_url).hostname or "").strip().lower()
    except Exception:
        host = ""
    return host[4:] if host.startswith("www.") else host


def _looks_noindex(value: str | None) -> bool:
    v = (value or "").strip().lower()
    return "noindex" in v


def _norm_url_for_compare(url: str | None) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    if u.endswith("/"):
        u = u[:-1]
    return u


def _is_indexable_page(p: dict[str, Any]) -> bool:
    try:
        sc = int(p.get("status_code")) if p.get("status_code") is not None else None
    except Exception:
        sc = None
    if sc != 200:
        return False
    ct = str(p.get("content_type") or "").lower()
    if ct and "html" not in ct:
        return False
    if str(p.get("error") or "").strip():
        return False
    if _looks_noindex(str(p.get("meta_robots") or "")) or _looks_noindex(str(p.get("x_robots_tag") or "")):
        return False
    canonical = str(p.get("canonical") or "").strip()
    final_url = str(p.get("final_url") or p.get("url") or "").strip()
    if canonical and _norm_url_for_compare(canonical) and _norm_url_for_compare(canonical) != _norm_url_for_compare(final_url):
        return False
    return True


@dataclass(frozen=True)
class TopAction:
    issue_key: str
    label: str
    severity: str
    count: int
    priority: str
    effort: str
    why: str
    fix: list[str]
    verify: list[str]
    sample_urls: list[str]


def _priority_rank(priority: str) -> int:
    p = (priority or "").strip().lower()
    return {"high": 3, "medium": 2, "low": 1}.get(p, 0)


def _severity_rank(sev: str) -> int:
    s = (sev or "").strip().lower()
    return {"error": 3, "warning": 2, "notice": 1}.get(s, 0)


def top_actions(report: dict[str, Any], *, site_name: str, base_url: str, limit: int = 3) -> list[TopAction]:
    summary = dash.summarize_report(report)
    issues = summary.get("issues") if isinstance(summary.get("issues"), list) else []

    actions: list[TopAction] = []
    for it in issues:
        if not isinstance(it, dict):
            continue
        issue_key = str(it.get("key") or "").strip()
        if not issue_key:
            continue
        count = int(it.get("count") or 0)
        if count <= 0:
            continue
        suggestion = fix_suggestions.suggest_issue_fix(
            issue_key=issue_key,
            label=str(it.get("label") or ""),
            category=str(it.get("category") or ""),
            severity=str(it.get("severity") or ""),
            count=count,
            report=report,
            site_name=site_name,
            base_url=base_url,
        )
        actions.append(
            TopAction(
                issue_key=issue_key,
                label=str(suggestion.get("label") or issue_key),
                severity=str(suggestion.get("severity") or ""),
                count=int(suggestion.get("count") or 0),
                priority=str(suggestion.get("priority") or ""),
                effort=str(suggestion.get("effort") or ""),
                why=str(suggestion.get("why") or ""),
                fix=list(suggestion.get("fix") or []) if isinstance(suggestion.get("fix"), list) else [],
                verify=list(suggestion.get("verify") or []) if isinstance(suggestion.get("verify"), list) else [],
                sample_urls=list(suggestion.get("sample_urls") or []) if isinstance(suggestion.get("sample_urls"), list) else [],
            )
        )

    actions.sort(
        key=lambda a: (
            -_priority_rank(a.priority),
            -_severity_rank(a.severity),
            -int(a.count),
            a.issue_key,
        )
    )
    return actions[: max(0, int(limit))]


def _md_top_actions(actions: list[TopAction]) -> str:
    lines: list[str] = []
    if not actions:
        return "Aucune action prioritaire détectée.\n"

    for i, a in enumerate(actions, start=1):
        lines.append(f"## Action {i} — {a.label}")
        lines.append(f"- Issue: `{a.issue_key}`")
        lines.append(f"- Severity: **{a.severity}** · Priority: **{a.priority}** · Effort: **{a.effort}** · Count: **{a.count}**")
        if a.why:
            lines.append("")
            lines.append("### Pourquoi c’est important")
            lines.append(a.why)
        if a.fix:
            lines.append("")
            lines.append("### Actions recommandées")
            for step in a.fix:
                if str(step).strip():
                    lines.append(f"- {str(step).strip()}")
        if a.verify:
            lines.append("")
            lines.append("### Vérification")
            for step in a.verify:
                if str(step).strip():
                    lines.append(f"- {str(step).strip()}")
        if a.sample_urls:
            lines.append("")
            lines.append("### URLs (exemples)")
            for u in a.sample_urls[:10]:
                if isinstance(u, str) and u.strip():
                    lines.append(f"- {u.strip()}")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def _robots_template(*, base_url: str, sitemaps: list[str]) -> str:
    lines: list[str] = []
    lines.append("# robots.txt (proposition)")
    lines.append("# À valider avant mise en prod (peut impacter l’indexation).")
    lines.append("")
    lines.append("User-agent: *")
    lines.append("Disallow:")
    lines.append("")
    if sitemaps:
        for sm in sitemaps:
            if isinstance(sm, str) and sm.strip().startswith(("http://", "https://")):
                lines.append(f"Sitemap: {sm.strip()}")
    else:
        lines.append(f"# Sitemap: {base_url.rstrip('/')}/sitemap.xml")
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def _schema_templates(*, site_name: str, base_url: str) -> dict[str, str]:
    domain = _domain_from_base_url(base_url)

    website = {
        "@context": "https://schema.org",
        "@type": "WebSite",
        "name": site_name or domain or "Website",
        "url": base_url.rstrip("/") + "/",
    }
    org = {
        "@context": "https://schema.org",
        "@type": "Organization",
        "name": site_name or domain or "Organization",
        "url": base_url.rstrip("/") + "/",
        "logo": "https://example.com/logo.png",
    }
    return {
        "schema/website.jsonld": json.dumps(website, ensure_ascii=False, indent=2) + "\n",
        "schema/organization.jsonld": json.dumps(org, ensure_ascii=False, indent=2) + "\n",
    }


def _how_to_apply_md() -> str:
    return (
        "# How to apply — Fix Pack\n\n"
        "Ce pack contient des exports et des templates. Vérifie toujours avant déploiement.\n\n"
        "## 1) Redirections (CSV)\n"
        "- Fichier: `exports/redirects_to_fill.csv`\n"
        "- Objectif: mapper les anciennes URLs (404) vers les meilleures pages (301).\n"
        "- Si tu es sur Netlify/Vercel/Cloudflare: tu peux souvent importer un CSV ou convertir en règles.\n\n"
        "## 2) Titles / Meta / H1 / Canonicals (CSV)\n"
        "- Fichier: `exports/pages_seo.csv`\n"
        "- Objectif: repérer les manquants/duplicats/longueurs.\n\n"
        "### WordPress\n"
        "- Mets à jour via ton plugin SEO (ex: Yoast/RankMath) ou directement dans l’éditeur.\n"
        "- Pour les redirections: plugin de redirection (ou règle serveur).\n\n"
        "### Shopify\n"
        "- Titles/meta: via l’admin produit/page/collection + éventuellement theme.\n"
        "- JSON-LD: souvent dans `theme.liquid` (à valider selon ton thème).\n\n"
        "### Webflow\n"
        "- Titles/meta: paramètres de page / collections.\n"
        "- Redirections: settings du projet → 301 redirects.\n\n"
        "### Sites codés (Next/Astro/etc.)\n"
        "- Applique en code + ouvre une PR (si tu veux, on ajoutera l’auto-fix GitHub ensuite).\n\n"
        "## 3) robots.txt / Sitemap\n"
        "- `robots.txt` proposé: `robots.txt`\n"
        "- URLs sitemap (liste): `exports/sitemap_urls.txt` (à transformer en sitemap.xml si besoin)\n\n"
        "## 4) Schema.org (JSON-LD)\n"
        "- Templates: `schema/website.jsonld`, `schema/organization.jsonld`\n"
        "- Remplace le logo + ajuste selon ton site (Organization/LocalBusiness/Product…).\n"
    )


def build_fix_pack_zip_bytes(
    *,
    runs_dir: Path,
    slug: str,
    timestamp: str,
    site_name: str,
    base_url: str,
    report: dict[str, Any],
) -> bytes:
    ts = str(timestamp or "").strip()
    domain = _domain_from_base_url(base_url) or slug
    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    issues_dir = (runs_dir / slug / ts / "audit" / "issues").resolve()

    summary = dash.summarize_report(report)
    actions = top_actions(report, site_name=site_name, base_url=base_url, limit=3)

    # --- Exports ---
    pages = report.get("pages") if isinstance(report.get("pages"), list) else []
    rows_pages: list[dict[str, Any]] = []
    for p in pages:
        if not isinstance(p, dict):
            continue
        h1 = p.get("h1") if isinstance(p.get("h1"), list) else []
        h1_first = str(h1[0]).strip() if h1 and isinstance(h1[0], str) else ""
        rows_pages.append(
            {
                "url": str(p.get("url") or ""),
                "final_url": str(p.get("final_url") or ""),
                "status_code": p.get("status_code"),
                "content_type": str(p.get("content_type") or ""),
                "title": str(p.get("title") or ""),
                "meta_description": str(p.get("meta_description") or ""),
                "canonical": str(p.get("canonical") or ""),
                "lang": str(p.get("lang") or ""),
                "meta_robots": str(p.get("meta_robots") or ""),
                "x_robots_tag": str(p.get("x_robots_tag") or ""),
                "h1_first": h1_first,
                "h1_count": int(p.get("h1_tag_count") or 0),
                "word_count": int(p.get("text_word_count") or 0) if p.get("text_word_count") is not None else "",
                "images_total": int(p.get("images_total") or 0),
                "images_missing_alt": int(p.get("images_missing_alt") or 0),
                "response_bytes": p.get("response_bytes") or "",
                "elapsed_ms": p.get("elapsed_ms") or "",
            }
        )

    redirects_observed: list[dict[str, Any]] = []
    for p in pages:
        if not isinstance(p, dict):
            continue
        chain = p.get("redirect_chain") if isinstance(p.get("redirect_chain"), list) else []
        statuses = p.get("redirect_statuses") if isinstance(p.get("redirect_statuses"), list) else []
        if not chain and not statuses:
            continue
        redirect_chain = [str(u) for u in chain if isinstance(u, str)]
        redirect_statuses = [str(s) for s in statuses if isinstance(s, int)]
        final_url = str(p.get("final_url") or "")
        full_chain = [str(p.get("url") or ""), *redirect_chain, final_url]
        redirects_observed.append(
            {
                "source_url": str(p.get("url") or ""),
                "final_url": final_url,
                "hops": len(redirect_statuses) if redirect_statuses else len(redirect_chain),
                "statuses": " -> ".join(redirect_statuses),
                "chain": " -> ".join([u for u in full_chain if u]),
            }
        )

    # A starter "mapping" file for 404s (user fills target_url).
    redirects_to_fill: list[dict[str, Any]] = []
    if issues_dir.exists():
        for u in _issue_rows(issues_dir, "http_404", limit=50000):
            if isinstance(u, str) and u.startswith(("http://", "https://")):
                redirects_to_fill.append({"from_url": u, "to_url": "", "reason": "404"})
        for u in _issue_rows(issues_dir, "http_4xx", limit=50000):
            if isinstance(u, str) and u.startswith(("http://", "https://")):
                redirects_to_fill.append({"from_url": u, "to_url": "", "reason": "4xx"})

    sitemap_urls: list[str] = []
    for p in pages:
        if not isinstance(p, dict):
            continue
        if not _is_indexable_page(p):
            continue
        u = str(p.get("final_url") or p.get("url") or "").strip()
        if u.startswith(("http://", "https://")):
            sitemap_urls.append(u)
    sitemap_urls = sorted(set(sitemap_urls))

    meta = report.get("meta") if isinstance(report.get("meta"), dict) else {}
    sitemaps = meta.get("sitemaps") if isinstance(meta.get("sitemaps"), list) else []
    sitemaps = [str(u).strip() for u in sitemaps if isinstance(u, str)]

    # --- Files in the zip ---
    readme = [
        f"# Fix Pack — {site_name or domain}",
        "",
        f"- Crawl: `{slug}` · `{ts}`",
        f"- Generated: `{generated_at}`",
        "",
        "## Top 3 actions (priorisées)",
        "",
    ]
    readme.append(_md_top_actions(actions).strip())
    readme.append("")
    readme.append("## Exports")
    readme.append("- `exports/pages_seo.csv` — titles/meta/h1/canonicals + signaux clés")
    readme.append("- `exports/redirects_observed.csv` — chaînes de redirection observées")
    readme.append("- `exports/redirects_to_fill.csv` — mapping à compléter (ex: 404 → meilleure page)")
    readme.append("- `exports/sitemap_urls.txt` — URLs indexables (pour générer un sitemap.xml si besoin)")
    readme.append("")
    readme.append("## Templates")
    readme.append("- `robots.txt` — proposition (à valider)")
    readme.append("- `schema/*.jsonld` — snippets JSON-LD à adapter")
    readme.append("")
    readme.append("## Comment appliquer")
    readme.append("Voir `HOW_TO_APPLY.md`.")
    readme_text = "\n".join(readme).strip() + "\n"

    issues_summary_rows: list[dict[str, Any]] = []
    issues_list = summary.get("issues") if isinstance(summary.get("issues"), list) else []
    for it in issues_list:
        if not isinstance(it, dict):
            continue
        issue_key = str(it.get("key") or "").strip()
        if not issue_key:
            continue
        count = int(it.get("count") or 0)
        if count <= 0:
            continue
        sug = fix_suggestions.suggest_issue_fix(
            issue_key=issue_key,
            label=str(it.get("label") or ""),
            category=str(it.get("category") or ""),
            severity=str(it.get("severity") or ""),
            count=count,
            report=report,
            site_name=site_name,
            base_url=base_url,
        )
        issues_summary_rows.append(
            {
                "issue_key": issue_key,
                "label": str(it.get("label") or ""),
                "category": str(it.get("category") or ""),
                "severity": str(it.get("severity") or ""),
                "count": count,
                "priority": str(sug.get("priority") or ""),
                "effort": str(sug.get("effort") or ""),
            }
        )

    pack_meta = {
        "version": 1,
        "generated_at": generated_at,
        "slug": slug,
        "timestamp": ts,
        "site_name": site_name,
        "base_url": base_url,
        "domain": domain,
    }

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        z.writestr("README.md", readme_text)
        z.writestr("HOW_TO_APPLY.md", _how_to_apply_md())
        z.writestr("meta.json", json.dumps(pack_meta, ensure_ascii=False, indent=2) + "\n")

        z.writestr(
            "exports/pages_seo.csv",
            _csv_bytes(
                rows_pages,
                fieldnames=[
                    "url",
                    "final_url",
                    "status_code",
                    "content_type",
                    "title",
                    "meta_description",
                    "canonical",
                    "lang",
                    "meta_robots",
                    "x_robots_tag",
                    "h1_first",
                    "h1_count",
                    "word_count",
                    "images_total",
                    "images_missing_alt",
                    "response_bytes",
                    "elapsed_ms",
                ],
            ),
        )
        z.writestr(
            "exports/issues_summary.csv",
            _csv_bytes(
                issues_summary_rows,
                fieldnames=["issue_key", "label", "category", "severity", "count", "priority", "effort"],
            ),
        )
        z.writestr(
            "exports/redirects_observed.csv",
            _csv_bytes(redirects_observed, fieldnames=["source_url", "final_url", "hops", "statuses", "chain"]),
        )
        z.writestr(
            "exports/redirects_to_fill.csv",
            _csv_bytes(redirects_to_fill, fieldnames=["from_url", "to_url", "reason"]),
        )
        z.writestr("exports/sitemap_urls.txt", "\n".join(sitemap_urls).strip() + ("\n" if sitemap_urls else ""))

        z.writestr("robots.txt", _robots_template(base_url=base_url, sitemaps=sitemaps))

        for name, content in _schema_templates(site_name=site_name, base_url=base_url).items():
            z.writestr(name, content)

    return buf.getvalue()

