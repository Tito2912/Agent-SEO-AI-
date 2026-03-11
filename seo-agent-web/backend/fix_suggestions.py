from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit

try:
    # When running as `uvicorn backend.app:app` (recommended).
    from . import audit_dashboard as dash  # type: ignore
except ImportError:
    # When running from inside this folder (`uvicorn app:app`) or with `--app-dir seo-agent-web/backend`.
    import audit_dashboard as dash  # type: ignore


def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _domain_from_base_url(base_url: str) -> str:
    try:
        host = (urlsplit(base_url).hostname or "").strip().lower()
    except Exception:
        host = ""
    if host.startswith("www."):
        host = host[4:]
    return host


def _priority(issue_key: str, severity: str) -> str:
    sev = (severity or "").strip().lower()
    key = (issue_key or "").strip().lower()

    # Hard overrides (always critical to fix).
    if any(tok in key for tok in ("http_500", "http_5xx", "timed_out", "redirect_loop", "broken_redirect")):
        return "high"
    if any(tok in key for tok in ("http_404", "http_4xx")):
        return "high"
    if any(tok in key for tok in ("noindex", "blocked_by_robots")):
        return "high"

    if sev == "error":
        return "high"
    if sev == "warning":
        return "medium"
    return "low"


def _effort(issue_key: str) -> str:
    key = (issue_key or "").strip().lower()
    if any(tok in key for tok in ("redirect", "canonical", "hreflang", "schema", "pagespeed", "cwv")):
        return "high"
    if any(tok in key for tok in ("javascript", "css", "image", "performance")):
        return "medium"
    if any(tok in key for tok in ("missing_title", "missing_h1", "missing_meta_description", "title_", "meta_description_")):
        return "low"
    return "medium"


def _sample_urls(report: dict[str, Any], issue_key: str, limit: int = 6) -> list[str]:
    issues = report.get("issues") if isinstance(report.get("issues"), dict) else {}
    block = issues.get(issue_key)
    impacted = dash.extract_impacted_pages(issue_key, block, limit=500) if isinstance(block, dict) else set()
    out = sorted({u for u in impacted if isinstance(u, str) and u.startswith(("http://", "https://"))})
    return out[: max(0, int(limit))]


def _looks_like_host_variant(urls: list[str]) -> bool:
    if len(urls) < 2:
        return False
    hosts = set()
    paths = set()
    schemes = set()
    for u in urls:
        try:
            p = urlsplit(u)
        except Exception:
            return False
        hosts.add((p.hostname or "").lower().lstrip("www."))
        paths.add(p.path or "/")
        schemes.add((p.scheme or "").lower())
    # Same path, different scheme/host → very likely variants.
    return len(paths) == 1 and (len(hosts) > 1 or len(schemes) > 1)


def suggest_issue_fix(
    *,
    issue_key: str,
    label: str,
    category: str,
    severity: str,
    count: int,
    report: dict[str, Any],
    site_name: str,
    base_url: str,
) -> dict[str, Any]:
    key = (issue_key or "").strip()
    label = (label or "").strip() or key
    category = (category or "").strip() or "Other"
    severity = (severity or "").strip().lower() or "notice"
    count = int(count or 0)

    domain = _domain_from_base_url(base_url) or (site_name or "").strip()
    sample_urls = _sample_urls(report, key, limit=6)

    priority = _priority(key, severity)
    effort = _effort(key)

    why = ""
    fix: list[str] = []
    verify: list[str] = []

    lk = key.lower()

    # Content fundamentals
    if lk == "missing_title":
        why = "Certaines pages n’ont pas de balise <title>. Cela dégrade la pertinence et le CTR dans Google."
        fix = [
            "Ajouter un <title> unique par page (idéalement 50–60 caractères, sans duplication).",
            f"Inclure le sujet principal de la page + la marque ({domain}) quand c’est pertinent.",
            "Éviter les titres génériques (« Accueil », « Page ») et les répétitions exactes.",
        ]
        verify = [
            "Relancer un crawl et vérifier que l’issue « missing_title » retombe à 0.",
            "Contrôler qu’il n’y a pas d’explosion de « duplicate_titles » après correction.",
        ]
    elif lk == "missing_meta_description":
        why = "Certaines pages n’ont pas de meta description. Même si elle n’est pas un facteur direct, elle influence le CTR."
        fix = [
            "Ajouter une meta description unique par page (≈ 140–160 caractères, orientée bénéfice).",
            "Inclure l’intention de la page et un CTA léger (sans spam).",
            "Éviter les descriptions dupliquées et trop courtes.",
        ]
        verify = [
            "Relancer un crawl et vérifier « missing_meta_description » et « duplicate_meta_descriptions ».",
            "Contrôler le rendu dans un simulateur SERP (coupe/ellipse).",
        ]
    elif lk == "missing_h1":
        why = "Certaines pages n’ont pas de H1. Le H1 aide la compréhension du sujet (SEO + accessibilité)."
        fix = [
            "Ajouter exactement 1 H1 descriptif, aligné avec l’intention de la page.",
            "Éviter les H1 vides ou purement décoratifs.",
        ]
        verify = [
            "Relancer un crawl et vérifier que « missing_h1 » retombe à 0.",
            "Contrôler « multiple_h1 » sur les pages modifiées.",
        ]
    elif lk == "multiple_h1":
        why = "Plusieurs H1 sur une même page peut diluer la hiérarchie (souvent un problème de template)."
        fix = [
            "Garder un seul H1 principal, convertir les autres en H2/H3 selon la structure.",
            "Vérifier les composants réutilisés (header/hero) qui injectent un H1.",
        ]
        verify = [
            "Relancer un crawl et vérifier que « multiple_h1 » retombe à 0.",
        ]
    elif lk in {"title_too_long", "title_too_short"} or lk.startswith("title_too_"):
        why = "La longueur des titres peut réduire le CTR (titres tronqués) ou la pertinence (titres trop courts)."
        fix = [
            "Raccourcir/étendre le titre pour qu’il reste lisible et unique (viser ~50–60 caractères).",
            "Mettre le mot-clé principal au début quand possible.",
        ]
        verify = [
            "Relancer un crawl et vérifier les issues de longueur de title.",
            "Contrôler un échantillon de pages dans un simulateur SERP.",
        ]
    elif lk.startswith("meta_description_too_long") or lk.startswith("meta_description_too_short"):
        why = "Des meta descriptions trop longues sont tronquées, trop courtes sont peu informatives."
        fix = [
            "Réécrire les descriptions (≈ 140–160 caractères) avec bénéfice + contexte + CTA léger.",
            "Rendre chaque description unique et cohérente avec le contenu réel de la page.",
        ]
        verify = [
            "Relancer un crawl et vérifier les issues de longueur de meta description.",
            "Contrôler un échantillon de pages en SERP.",
        ]

    # Duplication / canonicals / variants
    elif lk == "missing_canonical":
        why = "Sans canonical, tu augmentes le risque de duplication (paramètres, variantes, etc.)."
        fix = [
            "Ajouter une balise canonical auto-référente sur les pages indexables.",
            "Définir une politique d’URL (HTTPS, www ou non-www, trailing slash) et s’y tenir.",
        ]
        verify = [
            "Relancer un crawl et vérifier « missing_canonical » et les issues liées aux canonicals.",
            "Vérifier la cohérence canonicals ↔︎ URLs finales (pas de redirect/4xx/5xx).",
        ]
    elif lk == "duplicate_titles":
        why = "Des titres dupliqués peuvent indiquer des pages trop proches (ou des variantes http/https/www) et nuisent au ciblage."
        if _looks_like_host_variant(sample_urls):
            fix = [
                "Définir une URL canonique (ex: https + non-www) et forcer toutes les variantes en 301 vers celle-ci.",
                "Mettre à jour les balises canonical vers l’URL finale (200).",
                "Mettre à jour les liens internes, sitemap et hreflang (si présent) vers l’URL canonique.",
            ]
        else:
            fix = [
                "Rendre les titres uniques : mot-clé principal + différenciateur (catégorie, ville, produit, etc.).",
                f"Conserver une convention de marque (ex: « … | {domain} ») sans rendre tous les titres identiques.",
            ]
        verify = [
            "Relancer un crawl et vérifier « duplicate_titles ».",
            "Contrôler que la page ciblée dans Google est bien l’URL canonique (pas une variante).",
        ]
    elif lk == "duplicate_meta_descriptions":
        why = "Des meta descriptions dupliquées donnent des snippets répétitifs et peuvent réduire le CTR."
        fix = [
            "Réécrire des descriptions uniques par page (bénéfice + détail spécifique + CTA léger).",
            "Éviter les templates identiques sur de nombreuses pages (ex: pages catégories).",
        ]
        verify = [
            "Relancer un crawl et vérifier « duplicate_meta_descriptions ».",
            "Contrôler dans GSC si le CTR progresse sur les pages touchées.",
        ]

    # Status / crawl / redirects
    elif lk.startswith("http_404") or lk.startswith("http_4xx") or "4xx" in lk:
        why = "Les erreurs 4xx créent des liens cassés, gaspillent du budget de crawl et dégradent l’UX."
        fix = [
            "Identifier la source (liens internes, sitemap, backlinks) vers ces URLs.",
            "Corriger le lien à la source OU créer une redirection 301 vers la page la plus pertinente.",
            "Si la page est supprimée sans équivalent : retourner 410 et retirer des sitemaps/liens internes.",
        ]
        verify = [
            "Relancer un crawl et vérifier que ces URLs ne renvoient plus 4xx.",
            "Vérifier la couverture dans Google Search Console (pages non trouvées / soft 404).",
        ]
    elif lk.startswith("http_500") or lk.startswith("http_5xx") or "5xx" in lk:
        why = "Les erreurs 5xx/timeout empêchent l’indexation et dégradent fortement l’expérience."
        fix = [
            "Diagnostiquer serveur/app (logs, CPU/RAM, timeouts, base de données).",
            "Corriger les erreurs et mettre en place une surveillance (alerting) + retry/backoff côté crawl.",
        ]
        verify = [
            "Relancer un crawl et vérifier la stabilité (0 5xx / timeouts).",
            "Contrôler dans GSC les erreurs serveur.",
        ]
    elif "redirect_chain" in lk:
        why = "Les chaînes de redirection augmentent la latence et gaspillent le budget de crawl."
        fix = [
            "Réduire à une seule redirection 301 (source → destination finale).",
            "Mettre à jour les liens internes, canonicals et sitemaps vers la destination finale.",
        ]
        verify = [
            "Relancer un crawl et vérifier « redirect_chain* ».",
            "Contrôler quelques URLs avec un outil de trace de redirections.",
        ]
    elif "redirect_loop" in lk:
        why = "Une boucle de redirection rend l’URL inatteignable (crawl impossible)."
        fix = [
            "Identifier la règle fautive (server config, middleware, CDN) et casser la boucle.",
            "Vérifier les canonicals / règles http↔https / www↔non-www / trailing slash.",
        ]
        verify = [
            "Relancer un crawl et vérifier que l’URL finale renvoie 200.",
        ]
    elif lk in {"http_to_https_redirect", "https_to_http_redirect"} or "http_to_https" in lk or "https_to_http" in lk:
        why = "Les variantes HTTP/HTTPS créent des duplications et des signaux incohérents."
        fix = [
            "Choisir HTTPS comme version canonique et rediriger HTTP → HTTPS en 301.",
            "Mettre à jour les liens internes, canonicals et sitemaps pour utiliser HTTPS partout.",
        ]
        verify = [
            "Relancer un crawl et vérifier que les URLs HTTP ne restent pas indexables.",
        ]

    # Indexability / robots
    elif lk.startswith("noindex") or lk.startswith("nofollow") or "blocked_by_robots" in lk:
        why = "Les directives robots/noindex peuvent empêcher l’indexation ou le transfert de signaux."
        fix = [
            "Vérifier si la page doit être indexée (oui/non) selon sa valeur business.",
            "Si elle doit être indexée : retirer noindex/nofollow (meta robots et/ou X-Robots-Tag) et s’assurer qu’elle n’est pas bloquée par robots.txt.",
            "Si elle ne doit pas être indexée : la retirer des sitemaps, limiter le maillage interne, et garder une canonical cohérente.",
        ]
        verify = [
            "Relancer un crawl et vérifier les issues « noindex* / nofollow* / blocked_by_robots ».",
            "Contrôler dans GSC (inspection d’URL) la prise en compte du statut d’indexation.",
        ]

    # Canonicals quality
    elif lk.startswith("canonical_") or "canonical" in lk:
        why = "Des canonicals incohérents peuvent faire indexer la mauvaise URL ou diluer les signaux."
        fix = [
            "S’assurer que la canonical pointe vers une URL 200, indexable, et stable (pas de redirect).",
            "Aligner canonicals, sitemap, et liens internes sur la même URL canonique.",
            "Traiter les variantes (paramètres, http/https, www) via redirections + canonicals.",
        ]
        verify = [
            "Relancer un crawl et vérifier toutes les issues « canonical_* ».",
            "Contrôler un échantillon dans GSC (Google canonical vs user canonical).",
        ]

    # Internal linking / sitemap coverage
    elif lk.startswith("orphan_page") or "incoming_internal_link" in lk or "not_in_sitemap" in lk:
        why = "Les pages orphelines ou peu liées sont difficiles à découvrir et à bien positionner."
        fix = [
            "Ajouter des liens internes contextuels depuis des pages pertinentes (catégories, hubs, contenus proches).",
            "Vérifier la navigation (menu/footer) pour les pages importantes.",
            "Ajouter les pages indexables dans le sitemap XML et le soumettre dans GSC.",
        ]
        verify = [
            "Relancer un crawl et vérifier les issues d’orphelines / faible maillage / sitemap coverage.",
            "Contrôler dans GSC que les pages sont découvertes et indexées.",
        ]

    # Assets / performance
    elif lk.startswith("image_") or lk.startswith("css_") or lk.startswith("javascript_") or "mixed_content" in lk:
        why = "Les ressources cassées, lourdes ou non sécurisées dégradent performance, UX et parfois l’indexation."
        fix = [
            "Corriger les URLs de ressources cassées/redirect (mettre à jour les liens vers la ressource finale).",
            "Optimiser poids des assets (compression, cache, minification, chargement différé).",
            "Éliminer le mixed-content : tout charger en HTTPS.",
        ]
        verify = [
            "Relancer un crawl (y compris resources) et vérifier que les issues assets retombent.",
            "Contrôler CWV/PageSpeed sur un échantillon de pages après déploiement.",
        ]

    # Lang / hreflang
    elif "hreflang" in lk or "lang" in lk:
        why = "Les erreurs de langue/hreflang peuvent créer des mauvaises versions dans les SERP (international)."
        fix = [
            "Définir correctement <html lang=\"…\"> sur chaque page (ex: fr, fr-FR, en, etc.).",
            "Si hreflang est utilisé : s’assurer des paires réciproques, URLs canonicals, codes de langue valides, et absence de redirections/4xx.",
        ]
        verify = [
            "Relancer un crawl et vérifier les issues « lang_* / hreflang_* ».",
            "Valider via l’outil de test hreflang (ou analyse GSC par pays/langue).",
        ]

    # Structured data
    elif "schema" in lk or "structured" in lk:
        why = "Le balisage Schema.org invalide peut empêcher l’affichage d’enrichissements (rich results)."
        fix = [
            "Corriger les erreurs Schema.org (JSON-LD) et s’assurer que les champs requis sont présents.",
            "Valider avec l’outil Rich Results Test / Schema Markup Validator.",
        ]
        verify = [
            "Relancer un crawl et vérifier que les erreurs schema retombent.",
            "Contrôler dans GSC (Améliorations) la disparition des erreurs.",
        ]

    # Social tags
    elif "open_graph" in lk or "twitter_" in lk:
        why = "Les meta OpenGraph/Twitter améliorent l’apparence des partages (social, messageries)."
        fix = [
            "Ajouter og:title, og:description, og:image, og:url (et éventuellement og:type).",
            "Ajouter twitter:card, twitter:title, twitter:description, twitter:image.",
        ]
        verify = [
            "Tester le rendu via les validateurs (Facebook Sharing Debugger, X Card Validator).",
        ]

    else:
        why = f"Issue détectée: {label}. Elle peut impacter SEO/UX selon le contexte."
        fix = [
            "Ouvrir l’issue, identifier les pages impactées et la cause racine (template, contenu, config serveur).",
            "Appliquer une correction systémique (éviter les fixes “au cas par cas” si c’est un pattern).",
        ]
        verify = [
            "Relancer un crawl pour confirmer la baisse du compteur.",
        ]

    return {
        "issue_key": key,
        "label": label,
        "category": category,
        "severity": severity,
        "count": count,
        "priority": priority,
        "effort": effort,
        "why": why,
        "fix": fix,
        "verify": verify,
        "sample_urls": sample_urls,
        "mode": "suggest-only",
    }


def build_fix_suggestions_payload(
    *,
    report: dict[str, Any],
    slug: str,
    timestamp: str,
    site_name: str,
    base_url: str,
) -> dict[str, Any]:
    summary = dash.summarize_report(report)
    issues = summary.get("issues") if isinstance(summary.get("issues"), list) else []

    out: dict[str, Any] = {
        "meta": {
            "version": 1,
            "generated_at": _utc_iso(),
            "slug": slug,
            "timestamp": timestamp,
            "site_name": site_name,
            "base_url": base_url,
        },
        "issues": {},
    }

    issues_map: dict[str, Any] = out["issues"]
    for it in issues:
        if not isinstance(it, dict):
            continue
        key = str(it.get("key") or "").strip()
        if not key:
            continue
        issues_map[key] = suggest_issue_fix(
            issue_key=key,
            label=str(it.get("label") or ""),
            category=str(it.get("category") or ""),
            severity=str(it.get("severity") or ""),
            count=int(it.get("count") or 0),
            report=report,
            site_name=site_name,
            base_url=base_url,
        )

    return out
