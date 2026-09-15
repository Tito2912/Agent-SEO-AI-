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


def _redirect_3xx_breakdown(report: dict[str, Any]) -> dict[str, int]:
    """Count the crawler's classification of each flagged redirect.

    All zeros for a report predating the evidence contract, which is why the caller keeps a
    generic branch: the advice degrades, it never lies."""
    out = {"self_loop": 0, "multi_loop": 0, "expected": 0, "other": 0}
    items: list[Any] = []
    try:
        block = ((report or {}).get("issues") or {}).get("redirect_3xx") or {}
        evidence = block.get("evidence") or {}
        if evidence.get("kind") == "page_values":
            items = evidence.get("items") or []
    except Exception:
        items = []
    for item in items:
        field = str((item or {}).get("field") or "") if isinstance(item, dict) else ""
        if field.startswith("boucle: "):
            out["self_loop"] += 1
        elif field.startswith("boucle"):
            out["multi_loop"] += 1
        elif field.startswith("canonicalisation"):
            out["expected"] += 1
        else:
            out["other"] += 1
    return out


def _norm_link(u: str) -> str:
    return (u or "").split("#")[0].rstrip("/").lower()


def _under_linked_context(report: dict[str, Any], issue_key: str) -> list[dict[str, Any]]:
    """For each under-linked page: who links to it today, and who plausibly should.

    Candidates are same-language pages that already link to at least two of the target's
    siblings — pages demonstrably about the same section — and do not link to it yet. Ranked by
    how many siblings they cover. Empty when the target has no real neighbourhood, which happens
    and must be said rather than papered over with generic advice.
    """
    pages = report.get("pages")
    pages = pages if isinstance(pages, list) else list((pages or {}).values())
    if not pages:
        return []
    by_url = {_norm_link(p.get("url")): p for p in pages if isinstance(p, dict)}
    out: list[dict[str, Any]] = []
    for target in _sample_urls(report, issue_key, limit=8):
        t = _norm_link(target)
        page = by_url.get(t) or {}
        lang = page.get("lang")
        prefix = t.rsplit("/", 1)[0]
        siblings = {u for u in by_url if u.startswith(prefix + "/") and u != t}
        current: list[str] = []
        candidates: list[tuple[int, str]] = []
        for src in pages:
            if not isinstance(src, dict):
                continue
            u = _norm_link(src.get("url"))
            if u == t:
                continue  # a page linking to itself is not an incoming link
            links = {_norm_link(i.get("target_url"))
                     for i in (src.get("internal_link_items") or [])
                     if isinstance(i, dict) and not i.get("nofollow")}
            if t in links:
                current.append(u)
                continue
            if src.get("lang") == lang:
                shared = len(links & siblings)
                if shared >= 2:
                    candidates.append((shared, u))
        candidates.sort(reverse=True)
        out.append({
            "url": target,
            "title": str(page.get("title") or "")[:120],
            "linked_by": sorted(set(current))[:3],
            "candidates": [u for _n, u in candidates[:3]],
        })
    return out


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

    # La variante d'indexabilite ne change pas le CONSEIL : `missing_h1_indexable` et
    # `missing_h1` se reparent de la meme facon, seule la gravite differe. Sans cette
    # normalisation, seule la forme nue trouvait sa branche et chaque variante tombait dans le
    # repli generique — mesure du 15/09/2026 : 28 familles dans ce cas, dont trois `error`.
    lk = key.lower().removesuffix("_not_indexable").removesuffix("_indexable")

    if lk.startswith("page_has_only_one_dofollow_incoming_internal_link"):
        rows = _under_linked_context(report, key)
        why = ("Ces pages ne reçoivent qu'UN seul lien interne en dofollow. Un lien unique rend "
               "la page fragile : si la page source change, elle devient orpheline, et Google lit "
               "ce maillage comme le signe d'une page secondaire.")
        fix = ["Ajouter un deuxième lien interne contextuel depuis une page qui traite du même "
               "sujet, avec une ancre descriptive (pas « cliquez ici »)."]
        for row in rows:
            src = row["linked_by"][0] if row["linked_by"] else "aucune page identifiée"
            if row["candidates"]:
                fix.append(f"{row['url']} — liée aujourd'hui par {src}. Candidates (même langue, "
                           f"traitant déjà du même dossier) : " + ", ".join(row["candidates"]))
            else:
                fix.append(f"{row['url']} — liée aujourd'hui par {src}. Aucune page candidate : "
                           "cette page n'a pas de voisinage. Le lien doit venir d'un contenu à "
                           "créer, ou d'un index de section qui n'existe pas encore.")
        verify = [
            "Relancer un crawl : la page doit avoir au moins deux pages sources distinctes.",
            "Vérifier que le lien ajouté est en dofollow et porte une ancre descriptive.",
        ]
        return {
            "key": key, "label": label, "category": category, "severity": severity,
            "count": count, "priority": priority, "effort": effort,
            "sample_urls": sample_urls, "why": why, "fix": fix, "verify": verify,
            "auto_fixable": False,
            "auto_fix_note": ("Non corrigé automatiquement : choisir la page source et écrire "
                              "l'ancre est un acte éditorial, et sur les sites mesurés un tiers "
                              "des cas n'a aucune page candidate. L'agent nomme les pages, la "
                              "décision reste au propriétaire."),
        }

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
    elif lk == "redirect_3xx":
        # The crawler classifies every flagged redirect, so name what actually deserves
        # attention instead of implying they are all defects. On a healthy site these ARE the
        # domain's own http→https / www canonicalisation and the right advice is "nothing to do".
        b = _redirect_3xx_breakdown(report)
        if b["self_loop"] or b["multi_loop"]:
            _n = b["self_loop"] + b["multi_loop"]
            why = (
                ("Une de ces redirections forme une boucle : " if _n == 1
                 else f"{_n} de ces redirections forment une boucle : ")
                + "l'URL ne parvient jamais à une page, elle est donc inatteignable pour Google."
            )
            fix = [
                "Casser la boucle dans la config de redirection (règle qui renvoie une URL vers elle-même).",
                "Vérifier les règles http↔https, www↔non-www et slash final, souvent en conflit entre elles.",
            ]
            if b["expected"]:
                fix.append(
                    ("Laisser l'autre telle quelle : c'est ta canonicalisation volontaire, elle est correcte."
                     if b["expected"] == 1 else
                     f"Laisser les {b['expected']} autres telles quelles : c'est ta canonicalisation "
                     "volontaire, elles sont correctes.")
                )
            verify = ["Après déploiement, appeler l'URL : elle doit répondre 200 sans en-tête Location."]
        elif b["expected"] and not b["other"]:
            why = (
                "Ces redirections sont la canonicalisation volontaire du site (http→https, www→apex, "
                "suppression du .html). Ce ne sont pas des défauts : Ahrefs les compte, mais elles font "
                "leur travail."
            )
            fix = [
                "Ne rien changer dans la config de redirection.",
                "Vérifier plutôt que tes liens internes, canonicals et sitemaps pointent directement "
                "vers l'URL finale, pour éviter un saut inutile à chaque visite.",
            ]
            verify = ["Le compte ne baissera pas et c'est normal : ces redirections doivent exister."]
        else:
            why = (
                "Des URLs du site répondent par une redirection. Certaines sont voulues "
                "(canonicalisation), d'autres non — la distinction se fait au cas par cas."
            )
            fix = [
                "Pour chaque URL, vérifier si la redirection est intentionnelle (http→https, www, .html).",
                "Corriger les liens internes qui pointent vers la source plutôt que vers la destination.",
            ]
            verify = ["Relancer un crawl et comparer le détail des URLs listées."]
    elif lk == "sitemap_3xx_redirect":
        why = (
            "Le sitemap liste des URLs qui redirigent. Google suit la redirection mais le sitemap "
            "envoie un signal contradictoire : il déclare canonique une URL qui n'est pas la finale."
        )
        fix = [
            "Remplacer chaque <loc> par la destination finale en 200.",
            "Ne pas se contenter d'ajouter la bonne URL : retirer l'ancienne, sinon les deux coexistent.",
        ]
        verify = ["Relancer un crawl et vérifier que « sitemap_3xx_redirect » retombe à 0."]
    elif lk == "certificate_expiration":
        why = (
            "Le certificat TLS approche de son expiration. Une fois expiré, les navigateurs bloquent "
            "l'accès au site et le crawl s'arrête net — c'est une panne totale, pas une dégradation."
        )
        fix = [
            "Vérifier que le renouvellement automatique est actif chez l'hébergeur (Let's Encrypt, CDN).",
            "S'il est manuel, renouveler maintenant et poser un rappel avant la prochaine échéance.",
        ]
        verify = ["Contrôler la date d'expiration du certificat après renouvellement."]
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

    # ── Ressources et liens internes cassés ────────────────────────────────────────────────
    # Mesure du 15/09/2026 : 63 des 157 familles montrées au client tombaient dans le repli
    # générique ci-dessous — « Issue détectée … peut impacter SEO/UX selon le contexte », qui
    # n'apprend rien. Onze d'entre elles étaient classées `error`. Ne pas corriger une anomalie
    # est défendable ; ne rien savoir en dire ne l'est pas.
    elif lk in {"broken_internal_links", "links_to_404_page",
                "page_has_links_to_broken_page", "page_has_links_to_broken_page_links",
                "broken_internal_javascript_and_css_files", "page_has_broken_image",
                "page_has_broken_javascript", "page_has_broken_javascript_links"}:
        why = ("Ces pages pointent vers une ressource interne qui ne répond plus. Le coût est "
               "double : le visiteur tombe sur une erreur, et le budget de crawl part dans une "
               "impasse au lieu d'alimenter les pages qui comptent. L'agent ne le corrige pas "
               "seul : choisir la cible de remplacement est une décision éditoriale, et "
               "supprimer un lien n'est jamais anodin.")
        fix = [
            "Retrouver la cible d'origine : une URL renommée se retrouve presque toujours dans "
            "l'historique du dépôt ou dans un ancien sitemap.",
            "Repointer le lien vers l'URL FINALE, pas vers une redirection qui y mène.",
            "Si la cible n'existe plus, retirer le lien plutôt que de le laisser mort : un lien "
            "vers une page supprimée ne transmet rien.",
        ]
        verify = ["Relancer un crawl : le compteur doit tomber à zéro sans qu'une redirection "
                  "apparaisse à la place."]

    elif lk in {"broken_external_images", "broken_external_js_css",
                "disallowed_external_resources"}:
        why = ("La ressource est hébergée par un tiers et ne répond plus, ou son hôte interdit "
               "son chargement. Vous ne contrôlez pas cette adresse : personne ne peut garantir "
               "qu'elle revienne, c'est pourquoi l'agent n'y touche pas.")
        fix = [
            "Rapatrier la ressource sur votre domaine quand la licence le permet : c'est la "
            "seule façon d'en maîtriser la disponibilité.",
            "Sinon, retirer la référence ou la remplacer par un équivalent que vous hébergez.",
        ]
        verify = ["Recharger la page et vérifier qu'aucune requête ne part plus vers l'hôte "
                  "fautif (onglet Réseau du navigateur)."]

    elif lk in {"broken_redirect", "meta_refresh_redirect", "redirect_3xx_links",
                "page_has_links_to_redirect_links", "permanent_redirects", "redirect_302"}:
        why = ("Une redirection s'intercale entre le lien et sa destination. Chaque saut retarde "
               "l'affichage, dilue le signal transmis par le lien, et une redirection cassée ne "
               "mène nulle part du tout. Une 302 annonce un déplacement TEMPORAIRE : Google "
               "conserve alors l'ancienne URL dans son index.")
        fix = [
            "Faire pointer les liens du site directement sur la destination finale : une "
            "redirection sert aux visiteurs venus d'ailleurs, pas à votre propre maillage.",
            "Remplacer une 302 par une 301 dès que le déplacement est définitif.",
            "Une redirection par meta refresh n'est comprise ni par tous les robots ni par les "
            "lecteurs d'écran : la remplacer par une vraie redirection HTTP.",
        ]
        verify = ["Suivre chaque lien avec `curl -I` : la première réponse doit être un 200."]

    # ── Sitemap et robots.txt ──────────────────────────────────────────────────────────────
    elif lk in {"sitemap_xml_not_found", "sitemap_invalid_format", "sitemap_file_too_large",
                "sitemap_http_urls_for_https", "sitemap_not_in_robots",
                "incorrect_pages_found_in_sitemap_xml", "orphaned_sitemap_pages",
                "page_in_multiple_sitemaps", "sitemap_page_timed_out"}:
        why = ("Le sitemap est la liste que vous SOUMETTEZ aux moteurs : absent, illisible ou "
               "peuplé d'URL que le site ne sert pas, il devient au mieux inutile, au pire une "
               "source de contradictions avec ce que le crawl découvre.")
        fix = [
            "Servir un `sitemap.xml` valide à la racine et le déclarer dans `robots.txt` "
            "(ligne `Sitemap: https://…/sitemap.xml`).",
            "N'y lister que des URL indexables, en https, sans redirection ni 4xx, et chacune "
            "dans un seul fichier.",
            "Au-delà de 50 000 URL ou 50 Mo, découper en plusieurs fichiers reliés par un index.",
        ]
        verify = ["Soumettre le sitemap dans la Search Console : elle doit annoncer zéro erreur "
                  "de lecture et un nombre d'URL découvertes cohérent avec le crawl."]

    elif lk in {"robots_txt_not_found", "robots_invalid_format"}:
        why = ("Sans `robots.txt` lisible, chaque robot applique ses propres règles par défaut. "
               "Un fichier mal formé est pire qu'absent : une directive inattendue peut "
               "désindexer tout un site, et c'est arrivé à des sites bien plus gros que le vôtre.")
        fix = [
            "Servir un `robots.txt` à la racine, en 200, avec au minimum un groupe "
            "`User-agent: *` et la ligne `Sitemap:`.",
            "Vérifier qu'aucun `Disallow: /` ne traîne — il bloque l'intégralité du site.",
        ]
        verify = ["Ouvrir `https://" + (domain or "votre-domaine") + "/robots.txt` : la réponse "
                  "doit être 200, en text/plain, et le testeur de la Search Console sans erreur."]

    # ── Serveur et transport : hors du dépôt, donc hors de portée d'un patch ───────────────
    elif lk in {"certificate_name_mismatch", "insecure_cipher", "old_tls_version", "no_hsts",
                "dns_resolution_issue"}:
        why = ("Ces réglages vivent chez votre hébergeur ou votre CDN, pas dans le dépôt : "
               "aucun changement de code ne peut les corriger. Ils comptent malgré tout — un "
               "certificat au mauvais nom déclenche un avertissement de sécurité en pleine page, "
               "et une résolution DNS qui échoue rend le site invisible.")
        fix = [
            "Certificat au mauvais nom : le réémettre en couvrant le domaine RÉELLEMENT servi, "
            "variante www comprise.",
            "TLS ancien ou suite de chiffrement faible : n'accepter que TLS 1.2 et 1.3 dans la "
            "configuration de l'hôte.",
            "HSTS : ajouter l'en-tête `Strict-Transport-Security` une fois le https stable "
            "partout — jamais avant, sa durée de vie le rend difficile à reprendre.",
        ]
        verify = ["Relancer un test SSL Labs sur le domaine : viser A, sans avertissement de nom."]

    # ── Performance et volume de contenu : mesures, pas anomalies de code ──────────────────
    elif lk in {"slow_page", "timed_out", "timed_out_links", "sitemap_page_timed_out",
                "low_text_to_html_ratio", "low_word_count", "document_uses_plugins",
                "pages_have_high_ai_content_levels"} or lk.startswith("pages_with_poor_"):
        why = ("Ce sont des MESURES, pas des défauts localisables dans un fichier : un temps de "
               "réponse, un volume de texte, un score d'expérience. Aucun patch mécanique ne les "
               "déplace — leur cause est un hébergement, un poids d'image, une architecture de "
               "page ou un contenu à écrire.")
        fix = [
            "Temps de réponse et délais d'attente : mesurer d'abord le temps SERVEUR seul. "
            "Au-delà de 600 ms, le problème est l'hébergement ou la base, pas le front.",
            "Core Web Vitals : traiter la plus grosse image de la page (LCP), réserver les "
            "dimensions des médias (CLS), alléger le JavaScript au chargement (INP, TBT).",
            "Trop peu de texte : la question n'est pas le nombre de mots mais ce que la page "
            "apporte qu'une autre n'apporte pas. Si la réponse est « rien », la fusionner.",
        ]
        verify = ["Reprendre la mesure sur les MÊMES URL après correction : ces familles se "
                  "jugent sur un avant/après chiffré, jamais sur une impression."]

    # ── Rapports Search Console / Bing : ce qu'un tiers OBSERVE de votre site ──────────────
    elif lk.startswith("gsc_") or lk.startswith("bing_") or lk == "pages_to_submit_to_indexnow":
        why = ("Ces lignes ne viennent pas du crawl de votre site mais de ce que Google ou Bing "
               "en RAPPORTENT. Elles n'ont donc pas de correctif dans le dépôt : ce sont des "
               "signaux à lire, parfois le symptôme d'une anomalie listée ailleurs dans ce "
               "rapport.")
        fix = [
            "Confronter chaque URL signalée au reste du rapport : une page « découverte, non "
            "indexée » est souvent déjà listée comme orpheline, en noindex ou hors sitemap.",
            "Traiter la cause dans le dépôt, puis demander une réindexation — jamais l'inverse.",
            "Pages à soumettre à IndexNow : c'est une opportunité de rapidité, pas un défaut.",
        ]
        verify = ["Revenir à la Search Console une à deux semaines après la correction : c'est "
                  "son propre délai de réexploration qui fait foi, pas un nouveau crawl."]

    # ── Sémantique des liens ──────────────────────────────────────────────────────────────
    elif lk in {"links_with_no_anchor_text", "page_has_no_outgoing_links",
                "page_has_nofollow_outgoing_internal_links",
                "http_page_has_internal_links_to_https", "more_than_three_parameters_in_url",
                "page_and_serp_titles_do_not_match", "llms_txt_not_found"}:
        why = ("Le maillage interne dit aux moteurs ce qui compte sur le site et de quoi chaque "
               "page traite. Une ancre vide ne transmet aucun sujet, un `nofollow` interne coupe "
               "volontairement la transmission, et une page sans lien sortant est un cul-de-sac.")
        fix = [
            "Ancre vide : donner au lien un texte qui décrit la page d'arrivée — une image "
            "seule comme lien doit porter un `alt` qui joue ce rôle.",
            "`nofollow` sur un lien INTERNE : le retirer, sauf raison explicite. Il n'économise "
            "pas de budget de crawl, c'est une idée reçue de longue date.",
            "URL à plus de trois paramètres : préférer une URL lisible et stable, et déclarer "
            "un canonical vers elle depuis les variantes paramétrées.",
            "`llms.txt` : fichier encore facultatif, qui décrit le site aux agents "
            "conversationnels. À considérer, pas à traiter comme un défaut.",
        ]
        verify = ["Relancer un crawl : chaque page visée doit avoir au moins un lien entrant en "
                  "dofollow et un lien sortant interne."]

    # ── Familles que l'agent CORRIGE : dire quand meme ce qu'il fait ──────────────────────
    # Un correctif automatique n'exempte pas d'explication. Le proprietaire relit une PR : il
    # doit pouvoir juger le changement, pas seulement l'accepter.
    elif lk in {"https_page_has_internal_links_to_http", "https_page_links_to_http_css",
                "https_page_links_to_http_image", "https_page_links_to_http_javascript"}:
        why = ("Une page servie en https appelle une ressource ou un lien en http. Le navigateur "
               "bloque ou signale ce contenu mixte, et l'internaute voit un avertissement de "
               "sécurité sur une page qui, elle, est parfaitement sécurisée.")
        fix = ["Réécrire ces URL en https quand l'hôte le sert déjà — c'est le cas ici, "
               "puisque la même adresse répond en https.",
               "L'agent applique cette réécriture mécaniquement : elle ne change que le schéma, "
               "jamais la destination."]
        verify = ["Recharger la page : plus aucun avertissement de contenu mixte dans la console."]

    elif lk in {"page_has_redirected_css", "page_has_redirected_image",
                "page_has_redirected_javascript"}:
        why = ("La page référence une ressource qui répond par une redirection. Chaque image, "
               "script ou feuille de style ainsi appelée coûte un aller-retour de plus avant de "
               "s'afficher — sur une page qui en compte plusieurs, cela se voit.")
        fix = ["Remplacer l'adresse par sa destination FINALE, celle que la redirection désigne.",
               "L'agent connaît cette destination : elle est mesurée pendant le crawl, il n'y a "
               "donc rien à deviner."]
        verify = ["`curl -I` sur chaque ressource : la première réponse doit être un 200."]

    elif lk == "page_has_links_to_redirect":
        why = ("Les liens de la page passent par une redirection avant d'atteindre leur "
               "destination. Une redirection sert aux visiteurs venus d'ailleurs ; à l'intérieur "
               "du site, elle ne fait qu'ajouter un saut et diluer le signal du lien.")
        fix = ["Faire pointer chaque lien interne directement sur l'URL finale."]
        verify = ["Relancer un crawl : aucun lien interne ne doit plus figurer dans cette famille."]

    elif lk == "double_slash_in_url":
        why = ("Une URL du site contient une double barre (`/blog//article`). Le serveur la sert "
               "souvent malgré tout, mais les moteurs y voient une adresse DIFFÉRENTE de la "
               "version propre : deux URL pour une page, et le signal se partage entre elles.")
        fix = ["Corriger le lien à la source — c'est presque toujours une concaténation où un "
               "segment porte déjà sa barre finale."]
        verify = ["Relancer un crawl : la variante à double barre ne doit plus être découverte."]

    elif lk == "missing_alt_text":
        why = ("Des images n'ont pas d'attribut `alt`. Il sert d'abord aux personnes qui "
               "naviguent au lecteur d'écran, et accessoirement aux moteurs, qui n'ont que ce "
               "texte pour savoir ce que l'image montre.")
        fix = ["Décrire ce que l'image APPORTE à la page, en une courte phrase.",
               "Une image purement décorative prend un `alt` VIDE (`alt=\"\"`) : c'est la façon "
               "correcte de dire au lecteur d'écran de l'ignorer."]
        verify = ["Relancer un crawl : plus aucune image sans attribut `alt` sur les pages visées."]

    elif lk in {"multiple_title_tags", "multiple_meta_description_tags"}:
        why = ("La page déclare plusieurs fois la même balise. Les moteurs n'en retiennent "
               "qu'une, et rien ne garantit que ce soit celle que vous vouliez : c'est un choix "
               "que vous laissez faire à leur place.")
        fix = ["Ne garder qu'une seule occurrence, celle qui décrit vraiment la page.",
               "Chercher la cause : le plus souvent un gabarit partagé qui pose déjà la balise "
               "que la page repose ensuite."]
        verify = ["Afficher le source de la page servie : une seule occurrence de la balise."]

    elif lk == "sitemap_noindex_page":
        why = ("Le sitemap propose aux moteurs une page qui leur interdit ensuite de l'indexer. "
               "Les deux signaux se contredisent : vous demandez l'exploration d'une page que "
               "vous refusez de voir indexée.")
        fix = ["Trancher : soit la page mérite d'être indexée et le `noindex` part, soit elle "
               "ne le mérite pas et elle sort du sitemap.",
               "Dans le doute, la sortir du sitemap : c'est le geste réversible des deux."]
        verify = ["Relancer un crawl : aucune URL du sitemap ne doit porter de `noindex`."]

    elif lk == "viewport_not_set":
        why = ("Sans balise `viewport`, un mobile affiche la page comme un écran de bureau "
               "réduit : texte minuscule, zoom obligatoire. Google indexe d'abord la version "
               "mobile, donc c'est cette version-là qu'il juge.")
        fix = ["Ajouter `<meta name=\"viewport\" content=\"width=device-width, "
               "initial-scale=1\">` dans le `<head>`, idéalement dans le gabarit partagé."]
        verify = ["Ouvrir la page sur un mobile ou en mode responsive : le texte doit être "
                  "lisible sans zoomer."]

    # ── Accès des robots d'IA ─────────────────────────────────────────────────────────────
    # La distinction RECHERCHE / ENTRAÎNEMENT décide du conseil, et se tromper de famille
    # reviendrait à pousser quelqu'un à ouvrir ses contenus à l'entraînement « pour le SEO ».
    elif lk in {"indexable_page_blocked_from_all_ai_search_bots",
                "indexable_page_blocked_from_some_ai_search_bots"}:
        why = ("Votre `robots.txt` interdit à des robots de RECHERCHE par IA d'aller lire ces "
               "pages. Ces robots-là ne collectent pas un corpus : ils vont chercher une page "
               "pour répondre à la question de quelqu'un, et citent leur source. Les bloquer "
               "revient à disparaître de ces réponses — et de leurs visites.")
        fix = [
            "Ouvrir les agents de recherche : `OAI-SearchBot`, `ChatGPT-User`, `PerplexityBot`, "
            "`Perplexity-User`, `Claude-User`. Un `Disallow: /` sur l'un d'eux suffit à couper "
            "la citation.",
            "Vérifier qu'aucune règle large — un `Disallow: /` sous `User-agent: *` — ne les "
            "attrape par ricochet.",
            "Décider de l'ENTRAÎNEMENT séparément : bloquer `GPTBot` ou `CCBot` est un choix "
            "légitime, sans effet sur la recherche. Les deux décisions sont indépendantes.",
        ]
        verify = ["Relancer un crawl : ces pages ne doivent plus figurer dans la famille.",
                  "Contrôler la règle appliquée agent par agent, pas seulement `User-agent: *`."]

    elif lk == "slow_server_response_for_ai_crawlers":
        why = ("Ces pages mettent trop longtemps à répondre pour un robot d'IA. Ces agents "
               "n'exécutent PAS le JavaScript et attendent moins longtemps qu'un navigateur : "
               "ce qu'un visiteur trouve simplement lent, eux l'abandonnent. La mesure porte sur "
               "le temps jusqu'au HTML complet, sans rendu — leur point de vue exact.")
        fix = [
            "Mesurer d'abord le temps SERVEUR seul. Au-delà de 600 ms, la cause est "
            "l'hébergement ou la base de données, et rien dans la page ne la corrigera.",
            "Servir un HTML déjà complet : ces agents ne verront jamais ce que le JavaScript "
            "ajoute après coup.",
            "Mettre en cache les pages qui ne changent pas à chaque visite.",
        ]
        verify = ["Comparer avant/après sur les MÊMES URL : cette famille se juge sur un écart "
                  "chiffré, jamais sur une impression."]

    elif lk == "similar_ai_generated_content":
        why = ("Ces pages se ressemblent beaucoup — mesuré sur leur texte, par comparaison de "
               "suites de mots. Des pages quasi identiques se font concurrence entre elles : les "
               "moteurs en choisissent une et ignorent les autres. À savoir : le caractère "
               "« généré par une IA » n'est PAS mesuré ici, faute d'un classifieur ; seule la "
               "ressemblance l'est, parmi des pages longues au format article.")
        fix = [
            "Décider pour chaque groupe : fusionner en une page qui fait autorité, ou donner à "
            "chacune un angle réellement différent.",
            "Si les pages doivent coexister (variantes locales, par exemple), déclarer un "
            "canonical vers celle qui fait référence.",
            "Un plan identique rempli par des synonymes reste du contenu identique aux yeux "
            "d'un moteur.",
        ]
        verify = ["Relancer un crawl : les pages fusionnées ou différenciées doivent sortir de "
                  "cette famille."]

    elif lk == "inconsistent_ai_training_bot_policy":
        why = ("Votre `robots.txt` autorise certains robots d'entraînement et en bloque "
               "d'autres. Ce n'est pas une faute en soi — c'en est une seulement si ce n'est pas "
               "voulu, ce qui est le cas le plus fréquent : une règle recopiée d'un modèle, un "
               "agent ajouté un jour et les suivants oubliés. En pratique, un éditeur apprend "
               "de vos contenus et son concurrent ne le peut pas.")
        fix = [
            "Choisir une position et l'appliquer à tous : tout ouvrir, ou tout fermer.",
            "Si le mélange est délibéré — un partenariat, une licence — l'écrire en commentaire "
            "dans le fichier, pour que le prochain qui l'ouvre ne le « corrige » pas.",
            "Ne pas confondre avec les robots de RECHERCHE : les fermer coûte des visites, "
            "fermer l'entraînement n'en coûte aucune.",
        ]
        verify = ["Relire `robots.txt` agent par agent et vérifier que la liste correspond à la "
                  "décision prise, pas à son historique."]

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
