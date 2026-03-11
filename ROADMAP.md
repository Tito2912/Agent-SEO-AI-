# Roadmap — Agent SEO IA (SaaS)

Objectif V1: une web app **professionnelle & monétisable** pour **propriétaires de sites** (tous types: WordPress/Shopify/Webflow/sites codés), avec **Audit + Suggestions IA + Automatisation**.

## Lot 1 — Stripe + quotas + paywall (monétisation)
- [ ] Définir les plans V1 (Solo/Pro) + limites (sites, pages/mois, IA/mois, automation/mois)
- [ ] Implémenter Stripe Billing
  - [ ] Checkout (subscription)
  - [ ] Customer Portal (manage/cancel/update)
  - [ ] Webhook (signature + traitement idempotent)
  - [ ] Stockage DB: customer/subscription/status/période/plan
- [ ] Paywall côté serveur (source de vérité)
  - [ ] Bloquer/limiter crawl au-delà du quota
  - [ ] Bloquer/limiter assistant IA au-delà du quota
  - [ ] Bloquer/limiter automation au-delà du quota (quand dispo)
- [ ] Tracking d’usage (DB) par période (mensuel) + affichage dans l’UI
- [ ] UI “Abonnement”
  - [ ] Écran plan actuel + usage
  - [ ] Boutons Upgrade/Manage + états (trialing/active/past_due/canceled)
  - [ ] Lien Pricing (interne au moins) + CTA “S’abonner”
- [ ] Emails transactionnels minimum
  - [ ] Reset password (si pas encore)
  - [ ] Emails facture/abonnement: gérés par Stripe + lien portail
- [ ] Migrations DB (Alembic) pour éviter les casse-prod sur évolutions futures
- [ ] Retirer/cacher les sections non finies qui font “amateur”
  - [ ] Masquer intégrations non fonctionnelles
  - [ ] Garder “beta basic auth” seulement en staging (webhooks Stripe doivent bypass)

## Lot 2 — Background jobs + fiabilité (scalabilité)
- [ ] Sortir les crawls/IA/export du process web (queue + worker)
- [ ] Persist job state en DB (queued/running/done/failed + progress)
- [ ] Reprise après crash + retry contrôlé + timeouts
- [ ] Storage externe pour artefacts (reports/logs) + rétention
- [ ] Observabilité
  - [ ] Sentry (exceptions)
  - [ ] Métriques (temps crawl, pages/min, taux d’échec)
- [ ] Durcir la sécurité
  - [ ] Rate-limit (login + endpoints coûteux)
  - [ ] CSRF pour POST HTML
  - [ ] Audit log minimal (actions sensibles)

## Lot 3 — “Fix Pack” universel (tous types de sites)
- [ ] Générer un pack téléchargeable par audit:
  - [ ] CSV redirections (mapping)
  - [ ] CSV titres/meta/h1/canonicals
  - [ ] Robots.txt/sitemap proposés
  - [ ] Snippets schema.org (JSON-LD)
  - [ ] “How-to apply” (WordPress/Shopify/Webflow/HTML)
- [ ] “Top 3 actions” auto (Impact/effort) + templates de recommandations IA
- [ ] Diff entre 2 crawls (améliorations/régressions) + checklist

## Lot 4 — Automatisation “Fix → PR” via GitHub (sites codés)
- [ ] Intégration GitHub (v1: PAT, v2: GitHub App)
- [ ] Associer un repo à un projet + détection stack (Next/Astro/Hugo…)
- [ ] Générer patch + Pull Request + résumé (avant/après, risques)
- [ ] Gate “1-click apply” (protéger, validations, rollback)
- [ ] Déploiement: s’appuyer sur la CI existante (Netlify/Vercel/Render) après merge

## Lot 5 — Connecteurs “auto-fix” (plus tard)
- [ ] WordPress (plugin) — appliquer metas/redirections/sitemap
- [ ] Shopify (app) — appliquer templates/meta/redirects
- [ ] Webflow (app) — appliquer metas/redirects

## Qualité produit (en continu)
- [ ] UX onboarding: ajout site → audit → 3 priorités → plan d’action
- [ ] Scoring crédible + priorisation (Impact / Effort) partout
- [ ] Exports (CSV/PDF) propres + marque blanche éventuelle
- [ ] Pages business: Pricing, CGU, Privacy, Support, Changelog
- [ ] Design system cohérent (typo, espacements, composants, micro-interactions)

