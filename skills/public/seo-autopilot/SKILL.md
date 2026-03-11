---
name: seo-autopilot
description: "Agent SEO autopilot (full-process) pour gérer le référencement de sites: audit technique/on-page, analyse Google Search Console (exports CSV), génération de backlog, implémentation des corrections (code/CMS), déploiement (ex: Netlify), suivi, et maillage inter-sites entre tes propres domaines. Utiliser quand tu veux que Codex prenne en charge le SEO de A à Z sur un ou plusieurs sites."
---

# SEO Autopilot

## Overview

Piloter le SEO d’un ou plusieurs sites de bout en bout: auditer, prioriser, corriger (tech + contenu), déployer, puis mesurer et itérer.

## Workflow (A → Z)

1. Cadrer (sites, objectifs, accès)
2. Baseline (crawl + GSC)
3. Backlog priorisé (impact/effort)
4. Corrections (code/CMS/infra) + validation
5. Déploiement + rollback possible
6. Suivi (GSC/GA4) et itération

## Onboarding (accès “full”)

- Liste des sites + type (vitrine, e-commerce, blog, local) + priorité business
- Pays/langues ciblés + saisonnalité
- Tech/CMS (WordPress/Shopify/Presta/custom) + accès (repo, admin, hébergeur)
- Accès données (GSC, GA4, logs serveur) ou exports CSV disponibles
- Contraintes: white-hat only (pas de spam / schémas de liens)

Référence sécurité: `references/secrets-and-access.md`.

## Setup (une fois)

1. Copier `assets/seo-autopilot.yml.example` → `seo-autopilot.yml` (dans ton projet)
2. Copier `assets/.env.example` → `.env` et renseigner les chemins/accès (local uniquement)
3. (Multi-sites) Optionnel: mettre ton CSV de domaines et renseigner `inventory.domains_csv` dans `seo-autopilot.yml`
4. (Netlify) Optionnel: définir `NETLIFY_TOKEN` dans `.env` pour mapper domaines → sites Netlify (voir `references/netlify.md`)
5. (Google) Optionnel: définir `GOOGLE_APPLICATION_CREDENTIALS` dans `.env` pour récupérer GSC via API (voir `references/google.md`)

## Quick start (autopilot)

- Lancer la baseline + backlog: `python3 scripts/seo_autopilot.py --config seo-autopilot.yml`
  - Sortie: `seo-runs/<site>/<timestamp>/` (audit + GSC + `backlog.md`)
  - (Si plusieurs sites) Maillage inter-sites: `seo-runs/_global/<timestamp>/interlinking-plan.md`
- (Optionnel) Audit seul: `python3 scripts/seo_audit.py https://example.com --max-pages 300`
- (Optionnel) GSC seul: `python3 scripts/gsc_analyze_csv.py ./gsc-export.csv --min-impressions 200`

## Construire le backlog

- `scripts/seo_autopilot.py` génère un `backlog.md` initial.
- Convertir/affiner en tâches testables (impact/effort + preuve + critères d’acceptation).
- Grouper par lots “faible risque” (titles/meta, H1, liens internes) avant les changements structurels.
- Format de référence: `references/task-template.md`.

## Exécution (corrections + déploiement)

- Prioriser: erreurs 4xx/5xx + indexation + pages business + pages à fort potentiel CTR (GSC).
- Appliquer les corrections (code/CMS): titles/meta, canonicals, maillage interne, schema, sitemaps/robots, perf.
- Déployer via les commandes définies dans `seo-autopilot.yml`:
  - Par défaut, le déploiement est **désactivé**; activer `autopilot.mode=execute` + `autopilot.auto_deploy=true`, puis lancer avec `--execute`.
  - Override ponctuel (sans modifier la config): `python3 scripts/seo_autopilot.py --mode execute --auto-deploy --execute`
- Mesurer après chaque lot (2–4 semaines selon volume) et documenter le résultat.

## Maillage inter-sites (tes domaines uniquement)

- Générer un plan de liens (si besoin en standalone): `python3 scripts/interlinking_plan.py --find-in seo-runs --output-dir seo-runs/_global/<timestamp>`
- Bonnes pratiques: `references/interlinking-between-sites.md`.
- Import domaines (si besoin): `python3 scripts/domains_csv_extract.py --csv domain.csv --output-txt domains.txt`

## Resources

- `scripts/seo_audit.py`: crawl + extraction SEO + rapports `report.md`/`report.json`
- `scripts/gsc_analyze_csv.py`: analyse exports CSV GSC (opportunités CTR + “push page 1”)
- `scripts/gsc_fetch.py`: récupérer des données GSC via API (service account) → CSV
- `scripts/seo_autopilot.py`: orchestrateur (baseline + backlog + déploiement optionnel)
- `scripts/domains_csv_extract.py`: extraire une liste propre de domaines depuis un CSV
- `scripts/netlify_map_domains.py`: mapper domaines → sites Netlify + infos repo/build (si dispo)
- `scripts/interlinking_plan.py`: plan de maillage inter-sites (MD + CSV)
- `references/checklists.md`: checklists techniques/on-page/contenu/off-page
- `references/task-template.md`: format de tâche SEO testable
- `references/secrets-and-access.md`: accès/secrets (full access sans fuite)
- `references/netlify.md`: setup Netlify (token + mapping)
- `references/git.md`: connecter les sites à Git
- `references/google.md`: connecter GSC/GA4 (service account)
- `references/interlinking-between-sites.md`: guidelines de maillage inter-sites
