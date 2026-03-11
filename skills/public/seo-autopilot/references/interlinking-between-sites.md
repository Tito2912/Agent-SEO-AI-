# Maillage inter-sites (référence)

Objectif: créer un maillage **entre tes propres sites** (pas de backlinks externes), sans schéma à risque.

## Principes

- Lien **uniquement si pertinent** pour l’utilisateur (ressource complémentaire, outil, comparaison, guide).
- Éviter les liens “sitewide” (footer/blogroll) qui relient tout à tout.
- Limiter les ancres exact-match; préférer ancres naturelles (titre, entité, bénéfice).
- Privilégier une logique de **clusters** (thèmes) plutôt qu’un maillage global.
- Ne pas lier vers des pages `noindex` / en erreur / redirigées.

## Workflow

1. Crawls des sites (via `scripts/seo_audit.py` ou `scripts/seo_autopilot.py`)
2. Générer un plan de liens proposés:
   - `python3 scripts/interlinking_plan.py --find-in seo-runs --output-dir seo-runs/_global/<timestamp>`
3. Appliquer manuellement ou automatiser par template (selon stack: Markdown/MDX, HTML, CMS).

## Sorties

- `interlinking-plan.md`: suggestions lisibles
- `interlinking-plan.csv`: exploitable en bulk

