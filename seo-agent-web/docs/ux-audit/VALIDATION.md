# Preuves et validation de l’audit

Base : main `44f575c`, 19 octobre 2026. Résultats structurés : [evidence.json](evidence.json).

## Navigateur

- Chromium avec Playwright ; locale française et fuseau Europe/Paris.
- **200 affichages locaux** : 50 scénarios × 4 largeurs (1440, 768, 390, 360 px). Serveur HTTP local éphémère servant l’application réelle, base SQLite neuve, comptes et rapports fictifs ; worker désactivé. Aucune erreur JavaScript relevée, aucun 500. Les 400 des liens de récupération invalides, le 403 administrateur et le 404 sont attendus.
- **32 navigations en production en lecture seule** : 14 pages publiques et 2 accès protégés redirigés vers la connexion, à 1440 et 390 px. Aucun débordement horizontal à ces deux tailles. Les écrans authentifiés de production n’ont pas été inspectés.
- Débordements locaux : récupération de compte (4 écrans), nom long dans le bilan, adresse longue dans le compte et l’équipe, automatisation projet et réglages d’analyse à 360 px. Mesures exactes par taille dans le JSON. Les adresses longues sont des données fictives valides ; elles servent à vérifier la résistance du layout.
- Ouverture/fermeture du menu mobile au clavier, fond inerte et retour du focus ; ouverture de la fenêtre d’ajout et fermeture avec Échap.
- Correction WordPress : aperçu sans écriture, clic explicite d’application, suivi, résultat vérifié après rapport simulé, correction suivante, retour à l’opération et rétablissement explicite. **API WordPress entièrement simulée** : deux écritures dans la simulation (application et rétablissement), aucune sur un site réel. Aucun service IA sollicité.
- Affichage de jobs synthétiques en attente, en cours, échoués et terminés ; affichage de validation de contenu fictive sans clic de fusion.
- Confirmation de sauvegarde : disparition de `.alert.ok` constatée après 5,7 secondes. Ce constat motive le traitement commun des messages proposé dans l’audit.

Ces essais valident les états et interactions décrits, pas chaque variante des 193 routes. Les formulaires d’invitation, suppression, paiement, envoi d’email, fournisseurs OAuth et publication de contenu n’ont pas été soumis à un service réel.

## Tests existants

**139 tests passent**, un avertissement de dépréciation Starlette/httpx, en 33,19 s. Base temporaire isolée, sans réutilisation des données locales. Commande :

```bash
SEO_AGENT_DATA_DIR=$(mktemp -d /tmp/noyaru-ux-tests-XXXXXX) \
DATABASE_URL= SEO_AGENT_DISABLE_WORKER=true \
python -m pytest -q \
  seo-agent-web/tests/test_first_audit_start.py \
  seo-agent-web/tests/test_first_verified_correction.py \
  seo-agent-web/tests/test_cms_integrations.py \
  seo-agent-web/tests/test_client_screen_workflows.py \
  seo-agent-web/tests/test_navigation_mobile.py \
  seo-agent-web/tests/test_project_overview_empty.py \
  seo-agent-web/tests/test_overview_search_connections.py
```

Ce n’est pas l’exécution de la CI complète (audit de dépendances, Bandit et totalité des tests). L’audit ne modifie aucun comportement de l’application.

## Inventaire

`python seo-agent-web/ops/ux_inventory.py` reconstruit l’inventaire depuis l’AST Python et les templates, sans démarrer l’application. Les API et transitions sont recensées, les balises de formulaires et fenêtres conservent les expressions Jinja. Les boutons JavaScript et les règles d’accès demandent une lecture complémentaire, effectuée sur les parcours cités dans le bilan. Ruff F/E9 et `git diff --check` passent pour ce livrable.

## Captures retenues

| Capture | Observation |
| --- | --- |
| [Premier audit mobile](first-audit-390.png) | Amélioration existante à préserver ; domaine unique, solde réel et CTA visible. |
| [Récupération mobile](reset-390.png) | Boutons sans retour à la ligne, page de 458 px pour un écran de 390 px. |
| [Compte mobile](accounts-390.png) | Adresse longue : débordement de page. |
| [Équipe mobile](team-390.png) | Même catégorie de défaut et ton différent. |
| [Correction guidée](first-correction-1440.png) | Aucun lien actif dans le menu. |
| [Aperçu WordPress](cms-preview-1440.png) | Confirmation avant application existante ; rangée dans le menu des réglages. |
| [Résultat vérifié](interaction-success-390.png) | Succès et correction suivante déjà disponibles sur main. |
| [Validation de contenu](content-validation-390.png) | Libellé de fusion à rapprocher de l’attente des contrôles serveur. |

À compléter pendant les lots : recette authentifiée en production ou préproduction, OAuth réel, Stripe en mode test, délivrabilité des emails, permissions réelles CMS, réseaux lents, lecteur d’écran, contrastes calculés et tests utilisateurs débutants. Aucune fusion ni aucun déploiement de production effectués.
