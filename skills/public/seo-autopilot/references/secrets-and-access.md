# Accès & secrets (référence)

Objectif: donner “full access” à l’agent sans compromettre la sécurité.

## Règles

- Ne jamais committer de secrets (mots de passe, tokens, clés API).
- Préférer des accès **scopés** (least privilege), rotables, et traçables.
- Centraliser les secrets dans des variables d’environnement (`.env` local, CI/CD secrets, vault).
- Journaliser ce que l’agent fait (commandes, PR, changements CMS).

## Types d’accès typiques

- **Code**: accès au repo (Git) + droits pour pousser une branche/PR.
- **Déploiement**: accès CI/CD (GitHub Actions, Vercel, Cloudflare, serveur) via tokens.
- **Search Console**: accès propriété (idéalement via compte dédié / service account si possible).
- **Analytics (GA4)**: lecture (et éventuellement annotations) via accès dédié.
- **CMS**: compte admin ou clé API (WordPress/Shopify/Presta) selon la stack.
- **Email/outreach** (backlinks): uniquement sur des listes légitimes/partenaires; éviter l’envoi de masse.

## Bonnes pratiques opérationnelles

- Créer un compte “agent-seo@…” dédié (pas ton compte perso).
- Activer 2FA partout; utiliser des tokens/app passwords si requis.
- Définir un processus de rollback (ex: revert commit + redeploy).

