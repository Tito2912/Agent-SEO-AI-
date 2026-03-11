# Git (référence)

Objectif: que chaque site ait un **repo Git** pour que l’agent puisse corriger (HTML) puis déployer via Netlify.

## Choisir une structure

- **1 repo par site** (simple, clair) — recommandé si les sites sont indépendants.
- **Monorepo** (un repo avec `sites/<domaine>/...`) — recommandé si tu veux factoriser et piloter en bulk (Netlify gère les monorepos).

## Cas 1 — Tes fichiers existent déjà en local

1. Dans le dossier du site:
   - `git init`
   - `git add .`
   - `git commit -m "Initial commit"`
2. Créer un repo GitHub (ou GitLab) vide
3. Ajouter le remote et pousser:
   - `git remote add origin <URL_DU_REPO>`
   - `git push -u origin main`

## Cas 2 — Tu as déployé sur Netlify “drag & drop” (pas de repo)

1. Netlify → ton site → **Deploys**
2. Ouvrir le dernier deploy → **Download deploy** (zip)
3. Dézipper en local → puis suivre “Cas 1” pour créer/pousser le repo

## Lier un site Netlify à Git (UI)

Netlify → ton site → **Site configuration** → **Build & deploy** → **Continuous deployment**

- Si le site n’est pas lié:
  - **Link site to Git repository**
  - Choisir le provider (GitHub/GitLab/Bitbucket)
  - Sélectionner repo + branche (ex: `main`)
- Réglages typiques (site HTML statique):
  - Build command: vide (ou `npm run build` si tu as un build)
  - Publish directory: `.` (ou `dist/`, `public/`, selon ton projet)

Après ça: chaque `git push` déclenche un déploiement Netlify.

