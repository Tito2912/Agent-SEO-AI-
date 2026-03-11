# Netlify (référence)

## Ce que l’agent a besoin

- **Netlify Personal Access Token** (env: `NETLIFY_TOKEN`) pour:
  - lister les sites
  - mapper domaines → sites Netlify
  - (optionnel) déclencher des builds via API / build hooks

## Créer le token

1. Netlify → **User settings** → **Applications** → **Personal access tokens**
2. Générer un token
3. Stocker en local (ex: `.env`, jamais committer):
   - `NETLIFY_TOKEN="..."`

## Connecter tes sites à Git (recommandé)

Voir aussi `references/git.md`.

En bref:
- Si ton site est déjà sur Netlify mais pas relié à Git:
  - Netlify → Site configuration → Build & deploy → Continuous deployment → **Link site to Git repository**
- Si tu as déployé en “drag & drop”:
  - Netlify → Deploys → **Download deploy**, puis créer un repo Git et relier.

## Mapper tes domaines vers tes sites Netlify

- Depuis ton CSV domaines:
  - `python3 scripts/netlify_map_domains.py --csv /chemin/vers/domain.csv`
- Sorties:
  - `netlify-domain-map.json`
  - `netlify-domain-map.csv`

Le mapping peut contenir `repo_url`/`repo_branch` si tes sites Netlify sont connectés à Git.
