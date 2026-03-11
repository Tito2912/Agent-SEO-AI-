# Google (GSC + GA4) — référence

Oui: tu peux connecter l’agent pour récupérer automatiquement les données.

## Option recommandée (A→Z): Service Account

### 1) Créer le projet et activer les APIs

1. Google Cloud Console → créer un projet
2. Activer:
   - **Google Search Console API**
   - (optionnel) **Google Analytics Data API** (GA4)

### 2) Créer un Service Account + clé JSON

1. IAM & Admin → Service Accounts → Create
2. Créer une clé **JSON** (download)
3. Stocker le fichier JSON en local (ex: `~/secrets/google/seo-agent.json`) et **ne jamais le committer**

### 3) Donner accès à la Search Console

Pour chaque propriété GSC (URL-prefix ou Domain):
1. Search Console → Settings → Users and permissions
2. Ajouter l’email du service account (ex: `seo-agent@project.iam.gserviceaccount.com`)
3. Rôle recommandé: **Owner** (ou au minimum read si tu ne fais que lire)

Notes:
- Pour une **Domain property**, l’identifiant API est souvent `sc-domain:example.com`.
- Pour une **URL-prefix property**, c’est l’URL (ex: `https://example.com/`).

### 4) Donner accès à GA4 (optionnel)

GA4 Admin → Property access management:
- Ajouter l’email du service account avec un rôle lecture (ou plus si besoin).

### 5) Brancher les secrets

Dans un `.env` local (jamais committé):
- `GOOGLE_APPLICATION_CREDENTIALS="/chemin/vers/seo-agent.json"`

## Récupérer les données GSC (script)

Script: `scripts/gsc_fetch.py`

Exemples:
- Requêtes (28 jours):
  - `python3 scripts/gsc_fetch.py --property https://example.com/ --dimensions query --days 28 --output gsc-queries.csv`
- Pages (28 jours):
  - `python3 scripts/gsc_fetch.py --property https://example.com/ --dimensions page --days 28 --output gsc-pages.csv`

Ces CSV sont compatibles avec `scripts/gsc_analyze_csv.py`.

