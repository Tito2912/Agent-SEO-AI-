# Runbook (Ops)

## Environnements & données

**Sources de vérité**
- **Postgres (Render)** : comptes, abonnements, quotas, jobs, logs.
- **Disque Render** (`/var/data`) : data locale (ex: `seo-runs`, `data/`).
- **S3** : artefacts de runs (si configuré).

## Configuration stricte

En production, `SEO_AGENT_STRICT_CONFIG=true` bloque le démarrage si les secrets critiques sont absents ou faibles.

Variables obligatoires en mode strict :
- `DATABASE_URL`
- `PUBLIC_BASE_URL` en `https://...`
- `SEO_AGENT_SECRET_KEY` long et aléatoire
- `SEO_AGENT_ENCRYPTION_KEY` ou `SEO_AGENT_ENCRYPTION_KEYS`, long, aléatoire et distinct de `SEO_AGENT_SECRET_KEY`
- `CRON_SECRET` long et aléatoire

Sur Render, `SEO_AGENT_TRUST_PROXY_HEADERS=true` permet d'utiliser `X-Forwarded-For` / `X-Forwarded-Proto` pour l'IP client et les cookies `Secure`. Ne l'active pas hors proxy de confiance.

La Content Security Policy est active par défaut (`SEO_AGENT_CSP_ENABLED=true`). Pour tester une politique sans blocage, définir `SEO_AGENT_CSP_REPORT_ONLY=true`. `SEO_AGENT_CSP` permet de remplacer entièrement la politique par défaut si nécessaire.
La prévisualisation `/file` est limitée par `SEO_AGENT_FILE_VIEW_MAX_BYTES` (2 Mo par défaut, maximum 20 Mo) pour éviter de charger de gros artefacts en mémoire.
Le body lu par le middleware CSRF est limité par `SEO_AGENT_CSRF_BODY_MAX_BYTES` (12 Mo par défaut, maximum 50 Mo), ce qui couvre les imports CSV actuels sans ouvrir un chemin de déni de service mémoire.

## Migrations DB (Alembic)

Le schéma est géré via Alembic (dossier `seo-agent-web/alembic/`). En production, le container exécute automatiquement `alembic upgrade head` au démarrage (voir `Dockerfile`).
En développement local SQLite (sans `DATABASE_URL`), l'app applique aussi `alembic upgrade head` au démarrage pour éviter les bases en retard.

### Appliquer les migrations (manuel)

Depuis `seo-agent-web/` :
- `alembic -c alembic.ini upgrade head`

Pour forcer ce comportement sur une base configurée via `DATABASE_URL`, définir `SEO_AGENT_DB_AUTO_MIGRATE=true`.

### Créer une nouvelle migration

Depuis `seo-agent-web/` :
- `alembic -c alembic.ini revision -m "..." --autogenerate`
- Vérifie le diff généré dans `seo-agent-web/alembic/versions/`
- Puis : `alembic -c alembic.ini upgrade head`

## Backups

### Backup Postgres (pg_dump)

Pré-requis : avoir accès à `DATABASE_URL`.

- Backup (SQL) : `pg_dump "$DATABASE_URL" > backup.sql`
- Backup (format custom, recommandé) : `pg_dump -Fc "$DATABASE_URL" > backup.dump`

### Restore Postgres

- Restore (SQL) : `psql "$DATABASE_URL" < backup.sql`
- Restore (custom) : `pg_restore -d "$DATABASE_URL" --clean --if-exists backup.dump`

### Backup disque Render

Le disque monté contient des fichiers applicatifs (ex: `SEO_AGENT_DATA_DIR`, runs, tokens OAuth fichier).

Idée simple :
- Archiver le contenu du disque et l’uploader dans un bucket (S3) ou un stockage équivalent.

## Monitoring / alerting

- **Sentry** : configure `SENTRY_DSN` + vérifie la réception d’une erreur test.
- **Santé** : endpoint `GET /healthz` utilisé par Render.
- **Exploitation prod** : connecte-toi avec le compte propriétaire système puis ouvre `/settings/operations`.
  Cette page vérifie sans afficher les secrets : configuration stricte, secrets critiques, Stripe, emails, IA, S3/backups, stockage disque, jobs bloqués et audit logs récents.

Avant ouverture publique, viser :
- 0 contrôle `error` sur `/settings/operations`
- backups S3 configurés et au moins un manifest vérifié
- Stripe en mode live testé de bout en bout
- aucun job actif bloqué depuis plus de 2h
- reset password et emails transactionnels testés

## Backups automatiques (S3)

Le repo inclut un script de backup qui exporte la base Postgres (format custom) et upload le dump (et optionnellement le data dir) vers S3.

Commande (dans le container) :
- `python -m backend.backup`

Variables nécessaires :
- `DATABASE_URL`
- `S3_BUCKET_NAME`
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` (et optionnel `AWS_S3_ENDPOINT_URL`)

Variables optionnelles :
- `BACKUP_S3_PREFIX` (défaut `backups`)
- `BACKUP_ENV` (sinon `SENTRY_ENVIRONMENT` / `RENDER_SERVICE_NAME`)
- `BACKUP_RETENTION_DAYS` (si > 0, suppression best-effort des objets plus vieux)
- `BACKUP_SKIP_DATA_DIR=true` (ne pas archiver `SEO_AGENT_DATA_DIR`)
- `BACKUP_INCLUDE_RUNS_DIR=true` (archive `SEO_AGENT_RUNS_DIR` — peut être gros)

Sur Render :
- le blueprint inclut un cron job `noyaru-db-backup`
- commande : `python -m backend.backup`
- schedule : `0 2 * * *` (02:00 UTC)
- variables minimales : `DATABASE_URL`, `S3_BUCKET_NAME`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`
- variables recommandées : `BACKUP_SKIP_DATA_DIR=true`, `BACKUP_ENV=noyaru-prod`, `BACKUP_S3_PREFIX=backups`, `BACKUP_RETENTION_DAYS=30`

### Vérifier un run de backup

Dans les logs du cron job, attendre :
- `[BACKUP] uploaded db ...`
- `[BACKUP] uploaded manifest ...`
- `[BACKUP] done`

Dans S3, vérifier :
- `backups/noyaru-prod/db-YYYYMMDD-HHMMSS.dump`
- `backups/noyaru-prod/manifest-YYYYMMDD-HHMMSS.json`

### Restore depuis un backup S3

1. Télécharger le dump ciblé depuis le manifest :
- `aws s3 cp "s3://$S3_BUCKET_NAME/backups/noyaru-prod/db-YYYYMMDD-HHMMSS.dump" ./restore.dump`

2. Restaurer dans une base cible :
- `pg_restore -d "$DATABASE_URL" --clean --if-exists ./restore.dump`

3. Vérifier l'application :
- login
- ouverture du dashboard
- crawl simple
- exports / intégrations critiques si concernées

Remarques :
- si `BACKUP_SKIP_DATA_DIR=true`, ce cron job ne sauvegarde que Postgres
- pour une restauration sans risque, restaurer d'abord dans une base temporaire/staging

## Procédure de déploiement (checklist)

- Migrations : `alembic upgrade head` (automatique en prod via Dockerfile).
- Smoke test : login, crawl simple, export, webhook Stripe (si actif).
- Vérifier la santé Render : logs + `healthz`.

## Worker séparé (scalabilité)

Le repo supporte 2 modes via `SEO_AGENT_SERVICE_MODE` (voir `seo-agent-web/entrypoint.sh`) :
- `web` (défaut) : migrations + API (uvicorn)
- `worker` : exécute les jobs (crawls / exports / autopilot)

### Render (recommandé)

- Crée un service **Worker** basé sur le même repo/image.
- Ajoute `SEO_AGENT_SERVICE_MODE=worker`.
- Copie les env vars nécessaires (au minimum : `DATABASE_URL`, `SEO_AGENT_ENCRYPTION_KEY`, creds S3, clés IA).
- Mets `SEO_AGENT_DISABLE_WORKER=true` sur le service web pour éviter de lancer le worker deux fois.

## Rotation clé de chiffrement (secrets)

Les secrets côté serveur sont chiffrés (préfixe `enc:`). Pour faire une rotation sans casser la lecture des anciens secrets :
- Ajoute `SEO_AGENT_ENCRYPTION_KEYS` avec la **nouvelle clé en premier**, puis les anciennes (séparées par virgules).
- Déploie.
- Les secrets seront progressivement ré-encryptés avec la clé courante lors de leur lecture/écriture.
