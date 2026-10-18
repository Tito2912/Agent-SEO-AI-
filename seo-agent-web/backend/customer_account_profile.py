"""Read-only customer dossiers: stored facts, without provider calls or credentials."""
from __future__ import annotations

import csv
import io
from datetime import datetime, timezone

from sqlalchemy import func, select

try:
    from . import billing, cms_guidance, customer_accounts as accounts
    from .models import (AccountActivation, AccountMember, AcquisitionCampaign, JobRecord, OAuthIdentity,
                         Project, UsageEvent, UserConnection)
except ImportError:
    import billing
    import cms_guidance
    import customer_accounts as accounts
    from models import (AccountActivation, AccountMember, AcquisitionCampaign, JobRecord, OAuthIdentity,
                        Project, UsageEvent, UserConnection)

METRICS = {'pages_crawled_month': 'Pages analysées', 'assistant_messages_month': 'Messages à l’assistant',
           'ai_corrections_month': 'Corrections IA', 'ai_articles_month': 'Articles IA',
           'backlink_searches_month': 'Recherches de backlinks', 'backlink_replies_month': 'Réponses de backlinks'}
JOB_STATUSES = {'queued': 'En attente', 'running': 'En cours', 'done': 'Terminée', 'failed': 'Échouée',
                'canceled': 'Annulée', 'cancel_requested': 'Annulation demandée'}
MILESTONES = {'project_added_at': 'Premier site observé', 'audit_available_at': 'Première analyse exploitable observée',
              'correction_verified_at': 'Première correction vérifiée observée',
              'clean_audit_at': 'Premier bilan sans anomalie observé'}


def epoch_date(value):
    if not value:
        return ''
    try:
        return accounts.display_date(datetime.fromtimestamp(value, timezone.utc))
    except (OverflowError, OSError, ValueError):
        return ''


def snapshot(database, account_id, *, google_key, bing_key):
    basic = accounts.snapshot(database, account_id=account_id)
    if not basic['rows']:
        return None
    account = basic['rows'][0]
    now = datetime.now(timezone.utc)
    period = billing._period_key(now)
    logins = accounts.login_query(now).subquery()
    with database.session() as db:
        login = db.execute(select(logins).where(logins.c.user_id == account_id)).mappings().first()
        google_since = db.scalar(select(func.min(OAuthIdentity.created_at)).where(
            OAuthIdentity.user_id == account_id, OAuthIdentity.provider == 'google'))
        usage = dict(db.execute(select(UsageEvent.metric, func.sum(UsageEvent.amount)).where(
            UsageEvent.user_id == account_id, UsageEvent.period == period,
            UsageEvent.metric.in_(list(METRICS))).group_by(UsageEvent.metric)).all())
        projects = db.execute(select(Project.id, Project.slug, Project.site_name, Project.base_url, Project.created_at,
                                     Project.settings['cms']['platform'].as_string().label('platform')).where(
            Project.owner_user_id == account_id).order_by(Project.created_at.desc(), Project.id)).mappings().all()
        # Read names and dates only; do not load, decrypt, migrate or refresh credentials.
        connections = {row.key: row for row in db.execute(select(UserConnection.key, UserConnection.updated_at).where(
            UserConnection.user_id == account_id)).all()}
        job_counts = dict(db.execute(select(JobRecord.status, func.count()).where(
            JobRecord.owner_user_id == account_id, JobRecord.kind == 'crawl',
            JobRecord.created_at <= now.timestamp()).group_by(JobRecord.status)).all())
        jobs = db.execute(select(JobRecord.id, JobRecord.slug, JobRecord.status, JobRecord.created_at,
                                 JobRecord.started_at, JobRecord.finished_at).where(
            JobRecord.owner_user_id == account_id, JobRecord.kind == 'crawl', JobRecord.created_at <= now.timestamp()
        ).order_by(JobRecord.created_at.desc(), JobRecord.id).limit(10)).mappings().all()
        campaign = db.execute(select(AcquisitionCampaign.source, AcquisitionCampaign.medium, AcquisitionCampaign.name).join(
            AccountActivation, AccountActivation.acquisition_campaign_id == AcquisitionCampaign.id).where(
            AccountActivation.user_id == account_id, AccountActivation.acquisition_consented_at.is_not(None))).mappings().first()
        members = db.scalar(select(func.count()).select_from(AccountMember).where(AccountMember.owner_user_id == account_id))

    def saved(keys):
        keys = [keys] if isinstance(keys, str) else keys
        connection = next((connections[key] for key in keys if key in connections), None)
        return {'saved': connection is not None, 'date': accounts.display_date(connection.updated_at) if connection else ''}

    sites = []
    for project in projects:
        sites.append({'name': project['site_name'] or project['slug'], 'url': project['base_url'],
                      'created_at': accounts.display_date(project['created_at']),
                      'platform': cms_guidance.PLATFORMS.get(project['platform'], 'Non renseigné'),
                      'google': saved(google_key(project['slug'])),
                      'bing': saved([bing_key(project['slug']), 'BING_OAUTH_REFRESH_TOKEN', 'BING_WEBMASTER_API_KEY']),
                      'wordpress': saved('CMS_WORDPRESS:' + project['id'])})
    limits = billing.plan_catalog()[account['plan_key']].get('limits') or {}
    quotas = []
    for key, label in METRICS.items():
        limit = limits.get(key)
        # Zero in this ledger means no numeric ceiling; feature access remains separate.
        limit_label = str(limit) if isinstance(limit, int) and limit > 0 else 'Sans plafond de compteur'
        if ((key == 'ai_articles_month' and billing.plan_rank(account['plan_key']) < billing.plan_rank('pro')) or
                (key.startswith('backlink_') and billing.plan_rank(account['plan_key']) < billing.plan_rank('solo'))):
            limit_label = 'Non inclus dans cette formule'
        quotas.append({'key': key, 'label': label, 'used': int(usage.get(key) or 0), 'limit_label': limit_label})
    login = dict(login) if login else {}
    return {'account': account, 'generated_at': basic['generated_at'], 'period': period, 'sites': sites,
            'google_since': accounts.display_date(google_since), 'members': int(members or 0), 'quotas': quotas,
            'login_count': int(login.get('login_count') or 0), 'first_login_at': accounts.display_date(login.get('first_login_at')),
            'password_login_count': int(login.get('password_login_count') or 0),
            'google_login_count': int(login.get('google_login_count') or 0),
            'campaign': dict(campaign) if campaign else None,
            'connections': [{'label': label, **saved(key)} for key, label in [
                ('GITHUB_TOKEN', 'GitHub'), ('NETLIFY_TOKEN', 'Netlify'),
                ('BING_OAUTH_REFRESH_TOKEN', 'Bing au niveau du compte'), ('BING_WEBMASTER_API_KEY', 'Bing par clé API')]],
            'job_counts': [{'label': JOB_STATUSES.get(status, 'Autre état'), 'count': count} for status, count in sorted(job_counts.items())],
            'jobs': [{'id': j['id'], 'slug': j['slug'], 'status': JOB_STATUSES.get(j['status'], 'Autre état'),
                      'created_at': epoch_date(j['created_at']), 'started_at': epoch_date(j['started_at']),
                      'finished_at': epoch_date(j['finished_at'])} for j in jobs],
            'milestones': [{'label': label, 'date': account[key]} for key, label in MILESTONES.items()]}


def csv_bytes(data):
    output = io.StringIO(newline='')
    writer = csv.writer(output, delimiter=';')
    writer.writerow(['Rubrique', 'Champ', 'Valeur', 'Lecture (Paris)'])

    def row(section, label, value):
        writer.writerow([accounts.csv_cell(v) for v in [section, label, value, data['generated_at']]])

    account = data['account']
    for key, label in [('user_id', 'Identifiant'), ('email', 'Email'), ('created_at', 'Inscription (Paris)'),
                       ('verification_label', 'Vérification email'), ('email_verified_at', 'Vérifié le (Paris)'),
                       ('plan_label', 'Formule'), ('access_label', 'Accès actuel'), ('status', 'Statut abonnement'),
                       ('current_period_end', 'Fin de période (Paris)'), ('country', 'Pays renseigné'),
                       ('language', 'Langue renseignée'), ('timezone', 'Fuseau renseigné'),
                       ('last_login_at', 'Dernière connexion connue (Paris)'), ('site_count', 'Sites actuels'),
                       ('analysis_count', 'Analyses enregistrées')]:
        row('Compte', label, account[key])
    row('Compte', 'Google associé', 'Oui' if account['google_linked'] else 'Non retrouvé')
    row('Compte', 'Nom du pays', account['country_label'])
    row('Compte', 'Source du pays', account['country_source'])
    row('Compte', 'Google associé depuis (Paris)', data['google_since'])
    row('Compte', 'Connexions réussies connues', data['login_count'])
    row('Compte', 'Première connexion connue (Paris)', data['first_login_at'])
    row('Compte', 'Connexions par email connues', data['password_login_count'])
    row('Compte', 'Connexions Google connues', data['google_login_count'])
    row('Compte', 'Collaborateurs actuels', data['members'])
    for key, label in [('source', 'Source'), ('medium', 'Support'), ('name', 'Campagne')]:
        row('Origine acceptée', label, (data['campaign'] or {}).get(key, ''))
    for item in data['milestones']:
        row('Étapes observées', item['label'], item['date'])
    for quota in data['quotas']:
        row('Consommation ' + data['period'] + ' (UTC)', quota['label'], quota['used'])
        row('Plafonds actuels', quota['label'], quota['limit_label'])
    for connection in data['connections']:
        row('Configurations du compte', connection['label'],
            'Configuration enregistrée' if connection['saved'] else 'Non retrouvée en base')
        row('Configurations du compte', connection['label'] + ' · mise à jour (Paris)', connection['date'])
    for item in data['job_counts']:
        row('Analyses par état', item['label'], item['count'])
    for index, site in enumerate(data['sites'], 1):
        section = 'Site ' + str(index)
        for key, label in [('name', 'Nom'), ('url', 'URL'), ('created_at', 'Ajouté le (Paris)'), ('platform', 'CMS renseigné')]:
            row(section, label, site[key])
        for key, label in [('google', 'Google Search Console'), ('bing', 'Bing'), ('wordpress', 'WordPress')]:
            row(section, label, 'Configuration enregistrée' if site[key]['saved'] else 'Non retrouvée en base')
            row(section, label + ' · mise à jour (Paris)', site[key]['date'])
    for job in data['jobs']:
        for key, label in [('id', 'Identifiant'), ('slug', 'Site'), ('status', 'État'), ('created_at', 'Demandée le (Paris)'),
                           ('started_at', 'Démarrée le (Paris)'), ('finished_at', 'Fin le (Paris)')]:
            row('Analyse récente ' + job['id'], label, job[key])
    return ('\ufeff' + output.getvalue()).encode('utf-8')
