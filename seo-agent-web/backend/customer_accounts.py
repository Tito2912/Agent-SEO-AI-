"""Owner-only account inventory, using exactly the population of the plan counters."""
from __future__ import annotations

import csv
import io
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from sqlalchemy import case, exists, func, select

try:
    from . import billing, customer_country, plan_counts
    from .models import AccountActivation, AuditLog, EmailVerificationToken, JobRecord, OAuthIdentity, Project, User
except ImportError:
    import billing
    import customer_country
    import plan_counts
    from models import AccountActivation, AuditLog, EmailVerificationToken, JobRecord, OAuthIdentity, Project, User

PAGE_SIZE = 25
USAGE_FILTERS = {'all': 'Tous les comptes', 'with_site': 'Avec un site', 'without_site': 'Sans site',
                 'recent_login': 'Connexion connue depuis 30 jours',
                 'no_recent_login': 'Sans connexion connue depuis 30 jours',
                 'verified_email': 'Email vérifié', 'unverified_email': 'Vérification d’email non retrouvée',
                 'with_country': 'Pays déclaré', 'without_country': 'Pays non renseigné',
                 'with_analysis': 'Avec une analyse enregistrée', 'without_analysis': 'Sans analyse enregistrée'}
STATUSES = {'active': 'actif', 'trialing': 'en essai', 'canceled': 'terminé', 'past_due': 'en retard de paiement',
            'unpaid': 'impayé', 'incomplete': 'incomplet', 'incomplete_expired': 'expiré', 'paused': 'en pause'}


def utc(value):
    return value.replace(tzinfo=timezone.utc) if value and value.tzinfo is None else value


def display_date(value):
    return utc(value).astimezone(ZoneInfo('Europe/Paris')).strftime('%d/%m/%Y à %H:%M') if value else ''


def login_query(now):
    """Successful logins only; Google identifies the new session by its user target."""
    login_user = case((AuditLog.action == 'auth.google.callback', AuditLog.target_id), else_=AuditLog.actor_user_id)
    return select(login_user.label('user_id'), func.max(AuditLog.created_at).label('last_login_at'),
                  func.min(AuditLog.created_at).label('first_login_at'), func.count().label('login_count'),
                  func.sum(case((AuditLog.action == 'auth.login', 1), else_=0)).label('password_login_count'),
                  func.sum(case((AuditLog.action == 'auth.google.callback', 1), else_=0)).label('google_login_count')).where(
        AuditLog.status == 'ok', AuditLog.created_at <= now,
        ((AuditLog.action == 'auth.login') | ((AuditLog.action == 'auth.google.callback') &
                                            (AuditLog.target_type == 'user')))).group_by(login_user)


def verification_query(now):
    return select(EmailVerificationToken.user_id,
                  func.min(EmailVerificationToken.used_at).label('email_verified_at')).where(
        EmailVerificationToken.used_at.is_not(None), EmailVerificationToken.used_at <= now).group_by(
        EmailVerificationToken.user_id)


def csv_cell(value):
    text = str(value) if value is not None else ''
    if text.lstrip().startswith(('=', '+', '-', '@')) or text.startswith(('\t', '\r', '\n')):
        return "'" + text
    return text


def snapshot(database, *, plan='', q='', usage='all', page=1, export=False, account_id=None):
    catalog = billing.plan_catalog()
    plan, q, usage = plan.strip().lower(), q.strip(), usage.strip()
    if plan and plan not in catalog:
        raise ValueError('Formule inconnue.')
    if usage not in USAGE_FILTERS or len(q) > 320 or page < 1:
        raise ValueError('Filtres de comptes invalides.')
    now = datetime.now(timezone.utc)
    projects = select(Project.owner_user_id.label('user_id'), func.count(Project.id).label('site_count')).group_by(
        Project.owner_user_id).subquery()
    logins = login_query(now).subquery()
    verified = verification_query(now).subquery()
    google_linked = exists(select(OAuthIdentity.id).where(OAuthIdentity.user_id == User.id, OAuthIdentity.provider == 'google'))
    analyses = select(JobRecord.owner_user_id.label('user_id'), func.count().label('analysis_count'),
                      func.max(JobRecord.created_at).label('last_analysis_epoch')).where(
        JobRecord.kind == 'crawl', JobRecord.created_at <= now.timestamp()).group_by(JobRecord.owner_user_id).subquery()
    query = plan_counts.account_query(
        User.id.label('user_id'), User.email, User.created_at, User.country, User.language, User.timezone,
        func.coalesce(projects.c.site_count, 0).label('site_count'), logins.c.last_login_at,
        verified.c.email_verified_at, google_linked.label('google_linked'),
        func.coalesce(analyses.c.analysis_count, 0).label('analysis_count'), analyses.c.last_analysis_epoch,
        AccountActivation.signup_observed, AccountActivation.project_added_at, AccountActivation.audit_available_at,
        AccountActivation.correction_verified_at, AccountActivation.clean_audit_at,
    ).outerjoin(projects, projects.c.user_id == User.id).outerjoin(logins, logins.c.user_id == User.id).outerjoin(
        AccountActivation, AccountActivation.user_id == User.id).outerjoin(verified, verified.c.user_id == User.id).outerjoin(
        analyses, analyses.c.user_id == User.id).order_by(User.created_at.desc(), User.id)
    if account_id is not None:
        query = query.where(User.id == str(account_id))
    with database.session() as db:
        records = db.execute(query).all()
    rows, counts = [], dict.fromkeys(catalog, 0)
    coverage = dict.fromkeys(['verified', 'country', 'with_sites', 'with_analyses', 'with_login'], 0)
    for record in records:
        row = dict(record._mapping)
        key = billing.plan_key_for_subscription(SimpleNamespace(**row))
        counts[key] += 1
        row['plan_key'], row['plan_label'] = key, str(catalog[key].get('label') or key)
        row['status'] = str(row['status'] or '')
        if key == 'free':
            row['access_label'] = ('Accès gratuit · abonnement ' + STATUSES.get(row['status'], row['status'])
                                   if row['status'] and row['status'] not in billing.ACTIVE_SUB_STATUSES
                                   else 'Accès gratuit')
        else:
            row['access_label'] = 'En essai' if row['status'] == 'trialing' else 'Abonnement actif'
        if row['cancel_at_period_end']:
            row['access_label'] += ' · résiliation prévue'
        row['recent_login'] = bool(row['last_login_at'] and utc(row['last_login_at']) >= now - timedelta(days=30))
        row['email_verified'] = bool(row['email_verified_at'])
        row['verification_label'] = 'Email vérifié' if row['email_verified'] else 'Vérification non retrouvée'
        row['country'] = customer_country.normalize(row['country'])
        row['country_label'] = customer_country.COUNTRIES.get(row['country'], 'Non renseigné')
        row['country_source'] = 'Déclaré par le client' if row['country'] else 'Non renseignée'
        for name, present in [('verified', row['email_verified']), ('country', row['country']),
                              ('with_sites', row['site_count']), ('with_analyses', row['analysis_count']),
                              ('with_login', row['last_login_at'])]:
            coverage[name] += int(bool(present))
        row['profile_url'] = '/settings/activation/accounts/' + row['user_id']
        last_analysis = row.pop('last_analysis_epoch')
        row['last_analysis_at'] = display_date(datetime.fromtimestamp(last_analysis, timezone.utc)) if last_analysis else ''
        if plan and key != plan:
            continue
        if q.casefold() not in (row['email'] + ' ' + row['user_id']).casefold():
            continue
        if ((usage == 'with_site' and not row['site_count']) or (usage == 'without_site' and row['site_count']) or
                (usage == 'recent_login' and not row['recent_login']) or
                (usage == 'no_recent_login' and row['recent_login']) or
                (usage == 'verified_email' and not row['email_verified']) or
                (usage == 'unverified_email' and row['email_verified']) or
                (usage == 'with_country' and not row['country']) or
                (usage == 'without_country' and row['country']) or
                (usage == 'with_analysis' and not row['analysis_count']) or
                (usage == 'without_analysis' and row['analysis_count'])):
            continue
        row['milestone'] = 'Historique non suivi'
        for field, label in [('correction_verified_at', 'Correction vérifiée'), ('clean_audit_at', 'Bilan sans anomalie'),
                             ('audit_available_at', 'Analyse exploitable'), ('project_added_at', 'Site ajouté')]:
            if row[field]:
                row['milestone'] = label
                break
        else:
            if row['signup_observed']:
                row['milestone'] = 'Inscription suivie'
        for field in ['created_at', 'last_login_at', 'current_period_end', 'project_added_at', 'audit_available_at',
                      'correction_verified_at', 'clean_audit_at', 'email_verified_at']:
            row[field] = display_date(row[field])
        # Price IDs are only useful for shared plan resolution, never for the output.
        row.pop('stripe_price_id')
        rows.append(row)
    matched = len(rows)
    pages = max(1, (matched + PAGE_SIZE - 1) // PAGE_SIZE)
    page = min(page, pages)
    params = {'plan': plan, 'q': q, 'usage': usage}
    return {'rows': rows if export else rows[(page - 1) * PAGE_SIZE:page * PAGE_SIZE],
            'total': len(records), 'matched': matched, 'counts': counts, 'coverage': coverage, 'plan': plan, 'q': q, 'usage': usage,
            'page': page, 'pages': pages, 'first': (page - 1) * PAGE_SIZE + 1 if matched else 0,
            'last': min(page * PAGE_SIZE, matched), 'generated_at': display_date(now),
            'export_url': '/settings/activation/accounts.csv?' + urlencode(params),
            'previous_url': '/settings/activation/accounts?' + urlencode({**params, 'page': page - 1}) if page > 1 else '',
            'next_url': '/settings/activation/accounts?' + urlencode({**params, 'page': page + 1}) if page < pages else ''}


def csv_bytes(data):
    fields = [('user_id', 'Identifiant du compte'), ('email', 'Email'), ('created_at', 'Inscription (Paris)'),
              ('plan_label', 'Formule actuelle'), ('access_label', 'Accès actuel'), ('status', 'Statut abonnement'),
              ('cancel_at_period_end', 'Résiliation prévue'), ('current_period_end', 'Fin de période (Paris)'),
              ('site_count', 'Sites actuels'), ('last_login_at', 'Dernière connexion connue (Paris)'),
              ('milestone', 'Étape observée'), ('project_added_at', 'Premier site observé (Paris)'),
              ('audit_available_at', 'Première analyse exploitable observée (Paris)'),
              ('correction_verified_at', 'Première correction vérifiée observée (Paris)'),
              ('clean_audit_at', 'Premier bilan sans anomalie observé (Paris)')]
    fields += [('verification_label', 'Vérification email'), ('email_verified_at', 'Email vérifié le (Paris)'),
               ('google_linked', 'Google associé'), ('analysis_count', 'Analyses enregistrées'),
               ('last_analysis_at', 'Dernière demande d’analyse (Paris)'),
               ('country', 'Pays renseigné'), ('country_label', 'Nom du pays'), ('country_source', 'Source du pays'),
               ('language', 'Langue renseignée'), ('timezone', 'Fuseau renseigné')]
    output = io.StringIO(newline='')
    writer = csv.writer(output, delimiter=';')
    writer.writerow([title for _, title in fields] + ['Lecture (Paris)'])
    for row in data['rows']:
        values = {**row, 'cancel_at_period_end': 'Oui' if row['cancel_at_period_end'] else 'Non',
                  'google_linked': 'Oui' if row['google_linked'] else 'Non'}
        writer.writerow([csv_cell(values[key]) for key, _ in fields] + [data['generated_at']])
    return ('\ufeff' + output.getvalue()).encode('utf-8')
