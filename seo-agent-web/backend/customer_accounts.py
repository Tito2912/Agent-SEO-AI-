"""Owner-only account inventory, using exactly the population of the plan counters."""
from __future__ import annotations

import csv
import io
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from sqlalchemy import case, func, select

try:
    from . import billing, plan_counts
    from .models import AccountActivation, AuditLog, Project, User
except ImportError:
    import billing
    import plan_counts
    from models import AccountActivation, AuditLog, Project, User

PAGE_SIZE = 25
USAGE_FILTERS = {'all': 'Tous les comptes', 'with_site': 'Avec un site', 'without_site': 'Sans site',
                 'recent_login': 'Connexion connue depuis 30 jours',
                 'no_recent_login': 'Sans connexion connue depuis 30 jours'}
STATUSES = {'active': 'actif', 'trialing': 'en essai', 'canceled': 'terminé', 'past_due': 'en retard de paiement',
            'unpaid': 'impayé', 'incomplete': 'incomplet', 'incomplete_expired': 'expiré', 'paused': 'en pause'}


def utc(value):
    return value.replace(tzinfo=timezone.utc) if value and value.tzinfo is None else value


def display_date(value):
    return utc(value).astimezone(ZoneInfo('Europe/Paris')).strftime('%d/%m/%Y à %H:%M') if value else ''


def snapshot(database, *, plan='', q='', usage='all', page=1, export=False):
    catalog = billing.plan_catalog()
    plan, q, usage = plan.strip().lower(), q.strip(), usage.strip()
    if plan and plan not in catalog:
        raise ValueError('Formule inconnue.')
    if usage not in USAGE_FILTERS or len(q) > 320 or page < 1:
        raise ValueError('Filtres de comptes invalides.')
    now = datetime.now(timezone.utc)
    projects = select(Project.owner_user_id.label('user_id'), func.count(Project.id).label('site_count')).group_by(
        Project.owner_user_id).subquery()
    # Google logs identify the newly authenticated user as the target, which may differ
    # from the actor of the request (or be NULL). Never join successful logins by email.
    login_user = case((AuditLog.action == 'auth.google.callback', AuditLog.target_id), else_=AuditLog.actor_user_id)
    logins = select(login_user.label('user_id'), func.max(AuditLog.created_at).label('last_login_at')).where(
        AuditLog.status == 'ok', AuditLog.created_at <= now,
        ((AuditLog.action == 'auth.login') | ((AuditLog.action == 'auth.google.callback') &
                                            (AuditLog.target_type == 'user')))).group_by(login_user).subquery()
    query = plan_counts.account_query(
        User.id.label('user_id'), User.email, User.created_at,
        func.coalesce(projects.c.site_count, 0).label('site_count'), logins.c.last_login_at,
        AccountActivation.signup_observed, AccountActivation.project_added_at, AccountActivation.audit_available_at,
        AccountActivation.correction_verified_at, AccountActivation.clean_audit_at,
    ).outerjoin(projects, projects.c.user_id == User.id).outerjoin(logins, logins.c.user_id == User.id).outerjoin(
        AccountActivation, AccountActivation.user_id == User.id).order_by(User.created_at.desc(), User.id)
    with database.session() as db:
        records = db.execute(query).all()
    rows, counts = [], dict.fromkeys(catalog, 0)
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
        if plan and key != plan:
            continue
        if q.casefold() not in (row['email'] + ' ' + row['user_id']).casefold():
            continue
        if ((usage == 'with_site' and not row['site_count']) or (usage == 'without_site' and row['site_count']) or
                (usage == 'recent_login' and not row['recent_login']) or
                (usage == 'no_recent_login' and row['recent_login'])):
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
                      'correction_verified_at', 'clean_audit_at']:
            row[field] = display_date(row[field])
        # Price IDs are only useful for shared plan resolution, never for the output.
        row.pop('stripe_price_id')
        rows.append(row)
    matched = len(rows)
    pages = max(1, (matched + PAGE_SIZE - 1) // PAGE_SIZE)
    page = min(page, pages)
    params = {'plan': plan, 'q': q, 'usage': usage}
    return {'rows': rows if export else rows[(page - 1) * PAGE_SIZE:page * PAGE_SIZE],
            'total': len(records), 'matched': matched, 'counts': counts, 'plan': plan, 'q': q, 'usage': usage,
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

    def cell(value):
        text = str(value) if value is not None else ''
        if text.lstrip().startswith(('=', '+', '-', '@')) or text.startswith(('\t', '\r', '\n')):
            return "'" + text
        return text

    output = io.StringIO(newline='')
    writer = csv.writer(output, delimiter=';')
    writer.writerow([title for _, title in fields] + ['Lecture (Paris)'])
    for row in data['rows']:
        values = {**row, 'cancel_at_period_end': 'Oui' if row['cancel_at_period_end'] else 'Non'}
        writer.writerow([cell(values[key]) for key, _ in fields] + [data['generated_at']])
    return ('\ufeff' + output.getvalue()).encode('utf-8')
