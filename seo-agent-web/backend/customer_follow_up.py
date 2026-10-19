"""Owner-only assistance queues from stored facts, without opening reports or calling providers."""
from __future__ import annotations

import logging
import hashlib
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode, urlsplit
from zoneinfo import ZoneInfo

from sqlalchemy import func, select

try:
    from . import audit_dashboard as dash, first_correction, plan_counts
    from .models import AccountActivation, AuditLog, IssueTask, JobRecord, Project, User
except ImportError:
    import first_correction
    import plan_counts
    import audit_dashboard as dash
    from models import AccountActivation, AuditLog, IssueTask, JobRecord, Project, User

log = logging.getLogger(__name__)
ACTION = 'activation.audit_facts'
GROUPS = {
    'no_site_48h': {'label': 'Sans site après 48 h',
                   'detail': 'Compte créé depuis au moins 48 h, sans site actuellement.'},
    'no_first_audit': {'label': 'Site sans premier bilan',
                       'detail': 'Inscription suivie, site présent et aucun premier bilan exploitable observé. Analyses actives exclues.'},
    'failed_analysis': {'label': 'Dernière analyse échouée',
                        'detail': 'Au moins un site dont la dernière demande d’analyse a échoué.'},
    'no_correction': {'label': 'Bilan sans correction choisie',
                      'detail': 'Bilan exploitable contenant des problèmes, sans correction technique enregistrée pour ce site.'},
}
LIVE = {'queued', 'running', 'cancel_requested'}


def utc(value):
    return value.replace(tzinfo=timezone.utc) if value and value.tzinfo is None else value


def date(value):
    return utc(value).astimezone(ZoneInfo('Europe/Paris')).strftime('%d/%m/%Y à %H:%M') if value else ''


def target_hash(base_url):
    return hashlib.sha256(str(base_url).encode('utf-8')).hexdigest()


def observe(database, project, report, facts, *, crawl_ts='', now=None):
    """Called only after the existing verifier establishes a usable report; no report contents stored."""
    try:
        now = now or datetime.now(timezone.utc)
        meta = report.get('meta') if isinstance(report.get('meta'), dict) else {}
        stamp = first_correction.timestamp(crawl_ts or meta.get('timestamp') or meta.get('crawl_ts') or meta.get('started_at'))
        if not stamp or stamp > now or stamp < utc(project.created_at).replace(microsecond=0):
            return
        positive = False
        for key, issue in report['issues'].items():
            # The shared validator already ignores delta metrics for milestone evidence.
            if dash.is_delta_issue_key(key):
                continue
            count = issue.get('count') if isinstance(issue, dict) else None
            if isinstance(count, bool) or not isinstance(count, (int, str)):
                positive = None
                break
            try:
                value = int(count)
            except (TypeError, ValueError):
                positive = None
                break
            if value < 0:
                positive = None
                break
            positive |= value > 0
        with database.session() as db:
            db.add(AuditLog(action=ACTION, status='ok', actor_user_id=str(project.owner_user_id),
                            target_type='project', target_id=str(project.id), created_at=now,
                            meta={'version': 1, 'crawl_ts': stamp.strftime('%Y%m%d-%H%M%S'),
                                  'positive_issues': positive, 'clean': bool(facts['clean']),
                                  'target_hash': target_hash(project.base_url)}))
            db.commit()
    except Exception as error:
        # Analytics must not change the outcome of the crawl or its existing milestones.
        log.warning('Assistance report evidence unavailable (%s)', type(error).__name__)


def snapshot(database, *, now=None):
    now = now or datetime.now(timezone.utc)
    eligible = plan_counts.account_query(User.id.label('user_id'), User.created_at.label('registered_at'),
        AccountActivation.signup_observed, AccountActivation.audit_available_at).outerjoin(
            AccountActivation, AccountActivation.user_id == User.id)
    scope = eligible.subquery()
    project_query = select(Project.id, Project.owner_user_id, Project.slug, Project.site_name, Project.base_url, Project.created_at).join(
        scope, scope.c.user_id == Project.owner_user_id).where(Project.created_at <= now)
    projects_scope = project_query.subquery()
    ranked_jobs = select(JobRecord.id, JobRecord.owner_user_id, JobRecord.slug, JobRecord.status,
        JobRecord.created_at, JobRecord.finished_at, JobRecord.result['timestamp'].as_string().label('crawl_ts'),
        func.row_number().over(partition_by=(JobRecord.owner_user_id, JobRecord.slug),
            order_by=(JobRecord.created_at.desc(), JobRecord.id.desc())).label('rank')).join(projects_scope,
        (projects_scope.c.owner_user_id == JobRecord.owner_user_id) &
        (projects_scope.c.slug == JobRecord.slug)).where(
        JobRecord.kind == 'crawl', JobRecord.created_at <= now.timestamp()).subquery()
    ranked_facts = select(AuditLog.target_id.label('project_id'), AuditLog.created_at,
        AuditLog.meta['crawl_ts'].as_string().label('crawl_ts'),
        AuditLog.meta['positive_issues'].as_boolean().label('positive'),
        AuditLog.meta['target_hash'].as_string().label('target_hash'),
        func.row_number().over(partition_by=AuditLog.target_id, order_by=(
            AuditLog.meta['crawl_ts'].as_string().desc(), AuditLog.created_at.desc(), AuditLog.id.desc())).label('rank')
    ).join(projects_scope, (projects_scope.c.id == AuditLog.target_id) &
           (projects_scope.c.owner_user_id == AuditLog.actor_user_id)).where(
        AuditLog.action == ACTION, AuditLog.status == 'ok', AuditLog.target_type == 'project',
        AuditLog.meta['version'].as_integer() == 1, AuditLog.created_at <= now).subquery()
    with database.session() as db:
        accounts = db.execute(eligible).mappings().all()
        projects = db.execute(project_query).mappings().all()
        jobs = db.execute(select(ranked_jobs).where(ranked_jobs.c.rank == 1)).mappings().all()
        evidence = db.execute(select(ranked_facts).where(ranked_facts.c.rank == 1)).mappings().all()
        tasks = db.execute(select(IssueTask.project_id, IssueTask.issue_key, IssueTask.url, IssueTask.crawl_ts).join(
            projects_scope, projects_scope.c.id == IssueTask.project_id).where(
                IssueTask.status != 'ignored', IssueTask.created_at <= now)).mappings().all()
    sites = {p['id']: p for p in projects}
    by_account = {}
    for site in projects:
        by_account.setdefault(site['owner_user_id'], []).append(site)
    latest = {(j['owner_user_id'], j['slug']): j for j in jobs}
    known, invalid = {}, set()
    for item in evidence:
        stamp = first_correction.timestamp(item['crawl_ts'])
        if (stamp and utc(sites[item['project_id']]['created_at']).replace(microsecond=0) <= stamp <= now
                and item['target_hash'] == target_hash(sites[item['project_id']]['base_url'])):
            known[item['project_id']] = item
        else:
            invalid.add(item['project_id'])
    chosen = set()
    for task in tasks:
        url, stamp = first_correction.web_url(task['url']), first_correction.timestamp(task['crawl_ts'])
        base = first_correction.web_url(sites[task['project_id']]['base_url'])
        if (not task['issue_key'].startswith('_') and url and base and stamp and stamp <= now
                and urlsplit(url).hostname == urlsplit(base).hostname):
            chosen.add(task['project_id'])
    reasons, unknown = {}, set()

    def add(uid, key, site=None, when=None):
        reasons.setdefault(uid, []).append({'key': key, 'label': GROUPS[key]['label'],
            'site': (site['site_name'] or site['slug']) if site else '', 'slug': site['slug'] if site else '',
            'date': date(when), 'date_label': 'Inscription' if key == 'no_site_48h' else (
                'Site ajouté' if key == 'no_first_audit' else ('Demande d’analyse' if key == 'failed_analysis' else 'Bilan'))})

    for account in accounts:
        uid, registered = account['user_id'], utc(account['registered_at'])
        if not registered or registered > now:
            continue
        owned = by_account.get(uid, [])
        if not owned and registered and registered <= now - timedelta(hours=48):
            add(uid, 'no_site_48h', when=registered)
        active, unknown_done, usable = False, False, False
        for site in owned:
            job = latest.get((uid, site['slug']))
            if job and job['created_at'] < utc(site['created_at']).timestamp():
                job = None  # A deleted site with the same slug must not contaminate its replacement.
            fact = known.get(site['id'])
            if site['id'] in invalid:
                unknown.add(uid)
                unknown_done = True
            active |= bool(job and job['status'] in LIVE)
            usable |= bool(fact)
            matches = bool(fact and (not job or (job['status'] == 'done' and job['crawl_ts'] == fact['crawl_ts'])))
            if job and job['status'] == 'failed' and (not job['finished_at'] or job['finished_at'] <= now.timestamp()):
                add(uid, 'failed_analysis', site, datetime.fromtimestamp(job['created_at'], timezone.utc))
            if job and job['status'] == 'done' and not matches:
                unknown_done = True
                unknown.add(uid)
            if not fact and not account['signup_observed']:
                unknown.add(uid)
            if matches and fact['positive'] is None:
                unknown.add(uid)
            if matches and fact['positive'] is True and site['id'] not in chosen:
                add(uid, 'no_correction', site, first_correction.timestamp(fact['crawl_ts']))
        observed_audit = account['audit_available_at'] and utc(account['audit_available_at']) <= now
        if owned and account['signup_observed'] and not observed_audit and not usable and not active and not unknown_done:
            add(uid, 'no_first_audit', min(owned, key=lambda s: utc(s['created_at'])),
                min(utc(s['created_at']) for s in owned))
    groups = [{**value, 'key': key, 'count': sum(any(r['key'] == key for r in items) for items in reasons.values()),
               'url': '/settings/activation/accounts?' + urlencode({'attention': key})} for key, value in GROUPS.items()]
    return {'groups': groups, 'reasons': reasons, 'total': len(accounts), 'unique_count': len(reasons),
            'unknown_count': len(unknown), 'generated_at': date(now),
            'url': '/settings/activation/accounts?attention=any'}
