"""First account milestones, recorded from committed product actions; aggregate cohorts only."""
from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta, timezone
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from sqlalchemy import case, event, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

try:
    from . import audit_dashboard as dash
    from . import signup_attribution
    from .models import AccountActivation, AccountInvite, AcquisitionCampaign, Project, User
except ImportError:
    import audit_dashboard as dash
    import signup_attribution
    from models import AccountActivation, AccountInvite, AcquisitionCampaign, Project, User

log = logging.getLogger('seo_agent.activation')
PARIS = ZoneInfo('Europe/Paris')
TIMES = ('project_added_at', 'audit_available_at', 'correction_verified_at', 'clean_audit_at')


def utc(value):
    if not isinstance(value, datetime):
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _write(connection, user_id, **values):
    factory = pg_insert if connection.dialect.name == 'postgresql' else sqlite_insert
    statement = factory(AccountActivation).values(user_id=str(user_id), **values)
    updates = {}
    for name in TIMES:
        current, incoming = getattr(AccountActivation, name), getattr(statement.excluded, name)
        updates[name] = case((current.is_(None), incoming), (incoming.is_(None), current),
                             (incoming < current, incoming), else_=current)
    updates['signup_observed'] = or_(AccountActivation.signup_observed, statement.excluded.signup_observed)
    updates['invited_signup'] = or_(AccountActivation.invited_signup, statement.excluded.invited_signup)
    connection.execute(statement.on_conflict_do_update(index_elements=['user_id'], set_=updates))


def _after_signup(mapper, connection, target):
    try:
        # Savepoint failure must not prevent account creation. Signup and its receipt commit together.
        with connection.begin_nested():
            invitation = connection.scalar(select(AccountInvite.id).where(
                AccountInvite.email == target.email.lower(), AccountInvite.used_at.is_(None),
                AccountInvite.expires_at > datetime.now(timezone.utc)).limit(1))
            _write(connection, target.id, signup_observed=True, invited_signup=bool(invitation),
                   **signup_attribution.signup_fields(connection, target))
    except Exception as error:
        log.warning('Signup milestone unavailable (%s)', type(error).__name__)


def _after_project(mapper, connection, target):
    try:
        with connection.begin_nested():
            _write(connection, target.owner_user_id, project_added_at=utc(target.created_at) or datetime.now(timezone.utc))
    except Exception as error:
        log.warning('Project milestone unavailable (%s)', type(error).__name__)


def install():
    for model, handler in ((User, _after_signup), (Project, _after_project)):
        if not event.contains(model, 'after_insert', handler):
            event.listen(model, 'after_insert', handler)


def report_facts(report, base_url):
    if not isinstance(report, dict) or not isinstance(report.get('issues'), dict) or dash.site_injoignable(report):
        return None
    host = (urlsplit(base_url).hostname or '').lower()
    pages = report.get('pages')
    if not host or not isinstance(pages, list):
        return None
    available = False
    for row in pages:
        if not isinstance(row, dict) or row.get('error') or type(row.get('status_code')) is not int or not 200 <= row['status_code'] < 300:
            continue
        if row.get('content_type') and 'html' not in str(row['content_type']).lower():
            continue
        for field in ('url', 'final_url'):
            try:
                parts = urlsplit(row.get(field) or '')
                available |= parts.scheme in {'http', 'https'} and (parts.hostname or '').lower() == host
            except ValueError:
                pass
    if not available:
        return None
    meta = report.get('meta') if isinstance(report.get('meta'), dict) else {}
    blocked = meta.get('blocked_by_host') if isinstance(meta.get('blocked_by_host'), dict) else {}
    counts = []
    for key, issue in report['issues'].items():
        if dash.is_delta_issue_key(key):
            continue
        if not isinstance(issue, dict):
            return {'clean': False}
        try:
            count = int(issue.get('count') or 0)
        except (ValueError, TypeError):
            return {'clean': False}
        if count < 0:
            return {'clean': False}
        counts.append(count)
    return {'clean': not meta.get('stopped_on_time_budget') and not blocked.get('count') and not any(counts)}


def has_baseline_defect(baseline, family, scope, normalize):
    """Extra analytics evidence must never interrupt the product's correction verifier."""
    try:
        issues = baseline.get('issues', {})
        original_scope = {normalize(url) for key in family
                          if isinstance(issues.get(key), dict) and int(issues[key].get('count') or 0) > 0
                          for url in dash.extract_impacted_pages(key, issues[key])}
        return bool(scope & original_scope)
    except Exception as error:
        log.warning('Correction milestone evidence unavailable (%s)', type(error).__name__)
        return False


def observe_report(database, slug, report, *, user_id=None, verified=False, crawl_ts=''):
    """Called by the crawl verifier, never from a client note or a declared task status."""
    try:
        with database.session() as db:
            query = select(Project).where(Project.slug == slug)
            if user_id is not None:
                query = query.where(Project.owner_user_id == str(user_id))
            projects = list(db.scalars(query))
        if len(projects) != 1:
            return
        project = projects[0]
        facts = report_facts(report, project.base_url)
        if facts is None:
            return
        now = datetime.now(timezone.utc)
        values = {'project_added_at': utc(project.created_at), 'audit_available_at': now}
        if facts['clean']:
            values['clean_audit_at'] = now
        if verified:
            values['correction_verified_at'] = now
        with database.session() as db:
            _write(db.connection(), project.owner_user_id, **values)
            db.commit()
        if not verified:
            try:
                from . import customer_follow_up
            except ImportError:
                import customer_follow_up
            customer_follow_up.observe(database, project, report, facts, crawl_ts=crawl_ts)
    except Exception as error:
        log.warning('Crawl milestone unavailable (%s)', type(error).__name__)


def period(start='', end='', *, now=None):
    today = (utc(now) or datetime.now(timezone.utc)).astimezone(PARIS).date()
    try:
        first = date.fromisoformat(start) if start else today - timedelta(days=29)
        last = date.fromisoformat(end) if end else today
        if first > last or (last - first).days > 365 or last > today:
            raise ValueError()
    except (ValueError, TypeError):
        raise ValueError('Choisissez une période valide de 366 jours maximum, jusqu’à aujourd’hui.') from None
    return first, last


def snapshot(database, *, start='', end='', now=None):
    now = utc(now) or datetime.now(timezone.utc)
    first, last = period(start, end, now=now)
    low = datetime.combine(first, time.min, PARIS).astimezone(timezone.utc)
    high = datetime.combine(last + timedelta(days=1), time.min, PARIS).astimezone(timezone.utc)
    with database.session() as db:
        rows = db.execute(select(User.created_at, AccountActivation, AcquisitionCampaign).outerjoin(
            AccountActivation, AccountActivation.user_id == User.id).where(
            User.is_admin.is_(False), User.created_at >= low, User.created_at < high,
            User.created_at <= now).outerjoin(AcquisitionCampaign,
            AcquisitionCampaign.id == AccountActivation.acquisition_campaign_id)).all()
    counts = [0, 0, 0, 0]
    clean = untracked = invited = 0
    weeks = {}
    acquisition = {}
    pending = [0, 0, 0]
    for created_at, activation, campaign in rows:
        if not activation or not activation.signup_observed:
            untracked += 1
            continue
        if activation.invited_signup:
            invited += 1
            continue
        created = utc(created_at)
        flags = [True]
        previous = created
        for name in TIMES[:3]:
            reached = utc(getattr(activation, name))
            flags.append(bool(flags[-1] and reached and previous <= reached <= now))
            previous = reached or previous
        clean_at = utc(activation.clean_audit_at)
        clean_only = bool(flags[2] and not flags[3] and clean_at and utc(activation.audit_available_at) <= clean_at <= now)
        day = created.astimezone(PARIS).date()
        monday = (day - timedelta(days=day.weekday())).isoformat()
        week = weeks.setdefault(monday, {'week': monday, 'counts': [0, 0, 0, 0], 'clean': 0})
        consented = utc(activation.acquisition_consented_at)
        attributed = campaign is not None and consented is not None and created <= consented <= now
        origin = (campaign.source, campaign.medium, campaign.name) if attributed else ('', '', '')
        group = acquisition.setdefault(origin, {'source': origin[0], 'medium': origin[1], 'campaign': origin[2],
                                                'counts': [0, 0, 0, 0], 'clean': 0})
        for i, reached in enumerate(flags):
            counts[i] += reached
            week['counts'][i] += reached
            group['counts'][i] += reached
        clean += clean_only
        week['clean'] += clean_only
        group['clean'] += clean_only
        pending[0] += not flags[1]
        pending[1] += flags[1] and not flags[2]
        pending[2] += flags[2] and not flags[3] and not clean_only
    steps = []
    for i, (key, label) in enumerate([('signup', 'Inscription'), ('site', 'Site ajouté'),
                                      ('audit', 'Analyse exploitable'), ('verified', 'Correction vérifiée')]):
        previous = counts[i - 1] if i else counts[0]
        steps.append({'key': key, 'label': label, 'count': counts[i],
                      'rate': round(100 * counts[i] / counts[0], 1) if counts[0] else None,
                      'previous_rate': round(100 * counts[i] / previous, 1) if previous else None})
    waiting = [{'label': label, 'count': count} for label, count in zip(
        ['Site à ajouter', 'Analyse à obtenir', 'Correction ou contrôle à terminer'], pending)]
    for week in weeks.values():
        week['rate'] = round(100 * week['counts'][3] / week['counts'][0], 1)
    for group in acquisition.values():
        group['rate'] = round(100 * group['counts'][3] / group['counts'][0], 1)
    value = counts[3] + clean
    return {'date_from': first.isoformat(), 'date_to': last.isoformat(), 'today': now.astimezone(PARIS).date().isoformat(), 'steps': steps,
            'weeks': [weeks[key] for key in sorted(weeks, reverse=True)], 'waiting': waiting,
            'acquisition': sorted(acquisition.values(), key=lambda row: (-row['counts'][0], row['source'], row['medium'], row['campaign'])),
            'clean_count': clean, 'value_count': value,
            'value_rate': round(100 * value / counts[0], 1) if counts[0] else None,
            'untracked': untracked, 'invited': invited, 'generated_at': now.astimezone(PARIS).strftime('%d/%m/%Y à %H:%M')}
