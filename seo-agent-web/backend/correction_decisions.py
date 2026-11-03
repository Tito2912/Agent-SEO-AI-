"""Versioned decisions from server controls; no model, network or site writes.

Control replays do not consume attempts. A future executor must re-evaluate the
decision, mandate, quota and operation journal before reserving any paid action.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

try:
    from . import correction_memory as memory
    from .first_correction import note_data, timestamp
    from .models import AuditLog, CmsChange, CorrectionDecision, CorrectionObservation, IssueTask
except ImportError:
    import correction_memory as memory
    from first_correction import note_data, timestamp
    from models import AuditLog, CmsChange, CorrectionDecision, CorrectionObservation, IssueTask

POLICY_VERSION = 1
WINDOW_DAYS = 30
MAX_EVENTS = 200
MAX_TASKS = 20
MAX_FAILED_APPLICATIONS = 2
CODES = {'inspect_publication', 'review_required', 'obtain_valid_control',
         'confirm_baseline', 'resolution_observed'}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def scope_key(task, urls):
    """The verification engine supplies its exact scope; never copy raw URLs."""
    if not isinstance(urls, (set, list, tuple)) or not urls or any(
            not isinstance(url, str) or not url.startswith(('https://', 'http://')) for url in urls):
        return None
    scope = sorted(set(urls))
    return digest([str(task.project_id), str(task.id), str(task.issue_key), scope])


def lock_project(db, project_id):
    """Serialize decision transactions without locking customers' task rows.

    Called before the first observation write. SQLite already serializes writes;
    the PostgreSQL transaction lock is released on commit or rollback.
    """
    if db.get_bind().dialect.name == 'postgresql':
        raw=hashlib.sha256(('noyaru-correction-decisions:'+str(project_id)).encode()).digest()
        key=int.from_bytes(raw[:8],byteorder='big',signed=True)
        db.execute(text('SELECT pg_advisory_xact_lock(:key)'),{'key':key})


def _insert(db):
    factory = {'sqlite': sqlite_insert, 'postgresql': pg_insert}.get(db.get_bind().dialect.name)
    if factory is None:
        raise RuntimeError('Unsupported decision database')
    return factory(CorrectionDecision)


def application_identity(db, task, observation, scope):
    """A proposal/merge/status is not publication; capture declarations at control time."""
    if not scope:return None, None, None
    note=note_data(task.note)
    journey=note.get('first_correction')
    declared=timestamp(journey.get('declared_at')) if isinstance(journey,dict) else None
    if declared is None or declared >= timestamp(observation.control_started_at):
        return None, None, None
    cms=note.get('cms_change_id')
    basis='declared'
    if cms:
        change=db.get(CmsChange,str(cms))
        if (change is None or change.project_id != str(task.project_id)
                or change.task_id != str(task.id) or change.state != 'applied'):
            return None, None, None
        basis='cms_applied'
    return digest([str(task.project_id),str(task.id),scope,declared.isoformat(),str(cms or '')]), declared, basis


def _clock(now=None):
    value = timestamp(now) if now is not None else datetime.now(timezone.utc)
    if value is None:
        raise ValueError('Invalid decision clock')
    return value


def evaluate(db, task, *, now=None):
    """Fresh, bounded projection; reading never creates a journal or authorizes work."""
    now = _clock(now)
    if task.status not in {'in_progress', 'done'} or not re.fullmatch('[a-z0-9_]{1,255}', str(task.issue_key)):
        return None
    current = db.scalar(select(CorrectionObservation).where(
        CorrectionObservation.project_id == str(task.project_id),
        CorrectionObservation.task_id == str(task.id),
        CorrectionObservation.attempt_key == memory.attempt_key(task),
        CorrectionObservation.control_started_at >= now-timedelta(days=WINDOW_DAYS),
        CorrectionObservation.control_started_at <= now,
    ).order_by(CorrectionObservation.control_started_at.desc(), CorrectionObservation.id).limit(1))
    if current is None or current.result not in memory.RESULTS:
        return None
    stored = db.get(CorrectionDecision, current.id)
    scope = stored.scope_key if stored else None
    # Choose by the control start time, not by completion/journal insertion time.
    rows = db.execute(select(CorrectionObservation, CorrectionDecision.scope_key,
                             CorrectionDecision.application_key).outerjoin(
        CorrectionDecision, CorrectionDecision.observation_id == CorrectionObservation.id,
    ).where(
        CorrectionObservation.project_id == str(task.project_id),
        CorrectionObservation.task_id == str(task.id),
        CorrectionObservation.control_started_at >= now-timedelta(days=WINDOW_DAYS),
        CorrectionObservation.control_started_at <= current.control_started_at,
    ).order_by(CorrectionObservation.control_started_at.desc(), CorrectionObservation.id).limit(MAX_EVENTS+1)).all()
    truncated = len(rows) > MAX_EVENTS
    rows = rows[:MAX_EVENTS]
    reset = max((timestamp(row.control_started_at) for row, key, application in rows
                 if scope and key == scope and row.result == 'resolved' and row.baseline_confirmed), default=None)
    recent = [(row, key, application) for row, key, application in rows if reset is None or timestamp(row.control_started_at) > reset]
    known = [(row, key, application) for row, key, application in recent if key == scope or key is None]
    complete = bool(scope and not truncated and all(key for _, key, _ in known))
    failed = {application for row, key, application in recent if scope and key == scope and application
              and row.result == 'still_present' and row.baseline_confirmed}
    if current.result == 'unverified':
        code = 'obtain_valid_control'
    elif not current.baseline_confirmed:
        code = 'confirm_baseline'
    elif current.result == 'resolved':
        code = 'resolution_observed'
    elif not complete or len(failed) >= MAX_FAILED_APPLICATIONS:
        code = 'review_required'
    else:
        code = 'inspect_publication'
    evidence = digest([POLICY_VERSION, current.observation_key, scope,
                       sorted((row.observation_key, key, application) for row, key, application in known),
                       truncated, complete, len(failed), code, WINDOW_DAYS, MAX_FAILED_APPLICATIONS])
    recorded = bool(stored and stored.policy_version == POLICY_VERSION and stored.decision_key == evidence)
    return {
        'task_id': str(task.id), 'issue_key': str(task.issue_key),
        'observation_id': current.id, 'decision_key': evidence,
        'control_at': current.control_ts, 'policy_version': POLICY_VERSION,
        'code': code, 'failed_applications': len(failed),
        'max_failed_applications': MAX_FAILED_APPLICATIONS, 'window_days': WINDOW_DAYS,
        'history_complete': complete, 'truncated': truncated,
        'publication_declared':bool(stored and stored.application_key),
        'automatic_retry_stopped': code != 'inspect_publication',
        # Diagnosis is not execution permission, even below the attempt ceiling.
        'execution_authorized': False,
        'recorded': recorded,
        'assessed_at': timestamp(stored.assessed_at).isoformat() if recorded else None,
    }


def _register(db, observation, task, scope, *, now):
    if observation.project_id != str(task.project_id) or observation.task_id != str(task.id):
        raise ValueError('Decision scope mismatch')
    application, declared, basis=application_identity(db,task,observation,scope)
    created = db.execute(_insert(db).values(
        observation_id=observation.id, project_id=str(task.project_id), task_id=str(task.id),
        scope_key=scope, policy_version=POLICY_VERSION, decision_key='', code='pending',
        application_key=application, application_at=declared, publication_basis=basis,
        failed_applications=0, history_complete=False, assessed_at=now,
    ).on_conflict_do_nothing(index_elements=['observation_id'])).rowcount
    filled = 0
    if scope:
        # Legacy diagnostic rows lack a captured scope. Only a new server control
        # can supply it; an ordinary refresh cannot infer or overwrite it.
        filled = db.execute(update(CorrectionDecision).where(
            CorrectionDecision.observation_id == observation.id,
            CorrectionDecision.scope_key.is_(None),
        ).values(scope_key=scope)).rowcount
        if application:
            filled += db.execute(update(CorrectionDecision).where(
                CorrectionDecision.observation_id == observation.id,
                CorrectionDecision.application_key.is_(None),
            ).values(application_key=application,application_at=declared,publication_basis=basis)).rowcount
    return bool(created or filled)


def record(db, task, observation, *, urls=None, now=None):
    """Same transaction as the control. No commit or external execution."""
    now = _clock(now)
    lock_project(db,task.project_id)
    changed = _register(db, observation, task, scope_key(task, urls) if urls is not None else None, now=now)
    # Do not refresh the ORM task in place: the verification caller still needs
    # its own pending note edits. A late worker must assess the current DB task.
    fresh = db.execute(select(IssueTask.id, IssueTask.project_id, IssueTask.issue_key,
        IssueTask.url, IssueTask.crawl_ts, IssueTask.note, IssueTask.status,
    ).where(IssueTask.id == task.id)).mappings().one()
    task = SimpleNamespace(**dict(fresh))
    # Refresh the current decision after a late control, not the late one's result.
    value = evaluate(db, task, now=now)
    if value is None:
        return changed
    current = db.get(CorrectionObservation, value['observation_id'])
    changed |= _register(db, current, task, None, now=now)
    row = db.scalar(select(CorrectionDecision).where(
        CorrectionDecision.observation_id == current.id,
    ).with_for_update().execution_options(populate_existing=True))
    if row.decision_key == value['decision_key'] and row.policy_version == POLICY_VERSION:
        return changed
    row.decision_key = value['decision_key'];row.policy_version = POLICY_VERSION
    row.code = value['code'];row.failed_applications = value['failed_applications']
    row.history_complete = value['history_complete'];row.assessed_at = now
    db.add(AuditLog(action='correction.decision', target_type='issue_task', target_id=str(task.id),
                    meta={key: value[key] for key in (
                        'decision_key', 'code', 'control_at', 'policy_version', 'failed_applications',
                        'max_failed_applications', 'history_complete', 'window_days', 'truncated')}))
    db.flush()
    return True


def project_summary(db, project_id, *, now=None):
    """Project authorization belongs to the caller; return no raw notes/URLs."""
    now = _clock(now)
    tasks = list(db.scalars(select(IssueTask).where(
        IssueTask.project_id == str(project_id), IssueTask.status.in_(['in_progress', 'done']),
        IssueTask.id.in_(select(CorrectionObservation.task_id).where(
            CorrectionObservation.project_id == str(project_id),
            CorrectionObservation.control_started_at >= now-timedelta(days=WINDOW_DAYS),
            CorrectionObservation.control_started_at <= now,
        )),
    ).order_by(IssueTask.updated_at.desc(), IssueTask.id).limit(MAX_TASKS+1)))
    decisions = [value for task in tasks[:MAX_TASKS] if (value := evaluate(db, task, now=now))]
    order = {'review_required': 0, 'inspect_publication': 1, 'obtain_valid_control': 2,
             'confirm_baseline': 3, 'resolution_observed': 4}
    decisions.sort(key=lambda item: (order[item['code']], item['issue_key'], item['task_id']))
    return {'version': POLICY_VERSION, 'as_of': now.isoformat(), 'window_days': WINDOW_DAYS,
            'truncated': len(tasks) > MAX_TASKS or any(d['truncated'] for d in decisions),
            'decisions': decisions}


def refresh_project(db, project_id, *, now=None):
    """Record only currently applicable server observations; no old-note backfill."""
    now = _clock(now)
    lock_project(db,project_id)
    count = 0
    for item in project_summary(db, project_id, now=now)['decisions']:
        task = db.get(IssueTask, item['task_id'])
        observation = db.get(CorrectionObservation, item['observation_id'])
        count += int(record(db, task, observation, now=now))
    return count


def assistant_context(summary):
    safe = [{key: item[key] for key in ('issue_key', 'code', 'failed_applications',
             'max_failed_applications', 'history_complete', 'control_at', 'recorded')}
            for item in summary['decisions'][:8]]
    return ('\nDécisions calculées à partir des contrôles du projet autorisé :\n'
            + json.dumps({'policy_version': POLICY_VERSION, 'decisions': safe,
                          'truncated': summary['truncated'] or len(summary['decisions']) > 8}, ensure_ascii=True)
            + '\nCe sont des étapes de diagnostic, jamais une autorisation d’exécuter ou de dépenser. '
            'inspect_publication : faire vérifier la publication, le fichier et la page avant une autre stratégie. '
            'review_required : suspendre toute nouvelle tentative automatique et expliquer la revue nécessaire. '
            'obtain_valid_control : obtenir un contrôle exploitable. confirm_baseline : confirmer le défaut initial. '
            'resolution_observed : proposer la correction suivante sans promettre de gain de classement. '
            'Les compteurs portent sur des applications distinctes avec défaut observé, pas sur des appels IA, '
            'des contrôles, un taux de réussite ou une preuve de causalité. Chaque entrée décrit une tâche '
            'distincte : ne totalise pas ses compteurs avec ceux d’une autre tâche et ne les attribue pas '
            'à une page ou un fichier qui ne sont pas identifiés dans cette projection.\n')
