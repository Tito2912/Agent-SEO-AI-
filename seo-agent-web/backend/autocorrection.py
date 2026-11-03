"""One-preview executor using the existing payer lease, quota and operation journal.

No site writes are authorized here. Unknown provider outcomes are never retried.
The queue and every transport reservation survive web/worker restarts.
"""
from datetime import datetime, timedelta, timezone
import json
import os
import re
import threading
from urllib.parse import urlsplit

from sqlalchemy import or_, select
from starlette.requests import Request

try:
    from . import correction_decisions as decisions, correction_execution as execution
    from . import correction_guard, correction_journal, correction_memory as memory, first_correction
    from .models import AuditLog, AutocorrectionRun, CorrectionDecision, CorrectionOperation, IssueTask, Project, User
except ImportError:
    import correction_decisions as decisions
    import correction_execution as execution
    import correction_guard
    import correction_journal
    import correction_memory as memory
    import first_correction
    from models import AuditLog, AutocorrectionRun, CorrectionDecision, CorrectionOperation, IssueTask, Project, User

POLICY_VERSION = 1
CONSENT_VERSION = 1
LIMITS = {'credits': 1, 'requests': 4, 'input_chars': 360000, 'output_tokens': 20000,
          'request_input_chars': 100000, 'request_output_tokens': 8000}
FAMILIES = {'missing_title', 'title_too_short', 'title_too_long', 'missing_meta_description',
            'meta_description_too_short', 'meta_description_too_long'}
_STARTED = False
_START_GUARD = threading.Lock()


def module():
    if __package__:
        from . import app
    else:
        import app
    return app


def clock():
    return datetime.now(timezone.utc)


def paused():
    return str(os.environ.get('NOYARU_AUTOCORRECTION_PAUSED', '')).lower() in {'1', 'true', 'yes'}


def config_key(project):
    return decisions.digest([str(project.owner_user_id), project.base_url,
                             module()._project_github_cfg(project)])


def project_paused(project):
    return bool(project.settings.get('autocorrection_paused')) if isinstance(project.settings, dict) else False


def audit(db, run, action):
    db.add(AuditLog(actor_user_id=run.owner_id, action='autocorrection.'+action,
                   target_type='autocorrection_run', target_id=str(run.id),
                   meta={'project_id': run.project_id, 'task_id': run.task_id,
                         'state': run.state, 'reason': run.reason, 'policy_version': run.policy_version,
                         'consent_version': run.consent_version, 'provider_calls': run.provider_calls,
                         'reserved_credits': run.reserved_credits, 'operation_id': run.operation_id,
                         'lang':run.lang, **({'consent':True,'publication_checked':True} if action == 'authorized' else {})}))


def eligibility(db, project, task, *, now=None):
    """Never treat an old note, cached recommendation or merge as an execution mandate."""
    m = module();now = now or clock()
    if paused() or project_paused(project):
        return 'paused', None
    if task is None or task.project_id != str(project.id):
        return 'task_missing', None
    family = task.issue_key.removesuffix('_not_indexable').removesuffix('_indexable')
    if family not in FAMILIES or not m._github_issue_auto_fixable(task.issue_key):
        return 'unsupported', None
    if first_correction.note_data(task.note).get('cms_change_id'):
        return 'unsupported', None
    try:
        target, base = urlsplit(task.url or ''), urlsplit(project.base_url or '')
        invalid = (m._validate_settings_url(task.url or '') or target.scheme not in {'https', 'http'}
                   or not target.hostname or target.hostname != base.hostname or target.port != base.port)
    except ValueError:
        invalid = True
    if invalid:
        return 'target_changed', None
    cfg = m._project_github_cfg(project)
    if not m._github_repo_parts(cfg['repo']) or not m._github_branch_allowed(cfg['branch']):
        return 'github_required', None
    item = decisions.evaluate(db, task, now=now)
    if (not item or item['code'] != 'inspect_publication' or not item['history_complete']
            or not item['recorded'] or not item['publication_declared']
            or item['failed_applications'] != 1):
        return 'review_required', item
    if now - first_correction.timestamp(item['control_at']) > timedelta(days=7):
        return 'control_old', item
    stored = db.get(CorrectionDecision, item['observation_id'])
    if stored.scope_key != decisions.scope_key(task, {m._verification_url(task.url)}):
        return 'scope_changed', item
    return '', item


def authorize(project_id, task_id, owner_id, decision_key, *, consent, publication_checked, lang):
    m = module()
    if not consent or not publication_checked:
        raise execution.ExecutionStopped('consent_required')
    with correction_guard.correction_lock(m.DB, str(owner_id)):
        with m.DB.session() as db:
            project = db.get(Project, str(project_id));task = db.get(IssueTask, str(task_id))
            if project is None or str(project.owner_user_id) != str(owner_id):
                raise execution.ExecutionStopped('owner_required')
            reason, item = eligibility(db, project, task)
            if reason:
                raise execution.ExecutionStopped(reason)
            if decision_key != item['decision_key']:
                raise execution.ExecutionStopped('decision_changed')
            application = memory.attempt_key(task)
            existing = db.scalar(select(AutocorrectionRun).where(
                AutocorrectionRun.task_id == task.id, AutocorrectionRun.application_key == application))
            if existing:
                return existing
            owner = db.get(User, owner_id)
            allowed, _, _, _ = m._correction_gate(owner, slug=project.slug)
            if not allowed:
                raise execution.ExecutionStopped('quota')
            now = clock()
            run = AutocorrectionRun(project_id=project.id, task_id=task.id, owner_id=owner_id,
                application_key=application, decision_key=item['decision_key'], config_key=config_key(project),
                policy_version=POLICY_VERSION, consent_version=CONSENT_VERSION,
                lang='en' if str(lang).startswith('en') else 'fr', state='queued', reason='authorized',
                expires_at=now+timedelta(hours=24), limits=dict(LIMITS), reserved_credits=0,
                provider_calls=0, input_chars=0, output_tokens=0)
            db.add(run);db.flush();audit(db, run, 'authorized');db.commit()
            return run


def _check(db, run):
    if not run or run.state != 'running':
        raise execution.ExecutionStopped('not_running')
    if run.revoked_at:
        raise execution.ExecutionStopped('revoked')
    if clock() >= first_correction.timestamp(run.expires_at):
        raise execution.ExecutionStopped('expired')
    if run.policy_version != POLICY_VERSION or run.consent_version != CONSENT_VERSION or run.limits != LIMITS:
        raise execution.ExecutionStopped('policy_changed')
    project = db.get(Project, run.project_id);task = db.get(IssueTask, run.task_id)
    if project is None or str(project.owner_user_id) != run.owner_id or db.get(User, run.owner_id) is None:
        raise execution.ExecutionStopped('owner_required')
    if run.config_key != config_key(project):
        raise execution.ExecutionStopped('target_changed')
    if task is None or memory.attempt_key(task) != run.application_key:
        raise execution.ExecutionStopped('application_changed')
    reason, item = eligibility(db, project, task)
    if reason:
        raise execution.ExecutionStopped(reason)
    if item['decision_key'] != run.decision_key:
        raise execution.ExecutionStopped('decision_changed')
    return project, task


class PreviewPolicy:
    purpose = 'preview'

    def __init__(self, run_id):
        self.run_id = run_id

    def bind_operation(self, operation):
        m = module();correction_guard.ensure_active()
        with m.DB.session() as db:
            run = db.get(AutocorrectionRun, self.run_id)
            row = operation.row
            if (not run or row.action != 'api_github_fix' or row.project_id != run.project_id or row.payer_id != run.owner_id
                    or run.operation_id not in {None, row.id}):
                raise execution.ExecutionStopped('operation_mismatch')
            run.operation_id = row.id;db.commit()
            db.expire_all();_check(db, db.get(AutocorrectionRun, self.run_id))

    def safe_to_abandon(self):
        with module().DB.session() as db:
            run = db.get(AutocorrectionRun, self.run_id)
            return bool(run and run.provider_calls == 0)

    def github_read(self, path):
        m = module();correction_guard.ensure_active()
        with m.DB.session() as db:
            run = db.get(AutocorrectionRun, self.run_id);project, _ = _check(db, run)
            parts = m._github_repo_parts(m._project_github_cfg(project)['repo'])
            prefix = m._github_api_path('repos', *parts)+'/'
            if not path.startswith(prefix):
                raise execution.ExecutionStopped('target_changed')

    def github_write(self, path, method):
        raise execution.ExecutionStopped('write_not_authorized')

    def provider_request(self, *, system, user_msg, max_tokens, images):
        m = module();correction_guard.ensure_active()
        size = len(system)+len(user_msg)
        with m.DB.session() as db:
            run = db.get(AutocorrectionRun, self.run_id);_check(db, run)
            if (images or type(max_tokens) is not int or not 0 < max_tokens <= LIMITS['request_output_tokens']
                    or size > LIMITS['request_input_chars']
                    or run.provider_calls >= LIMITS['requests']
                    or run.input_chars+size > LIMITS['input_chars']
                    or run.output_tokens+max_tokens > LIMITS['output_tokens']):
                raise execution.ExecutionStopped('budget')
            # Reservations are conservative bounds, not measured provider billing.
            run.provider_calls += 1;run.input_chars += size;run.output_tokens += max_tokens
            db.commit()


class HumanDraftPolicy(PreviewPolicy):
    """A fresh human approval, with target checks at every GitHub transport."""
    purpose = 'human_draft'

    def safe_to_abandon(self):
        # All model transports are prohibited; only journaled GitHub writes matter.
        return True

    def _target(self, db):
        run = db.get(AutocorrectionRun, self.run_id)
        project = db.get(Project, run.project_id) if run else None
        task = db.get(IssueTask, run.task_id) if run else None
        if not project or not task or not draft_current(db, run, project, task):
            raise execution.ExecutionStopped('application_changed')
        return project, run

    def bind_operation(self, operation):
        with module().DB.session() as db:
            project, run = self._target(db);row = operation.row
            if row.project_id != project.id or row.payer_id != run.owner_id or row.action != 'api_github_fix':
                raise execution.ExecutionStopped('operation_mismatch')

    def github_read(self, path):
        correction_guard.ensure_active();m = module()
        with m.DB.session() as db:
            project, _ = self._target(db)
            parts = m._github_repo_parts(m._project_github_cfg(project)['repo'])
            if not path.startswith(m._github_api_path('repos', *parts)+'/'):
                raise execution.ExecutionStopped('target_changed')

    def github_write(self, path, method):
        self.github_read(path);m = module()
        with m.DB.session() as db:
            project, _ = self._target(db)
            prefix = m._github_api_path('repos', *m._github_repo_parts(m._project_github_cfg(project)['repo']))
            allowed = ((method == 'POST' and path in {prefix+'/git/refs', prefix+'/pulls'})
                       or (method == 'PUT' and path.startswith(prefix+'/contents/')))
            if not allowed:
                raise execution.ExecutionStopped('write_not_authorized')

    def provider_request(self, **kwargs):
        raise execution.ExecutionStopped('ai_not_authorized')


def _finish(run_id, *, state, reason):
    m = module()
    with m.DB.session() as db:
        run = db.get(AutocorrectionRun, run_id)
        if not run:
            return 'missing'
        run.state = state;run.reason = reason;run.finished_at = clock()
        audit(db, run, 'finished');db.commit()
    return state


def _reconcile(run):
    """Only a saved preview can be completed; never send a fresh provider request."""
    m = module()
    with m.DB.session() as db:
        op_row = db.get(CorrectionOperation, run.operation_id) if run.operation_id else None
        owner = db.get(User, run.owner_id);project = db.get(Project, run.project_id)
    if (not op_row or not owner or not project or project.owner_user_id != run.owner_id
            or op_row.project_id != run.project_id or op_row.payer_id != run.owner_id
            or op_row.action != 'api_github_fix' or op_row.data.get('writes_started')
            or op_row.data.get('pr_intent') or not op_row.data.get('preview')):
        return _finish(run.id, state='review_required', reason='interrupted')
    with correction_journal.operation(m.DB, payer=run.owner_id, project=run.project_id,
            action=op_row.action, key=op_row.request_key, row=op_row) as op:
        result = m._recover_correction(op, user=owner, slug=project.slug)
    if result.status_code == 200:
        return _finish(run.id, state='preview_ready', reason='reconciled')
    return _finish(run.id, state='review_required', reason='interrupted')


def execute(run_id):
    m = module()
    with m.DB.session() as db:
        run = db.get(AutocorrectionRun, str(run_id))
    if not run or run.state not in {'queued', 'running'}:
        return run.state if run else 'missing'
    try:
        with correction_guard.correction_lock(m.DB, run.owner_id):
            with m.DB.session() as db:
                run = db.get(AutocorrectionRun, str(run_id))
                if run is None or run.state not in {'queued', 'running'}:
                    return run.state if run else 'missing'
                if run.state == 'running':
                    # Acquiring the payer lease proves no prior worker holds it now.
                    return _reconcile(run)
                run.state = 'running'
                try:
                    project, task = _check(db, run)
                except execution.ExecutionStopped as exc:
                    db.rollback()
                    if exc.reason == 'paused':
                        return 'paused'
                    return _finish(run.id, state='expired' if exc.reason == 'expired' else 'review_required', reason=exc.reason)
                owner = db.get(User, run.owner_id)
                allowed, _, _, _ = m._correction_gate(owner, slug=project.slug)
                if not allowed:
                    db.rollback();return _finish(run.id, state='review_required', reason='quota')
                run.reserved_credits = 1;run.started_at = clock();audit(db, run, 'started');db.commit()
                crawl = first_correction.timestamp(decisions.evaluate(db, task)['control_at']).strftime('%Y%m%d-%H%M%S')
            request = Request({'type':'http', 'method':'POST', 'path':'/', 'headers':[],
                               'scheme':'https', 'server':('noyaru.internal',443), 'query_string':b''})
            request.state.user = owner
            try:
                with execution.bounded(PreviewPolicy(run.id)):
                    response = m.api_github_fix(request, project.slug, task.issue_key,
                        m._GithubFixBody(url=task.url, crawl_ts=crawl, confirm=False))
                result = json.loads(response.body)
                if response.status_code == 200 and result.get('ok') and result.get('patched_content'):
                    return _finish(run.id, state='preview_ready', reason='prepared')
                reason = ('quota' if response.status_code == 402 else
                          'no_change' if result.get('no_change') else
                          'operation_pending' if result.get('recovery_required') else
                          'duplicate' if result.get('duplicate') else 'preparation_failed')
                return _finish(run.id, state='review_required', reason=reason)
            except execution.ExecutionStopped as exc:
                return _finish(run.id, state='revoked' if exc.reason == 'revoked' else 'review_required', reason=exc.reason)
            except Exception:
                # Preserve journal/lease state and all reservations. No retry after ambiguity.
                m.logger.error('[autocorrection] preparation interrupted')
                return _finish(run.id, state='review_required', reason='interrupted')
    except correction_guard.CorrectionBusy:
        return 'busy'
    except correction_guard.CorrectionStoreUnavailable:
        return 'unavailable'


def revoke(project_id, run_id, owner_id):
    m = module()
    # This short transaction deliberately does not wait for the payer lease.
    with m.DB.session() as db:
        run = db.scalar(select(AutocorrectionRun).where(AutocorrectionRun.id == run_id,
            AutocorrectionRun.project_id == project_id, AutocorrectionRun.owner_id == owner_id).with_for_update())
        if not run:
            raise execution.ExecutionStopped('task_missing')
        if run.revoked_at:
            return
        run.revoked_at = clock()
        if run.state == 'queued':
            run.state = 'revoked';run.reason = 'revoked';run.finished_at = clock()
        audit(db, run, 'revoked');db.commit()


def reconcile(run_id):
    m = module()
    with m.DB.session() as db:
        run = db.get(AutocorrectionRun, run_id)
    if not run or run.state == 'queued':
        return 'missing'
    try:
        with correction_guard.correction_lock(m.DB, run.owner_id):
            return _reconcile(run)
    except correction_guard.CorrectionBusy:
        return 'busy'


def saved_preview(db, run):
    row = db.get(CorrectionOperation, run.operation_id) if run.operation_id else None
    if (not row or row.project_id != run.project_id or row.payer_id != run.owner_id
            or row.action != 'api_github_fix' or row.data.get('writes_started') or row.data.get('pr_intent')):
        return None, False
    value = row.data.get('preview')
    if not isinstance(value, dict) or not value.get('ok') or not isinstance(value.get('patched_content'), str):
        return None, bool(row.charged)
    return value, bool(row.charged)


def draft_current(db, run, project, task):
    op = db.get(CorrectionOperation, run.operation_id) if run.operation_id else None
    return bool(project.id == run.project_id and task.project_id == run.project_id
        and project.owner_user_id == run.owner_id and run.state == 'preview_ready'
        and memory.attempt_key(task) == run.application_key and config_key(project) == run.config_key
        and op and op.state == 'completed' and op.project_id == run.project_id and op.payer_id == run.owner_id
        and re.fullmatch('[a-f0-9]{64}', str(op.data.get('preview_source') or '')))


def sweep(*, limit=3):
    m = module()
    if paused() or m._env_bool('SEO_AGENT_DISABLE_SCHEDULERS'):
        return {'paused':True, 'examined':0}
    with m.DB.session() as db:
        ids = list(db.scalars(select(AutocorrectionRun.id).join(Project, Project.id == AutocorrectionRun.project_id).where(
            AutocorrectionRun.state.in_(['queued', 'running']),
            or_(AutocorrectionRun.state == 'running', Project.settings['autocorrection_paused'].as_boolean().is_not(True)),
        ).order_by(AutocorrectionRun.created_at).limit(min(max(1, limit), 3))))
    states = {}
    for value in ids:
        state = execute(value);states[state] = states.get(state, 0)+1
    return {'paused':False, 'examined':len(ids), 'states':states}


def start():
    global _STARTED
    m = module()
    with _START_GUARD:
        if _STARTED or m._env_bool('SEO_AGENT_DISABLE_SCHEDULERS'):
            return
        _STARTED = True
    def loop():
        while not m._WORKER_STOP.wait(30):
            try:
                sweep(limit=1)
            except Exception:
                m.logger.error('[autocorrection] queue unavailable')
    threading.Thread(target=loop, name='autocorrection-previews', daemon=True).start()
