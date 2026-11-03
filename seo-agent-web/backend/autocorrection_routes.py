"""Owner mandates, team-visible receipts and separate human draft confirmation."""
import json
from urllib.parse import quote

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

try:
    from . import autocorrection as service, correction_decisions as decisions
    from . import correction_execution as execution, correction_guard, correction_memory as memory, first_correction, i18n
    from .autocorrection_copy import strings
    from .models import AutocorrectionRun, IssueTask
except ImportError:
    import autocorrection as service
    import correction_decisions as decisions
    import correction_execution as execution
    import correction_guard
    import correction_memory as memory
    import first_correction
    import i18n
    from autocorrection_copy import strings
    from models import AutocorrectionRun, IssueTask

router = APIRouter()


def base(slug):
    return '/projects/'+quote(slug, safe='')+'/corrections/autonomy'


def owner_project(request, slug):
    project = service.module()._db_project_or_404(request, slug)
    if str(project.owner_user_id) != str(request.state.user.id):
        raise HTTPException(403, 'owner_required')
    return project


def redirect(slug, msg='', error=''):
    # All status messages are fixed codes, never provider/client text or redirect URLs.
    return RedirectResponse(base(slug)+('?'+'msg='+quote(msg) if msg else '?'+'error='+quote(error) if error else ''),
                            status_code=303, headers={'Cache-Control':'no-store'})


def throttle(request, project):
    retry = service.module()._rate_limit_retry_after(bucket='autocorrection_owner',
        subject=str(request.state.user.id)+':'+str(project.id), limit=12, window_s=60)
    if isinstance(retry, int):
        raise HTTPException(429, 'too_many_requests', headers={'Retry-After':str(retry)})


@router.get('/projects/{slug}/corrections/autonomy')
def page(request: Request, slug: str, msg: str = '', error: str = ''):
    m = service.module();project = m._db_project_or_404(request, slug);copy = strings(i18n.current_lang())
    items = [];runs = [];unavailable = False;limited = False
    try:
        with m.DB.session() as db:
            summary = decisions.project_summary(db, project.id);limited = summary['truncated']
            for item in summary['decisions']:
                task = db.get(IssueTask, item['task_id'])
                reason, value = service.eligibility(db, project, task)
                existing = db.scalar(select(AutocorrectionRun.id).where(
                    AutocorrectionRun.task_id == task.id,
                    AutocorrectionRun.application_key == memory.attempt_key(task)))
                if not reason and not existing:
                    items.append({'task':task, 'decision':value})
            rows = list(db.scalars(select(AutocorrectionRun).where(
                AutocorrectionRun.project_id == project.id).order_by(AutocorrectionRun.created_at.desc()).limit(21)))
            limited |= len(rows) > 20
            for row in rows[:20]:
                task = db.get(IssueTask, row.task_id)
                preview, charged = service.saved_preview(db, row)
                current = bool(task and service.draft_current(db, row, project, task))
                runs.append({'row':row, 'task':task, 'preview':preview, 'charged':int(charged),
                    'draft_allowed':bool(preview and current and row.state == 'preview_ready'),
                    'recoverable':bool(preview and row.reason != 'source_changed' and row.state not in {'queued','preview_ready'}),
                    'expires':first_correction.timestamp(row.expires_at).strftime('%Y-%m-%d %H:%M UTC'),
                    'tracking':'/projects/'+quote(slug,safe='')+'/corrections?task_id='+quote(row.task_id,safe='')+'#correction-task-'+quote(row.task_id,safe='')})
    except SQLAlchemyError:
        m.logger.error('[autocorrection] tracking unavailable');unavailable = True
    return m.templates.TemplateResponse('autocorrection.html', {'request':request, 'project':project,
        'slug':slug, 'copy':copy, 'items':items, 'runs':runs, 'limited':limited, 'unavailable':unavailable,
        'owner':str(project.owner_user_id) == str(request.state.user.id), 'base':base(slug),
        'paused':service.paused() or service.project_paused(project),
        'project_paused':service.project_paused(project),
        'msg':msg if msg in {'queued','revoked','draft_done'} else '',
        'error':copy['reasons'].get(error, '')}, status_code=503 if unavailable else 200,
        headers={'Cache-Control':'no-store', 'Referrer-Policy':'no-referrer'})


@router.post('/projects/{slug}/corrections/autonomy/authorize')
def authorize(request: Request, slug: str, task_id: str = Form(''), decision_key: str = Form(''),
              consent: bool = Form(False), publication_checked: bool = Form(False)):
    project = owner_project(request, slug)
    throttle(request, project)
    try:
        run = service.authorize(project.id, task_id, str(request.state.user.id), decision_key,
            consent=consent, publication_checked=publication_checked, lang=i18n.current_lang())
    except execution.ExecutionStopped as exc:
        if exc.reason == 'task_missing':
            raise HTTPException(404, 'correction_not_found')
        return redirect(slug, error=exc.reason)
    except (SQLAlchemyError, correction_guard.CorrectionStoreUnavailable, correction_guard.CorrectionBusy):
        return redirect(slug, error='interrupted')
    return redirect(slug, msg='queued' if run.state in {'queued','running'} else '')


@router.post('/projects/{slug}/corrections/autonomy/{run_id}/revoke')
def revoke(request: Request, slug: str, run_id: str):
    project = owner_project(request, slug)
    try:
        service.revoke(project.id, run_id, str(request.state.user.id))
    except execution.ExecutionStopped:
        raise HTTPException(404, 'preparation_not_found')
    return redirect(slug, msg='revoked')


@router.post('/projects/{slug}/corrections/autonomy/pause')
def pause(request: Request, slug: str, paused: bool = Form(True)):
    m = service.module();project = owner_project(request, slug)
    with m._automation_settings_transaction(project.id) as (db, fresh):
        fresh.settings = {**(fresh.settings or {}), 'autocorrection_paused':paused}
        m._journaliser_mode_auto(request, 'autocorrection.pause', slug, {'paused':paused}, db=db, project=fresh)
    return redirect(slug)


def own_run(request, slug, run_id):
    m = service.module();project = owner_project(request, slug)
    with m.DB.session() as db:
        run = db.get(AutocorrectionRun, run_id)
        if not run or run.project_id != str(project.id) or run.owner_id != str(request.state.user.id):
            raise HTTPException(404, 'preparation_not_found')
    return project, run


@router.post('/projects/{slug}/corrections/autonomy/{run_id}/reconcile')
def reconcile(request: Request, slug: str, run_id: str):
    project, _ = own_run(request, slug, run_id);throttle(request, project)
    state = service.reconcile(run_id)
    return redirect(slug, error='' if state == 'preview_ready' else 'interrupted')


@router.post('/projects/{slug}/corrections/autonomy/{run_id}/draft')
def draft(request: Request, slug: str, run_id: str, confirm: bool = Form(False)):
    m = service.module();project, run = own_run(request, slug, run_id)
    throttle(request, project)
    if not confirm:
        return redirect(slug, error='consent_required')
    with m.DB.session() as db:
        task = db.get(IssueTask, run.task_id);preview, _ = service.saved_preview(db, run)
        if not task or not preview or not service.draft_current(db, run, project, task):
            return redirect(slug, error='application_changed')
    # A fresh human approval enters the existing guarded confirmation path.
    # Its context rechecks the target on every transport and forbids all AI calls.
    try:
        with execution.bounded(service.HumanDraftPolicy(run.id)):
            response = m.api_github_fix(request, slug, task.issue_key, m._GithubFixBody(
                url=task.url, crawl_ts=task.crawl_ts, confirm=True, file_path=preview['file'],
                patched_content=preview['patched_content']))
    except execution.ExecutionStopped as exc:
        return redirect(slug, error=exc.reason)
    if response.status_code == 409 and json.loads(response.body).get('stale_preview'):
        service._finish(run.id, state='review_required', reason='source_changed')
        return redirect(slug, error='source_changed')
    return redirect(slug, msg='draft_done') if response.status_code == 200 else redirect(slug, error='preparation_failed')


@router.post('/cron/autocorrection-previews')
def cron(request: Request):
    m = service.module()
    if not m._cron_auth_check(request):
        raise HTTPException(401, 'Unauthorized')
    return m._assistant_json({'ok':True, 'result':service.sweep(limit=1)})
