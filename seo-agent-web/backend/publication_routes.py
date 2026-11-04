"""Explicit owner grants; GET requests only display stored previews and receipts."""
from urllib.parse import quote

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

try:
    from . import autocorrection as preparation, correction_publication as service, correction_execution as execution, correction_guard, i18n
    from .autocorrection_routes import owner_project, own_run, throttle
    from .models import AutocorrectionRun, CorrectionPublication, IssueTask
    from .publication_copy import strings
except ImportError:
    import autocorrection as preparation
    import correction_publication as service
    import correction_execution as execution
    import correction_guard
    import i18n
    from autocorrection_routes import owner_project, own_run, throttle
    from models import AutocorrectionRun, CorrectionPublication, IssueTask
    from publication_copy import strings

router = APIRouter()


def base(slug):
    return '/projects/'+quote(slug,safe='')+'/corrections/autonomy/publications'


def redirect(slug, *, error='', msg=''):
    query = '?error='+quote(error) if error else '?msg='+quote(msg) if msg else ''
    return RedirectResponse(base(slug)+query,status_code=303,headers={'Cache-Control':'no-store'})


@router.get('/projects/{slug}/corrections/autonomy/publications')
def page(request: Request, slug: str, msg: str = '', error: str = ''):
    m = preparation.module()
    project = m._db_project_or_404(request,slug)
    copy = strings(i18n.current_lang())
    candidates = []
    history = []
    limited = unavailable = False
    try:
        with m.DB.session() as db:
            rows = list(db.scalars(select(AutocorrectionRun).where(AutocorrectionRun.project_id == project.id,
                AutocorrectionRun.state == 'preview_ready').order_by(AutocorrectionRun.created_at.desc()).limit(21)))
            limited = len(rows) > 20
            for run in rows[:20]:
                task = db.get(IssueTask,run.task_id)
                used = db.scalar(select(CorrectionPublication.id).where(CorrectionPublication.run_id == run.id))
                preview,_ = preparation.saved_preview(db,run)
                if not used and preview and task and preparation.draft_current(db,run,project,task):
                    candidates.append({'run':run,'task':task,'preview':preview,'key':service.preview_key(preview)})
            rows = list(db.scalars(select(CorrectionPublication).where(CorrectionPublication.project_id == project.id)
                .order_by(CorrectionPublication.created_at.desc()).limit(21)))
            limited |= len(rows) > 20
            for row in rows[:20]:
                task = db.get(IssueTask,row.task_id)
                receipt = row.receipt or {}
                pr_url = ''
                if type(receipt.get('number')) is int and receipt['number'] > 0:
                    owner,repo = m._github_repo_parts(m._project_github_cfg(project)['repo'])
                    pr_url = 'https://github.com/'+quote(owner,safe='')+'/'+quote(repo,safe='')+'/pull/'+str(receipt['number'])
                history.append({'row':row,'task':task,'pr_url':pr_url,
                    'expires':row.expires_at.strftime('%Y-%m-%d %H:%M UTC')})
    except SQLAlchemyError:
        m.logger.error('[publication] tracking unavailable')
        unavailable = True
    return m.templates.TemplateResponse('correction_publication.html',{'request':request,'project':project,
        'slug':slug,'copy':copy,'base':base(slug),'candidates':candidates,'history':history,'limited':limited,
        'unavailable':unavailable,'owner':str(project.owner_user_id) == str(request.state.user.id),
        'paused':preparation.paused() or preparation.project_paused(project),'terminal':service.TERMINAL,
        'msg':copy.get('messages',{}).get(msg,''),'error':copy['reasons'].get(error,copy['fallback']) if error else ''},
        status_code=503 if unavailable else 200,headers={'Cache-Control':'no-store','Referrer-Policy':'no-referrer'})


@router.post('/projects/{slug}/corrections/autonomy/publications/authorize')
def authorize(request: Request, slug: str, run_id: str = Form(''), approved_key: str = Form(''),
              approved: bool = Form(False), consent: bool = Form(False), pages_budget: int = Form(100),
              environment: str = Form('production')):
    project,run = own_run(request,slug,run_id)
    throttle(request,project)
    try:
        service.authorize(project.id,run.id,str(request.state.user.id),approved_key,approved=approved,
            consent=consent,pages_budget=pages_budget,environment=environment,lang=i18n.current_lang())
    except execution.ExecutionStopped as exc:
        return redirect(slug,error=exc.reason)
    except (SQLAlchemyError,correction_guard.CorrectionStoreUnavailable,correction_guard.CorrectionBusy):
        return redirect(slug,error='unavailable')
    return redirect(slug,msg='authorized')


@router.post('/projects/{slug}/corrections/autonomy/publications/{publication_id}/revoke')
def revoke(request: Request, slug: str, publication_id: str):
    project = owner_project(request,slug)
    with preparation.module().DB.session() as db:
        row = db.get(CorrectionPublication,publication_id)
        if not row or row.project_id != project.id:
            raise HTTPException(404,'publication_not_found')
    service.revoke(publication_id,str(request.state.user.id))
    return redirect(slug,msg='revoked')


@router.post('/cron/correction-publications')
def cron(request: Request):
    m = preparation.module()
    if not m._cron_auth_check(request):
        raise HTTPException(401,'Unauthorized')
    return m._assistant_json({'ok':True,'result':service.sweep(limit=1)})
