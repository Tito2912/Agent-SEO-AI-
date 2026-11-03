"""Project-scoped read and internal decision refresh; no external actions."""
from urllib.parse import quote, urlencode

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.exc import SQLAlchemyError

try:
    from . import correction_decisions as decisions, i18n
    from .correction_decision_copy import strings
    from .models import IssueTask
except ImportError:
    import correction_decisions as decisions
    import i18n
    from correction_decision_copy import strings
    from models import IssueTask

router = APIRouter()


def module():
    if __package__:
        from . import app
    else:
        import app
    return app


def report_resolver(request, project):
    m=module();runs=m._runs_dir_pour_slug(request,project.slug);cache={}
    def resolve(value):
        date=decisions.timestamp(value)
        if date is None:return ''
        key=date.strftime('%Y%m%d-%H%M%S')
        if date.microsecond:return ''
        if key not in cache:
            path=m.dash.report_path(runs,project.slug,key)
            try:
                if not path.is_file():m._ensure_runs_file_local(path)
                report=m.dash.load_report_json(runs,project.slug,key)
                cache[key]={'timestamp':key,'issues':report.get('issues')} if isinstance(report,dict) and report else None
            except Exception:
                m.logger.warning('[DECISIONS] report unavailable');cache[key]=''
        return cache[key]
    return resolve


def observed_target(issue_key, url, reference):
    """Keep the exact observed variant and raw page URL accepted by issue_detail."""
    m=module();target=m._verification_url(url or '')
    issues=reference.get('issues')
    if not target or not isinstance(issues,dict):return None
    family=m._verification_family_keys(issue_key)
    for key in [issue_key]+sorted(family-{issue_key}):
        block=issues.get(key)
        if not isinstance(block,dict):continue
        try:
            if int(block.get('count') or 0)<=0:continue
        except (ValueError,TypeError):continue
        pages=m.dash.extract_impacted_pages(key,block)
        for page in sorted(value for value in pages if isinstance(value,str)):
            if m._verification_url(page)==target:return key,page
    return None


def presentation(db, project, summary, copy, resolve_report):
    base = '/projects/'+quote(project.slug, safe='')
    result = []
    for item in summary['decisions']:
        task = db.get(IssueTask, item['task_id'])
        if task is None or task.project_id != str(project.id):
            continue
        title, text, label = copy['codes'][item['code']]
        missing=False;context_missing=False
        href=base+'/corrections?'+urlencode({'task_id':str(task.id)})+'#correction-task-'+quote(str(task.id),safe='')
        if item['code'] == 'resolution_observed':
            href = base+'/first-correction?choose=1'
        else:
            reference=resolve_report(task.crawl_ts if item['code']=='confirm_baseline' else item['control_at'])
            if not reference:
                missing=True;label=copy['tracking']
            elif item['code'] in {'obtain_valid_control','confirm_baseline'}:
                href=base+'?'+urlencode({'crawl':reference['timestamp']})
            elif (observed:=observed_target(item['issue_key'],task.url,reference)):
                key,page=observed
                href=base+'/issues/'+quote(key,safe='')+'?'+urlencode({
                    'crawl':reference['timestamp'],'page_url':page,
                })+'#page-selection'
            else:
                context_missing=True;label=copy['tracking']
        result.append({**item, 'title':title, 'text':text, 'action':label, 'href':href,
                       'page_url':task.url or '', 'label':task.issue_label or task.issue_key,
                       'report_missing':missing, 'context_missing':context_missing,
                       'control_stamp':decisions.timestamp(item['control_at']).strftime('%Y%m%d-%H%M%S'),
                       'control_label':decisions.timestamp(item['control_at']).strftime(copy['date_format'])})
    return result


@router.get('/projects/{slug}/corrections/decisions')
def page(request: Request, slug: str, msg: str = ''):
    m=module();project=m._db_project_or_404(request,slug);copy=strings(i18n.current_lang())
    error=False;summary={'decisions':[],'truncated':False};items=[]
    try:
        with m.DB.session() as db:
            summary=decisions.project_summary(db,str(project.id))
            items=presentation(db,project,summary,copy,report_resolver(request,project))
    except SQLAlchemyError:
        m.logger.error('[DECISIONS] read unavailable');error=True
    return m.templates.TemplateResponse('correction_decisions.html', {
        'request':request, 'project':{'slug':slug,'site_name':project.site_name or slug},
        'slug':slug, 'copy':copy, 'items':items, 'summary':summary, 'error':error,
        'refreshed':msg=='refreshed',
    },status_code=503 if error else 200,
       headers={'Cache-Control':'no-store','Referrer-Policy':'no-referrer'})


@router.get('/api/projects/{slug}/corrections/decisions')
def api(request: Request, slug: str):
    m=module();project=m._db_project_or_404(request,slug)
    try:
        with m.DB.session() as db:summary=decisions.project_summary(db,str(project.id))
    except SQLAlchemyError:
        return m._assistant_json({'ok':False,'error':'decisions_unavailable'},status_code=503)
    return m._assistant_json({'ok':True,'summary':summary})


@router.post('/projects/{slug}/corrections/decisions/refresh')
def refresh(request: Request, slug: str):
    m=module();project=m._db_project_or_404(request,slug)
    retry=m._rate_limit_retry_after(bucket='correction_decision_refresh',
          subject=str(request.state.user.id)+':'+str(project.id),limit=6,window_s=60)
    if isinstance(retry,int):
        return m._assistant_json({'ok':False,'error':'too_many_requests'},status_code=429,
                                 headers={'Retry-After':str(retry)})
    try:
        with m.DB.session() as db:
            decisions.refresh_project(db,str(project.id));db.commit()
    except SQLAlchemyError:
        return m._assistant_json({'ok':False,'error':'decisions_unavailable'},status_code=503)
    return RedirectResponse('/projects/'+quote(slug,safe='')+'/corrections/decisions?msg=refreshed',
                            status_code=303,headers={'Cache-Control':'no-store'})
