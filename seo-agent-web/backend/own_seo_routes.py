"""System-owner workspace; all writes are local state, not content publication."""
from urllib.parse import urlencode
from fastapi import APIRouter,Form,HTTPException,Request
from fastapi.responses import RedirectResponse,Response
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
try:
    from . import own_seo as service,i18n
    from .own_seo_copy import strings
    from .models import Project
except ImportError:
    import own_seo as service
    import i18n
    from own_seo_copy import strings
    from models import Project

router=APIRouter()
PATH='/settings/system/own-seo'


def redirect(*,msg='',error='',brief=''):
    return RedirectResponse(PATH+'?'+urlencode({'msg':msg,'err':error})+('#brief-'+brief if brief else ''),status_code=303,headers={'Cache-Control':'no-store'})


def owner(request):
    return service.module()._require_system_owner(request)


def throttle(request):
    retry=service.module()._rate_limit_retry_after(bucket='own_seo',subject=str(request.state.user.id),limit=20,window_s=60)
    if isinstance(retry,int):raise HTTPException(429,'too_many_requests',headers={'Retry-After':str(retry)})


@router.get(PATH)
def page(request:Request,msg:str='',err:str=''):
    m=service.module();user=owner(request);copy=strings(i18n.current_lang())
    state={};briefs=[];projects=[];unavailable=False
    try:
        with m.DB.session() as db:
            state=service.view(db,user.id,lang=i18n.current_lang())
            briefs=service.history(db,user.id,state)
            for item in briefs:
                item['text']=service.export(item['row'])
                item['content_link']='/projects/'+state['project'].slug+'/content?'+urlencode({'sujet':item['row'].title})
                evidence=item['row'].evidence
                item['performance_link']='/projects/'+state['project'].slug+'/performance?'+urlencode({
                    'source':'gsc' if evidence.get('source')=='gsc_api' else 'bing','dim':'page' if evidence.get('page') else 'query',
                    'q':evidence.get('page') or evidence.get('query') or '',
                    'days':min(365,max(1,(service.first_correction.timestamp(evidence['end'])-service.first_correction.timestamp(evidence['start'])).days+1))
                    if evidence.get('start') and evidence.get('end') else 28})
            projects=[p for p in db.scalars(select(Project).where(Project.owner_user_id==user.id).order_by(Project.created_at)) if service.is_target(p.base_url)]
    except SQLAlchemyError:
        m.logger.error('[own-seo] workspace unavailable');unavailable=True
    return m.templates.TemplateResponse('settings_own_seo.html',{'request':request,'project':None,'copy':copy,'state':state,
        'briefs':briefs,'projects':projects,'base':PATH,'unavailable':unavailable,'msg':copy['messages'].get(msg,''),
        'err':copy['errors'].get(err,copy['unavailable']) if err else ''},status_code=503 if unavailable else 200,
        headers={'Cache-Control':'no-store','Referrer-Policy':'no-referrer'})


@router.post(PATH+'/bind')
def bind(request:Request,project_id:str=Form(''),token:str=Form(''),confirm:bool=Form(False)):
    user=owner(request);throttle(request)
    try:service.bind(user.id,project_id,token,confirm=confirm)
    except ValueError as exc:return redirect(error=str(exc))
    except SQLAlchemyError:return redirect(error='unavailable')
    return redirect(msg='bound')


@router.post(PATH+'/pause')
def pause(request:Request,token:str=Form(''),paused:bool=Form(True)):
    user=owner(request)
    try:service.pause(user.id,token,paused)
    except ValueError as exc:return redirect(error=str(exc))
    except SQLAlchemyError:return redirect(error='unavailable')
    return redirect()


@router.post(PATH+'/prepare')
def prepare(request:Request,token:str=Form(''),candidate_key:str=Form('')):
    user=owner(request);throttle(request)
    try:brief=service.prepare(user.id,token,candidate_key,i18n.current_lang())
    except ValueError as exc:return redirect(error=str(exc))
    except SQLAlchemyError:return redirect(error='unavailable')
    return redirect(msg='prepared',brief=brief)


@router.post(PATH+'/briefs/{brief_id}/review')
def review(request:Request,brief_id:str,token:str=Form(''),confirm:bool=Form(False),status:str=Form('reviewed')):
    user=owner(request);throttle(request)
    try:service.review(user.id,brief_id,token,confirm=confirm,status=status)
    except ValueError as exc:
        if str(exc)=='brief_missing':raise HTTPException(404,'brief_not_found')
        return redirect(error=str(exc))
    except SQLAlchemyError:return redirect(error='unavailable')
    return redirect(msg='reviewed',brief=brief_id)


@router.get(PATH+'/briefs/{brief_id}/download')
def download(request:Request,brief_id:str):
    user=owner(request)
    with service.module().DB.session() as db:
        try:_,_,brief=service.brief_for(db,user.id,brief_id)
        except ValueError:raise HTTPException(404,'brief_not_found')
        text=service.export(brief)
    return Response(text,media_type='text/markdown; charset=utf-8',headers={
        'Content-Disposition':'attachment; filename="noyaru-brief-'+brief.id+'.md"','Cache-Control':'no-store','X-Content-Type-Options':'nosniff'})
