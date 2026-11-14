"""Owner-only private writing; cron only resumes explicitly authorized rows."""
import uuid
from urllib.parse import urlencode
from fastapi import APIRouter,BackgroundTasks,Form,HTTPException,Request
from fastapi.responses import JSONResponse,RedirectResponse,Response
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
try:
    from . import own_writing as w,i18n,own_seo_routes
    from .own_writing_copy import strings
    from .models import OwnSeoDraft, OwnBlogPublication
except ImportError:
    import own_writing as w
    import i18n
    import own_seo_routes
    from own_writing_copy import strings
    from models import OwnSeoDraft, OwnBlogPublication

router=APIRouter()


def redirect(brief,error=''):
    return RedirectResponse(w.PATH+'?'+urlencode({'brief':brief,'err':error}),status_code=303,headers={'Cache-Control':'no-store'})


@router.get('/settings/system/own-seo/writing')
def page(request:Request,brief:str='',err:str=''):
    m=w.module();user=m._require_system_owner(request);copy=strings(i18n.current_lang())
    state={};selected=None;dossier=None;fingerprint='';items=[];eligible=[];used=0;unavailable=False;problem=''
    try:
        with m.DB.session() as db:
            state=w.seo.view(db,user.id,lang=i18n.current_lang())
            eligible=[i['row'] for i in w.seo.history(db,user.id,state) if i['current'] and i['row'].status in {'prepared','reviewed'} and i['row'].kind in {'research','guide'}]
            if brief:
                try:_,_,selected=w.seo.brief_for(db,user.id,brief)
                except ValueError:raise HTTPException(404,'brief_not_found')
                try:dossier,fingerprint=w.bundle(db,user.id,brief,allow_prepared=True)
                except ValueError as exc:problem=str(exc)
            used=w.reserved(db,user.id)
            if state.get('project'):
                query=select(OwnSeoDraft).where(OwnSeoDraft.owner_id==user.id,OwnSeoDraft.workspace_token==state['workspace'].token)
                if brief:query=query.where(OwnSeoDraft.brief_id==brief)
                for row in db.scalars(query.order_by(OwnSeoDraft.created_at.desc()).limit(20)):
                    publication=db.scalar(select(OwnBlogPublication).where(OwnBlogPublication.owner_id==user.id,OwnBlogPublication.fr_id==row.id).order_by(OwnBlogPublication.created_at.desc()).limit(1))
                    items.append({'row':row,'publication':publication,'current':w.current(db,row),'sources':{s['id']:{**s,'number':i+1} for i,s in enumerate(row.bundle['sources'])}})
    except SQLAlchemyError:unavailable=True
    existing=next((i['row'] for i in items if i['row'].brief_id==brief and i['row'].input_hash==fingerprint
        and i['row'].status in {'queued','running','draft','reviewed','uncertain'}),None)
    active_publication=next((i['publication'] for i in items if existing and i['row'].id==existing.id),None)
    return m.templates.TemplateResponse('settings_own_writing.html',{'request':request,'project':None,'copy':copy,'state':state,
        'selected':selected,'dossier':dossier,'input_hash':fingerprint,'key':uuid.uuid4().hex,'items':items,'eligible':eligible,
        'used':used,'existing':existing,'base':w.PATH,'unavailable':unavailable,'problem':copy['errors'].get(problem,''),
        'active_publication':active_publication,
        'journey_version':w.seo.digest([selected.status if selected else '',[(i['row'].id,i['row'].status,i['publication'].state if i['publication'] else '') for i in items]]),
        'watch':bool(selected and not existing) or any(i['row'].status in {'queued','running'} or (i['publication'] and i['publication'].state in {'checking','queued_prepare','preparing','reviewing','queued_publish','publishing','observing'}) for i in items),
        'err':copy['errors'].get(err,copy['errors']['unavailable']) if err else ''},status_code=503 if unavailable else 200,
        headers={'Cache-Control':'no-store','Referrer-Policy':'no-referrer'})


@router.post('/settings/system/own-seo/writing/authorize')
def authorize(request:Request,background:BackgroundTasks,brief:str=Form(''),input_hash:str=Form(''),key:str=Form(''),confirm:bool=Form(False)):
    user=w.module()._require_system_owner(request);own_seo_routes.throttle(request)
    try:draft_id=w.authorize(user.id,brief,input_hash,key,confirm,article_journey=True)
    except ValueError as exc:return redirect(brief,str(exc))
    except SQLAlchemyError:return redirect(brief,'unavailable')
    background.add_task(w.run,draft_id)
    return redirect(brief)


@router.post('/settings/system/own-seo/writing/{draft_id}/resume')
def resume(request:Request,draft_id:str,background:BackgroundTasks):
    user=w.module()._require_system_owner(request);own_seo_routes.throttle(request)
    with w.module().DB.session() as db:
        try:row=w.row_for(db,user.id,draft_id)
        except ValueError:raise HTTPException(404,'draft_not_found')
        brief=row.brief_id
    background.add_task(w.run,draft_id)
    return redirect(brief)


@router.post('/settings/system/own-seo/writing/{draft_id}/review')
def review(request:Request,draft_id:str,content_hash:str=Form(''),confirm:bool=Form(False)):
    user=w.module()._require_system_owner(request);own_seo_routes.throttle(request)
    with w.module().DB.session() as db:
        try:row=w.row_for(db,user.id,draft_id)
        except ValueError:raise HTTPException(404,'draft_not_found')
        brief=row.brief_id
    try:w.review(user.id,draft_id,content_hash,confirm)
    except ValueError as exc:return redirect(brief,str(exc))
    except SQLAlchemyError:return redirect(brief,'unavailable')
    return redirect(brief)


@router.post('/settings/system/own-seo/writing/{draft_id}/cancel')
def cancel(request:Request,draft_id:str):
    user=w.module()._require_system_owner(request)
    with w.module().DB.session() as db:
        try:row=w.row_for(db,user.id,draft_id)
        except ValueError:raise HTTPException(404,'draft_not_found')
        brief=row.brief_id
    w.cancel(user.id,draft_id);return redirect(brief)


@router.get('/settings/system/own-seo/writing/{draft_id}/download')
def download(request:Request,draft_id:str):
    user=w.module()._require_system_owner(request)
    with w.module().DB.session() as db:
        try:row=w.row_for(db,user.id,draft_id)
        except ValueError:raise HTTPException(404,'draft_not_found')
        if not row.content:raise HTTPException(404,'draft_not_ready')
        text=w.export(row)
    return Response(text,media_type='text/markdown; charset=utf-8',headers={'Content-Disposition':'attachment; filename="noyaru-draft-'+draft_id+'.md"','Cache-Control':'no-store','X-Content-Type-Options':'nosniff'})


@router.post('/cron/own-seo-writing')
def cron(request:Request):
    if not w.module()._cron_auth_check(request):return JSONResponse({'ok':False,'error':'unauthorized'},status_code=401)
    try:count=w.tick()
    except SQLAlchemyError:return JSONResponse({'ok':False,'error':'store_unavailable'},status_code=503)
    return JSONResponse({'ok':True,'examined':count},headers={'Cache-Control':'no-store'})
