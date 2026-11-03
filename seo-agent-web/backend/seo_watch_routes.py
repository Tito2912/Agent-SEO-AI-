"""Owner-only knowledge review and a separately authenticated daily read-only watch."""
from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select

try:
    from . import i18n, seo_watch
    from .models import AuditLog, SeoKnowledgeRevision, SeoKnowledgeSource
    from .seo_watch_copy import strings
except ImportError:
    import i18n
    import seo_watch
    from models import AuditLog, SeoKnowledgeRevision, SeoKnowledgeSource
    from seo_watch_copy import strings

router = APIRouter()
PATH = '/settings/system/seo-knowledge'


def module():
    if __package__:
        from . import app
    else:
        import app
    return app


def redirect(message='', *, error=''):
    return RedirectResponse(module()._path_with_flash(PATH, msg=message, err=error), status_code=303)


@router.get('/settings/system/seo-knowledge')
def page(request: Request, msg: str = '', err: str = ''):
    m = module();m._require_system_owner(request)
    with m.DB.session() as db:
        catalog = seo_watch.catalog(db, lang=i18n.current_lang())
        for item in catalog['sources']:
            item['row'] = db.get(SeoKnowledgeSource, item['key'])
            item['history'] = list(db.scalars(select(SeoKnowledgeRevision).where(
                SeoKnowledgeRevision.source_key == item['key'],
            ).order_by(SeoKnowledgeRevision.first_seen_at.desc()).limit(10)))
            item['reviews'] = list(db.scalars(select(AuditLog).where(
                AuditLog.target_type == 'seo_source', AuditLog.target_id == item['key'],
                AuditLog.action == 'seo_watch.review',
            ).order_by(AuditLog.created_at.desc()).limit(5)))
    return m.templates.TemplateResponse('settings_seo_knowledge.html', {
        'request': request, 'project': None, 'catalog': catalog, 'copy': strings(i18n.current_lang()),
        'msg': msg, 'err': err,
    }, headers={'Cache-Control': 'no-store', 'Referrer-Policy': 'no-referrer'})


@router.post('/settings/system/seo-knowledge/refresh')
def refresh(request: Request):
    m = module();m._require_system_owner(request)
    seo_watch.refresh_due(m.DB)
    return redirect(strings(i18n.current_lang())['refreshed'])


@router.post('/settings/system/seo-knowledge/{key}/review')
def review(request: Request, key: str, fingerprint: str = Form(''),
           guidance_fr: str = Form(''), guidance_en: str = Form(''), confirm: str = Form('')):
    m = module();actor = m._require_system_owner(request);copy = strings(i18n.current_lang())
    if key not in seo_watch.REGISTRY:
        raise HTTPException(404, 'source_not_found')
    try:
        if confirm != '1':
            raise ValueError('invalid_review')
        with m.DB.session() as db:
            seo_watch.review(db, key, fingerprint, fr=guidance_fr, en=guidance_en, actor=actor)
            db.commit()
    except ValueError as exc:
        return redirect(error=copy['changed_error' if str(exc) == 'review_changed' else 'form_error'])
    return redirect(copy['saved'])


@router.post('/settings/system/seo-knowledge/{key}/withdraw')
def withdraw(request: Request, key: str):
    m = module();actor = m._require_system_owner(request)
    if key not in seo_watch.REGISTRY:
        raise HTTPException(404, 'source_not_found')
    with m.DB.session() as db:
        seo_watch.withdraw(db, key, actor=actor);db.commit()
    return redirect(strings(i18n.current_lang())['withdrawn'])


@router.post('/settings/system/seo-knowledge/{key}/pause')
def pause(request: Request, key: str, paused: str = Form('1')):
    m = module();actor = m._require_system_owner(request)
    if key not in seo_watch.REGISTRY or paused not in {'0', '1'}:
        raise HTTPException(404, 'source_not_found')
    with m.DB.session() as db:
        seo_watch.pause(db, key, paused=paused == '1', actor=actor);db.commit()
    return redirect(strings(i18n.current_lang())['paused' if paused == '1' else 'resumed'])


@router.post('/cron/seo-watch')
def cron(request: Request):
    m = module()
    if not m._cron_auth_check(request):
        return i18n.JSONResponse({'ok': False, 'error': 'Unauthorized'}, status_code=401,
                                 headers={'Cache-Control': 'no-store'})
    result = seo_watch.refresh_due(m.DB)
    with m.DB.session() as db:
        states = seo_watch.catalog(db)['sources']
    success = (all(value in {'checked', 'not_due'} for value in result.values())
               and all(item['status'] != 'unavailable' for item in states))
    return i18n.JSONResponse({'ok': success, 'sources': result}, status_code=200 if success else 503,
                             headers={'Cache-Control': 'no-store'})
