"""Owner-only, read-only social planning. No publication or paid-generation routes."""
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response
from sqlalchemy.exc import SQLAlchemyError

try:
    from . import i18n, own_social as s
    from .own_social_copy import strings
except ImportError:
    import i18n
    import own_social as s
    from own_social_copy import strings

router = APIRouter()
try:
    from .own_social_media_routes import router as media_router
except ImportError:
    from own_social_media_routes import router as media_router
router.include_router(media_router)
HEADERS = {'Cache-Control': 'no-store', 'Referrer-Policy': 'no-referrer', 'X-Content-Type-Options': 'nosniff'}


def render(request, state, selected=None, *, unavailable=False, changed=False):
    return s.own_seo.module().templates.TemplateResponse('settings_own_social.html', {'request': request, 'project': None,
        'copy': strings(i18n.current_lang()), 'state': state, 'selected': selected, 'unavailable': unavailable,
        'changed': changed, 'base': s.PATH}, status_code=503 if unavailable else 409 if changed else 200, headers=HEADERS)


def state_for(request, network, goal, audience):
    m = s.own_seo.module()
    m._require_system_owner(request)
    try:
        s.options(network, goal, audience)
    except ValueError:
        raise HTTPException(422, 'invalid_choice')
    with m.DB.session() as db:
        return s.plan(db, network=network, goal=goal, audience=audience, lang=i18n.current_lang())


@router.get('/settings/system/social')
def page(request: Request, network: str = Query('instagram', max_length=20), goal: str = Query('discovery', max_length=20),
         audience: str = Query('freelance', max_length=20), topic: str = Query('', max_length=32)):
    m = s.own_seo.module()
    m._require_system_owner(request)
    state, selected, unavailable = {}, None, False
    try:
        state = state_for(request, network, goal, audience)
        if topic:
            selected = next((i for i in state['ideas'] + state['deferred'] if i['key'] == topic), None)
            if not selected:
                raise HTTPException(404, 'idea_not_found')
    except SQLAlchemyError:
        unavailable = True
    return render(request, state, selected, unavailable=unavailable)


@router.get('/settings/system/social/{topic}/download')
def download(request: Request, topic: str, network: str = Query('instagram', max_length=20),
             goal: str = Query('discovery', max_length=20), audience: str = Query('freelance', max_length=20),
             version: str = Query('', max_length=64)):
    try:
        state = state_for(request, network, goal, audience)
    except SQLAlchemyError:
        return render(request, {}, unavailable=True)
    item = next((i for i in state['ideas'] + state['deferred'] if i['key'] == topic), None)
    if not item:
        raise HTTPException(404, 'idea_not_found')
    if not item['ready'] or version != item['version']:
        return render(request, state, item, changed=True)
    return Response(s.export(state, item), media_type='text/markdown; charset=utf-8',
        headers={**HEADERS, 'Content-Disposition': 'attachment; filename="noyaru-social-' + topic + '-' + network + '-' + state['lang'] + '.md"'})
