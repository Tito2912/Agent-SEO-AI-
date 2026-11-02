"""System-owner administration of a temporary, noncommercial recipe permission."""
from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import RedirectResponse

try:
    from . import customer_accounts, recipe_access
    from .models import ProjectRecipeAccess, User
except ImportError:
    import customer_accounts
    import recipe_access
    from models import ProjectRecipeAccess, User

router = APIRouter()


def module():
    if __package__:
        from . import app
    else:
        import app
    return app


def path(user_id):
    return '/settings/activation/accounts/' + str(user_id) + '/recipe-access'


@router.get('/settings/activation/accounts/{account_id}/recipe-access')
def access_page(request: Request, account_id: str, msg: str = '', err: str = ''):
    m = module()
    m._require_system_owner(request)
    with m.DB.session() as db:
        account = db.get(User, account_id)
        if account is None or account.is_admin:
            raise HTTPException(404, 'Compte client introuvable dans ce périmètre.')
        row = db.get(ProjectRecipeAccess, account_id)
    return m.templates.TemplateResponse('settings_recipe_access.html', {
        'request': request, 'project': None, 'account': account, 'access': row,
        'access_active': recipe_access.active(row), 'date_label': customer_accounts.display_date,
        'msg': msg, 'err': err,
    }, headers={'Cache-Control': 'no-store', 'Referrer-Policy': 'no-referrer'})


@router.post('/settings/activation/accounts/{account_id}/recipe-access')
def save_access(request: Request, account_id: str, base_url: str = Form(''), hours: int = Form(24),
                reason: str = Form(''), confirm: str = Form('')):
    m = module()
    actor = m._require_system_owner(request)
    try:
        if confirm != '1':
            raise ValueError('Confirmez cette autorisation temporaire d’ajout.')
        origin = recipe_access.canonical(base_url)
        if m._validate_public_crawl_target(origin):
            raise ValueError('Choisissez un site public accessible en HTTPS.')
        with m.DB.session() as db:
            recipe_access.grant(db, actor=actor, user_id=account_id, base_url=origin, hours=hours, reason=reason)
            db.commit()
    except ValueError as error:
        return RedirectResponse(m._path_with_flash(path(account_id), err=str(error)), status_code=303)
    return RedirectResponse(m._path_with_flash(path(account_id), msg='Autorisation de recette enregistrée.'), status_code=303)


@router.post('/settings/activation/accounts/{account_id}/recipe-access/revoke')
def revoke_access(request: Request, account_id: str, grant_id: str = Form('')):
    m = module()
    actor = m._require_system_owner(request)
    try:
        with m.DB.session() as db:
            recipe_access.revoke(db, actor=actor, user_id=account_id, grant_id=grant_id)
            db.commit()
    except ValueError as error:
        return RedirectResponse(m._path_with_flash(path(account_id), err=str(error)), status_code=303)
    return RedirectResponse(m._path_with_flash(path(account_id), msg='Autorisation de recette retirée.'), status_code=303)
