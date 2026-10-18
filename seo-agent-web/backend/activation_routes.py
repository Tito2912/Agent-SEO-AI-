"""System-owner activation reporting and private customer account inventory."""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response
from sqlalchemy import update

try:
    from . import customer_account_profile, customer_accounts, customer_activation, customer_country, plan_counts, signup_attribution
    from .models import User
except ImportError:
    import customer_account_profile
    import customer_accounts
    import customer_activation
    import customer_country
    import plan_counts
    import signup_attribution
    from models import User

router = APIRouter()


def module():
    if __package__:
        from . import app
    else:
        import app
    return app


def metrics(request, date_from, date_to):
    m = module()
    m._require_system_owner(request)
    try:
        return customer_activation.snapshot(m.DB, start=date_from, end=date_to)
    except ValueError as error:
        raise HTTPException(400, str(error)) from None
    except Exception:
        raise HTTPException(503, 'Le suivi des inscriptions n’est pas disponible pour le moment.') from None


@router.get('/settings/activation')
def activation_page(request: Request, date_from: str = '', date_to: str = '', msg: str = '', err: str = ''):
    data = metrics(request, date_from, date_to)
    m = module()
    try:
        campaigns = signup_attribution.links(m.DB, m._public_base_url(request))
    except Exception:
        raise HTTPException(503, 'Les liens de campagne ne sont pas disponibles pour le moment.') from None
    try:
        current_plans = plan_counts.snapshot(m.DB)
    except Exception:
        current_plans = None
    response = module().templates.TemplateResponse('settings_activation.html', {
        'request': request, 'project': None, 'metrics': data,
        'campaigns': campaigns, 'campaign_mediums': signup_attribution.MEDIUMS, 'msg': msg, 'err': err,
        'current_plans': current_plans, 'plan_catalog': m.billing.plan_catalog(),
    })
    response.headers['Cache-Control'] = 'no-store'
    return response


@router.get('/api/settings/activation/plans')
def current_plan_counts(request: Request):
    m = module()
    m._require_system_owner(request)
    try:
        data = plan_counts.snapshot(m.DB)
    except Exception:
        raise HTTPException(503, 'La répartition des plans est indisponible pour le moment.') from None
    return JSONResponse({'ok': True, **data}, headers={'Cache-Control': 'no-store'})


def accounts(request, plan, q, usage, page=1, export=False):
    m = module()
    m._require_system_owner(request)
    try:
        return customer_accounts.snapshot(m.DB, plan=plan, q=q, usage=usage, page=page, export=export)
    except ValueError as error:
        raise HTTPException(400, str(error)) from None
    except Exception:
        raise HTTPException(503, 'La liste des comptes est indisponible pour le moment.') from None


@router.get('/settings/activation/accounts')
def accounts_page(request: Request, plan: str = '', q: str = '', usage: str = 'all', page: int = 1):
    data = accounts(request, plan, q, usage, page)
    return module().templates.TemplateResponse('settings_customer_accounts.html', {
        'request': request, 'project': None, 'inventory': data, 'plan_catalog': module().billing.plan_catalog(),
        'usage_filters': customer_accounts.USAGE_FILTERS,
        'verification_mode': customer_country.verification_mode(module()),
    }, headers={'Cache-Control': 'no-store', 'Referrer-Policy': 'no-referrer'})


@router.post('/settings/account/country')
def save_own_country(request: Request, country: str = Form(''), next: str = Form('/')):
    m = module()
    user = getattr(request.state, 'user', None)
    if not user:
        raise HTTPException(401, 'auth_required')
    code = customer_country.normalize(country)
    if code is None:
        raise HTTPException(400, 'Choisissez votre pays de résidence dans la liste.')
    try:
        with m.DB.session() as db:
            db.execute(update(User).where(User.id == user.id).values(country=code))
            db.commit()
    except Exception:
        raise HTTPException(503, 'Votre pays n’a pas pu être enregistré. Réessayez dans quelques instants.') from None
    return RedirectResponse(m._path_with_flash(m._safe_next_path(next), msg='Pays enregistré.'), status_code=303)


@router.get('/settings/activation/accounts.csv')
def accounts_csv(request: Request, plan: str = '', q: str = '', usage: str = 'all'):
    data = accounts(request, plan, q, usage, export=True)
    return Response(customer_accounts.csv_bytes(data), media_type='text/csv; charset=utf-8', headers={
        'Cache-Control': 'no-store', 'Referrer-Policy': 'no-referrer',
        'Content-Disposition': 'attachment; filename="comptes-clients.csv"',
    })


def account_profile(request, account_id):
    m = module()
    m._require_system_owner(request)
    try:
        data = customer_account_profile.snapshot(m.DB, account_id,
            google_key=m._gsc_oauth_connection_key, bing_key=m._bing_oauth_connection_key)
    except Exception:
        raise HTTPException(503, 'La fiche de ce compte est indisponible pour le moment.') from None
    if data is None:
        raise HTTPException(404, 'Compte client introuvable dans ce périmètre.')
    return data


@router.get('/settings/activation/accounts/{account_id}/export.csv')
def account_profile_csv(request: Request, account_id: str):
    data = account_profile(request, account_id)
    return Response(customer_account_profile.csv_bytes(data), media_type='text/csv; charset=utf-8', headers={
        'Cache-Control': 'no-store', 'Referrer-Policy': 'no-referrer',
        'Content-Disposition': 'attachment; filename="fiche-client.csv"',
    })


@router.get('/settings/activation/accounts/{account_id}')
def account_profile_page(request: Request, account_id: str):
    data = account_profile(request, account_id)
    return module().templates.TemplateResponse('settings_customer_account_profile.html', {
        'request': request, 'project': None, 'dossier': data,
    }, headers={'Cache-Control': 'no-store', 'Referrer-Policy': 'no-referrer'})


@router.get('/api/settings/activation')
def activation_api(request: Request, date_from: str = '', date_to: str = ''):
    response = JSONResponse({'ok': True, **metrics(request, date_from, date_to)})
    response.headers['Cache-Control'] = 'no-store'
    return response


@router.get('/settings/activation.csv')
def activation_csv(request: Request, date_from: str = '', date_to: str = ''):
    data = metrics(request, date_from, date_to)
    output = io.StringIO(newline='')
    writer = csv.writer(output, delimiter=';')
    writer.writerow(['Inscrits du', 'Au', 'Semaine du', 'Inscriptions suivies', 'Site ajouté',
                     'Analyse exploitable', 'Correction vérifiée', 'Bilan sans anomalie', 'Conversion correction (%)'])
    for week in data['weeks']:
        writer.writerow([data['date_from'], data['date_to'], week['week'], *week['counts'], week['clean'],
                         str(week['rate']).replace('.', ',')])
    return Response(('\ufeff' + output.getvalue()).encode('utf-8'), media_type='text/csv; charset=utf-8', headers={
        'Cache-Control': 'no-store',
        'Content-Disposition': f'attachment; filename="activation-{data["date_from"]}-{data["date_to"]}.csv"',
    })


@router.get('/settings/activation/acquisition.csv')
def acquisition_csv(request: Request, date_from: str = '', date_to: str = ''):
    data = metrics(request, date_from, date_to)
    output = io.StringIO(newline='')
    writer = csv.writer(output, delimiter=';')
    writer.writerow(['Inscrits du', 'Au', 'Source', 'Support', 'Campagne', 'Inscriptions suivies', 'Site ajouté',
                     'Analyse exploitable', 'Correction vérifiée', 'Bilan sans anomalie', 'Conversion correction (%)'])
    for group in data['acquisition']:
        writer.writerow([data['date_from'], data['date_to'], group['source'] or 'Origine non renseignée',
                         group['medium'], group['campaign'], *group['counts'], group['clean'],
                         str(group['rate']).replace('.', ',')])
    return Response(('\ufeff' + output.getvalue()).encode('utf-8'), media_type='text/csv; charset=utf-8', headers={
        'Cache-Control': 'no-store',
        'Content-Disposition': f'attachment; filename="campagnes-{data["date_from"]}-{data["date_to"]}.csv"',
    })


@router.post('/settings/activation/campaigns')
def create_campaign(request: Request, source: str = Form(''), medium: str = Form(''), campaign: str = Form('')):
    m = module()
    m._require_system_owner(request)
    try:
        signup_attribution.create(m.DB, source, medium, campaign)
    except ValueError as error:
        return RedirectResponse(m._path_with_flash('/settings/activation', err=str(error)) + '#campaign-links', status_code=303)
    except Exception:
        raise HTTPException(503, 'Le lien n’a pas pu être préparé. Réessayez dans quelques instants.') from None
    return RedirectResponse(m._path_with_flash('/settings/activation', msg='Lien de campagne prêt à copier.') + '#campaign-links', status_code=303)


@router.post('/settings/account/attribution/withdraw')
def withdraw_attribution(request: Request):
    m = module()
    user = getattr(request.state, 'user', None)
    if not user:
        raise HTTPException(401, 'auth_required')
    try:
        signup_attribution.withdraw(m.DB, user.id)
    except Exception:
        raise HTTPException(503, 'Votre choix n’a pas pu être enregistré. Réessayez dans quelques instants.') from None
    return RedirectResponse(m._path_with_flash('/settings/accounts', msg='Votre accord a été retiré et l’origine de votre inscription effacée.') + '#signup-attribution', status_code=303)
