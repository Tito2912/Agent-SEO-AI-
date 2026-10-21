"""System-owner screen to announce a change of the terms or the service to every account."""
from __future__ import annotations

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import RedirectResponse

try:
    from . import legal_notices
except ImportError:
    import legal_notices

router = APIRouter()
PAGE = '/settings/legal-notices'


def module():
    if __package__:
        from . import app
    else:
        import app
    return app


def _start_sending(request: Request, notice_id: str) -> None:
    m = module()
    legal_notices.dispatch(legal_notices.send_pending, m.DB, notice_id, base_url=m._public_base_url(request),
                           app_name=m._app_name(), support_email=m._support_email(), send=m._send_email)


@router.get('/settings/legal-notices')
def legal_notices_page(request: Request, msg: str = '', err: str = ''):
    m = module()
    m._require_system_owner(request)
    try:
        with m.DB.session() as db:
            recipients = legal_notices.recipient_count(db)
        notices = legal_notices.history(m.DB)
    except Exception:
        raise HTTPException(503, 'Les annonces ne sont pas disponibles pour le moment.') from None
    earliest = legal_notices.earliest()
    response = m.templates.TemplateResponse('settings_legal_notices.html', {
        'request': request,
        'project': None,
        'documents': legal_notices.DOCUMENTS,
        'earliest': earliest.isoformat(),
        'earliest_label': legal_notices.legal.format_date(earliest, 'fr'),
        'notice_days': legal_notices.NOTICE_DAYS,
        'summary_max': legal_notices.SUMMARY_MAX,
        'recipients': recipients,
        'notices': notices,
        'msg': msg,
        'err': err,
    })
    response.headers['Cache-Control'] = 'no-store'
    return response


@router.post('/settings/legal-notices')
def announce(request: Request, document: str = Form(''), effective_on: str = Form(''), summary_fr: str = Form(''),
             summary_en: str = Form(''), new_version_url: str = Form(''), confirm: str = Form('')):
    m = module()
    owner = m._require_system_owner(request)
    if confirm != 'yes':
        return RedirectResponse(m._path_with_flash(PAGE, err='Cochez la confirmation avant d’envoyer l’annonce.'), status_code=303)
    try:
        notice_id, recipients = legal_notices.create(
            m.DB, author=owner, document=document, effective_on=effective_on, summary_fr=summary_fr,
            summary_en=summary_en, new_version_url=new_version_url)
    except ValueError as error:
        return RedirectResponse(m._path_with_flash(PAGE, err=str(error)), status_code=303)
    except Exception:
        raise HTTPException(503, 'L’annonce n’a pas pu être enregistrée. Réessayez dans quelques instants.') from None
    m._audit_log(request, action='legal_notice.announce', target_type='legal_notice', target_id=notice_id,
                 meta={'document': document, 'effective_on': effective_on, 'recipients': recipients})
    _start_sending(request, notice_id)
    message = f'Annonce enregistrée. L’envoi à {recipients} compte{"s" if recipients != 1 else ""} a commencé.'
    return RedirectResponse(m._path_with_flash(PAGE, msg=message) + '#history', status_code=303)


@router.post('/settings/legal-notices/{notice_id}/resume')
def resume(request: Request, notice_id: str):
    m = module()
    m._require_system_owner(request)
    m._audit_log(request, action='legal_notice.resume', target_type='legal_notice', target_id=notice_id)
    _start_sending(request, notice_id)
    return RedirectResponse(m._path_with_flash(PAGE, msg='Envoi relancé pour les e-mails en attente ou en échec.') + '#history', status_code=303)
