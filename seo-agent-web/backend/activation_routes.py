"""Owner-only, aggregate activation reporting. No customer identifiers leave these routes."""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, Response

try:
    from . import customer_activation
except ImportError:
    import customer_activation

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
def activation_page(request: Request, date_from: str = '', date_to: str = ''):
    data = metrics(request, date_from, date_to)
    response = module().templates.TemplateResponse('settings_activation.html', {
        'request': request, 'project': None, 'metrics': data,
    })
    response.headers['Cache-Control'] = 'no-store'
    return response


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
