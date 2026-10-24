"""A guided choice must not ask the customer to remove a proved expected redirect."""
from __future__ import annotations

import json

import pytest
from sqlalchemy import select

from backend import app as m
from backend.models import IssueTask
from test_first_verified_correction import BASELINE, OTHER, URL, _post, journey as journey

EXPECTED = 'http://atelier.example/'
LOOP = 'https://atelier.example/loop'


def redirects(*items, urls=(EXPECTED, LOOP)):
    return {'count': len(urls), 'examples': list(urls),
            'evidence': {'kind': 'page_values', 'items': [
                {'page': page, 'field': field, 'value': 'local evidence'} for page, field in items]}}


def choice(data):
    return data['client'].get(f'/projects/{data["slug"]}/first-correction')


def test_expected_redirects_stay_in_diagnostics_without_becoming_a_guided_task(journey):
    journey['report'](issues={'redirect_3xx': redirects((EXPECTED, 'canonicalisation attendue'), urls=(EXPECTED,))})
    page = choice(journey)
    assert page.status_code == 200 and page.context['candidates'] == []
    assert not page.context['clean']  # Keeping the diagnostic is not a clean-site certificate.
    detail = journey['client'].get(f'/projects/{journey["slug"]}/issues/redirect_3xx?crawl={BASELINE}')
    assert detail.status_code == 200 and 'canonicalisation volontaire' in detail.text
    assert 'Ne rien changer' in detail.text
    response = _post(journey, action='select', issue_key='redirect_3xx', url=EXPECTED, crawl_ts=BASELINE)
    assert response.status_code == 303 and 'err=' in response.headers['location']
    with m.DB.session() as db:
        assert list(db.scalars(select(IssueTask).where(IssueTask.project_id == journey['project_id']))) == []


def test_a_mixed_redirect_report_offers_the_loop_and_rejects_the_expected_url(journey):
    journey['report'](issues={'redirect_3xx': redirects(
        (EXPECTED, 'canonicalisation attendue'), (LOOP, 'boucle: redirige vers elle-meme'))})
    assert choice(journey).context['candidates'][0]['urls'] == [LOOP]
    denied = _post(journey, action='select', issue_key='redirect_3xx', url=EXPECTED, crawl_ts=BASELINE)
    assert 'err=' in denied.headers['location']
    allowed = _post(journey, action='select', issue_key='redirect_3xx', url=LOOP, crawl_ts=BASELINE)
    assert 'err=' not in allowed.headers['location']
    assert choice(journey).context['state']['url'] == LOOP


@pytest.mark.parametrize('evidence', [
    None, {'kind': 'legacy', 'items': [{'page': EXPECTED, 'field': 'canonicalisation attendue'}]},
    {'kind': 'page_values', 'items': []},
    {'kind': 'page_values', 'items': [{'page': EXPECTED, 'field': 'canonicalisation inconnue'}]},
    {'kind': 'page_values', 'items': [{'page': EXPECTED, 'field': 'canonicalisation attendue'},
                                     {'page': EXPECTED, 'field': 'boucle: redirige vers elle-meme'}]},
])
def test_unknown_or_conflicting_redirect_evidence_keeps_the_manual_choice(journey, evidence):
    block = {'count': 1, 'examples': [EXPECTED]}
    if evidence is not None:
        block['evidence'] = evidence
    journey['report'](issues={'redirect_3xx': block})
    assert choice(journey).context['candidates'][0]['urls'] == [EXPECTED]


def test_skipped_diagnostics_leave_room_for_three_actual_problems(journey):
    journey['report'](issues={
        'redirect_3xx': redirects((EXPECTED, 'canonicalisation attendue'), urls=(EXPECTED,)),
        'missing_title': {'count': 1, 'examples': [URL]},
        'missing_meta_description': {'count': 1, 'examples': [OTHER]},
        'meta_description_too_long_indexable': {'count': 1, 'examples': [URL]},
    }, urls=(URL, OTHER))
    candidates = choice(journey).context['candidates']
    assert len(candidates) == 3
    assert {candidate['issue_key'] for candidate in candidates} == {
        'missing_title', 'missing_meta_description', 'meta_description_too_long_indexable'}


def test_new_classification_does_not_erase_a_historical_selected_task(journey):
    journey['report'](issues={'redirect_3xx': {'count': 1, 'examples': [EXPECTED]}})
    assert 'err=' not in _post(journey, action='select', issue_key='redirect_3xx',
                               url=EXPECTED, crawl_ts=BASELINE).headers['location']
    old = choice(journey).context['state']
    with m.DB.session() as db:
        task = db.get(IssueTask, old['task_id'])
        saved = (task.status, task.note, task.updated_at)
    journey['report']('20261002-100000', issues={'redirect_3xx': redirects(
        (EXPECTED, 'canonicalisation attendue'), urls=(EXPECTED,))})
    page = choice(journey)
    assert page.context['state']['task_id'] == old['task_id'] and page.context['candidates'] == []
    with m.DB.session() as db:
        task = db.get(IssueTask, old['task_id'])
        assert (task.status, task.note, task.updated_at) == saved
        assert json.loads(task.note)['first_correction']['selected_at']
