"""Manual GitHub proposals wait for review, without changing older receipt identities."""
import json
import pytest
from sqlalchemy import select
from fastapi.testclient import TestClient

from backend import app as m, auth, correction_journal
from backend.models import CorrectionOperation, IssueTask
from test_corrector_customer_workflow import customer as customer
from test_corrector_operational import HTML, URL, harness as harness


@pytest.mark.parametrize('kind', ['deep', 'bulk'])
@pytest.mark.parametrize('review_only', [True, False])
def test_manual_review_disables_deferred_auto_merge_even_for_a_deterministic_fix(harness, monkeypatch, kind, review_only):
    state, _ = harness
    key = 'viewport_not_set'
    state.update(key=key, sources={'index.html': HTML}, report={
        'issues': {key: {'count': 1, 'examples': [URL]}},
        'pages': [{'url': URL, 'final_url': URL, 'status_code': 200, 'content_type': 'text/html',
                   'canonical': URL, 'meta_viewport': None, 'meta_viewport_tag_count': 0}]})
    cfg = {'repo': 'review/site', 'branch': 'main', 'mode': 'auto'}
    monkeypatch.setattr(m, '_project_github_cfg', lambda *a, **k: cfg)
    monkeypatch.setattr(m, '_sitemap_https_page', lambda *a: True)
    rights, original = [], m._bloc_verification

    def capture(data, **kwargs):
        rights.append(kwargs['fusion_auto'])
        return original(data, **kwargs)

    monkeypatch.setattr(m, '_bloc_verification', capture)
    client = TestClient(m.app)
    client.cookies.set(auth.SESSION_COOKIE_NAME,
        auth.make_session_token(user_id=state['user'].id, secret=m._safe_env('SEO_AGENT_SECRET_KEY')))
    client.get('/')
    headers = {m._CSRF_HEADER_NAME: client.cookies.get(m._CSRF_COOKIE_NAME)}
    if kind == 'deep':
        response = client.post(f'/api/projects/review/issues/{key}/deep-fix', headers=headers,
            json={'url': URL, 'crawl_ts': state['crawl_ts'], 'review_only': review_only})
        values = {'issue_key': key, 'body': {'url': URL, 'crawl_ts': state['crawl_ts']}}
        if review_only:
            values['body']['review_only'] = True
        action = 'api_issue_deep_fix'
    else:
        response = client.post('/api/projects/review/github/bulk-fix', headers=headers, json={'review_only': review_only})
        values = {'body': {'review_only': True}} if review_only else {}
        action = 'api_github_bulk_fix'
    assert response.status_code == 200, response.text
    assert state['written'] and state['pr_bodies']
    assert rights and any(rights) == (not review_only)
    if review_only:
        assert not any(rights)
    expected = correction_journal.request_key(state['user'].id, state['project'].id,
        action, {'values': values, 'github': cfg})
    with m.DB.session() as db:
        operation = db.scalar(select(CorrectionOperation).where(CorrectionOperation.project_id == state['project'].id))
        assert operation.request_key == expected


def test_the_review_contains_changes_after_the_first_thirty_lines(customer, monkeypatch):
    state, send, used, _, _ = customer
    old = '<html><head>\n' + '\n' * 35 + '<title>Avant</title></head><body></body></html>'
    new = old.replace('Avant', 'Après')
    monkeypatch.setattr(m, '_github_find_seo_files', lambda *a: [{'path': 'index.html', 'content': old}])
    monkeypatch.setattr(m, '_openai_generate_file_patch', lambda **kw: {'patched_content': new})
    response = send()
    assert response.status_code == 200
    assert 'Avant' not in response.json()['original_preview']
    diff = response.json()['diff']
    assert '-<title>Avant' in diff and '+<title>Après' in diff
    assert used() == 1 and not state['writes'] and not state['posts']


def test_canceled_review_reopens_the_same_paid_preview_without_another_model_call(customer):
    state, send, used, _, _ = customer
    first, resumed = send(), send()
    assert first.status_code == resumed.status_code == 200
    assert first.json()['patched_content'] == resumed.json()['patched_content']
    assert used() == 1 and state['models'] == 1
    assert not state['writes'] and not state['posts']


def test_a_github_proposal_does_not_declare_the_site_modified_before_explicit_publication(customer, monkeypatch, tmp_path):
    state, send, _, uid, slug = customer
    preview = send().json()
    response = send(confirm=True, file_path=preview['file'], patched_content=preview['patched_content'])
    assert response.status_code == 200
    with m.DB.session() as db:
        task = db.scalar(select(IssueTask).where(IssueTask.user_id == uid))
    progress = m.first_correction.progress([task])
    assert progress['pr_url'] and not progress['declared'] and not progress['verified']
    resume = m.first_audit.resume({'slug': slug}, progress, None, {}, True, False)
    assert resume['title'] == 'Votre proposition attend sa publication'
    client = state['client']
    monkeypatch.setattr(m, '_runs_dir_pour_slug', lambda *a: tmp_path)
    report_dir = tmp_path / slug / '20261002-090000' / 'audit'
    report_dir.mkdir(parents=True)
    (report_dir / 'report.json').write_text(json.dumps({'meta': {'base_url': URL},
        'pages': [{'url': URL, 'status_code': 200, 'content_type': 'text/html', 'indexable': True}],
        'issues': {'missing_title': {'count': 1, 'examples': [URL]}}}))
    page = client.get(f'/projects/{slug}/first-correction')
    assert page.status_code == 200 and 'Relire la proposition GitHub' in page.text
    assert 'Faites vérifier le résultat' not in page.text
    declared = client.post(f'/projects/{slug}/first-correction', data={'action': 'ready',
        'task_id': str(task.id), '_csrf': client.cookies.get(m._CSRF_COOKIE_NAME)}, follow_redirects=False)
    assert declared.status_code == 303 and 'err=' not in declared.headers['location']
    with m.DB.session() as db:
        task = db.get(IssueTask, task.id)
    progress = m.first_correction.progress([task])
    assert progress['declared'] and not progress['verified']
    assert 'Faites vérifier le résultat' in client.get(f'/projects/{slug}/first-correction').text
