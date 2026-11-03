"""Real guarded preview path with simulated transports; never real AI/site writes."""
import json
import threading
from datetime import timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select

from backend import app as m, autocorrection as a, correction_execution as e, correction_guard
from backend import correction_decisions as d, first_correction
from backend.models import AccountMember, AuditLog, AutocorrectionRun, CorrectionOperation, IssueTask, Project
from test_customer_activation import client, lab as lab, user
from test_correction_decisions import add, application, case
from test_correction_memory import CONTROL, NOW

SOURCE = '<html><head><title></title></head><body><h1>Atelier</h1></body></html>'
PATCHED = SOURCE.replace('<title></title>', '<title>Atelier de réparation</title>')


@pytest.fixture()
def prepared(lab, monkeypatch):
    owner, project, task = case(lab)
    with lab['db'].session() as db:
        p = db.get(Project, project.id)
        p.settings = {'github_repo':'fixture/site', 'github_branch':'main', 'github_mode':'review'}
        db.commit();project = p
    add(lab['db'], task)
    monkeypatch.setenv('NOYARU_AUTOCORRECTION_PAUSED', 'false')
    monkeypatch.setenv('ANTHROPIC_API_KEY', 'simulated-private-provider-key')
    monkeypatch.setattr(m, '_correction_gate', lambda *args, **kwargs: (True, '', 1, 'fixture-model'))
    monkeypatch.setattr(m, '_effective_user_connection_value', lambda **kwargs: ('simulated-private-github-key', 'user'))
    monkeypatch.setattr(m, '_open_pr_for_issue', lambda **kwargs: None)
    monkeypatch.setattr(m, '_github_find_seo_files', lambda *args: [{'path':'service/index.html', 'content':SOURCE, 'sha':'fixture'}])
    monkeypatch.setattr(m, '_correction_ai_provider', lambda: 'anthropic')
    monkeypatch.setattr(m, '_correction_ai_model', lambda provider: 'fixture-model')
    monkeypatch.setattr(m, '_noter_consommation_ia', lambda **kwargs: None)
    calls = []
    def post(url, **kwargs):
        calls.append((url, kwargs))
        assert url.endswith('/messages'), 'No GitHub or other write may leave the process'
        payload = {'edits':[{'old':'<title></title>', 'new':'<title>Atelier de réparation</title>'}],
                   'description':'Titre précis pour cette page', 'pr_title':'fix: title'}
        return SimpleNamespace(status_code=200, json=lambda: {'content':[{'type':'text','text':json.dumps(payload)}]})
    monkeypatch.setattr(m.requests, 'post', post)
    return {**lab, 'owner':owner, 'project':project, 'task':task, 'calls':calls}


def grant(x, **kwargs):
    with x['db'].session() as db:
        item = d.evaluate(db, db.get(IssueTask, x['task'].id))
    return a.authorize(x['project'].id, x['task'].id, x['owner'].id, item['decision_key'],
                       consent=kwargs.get('consent', True), publication_checked=kwargs.get('publication_checked', True), lang='fr')


def read(x, rid):
    with x['db'].session() as db:
        return db.get(AutocorrectionRun, rid)


def submit(c, path, data=None):
    c.get(path.split('/corrections')[0]+'/corrections/autonomy')
    return c.post(path, data={**(data or {}), '_csrf':c.cookies.get(m._CSRF_COOKIE_NAME)}, follow_redirects=False)


def test_real_guarded_preview_is_bounded_journaled_charged_once_and_never_writes_github(prepared):
    x = prepared;run = grant(x)
    assert not x['calls']
    assert a.execute(run.id) == 'preview_ready'
    stored = read(x, run.id)
    assert stored.reserved_credits == 1 and stored.provider_calls == 1 and stored.output_tokens == 2000
    with x['db'].session() as db:
        operation = db.get(CorrectionOperation, stored.operation_id)
        assert operation.state == 'completed' and operation.charged
        assert operation.data['preview']['patched_content'] == PATCHED
        assert not operation.data.get('writes_started') and not operation.data.get('pr')
        audit = list(db.scalars(select(AuditLog).where(AuditLog.action.like('autocorrection.%'))))
    assert len(audit) == 3 and all('source-secret' not in repr(row.meta) for row in audit)
    assert a.execute(run.id) == 'preview_ready' and len(x['calls']) == 1
    assert grant(x).id == run.id
    assert a.reconcile(run.id) == 'preview_ready' and len(x['calls']) == 1


@pytest.mark.parametrize('confirmation', ['consent', 'publication_checked'])
def test_both_owner_confirmations_are_required(prepared, confirmation):
    with pytest.raises(e.ExecutionStopped, match='consent_required'):
        grant(prepared, **{confirmation:False})
    assert not prepared['calls']
    with prepared['db'].session() as db:
        assert db.scalar(select(func.count()).select_from(AutocorrectionRun)) == 0


@pytest.mark.parametrize('change', ['proposal','application','repo','url','owner','decision','pause','expired','policy','revoked','quota'])
def test_changed_evidence_rights_target_quota_or_mandate_stop_before_ai(prepared, monkeypatch, change):
    x = prepared;run = grant(x)
    with x['db'].session() as db:
        row = db.get(AutocorrectionRun, run.id);task = db.get(IssueTask, x['task'].id);p = db.get(Project, x['project'].id)
        if change == 'proposal':task.note = json.dumps({'pr_number':99})
        elif change == 'application':
            note = first_correction.note_data(task.note);note['first_correction']['declared_at'] = NOW.isoformat();task.note = json.dumps(note)
        elif change == 'repo':p.settings = {**p.settings, 'github_repo':'fixture/other'}
        elif change == 'url':task.url = 'https://other.example/'
        elif change == 'owner':p.owner_user_id = x['admin'].id
        elif change == 'pause':p.settings = {**p.settings, 'autocorrection_paused':True}
        elif change == 'expired':row.expires_at = a.clock()-timedelta(seconds=1)
        elif change == 'policy':row.policy_version = 99
        elif change == 'revoked':row.revoked_at = a.clock()
        db.commit()
    if change == 'decision':add(x['db'], x['task'], at=CONTROL+timedelta(hours=1), result='resolved')
    if change == 'quota':monkeypatch.setattr(m, '_correction_gate', lambda *args, **kwargs: (False, 'quota', 0, ''))
    assert a.execute(run.id) in {'review_required','expired','paused'}
    assert not x['calls'] and read(x, run.id).provider_calls == 0


@pytest.mark.parametrize('options', [{'result':'unverified'}, {'baseline':False}, {'urls':['https://atelier.example/else/']}, {'result':'resolved'}])
def test_non_actionable_controls_cannot_be_authorized(prepared, options):
    x = prepared
    add(x['db'], x['task'], at=CONTROL+timedelta(hours=1), **options)
    with pytest.raises(e.ExecutionStopped):grant(x)
    assert not x['calls']


def test_threshold_of_two_applications_blocks_another_authorization(prepared):
    x = prepared;application(x['db'], x['task'], 3)
    add(x['db'], x['task'], at=CONTROL+timedelta(hours=1))
    with pytest.raises(e.ExecutionStopped, match='review_required'):grant(x)


def test_unknown_provider_result_is_not_retried_or_fallen_back(prepared, monkeypatch):
    x = prepared;run = grant(x)
    monkeypatch.setenv('OPENAI_API_KEY', 'simulated-fallback-key')
    calls = []
    def timeout(*args, **kwargs):calls.append(1);raise TimeoutError('secret-internal-provider-detail')
    monkeypatch.setattr(m.requests, 'post', timeout)
    assert a.execute(run.id) == 'review_required'
    assert read(x, run.id).reason == 'provider_uncertain' and len(calls) == 1
    assert a.execute(run.id) == 'review_required' and len(calls) == 1
    assert a.reconcile(run.id) == 'review_required' and len(calls) == 1


def test_revocation_during_provider_call_preserves_known_result_without_more_requests(prepared, monkeypatch):
    x = prepared;run = grant(x);post = m.requests.post
    def stopped(url, **kwargs):
        a.revoke(x['project'].id, run.id, x['owner'].id)
        return post(url, **kwargs)
    monkeypatch.setattr(m.requests, 'post', stopped)
    assert a.execute(run.id) == 'preview_ready'
    assert read(x, run.id).revoked_at and len(x['calls']) == 1
    # An already sent request may finish; its receipt and credit are kept.
    with x['db'].session() as db:assert db.get(CorrectionOperation, read(x, run.id).operation_id).charged


def test_revocation_stops_a_full_file_fallback_before_transmission(prepared, monkeypatch):
    x = prepared;run = grant(x);calls = []
    def no_edit(*args, **kwargs):
        calls.append(1);a.revoke(x['project'].id, run.id, x['owner'].id)
        return SimpleNamespace(status_code=200, json=lambda: {'content':[{'type':'text','text':'{"edits":[]}'}]})
    monkeypatch.setattr(m.requests, 'post', no_edit)
    assert a.execute(run.id) == 'revoked' and len(calls) == 1


def test_revocation_before_any_provider_request_releases_only_its_unstarted_journal(prepared, monkeypatch):
    x = prepared;run = grant(x);find = m._github_find_seo_files
    def revoked(*args):
        a.revoke(x['project'].id,run.id,x['owner'].id)
        return find(*args)
    monkeypatch.setattr(m,'_github_find_seo_files',revoked)
    assert a.execute(run.id) == 'revoked' and not x['calls']
    with x['db'].session() as db:
        op = db.get(CorrectionOperation, read(x,run.id).operation_id)
        assert op.state == 'abandoned' and op.request_key is None and not op.charged


def test_two_workers_cannot_start_the_same_run_or_spend_twice(prepared, monkeypatch):
    x = prepared;run = grant(x);entered = threading.Event();release = threading.Event();post = m.requests.post;results = []
    def slow(url, **kwargs):
        entered.set();assert release.wait(10);return post(url, **kwargs)
    monkeypatch.setattr(m.requests, 'post', slow)
    thread = threading.Thread(target=lambda:results.append(a.execute(run.id)))
    thread.start();assert entered.wait(10)
    try:assert a.execute(run.id) == 'busy'
    finally:release.set();thread.join(10)
    assert not thread.is_alive() and results == ['preview_ready'] and len(x['calls']) == 1


@pytest.mark.parametrize('field,value', [('provider_calls',4), ('output_tokens',20000), ('input_chars',360000)])
def test_durable_request_budget_stops_before_the_next_transport(prepared, field, value):
    x = prepared;run = grant(x)
    with x['db'].session() as db:
        row = db.get(AutocorrectionRun, run.id);row.state = 'running';setattr(row, field, value);db.commit()
    with correction_guard.correction_lock(x['db'], x['owner'].id), e.bounded(a.PreviewPolicy(run.id)):
        with pytest.raises(e.ExecutionStopped, match='budget'):
            e.provider_request(system='system', user_msg='data', max_tokens=2000)
        with pytest.raises(e.ExecutionStopped, match='write_not_authorized'):
            m._github_api_post('/repos/fixture/site/git/refs', token='fixture', json_body={})
        with pytest.raises(e.ExecutionStopped, match='target_changed'):
            m._github_api_get('/repos/foreign/site/contents/page.html', token='fixture')
    assert not x['calls']


def test_request_started_then_interrupted_is_not_reexecuted(prepared):
    x = prepared;run = grant(x)
    with x['db'].session() as db:
        row = db.get(AutocorrectionRun, run.id);row.state = 'running';row.provider_calls = 1;db.commit()
    assert a.execute(run.id) == 'review_required' and not x['calls']
    assert read(x, run.id).reason == 'interrupted'


def test_saved_preview_is_reconciled_and_charged_once_after_lost_response(prepared):
    x = prepared;run = grant(x);assert a.execute(run.id) == 'preview_ready'
    with x['db'].session() as db:
        row = db.get(AutocorrectionRun, run.id);row.state = 'running';row.finished_at = None;db.commit()
    assert a.execute(run.id) == 'preview_ready'
    assert read(x, run.id).reason == 'reconciled' and len(x['calls']) == 1


def test_saved_unbilled_preview_recovers_after_charge_interruption_without_more_ai(prepared, monkeypatch):
    x = prepared;run = grant(x);charge = m._correction_charge
    def failed(*args, **kwargs):
        raise correction_guard.CorrectionStoreUnavailable()
    monkeypatch.setattr(m, '_correction_charge', failed)
    assert a.execute(run.id) == 'review_required'
    with x['db'].session() as db:
        op = db.get(CorrectionOperation, read(x, run.id).operation_id)
        assert op.data['preview']['patched_content'] == PATCHED and not op.charged
    monkeypatch.setattr(m, '_correction_charge', charge)
    assert a.reconcile(run.id) == 'preview_ready'
    assert a.reconcile(run.id) == 'preview_ready' and len(x['calls']) == 1
    with x['db'].session() as db:assert db.get(CorrectionOperation, op.id).charged


def test_human_draft_transport_rechecks_target_and_forbids_merge_delete_and_ai(prepared):
    x = prepared;run = grant(x);a.execute(run.id)
    policy = a.HumanDraftPolicy(run.id)
    with correction_guard.correction_lock(x['db'], x['owner'].id), e.bounded(policy):
        for path, method in [('/repos/fixture/site/pulls/1/merge','PUT'), ('/repos/fixture/site/contents/service/index.html','DELETE'), ('/graphql','POST')]:
            with pytest.raises(e.ExecutionStopped):e.github_write(path, method)
        with pytest.raises(e.ExecutionStopped, match='ai_not_authorized'):
            e.provider_request(system='s', user_msg='u', max_tokens=1)
        with x['db'].session() as db:
            project = db.get(Project, x['project'].id)
            project.settings = {**project.settings, 'github_repo':'fixture/other'};db.commit()
        with pytest.raises(e.ExecutionStopped, match='application_changed'):
            e.github_write('/repos/fixture/site/git/refs', 'POST')
    assert len(x['calls']) == 1


def test_openai_temperature_retry_reserves_each_real_request(prepared, monkeypatch):
    x = prepared;run = grant(x);calls = []
    monkeypatch.setenv('OPENAI_API_KEY', 'simulated-openai-key')
    def response(*args, **kwargs):
        calls.append(kwargs['json'])
        if len(calls) == 1:return SimpleNamespace(status_code=400, text='temperature unsupported')
        return SimpleNamespace(status_code=200, json=lambda:{'choices':[{'message':{'content':'{}'}}]})
    monkeypatch.setattr(m.requests, 'post', response)
    with x['db'].session() as db:
        row = db.get(AutocorrectionRun, run.id);row.state = 'running';db.commit()
    with correction_guard.correction_lock(x['db'], x['owner'].id), e.bounded(a.PreviewPolicy(run.id)):
        assert m._openai_chat_text(system='s', user_msg='u', model='autonomy-fixture-peer', max_tokens=100,
                                   temperature=.1, json_mode=True) == '{}'
    assert len(calls) == 2
    stored = read(x, run.id)
    assert stored.provider_calls == 2 and stored.output_tokens == 200 and stored.input_chars == 4


def test_preexisting_unknown_operation_blocks_preparation(prepared):
    x = prepared;run = grant(x)
    with x['db'].session() as db:
        db.add(CorrectionOperation(payer_id=x['owner'].id, project_id=x['project'].id,
                                  action='api_issue_deep_fix', request_key='0'*64, state='blocked', data={}))
        db.commit()
    assert a.execute(run.id) == 'review_required'
    assert read(x, run.id).reason == 'operation_pending' and not x['calls']


def test_routes_require_owner_csrf_and_preserve_equal_slug_isolation(prepared):
    x = prepared;owner = client(x['owner']);member = user(x['db']);foreign = user(x['db'])
    with x['db'].session() as db:
        db.add(AccountMember(owner_user_id=x['owner'].id, member_user_id=member.id))
        db.add(Project(owner_user_id=foreign.id, slug=x['project'].slug, site_name='Other', base_url='https://other.example/', settings={}))
        db.commit()
        key = d.evaluate(db, db.get(IssueTask, x['task'].id))['decision_key']
    path = f'/projects/{x["project"].slug}/corrections/autonomy'
    assert owner.get(path).status_code == client(member).get(path).status_code == 200
    assert owner.post(path+'/authorize', data={'task_id':x['task'].id}).status_code == 403
    data = {'task_id':x['task'].id, 'decision_key':key, 'consent':'true', 'publication_checked':'true'}
    assert submit(client(member), path+'/authorize', data).status_code == 403
    assert submit(client(foreign), path+'/authorize', data).status_code == 404
    response = submit(owner, path+'/authorize', data)
    assert response.status_code == 303 and 'msg=queued' in response.headers['location']
    assert not x['calls']


@pytest.mark.parametrize('lang,heading', [('fr','Préparation autonome'), ('en','Automatic preparation')])
def test_page_has_escaped_preview_and_explicit_separate_draft_confirmation(prepared, lang, heading):
    x = prepared;run = grant(x);a.execute(run.id)
    path = f'/projects/{x["project"].slug}/corrections/autonomy'
    response = client(x['owner']).get(path+'?lang='+lang)
    assert response.status_code == 200 and heading in response.text
    assert '&lt;title&gt;' in response.text and '<title>Atelier de réparation</title>' not in response.text
    assert f'{run.id}/draft' in response.text and 'name="confirm"' in response.text
    assert response.headers['cache-control'] == 'no-store'
    assert 'simulated-private' not in response.text


def test_draft_confirmation_never_requests_another_preview(prepared, monkeypatch):
    x = prepared;run = grant(x);a.execute(run.id);calls = []
    original = m.api_github_fix
    def confirm(request, slug, key, body):
        assert body.confirm and body.patched_content == PATCHED
        assert isinstance(e.current(), a.HumanDraftPolicy)
        calls.append(body)
        return m.JSONResponse({'ok':True, 'pr_number':1})
    monkeypatch.setattr(m, 'api_github_fix', confirm)
    path = f'/projects/{x["project"].slug}/corrections/autonomy/{run.id}/draft'
    assert 'consent_required' in submit(client(x['owner']), path).headers['location']
    assert not calls
    assert 'msg=draft_done' in submit(client(x['owner']), path, {'confirm':'true'}).headers['location']
    assert len(calls) == len(x['calls']) == 1
    monkeypatch.setattr(m, 'api_github_fix', original)


@pytest.mark.parametrize('changed', [False, True])
def test_real_human_draft_path_reuses_preview_and_refuses_changed_source(prepared, monkeypatch, changed):
    import base64
    x = prepared;run = grant(x);a.execute(run.id);writes = []
    def get(path, **kwargs):
        if '/git/ref/' in path:return {'object':{'sha':'base-sha'}}
        if '/contents/' in path:
            source = SOURCE+'<!-- changed remotely -->' if changed else SOURCE
            return {'sha':'file-sha', 'content':base64.b64encode(source.encode()).decode()}
        raise AssertionError('Unexpected GitHub read: '+path)
    def post(url, **kwargs):
        writes.append(('POST',url,kwargs['json']))
        if url.endswith('/git/refs'):result = {}
        elif url.endswith('/pulls'):
            assert kwargs['json']['draft'] is True
            result = {'number':91, 'html_url':'https://github.com/fixture/site/pull/91',
                      'head':{'sha':'patch-sha'}, 'node_id':'fixture-node'}
        else:raise AssertionError('Unexpected write: '+url)
        return SimpleNamespace(status_code=201, json=lambda:result)
    def put(url, **kwargs):
        writes.append(('PUT',url,kwargs['json']))
        assert base64.b64decode(kwargs['json']['content']).decode() == PATCHED
        return SimpleNamespace(status_code=200, json=lambda:{'content':{'sha':'new-file-sha'},
                            'commit':{'sha':'patch-sha', 'html_url':'https://github.com/fixture/site/commit/patch-sha'}})
    monkeypatch.setattr(m, '_github_api_get', get)
    monkeypatch.setattr(m.requests, 'post', post);monkeypatch.setattr(m.requests, 'put', put)
    path = f'/projects/{x["project"].slug}/corrections/autonomy/{run.id}/draft'
    response = submit(client(x['owner']), path, {'confirm':'true'})
    if changed:
        assert 'error=preparation_failed' in response.headers['location'] and not writes
    else:
        assert 'msg=draft_done' in response.headers['location']
        assert len(writes) == 3 and all('/merge' not in value[1] for value in writes)
        with x['db'].session() as db:
            note = first_correction.note_data(db.get(IssueTask,x['task'].id).note)
            assert note['pr_number'] == 91 and not note['verification']['fusion_auto']
    assert len(x['calls']) == 1


def test_operator_pause_and_scheduler_disable_do_not_consume_the_queue(prepared, monkeypatch):
    x = prepared;run = grant(x)
    monkeypatch.setenv('NOYARU_AUTOCORRECTION_PAUSED', 'true')
    assert a.sweep()['examined'] == 0 and read(x, run.id).state == 'queued'
    monkeypatch.setenv('NOYARU_AUTOCORRECTION_PAUSED', 'false')
    monkeypatch.setenv('SEO_AGENT_DISABLE_SCHEDULERS', 'true')
    assert a.sweep()['examined'] == 0 and not x['calls']


def test_project_pause_keeps_the_queue_resumable_and_excludes_it_from_the_scan(prepared, monkeypatch):
    x = prepared;run = grant(x)
    with x['db'].session() as db:
        p = db.get(Project, x['project'].id);p.settings = {**p.settings,'autocorrection_paused':True};db.commit()
    assert a.execute(run.id) == 'paused' and read(x,run.id).state == 'queued'
    monkeypatch.setenv('SEO_AGENT_DISABLE_SCHEDULERS', 'false')
    assert a.sweep()['examined'] == 0
    with x['db'].session() as db:
        p = db.get(Project, x['project'].id);p.settings = {**p.settings,'autocorrection_paused':False};db.commit()
    assert a.sweep()['states'] == {'preview_ready':1} and len(x['calls']) == 1


def test_deleting_project_removes_mandates_and_leaves_other_projects_intact(prepared):
    x = prepared;run = grant(x)
    with x['db'].session() as db:
        db.delete(db.get(Project, x['project'].id));db.commit()
    assert read(x, run.id) is None
