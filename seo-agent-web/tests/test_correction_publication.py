"""Real guarded publication path; every HTTP transport is simulated locally."""
import base64
import copy
from datetime import timedelta
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest
from sqlalchemy import func, select

from backend import app as m, autocorrection as a, correction_execution as e, correction_guard
from backend import correction_publication as p, publication_control as c, correction_memory as memory, first_correction
from backend.models import AutocorrectionRun, CorrectionPublication, JobRecord, Project, IssueTask, UsageEvent
from test_autocorrection import prepared as prepared, grant, SOURCE, PATCHED
from test_customer_activation import client, lab as lab, user
from test_correction_decisions import add
from test_correction_memory import NOW, stamp

BASE='a'*40
HEAD='b'*40
MERGE='c'*40


def stored(x):
    with x['db'].session() as db:return db.get(CorrectionPublication,x['pub'].id)


@pytest.fixture()
def publication(prepared,monkeypatch):
    x=prepared
    monkeypatch.setattr(a,'clock',lambda:NOW)
    run=grant(x)
    assert a.execute(run.id)=='preview_ready'
    with x['db'].session() as db:
        preview,_=a.saved_preview(db,db.get(AutocorrectionRun,run.id))
    pub=p.authorize(x['project'].id,run.id,x['owner'].id,p.preview_key(preview),
        approved=True,consent=True,pages_budget=100,environment='production',lang='fr')
    state={'draft':True,'merged':False,'branch':'','checks':'success','deployed':False,
           'source':SOURCE,'base':BASE,'head':HEAD,'protection':True,'origin':'https://atelier.example/',
           'transports':[],'lost':''}
    def response(value,status=200):return SimpleNamespace(status_code=status,text='',json=lambda:copy.deepcopy(value))
    def pr():
        return dict(number=91,node_id='fixture-node',html_url='https://github.com/fixture/site/pull/91',
            head={'sha':state['head'],'ref':state['branch'],'repo':{'full_name':'fixture/site'}},
            base={'sha':BASE,'ref':'main','repo':{'full_name':'fixture/site'}},state='closed' if state['merged'] else 'open',
            draft=state['draft'],merged=state['merged'],merge_commit_sha=MERGE if state['merged'] else None,
            merged_at=NOW.isoformat() if state['merged'] else None,mergeable=True,mergeable_state='clean',changed_files=1,commits=1)
    def get(url,**kw):
        path=urlsplit(url).path;params=kw.get('params') or {};state['transports'].append(('GET',path,params))
        if path.endswith('/protection'):
            return response({'required_status_checks':{'strict':state['protection'],'contexts':['CI']},'enforce_admins':{'enabled':True}})
        if '/git/ref/' in path:return response({'object':{'sha':state['base']}})
        if '/contents/' in path:
            content=PATCHED if params.get('ref')==HEAD else state['source']
            return response({'sha':'file-sha','content':base64.b64encode(content.encode()).decode()})
        if path.endswith('/pulls/91'):return response(pr())
        if path.endswith('/pulls'):return response([pr()])
        if path.endswith('/files'):return response([{'filename':'service/index.html','status':'modified'}])
        if path.endswith('/check-runs'):
            return response({'total_count':1,'check_runs':[{'name':'CI','head_sha':HEAD,'status':'completed','conclusion':state['checks']}]})
        if path.endswith('/status'):return response({'total_count':0,'statuses':[]})
        if path.endswith('/deployments'):
            return response([{'id':8,'sha':MERGE,'environment':'production','production_environment':True,'transient_environment':False}] if state['deployed'] else [])
        if path.endswith('/deployments/8/statuses'):
            return response([{'id':9,'state':'success','environment_url':state['origin'],'created_at':NOW.isoformat()}])
        raise AssertionError(path)
    def post(url,**kw):
        path=urlsplit(url).path;body=kw['json'];state['transports'].append(('POST',path,body))
        if path.endswith('/git/refs'):state['branch']=body['ref'].removeprefix('refs/heads/');value={}
        elif path.endswith('/pulls'):value=pr()
        elif path=='/graphql':state['draft']=False;value={'data':{'markPullRequestReadyForReview':{'pullRequest':{'isDraft':False}}}}
        else:raise AssertionError('Unexpected write '+path)
        if state['lost']==path:raise TimeoutError('private-provider-detail')
        return response(value,200 if path=='/graphql' else 201)
    def put(url,**kw):
        path=urlsplit(url).path;body=kw['json'];state['transports'].append(('PUT',path,body))
        if path.endswith('/merge'):
            assert body=={'sha':HEAD,'merge_method':'squash'}
            state['merged']=True;state['base']=MERGE;value={'merged':True,'sha':MERGE}
        elif '/contents/' in path:
            assert base64.b64decode(body['content']).decode()==PATCHED
            value={'commit':{'sha':HEAD},'content':{'sha':'newfile'}}
        else:raise AssertionError(path)
        if state['lost']==path:raise TimeoutError('private-provider-detail')
        return response(value)
    monkeypatch.setattr(m.requests,'get',get)
    monkeypatch.setattr(m.requests,'post',post)
    monkeypatch.setattr(m.requests,'put',put)
    return {**x,'run':run,'pub':pub,'remote':state}


def step(x):return p.step(x['pub'].id,force=True)


def merge(x):
    assert step(x)=='waiting_checks'
    assert step(x)=='waiting_checks'  # ready-for-review is separately journaled
    assert step(x)=='waiting_deployment'


def deploy(x,monkeypatch):
    merge(x);x['remote']['deployed']=True
    assert step(x)=='deployment_confirmed'
    monkeypatch.setattr(a,'clock',lambda:NOW+timedelta(seconds=10))
    assert step(x)=='waiting_control'
    return stored(x).job_id


def test_full_real_confirmation_and_merge_are_pinned_no_extra_ai_and_no_crawl_before_deployment(publication,monkeypatch):
    x=publication;merge(x)
    assert len(x['calls'])==1
    writes=[r for r in x['remote']['transports'] if r[0]!='GET']
    assert len(writes)==5 and stored(x).writes==5
    with x['db'].session() as db:
        task=db.get(IssueTask,x['task'].id)
        note=first_correction.note_data(task.note)
        assert note['verification']['publication_id']==x['pub'].id
        assert m._reprendre_une_verification(task)=='autocorrection'
        assert db.scalar(select(func.count()).select_from(JobRecord))==0
    assert step(x)=='waiting_deployment'
    x['remote']['deployed']=True
    assert step(x)=='deployment_confirmed'
    monkeypatch.setattr(a,'clock',lambda:NOW+timedelta(seconds=10))
    assert step(x)=='waiting_control'
    row=stored(x)
    assert row.job_issued and row.reserved_pages==100
    assert c.enqueue(row.id)==row.job_id
    with x['db'].session() as db:
        task=db.get(IssueTask,x['task'].id)
        assert row.applied_key==memory.attempt_key(task)!=row.application_key
        assert db.scalar(select(func.count()).select_from(JobRecord))==1
        assert sum(db.scalars(select(UsageEvent.amount).where(UsageEvent.metric=='pages_crawled_month')))==100


@pytest.mark.parametrize('change,reason',[('protection','branch_protection'),('source','source_changed')])
def test_changed_source_or_unprotected_branch_never_writes(publication,change,reason):
    x=publication;x['remote'][change]=False if change=='protection' else SOURCE+'changed'
    assert step(x)=='review_required'
    assert stored(x).reason==reason
    assert not [r for r in x['remote']['transports'] if r[0]!='GET']


@pytest.mark.parametrize('change,reason',[('head','pr_changed'),('base','base_changed'),('checks','checks_failed')])
def test_changed_commit_base_or_failed_checks_stop_before_merge(publication,change,reason):
    x=publication;assert step(x)=='waiting_checks'
    x['remote'][change]='failure' if change=='checks' else 'd'*40
    assert step(x)=='review_required' and stored(x).reason==reason
    assert not any(r[1].endswith('/merge') for r in x['remote']['transports'])


@pytest.mark.parametrize('lost',['/repos/fixture/site/pulls','/graphql','/repos/fixture/site/pulls/91/merge'])
def test_unknown_write_is_read_back_without_repeating_it(publication,lost):
    x=publication;x['remote']['lost']=lost
    for _ in range(7):step(x)
    writes=[r for r in x['remote']['transports'] if r[0]!='GET' and r[1]==lost]
    assert len(writes)==1
    assert stored(x).phase=='waiting_deployment', stored(x).reason


@pytest.mark.parametrize('stage',['queued','waiting_checks','waiting_deployment','waiting_control'])
def test_revocation_stops_further_transports_and_refunds_only_unstarted_control(publication,monkeypatch,stage):
    x=publication
    if stage=='waiting_checks':assert step(x)==stage
    elif stage=='waiting_deployment':merge(x)
    elif stage=='waiting_control':deploy(x,monkeypatch)
    before=len(x['remote']['transports'])
    p.revoke(x['pub'].id,x['owner'].id)
    assert step(x)=='revoked' and len(x['remote']['transports'])==before
    if stage=='waiting_control':
        with x['db'].session() as db:
            assert db.get(JobRecord,stored(x).job_id).status=='canceled'
            assert sum(db.scalars(select(UsageEvent.amount).where(UsageEvent.metric=='pages_crawled_month')))==0


def test_wrong_deployment_target_never_queues_control(publication):
    x=publication;merge(x);x['remote'].update(deployed=True,origin='https://foreign.example/')
    assert step(x)=='review_required' and stored(x).reason=='deployment_target'
    assert not stored(x).job_issued


def test_control_settlement_is_once_and_stays_in_reservation_period(publication,monkeypatch):
    x=publication;jid=deploy(x,monkeypatch)
    job=m._load_job(jid);c.settle(job,actual=7);c.settle(job,actual=7)
    assert stored(x).control_settled
    with x['db'].session() as db:
        events=list(db.scalars(select(UsageEvent).where(UsageEvent.metric=='pages_crawled_month')))
        assert len(events)==2 and sum(v.amount for v in events)==7
        assert len({v.period for v in events})==1


@pytest.mark.parametrize('result,baseline,state',[('resolved',True,'verified'),('still_present',True,'review_required'),('resolved',False,'review_required')])
def test_only_exact_trusted_control_can_verify(publication,monkeypatch,result,baseline,state):
    x=publication;jid=deploy(x,monkeypatch);when=NOW+timedelta(minutes=2)
    with x['db'].session() as db:
        job=db.get(JobRecord,jid);job.status='done';job.finished_at=when.timestamp()
        job.result={**job.result,'timestamp':stamp(when)}
        db.commit()
    add(x['db'],x['task'],at=when,result=result,baseline=baseline)
    monkeypatch.setattr(a,'clock',lambda:when+timedelta(seconds=1))
    assert step(x)==state
    assert bool(stored(x).observation_id)==(state=='verified')


@pytest.mark.parametrize('mutation',['attempts','target','config','revoked'])
def test_worker_rechecks_scope_and_never_runs_twice(publication,monkeypatch,mutation):
    x=publication;jid=deploy(x,monkeypatch)
    with x['db'].session() as db:
        job=db.get(JobRecord,jid);job.status='running';job.attempts=2 if mutation=='attempts' else 1
        if mutation=='target':db.get(Project,x['project'].id).base_url='https://other.example/'
        if mutation=='config':db.get(Project,x['project'].id).settings={}
        if mutation=='revoked':db.get(CorrectionPublication,x['pub'].id).revoked_at=NOW
        db.commit()
    assert not c.guard_start(m._load_job(jid))


@pytest.mark.parametrize('lang,heading',[('fr','Publier et vérifier une correction'),('en','Publish and verify a correction')])
def test_owner_page_and_csrf_do_not_activate_a_mandate(prepared,lang,heading):
    x=prepared;run=grant(x);a.execute(run.id)
    url=f'/projects/{x["project"].slug}/corrections/autonomy/publications'
    browser=client(x['owner']);response=browser.get(url,headers={'Accept-Language':lang})
    assert response.status_code==200 and heading in response.text
    assert 'name="approved"' in response.text and 'name="consent"' in response.text
    assert '&lt;title&gt;' in response.text and 'simulated-private' not in response.text
    assert browser.post(url+'/authorize',data={'run_id':run.id}).status_code==403
    with x['db'].session() as db:assert db.scalar(select(func.count()).select_from(CorrectionPublication))==0


def test_runtime_policy_denies_ai_foreign_repo_and_changed_write_body(publication):
    x=publication
    with correction_guard.correction_lock(x['db'],x['owner'].id), e.bounded(p.Policy(x['pub'].id,x['run'].id)):
        with pytest.raises(e.ExecutionStopped,match='ai_not_authorized'):
            e.provider_request(system='x',user_msg='x',max_tokens=10)
        with pytest.raises(e.ExecutionStopped,match='target_changed'):
            m._github_api_get('/repos/foreign/site/contents/page.html',token='fixture')
        with pytest.raises(e.ExecutionStopped,match='write_not_authorized'):
            m._github_api_put('/repos/fixture/site/pulls/91/merge',token='fixture',json_body={'sha':HEAD})
    assert not x['remote']['transports']


@pytest.mark.parametrize('change',['preview','config','owner','application','expired','version','resolved'])
def test_scope_changes_after_grant_stop_before_transport(publication,change):
    x=publication
    with x['db'].session() as db:
        pub=db.get(CorrectionPublication,x['pub'].id)
        if change=='preview':pub.preview_key='f'*64
        elif change=='config':db.get(Project,x['project'].id).settings={}
        elif change=='owner':db.get(Project,x['project'].id).owner_user_id=x['admin'].id
        elif change=='application':db.get(IssueTask,x['task'].id).url='https://atelier.example/other/'
        elif change=='expired':pub.expires_at=NOW-timedelta(seconds=1)
        elif change=='version':pub.version=99
        db.commit()
    if change=='resolved':add(x['db'],x['task'],at=NOW,result='resolved')
    assert step(x) in {'review_required','expired'}
    assert not x['remote']['transports']


def test_page_owner_rights_and_equal_slug_isolation(publication):
    from backend.models import AccountMember
    x=publication;url=f'/projects/{x["project"].slug}/corrections/autonomy/publications'
    member=user(x['db'])
    with x['db'].session() as db:
        db.add(AccountMember(owner_user_id=x['owner'].id,member_user_id=member.id,role='member'))
        db.commit()
    browser=client(member)
    assert browser.get(url).status_code==200
    response=browser.post(url+'/'+x['pub'].id+'/revoke',data={'_csrf':browser.cookies.get(m._CSRF_COOKIE_NAME)},follow_redirects=False)
    assert response.status_code==403
    stranger=client(user(x['db']))
    assert stranger.get(url).status_code in {403,404}
    assert not stored(x).revoked_at


def test_budget_is_reserved_before_transport_and_explicit_repeat_is_idempotent(publication):
    x=publication
    with x['db'].session() as db:
        pub=db.get(CorrectionPublication,x['pub'].id);pub.reads=p.MAX_READS;db.commit()
    assert step(x)=='review_required' and stored(x).reason=='request_budget'
    assert not x['remote']['transports']


def test_crawl_enqueue_rolls_back_declaration_quota_and_job_together(publication,monkeypatch):
    x=publication;merge(x);x['remote']['deployed']=True;assert step(x)=='deployment_confirmed'
    before=None
    with x['db'].session() as db:before=db.get(IssueTask,x['task'].id).note
    original=c.billing.usage_add
    def fail(*args,**kwargs):
        original(*args,**kwargs)
        raise RuntimeError('simulated ledger rollback')
    monkeypatch.setattr(c.billing,'usage_add',fail)
    with pytest.raises(RuntimeError):c.enqueue(x['pub'].id)
    with x['db'].session() as db:
        assert db.get(IssueTask,x['task'].id).note==before
        assert db.scalar(select(func.count()).select_from(JobRecord))==0
        assert not list(db.scalars(select(UsageEvent).where(UsageEvent.metric=='pages_crawled_month')))
        assert not db.get(CorrectionPublication,x['pub'].id).job_issued


def test_actual_crawl_worker_uses_authoritative_target_preserves_receipts_and_real_verification(publication,monkeypatch):
    import json
    from datetime import datetime, timezone
    from pathlib import Path
    from test_correction_memory import report, BASE as BASE_TIME, URL
    x=publication;jid=deploy(x,monkeypatch)
    root=m._runs_dir_for_user(x['owner'].id)
    baseline=root/x['project'].slug/stamp(BASE_TIME)/'audit'
    baseline.mkdir(parents=True,exist_ok=True)
    (baseline/'report.json').write_text(json.dumps(report(present=True)))
    (baseline.parent/'run.json').write_text(json.dumps({'base_url':'https://foreign.example/'}))
    # Allow the worker's second-resolution timestamp to be strictly after the declaration.
    from backend import publication_control
    now=datetime.now(timezone.utc)
    with x['db'].session() as db:
        pub=db.get(CorrectionPublication,x['pub'].id)
        task=db.get(IssueTask,x['task'].id)
        note=first_correction.note_data(task.note);note['first_correction']['declared_at']=(now-timedelta(seconds=2)).isoformat()
        task.note=json.dumps(note);pub.applied_key=memory.attempt_key(task)
        pub.receipt={**pub.receipt,'deployed_at':(now-timedelta(seconds=3)).isoformat()}
        job=db.get(JobRecord,jid);job.attempts=1;job.status='running';db.commit()
    calls=[]
    def subprocess(job,cmd,**kwargs):
        calls.append(cmd)
        assert 'https://atelier.example/' in cmd and 'https://foreign.example/' not in cmd
        assert cmd[cmd.index('--max-pages')+1]=='100'
        directory=Path(job.result['report_json']).parent;directory.mkdir(parents=True,exist_ok=True)
        output=report(present=False);output['meta']['pages_crawled']=1
        output['pages'][0]['url']=URL
        (directory/'report.json').write_text(json.dumps(output))
        return 0
    monkeypatch.setattr(m,'_run_subprocess_streaming',subprocess)
    monkeypatch.setattr(m,'_ramener_les_crawls_precedents',lambda *args:None)
    monkeypatch.setattr(m,'_envoyer_le_crawl',lambda *args:None)
    m._run_crawl_job(jid,x['owner'].id,x['project'].slug,publication_control.options(x['project'])[0])
    assert len(calls)==1
    job=m._load_job(jid)
    assert job.status=='done'
    assert job.result['publication_control']==x['pub'].id and job.result['publication_settled']
    assert step(x)=='verified'
    assert stored(x).observation_id
    assert not c.guard_start(job)
    assert len(calls)==1


def test_cron_requires_bearer_secret_without_session_or_csrf(publication,monkeypatch):
    x=publication;monkeypatch.setenv('CRON_SECRET','simulated-publication-cron')
    browser=client()
    assert browser.post('/cron/correction-publications').status_code==401
    monkeypatch.setenv('SEO_AGENT_DISABLE_SCHEDULERS','false')
    response=browser.post('/cron/correction-publications',headers={'Authorization':'Bearer simulated-publication-cron'})
    assert response.status_code==200 and response.json()['result']['examined']==1
    assert stored(x).phase=='waiting_checks'


@pytest.mark.parametrize('mode,expected',[('partial','review_required'),('wrong_sha','review_required'),('neutral','waiting_checks'),('no_required','waiting_checks')])
def test_incomplete_or_non_successful_required_checks_cannot_merge(publication,monkeypatch,mode,expected):
    x=publication;assert step(x)=='waiting_checks'
    original=m.requests.get
    def changed(url,**kwargs):
        response=original(url,**kwargs)
        if url.endswith('/check-runs'):
            data=response.json()
            if mode=='partial':data['total_count']=2
            if mode=='wrong_sha':data['check_runs'][0]['head_sha']='f'*40
            if mode=='neutral':data['check_runs'][0]['conclusion']='neutral'
            if mode=='no_required':data['check_runs'][0]['name']='Other'
            return SimpleNamespace(status_code=200,json=lambda:data)
        return response
    monkeypatch.setattr(m.requests,'get',changed)
    assert step(x)==expected
    assert not any(r[1].endswith('/merge') for r in x['remote']['transports'])


def test_page_reservation_lock_serializes_parallel_existing_and_publication_reservations(prepared,monkeypatch):
    import threading
    from backend import billing
    x=prepared;ready=threading.Barrier(2);out=[];errors=[]
    def remaining(db,**kwargs):
        used=sum(db.scalars(select(UsageEvent.amount).where(UsageEvent.user_id==x['owner'].id,UsageEvent.metric=='pages_crawled_month')))
        return 1-used
    monkeypatch.setattr(billing,'remaining_quota',remaining)
    def reserve():
        try:
            with x['db'].session() as db:
                db.get(Project,x['project'].id)
                ready.wait(5)
                ok,_=billing.ensure_within_quota(db,user_id=x['owner'].id,metric='pages_crawled_month',planned_amount=1)
                if ok:billing.usage_add(db,user_id=x['owner'].id,metric='pages_crawled_month',amount=1,commit=False)
                db.commit();out.append(ok)
        except Exception as exc:errors.append(type(exc).__name__)
    threads=[threading.Thread(target=reserve) for _ in range(2)]
    for thread in threads:thread.start()
    for thread in threads:thread.join(10)
    assert not errors and sorted(out)==[False,True]


def test_old_worker_snapshot_cannot_erase_stop_or_settlement(publication,monkeypatch):
    x=publication;jid=deploy(x,monkeypatch)
    with x['db'].session() as db:
        row=db.get(JobRecord,jid);row.status='running';row.attempts=1;db.commit()
    job=m._load_job(jid)
    p.revoke(x['pub'].id,x['owner'].id)
    m._save_job(job)
    assert m._load_job(jid).status=='cancel_requested'
    c.settle(job,actual=1)
    stale=m._load_job(jid);stale.result={**stale.result,'publication_settled':False,'quota_reserved_pages':100}
    m._save_job(stale)
    with x['db'].session() as db:
        row=db.get(JobRecord,jid)
        assert row.result['publication_settled'] and row.result['quota_reserved_pages']==0
    c.settle(stale,actual=1)
    with x['db'].session() as db:
        assert sum(db.scalars(select(UsageEvent.amount).where(UsageEvent.metric=='pages_crawled_month')))==1


def test_newer_pending_deployment_cannot_fall_back_to_old_success(publication,monkeypatch):
    x=publication;merge(x);x['remote']['deployed']=True;original=m.requests.get
    def get(url,**kwargs):
        if url.endswith('/deployments'):
            rows=[dict(id=value,sha=MERGE,environment='production',production_environment=True,transient_environment=False) for value in (8,10)]
            return SimpleNamespace(status_code=200,json=lambda:rows)
        if url.endswith('/deployments/10/statuses'):
            return SimpleNamespace(status_code=200,json=lambda:[{'id':11,'state':'in_progress'}])
        return original(url,**kwargs)
    monkeypatch.setattr(m.requests,'get',get)
    assert step(x)=='waiting_deployment' and not stored(x).job_issued
    assert not any(r[1].endswith('/deployments/8/statuses') for r in x['remote']['transports'])


def test_history_link_keeps_original_repository_after_project_settings_change(publication):
    x=publication;assert step(x)=='waiting_checks'
    with x['db'].session() as db:
        project=db.get(Project,x['project'].id)
        project.settings={**project.settings,'github_repo':'fixture/other'}
        db.commit()
    response=client(x['owner']).get(f'/projects/{x["project"].slug}/corrections/autonomy/publications')
    assert response.status_code==200
    assert 'https://github.com/fixture/site/pull/91' in response.text
    assert 'https://github.com/fixture/other/pull/91' not in response.text
