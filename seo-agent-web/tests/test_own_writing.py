"""Real transport fences with simulated HTTP; never real AI, site or GitHub writes."""
from datetime import timedelta
import json
import threading
from types import SimpleNamespace
import uuid

import pytest
from bs4 import BeautifulSoup
from sqlalchemy import func,select
from backend import own_writing as w,own_seo as s,app as m
from backend.models import OwnSeoDraft,OwnSeoBrief,OwnSeoWorkspace,Project,UsageEvent
from test_own_seo import own as own,bind,view,change_report,NOW
from test_customer_activation import lab as lab,client,user,project
from test_seo_watch import refresh,approve


def output():
    p={'text':'Un audit technique aide à repérer les problèmes observés sur les pages analysées. Son score interne ne représente pas une note attribuée par Google.',
       'sources':['product_lire-le-rapport']}
    return {'title':'Comprendre les limites d’un audit SEO',
        'description':'Un audit SEO aide à comprendre les anomalies observées sur un site et les limites des conclusions que vous pouvez en tirer.',
        'slug':'comprendre-les-limites-audit-test','intro':p,
        'sections':[{'heading':'Comprendre les constats','paragraphs':[p]},
                    {'heading':'Vérifier sans promettre','paragraphs':[{'text':'Une nouvelle analyse peut observer si un problème technique subsiste. Cette observation ne prouve pas une amélioration de classement ou sa cause.',
                    'sources':['product_verifier-une-correction','google_essentials']}]}]}


@pytest.fixture()
def writing(own,monkeypatch):
    x=own;token=bind(x)
    for source in ('google_essentials','google_generative_search','bing_ai_performance'):
        refresh(x['db'],at=NOW,key=source);approve(x,at=NOW,key=source)
    item=next(c for c in view(x)['candidates'] if c['kind']=='research')
    bid=s.prepare(x['owner'].id,token,item['key'],'fr');s.review(x['owner'].id,bid,token,confirm=True,status='reviewed')
    monkeypatch.setenv('ANTHROPIC_API_KEY','simulated');monkeypatch.setenv('OPENAI_API_KEY','')
    monkeypatch.setattr(m,'_rate_limit_retry_after',lambda **kw:None)
    calls=[]
    def post(url,**kw):
        assert '/messages' in url and url.startswith('https://api.anthropic.com/')
        calls.append({'url':url,'body':kw['json']})
        return SimpleNamespace(status_code=200,text='',json=lambda:{'content':[{'type':'text','text':json.dumps(output())}]})
    monkeypatch.setattr(m.requests,'post',post)
    return {**x,'brief':bid,'token':token,'calls':calls}


def authorize(x,key=None):
    with x['db'].session() as db:_,fingerprint=w.bundle(db,x['owner'].id,x['brief'])
    return w.authorize(x['owner'].id,x['brief'],fingerprint,key or uuid.uuid4().hex,True)


def row(x,draft_id):
    with x['db'].session() as db:return db.get(OwnSeoDraft,draft_id)


def test_read_only_get_has_no_authorization_calls_or_commercial_credit_usage(writing):
    x=writing;response=client(x['owner']).get(w.PATH+'?brief='+x['brief'])
    assert response.status_code==200 and 'Dossier transmis' in response.text
    assert not x['calls']
    with x['db'].session() as db:
        assert db.scalar(select(func.count()).select_from(OwnSeoDraft))==0
        assert db.scalar(select(func.count()).select_from(UsageEvent))==0


def test_dossier_contains_approved_public_facts_not_search_metrics_or_accounts(writing):
    x=writing
    with x['db'].session() as db:value,_=w.bundle(db,x['owner'].id,x['brief'])
    text=json.dumps(value)
    for private in ('"impressions":','"clicks":','"position":','"crawl":',x['owner'].id,x['project'].id,'api_key','simulated'):
        assert private not in text
    assert {s['id'] for s in value['sources']}=={'google_essentials','google_generative_search','bing_ai_performance','product_lire-le-rapport','product_verifier-une-correction'}


def test_repeated_authorization_and_worker_run_make_one_metered_transport_call(writing):
    x=writing;key=uuid.uuid4().hex;draft=authorize(x,key)
    assert authorize(x,key)==draft
    w.run(draft);w.run(draft)
    assert len(x['calls'])==1
    result=row(x,draft);assert result.status=='draft' and result.provider_calls==1
    assert result.content_hash==s.digest(result.content)
    assert authorize(x,key)==draft and authorize(x)==draft
    page=BeautifulSoup(client(x['owner']).get(w.PATH+'?brief='+x['brief']).text,'html.parser')
    assert not page.select('form[action$="/authorize"]')
    assert page.select_one('a[href="#draft-'+draft+'"]')


@pytest.mark.parametrize('change',['paused','unreviewed','source_withdrawn','report_changed','owner_changed','target_changed','rebound','expired'])
def test_changes_before_execution_stop_without_any_provider_call(writing,change):
    x=writing;draft=authorize(x)
    with x['db'].session() as db:
        if change=='paused':db.get(OwnSeoWorkspace,s.KEY).paused=True
        if change=='unreviewed':db.get(OwnSeoBrief,x['brief']).status='discarded'
        if change=='owner_changed':db.get(Project,x['project'].id).owner_user_id=user(x['db']).id
        if change=='target_changed':db.get(Project,x['project'].id).base_url='https://customer.example/'
        if change=='rebound':db.get(OwnSeoWorkspace,s.KEY).token='f'*32
        if change=='expired':db.get(OwnSeoDraft,draft).authorized_until=NOW-timedelta(seconds=1)
        db.commit()
    if change=='source_withdrawn':
        from backend import seo_watch
        with x['db'].session() as db:seo_watch.withdraw(db,'google_essentials',actor=x['owner']);db.commit()
    if change=='report_changed':change_report(x,lambda data:data['meta']['gsc_api']['queries']['top'][0].update(impressions=999))
    w.run(draft)
    assert not x['calls'] and row(x,draft).status=='stopped'


def test_pause_or_cancellation_while_provider_is_running_never_retains_result(writing,monkeypatch):
    x=writing;draft=authorize(x);original=m.requests.post
    def post(*args,**kw):
        result=original(*args,**kw);w.cancel(x['owner'].id,draft);return result
    monkeypatch.setattr(m.requests,'post',post);w.run(draft)
    assert row(x,draft).status=='cancelled' and not row(x,draft).content


def test_provider_uncertainty_blocks_fallback_and_repeat_authorizations(writing,monkeypatch):
    x=writing;draft=authorize(x);monkeypatch.setenv('OPENAI_API_KEY','simulated-fallback')
    def post(*args,**kw):x['calls'].append('uncertain');raise TimeoutError('simulated')
    monkeypatch.setattr(m.requests,'post',post)
    w.run(draft);w.run(draft)
    assert len(x['calls'])==1 and row(x,draft).status=='uncertain'
    assert authorize(x)==draft


def test_openai_temperature_retry_cannot_bypass_the_one_request_limit(writing,monkeypatch):
    x=writing;monkeypatch.setenv('ANTHROPIC_API_KEY','');monkeypatch.setenv('OPENAI_API_KEY','simulated')
    monkeypatch.setattr(m,'_OPENAI_SANS_TEMPERATURE',set())
    def post(url,**kw):
        assert url.endswith('/chat/completions');x['calls'].append('openai')
        return SimpleNamespace(status_code=400,text='temperature unsupported')
    monkeypatch.setattr(m.requests,'post',post)
    draft=authorize(x);w.run(draft)
    assert len(x['calls'])==1 and row(x,draft).provider_calls==1 and row(x,draft).status=='uncertain'


@pytest.mark.parametrize('started',[False,True])
def test_expired_lease_only_retries_when_no_request_started(writing,started):
    x=writing;draft=authorize(x)
    with x['db'].session() as db:
        r=db.get(OwnSeoDraft,draft);r.status='running';r.lease_token='old';r.lease_until=NOW-timedelta(seconds=1)
        if started:r.provider_calls=1;r.provider_started_at=NOW-timedelta(minutes=11)
        db.commit()
    w.run(draft)
    assert row(x,draft).status==('uncertain' if started else 'draft')
    assert len(x['calls'])==(0 if started else 1)


def test_budget_applies_to_owner_and_new_authorization_keys(writing,monkeypatch):
    x=writing
    monkeypatch.setattr(w,'validate',lambda *a:(_ for _ in ()).throw(ValueError('invalid_output')))
    for _ in range(2):draft=authorize(x);w.run(draft);assert row(x,draft).status=='rejected'
    with pytest.raises(ValueError,match='budget'):authorize(x)
    assert len(x['calls'])==2


def test_cancel_before_start_releases_reservation_without_reusing_old_consent(writing):
    x=writing;key=uuid.uuid4().hex;draft=authorize(x,key);w.cancel(x['owner'].id,draft);w.run(draft)
    assert not x['calls']
    with x['db'].session() as db:assert w.reserved(db,x['owner'].id)==0
    assert authorize(x,key)==draft
    replacement=authorize(x);assert replacement!=draft
    w.run(replacement);assert len(x['calls'])==1


def test_rolling_budget_does_not_reset_when_owner_changes_the_brief(writing,monkeypatch):
    x=writing
    monkeypatch.setattr(w,'validate',lambda *a:(_ for _ in ()).throw(ValueError('invalid_output')))
    for _ in range(2):draft=authorize(x);w.run(draft)
    monkeypatch.setattr(s,'clock',lambda:NOW+timedelta(hours=24,seconds=1))
    assert authorize(x)!=draft


def test_changed_guidance_after_generation_prevents_review(writing):
    from backend.models import SeoKnowledgeSource
    x=writing;draft=authorize(x);w.run(draft);r=row(x,draft)
    with x['db'].session() as db:db.get(SeoKnowledgeSource,'google_essentials').guidance_fr+=' Consigne revue.';db.commit()
    with pytest.raises(ValueError,match='evidence_changed'):w.review(x['owner'].id,draft,r.content_hash,True)
    assert client(x['owner']).get(w.PATH+'/'+draft+'/download').status_code==200


def test_changed_prompt_and_second_request_are_blocked_by_transport_policy(writing,monkeypatch):
    x=writing;draft=authorize(x)
    def altered(**kw):
        kw['user_msg']='different payload'
        return m._anthropic_messages_text(**{k:v for k,v in kw.items() if k!='temperature'},model='simulated')
    monkeypatch.setattr(m,'_correction_ai_json',altered);w.run(draft)
    assert not x['calls'] and row(x,draft).status=='stopped' and row(x,draft).provider_calls==0


def test_late_provider_result_is_not_accepted_after_lease_expiry(writing,monkeypatch):
    x=writing;draft=authorize(x);original=m.requests.post
    def late(*a,**kw):
        result=original(*a,**kw);monkeypatch.setattr(s,'clock',lambda:NOW+timedelta(minutes=11));return result
    monkeypatch.setattr(m.requests,'post',late);w.run(draft)
    assert row(x,draft).status=='uncertain' and not row(x,draft).content and len(x['calls'])==1


def test_active_worker_cannot_be_started_twice(writing,monkeypatch):
    x=writing;draft=authorize(x);entered=threading.Event();release=threading.Event();original=m.requests.post;errors=[]
    def post(*a,**kw):entered.set();assert release.wait(8);return original(*a,**kw)
    monkeypatch.setattr(m.requests,'post',post)
    def run():
        try:w.run(draft)
        except Exception as exc:errors.append(type(exc).__name__)
    thread=threading.Thread(target=run);thread.start();assert entered.wait(8)
    w.run(draft);release.set();thread.join(10)
    assert not errors and len(x['calls'])==1 and row(x,draft).status=='draft'


@pytest.mark.parametrize('change',['unknown_source','html','foreign_link','missing_sources','extra_key','duplicate_slug','too_long','insufficient'])
def test_invalid_or_unsupported_output_is_rejected_before_storage(writing,change):
    x=writing
    with x['db'].session() as db:sources=w.bundle(db,x['owner'].id,x['brief'])[0]['sources']
    value=output()
    if change=='unknown_source':value['intro']['sources']=['invented']
    if change=='html':value['intro']['text']+='<script>alert(1)</script>'
    if change=='foreign_link':value['intro']['text']+=' https://evil.example/'
    if change=='missing_sources':value['intro']['sources']=[]
    if change=='extra_key':value['tools']=['publish']
    if change=='duplicate_slug':value['slug']='audit-seo-technique-checklist-priorites'
    if change=='too_long':value['intro']['text']='word '*901
    if change=='insufficient':value={'insufficient_sources':True}
    with pytest.raises(ValueError):w.validate(value,sources)


def test_reviewer_confirms_exact_current_version_and_export_keeps_evidence(writing):
    x=writing;draft=authorize(x);w.run(draft);r=row(x,draft)
    with pytest.raises(ValueError,match='confirm_required'):w.review(x['owner'].id,draft,r.content_hash,False)
    with pytest.raises(ValueError,match='changed'):w.review(x['owner'].id,draft,'f'*64,True)
    w.review(x['owner'].id,draft,r.content_hash,True)
    response=client(x['owner']).get(w.PATH+'/'+draft+'/download')
    assert response.status_code==200 and 'Photographie historique' in response.text and 'developers.google.com' in response.text
    assert row(x,draft).status=='reviewed'
    s.pause(x['owner'].id,x['token'],True)
    with pytest.raises(ValueError,match='evidence_changed'):w.review(x['owner'].id,draft,r.content_hash,True)
    assert client(x['owner']).get(w.PATH+'/'+draft+'/download').status_code==200


@pytest.mark.parametrize('lang',['fr','en'])
def test_native_forms_role_csrf_review_and_download(writing,lang):
    x=writing;browser=client(x['owner']);url=w.PATH+'?brief='+x['brief']
    response=browser.get(url,headers={'Accept-Language':lang});soup=BeautifulSoup(response.text,'html.parser')
    form=soup.find('form',action=w.PATH+'/authorize');data={e['name']:e.get('value','') for e in form.select('input[name]') if e['name']!='confirm'}
    response=browser.post(form['action'],data=data,follow_redirects=False);assert 'confirm_required' in response.headers['location']
    data['confirm']='true';response=browser.post(form['action'],data=data,follow_redirects=False);assert response.status_code==303
    assert len(x['calls'])==1
    with x['db'].session() as db:r=db.scalar(select(OwnSeoDraft));draft=r.id
    assert 'À relire' in browser.get(url).text
    assert browser.post(w.PATH+'/'+draft+'/review',data={'confirm':'true'}).status_code==403
    assert client(user(x['db'])).get(url).status_code==403
    assert client(user(x['db'])).get(w.PATH+'/'+draft+'/download').status_code==403


def test_cron_only_resumes_authorized_operations_and_requires_its_secret(writing,monkeypatch):
    x=writing;monkeypatch.setenv('CRON_SECRET','simulated-cron-secret')
    assert client().post('/cron/own-seo-writing').status_code==401
    assert client().post('/cron/own-seo-writing',headers={'Authorization':'Bearer simulated-cron-secret'}).json()=={'ok':True,'examined':0}
    draft=authorize(x)
    assert client().post('/cron/own-seo-writing',headers={'Authorization':'Bearer simulated-cron-secret'}).status_code==200
    assert row(x,draft).status=='draft' and len(x['calls'])==1


def test_rebinding_and_project_deletion_remove_access_and_children(writing):
    x=writing;draft=authorize(x);w.run(draft)
    other=project(x['db'],x['owner'])
    with x['db'].session() as db:db.get(Project,other.id).base_url=s.BASE;db.commit()
    s.bind(x['owner'].id,other.id,x['token'],confirm=True)
    assert client(x['owner']).get(w.PATH+'/'+draft+'/download').status_code==404
    with x['db'].session() as db:db.delete(db.get(Project,x['project'].id));db.commit()
    assert row(x,draft) is None
