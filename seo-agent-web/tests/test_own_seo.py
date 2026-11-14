"""Own-site evidence is bound explicitly; no client, provider or publication side effects."""
from datetime import datetime,timedelta,timezone
import json
import threading

import pytest
from bs4 import BeautifulSoup
from sqlalchemy import func,select
from backend import app as m,own_seo as s,seo_watch as w
from backend.models import OwnSeoBrief,OwnSeoWorkspace,Project,AuditLog,UsageEvent
from test_customer_activation import client,lab as lab,project,user
from test_seo_watch import refresh,approve

PATH='/settings/system/own-seo'
NOW=datetime.now(timezone.utc).replace(microsecond=0)
ARTICLE='audit-seo-technique-checklist-priorites'
PAGE='https://noyaru.com/ressources-seo/'+ARTICLE


def report():
    period={'enabled':True,'ok':True,'start_date':(NOW-timedelta(days=30)).date().isoformat(),'end_date':(NOW-timedelta(days=3)).date().isoformat(),
        'queries':{'rows':1,'top':[{'query':'comment lire un audit seo','impressions':200,'clicks':4,'position':12.3}]},
        'pages':{'rows':1,'top':[{'page':PAGE,'impressions':400,'clicks':7,'position':7.2}]}}
    return {'meta':{'base_url':s.BASE,'gsc_api':{**period,'property':'sc-domain:noyaru.com'},
        'bing':{**period,'site_url':s.BASE}},'pages':[{'url':PAGE,'status_code':200}],
        'issues':{'missing_title':{'count':1,'examples':[PAGE]}}}


@pytest.fixture()
def own(lab,monkeypatch):
    x=lab;owner=x['admin'];p=project(x['db'],owner)
    with x['db'].session() as db:
        p=db.get(Project,p.id);p.base_url=s.BASE;p.site_name='Noyaru';db.commit()
    monkeypatch.setattr(s,'clock',lambda:NOW)
    def forbidden(*args,**kwargs):raise AssertionError('No provider/site requests allowed')
    monkeypatch.setattr(m.requests,'get',forbidden);monkeypatch.setattr(m.requests,'post',forbidden);monkeypatch.setattr(m.requests,'put',forbidden)
    ts=(NOW-timedelta(days=1)).strftime('%Y%m%d-%H%M%S')
    folder=m._runs_dir_for_user(owner.id)/p.slug/ts/'audit';folder.mkdir(parents=True)
    (folder/'report.json').write_text(json.dumps(report()))
    return {**x,'owner':owner,'project':p,'file':folder/'report.json','ts':ts}


def bind(x):return s.bind(x['owner'].id,x['project'].id,'',confirm=True)


def view(x,lang='fr'):
    with x['db'].session() as db:return s.view(db,x['owner'].id,lang=lang)


def change_report(x,fn):
    data=json.loads(x['file'].read_text());fn(data);x['file'].write_text(json.dumps(data))


def test_get_does_not_bind_plan_spend_or_call_a_provider(own):
    x=own;response=client(x['owner']).get(PATH)
    assert response.status_code==200 and 'Aucun projet Noyaru lié' in response.text
    with x['db'].session() as db:
        assert not db.get(OwnSeoWorkspace,s.KEY)
        assert db.scalar(select(func.count()).select_from(OwnSeoBrief))==0
        assert db.scalar(select(func.count()).select_from(UsageEvent))==0


@pytest.mark.parametrize('url',[s.BASE,'https://noyaru.com','https://NOYARU.com:443/'])
def test_binding_uses_exact_owned_canonical_site(own,url):
    x=own
    with x['db'].session() as db:db.get(Project,x['project'].id).base_url=url;db.commit()
    token=bind(x)
    assert view(x)['project'].id==x['project'].id and len(token)==32


@pytest.mark.parametrize('url',['http://noyaru.com/','https://www.noyaru.com/','https://noyaru.com/client/','https://noyaru.com/?client=1',
    'https://noyaru.com.example/','https://customer.example/','https://user@noyaru.com/','https://noyaru.com:8443/'])
def test_foreign_or_ambiguous_targets_cannot_be_bound(own,url):
    x=own
    with x['db'].session() as db:db.get(Project,x['project'].id).base_url=url;db.commit()
    with pytest.raises(ValueError,match='target_required'):bind(x)


def test_team_project_and_other_admin_are_not_the_noyaru_owner(own):
    from backend.models import AccountMember
    x=own;other=user(x['db'],admin=True)
    with x['db'].session() as db:
        db.get(Project,x['project'].id).owner_user_id=other.id
        db.add(AccountMember(owner_user_id=other.id,member_user_id=x['owner'].id));db.commit()
    with pytest.raises(ValueError,match='target_required'):bind(x)
    assert client(other).get(PATH).status_code==403


def test_candidates_are_measured_page_or_research_not_claimed_content_gaps(own):
    x=own;bind(x);state=view(x)
    assert state['report_status']=='current'
    assert len(state['candidates'])==2 and {c['kind'] for c in state['candidates']}=={'page','research'}
    assert state['technical'][0]['key']=='missing_title'
    assert all(c['evidence']['property'] in {s.BASE,'sc-domain:noyaru.com'} for c in state['candidates'])
    assert all(c['evidence']['crawl']==x['ts'] for c in state['candidates'])


@pytest.mark.parametrize('change',['foreign_report','partial','foreign_bing','foreign_google','future_period','old_period','bad_metrics','private_page','missing_property','fractional','bad_position'])
def test_untrusted_or_non_public_evidence_is_excluded(own,change):
    x=own;bind(x)
    def edit(data):
        if change=='foreign_report':data['meta']['base_url']='https://customer.example/'
        elif change=='partial':data['meta']['stopped_on_time_budget']=True
        else:
            for source in ('gsc_api','bing'):
                node=data['meta'][source]
                if change=='foreign_bing' and source=='bing':node['site_url']='https://customer.example/'
                if change=='foreign_google' and source=='gsc_api':node['property']='sc-domain:customer.example'
                if change=='future_period':node['end_date']=(NOW+timedelta(days=2)).date().isoformat()
                if change=='old_period':node['end_date']=(NOW-timedelta(days=90)).date().isoformat()
                if change=='bad_metrics':
                    for dim in ('pages','queries'):node[dim]['top'][0]['impressions']=float('nan')
                if change in {'fractional','bad_position'}:
                    for dim in ('pages','queries'):node[dim]['top'][0].update({'impressions':20.5} if change=='fractional' else {'position':0.5})
                if change=='private_page':node['pages']['top'][0]['page']='https://noyaru.com/projects/private?token=secret'
                if change=='missing_property':node.pop('property' if source=='gsc_api' else 'site_url')
    change_report(x,edit);state=view(x)
    if change in {'foreign_report','partial','future_period','old_period','bad_metrics','missing_property','fractional','bad_position'}:assert not state['candidates']
    if change=='private_page':assert {c['kind'] for c in state['candidates']}=={'research'}
    if change=='foreign_bing':assert {c['evidence']['source'] for c in state['candidates']}=={'gsc_api'}
    if change=='foreign_google':assert {c['evidence']['source'] for c in state['candidates']}=={'bing'}


def test_only_current_approved_sources_create_an_editorial_idea(own):
    x=own;bind(x)
    assert not [row for row in view(x)['candidates'] if row['kind']=='guide']
    for key in ('google_generative_search','bing_ai_performance'):
        refresh(x['db'],at=NOW,key=key);approve(x,at=NOW,key=key)
    guide=next(row for row in view(x)['candidates'] if row['kind']=='guide')
    assert len(guide['evidence']['references'])==2
    with x['db'].session() as db:w.withdraw(db,'google_generative_search',actor=x['owner']);db.commit()
    assert not [row for row in view(x)['candidates'] if row['kind']=='guide']


def test_preparation_is_idempotent_and_does_not_consume_article_credits(own):
    x=own;token=bind(x);item=view(x)['candidates'][0]
    ids=[s.prepare(x['owner'].id,token,item['key'],'fr') for _ in range(2)]
    assert ids[0]==ids[1]
    with x['db'].session() as db:
        assert db.scalar(select(func.count()).select_from(OwnSeoBrief))==1
        assert db.scalar(select(func.count()).select_from(UsageEvent))==0
        audit=list(db.scalars(select(AuditLog).where(AuditLog.action=='own_seo.prepare')))
        assert len(audit)==1 and PAGE not in repr(audit[0].meta)


def test_changed_report_invalidates_pending_action_and_review(own):
    x=own;token=bind(x);item=view(x)['candidates'][0];bid=s.prepare(x['owner'].id,token,item['key'],'fr')
    change_report(x,lambda data:data['meta']['gsc_api']['pages']['top'][0].update(impressions=999))
    with pytest.raises(ValueError,match='evidence_changed'):s.prepare(x['owner'].id,token,item['key'],'fr')
    with pytest.raises(ValueError,match='evidence_changed'):s.review(x['owner'].id,bid,token,confirm=True,status='reviewed')


def test_rebinding_and_owner_target_changes_cannot_reuse_old_briefs(own):
    x=own;token=bind(x);bid=s.prepare(x['owner'].id,token,view(x)['candidates'][0]['key'],'fr')
    second=project(x['db'],x['owner'])
    with x['db'].session() as db:db.get(Project,second.id).base_url=s.BASE;db.commit()
    new=s.bind(x['owner'].id,second.id,token,confirm=True)
    assert new!=token
    with x['db'].session() as db:
        with pytest.raises(ValueError,match='brief_missing'):s.brief_for(db,x['owner'].id,bid)
    assert client(x['owner']).get(PATH+'/briefs/'+bid+'/download').status_code==404


def test_pause_blocks_preparation_and_review_but_not_read_or_discard(own):
    x=own;token=bind(x);item=view(x)['candidates'][0];bid=s.prepare(x['owner'].id,token,item['key'],'fr')
    s.pause(x['owner'].id,token,True)
    with pytest.raises(ValueError,match='paused'):s.prepare(x['owner'].id,token,item['key'],'fr')
    with pytest.raises(ValueError,match='paused'):s.review(x['owner'].id,bid,token,confirm=True,status='reviewed')
    s.review(x['owner'].id,bid,token,confirm=False,status='discarded')
    assert client(x['owner']).get(PATH).status_code==200


@pytest.mark.parametrize('lang,heading',[('fr','Référencement de Noyaru'),('en','Noyaru search visibility')])
def test_native_form_review_export_and_context_preserving_content_link(own,lang,heading):
    x=own;token=bind(x);item=next(row for row in view(x,lang)['candidates'] if row['kind']=='research')
    browser=client(x['owner']);page=browser.get(PATH,headers={'Accept-Language':lang})
    assert page.status_code==200 and heading in page.text
    csrf=browser.cookies.get(m._CSRF_COOKIE_NAME)
    response=browser.post(PATH+'/prepare',data={'token':token,'candidate_key':item['key'],'_csrf':csrf},headers={'Accept-Language':lang},follow_redirects=False)
    assert response.status_code==303
    assert response.headers['location'].startswith(PATH+'/writing?brief=')
    bid=response.headers['location'].split('brief=')[1]
    response=browser.post(PATH+'/briefs/'+bid+'/review',data={'token':token,'confirm':'true','_csrf':csrf},headers={'Accept-Language':lang},follow_redirects=False)
    assert response.status_code==303
    page=browser.get(PATH,headers={'Accept-Language':lang})
    assert PATH+'/writing?brief='+bid in page.text
    assert '/content?sujet=' not in page.text
    exported=browser.get(PATH+'/briefs/'+bid+'/download')
    assert exported.status_code==200 and 'attachment;' in exported.headers['content-disposition']
    assert ('not a publishable article' if lang=='en' else 'pas un article publiable') in exported.text


@pytest.mark.parametrize('lang',['fr','en'])
@pytest.mark.parametrize('kind',['research','page'])
@pytest.mark.parametrize('source',['gsc_api','bing'])
def test_observation_link_opens_saved_dates_and_metrics_after_new_report(own,lang,kind,source):
    x=own;token=bind(x)
    def older(data):
        for name in ('gsc_api','bing'):
            node=data['meta'][name]
            node['start_date']=(NOW-timedelta(days=36)).date().isoformat()
            node['end_date']=(NOW-timedelta(days=9)).date().isoformat()
            if name==source:
                for dim in ('queries','pages'):node[dim]['top'][0]['impressions']=800
    change_report(x,older)
    item=next(row for row in view(x,lang)['candidates'] if row['kind']==kind)
    assert item['evidence']['source']==source
    bid=s.prepare(x['owner'].id,token,item['key'],lang)
    change_report(x,lambda data:data['meta'].update(report()['meta']))
    browser=client(x['owner'])
    page=BeautifulSoup(browser.get(PATH,headers={'Accept-Language':lang}).text,'html.parser')
    link=page.select_one('a[href*="observation='+bid+'"]')
    assert link and '/performance' not in link['href']
    response=browser.get(link['href'],headers={'Accept-Language':lang})
    assert response.status_code==200
    saved=BeautifulSoup(response.text,'html.parser').find('details',id='brief-evidence-'+bid)
    assert saved.has_attr('open')
    evidence=json.loads(saved.get_text().split('```json\n')[1].split('\n```')[0])
    assert evidence==item['evidence']
    assert evidence['start']==(NOW-timedelta(days=36)).date().isoformat()
    assert evidence['end']==(NOW-timedelta(days=9)).date().isoformat()
    assert evidence['impressions']==800
    assert client(user(x['db'])).get(link['href']).status_code==403


def test_csrf_missing_consent_and_nonowner_access_are_denied(own):
    x=own;browser=client(x['owner']);browser.get(PATH)
    assert browser.post(PATH+'/bind',data={'project_id':x['project'].id}).status_code==403
    response=browser.post(PATH+'/bind',data={'project_id':x['project'].id,'_csrf':browser.cookies.get(m._CSRF_COOKIE_NAME)},follow_redirects=False)
    assert 'confirm_required' in response.headers['location']
    assert client(user(x['db'])).get(PATH).status_code==403


def test_concurrent_clicks_create_one_brief(own):
    x=own;token=bind(x);key=view(x)['candidates'][0]['key'];results=[];errors=[];barrier=threading.Barrier(2)
    def prepare():
        try:barrier.wait(5);results.append(s.prepare(x['owner'].id,token,key,'fr'))
        except Exception as exc:errors.append(type(exc).__name__)
    threads=[threading.Thread(target=prepare) for _ in range(2)]
    for thread in threads:thread.start()
    for thread in threads:thread.join(10)
    assert not errors and len(results)==2 and results[0]==results[1]


def test_exports_do_not_render_search_query_as_html_or_markdown_link(own):
    x=own;token=bind(x)
    change_report(x,lambda data:data['meta']['gsc_api']['queries']['top'][0].update(query='<img src="https://example.invalid/x"> [click](https://example.invalid/)'))
    item=next(row for row in view(x)['candidates'] if row['kind']=='research' and row['evidence']['source']=='gsc_api')
    bid=s.prepare(x['owner'].id,token,item['key'],'fr')
    response=client(x['owner']).get(PATH+'/briefs/'+bid+'/download')
    assert '<img ' not in response.text.split('```json')[0] and r'\[click\]' in response.text


def test_same_source_rechecked_without_changes_does_not_duplicate_a_guide(own,monkeypatch):
    x=own;bind(x)
    for key in ('google_generative_search','bing_ai_performance'):
        refresh(x['db'],at=NOW,key=key);approve(x,at=NOW,key=key)
    before=next(row for row in view(x)['candidates'] if row['kind']=='guide')['key']
    monkeypatch.setattr(s,'clock',lambda:NOW+timedelta(hours=1))
    for key in ('google_generative_search','bing_ai_performance'):refresh(x['db'],at=NOW+timedelta(hours=1),key=key)
    assert next(row for row in view(x)['candidates'] if row['kind']=='guide')['key']==before


def test_project_deletion_removes_briefs_and_invalidates_the_binding(own):
    x=own;token=bind(x);bid=s.prepare(x['owner'].id,token,view(x)['candidates'][0]['key'],'fr')
    with x['db'].session() as db:db.delete(db.get(Project,x['project'].id));db.commit()
    with x['db'].session() as db:
        assert db.get(OwnSeoBrief,bid) is None
        assert db.get(OwnSeoWorkspace,s.KEY).project_id is None
    assert view(x)['project'] is None
    assert client(x['owner']).get(PATH+'/briefs/'+bid+'/download').status_code==404
