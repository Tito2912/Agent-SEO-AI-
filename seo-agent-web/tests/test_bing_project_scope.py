"""Web, standalone crawl and saved reports enforce the same project boundary."""
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest
from backend import app as m,search_scope,audit_dashboard as dash


@pytest.fixture(scope='module')
def crawler():
    path=Path(__file__).resolve().parents[2]/'skills/public/seo-autopilot/scripts/seo_audit.py'
    spec=importlib.util.spec_from_file_location('noyaru_bing_scope_audit',path)
    module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module;spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('site,expected',[
    ('https://noyaru.com/',True),('https://NOYARU.com:443',True),('http://noyaru.com/',False),
    ('https://www.noyaru.com/',False),('https://noyaru.com/blog/',False),('https://other.example/',False),
    ('https://noyaru.com.evil.example/',False),('https://u@noyaru.com/',False),('https://noyaru.com/?x=1',False),
    ('https://noyaru.com/#x',False),('https://noyaru.com:8443/',False),('not a URL',False),('https://noyaru.\ncom/',False)])
def test_web_and_portable_crawler_share_exact_property_contract(crawler,site,expected):
    assert search_scope.same_property('https://noyaru.com/',site) is expected
    assert crawler._bing_same_property('https://noyaru.com/',site) is expected


@pytest.mark.parametrize('module_name',['web','crawler'])
def test_no_matching_registered_property_never_selects_another_site(crawler,monkeypatch,module_name):
    module=m if module_name=='web' else crawler
    monkeypatch.setattr(module,'_bing_call',lambda *args,**kwargs:{'d':[{'Url':'https://customer.example/'}]})
    chosen,_,reason=module._bing_pick_site_url(base_url='https://noyaru.com/',timeout_s=1,api_key='simulated')
    assert chosen is None and reason=='site_not_found'


@pytest.mark.parametrize('items',[False,True])
def test_foreign_configured_property_stops_before_auth_or_any_stats_call(monkeypatch,items):
    def denied(*args,**kwargs):raise AssertionError('No provider call allowed')
    monkeypatch.setattr(m,'_effective_bing_connection',denied)
    fn=m._fetch_bing_live_items if items else m._fetch_bing_live_series
    options={'dim':'query','limit':10} if items else {}
    result=fn(user_id='fixture',base_url='https://noyaru.com/',bing_cfg={'enabled':True,'site_url':'https://customer.example/'},days=28,**options)
    assert not result['ok'] and result['reason']=='site_mismatch'


@pytest.mark.parametrize('items',[False,True])
def test_live_no_match_does_not_request_foreign_performance(monkeypatch,items):
    monkeypatch.setattr(m,'_effective_bing_connection',lambda **kwargs:{'token':'simulated','mode':'api_key'})
    calls=[]
    def call(method,**kwargs):
        calls.append(method);assert method=='GetUserSites'
        return {'d':[{'Url':'https://customer.example/'}]}
    monkeypatch.setattr(m,'_bing_call',call)
    fn=m._fetch_bing_live_items if items else m._fetch_bing_live_series
    result=fn(user_id='fixture',base_url='https://noyaru.com/',bing_cfg={'enabled':True},days=28,**({'dim':'query','limit':10} if items else {}))
    assert not result['ok'] and result['reason']=='site_not_found' and calls==['GetUserSites']


def test_standalone_crawl_no_match_stops_before_performance(crawler,monkeypatch,tmp_path):
    calls=[]
    def call(method,**kwargs):
        calls.append(method);assert method=='GetUserSites'
        return {'d':[{'Url':'https://customer.example/'}]}
    monkeypatch.setattr(crawler,'_bing_call',call)
    cfg=SimpleNamespace(bing_enabled=True,base_url='https://noyaru.com/',bing_site_url='',bing_api_key='fixture',bing_access_token='',
        bing_output_dir=str(tmp_path),output_dir=str(tmp_path),bing_days=28,bing_timeout_s=1)
    assert crawler._run_bing_api(cfg)['reason']=='site_not_found' and calls==['GetUserSites']


def test_cache_is_quarantined_without_rewriting_saved_evidence(tmp_path):
    report={'meta':{'base_url':'https://noyaru.com/','bing':{'enabled':True,'ok':True,'site_url':'https://customer.example/',
        'daily':[{'clicks':999}],'queries_csv':'/private/customer.csv'}},'issues':{'missing_title':{'count':1},'bing_pages_quick_wins':{'count':8}}}
    file=tmp_path/'noyaru'/'20261104-100000'/'audit'/'report.json';file.parent.mkdir(parents=True);file.write_text(json.dumps(report))
    result=dash.load_report_json(tmp_path,'noyaru','20261104-100000')
    assert result['meta']['bing']=={'enabled':True,'ok':False,'reason':'site_mismatch'}
    assert 'bing_pages_quick_wins' not in result['issues'] and 'missing_title' in result['issues']
    assert json.loads(file.read_text())==report
