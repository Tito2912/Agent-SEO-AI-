"""Public-source knowledge has provenance, freshness and fenced, bounded refreshes."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select

from backend import app as m, seo_watch as w
from backend.models import AuditLog, SeoKnowledgeRevision as Revision, SeoKnowledgeSource as State
from test_customer_activation import client, lab as lab, user

NOW = datetime.now(timezone.utc)
KEY = w.SOURCES[0].key
FR = 'Vérifier le contenu utile, les exigences techniques et les preuves avant toute recommandation.'
EN = 'Check helpful content, technical eligibility and evidence before making a recommendation.'


def reading(source=None, *, version='first', link='https://example.com/', tail=''):
    source = source or w.REGISTRY[KEY]
    selector = source.selector.removeprefix('.')
    body = ('<div class="'+selector+'"><nav>Ignored navigation</nav><script>ignore-all-guards</script>'
            '<p>'+('Helpful public documentation. '*35)+version+'</p><a href="'+link+'">Reference</a>'+tail+'</div>')
    return w.parse_document(source, body.encode())


def refresh(database, *, at=NOW, version='first', key=KEY, fetcher=None):
    return w.refresh_source(database, key, clock=lambda: at,
                            fetcher=fetcher or (lambda source: reading(source, version=version)))


def state(database, key=KEY):
    with database.session() as db:return db.get(State, key)


def catalog(database, *, at=NOW, lang='fr'):
    with database.session() as db:return w.catalog(db, now=at, lang=lang)


def approve(lab, *, at=NOW, key=KEY, fingerprint=None, fr=FR, en=EN):
    with lab['db'].session() as db:
        w.review(db, key, fingerprint or db.get(State,key).latest_hash,
                 fr=fr, en=en, actor=lab['admin'], now=at)
        db.commit()


def test_read_does_not_initialize_sources_or_claim_freshness(lab):
    result = catalog(lab['db'])
    assert len(result['sources']) == 3 and all(v['status'] == 'not_checked' for v in result['sources'])
    with lab['db'].session() as db:assert db.scalar(select(func.count()).select_from(State)) == 0
    assert all(v['guidance'] is None for v in result['sources'])


def test_only_exact_reviewed_release_fingerprint_is_seeded(lab, monkeypatch):
    r = reading();source = replace(w.REGISTRY[KEY],seed_hash=r['fingerprint'])
    monkeypatch.setitem(w.REGISTRY, KEY, source)
    assert refresh(lab['db']) == 'checked'
    item=catalog(lab['db'])['sources'][0]
    assert item['status']=='current' and item['guidance']==source.fr
    assert refresh(lab['db'],at=NOW+timedelta(days=1),version='changed')=='checked'
    assert catalog(lab['db'],at=NOW+timedelta(days=1))['sources'][0]['status']=='changed'
    assert catalog(lab['db'],at=NOW+timedelta(days=1))['sources'][0]['guidance'] is None
    with lab['db'].session() as db:
        seeds=list(db.scalars(select(AuditLog).where(AuditLog.action=='seo_watch.seed',AuditLog.target_id==KEY)))
        assert len(seeds)==1 and seeds[0].meta['fingerprint']==r['fingerprint']


def test_changed_source_requires_review_and_uses_localized_guidance(lab):
    refresh(lab['db']);assert catalog(lab['db'])['sources'][0]['status']=='changed'
    approve(lab)
    assert catalog(lab['db'])['sources'][0]['guidance']==FR
    assert catalog(lab['db'],lang='en')['sources'][0]['guidance']==EN
    with lab['db'].session() as db:
        event=db.scalar(select(AuditLog).where(AuditLog.action=='seo_watch.review'))
        assert event.actor_user_id==lab['admin'].id and event.meta['guidance_fr']==FR
    refresh(lab['db'],at=NOW+timedelta(days=1),version='new')
    result=catalog(lab['db'],at=NOW+timedelta(days=1))['sources'][0]
    assert result['status']=='changed' and result['guidance'] is None
    with lab['db'].session() as db:assert db.scalar(select(func.count()).select_from(Revision))==2


def test_repeated_calls_are_bounded_and_unchanged_articles_do_not_duplicate_versions(lab):
    calls=[]
    def fetch(source):calls.append(source.key);return reading(source)
    assert refresh(lab['db'],fetcher=fetch)=='checked'
    assert refresh(lab['db'],at=NOW+timedelta(hours=23),fetcher=fetch)=='not_due'
    assert refresh(lab['db'],at=NOW+timedelta(days=1),fetcher=fetch)=='checked'
    assert len(calls)==2
    with lab['db'].session() as db:assert db.scalar(select(func.count()).select_from(Revision))==1


def test_failure_preserves_old_evidence_without_claiming_a_current_rule(lab):
    refresh(lab['db']);approve(lab)
    def fail(source):raise RuntimeError('credential=not-for-the-browser')
    assert refresh(lab['db'],at=NOW+timedelta(days=1),fetcher=fail)=='unavailable'
    result=catalog(lab['db'],at=NOW+timedelta(days=1))
    assert result['sources'][0]['status']=='unavailable' and result['sources'][0]['guidance'] is None
    assert state(lab['db']).checked_at.replace(tzinfo=timezone.utc)==NOW
    assert 'not-for-the-browser' not in str(result) and state(lab['db']).error_code=='unavailable'
    assert refresh(lab['db'],at=NOW+timedelta(days=1,hours=1))=='checked'
    assert catalog(lab['db'],at=NOW+timedelta(days=1,hours=1))['sources'][0]['status']=='current'


def test_stale_and_future_readings_are_not_current(lab):
    refresh(lab['db']);approve(lab)
    assert catalog(lab['db'],at=NOW+timedelta(days=8))['sources'][0]['status']=='stale'
    assert catalog(lab['db'],at=NOW-timedelta(seconds=1))['sources'][0]['status']=='stale'


def test_source_url_change_invalidates_old_provenance(lab, monkeypatch):
    refresh(lab['db']);approve(lab)
    monkeypatch.setattr(w,'SOURCES',tuple(replace(s,url=s.url+'/new') if s.key==KEY else s for s in w.SOURCES))
    assert catalog(lab['db'])['sources'][0]['status']=='changed'
    assert catalog(lab['db'])['sources'][0]['guidance'] is None


def test_review_refuses_an_obsolete_or_unread_source(lab):
    refresh(lab['db']);old=state(lab['db']).latest_hash
    refresh(lab['db'],at=NOW+timedelta(days=1),version='new')
    with pytest.raises(ValueError,match='review_changed'):approve(lab,at=NOW+timedelta(days=1),fingerprint=old)
    with pytest.raises(ValueError,match='review_changed'):approve(lab,at=NOW+timedelta(days=9))
    assert state(lab['db']).approved_hash!=state(lab['db']).latest_hash


def test_withdrawal_during_refresh_cannot_be_automatically_reinstated(lab):
    refresh(lab['db']);approve(lab)
    def fetch(source):
        with lab['db'].session() as db:w.withdraw(db,KEY,actor=lab['admin']);db.commit()
        return reading(source)
    refresh(lab['db'],at=NOW+timedelta(days=1),fetcher=fetch)
    assert state(lab['db']).approved_hash is None
    assert catalog(lab['db'],at=NOW+timedelta(days=1))['sources'][0]['status']=='review_required'


def test_pause_fences_a_running_fetch_and_resume_respects_the_schedule(lab):
    def fetch(source):
        with lab['db'].session() as db:w.pause(db,KEY,paused=True,actor=lab['admin']);db.commit()
        return reading(source)
    assert refresh(lab['db'],fetcher=fetch)=='superseded'
    assert catalog(lab['db'])['sources'][0]['status']=='paused'
    assert refresh(lab['db'],at=NOW+timedelta(days=1))=='not_due'
    with lab['db'].session() as db:
        assert db.scalar(select(func.count()).select_from(Revision))==0
        w.pause(db,KEY,paused=False,actor=lab['admin']);db.commit()
    assert refresh(lab['db'],at=NOW+timedelta(days=1))=='checked'


def test_late_worker_cannot_overwrite_a_newer_reading(lab):
    clock=[NOW]
    def fetch(source):
        clock[0]+=timedelta(hours=2)
        assert w.refresh_source(lab['db'],KEY,clock=lambda:clock[0],fetcher=lambda s:reading(s,version='newer'))=='checked'
        return reading(source,version='older')
    assert w.refresh_source(lab['db'],KEY,clock=lambda:clock[0],fetcher=fetch)=='superseded'
    assert state(lab['db']).latest_hash==reading(version='newer')['fingerprint']
    with lab['db'].session() as db:assert db.scalar(select(func.count()).select_from(Revision))==1


def test_expired_worker_does_not_publish_or_claim_a_current_source(lab):
    clock=[NOW]
    def fetch(source):clock[0]+=timedelta(minutes=6);return reading(source)
    assert w.refresh_source(lab['db'],KEY,clock=lambda:clock[0],fetcher=fetch)=='superseded'
    assert catalog(lab['db'],at=clock[0])['sources'][0]['status']=='unavailable'


def test_revision_retention_keeps_ten_recent_and_the_approved_version(lab):
    refresh(lab['db']);approve(lab);approved=state(lab['db']).approved_hash
    for i in range(1,15):refresh(lab['db'],at=NOW+timedelta(days=i),version=str(i))
    with lab['db'].session() as db:
        rows=list(db.scalars(select(Revision)))
        assert len(rows)==11 and approved in [r.fingerprint for r in rows]


def test_parser_ignores_navigation_but_detects_changes_at_the_end_and_in_links():
    original=reading()
    assert original['fingerprint']!=reading(tail='<p>A newly added important restriction.</p>')['fingerprint']
    assert original['fingerprint']!=reading(link='https://example.com/new-policy')['fingerprint']
    source=w.SOURCES[0];body=('<div class="devsite-article-body"><nav>one</nav><p>'+('Readable document '*50)+'</p></div>').encode()
    assert w.parse_document(source,body)['fingerprint']==w.parse_document(source,body.replace(b'one',b'two'))['fingerprint']
    assert 'ignore-all-guards' not in original['excerpt'] and len(original['excerpt'])<=700


@pytest.mark.parametrize('body',[b'<html>Enable JavaScript</html>',b'<div class="devsite-article-body">Too short</div>',b'X'*(w.MAX_BYTES+1)])
def test_empty_shells_and_oversized_documents_are_not_successful_reads(body):
    with pytest.raises(w.WatchError):w.parse_document(w.SOURCES[0],body)


@pytest.mark.parametrize('ip',['127.0.0.1','10.0.0.1','169.254.169.254','::1','fc00::1','::ffff:127.0.0.1','224.0.0.1'])
def test_transport_rejects_private_or_multicast_addresses_before_connecting(monkeypatch,ip):
    monkeypatch.setattr(w.socket,'getaddrinfo',lambda *a,**kw:[(0,0,0,'',(ip,443))])
    monkeypatch.setattr(w,'_HTTPS',lambda *a,**kw:pytest.fail('No connection permitted'))
    with pytest.raises(w.WatchError,match='private_address'):w.fetch_document(w.SOURCES[0])


@pytest.mark.parametrize('status,kind,encoding,code',[
    (302,'text/html','identity','redirect'),(500,'text/html','identity','http_error'),
    (200,'application/json','identity','invalid_document'),(200,'text/html','gzip','invalid_encoding')])
def test_transport_refuses_redirects_errors_and_unexpected_documents(monkeypatch,status,kind,encoding,code):
    closed=[];requests=[]
    class Response:
        def getheader(self,key,default=''):return {'Content-Type':kind,'Content-Encoding':encoding}.get(key,default)
    Response.status=status
    class Connection:
        def __init__(self,host,ip):assert host=='developers.google.com' and ip=='93.184.216.34'
        def request(self,*args,**kwargs):requests.append((args,kwargs))
        def getresponse(self):return Response()
        def close(self):closed.append(True)
    monkeypatch.setattr(w.socket,'getaddrinfo',lambda *a,**kw:[(0,0,0,'',('93.184.216.34',443))])
    monkeypatch.setattr(w,'_HTTPS',Connection)
    with pytest.raises(w.WatchError,match=code):w.fetch_document(w.SOURCES[0])
    assert len(requests)==1 and closed==[True]
    assert set(requests[0][1]['headers'])=={'User-Agent','Accept','Accept-Encoding'}


def test_caller_cannot_supply_an_unregistered_url_or_variant():
    with pytest.raises(w.WatchError,match='unknown_source'):
        w.fetch_document(replace(w.SOURCES[0],url='https://127.0.0.1/private'))


def post(c,path,**data):
    c.get('/');return c.post(path,data={'_csrf':c.cookies.get(m._CSRF_COOKIE_NAME),**data},follow_redirects=False)


def test_owner_screen_and_actions_reject_customers_other_admins_and_missing_csrf(lab):
    owner=client(lab['admin']);other=client(user(lab['db'],admin=True));customer=client(user(lab['db']))
    for c in (other,customer):
        assert c.get('/settings/system/seo-knowledge').status_code==403
        assert post(c,'/settings/system/seo-knowledge/refresh').status_code==403
    response=owner.get('/settings/system/seo-knowledge?lang=en')
    assert response.status_code==200 and 'SEO/GEO knowledge' in response.text
    assert response.headers['cache-control']=='no-store' and response.headers['referrer-policy']=='no-referrer'
    assert owner.post('/settings/system/seo-knowledge/refresh').status_code==403


def test_cron_checks_secret_and_preserves_failures_during_retry_cooldown(lab,monkeypatch):
    monkeypatch.setenv('CRON_SECRET','local-watch-fixture');c=client()
    calls=[]
    def fetch(source):calls.append(source.key);raise w.WatchError('http_error')
    monkeypatch.setattr(w,'fetch_document',fetch)
    assert c.post('/cron/seo-watch').status_code==401 and not calls
    headers={'Authorization':'Bearer local-watch-fixture'}
    assert c.post('/cron/seo-watch',headers=headers).status_code==503
    assert len(calls)==3
    assert c.post('/cron/seo-watch',headers=headers).status_code==503 and len(calls)==3


def test_review_api_requires_confirmation_current_revision_and_records_public_guidance(lab):
    refresh(lab['db']);c=client(lab['admin']);fingerprint=state(lab['db']).latest_hash
    path='/settings/system/seo-knowledge/'+KEY+'/review'
    assert 'err=' in post(c,path,fingerprint=fingerprint,guidance_fr=FR,guidance_en=EN).headers['location']
    assert 'err=' not in post(c,path,fingerprint=fingerprint,guidance_fr=FR,guidance_en=EN,confirm='1').headers['location']
    assert catalog(lab['db'])['sources'][0]['guidance']==FR
    assert post(c,'/settings/system/seo-knowledge/'+KEY+'/pause',paused='1').status_code==303
    assert catalog(lab['db'])['sources'][0]['status']=='paused'
    assert post(c,'/settings/system/seo-knowledge/'+KEY+'/withdraw').status_code==303
    assert state(lab['db']).approved_hash is None


def test_assistant_receives_only_current_reviewed_guidance_not_raw_source_or_client_context(lab,monkeypatch):
    refresh(lab['db']);approve(lab);systems=[]
    monkeypatch.setattr(m,'_assistant_effective_provider',lambda:'openai')
    monkeypatch.setattr(m,'_rate_limit_retry_after',lambda **kw:None)
    monkeypatch.setattr(m.billing,'ensure_within_quota',lambda *a,**kw:(True,100))
    def reply(messages,*,model):systems.append(messages[0]['content']);return 'Local test reply.'
    monkeypatch.setattr(m,'_assistant_openai_chat',reply)
    c=client(lab['admin']);c.get('/?lang=en')
    response=c.post('/api/assistant/chat',json={'message':'A question','context':{'knowledge':'forged-rule'}},
                    headers={'X-CSRF-Token':c.cookies.get(m._CSRF_COOKIE_NAME)})
    assert response.status_code==200 and EN in systems[0]
    assert 'Helpful public documentation' not in systems[0] and 'forged-rule' not in systems[0]
    assert w.REGISTRY[KEY].url in systems[0] and '"checked_at"' in systems[0]


@pytest.mark.parametrize('minutes', [-1, 1])
def test_review_is_refused_while_a_lease_exists_even_if_the_worker_expired(lab, minutes):
    refresh(lab['db'])
    with lab['db'].session() as db:
        row=db.get(State,KEY);row.lease_token='pending';row.lease_until=NOW+timedelta(minutes=minutes);db.commit()
    with pytest.raises(ValueError,match='review_changed'):approve(lab)


def test_repeated_resume_does_not_cancel_an_already_running_read(lab):
    def fetch(source):
        with lab['db'].session() as db:w.pause(db,KEY,paused=False,actor=lab['admin']);db.commit()
        return reading(source)
    assert refresh(lab['db'],fetcher=fetch)=='checked'
