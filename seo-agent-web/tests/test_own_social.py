"""Owner isolation, native formats and source freshness; no real service requests."""
from datetime import datetime, timedelta, timezone

import pytest
from bs4 import BeautifulSoup
from sqlalchemy import func, select

from backend import app as m, own_social as s, own_seo, seo_watch
from backend.models import OwnSeoBrief, OwnSeoDraft, OwnSeoWorkspace, OwnBlogPublication, UsageEvent
from test_customer_activation import lab as lab, client, user
from test_seo_watch import refresh, approve

NOW = datetime.now(timezone.utc).replace(microsecond=0)


@pytest.fixture()
def studio(lab, monkeypatch):
    monkeypatch.setattr(own_seo, 'clock', lambda: NOW)
    def forbidden(*args, **kwargs):
        raise AssertionError('No AI, inbox, social or customer service call')
    for method in ('get', 'post', 'put', 'request'):
        monkeypatch.setattr(m.requests, method, forbidden)
    monkeypatch.setattr(own_seo, 'view', forbidden)
    return {**lab, 'owner': lab['admin']}


def plan(x, **kwargs):
    with x['db'].session() as db:
        return s.plan(db, **kwargs)


def current(x):
    for key in ('google_essentials', 'google_generative_search', 'bing_ai_performance'):
        refresh(x['db'], at=NOW, key=key)
        source = seo_watch.REGISTRY[key]
        approve(x, at=NOW, key=key, fr=source.fr, en=source.en)


def test_owner_only_even_for_exports_and_other_admin(studio):
    x = studio
    assert client().get(s.PATH, follow_redirects=False).status_code == 303
    for who in (user(x['db']), user(x['db'], admin=True)):
        assert client(who).get(s.PATH).status_code == 403
        assert client(who).get(s.PATH + '/score/download?version=abc').status_code == 403
    assert client(x['owner']).get(s.PATH).status_code == 200


def test_preparation_does_not_bind_create_approve_or_spend(studio):
    x = studio
    for path in (s.PATH, s.PATH + '?network=tiktok&goal=product&topic=first'):
        response = client(x['owner']).get(path)
        assert response.status_code == 200
        assert response.headers['cache-control'] == 'no-store'
        assert response.headers['referrer-policy'] == 'no-referrer'
        assert not BeautifulSoup(response.text, 'html.parser').select('.social-studio form[method="post"]')
    with x['db'].session() as db:
        for model in (OwnSeoWorkspace, OwnSeoBrief, OwnSeoDraft, OwnBlogPublication, UsageEvent):
            assert db.scalar(select(func.count()).select_from(model)) == 0


def test_goal_changes_shortlist_without_a_blog_dependency(studio, monkeypatch):
    # Only the explicitly relevant optional article is consulted, never the latest article.
    seen = []
    monkeypatch.setattr(s.content_library, 'get_article', lambda slug, lang: seen.append((slug, lang)))
    discovery = plan(studio)
    product = plan(studio, goal='product')
    assert discovery['ideas'][0]['key'] == 'score'
    assert product['ideas'][0]['key'] == 'first'
    assert all(i['blog'] is None for i in product['ideas'])
    assert seen == [('seo-geo-resultats-mesurer-petite-entreprise', 'fr')]
    assert {i['key'] for i in discovery['deferred']} == {'geo', 'essentials'}


@pytest.mark.parametrize('lang', ['fr', 'en'])
def test_native_carousel_and_video_have_distinct_usable_sequences(studio, lang):
    a = plan(studio, lang=lang)['ideas'][0]
    b = plan(studio, lang=lang, network='tiktok')['ideas'][0]
    assert len(a['sequence']) == 4 and all(len(p) == 2 for p in a['sequence'])
    assert len(b['sequence']) == 3 and all(len(p) == 3 for p in b['sequence'])
    assert a['hook'] != b['hook'] and a['production'] != b['production']
    assert a['sources'] and a['caption'] and b['cta']
    assert a['version'] != b['version']


@pytest.mark.parametrize('state', ['changed', 'paused', 'stale', 'future', 'error', 'withdrawn', 'missing-revision'],
                         ids=['changed', 'paused', 'old', 'future', 'error', 'withdrawn', 'revision'])
def test_unusable_official_source_cannot_offer_or_export_geo(studio, state):
    x = studio
    current(x)
    before = next(i for i in plan(x)['ideas'] if i['key'] == 'geo')
    with x['db'].session() as db:
        row = db.get(seo_watch.SeoKnowledgeSource, 'bing_ai_performance')
        if state == 'changed': row.latest_hash = 'f' * 64
        if state == 'paused': row.paused = True
        if state == 'stale': row.checked_at = NOW - timedelta(days=8)
        if state == 'future': row.checked_at = NOW + timedelta(days=1)
        if state == 'error': row.error_code = 'unavailable'
        if state == 'withdrawn': row.approved_hash = None
        if state == 'missing-revision': db.delete(db.get(seo_watch.SeoKnowledgeRevision, (row.key, row.latest_hash)))
        db.commit()
    after = plan(x)
    assert 'geo' not in {i['key'] for i in after['ideas']}
    assert next(i for i in after['deferred'] if i['key'] == 'geo')['ready'] is False
    assert client(x['owner']).get(before['download']).status_code == 409
    page = client(x['owner']).get(s.PATH + '?topic=geo')
    assert page.status_code == 200 and 'Une référence manque' in page.text
    assert not BeautifulSoup(page.text, 'html.parser').select('#production-pack a[href*="/download"]')


def test_refresh_identical_source_keeps_pack_but_reviewed_change_invalidates_it(studio, monkeypatch):
    x = studio
    current(x)
    old = next(i for i in plan(x)['ideas'] if i['key'] == 'geo')
    for key in ('google_generative_search', 'bing_ai_performance'):
        refresh(x['db'], at=NOW + timedelta(days=1), key=key)
    monkeypatch.setattr(own_seo, 'clock', lambda: NOW + timedelta(days=1))
    assert next(i for i in plan(x)['ideas'] if i['key'] == 'geo')['version'] == old['version']
    with x['db'].session() as db:
        db.get(seo_watch.SeoKnowledgeSource, 'bing_ai_performance').guidance_fr += ' Repère relu modifié.'
        db.commit()
    assert client(x['owner']).get(old['download']).status_code == 409
    assert next(i for i in plan(x)['deferred'] if i['key'] == 'geo')['missing'] == 'guidance'


def test_changed_or_missing_product_doc_invalidates_pack(studio, monkeypatch):
    x = studio
    old = plan(x)['ideas'][0]
    actual = s.content_library.get_doc
    monkeypatch.setattr(s.content_library, 'get_doc', lambda slug, lang: {**actual(slug, lang), 'source_sha256': 'f' * 64})
    assert client(x['owner']).get(old['download']).status_code == 409
    monkeypatch.setattr(s.content_library, 'get_doc', lambda *args: None)
    assert not plan(x, goal='product')['ideas']
    assert client(x['owner']).get(old['download']).status_code == 409


def test_language_links_export_and_exact_optional_article(studio):
    x = studio
    current(x)
    response = client(x['owner']).get(s.PATH + '?network=tiktok&goal=product&audience=business&lang=en&topic=first')
    assert response.status_code == 200 and '<html lang="en">' in response.text
    soup = BeautifulSoup(response.text, 'html.parser')
    pack = soup.select_one('#production-pack')
    download = pack.select_one('a[href*="/download"]')['href']
    assert 'network=tiktok' in download and 'audience=business' in download and 'lang=en' in download
    result = client(x['owner']).get(download)
    assert result.status_code == 200 and 'Production sequence' in result.text
    assert 'first-tiktok-en.md' in result.headers['content-disposition']
    assert result.headers['x-content-type-options'] == 'nosniff'
    assert 'https://noyaru.com/en/docs/demarrer' in result.text
    geo = next(i for i in plan(x)['ideas'] if i['key'] == 'geo')
    assert geo['blog']['url'].endswith('/seo-geo-resultats-mesurer-petite-entreprise')
    assert next(i for i in plan(x, lang='en')['ideas'] if i['key'] == 'geo')['blog'] is None


@pytest.mark.parametrize('query', ['network=other', 'goal=other', 'audience=other', 'network=' + 'x' * 30], ids=['network', 'goal', 'audience', 'long'])
def test_untrusted_choices_are_rejected(studio, query):
    assert client(studio['owner']).get(s.PATH + '?' + query).status_code == 422


def test_unknown_or_wrong_goal_idea_and_stale_export(studio):
    c = client(studio['owner'])
    assert c.get(s.PATH + '?topic=unknown').status_code == 404
    assert c.get(s.PATH + '?goal=product&topic=geo').status_code == 404
    assert c.get(s.PATH + '/unknown/download').status_code == 404
    response = c.get(s.PATH + '/score/download?network=tiktok&audience=business&lang=en')
    assert response.status_code == 409
    recovery = BeautifulSoup(response.text, 'html.parser').select_one('[role="alert"] a')['href']
    assert 'network=tiktok' in recovery and 'audience=business' in recovery and 'lang=en' in recovery
    assert c.post(s.PATH + '/publish').status_code in {403, 404, 405}


def test_unavailable_store_is_reported_without_false_sources(studio, monkeypatch):
    from sqlalchemy.exc import OperationalError
    def failure(*args, **kwargs):
        raise OperationalError('unavailable', {}, None)
    monkeypatch.setattr(seo_watch, 'catalog', failure)
    c = client(studio['owner'])
    result = c.get(s.PATH)
    assert result.status_code == 503 and 'momentanément indisponibles' in result.text
    assert not BeautifulSoup(result.text, 'html.parser').select('.social-idea')
    assert c.get(s.PATH + '/score/download?version=old').status_code == 503
