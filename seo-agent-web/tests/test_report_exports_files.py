"""Reports keep their date/filter scope; file return parameters cannot grant access."""
from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from bs4 import BeautifulSoup

from backend import app as m, billing
from backend.models import AccountMember, Project
from test_customer_activation import client, lab as lab, user

OLD = '20261001-100000'
NEW = '20261002-100000'


@pytest.fixture()
def reports(lab, tmp_path, monkeypatch):
    owner, member, stranger = [user(lab['db']) for _ in range(3)]
    slug = 'atelier-exports'
    with lab['db'].session() as db:
        db.add_all([
            Project(owner_user_id=owner.id, slug=slug, site_name='Atelier', base_url='https://atelier.example/', settings={}),
            Project(owner_user_id=owner.id, slug='autre-site', site_name='Autre site', base_url='https://other.example/', settings={}),
            AccountMember(owner_user_id=owner.id, member_user_id=member.id),
        ])
        db.commit()
    root = tmp_path / 'rapports & preuves #local'
    monkeypatch.setattr(m, '_runs_dir_for_user', lambda uid: root / uid)
    monkeypatch.setattr(m, 'DEFAULT_RUNS_DIR', root)
    plan = {'value': 'solo', 'payer': None}

    def effective_plan(db, *, user_id):
        plan['payer'] = user_id
        return plan['value']

    monkeypatch.setattr(billing, 'effective_plan_key', effective_plan)
    for ts, count in [(OLD, 2), (NEW, 1)]:
        audit = root / owner.id / slug / ts / 'audit'
        audit.mkdir(parents=True)
        pages = [{'url': f'https://atelier.example/p{i}?a=1&b=2', 'status_code': 200,
                  'content_type': 'text/html', 'indexable': True} for i in range(count)]
        payload = {'meta': {'base_url': 'https://atelier.example/', 'site_name': 'Atelier'},
                   'pages': pages, 'issues': {'missing_title': {'count': count, 'examples': [p['url'] for p in pages]}}}
        (audit / 'report.json').write_text(json.dumps(payload))
        (audit / 'report.md').write_text('# Local\n<script>alert(1)</script>\n' + 'longue-ligne-' * 100)
        (audit / 'fix-suggestions.json').write_text('{"source":"local & #"}')
    return {'root': root, 'owner': owner, 'member': member, 'stranger': stranger, 'slug': slug,
            'file': root / owner.id / slug / OLD / 'audit' / 'report.md', 'plan': plan}


@pytest.mark.parametrize('lang', ['fr', 'en'])
@pytest.mark.parametrize('actor', ['owner', 'member'])
def test_historical_exports_and_files_keep_report_and_filter_scope(reports, lang, actor):
    r = reports
    c = client(r[actor])
    query = {'crawl': OLD, 'compare': NEW, 'category': 'Content', 'q': 'manquante', 'lang': lang}
    for suffix in ['', '/issues', '/issues/missing_title']:
        response = c.get('/projects/' + r['slug'] + suffix, params=query)
        assert response.status_code == 200
        soup = BeautifulSoup(response.text, 'html.parser')
        assert soup.html['lang'] == lang
        exports = soup.select_one('.report-exports')
        assert exports
        for link in exports.select('a[href*="/export/"]'):
            target = urlsplit(link['href'])
            params = parse_qs(target.query)
            assert params['crawl'] == [OLD]
            if suffix == '/issues':
                assert params['category'] == ['Content'] and params['q'] == ['manquante']
            if target.path.endswith('issues-all-urls.csv'):
                assert 'compare' not in params
            download = c.get(link['href'], follow_redirects=False)
            assert download.status_code == 200, link['href']
            assert download.headers['cache-control'] == 'no-store'
            assert OLD in download.headers['content-disposition']
            if target.path.endswith('.pdf'):
                assert download.content.startswith(b'%PDF-')
        source = exports.select_one('a[href^="/file?"]')
        if source:
            assert parse_qs(urlsplit(source['href']).query)['path'][0].endswith(('report.json', 'report.md'))
            preview = c.get(source['href'])
            assert preview.status_code == 200
            preview_soup = BeautifulSoup(preview.text, 'html.parser')
            back = preview_soup.select_one('.file-preview-actions a')
            assert urlsplit(back['href']).path == '/projects/' + r['slug'] + suffix
            assert parse_qs(urlsplit(back['href']).query)['crawl'] == [OLD]
            if suffix == '':
                assert 'alert(1)' in preview.text
    assert r['plan']['payer'] == r['owner'].id
    rows = list(csv.DictReader(io.StringIO(c.get(f'/projects/{r["slug"]}/export/issues-all-urls.csv?crawl={OLD}').content.decode('utf-8-sig'))))
    assert len(rows) == 2 and {row['timestamp'] for row in rows} == {OLD}


def test_preview_link_encodes_special_paths_and_preserves_only_useful_source_context(reports):
    r = reports
    c = client(r['member'])
    query = {'crawl': OLD, 'compare': NEW, 'page': '3', 'per_page': '50', 'q': 'a & # <script>', 'discard': 'private'}
    response = c.get(f'/projects/{r["slug"]}/issues/missing_title', params=query)
    soup = BeautifulSoup(response.text, 'html.parser')
    link = soup.select_one('.report-exports a[href^="/file?"]')['href']
    params = parse_qs(urlsplit(link).query)
    assert '#' not in link
    assert '& preuves #local' in params['path'][0]
    next_query = parse_qs(urlsplit(params['next'][0]).query)
    assert next_query['page'] == ['3'] and next_query['q'] == [query['q']] and 'discard' not in next_query
    preview = c.get(link)
    assert preview.status_code == 200
    context = preview.context['file_context']
    assert parse_qs(urlsplit(context['back_href']).query) == next_query
    assert parse_qs(urlsplit(context['pdf_href']).query) == {'crawl': [OLD]}
    assert preview.headers['cache-control'] == 'no-store' and preview.headers['referrer-policy'] == 'no-referrer'


@pytest.mark.parametrize('target', ['https://other.example/', '//other.example/', '/file?path=anything',
                                  '/settings/system', '/projects/autre-site?crawl=20261002-100000',
                                  '/projects/not-accessible/issues'])
def test_unrelated_or_external_return_is_discarded_after_file_authorization(reports, target):
    r = reports
    response = client(r['member']).get('/file', params={'path': str(r['file']), 'next': target})
    assert response.status_code == 200
    assert response.context['file_context'] == {'back_href': '/', 'back_label': 'Retrouver mes sites'}
    assert BeautifulSoup(response.text, 'html.parser').select_one('pre').get_text() == r['file'].read_text()


def test_wrong_return_date_is_replaced_by_files_date_and_missing_report_offers_no_pdf(reports):
    r = reports
    c = client(r['owner'])
    target = f'/projects/{r["slug"]}/issues?crawl={NEW}&q=keep&discard=drop'
    response = c.get('/file', params={'path': str(r['file']), 'next': target})
    assert response.context['file_context']['timestamp'] == OLD
    assert parse_qs(urlsplit(response.context['file_context']['back_href']).query) == {'crawl': [OLD], 'q': ['keep']}
    r['file'].with_name('report.json').unlink()
    response = c.get('/file', params={'path': str(r['file']), 'next': target})
    assert response.status_code == 200 and not response.context['file_context']['pdf_href']


def test_file_scope_is_checked_before_hydration_and_ignores_return_as_authority(reports, monkeypatch):
    r = reports
    hydrations = []
    monkeypatch.setattr(m, '_ensure_runs_artifact_local', lambda path: hydrations.append(path))
    query = {'path': str(r['file']), 'next': f'/projects/{r["slug"]}?crawl={OLD}'}
    denied = client(r['stranger']).get('/file', params=query)
    assert denied.status_code == 403 and not hydrations
    assert 'alert(1)' not in denied.text and r['owner'].id not in denied.text
    assert denied.headers['cache-control'] == 'no-store'
    assert client().get('/file', params=query, follow_redirects=False).status_code == 303
    # A symlink under an authorized root still cannot point outside it.
    outside = r['root'].parent / 'private.txt'
    outside.write_text('private-external-content')
    linked = r['file'].with_name('linked.txt')
    linked.symlink_to(outside)
    denied = client(r['member']).get('/file', params={'path': str(linked), 'next': query['next']})
    assert denied.status_code == 403 and 'private-external-content' not in denied.text and not hydrations


@pytest.mark.parametrize('kind,status', [('empty', 200), ('malformed', 200), ('missing', 404), ('large', 413), ('unreadable', 503)])
def test_file_states_are_read_only_safe_and_recoverable(reports, monkeypatch, kind, status):
    r = reports
    target = r['file'].with_name('state.json')
    if kind != 'missing':
        target.write_text('' if kind == 'empty' else ('x' * 65537 if kind == 'large' else '{bad-json <script>alert(1)</script>'))
    monkeypatch.setattr(m, '_file_view_max_bytes', lambda: 65536)
    if kind == 'unreadable':
        original = Path.open

        def restricted(path, *args, **kwargs):
            if path == target:
                raise PermissionError('private-filesystem-exception')
            return original(path, *args, **kwargs)

        monkeypatch.setattr(Path, 'open', restricted)
    response = client(r['member']).get('/file', params={
        'path': str(target), 'next': f'/projects/{r["slug"]}?crawl={OLD}', 'lang': 'en',
    })
    assert response.status_code == status
    assert response.headers['cache-control'] == 'no-store' and response.headers['referrer-policy'] == 'no-referrer'
    soup = BeautifulSoup(response.text, 'html.parser')
    assert soup.html['lang'] == 'en' and soup.select_one('.file-preview-actions a')['href'].endswith('crawl=' + OLD)
    assert not soup.select_one('.file-preview-content script') and 'private-filesystem-exception' not in response.text
    if kind == 'empty':
        assert soup.select_one('[role=status]')
    elif status != 200:
        assert soup.select_one('[role=alert]') and not soup.select_one('pre')
    else:
        assert soup.select_one('pre').get_text() == target.read_text()


def test_shared_project_archive_still_uses_owners_entitlement(reports):
    r = reports
    r['plan']['value'] = 'free'
    c = client(r['member'])
    response = c.get('/projects/' + r['slug'])
    soup = BeautifulSoup(response.text, 'html.parser')
    assert r['plan']['payer'] == r['owner'].id
    assert not soup.select_one('.report-exports a[href*="fix-pack.zip"]')
    assert soup.select_one('.report-exports a[href*="/billing"]')
    assert c.get(f'/projects/{r["slug"]}/export/fix-pack.zip?crawl={OLD}', follow_redirects=False).status_code == 303


def test_file_growing_after_stat_still_cannot_exceed_preview_limit(reports, monkeypatch):
    r = reports
    original = Path.open

    def grown(path, mode='r', *args, **kwargs):
        if path == r['file'] and mode == 'rb':
            return io.BytesIO(b'x' * 70000)
        return original(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, 'open', grown)
    monkeypatch.setattr(m, '_file_view_max_bytes', lambda: 65536)
    response = client(r['member']).get('/file', params={'path': str(r['file'])})
    assert response.status_code == 413
    assert not BeautifulSoup(response.text, 'html.parser').select_one('pre')


@pytest.mark.parametrize('resource', ['report.pdf', 'issues.csv', 'issues-all-urls.csv', 'fix-pack.zip'])
def test_export_links_cannot_bypass_project_access(reports, resource):
    r = reports
    response = client(r['stranger']).get(
        f'/projects/{r["slug"]}/export/{resource}?crawl={OLD}', follow_redirects=False,
    )
    assert response.status_code == 404 and 'content-disposition' not in response.headers
