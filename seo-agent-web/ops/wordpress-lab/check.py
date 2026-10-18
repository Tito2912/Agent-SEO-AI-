"""Exercise Noyaru forms against native WordPress/Yoast, with an isolated database.

Only the test process maps the fictional HTTPS origin to the loopback Playground.
Production DNS/TLS validation is unchanged and remains covered by the normal suite.
"""
from __future__ import annotations

import json
import os
import secrets
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit

import requests
from bs4 import BeautifulSoup

ROOT = Path(os.environ['NOYARU_LAB_ROOT']).resolve()
config = json.loads((ROOT / 'credentials.json').read_text())
server = urlsplit(config['server_url'])
assert server.scheme == 'http' and server.hostname == '127.0.0.1' and not server.username
assert config['site_url'] == 'https://wordpress.noyaru.test'
os.environ.update({
    'DATABASE_URL': 'sqlite:///' + str(ROOT / 'noyaru.db'),
    'SEO_AGENT_DATA_DIR': str(ROOT / 'data'), 'SEO_AGENT_RUNS_DIR': str(ROOT / 'runs'),
    'SEO_AGENT_SECRET_KEY': secrets.token_urlsafe(40), 'SEO_AGENT_ENCRYPTION_KEY': secrets.token_urlsafe(40),
    'SEO_AGENT_DISABLE_WORKER': 'true', 'PUBLIC_BASE_URL': 'http://testserver', 'SENTRY_DSN': '',
})
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import select  # noqa: E402
from backend import app as m, auth, first_correction, wordpress_cms as wp  # noqa: E402
from backend.models import CmsChange, IssueTask, Project, User  # noqa: E402

http = requests.Session()
http.trust_env = False
calls = []
checks = []
editor = config['accounts']['editor']
origin = config['site_url']
url = config['url']
slug = 'wordpress-lab'
base = f'/projects/{slug}'


def exchange(site, username, password, method, path, payload=None):
    assert site == origin and method in {'GET', 'POST'}
    assert path.startswith(('wp/v2/users/me?', 'wp-abilities/v1/abilities/yoast-seo/'))
    response = http.request(method, config['server_url'] + '/wp-json/' + path,
                            auth=(username, password), json=payload,
                            headers={'Host': 'wordpress.noyaru.test', 'X-Forwarded-Proto': 'https'},
                            allow_redirects=False, timeout=40)
    calls.append({'method': method, 'status': response.status_code})
    if response.status_code != 200:
        raise wp.CmsError(f'WordPress HTTP {response.status_code}', uncertain=method == 'POST')
    return response.json()


# A loopback transport adapter exists only in this disposable test subprocess.
wp.exchange = exchange
native = wp.WordPressClient({'site_url': origin, **editor})


def public_page():
    parsed = urlsplit(url)
    path = parsed.path + ('?' + parsed.query if parsed.query else '')
    response = http.get(config['server_url'] + path, headers={'Host': 'wordpress.noyaru.test'},
                        allow_redirects=False, timeout=40)
    assert response.status_code == 200, 'The native WordPress page is unavailable'
    soup = BeautifulSoup(response.content, 'html.parser')
    meta = soup.find('meta', attrs={'name': 'description'})
    robots = soup.find('meta', attrs={'name': 'robots'})
    return {'url': url, 'status_code': 200, 'content_type': 'text/html',
            'title': soup.title.get_text().strip() if soup.title else '',
            'meta_description': meta.get('content', '') if meta else '',
            'indexable': not robots or 'noindex' not in robots.get('content', '').lower(),
            'h1': [h.get_text() for h in soup.find_all('h1')]}


def report(ts):
    page = public_page()
    assert page['indexable']
    issues = {}
    if not page['meta_description'].strip():
        issues['missing_meta_description'] = {'count': 1, 'examples': [url]}
    if len(page['title']) < 30:
        issues['title_too_short_indexable'] = {'count': 1, 'examples': [url]}
    value = {'meta': {'base_url': origin}, 'pages': [page], 'issues': issues}
    audit = m._runs_dir_for_user(uid) / slug / ts / 'audit'
    audit.mkdir(parents=True, exist_ok=True)
    (audit / 'report.json').write_text(json.dumps(value))
    return value


m.DB.create_tables()
with m.DB.session() as db:
    user = User(email='lab@example.invalid', password_hash='disposable', is_admin=False)
    db.add(user)
    db.commit()
    uid = str(user.id)
    project = Project(owner_user_id=uid, slug=slug, site_name='Noyaru Démo', base_url=origin, settings={})
    db.add(project)
    db.commit()
    project_id = str(project.id)
client = TestClient(m.app)
client.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(user_id=uid, secret=os.environ['SEO_AGENT_SECRET_KEY']))


def post(path, **data):
    client.get(base + '/cms')
    response = client.post(path, data={'_csrf': client.cookies.get(m._CSRF_COOKIE_NAME), **data}, follow_redirects=False)
    assert response.status_code == 303
    return response


def success(response):
    assert 'err=' not in response.headers['location'], response.headers['location']
    return response


def writes():
    return sum(call['method'] == 'POST' for call in calls)


native.check()
checks.append('Identité, droits et schémas Yoast natifs')
reader = wp.WordPressClient({'site_url': origin, **config['accounts']['subscriber']})
try:
    reader.check()
    raise AssertionError('A subscriber must not be accepted')
except wp.CmsError:
    checks.append('Compte sans droits refusé')
success(post(base + '/cms/wordpress/connect', site_url=origin, **editor))
current_ts = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y%m%d-%H%M%S')
initial = report(current_ts)
assert {'missing_meta_description', 'title_too_short_indexable'} <= initial['issues'].keys()

for key, field, value in [
    ('missing_meta_description', 'meta_description', 'Découvrez notre atelier de réparation de vélos à Lyon et prenez rendez-vous avec nos mécaniciens.'),
    ('title_too_short_indexable', 'seo_title', 'Réparation de vélos à Lyon — Atelier Noyaru Démo'),
]:
    original = native.read(url)
    success(post(base + '/first-correction', action='select', issue_key=key, crawl_ts=current_ts, url=url))
    with m.DB.session() as db:
        task = db.scalar(select(IssueTask).where(IssueTask.project_id == project_id, IssueTask.issue_key == key))
        task_id = str(task.id)
    before_writes = writes()
    preview = success(post(base + '/cms/wordpress/preview', task_id=task_id, value=value))
    receipt = preview.headers['location']
    change_id = receipt.rsplit('/', 1)[1]
    assert client.get(receipt).status_code == 200 and writes() == before_writes
    checks.append(field + ' : aperçu sans écriture')
    success(post(receipt + '/apply'))
    assert writes() == before_writes + 1 and native.read(url)[field] == value
    page = public_page()
    assert page['meta_description' if field == 'meta_description' else 'title'] == value
    assert page['h1'] == initial['pages'][0]['h1']
    success(post(receipt + '/apply'))
    assert writes() == before_writes + 1
    with m.DB.session() as db:
        assert db.get(CmsChange, change_id).state == 'applied'
        task = db.get(IssueTask, task_id)
        assert task.status == 'in_progress' and not first_correction.note_data(task.note).get('verify')
    checks.append(field + ' : une seule écriture, résultat visible dans le HTML WordPress')
    time.sleep(1.1)
    current_ts = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
    control = report(current_ts)
    m._verify_corrections_after_crawl(slug, control, m._runs_dir_for_user(uid), user_id=uid, crawl_ts=current_ts)
    with m.DB.session() as db:
        task = db.get(IssueTask, task_id)
        assert task.status == 'done' and first_correction.note_data(task.note)['verify']['result'] == 'resolved'
    checks.append(field + ' : résolution confirmée à partir du HTML réel')
    success(post(receipt + '/restore'))
    assert writes() == before_writes + 2 and wp.values(native.read(url)) == wp.values(original)
    with m.DB.session() as db:
        assert db.get(CmsChange, change_id).state == 'restored'
        task = db.get(IssueTask, task_id)
        assert task.status == 'in_progress' and not first_correction.note_data(task.note).get('verify')
    time.sleep(1.1)
    current_ts = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
    restored = report(current_ts)
    assert key in restored['issues']
    checks.append(field + ' : rétablissement confirmé dans le CMS et la page')

# A real edit made outside Noyaru must invalidate the already approved snapshot.
preview = success(post(base + '/cms/wordpress/preview', task_id=task_id, value='Un nouveau titre proposé par Noyaru'))
receipt = preview.headers['location']
native.update(native.read(url), {'seo_title': 'Modification directe dans WordPress'})
count = writes()
assert 'err=' in post(receipt + '/apply').headers['location'] and writes() == count
assert native.read(url)['seo_title'] == 'Modification directe dans WordPress'
native.update(native.read(url), {'seo_title': original['seo_title']})
checks.append('Modification externe : aperçu périmé bloqué sans écriture supplémentaire')
success(post(base + '/cms/wordpress/disconnect'))
assert 'err=' in post(receipt + '/apply').headers['location']
checks.append('Déconnexion : anciens aperçus bloqués')

result = {'wordpress': config['wp'], 'yoast': config['yoast'], 'checks': checks,
          'native_api_requests': len(calls), 'native_api_writes': writes(),
          'scope': 'WordPress local réel ; transport loopback de test, aucun site client ni HTTPS public',
          'outcome': 'passed'}
output = os.environ.get('NOYARU_LAB_REPORT')
if output:
    Path(output).write_text(json.dumps(result, indent=2, ensure_ascii=False))
print(json.dumps(result, indent=2, ensure_ascii=False))
