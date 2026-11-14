"""Real local routes, fictional owner, no external network or production credentials."""
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import threading
import time
from datetime import timedelta

WEB = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(WEB), str(WEB / 'tests')]
temporary = tempfile.TemporaryDirectory(prefix='noyaru-social-browser-')
root = Path(temporary.name)
os.environ.update(DATABASE_URL='sqlite:///' + str(root / 'bootstrap.db'),
    SEO_AGENT_SECRET_KEY='fictional-local-social-session', SEO_AGENT_DISABLE_WORKER='true',
    SEO_AGENT_DISABLE_SCHEDULERS='true', SEO_AGENT_DISABLE_TELEGRAM_RECEIVER='true',
    TELEGRAM_BOT_TOKEN='', TELEGRAM_CHAT_ID='', AWS_EC2_METADATA_DISABLED='true')
import pytest  # noqa: E402
import uvicorn  # noqa: E402
from sqlalchemy.exc import OperationalError  # noqa: E402
from playwright.sync_api import sync_playwright  # noqa: E402
from backend import app as m, auth, own_social as s  # noqa: E402
from test_customer_activation import lab  # noqa: E402
from test_own_social import studio, current, NOW  # noqa: E402

patch = pytest.MonkeyPatch()
laboratory = lab.__wrapped__(root, patch)
x = studio.__wrapped__(next(laboratory), patch)
current(x)
get_doc, catalog = s.content_library.get_doc, s.seo_watch.catalog
scene = {'name': 'ready'}


def docs(slug, lang):
    return None if scene['name'] == 'empty' else get_doc(slug, lang)


def knowledge(*args, **kwargs):
    if scene['name'] == 'unavailable':
        raise OperationalError('unavailable', {}, None)
    if scene['name'] == 'old':
        kwargs['now'] = NOW + timedelta(days=8)
    return catalog(*args, **kwargs)


patch.setattr(s.content_library, 'get_doc', docs)
patch.setattr(s.seo_watch, 'catalog', knowledge)
with socket.socket() as sock:
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
base = f'http://127.0.0.1:{port}'
patch.setenv('PUBLIC_BASE_URL', base)
server = uvicorn.Server(uvicorn.Config(m.app, host='127.0.0.1', port=port, lifespan='off', log_level='error'))
thread = threading.Thread(target=server.run, daemon=True)
thread.start()
for _ in range(100):
    if server.started: break
    time.sleep(.05)
assert server.started
if os.environ.get('CHROMIUM_LIBRARY_PATH'):
    os.environ['LD_LIBRARY_PATH'] = os.environ['CHROMIUM_LIBRARY_PATH']
output = Path(os.environ.get('SOCIAL_BROWSER_OUTPUT', str(root / 'proof')))
output.mkdir(parents=True, exist_ok=True)
observations, errors, external = [], [], []
try:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, executable_path=os.environ.get('CHROMIUM_EXECUTABLE'))
        for lang in ('fr', 'en'):
            context = browser.new_context(locale='fr-FR' if lang == 'fr' else 'en-US', reduced_motion='reduce')
            context.add_cookies([{'name': auth.SESSION_COOKIE_NAME, 'value': auth.make_session_token(
                user_id=str(x['owner'].id), secret=os.environ['SEO_AGENT_SECRET_KEY']), 'url': base}])
            def route(r):
                if r.request.url.startswith(base + '/'): r.continue_()
                else:
                    external.append(r.request.url.split('/', 3)[2]); r.abort()
            context.route('**/*', route)
            page = context.new_page()
            page.on('pageerror', lambda e: errors.append(str(e)))
            cases = [
                ('ideas', 'ready', '', 200),
                ('instagram', 'ready', '&topic=score', 200),
                ('tiktok', 'ready', '&network=tiktok&goal=help&audience=business&topic=verify', 200),
                ('blocked', 'old', '&topic=geo', 200),
                ('empty', 'empty', '&goal=product', 200),
                ('unavailable', 'unavailable', '', 503),
            ]
            for label, fixture, query, status in cases:
                scene['name'] = fixture
                for width, height in ((1440, 1000), (768, 1024), (390, 844), (360, 800)):
                    page.set_viewport_size({'width': width, 'height': height})
                    response = page.goto(base + s.PATH + '?lang=' + lang + query)
                    assert response.status == status
                    page.wait_for_load_state('networkidle')
                    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1'), (label, lang, width)
                    if width in (1440, 390):
                        page.evaluate('scrollTo(0,0)')
                        page.screenshot(path=str(output / f'{label}-{lang}-{width}.png'), full_page=True)
                    if label in ('instagram', 'tiktok'):
                        assert page.locator('.social-sequence > li').count() == (4 if label == 'instagram' else 3)
                        with page.expect_download() as download:
                            page.locator('#production-pack a[href*="/download"]').click()
                        saved = Path(download.value.path()).read_text(encoding='utf-8')
                        assert ('Production sequence' if lang == 'en' else 'Déroulé à réaliser') in saved
                        page.locator('#production-pack a[href*="#social-ideas"]').click()
                        assert 'topic=' not in page.url and '#social-ideas' in page.url
                    if label in ('blocked', 'empty', 'unavailable'):
                        assert page.locator('#production-pack a[href*="/download"]').count() == 0
                    if label == 'ideas':
                        # The browser's form changes context without executing a background action.
                        page.select_option('#social-network', 'tiktok')
                        page.select_option('#social-goal', 'product')
                        page.select_option('#social-audience', 'business')
                        page.locator('.social-filters button').click()
                        page.wait_for_load_state('networkidle')
                        assert all(v in page.url for v in ('network=tiktok', 'goal=product', 'audience=business', 'lang=' + lang))
                        page.locator('.social-idea a.btn').first.click()
                        assert 'topic=first' in page.url and '#production-pack' in page.url
                    observations.append({'lang': lang, 'scene': label, 'width': width, 'status': status, 'overflow': False})
            context.close()
            # GET filters, native controls and downloads also work without page scripts.
            context = browser.new_context(java_script_enabled=False, viewport={'width': 360, 'height': 800})
            context.add_cookies([{'name': auth.SESSION_COOKIE_NAME, 'value': auth.make_session_token(
                user_id=str(x['owner'].id), secret=os.environ['SEO_AGENT_SECRET_KEY']), 'url': base}])
            context.route('**/*', route)
            scene['name'] = 'ready'
            page = context.new_page()
            assert page.goto(base + s.PATH + '?lang=' + lang).status == 200
            page.select_option('#social-network', 'tiktok')
            page.locator('.social-filters button').click()
            page.locator('.social-idea a.btn').first.click()
            with page.expect_download() as download:
                page.locator('#production-pack a[href*="/download"]').click()
            assert Path(download.value.path()).stat().st_size > 300
            observations.append({'lang': lang, 'scene': 'no-javascript', 'width': 360, 'download': True})
            context.close()
        browser.close()
    assert not errors and not external, (errors, external)
    result = {'scope': 'Local fictional owner, real routes and Chromium; no production access, AI or external requests',
        'observations': observations, 'javascript_errors': errors, 'external_requests': external}
    (output / 'browser-results.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({'observations': len(observations), 'output': str(output)}))
finally:
    server.should_exit = True
    thread.join(timeout=5)
    laboratory.close()
    patch.undo()
    temporary.cleanup()
