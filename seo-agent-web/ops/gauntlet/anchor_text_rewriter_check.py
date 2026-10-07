"""Local, network-blocked browser check. Not a deployment or a family certification."""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend import anchor_text  # noqa: E402

NAME = 'Contactez-nous'
OPENING = '<a id="empty" data-href="/other" data-title="incidental" href="/contact">'
LABEL = ' aria-label="' + NAME + '"'
ITEMS = [{'page': 'https://site.test/', 'field': href, 'value': value}
         for href, value in [('/contact', NAME), ('/named', 'Lien deja nomme'), ('/images', 'Image du contact')]]
SOURCES = ['backend/app.py', 'backend/anchor_text.py', 'tests/test_anchor_text_literal_rewriter.py',
           'ops/gauntlet/anchor_text_rewriter_check.py', 'tests/test_gauntlet_anchor_text_rewriter_check.py']
ICON = '<svg width="28" height="28" viewBox="0 0 28 28"><path d="M2 2H26V26H2Z" fill="#168269" /></svg>'


def fixture():
    image = base64.b64encode(ICON.replace('<svg ', '<svg xmlns="http://www.w3.org/2000/svg" ').encode()).decode()
    decoy = '<a href="/contact">' + ICON + '</a>'
    return ('<!DOCTYPE html><html lang="fr"><head><title>Temoin des ancres</title>'
            '<meta http-equiv="Content-Security-Policy" content="default-src \'none\'; '
            'style-src \'unsafe-inline\'; img-src data:">'
            '<style>body{font:16px Arial;margin:24px;color:#17201e}nav{display:flex;gap:16px}a{display:block}</style>'
            '</head><body><h1>Contacts</h1><h2 id="name">Lien deja nomme</h2><nav>'
            + OPENING + ICON + '</a>'
            + '<a id="already-named" href="/named" aria-labelledby="name">' + ICON + '</a>'
            + '<a id="duplicate-empty" href="/named">' + ICON + '</a>'
            + '<a id="image" href="/images"><img width="28" height="28" alt="Image du contact" '
            + 'src="data:image/svg+xml;base64,' + image + '" /></a>'
            + '<a id="decoy" data-href="/contact" href="/other">' + ICON + '</a>'
            + '<a id="unproved" href="/unproved">' + ICON + '</a></nav>'
            + '<!-- ' + decoy + ' --><template>' + decoy + '</template>'
            + '<script type="application/json">' + json.dumps({'link': decoy}) + '</script>'
            + '</body></html>')


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def manifest():
    return {path: sha((ROOT / path).read_bytes().replace(b'\r\n', b'\n')) for path in SOURCES}


def check_rewrite(rewriter=anchor_text.rewrite):
    raw = fixture()
    expected = raw.replace(OPENING, OPENING[:-1] + LABEL + '>', 1)
    actual, count = rewriter(raw, ITEMS)
    if count != 1 or actual != expected or rewriter(actual, ITEMS) != (actual, 0):
        raise AssertionError('The exact single-attribute edit and its idempotence are required.')
    return raw, actual


def check_dom(before, after):
    ids = {'empty', 'already-named', 'duplicate-empty', 'image', 'decoy', 'unproved'}
    if set(before) != ids or set(after) != ids:
        raise AssertionError('The real document must retain all six anchors.')
    for key in ids:
        old, new = dict(before[key]), dict(after[key])
        if key == 'empty':
            if old.get('aria_label') is not None or new.pop('aria_label', None) != NAME:
                raise AssertionError('Only the selected empty anchor gains the measured name.')
            del old['aria_label']
        if new != old:
            raise AssertionError('Unselected names, destinations, content and geometry must remain unchanged.')


def dom(page):
    return page.evaluate('''() => Object.fromEntries([...document.querySelectorAll('a[href]')].map(a => [a.id, {
        href: a.getAttribute('href'), aria_label: a.getAttribute('aria-label'),
        labelledby: a.getAttribute('aria-labelledby'), inner: a.innerHTML,
        text: a.textContent, rectangle: a.getBoundingClientRect().toJSON()
    }]))''')


def run(workdir):
    workdir = Path(workdir)
    workdir.mkdir(parents=True, exist_ok=False)
    frozen = manifest()
    raw, actual = check_rewrite()
    (workdir / 'before.html').write_bytes(raw.encode('utf-8'))
    (workdir / 'after.html').write_bytes(actual.encode('utf-8'))
    requests, results = [], []
    from playwright.sync_api import sync_playwright

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        try:
            context = browser.new_context()

            def deny(route):
                requests.append(route.request.url)
                route.abort()

            context.route('**/*', deny)
            for name, size in [('desktop', {'width': 1280, 'height': 800}), ('mobile', {'width': 390, 'height': 844})]:
                page = context.new_page()
                page.set_viewport_size(size)
                facts, screenshots, names = [], [], []
                for stage, text in [('before', raw), ('after', actual)]:
                    page.set_content(text, wait_until='load')
                    page.evaluate('() => Promise.all([...document.images].map(image => image.decode()))')
                    facts.append(dom(page))
                    names.append(page.get_by_role('link', name=NAME, exact=True).count())
                    if (page.get_by_role('link', name='Lien deja nomme', exact=True).count() != 1
                            or page.get_by_role('link', name='Image du contact', exact=True).count() != 1):
                        raise AssertionError('Existing accessible names must survive.')
                    screenshots.append(page.screenshot(path=str(workdir / (name + '-' + stage + '.png'))))
                check_dom(*facts)
                if names != [0, 1] or screenshots[0] != screenshots[1]:
                    raise AssertionError('A real accessible name must be added without changing these rendered pixels.')
                results.append({'viewport': size, 'selected_named_link_counts': names, 'unchanged_dom_controls': 5,
                                'screenshot_sha256': [sha(value) for value in screenshots], 'dom': facts})
                page.close()
        finally:
            browser.close()
    if requests or frozen != manifest():
        raise AssertionError('Network requests and changing source files invalidate this local check.')
    result = {'status': 'local_literal_rewriter_only', 'http_requests': 0, 'github_writes': 0, 'ai_calls': 0,
              'source_sha256_lf': frozen, 'html_sha256': [sha(raw.encode()), sha(actual.encode())], 'browser': results,
              'limits': ['No deployment, live crawl or complete issue-family acceptance.',
                         'Destination freshness, route binding and shared components remain to be verified.',
                         'Equal pixels apply to this fixture, not arbitrary CSS using aria-label selectors.',
                         'An accessible name is not a promise of Google anchor-text treatment.']}
    (workdir / 'check.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workdir', type=Path, required=True, help='A new local artifact directory.')
    result = run(parser.parse_args().workdir)
    print(json.dumps({key: result[key] for key in ('status', 'http_requests', 'github_writes', 'ai_calls')}))
