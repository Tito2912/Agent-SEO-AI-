"""Local typography check: real formatters/Markdown, fictitious text, no app session."""
import ast
from datetime import datetime, timezone
import html
import importlib.util
import json
import os
from pathlib import Path
import re
from types import SimpleNamespace

from playwright.sync_api import sync_playwright

WEB = Path(__file__).resolve().parents[2]
OUT = Path(os.environ.get('NOYARU_TEXT_PROOF_OUT', '/tmp/noyaru-own-blog-text-browser'))
OUT.mkdir(parents=True, exist_ok=True)


def functions(path, names):
    tree = ast.parse(path.read_text(encoding='utf-8'))
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    assert {node.name for node in nodes} == set(names)
    namespace = {'html': html, 're': re}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec'), namespace)
    return namespace


writing = functions(WEB / 'backend/own_writing.py', ['export'])
blog = functions(WEB / 'backend/own_blog_content.py', ['tokenized', 'prose'])
spec = importlib.util.spec_from_file_location('local_content_library', WEB / 'backend/content_library.py')
library = importlib.util.module_from_spec(spec)
spec.loader.exec_module(library)
styles = '''body{background:#111;color:#f2f4f1;font:18px/1.6 system-ui;margin:0}
main{max-width:960px;margin:auto;padding:24px}h1{font-size:26px}section{background:#191c19;
border:1px solid #343c34;border-radius:16px;padding:20px;margin:20px 0}pre{white-space:pre-wrap;
overflow-wrap:anywhere;font:16px/1.6 monospace}a{color:#a6d8b6}'''
records = []
with sync_playwright() as p:
    browser = p.chromium.launch(executable_path='/home/aron/.cache/ms-playwright/chromium-1223/chrome-linux64/chrome',
        headless=True, env={**os.environ, 'LD_LIBRARY_PATH': '/tmp/noyaru-browser-libs-20261112/extracted/usr/lib/x86_64-linux-gnu'})
    page = browser.new_page()
    page.route('**/*', lambda route: route.abort())
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    for lang in ['fr', 'en']:
        sentence = ('L\'audit compare "avant" et "après" : clics & citations, avec *prudence* et des limites explicites.'
            if lang == 'fr' else 'An audit doesn\'t prove "ranking gains": clicks & citations require *careful* interpretation over time.')
        label = 'Aperçu du texte' if lang == 'fr' else 'Text preview'
        row = SimpleNamespace(bundle={'brief': {'lang': lang}, 'sources': [{'id': 'example',
            'title': 'L\'audit "SEO"', 'url': 'https://example.invalid/source', 'checked_at': '2026-11-14', 'fingerprint': 'f'*64}]},
            content={'title': label, 'description': label, 'intro': {'text': sentence, 'sources': ['example']}, 'sections': []},
            status='reviewed', created_at=datetime(2026, 11, 14, tzinfo=timezone.utc))
        exported = writing['export'](row)
        rendered = library._markdown_instance().convert(blog['prose'](sentence))
        document = '<!doctype html><html lang="'+lang+'"><meta charset="utf-8"><meta name="viewport" content="width=device-width">'
        document += '<title>'+label+'</title><style>'+styles+'</style><main><h1>'+label+'</h1>'
        document += '<section><h2>Markdown</h2><pre id="raw">'+html.escape(exported)+'</pre></section>'
        document += '<section id="preview">'+rendered+'</section></main></html>'
        for width in [1440, 1024, 768, 390, 360]:
            page.set_viewport_size({'width': width, 'height': 1000})
            page.set_content(document)
            raw = page.locator('#raw').inner_text()
            assert 'L\'audit "SEO"' in raw and '&#x27;' not in raw and '&quot;' not in raw
            assert page.locator('#preview').inner_text() == sentence
            assert page.locator('#preview em, #preview strong, #preview script').count() == 0
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
            assert not errors
            records.append({'lang': lang, 'width': width, 'literal_quotes': True, 'rendered_text_exact': True,
                'markup_inert': True, 'overflow': False, 'page_errors': 0, 'network': 'blocked'})
            if width in [1440, 360]:
                page.screenshot(path=str(OUT / (lang+'-'+str(width)+'.png')), full_page=True)
    browser.close()
(OUT / 'browser.json').write_text(json.dumps(records, indent=2), encoding='utf-8')
print(json.dumps({'observations': len(records), 'output': str(OUT), 'errors': errors}))
