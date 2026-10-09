"""A missing HTML language comes only from an unambiguous literal self alternate."""

import base64
from types import SimpleNamespace
from urllib.parse import unquote

import pytest

from backend import app as m, repo_index
from tests.test_verified_sitemap_https import row, A, HTML as BASE_HTML

KEY = 'hreflang_defined_but_html_lang_missing'
B = 'https://site.test/b'
ALT = '<link rel="alternate" hreflang="fr" href="' + A + '" />'
HTML = BASE_HTML.replace(' lang="fr"', '').replace('</head>', ALT + '</head>')


def page(url=A, **changes):
    return row(url, **{'lang': None, 'served_lang': None, 'hreflang': {'fr': url},
                      'hreflang_raw': [{'hreflang': 'fr', 'href': url}], **changes})


def prepare(rows, urls=None, paths=None, key=KEY):
    urls = [A] if urls is None else urls
    return m._prepare_issue_fix(issue_key=key, issues={key: {'count': len(urls), 'examples': urls}}, impacted=urls,
        all_paths=['a.html', 'b.html'] if paths is None else paths, site_name='site.test', owner='fixture',
        repo_name='fixture', branch='baseline', token='unused', pages=rows)


@pytest.mark.parametrize('change', [{'status_code': 404}, {'status_code': 301}, {'status_code': '200'},
    {'status_code': True}, {'content_type': 'application/json'}, {'error': 'timeout'}, {'blocked_by_host': True},
    {'final_url': B}, {'canonical': B}, {'meta_robots': 'noindex'}, {'meta_robots': 'none'}, {'x_robots_tag': 'noindex'},
    {'redirect_chain': [A]}, {'redirect_statuses': [302]}, {'canonical': None}, {'lang': 'fr'}, {'lang': False}, {'lang': []},
    {'served_lang': 'en'}, {'served_lang': {}}, {'hreflang_raw': None}, {'hreflang': None}])
def test_unhealthy_existing_unknown_or_malformed_language_refuses(change):
    prep = prepare([dict(page(), **change)])
    assert prep['refusal'] and prep.get('hreflang_lang_urls') == [] and not prep['rewriter_ai_fallback']


@pytest.mark.parametrize('field', ['lang', 'served_lang', 'hreflang', 'hreflang_raw'])
def test_unknown_observations_are_not_assumed_missing(field):
    observed = page()
    del observed[field]
    assert prepare([observed])['refusal']


@pytest.mark.parametrize('code,href', [('x-default', A), ('fr_fr', A), ('', A), ('fra', A), ('fr-', A),
    ('fr', B), ('fr', A + '#fragment'), ('fr', '/a'), ('fr', 'http://site.test/a')])
def test_invalid_or_nonself_or_default_only_annotations_do_not_invent_a_language(code, href):
    assert prepare([page(hreflang={code: href}, hreflang_raw=[{'hreflang': code, 'href': href}])])['refusal']


@pytest.mark.parametrize('extra', [{'hreflang': 'en', 'href': A}, {'hreflang': 'fr-FR', 'href': A},
    {'hreflang': 'fr', 'href': A}, {'hreflang': 'FR', 'href': B}])
def test_competing_self_languages_regions_and_duplicate_codes_refuse(extra):
    raw = page()['hreflang_raw'] + [extra]
    values = {'fr': A, extra['hreflang']: extra['href']}
    assert prepare([page(hreflang=values, hreflang_raw=raw)])['refusal']


def test_x_default_and_other_translations_are_preserved_not_used_to_guess():
    raw = page()['hreflang_raw'] + [{'hreflang': 'x-default', 'href': A}, {'hreflang': 'en', 'href': B}]
    prep = prepare([page(hreflang={'fr': A, 'x-default': A, 'en': B}, hreflang_raw=raw)])
    assert not prep['refusal'] and prep['hreflang_lang_urls'] == [A]


def test_map_raw_and_duplicate_observations_must_agree():
    for rows in ([], [page(B, final_url=A)], [page(), page(canonical=None)],
                 [page(), page(hreflang={'en': A}, hreflang_raw=[{'hreflang': 'en', 'href': A}])],
                 [page(hreflang={'fr': B})], [page(hreflang_raw=[])],
                 [page(hreflang_raw=[{'hreflang': [], 'href': A}])]):
        assert prepare(rows)['refusal']


@pytest.mark.parametrize('url', ['http://site.test/a', 'https://other.test/a', 'https://site.test/a#fragment',
    'https://user:secret@site.test/a', 'https://site.test:443/a'])
def test_foreign_or_ambiguous_target_refuses(url):
    assert prepare([page(url)], [url])['refusal']


@pytest.mark.parametrize('key', [KEY, KEY + '_indexable', KEY + '_not_indexable', ' ' + KEY.upper() + ' '])
def test_verified_subset_and_key_variants_are_explicit_and_model_free(key):
    prep = prepare([page(), page(B, meta_robots='noindex')], [A, B], key=key)
    assert not prep['refusal'] and prep['hreflang_lang_urls'] == [A] and B in prep['side_effects']
    assert not prep['rewriter_ai_fallback'] and not prep['rewriter_is_ai']


@pytest.mark.parametrize('opening', ['<html>', '<HTML class="page">', "<html\n  class='page' >"])
@pytest.mark.parametrize('code', ['fr', 'en-US', 'zh-Hant'])
def test_add_only_one_root_attribute_preserving_every_other_byte_and_idempotence(opening, code):
    from backend import hreflang_lang
    raw = HTML.replace('<html>', opening).replace('hreflang="fr"', 'hreflang="' + code + '"')
    expected = raw.replace(opening, opening[:-1] + ' lang="' + code.lower() + '">', 1)
    assert hreflang_lang.add(raw, A, m._duplicate_html_document, m._verification_url) == (expected, 1)
    assert hreflang_lang.add(expected, A, m._duplicate_html_document, m._verification_url) == (expected, 0)


@pytest.mark.parametrize('change', [
    lambda s: s.replace('<html>', '<html lang="fr">'),
    lambda s: s.replace('<html>', '<html LANG="">'),
    lambda s: s.replace('<html>', '<html lang>'),
    lambda s: s.replace('<html>', '<html xml:lang="fr">'),
    lambda s: s.replace('<html>', '<html class="x" class="y">'),
    lambda s: s.replace('</head>', '<base href="https://other.test/" /></head>'),
    lambda s: s.replace('</head>', '<meta http-equiv="refresh" content="0" /></head>'),
    lambda s: s.replace('</head>', '<meta name="robots" content="none" /></head>'),
    lambda s: s.replace('</head>', '<script src="/head.js"></script></head>'),
    lambda s: s.replace('</body>', '<script src="/app.js"></script></body>'),
    lambda s: s.replace(ALT, '<template>' + ALT + '</template>'),
    lambda s: s.replace(ALT, '<noscript>' + ALT + '</noscript>'),
    lambda s: s.replace(ALT, '').replace('</body>', ALT + '</body>'),
    lambda s: s.replace(ALT, ALT + ALT),
    lambda s: s.replace('hreflang="fr"', 'hreflang="fr_fr"'),
    lambda s: s.replace('rel="alternate"', 'rel="stylesheet"'),
    lambda s: s.replace('hreflang="fr"', 'hreflang="fr" hreflang="en"'),
    lambda s: s.replace(ALT, ALT + '<link rel="alternate" hreflang="en" href="' + A + '" />'),
    lambda s: s.replace('</head>', '</head><head></head>'),
    lambda s: s.replace('</body>', '{{ dynamic }}</body>'),
])
def test_existing_empty_ambiguous_and_nonliteral_sources_are_unchanged(change):
    from backend import hreflang_lang
    raw = change(HTML)
    assert hreflang_lang.add(raw, A, m._duplicate_html_document, m._verification_url) == (raw, 0)


def test_comments_entities_and_crlf_are_not_reformatted_or_counted_as_active_annotations():
    from backend import hreflang_lang
    url = A + '?x=1&y=2'
    raw = ('<!-- <html lang="en"> -->\r\n' + HTML.replace(A, url.replace('&', '&amp;'))).replace('</head>',
        '<!-- <link rel="alternate" hreflang="en" href="' + url + '" /> --></head>')
    assert hreflang_lang.add(raw, url, m._duplicate_html_document, m._verification_url) == (
        raw.replace('<html>', '<html lang="fr">', 1), 1)


@pytest.mark.parametrize('body', ['x' * (80_000 - len(HTML) - 2), '\u00e9' * 40_000], ids=['near_limit', 'multibyte'])
def test_source_and_output_byte_limits_refuse_without_truncation(body):
    from backend import hreflang_lang
    raw = HTML.replace('</body>', body + '</body>')
    assert hreflang_lang.add(raw, A, m._duplicate_html_document, m._verification_url) == (raw, 0)


def test_only_the_explicit_language_addition_can_inspect_missing_language_documents():
    assert m._duplicate_html_document(HTML) is None
    assert m._duplicate_html_document(HTML, allow_missing_lang=True)['lang'] == ''


def test_default_https_probe_still_refuses_missing_language_even_on_healthy_public_html(monkeypatch):
    closed = []
    response = SimpleNamespace(status_code=200, url=A, history=[], headers={'content-type': 'text/html'},
        close=lambda: closed.append(True), iter_content=lambda chunk_size: iter([HTML.encode()]))
    monkeypatch.setattr(m, '_validate_public_crawl_target', lambda url: None)
    monkeypatch.setattr(m.requests, 'get', lambda *a, **kw: response)
    assert m._sitemap_https_page(A, A) is False
    assert closed == [True]


def test_ambiguous_source_offsets_cannot_add_a_language_outside_the_actual_root():
    from backend import hreflang_lang
    raw = '<!-- first\u2028 second -->\n' + HTML
    assert hreflang_lang.add(raw, A, m._duplicate_html_document, m._verification_url) == (raw, 0)


@pytest.mark.parametrize('case', ['verified', 'body_script', 'head_script', 'already_present', 'changed_code',
    'changed_href', 'duplicate_self', 'unknown_observation', 'noindex_header', 'none_header', 'noindex_meta',
    'unsafe', 'redirect', 'history', 'wrong_url', 'json', 'oversize', 'timeout', 'bad_utf8', 'retry_after'])
def test_fresh_https_probe_attests_absence_self_language_and_actual_alternates_and_closes_response(monkeypatch, case):
    raw, observed = HTML, page()
    changes = {'already_present': ('<html>', '<html lang="fr">'), 'changed_code': ('hreflang="fr"', 'hreflang="en"'),
        'changed_href': (ALT, ALT.replace(A, B)), 'duplicate_self': (ALT, ALT + ALT),
        'body_script': ('</body>', '<script src="/analytics.js"></script></body>'),
        'head_script': ('</head>', '<script src="/app.js"></script></head>'),
        'noindex_meta': ('</head>', '<meta name="robots" content="noindex" /></head>')}
    if case in changes:
        raw = raw.replace(*changes[case])
    elif case == 'unknown_observation':
        del observed['served_lang']
    elif case == 'oversize':
        raw += 'x' * 80_001
    fetched, closed = [], []
    headers = {'content-type': 'application/json' if case == 'json' else 'text/html; charset=utf-8'}
    if case in {'noindex_header', 'none_header'}:
        headers['x-robots-tag'] = 'none' if case == 'none_header' else 'noindex'
    elif case == 'retry_after':
        headers['retry-after'] = '10'
    response = SimpleNamespace(status_code=302 if case == 'redirect' else 200, url=B if case == 'wrong_url' else A,
        history=[object()] if case == 'history' else [], headers=headers, close=lambda: closed.append(True),
        iter_content=lambda chunk_size: iter([b'\xff' if case == 'bad_utf8' else raw.encode()]))
    monkeypatch.setattr(m, '_validate_public_crawl_target', lambda url: 'unsafe' if case == 'unsafe' else None)
    def get(url, **kw):
        fetched.append(url)
        assert kw == {'allow_redirects': False, 'stream': True, 'timeout': (5, 10)}
        if case == 'timeout':
            raise TimeoutError()
        return response
    monkeypatch.setattr(m.requests, 'get', get)
    assert m._hreflang_lang_page(A, A, observed) is (case in {'verified', 'body_script'})
    assert fetched == ([] if case == 'unsafe' else [A])
    assert closed == ([] if case in {'unsafe', 'timeout'} else [True])


@pytest.mark.parametrize('case', ['verified', 'already_present', 'stale_values', 'stale_noindex', 'scripted',
    'ambiguous_route', 'missing_route', 'shared_template', 'changed_rules', 'wildcard_rules', 'toml_rules',
    'unreadable', 'missing_sha', 'invalid_base64', 'unknown_encoding', 'budget', 'second_stale', 'fresh_http_refused',
    'second_http_refused', 'put_failure', 'two_verified', 'insufficient_cap', 'second_put_failure', 'shared_route_query'])
def test_pipeline_preflights_all_sources_and_https_witnesses_before_any_write(monkeypatch, case):
    sources = {'a.html': HTML, 'b.html': HTML.replace(A, B)}
    urls, rows = [A], [page()]
    if case == 'already_present':
        sources['a.html'] = HTML.replace('<html>', '<html lang="fr">')
    elif case == 'stale_values':
        sources['a.html'] = HTML.replace('hreflang="fr"', 'hreflang="en"')
    elif case == 'stale_noindex':
        sources['a.html'] = HTML.replace('</head>', '<meta name="robots" content="noindex" /></head>')
    elif case == 'scripted':
        sources['a.html'] = HTML.replace('</body>', '<script src="/app.js"></script></body>')
    elif case == 'ambiguous_route':
        sources['a/index.html'] = HTML
    elif case == 'missing_route':
        del sources['a.html']
    elif case == 'shared_template':
        sources = {'app/layout.tsx': 'export default function Layout(){return <html><head /></html>;}'}
    elif case in {'changed_rules', 'wildcard_rules'}:
        sources['_redirects'] = '/a /b 301\n' if case == 'changed_rules' else '/* /index.html 200\n'
    elif case == 'toml_rules':
        sources['netlify.toml'] = '[[redirects]]\nfrom="/a"\nto="/b"\nstatus=200\n'
    elif case in {'second_stale', 'second_http_refused', 'two_verified', 'insufficient_cap', 'second_put_failure'}:
        urls, rows = [A, B], [page(), page(B)]
        if case == 'second_stale':
            sources['b.html'] = sources['b.html'].replace('hreflang="fr"', 'hreflang="en"')
    elif case == 'shared_route_query':
        urls, rows = [A, A + '?copy=1'], [page(), page(A + '?copy=1')]
    probes, written = [], {}
    def get(api, **kw):
        path = unquote(api.split('/contents/', 1)[1])
        if case == 'unreadable':
            raise OSError('unreadable')
        return {'sha': '' if case == 'missing_sha' else 'original', 'encoding': 'none' if case == 'unknown_encoding' else 'base64',
                'content': '!!!!' if case == 'invalid_base64' else base64.b64encode(sources[path].encode()).decode()}
    def put(api, **kw):
        path = unquote(api.split('/contents/', 1)[1])
        if case == 'put_failure' or case == 'second_put_failure' and path == 'b.html':
            raise OSError('write failed')
        assert kw['json_body']['sha'] == 'original'
        assert probes == ([(A, A), (B, B)] if case in {'two_verified', 'second_put_failure'} else [(A, A)])
        written[path] = base64.b64decode(kw['json_body']['content']).decode()
        return {'content': {'sha': 'new'}}
    def probe(url, canonical, observed):
        assert observed in rows
        probes.append((url, canonical))
        return case != 'fresh_http_refused' and not (case == 'second_http_refused' and url == B)
    def forbidden(*a, **kw):
        pytest.fail('No model or heuristic targeting for a verified self-hreflang language')
    monkeypatch.setattr(m, '_github_api_get', get)
    monkeypatch.setattr(m, '_github_api_put', put)
    monkeypatch.setattr(m, '_hreflang_lang_page', probe, raising=False)
    for name in ('_openai_generate_file_patch', '_ai_map_urls_to_files', '_ai_pick_repo_files', '_github_tarball_grep'):
        monkeypatch.setattr(m, name, forbidden)
    applied = m._apply_prepared_issue_fix(owner='fixture', repo_name='fixture', branch='baseline', token='unused', fix_branch='qa',
        all_paths=list(sources), issue_key=KEY, issue_label=KEY, impacted=urls, site_name='site.test', file_state={},
        max_files=0 if case == 'budget' else 1 if case == 'insufficient_cap' else 3, prep=prepare(rows, urls, list(sources)), pages=rows,
        index=repo_index.build_repo_index(list(sources)), allow_ai_targeting=True)
    assert not applied['ai_files']
    if case in {'verified', 'two_verified', 'second_put_failure'}:
        expected = {'a.html': HTML.replace('<html>', '<html lang="fr">')}
        if case == 'two_verified':
            expected['b.html'] = expected['a.html'].replace(A, B)
        assert written == expected and applied['patched'] == list(expected)
        assert applied['skipped'] == ([] if case != 'second_put_failure' else ['b.html'])
    else:
        assert not written
    if case == 'second_http_refused':
        assert probes == [(A, A), (B, B)]
