"""A missing card must not become an incomplete one or overwrite custom metadata."""

import base64
from types import SimpleNamespace
from urllib.parse import unquote

import pytest

from backend import app as m, repo_index
from tests.test_verified_sitemap_https import row, A, HTML as BASE_HTML

KEY = 'twitter_card_missing'
B = 'https://site.test/b'
IMAGE = 'https://site.test/social.png'
FIELDS = ('twitter_card', 'twitter_title', 'twitter_description', 'twitter_image',
          'og_title', 'og_description', 'og_image', 'title', 'meta_description')
VALUES = dict.fromkeys(FIELDS)
VALUES.update(twitter_title='Custom title', twitter_image=IMAGE, og_title='OG title', og_description='OG description',
              og_image=IMAGE, title='Page title', meta_description='Page description')
HEAD = ('<title>Page title</title><meta name="description" content="Page description" />'
        '<meta property="og:title" content="OG title" /><meta property="og:description" content="OG description" />'
        '<meta property="og:image" content="' + IMAGE + '" /><meta name="twitter:title" content="Custom title" />'
        '<meta name="twitter:image" content="' + IMAGE + '" />')
HTML = BASE_HTML.replace('</head>', HEAD + '\n</head>')
TAGS = ('<meta name="twitter:card" content="summary_large_image" />',
        '<meta name="twitter:description" content="OG description" />')


def page(url=A, **changes):
    return row(url, **dict(VALUES, **changes))


def prepare(rows, urls=None, paths=None, key=KEY):
    urls = [A] if urls is None else urls
    return m._prepare_issue_fix(issue_key=key, issues={key: {'count': len(urls), 'examples': urls}}, impacted=urls,
        all_paths=['a.html', 'b.html'] if paths is None else paths, site_name='site.test', owner='fixture',
        repo_name='fixture', branch='baseline', token='unused', pages=rows)


@pytest.mark.parametrize('change', [{'status_code': 404}, {'status_code': 301}, {'status_code': '200'},
    {'status_code': True}, {'content_type': 'application/json'}, {'error': 'timeout'}, {'blocked_by_host': True},
    {'final_url': B}, {'canonical': B}, {'meta_robots': 'noindex'}, {'meta_robots': 'none'}, {'x_robots_tag': 'noindex'},
    {'redirect_chain': [A]}, {'redirect_statuses': [302]}, {'twitter_card': 'summary'}, {'twitter_card': []},
    {'twitter_title': {}}, {'twitter_image': 'http://site.test/social.png'}, {'twitter_image': 'https://localhost/a.png'}])
def test_unhealthy_or_existing_or_malformed_card_refuses(change):
    prep = prepare([dict(page(), **change)])
    assert prep['refusal'] and prep.get('twitter_missing_urls') == [] and not prep['rewriter_ai_fallback']


@pytest.mark.parametrize('image', [None, '', '/social.png', '//site.test/social.png', 'data:image/png;base64,x',
    'https://127.0.0.1/a.png', 'https://[::1]/a.png', 'https://10.0.0.1/a.png', 'https://user:secret@site.test/a.png',
    'https://site.test:443/a.png', 'https://site.test/a.png#fragment', 'https://site.test/a png', 'https://other.local/a.png',
    'https://127.1/a.png', 'https://2130706433/a.png', 'https://0x7f000001/a.png'])
def test_no_image_is_invented_or_upgraded_and_private_ambiguous_images_refuse(image):
    assert prepare([page(twitter_image=None, og_image=image)])['refusal']


@pytest.mark.parametrize('field', FIELDS[:4])
def test_unknown_twitter_observations_refuse(field):
    observed = page()
    del observed[field]
    assert prepare([observed])['refusal']


@pytest.mark.parametrize('url', ['http://site.test/a', 'https://other.test/a', 'https://site.test/a#fragment',
    'https://user:secret@site.test/a', 'https://site.test:443/a'])
def test_foreign_or_ambiguous_target_refuses(url):
    assert prepare([page(url)], [url])['refusal']


@pytest.mark.parametrize('changes', [dict(twitter_title=None, og_title=None, title=None),
    dict(twitter_description=None, og_description=None, meta_description=None), dict(twitter_image=None, og_image=None)])
def test_card_is_not_added_when_it_would_become_incomplete(changes):
    assert prepare([page(**changes)])['refusal']


@pytest.mark.parametrize('field', ['title', 'description'])
@pytest.mark.parametrize('value', ['HTTP://LOCALHOST/title', 'HTTPS://127.0.0.1/title',
    'https://[::1]/title', 'https://10.0.0.1/title'])
def test_nonpublic_urls_cannot_be_used_as_card_text_even_with_uppercase_scheme(field, value):
    observed = page(**{'twitter_' + field: None, 'og_' + field: value})
    assert prepare([observed])['refusal']
    from backend import twitter_card
    raw = HTML
    if field == 'title':
        raw = raw.replace('<meta name="twitter:title" content="Custom title" />', '').replace('OG title', value)
    else:
        raw = raw.replace('OG description', value)
    assert twitter_card.add(raw, A, m._duplicate_html_document, m._verification_url) == (raw, 0)


def test_missing_conflicting_and_indirect_witnesses_refuse():
    for rows in ([], [page(B, final_url=A)], [page(), page(twitter_title='Other title')],
                 [page(), page(og_description='Other description')], [page(), page(canonical=None)]):
        assert prepare(rows)['refusal']


def test_healthy_og_only_page_is_not_a_missing_card_anomaly():
    assert prepare([page(twitter_title=None, twitter_image=None)])['refusal']


@pytest.mark.parametrize('key', [KEY, KEY + '_indexable', KEY + '_not_indexable', ' TWITTER_CARD_MISSING '])
def test_verified_subset_and_key_variants_do_not_use_a_model(key):
    prep = prepare([page(), page(B, meta_robots='noindex')], [A, B], key=key)
    assert not prep['refusal'] and prep.get('twitter_missing_urls') == [A] and B in prep['side_effects']
    assert not prep['rewriter_ai_fallback'] and not prep['rewriter_is_ai']


@pytest.mark.parametrize('newline,compact', [('\n', False), ('\r\n', False), ('', True)])
def test_minimal_addition_preserves_all_existing_bytes_and_is_idempotent(newline, compact):
    from backend import twitter_card
    raw = HTML.replace('\n', newline)
    tags = ('' if compact else newline).join(TAGS) + ('' if compact else newline)
    expected = raw.replace('</head>', tags + '</head>')
    assert twitter_card.add(raw, A, m._duplicate_html_document, m._verification_url) == (expected, 2)
    assert twitter_card.add(expected, A, m._duplicate_html_document, m._verification_url) == (expected, 0)


@pytest.mark.parametrize('change', [
    lambda s: s.replace('</head>', '<meta name="twitter:card" content="summary" /></head>'),
    lambda s: s.replace('</head>', '<meta name="twitter:card" content="" /></head>'),
    lambda s: s.replace('</head>', '<meta name="twitter:description" content="" /></head>'),
    lambda s: s.replace('</head>', '<meta name="twitter:image:src" content="' + IMAGE + '" /></head>'),
    lambda s: s.replace('</head>', '<meta name="twitter:title" content="Duplicate" /></head>'),
    lambda s: s.replace('</body>', '<meta name="twitter:card" content="summary" /></body>'),
    lambda s: s.replace('</head>', '<base href="https://other.test/" /></head>'),
    lambda s: s.replace('</head>', '<meta http-equiv="refresh" content="0" /></head>'),
    lambda s: s.replace('</head>', '<meta name="robots" content="none" /></head>'),
    lambda s: s.replace('</head>', '<script src="/head.js"></script></head>'),
    lambda s: s.replace('</body>', '<script src="/app.js"></script></body>'),
    lambda s: s.replace('</head>', '<template><meta name="twitter:card" /></template></head>'),
    lambda s: s.replace('</head>', '<noscript><meta name="twitter:card" /></noscript></head>'),
    lambda s: s.replace('</head>', '</head><head></head>'),
    lambda s: s.replace('lang="fr"', ''),
    lambda s: s.replace('</head>', '<title>Second title</title></head>'),
    lambda s: s.replace('</head>', '<meta name="description" content="Duplicate" /></head>'),
    lambda s: s.replace('</head>', '<meta property="og:image" content="' + IMAGE + '" /></head>'),
    lambda s: s.replace('</body>', '{{ dynamic }}</body>'),
    lambda s: s.replace('</body>', 'x' * 80_001 + '</body>'),
])
def test_existing_empty_duplicate_or_nonliteral_markup_stays_byte_identical(change):
    from backend import twitter_card
    raw = change(HTML)
    assert twitter_card.add(raw, A, m._duplicate_html_document, m._verification_url) == (raw, 0)


def test_comments_and_entities_are_preserved_and_new_attribute_values_are_escaped():
    from backend import twitter_card
    raw = HTML.replace('OG description', 'A &amp; B &quot;quoted&quot;').replace('</head>', '<!-- twitter:card -->\n  </head>')
    expected = raw.replace('  </head>', '  ' + TAGS[0] + '\n  <meta name="twitter:description" '
                           'content="A &amp; B &quot;quoted&quot;" />\n  </head>')
    assert twitter_card.add(raw, A, m._duplicate_html_document, m._verification_url) == (expected, 2)


def test_title_description_and_image_alias_fallbacks_are_same_page_only():
    from backend import twitter_card
    raw = HTML.replace(HEAD, '<title>Own title</title><meta name="description" content="Own description" />'
                       '<meta property="twitter:image:src" content="' + IMAGE + '" />')
    output, count = twitter_card.add(raw, A, m._duplicate_html_document, m._verification_url)
    assert count == 3 and 'content="Own title"' in output and 'name="twitter:image"' not in output
    assert 'name="twitter:title" content="Own title"' in output
    assert 'name="twitter:description" content="Own description"' in output


@pytest.mark.parametrize('body', ['x' * (80_000 - len(HTML) - 2), '\u00e9' * 40_000], ids=['near_limit', 'multibyte'])
def test_source_and_output_byte_limits_refuse(body):
    from backend import twitter_card
    raw = HTML.replace('</body>', body + '</body>')
    assert twitter_card.add(raw, A, m._duplicate_html_document, m._verification_url) == (raw, 0)


@pytest.mark.parametrize('case', ['verified', 'body_script', 'head_script', 'already_present', 'changed_title',
    'changed_description', 'changed_image', 'unknown_observation', 'duplicate_title', 'noindex_header', 'noindex_meta',
    'none_header', 'unsafe', 'redirect', 'history', 'wrong_url', 'json', 'oversize', 'timeout', 'bad_utf8', 'retry_after'])
def test_fresh_https_probe_checks_actual_card_and_values_and_closes_every_response(monkeypatch, case):
    raw, observed = HTML, page()
    changes = {'already_present': ('</head>', TAGS[0] + '</head>'), 'changed_title': ('Custom title', 'Changed'),
        'changed_description': ('OG description', 'Changed'), 'changed_image': (IMAGE, 'https://site.test/other.png'),
        'duplicate_title': ('</head>', '<meta name="twitter:title" content="Extra" /></head>'),
        'body_script': ('</body>', '<script src="/analytics.js"></script></body>'),
        'head_script': ('</head>', '<script src="/app.js"></script></head>'),
        'noindex_meta': ('</head>', '<meta name="robots" content="noindex" /></head>')}
    if case in changes:
        raw = raw.replace(*changes[case])
    elif case == 'unknown_observation':
        del observed['twitter_card']
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
    assert m._twitter_missing_page(A, A, observed) is (case in {'verified', 'body_script'})
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
        sources['a.html'] = HTML.replace('</head>', TAGS[0] + '</head>')
    elif case == 'stale_values':
        sources['a.html'] = HTML.replace('OG description', 'Other description')
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
            sources['b.html'] = sources['b.html'].replace('Custom title', 'Changed title')
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
        pytest.fail('No model or heuristic targeting for a verified Twitter Card')
    monkeypatch.setattr(m, '_github_api_get', get)
    monkeypatch.setattr(m, '_github_api_put', put)
    monkeypatch.setattr(m, '_twitter_missing_page', probe, raising=False)
    for name in ('_openai_generate_file_patch', '_ai_map_urls_to_files', '_ai_pick_repo_files', '_github_tarball_grep'):
        monkeypatch.setattr(m, name, forbidden)
    applied = m._apply_prepared_issue_fix(owner='fixture', repo_name='fixture', branch='baseline', token='unused', fix_branch='qa',
        all_paths=list(sources), issue_key=KEY, issue_label=KEY, impacted=urls, site_name='site.test', file_state={},
        max_files=0 if case == 'budget' else 1 if case == 'insufficient_cap' else 3, prep=prepare(rows, urls, list(sources)), pages=rows,
        index=repo_index.build_repo_index(list(sources)), allow_ai_targeting=True)
    assert not applied['ai_files']
    if case in {'verified', 'two_verified', 'second_put_failure'}:
        expected = {'a.html': HTML.replace('</head>', '\n'.join(TAGS) + '\n</head>')}
        if case == 'two_verified':
            expected['b.html'] = expected['a.html'].replace(A, B)
        assert written == expected and applied['patched'] == list(expected)
        assert applied['skipped'] == ([] if case != 'second_put_failure' else ['b.html'])
    else:
        assert not written
    if case == 'second_http_refused':
        assert probes == [(A, A), (B, B)]
