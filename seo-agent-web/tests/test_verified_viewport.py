"""A viewport addition is a single page-bound tag, never a free-form head rewrite."""

import base64
from urllib.parse import unquote

import pytest

from backend import app as m, repo_index
from tests.test_verified_sitemap_https import row, HTML as BASE_HTML, A

KEY = 'viewport_not_set'
TAG = '<meta name="viewport" content="width=device-width, initial-scale=1" />'
B = 'https://site.test/b'
HTML = BASE_HTML.replace('<head>', '<head>\n').replace('</head>', '\n</head>')


def page(url=A, **extra):
    return row(url, meta_viewport=None, meta_viewport_tag_count=0, **extra)


def prepare(rows, urls=None, paths=None):
    urls = [A] if urls is None else urls
    return m._prepare_issue_fix(issue_key=KEY, issues={KEY: {'count': len(urls), 'examples': urls}}, impacted=urls,
        all_paths=['a.html', 'b.html', 'control.html'] if paths is None else paths, site_name='site.test',
        owner='fixture', repo_name='fixture', branch='baseline', token='unused', pages=rows)


@pytest.mark.parametrize('change', [{'status_code': 404}, {'status_code': 301}, {'status_code': '200'},
    {'status_code': True}, {'content_type': 'application/json'}, {'error': 'timeout'}, {'blocked_by_host': True},
    {'final_url': B}, {'canonical': B}, {'meta_robots': 'noindex'}, {'meta_robots': 'none'}, {'x_robots_tag': 'googlebot: noindex'},
    {'redirect_chain': [A]}, {'redirect_statuses': [302]}])
def test_unhealthy_or_nonindexable_page_refuses(change):
    prep = prepare([dict(page(), **change)])
    assert prep['refusal'] and prep.get('viewport_urls') == [] and not prep['rewriter_ai_fallback']


@pytest.mark.parametrize('count,value', [(None, None), ('0', None), (False, None), (-1, None), (1, None),
    (1, ''), (2, 'width=device-width'), (0, 'width=device-width')])
def test_a_missing_viewport_requires_explicit_typed_zero_and_no_value(count, value):
    prep = prepare([dict(page(), meta_viewport_tag_count=count, meta_viewport=value)])
    assert prep['refusal'] and prep.get('viewport_urls') == []


@pytest.mark.parametrize('url', ['http://site.test/a', 'https://other.test/a', 'https://site.test/a#fragment',
    'https://user:secret@site.test/a', 'https://site.test:443/a'])
def test_foreign_or_ambiguous_url_refuses(url):
    assert prepare([page(url)], [url])['refusal']


def test_missing_indirect_and_conflicting_observations_refuse():
    for rows in ([], [page(B, final_url=A)], [page(), dict(page(), meta_viewport_tag_count=1)],
                 [page(), page(canonical=None)]):
        assert prepare(rows)['refusal']


def test_verified_subset_is_named_and_does_not_use_a_model():
    prep = prepare([page(), page(B, meta_robots='noindex')], [A, B])
    assert not prep['refusal'] and prep.get('viewport_urls') == [A] and B in prep['side_effects']
    assert not prep['rewriter_ai_fallback']


@pytest.mark.parametrize('newline,compact', [('\n', False), ('\r\n', False), ('', True)])
def test_only_one_minimal_tag_is_added_and_the_operation_is_idempotent(newline, compact):
    from backend import viewport
    raw = HTML.replace('\n', newline)
    output, count = viewport.add(raw, A, m._duplicate_html_document, m._verification_url)
    assert count == 1 and output == raw.replace('</head>', TAG + ('' if compact else newline) + '</head>')
    assert viewport.add(output, A, m._duplicate_html_document, m._verification_url) == (output, 0)


@pytest.mark.parametrize('change', [
    lambda s: s.replace('</head>', TAG + '</head>'),
    lambda s: s.replace('</body>', TAG + '</body>'),
    lambda s: s.replace('</head>', '<meta name="viewport" content="" /></head>'),
    lambda s: s.replace('</head>', '<META NAME="VIEWPORT" content="custom" /></head>'),
    lambda s: s.replace('</head>', '<base href="https://other.test/" /></head>'),
    lambda s: s.replace('</head>', '<meta http-equiv="refresh" content="0" /></head>'),
    lambda s: s.replace('</head>', '<meta name="robots" content="none" /></head>'),
    lambda s: s.replace('</head>', '<script src="/head.js"></script></head>'),
    lambda s: s.replace('</body>', '<script src="/app.js"></script></body>'),
    lambda s: s.replace('</head>', '<template>' + TAG + '</template></head>'),
    lambda s: s.replace('</head>', '<noscript>' + TAG + '</noscript></head>'),
    lambda s: s.replace('</head>', '</head><head></head>'),
    lambda s: s.replace('</head>', ''),
    lambda s: s.replace('lang="fr"', ''),
    lambda s: s.replace('</head>', '<meta name="x" name="y" /></head>'),
    lambda s: s.replace('</body>', '{{ dynamic }}</body>'),
    lambda s: s.replace('</body>', 'x' * 80_001 + '</body>'),
])
def test_existing_or_ambiguous_viewport_markup_is_left_byte_identical(change):
    from backend import viewport
    raw = change(HTML)
    assert viewport.add(raw, A, m._duplicate_html_document, m._verification_url) == (raw, 0)


def test_comments_are_not_viewports_and_existing_attributes_and_indentation_survive():
    from backend import viewport
    raw = HTML.replace('</head>', '<!-- ' + TAG + ' -->\n  </head>')
    expected = raw.replace('  </head>', '  ' + TAG + '\n  </head>')
    assert viewport.add(raw, A, m._duplicate_html_document, m._verification_url) == (expected, 1)


@pytest.mark.parametrize('body', ['x' * (80_000 - len(HTML) - 2), '\u00e9' * 40_000], ids=['near_limit', 'multibyte'])
def test_source_and_output_byte_limits_refuse_without_truncating(body):
    from backend import viewport
    raw = HTML.replace('</body>', body + '</body>')
    assert viewport.add(raw, A, m._duplicate_html_document, m._verification_url) == (raw, 0)


@pytest.mark.parametrize('value', [[], {}])
def test_malformed_observed_viewport_value_is_a_refusal_not_an_exception(value):
    assert prepare([dict(page(), meta_viewport=value)])['refusal']


@pytest.mark.parametrize('case', ['verified', 'already_present', 'stale_canonical', 'stale_noindex', 'ambiguous_route',
    'missing_route', 'shared_template', 'scripted', 'changed_rules', 'wildcard_rules', 'ambiguous_rules', 'toml_rules',
    'unreadable', 'missing_sha', 'invalid_base64', 'unknown_encoding', 'budget', 'second_stale', 'fresh_http_refused',
    'second_http_refused', 'put_failure', 'two_verified', 'insufficient_cap', 'second_put_failure', 'shared_route_query'])
def test_pipeline_preflights_sources_rules_and_all_http_witnesses_without_heuristics(monkeypatch, case):
    sources = {'a.html': HTML, 'b.html': HTML.replace(A, B), 'control.html': HTML.replace(A, 'https://site.test/control')}
    urls, rows = [A], [page()]
    if case == 'already_present':
        sources['a.html'] = sources['a.html'].replace('</head>', TAG + '</head>')
    elif case == 'stale_canonical':
        sources['a.html'] = sources['a.html'].replace(A, B)
    elif case == 'stale_noindex':
        sources['a.html'] = sources['a.html'].replace('</head>', '<meta name="robots" content="noindex" /></head>')
    elif case == 'ambiguous_route':
        sources['a/index.html'] = HTML
    elif case == 'missing_route':
        del sources['a.html']
    elif case == 'shared_template':
        sources = {'app/layout.tsx': 'export default function Layout(){return <html><head /></html>;}'}
    elif case == 'scripted':
        sources['a.html'] = HTML.replace('</body>', '<script src="/app.js"></script></body>')
    elif case in {'changed_rules', 'wildcard_rules', 'ambiguous_rules'}:
        sources['_redirects'] = '/a /b 301\n' if case == 'changed_rules' else '/* /index.html 200\n'
        if case == 'ambiguous_rules':
            sources['public/_redirects'] = '/a /b 301\n'
    elif case == 'toml_rules':
        sources['netlify.toml'] = '[[redirects]]\nfrom="/a"\nto="/b"\nstatus=200\n'
    elif case in {'second_stale', 'second_http_refused', 'two_verified', 'insufficient_cap', 'second_put_failure'}:
        urls, rows = [A, B], [page(), page(B)]
        if case == 'second_stale':
            sources['b.html'] = sources['b.html'].replace('</head>', '<script src="/head.js"></script></head>')
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
    def probe(url, canonical):
        probes.append((url, canonical))
        return case != 'fresh_http_refused' and not (case == 'second_http_refused' and url == B)
    def forbidden(*a, **kw):
        pytest.fail('No model, layout guess or heuristic target for a verified viewport addition')
    monkeypatch.setattr(m, '_github_api_get', get)
    monkeypatch.setattr(m, '_github_api_put', put)
    monkeypatch.setattr(m, '_sitemap_https_page', probe)
    for name in ('_openai_generate_file_patch', '_ai_map_urls_to_files', '_ai_pick_repo_files', '_github_tarball_grep'):
        monkeypatch.setattr(m, name, forbidden)
    applied = m._apply_prepared_issue_fix(owner='fixture', repo_name='fixture', branch='baseline', token='unused', fix_branch='qa',
        all_paths=list(sources), issue_key=KEY, issue_label=KEY, impacted=urls, site_name='site.test', file_state={},
        max_files=0 if case == 'budget' else 1 if case == 'insufficient_cap' else 3, prep=prepare(rows, urls, list(sources)), pages=rows,
        index=repo_index.build_repo_index(list(sources)), allow_ai_targeting=True)
    assert not applied['ai_files']
    if case in {'two_verified', 'second_put_failure'}:
        expected = {'a.html': HTML.replace('</head>', TAG + '\n</head>')}
        if case == 'two_verified':
            expected['b.html'] = HTML.replace(A, B).replace('</head>', TAG + '\n</head>')
        assert written == expected and applied['patched'] == list(expected)
        assert applied['skipped'] == ([] if case == 'two_verified' else ['b.html'])
    elif case == 'verified':
        assert written == {'a.html': HTML.replace('</head>', TAG + '\n</head>')} and applied['patched'] == ['a.html']
    else:
        assert not written
    if case == 'second_http_refused':
        assert probes == [(A, A), (B, B)]
