"""A canonical destination is not sufficient evidence of a translation."""
import base64
from types import SimpleNamespace

import pytest

from backend import app as m, repo_index
from tests.test_verified_sitemap_https import row

KEY = 'hreflang_to_non_canonical'
S, OLD, NEW = ('https://site.test/' + value for value in ('fr', 'en-alias', 'en-canonical'))
PAIR = {'page': S, 'from': OLD, 'to': NEW}


def page(url, language, canonical, values, served='known'):
    return row(url, lang=language, served_lang=language if served == 'known' else served, canonical=canonical,
        hreflang=dict(values), hreflang_raw=[{'hreflang': code, 'href': href} for code, href in values])


def pages():
    return [page(S, 'fr', S, [('fr', S), ('en', OLD)]),
            page(OLD, 'en', NEW, [('fr', S), ('en', NEW)], served=None),
            page(NEW, 'en', NEW, [('fr', S), ('en', NEW)])]


def source(url=S, language='fr', canonical=S, values=None):
    values = [('fr', S), ('en', OLD)] if values is None else values
    return ('<!doctype html>\n<html lang="' + language + '"><head>\n'
        '<title>A deliberately stable bilingual control</title>\n'
        '<link rel="canonical" href="' + canonical + '" />\n'
        '<meta property="og:url" content="' + OLD + '" />\n'
        + ''.join('<link rel="alternate" hreflang="' + code + '" href="' + href + '" />\n' for code, href in values)
        + '</head><body><h1>Control</h1><a href="' + OLD + '">Navigation</a>\n'
        '<!-- <link rel="alternate" hreflang="en" href="' + OLD + '" /> -->\n</body></html>\n')


def prepare(observations=None, pairs=None, impacted=None, key=KEY):
    return m._prepare_issue_fix(issue_key=key, issues={key: {'count': 1, 'examples': [S],
        'evidence': {'kind': 'url_pairs', 'items': [PAIR] if pairs is None else pairs}}},
        impacted=[S] if impacted is None else impacted, all_paths=['fr.html', 'en.html', 'en-alias.html'],
        site_name='site.test', owner='fixture', repo_name='fixture', branch='baseline', token='unused',
        pages=pages() if observations is None else observations)


@pytest.mark.parametrize('key', [KEY, KEY + '_indexable', KEY + '_not_indexable', ' ' + KEY.upper() + ' '])
def test_verified_two_language_group_has_an_explicit_model_free_plan(key):
    prep = prepare(key=key)
    assert not prep['refusal'] and prep.get('hreflang_canonical_pairs') == [PAIR]
    assert not prep['rewriter_ai_fallback'] and not prep['rewriter_is_ai']


@pytest.mark.parametrize('which', [0, 1, 2])
@pytest.mark.parametrize('change', [{'status_code': True}, {'status_code': '200'}, {'status_code': 404},
    {'content_type': 'application/json'}, {'error': 'timeout'}, {'blocked_by_host': True}, {'redirect_chain': [S]},
    {'redirect_statuses': [302]}, {'meta_robots': 'noindex'}, {'x_robots_tag': 'none'}, {'final_url': OLD + '?changed'},
    {'canonical': None}, {'canonical': NEW + '#fragment'}, {'url': S + '#fragment'}, {'final_url': None},
    {'lang': None}, {'lang': False}, {'hreflang': {}}, {'hreflang_raw': []}])
def test_every_source_alias_and_destination_observation_is_required_and_typed(which, change):
    observations = pages()
    observations[which].update(change)
    prep = prepare(observations)
    assert prep['refusal'] and prep.get('hreflang_canonical_pairs') == [] and not prep['rewriter_ai_fallback']


@pytest.mark.parametrize('which', [0, 1, 2])
@pytest.mark.parametrize('field', ['lang', 'served_lang', 'hreflang', 'hreflang_raw', 'canonical'])
def test_unknown_required_fields_refuse(which, field):
    observations = pages()
    del observations[which][field]
    assert prepare(observations)['refusal']


@pytest.mark.parametrize('bad', ['missing', 'final_alias_only', 'contradictory_duplicate', 'wrong_language',
    'wrong_alias_language', 'no_return', 'different_group', 'duplicate_code', 'missing_self', 'third_language',
    'x_default', 'canonical_chain', 'loop', 'foreign', 'http', 'fragment', 'port', 'credentials', 'conflicting_pairs'])
def test_destinations_groups_and_literal_identity_cannot_be_guessed(bad):
    observations, pairs = pages(), [dict(PAIR)]
    if bad == 'missing':
        observations.pop()
    elif bad == 'final_alias_only':
        observations[2]['url'] = NEW + '?alias'
    elif bad == 'contradictory_duplicate':
        observations.append(dict(observations[2], lang='fr'))
    elif bad in {'wrong_language', 'wrong_alias_language'}:
        observations[2 if bad == 'wrong_language' else 1]['lang'] = 'fr'
    elif bad in {'no_return', 'different_group', 'missing_self', 'third_language', 'x_default', 'duplicate_code'}:
        index = 0 if bad in {'third_language', 'x_default', 'duplicate_code', 'missing_self'} else 2
        values = [('en', NEW)] if bad == 'no_return' else [('fr', S + '?other'), ('en', NEW)] if bad == 'different_group' else [('en', OLD)] if bad == 'missing_self' else [('fr', S), ('en', OLD),
            ('fr' if bad == 'duplicate_code' else 'x-default' if bad == 'x_default' else 'de', S)]
        observations[index].update(hreflang=dict(values), hreflang_raw=[{'hreflang': code, 'href': href} for code, href in values])
    elif bad in {'canonical_chain', 'loop'}:
        observations[2]['canonical'] = OLD if bad == 'loop' else NEW + '-chain'
    elif bad == 'conflicting_pairs':
        pairs.append(dict(PAIR, to=NEW + '?other'))
    else:
        pairs[0]['to'] = {'foreign': 'https://elsewhere.test/en', 'http': 'http://site.test/en',
            'fragment': NEW + '#fragment', 'port': 'https://site.test:443/en', 'credentials': 'https://user:secret@site.test/en'}[bad]
    assert prepare(observations, pairs)['refusal']


def test_named_subset_refuses_the_unproved_page_without_fallback():
    prep = prepare(pairs=[PAIR, dict(PAIR, page=S + '?unproved')], impacted=[S, S + '?unproved'])
    assert not prep['refusal'] and prep['hreflang_canonical_pairs'] == [PAIR]
    assert S + '?unproved' in prep['side_effects']


def test_only_the_actual_one_hreflang_href_changes_and_repeated_application_is_idle():
    from backend import hreflang_canonical as h
    raw = source()
    expected = raw.replace('hreflang="en" href="' + OLD, 'hreflang="en" href="' + NEW, 1)
    assert h.rewrite(raw, PAIR, pages()[0], m._duplicate_html_document, m._verification_url) == (expected, 1)
    assert h.rewrite(expected, PAIR, pages()[0], m._duplicate_html_document, m._verification_url) == (expected, 0)
    assert '<meta property="og:url" content="' + OLD in expected and '<a href="' + OLD in expected
    assert '<!-- <link rel="alternate" hreflang="en" href="' + OLD in expected


@pytest.mark.parametrize('style', ['single_quotes', 'crlf', 'attribute_order', 'upper_case'])
def test_valid_literal_attribute_styles_preserve_every_other_byte(style):
    from backend import hreflang_canonical as h
    raw = source()
    if style == 'single_quotes':
        raw = raw.replace('"', "'")
    elif style == 'crlf':
        raw = raw.replace('\n', '\r\n')
    elif style == 'attribute_order':
        raw = raw.replace('hreflang="en" href="' + OLD + '"', 'href="' + OLD + '" hreflang="en"')
    else:
        raw = raw.replace('<link', '<LINK').replace('hreflang=', 'HREFLANG=').replace('href=', 'HREF=')
    new, count = h.rewrite(raw, PAIR, pages()[0], m._duplicate_html_document, m._verification_url)
    assert count == 1 and new.replace(NEW, OLD, 1) == raw


@pytest.mark.parametrize('bad', ['wrong_source', 'wrong_canonical', 'stale_language', 'stale_href', 'nested', 'body',
    'duplicate', 'duplicate_attr', 'existing_script', 'template', 'base', 'refresh', 'noindex', 'unquoted', 'oversize',
    'ambiguous_offset', 'decoy_attr'])
def test_nonliteral_stale_or_ambiguous_sources_remain_byte_identical(bad):
    from backend import hreflang_canonical as h
    raw = source()
    tag = '<link rel="alternate" hreflang="en" href="' + OLD + '" />'
    changes = {'wrong_source': (S, S + '?other'), 'wrong_canonical': ('rel="canonical" href="' + S, 'rel="canonical" href="' + OLD),
        'stale_language': ('lang="fr"', 'lang="en"'), 'stale_href': (tag, tag.replace(OLD, NEW)),
        'nested': (tag, '<template>' + tag + '</template>'), 'body': (tag + '\n</head>', '</head>' + tag),
        'duplicate': (tag, tag + tag), 'duplicate_attr': (tag, tag.replace('href=', 'href="other" href=')),
        'existing_script': ('</body>', '<script src="/app.js"></script></body>'), 'template': ('</body>', '{{ value }}</body>'),
        'base': ('</head>', '<base href="' + S + '" /></head>'), 'refresh': ('</head>', '<meta http-equiv="refresh" content="0" /></head>'),
        'noindex': ('</head>', '<meta name="robots" content="none" /></head>'), 'unquoted': (tag, tag.replace('href="' + OLD + '"', 'href=' + OLD)),
        'decoy_attr': (tag, '<link rel="alternate" hreflang="en" href=' + OLD + ' data-x=\'href="' + OLD + '"\' />')}
    if bad in changes:
        raw = raw.replace(*changes[bad], 1)
    elif bad == 'oversize':
        raw += 'x' * 80_001
    else:
        raw = '<!-- line\u2028 separator -->\n' + raw
    assert h.rewrite(raw, PAIR, pages()[0], m._duplicate_html_document, m._verification_url) == (raw, 0)


@pytest.mark.parametrize('case', ['verified', 'body_script', 'changed_lang', 'changed_alternate', 'changed_canonical',
    'noindex', 'redirect', 'history', 'wrong_url', 'json', 'oversize', 'bad_utf8', 'timeout'])
def test_fresh_probe_attests_the_actual_lang_canonical_and_group_and_closes(monkeypatch, case):
    raw = source(NEW, 'en', NEW, [('fr', S), ('en', NEW)])
    if case == 'body_script':
        raw = raw.replace('</body>', '<script src="/footer.js"></script></body>')
    elif case == 'changed_lang':
        raw = raw.replace('lang="en"', 'lang="fr"')
    elif case == 'changed_alternate':
        raw = raw.replace('hreflang="fr" href="' + S, 'hreflang="fr" href="' + S + '?changed')
    elif case == 'changed_canonical':
        raw = raw.replace('rel="canonical" href="' + NEW, 'rel="canonical" href="' + OLD)
    elif case == 'oversize':
        raw += 'x' * 80_001
    closed, fetched = [], []
    headers = {'content-type': 'application/json' if case == 'json' else 'text/html'}
    if case == 'noindex':
        headers['x-robots-tag'] = 'noindex'
    response = SimpleNamespace(status_code=302 if case == 'redirect' else 200, url=OLD if case == 'wrong_url' else NEW,
        history=[object()] if case == 'history' else [], headers=headers, close=lambda: closed.append(True),
        iter_content=lambda chunk_size: iter([b'\xff' if case == 'bad_utf8' else raw.encode()]))
    def get(url, **kw):
        fetched.append(url)
        if case == 'timeout':
            raise TimeoutError()
        return response
    monkeypatch.setattr(m, '_validate_public_crawl_target', lambda url: None)
    monkeypatch.setattr(m.requests, 'get', get)
    assert m._hreflang_canonical_page(NEW, NEW, pages()[2]) is (case in {'verified', 'body_script'})
    assert fetched == [NEW] and closed == ([] if case == 'timeout' else [True])


@pytest.mark.parametrize('case', ['verified', 'stale', 'no_sha', 'unreadable', 'encoding', 'routing', 'ambiguous',
    'budget', 'source_probe', 'alias_probe', 'destination_probe', 'put_failure'])
def test_pipeline_finishes_all_three_https_witnesses_before_the_only_guarded_put(monkeypatch, case):
    sources = {'fr.html': source()}
    if case == 'stale':
        sources['fr.html'] = sources['fr.html'].replace('hreflang="en"', 'hreflang="de"')
    elif case == 'routing':
        sources['_redirects'] = '/fr /en 301\n'
    elif case == 'ambiguous':
        sources['fr/index.html'] = source()
    writes, probes = {}, []
    def get(api, **kw):
        path = api.split('/contents/', 1)[1]
        if case == 'unreadable':
            raise OSError('read failure')
        return {'sha': '' if case == 'no_sha' else 'original', 'encoding': 'none' if case == 'encoding' else 'base64',
                'content': base64.b64encode(sources[path].encode()).decode()}
    def probe(url, canonical, observed):
        probes.append(url)
        assert canonical == (S if url == S else NEW) and observed in pages()
        return url != {'source_probe': S, 'alias_probe': OLD, 'destination_probe': NEW}.get(case)
    def put(api, **kw):
        assert probes == [S, OLD, NEW] and kw['json_body']['sha'] == 'original'
        if case == 'put_failure':
            raise OSError('write failure')
        writes[api.split('/contents/', 1)[1]] = base64.b64decode(kw['json_body']['content']).decode()
        return {'content': {'sha': 'new'}}
    def forbidden(*a, **kw):
        pytest.fail('No model or heuristic targeting is allowed')
    monkeypatch.setattr(m, '_github_api_get', get)
    monkeypatch.setattr(m, '_github_api_put', put)
    monkeypatch.setattr(m, '_hreflang_canonical_page', probe, raising=False)
    for name in ('_openai_generate_file_patch', '_ai_map_urls_to_files', '_ai_pick_repo_files', '_github_tarball_grep'):
        monkeypatch.setattr(m, name, forbidden)
    result = m._apply_prepared_issue_fix(owner='fixture', repo_name='fixture', branch='baseline', token='unused', fix_branch='qa',
        all_paths=list(sources), issue_key=KEY, issue_label=KEY, impacted=[S], site_name='site.test', file_state={},
        max_files=0 if case == 'budget' else 1, prep=prepare(), pages=pages(),
        index=repo_index.build_repo_index(list(sources)), allow_ai_targeting=True)
    assert not result['ai_files']
    assert result['patched'] == (['fr.html'] if case == 'verified' else [])
    if case == 'verified':
        assert writes == {'fr.html': source().replace('hreflang="en" href="' + OLD, 'hreflang="en" href="' + NEW, 1)}
    else:
        assert not writes


@pytest.mark.parametrize('suffix', ["?q='quoted'&b=1", '?q=' + 'x' * 80_000], ids=['escaped_query', 'oversize_output'])
def test_replacement_escapes_the_single_attribute_or_refuses_oversize_output(suffix):
    import html
    from backend import hreflang_canonical as h
    raw, destination = source(), NEW + suffix
    result, count = h.rewrite(raw, dict(PAIR, to=destination), pages()[0], m._duplicate_html_document, m._verification_url)
    if len(suffix) > 80_000:
        assert (result, count) == (raw, 0)
    else:
        assert count == 1 and result == raw.replace('hreflang="en" href="' + OLD + '"',
            'hreflang="en" href="' + html.escape(destination, quote=True) + '"', 1)


@pytest.mark.parametrize('case', ['two_verified', 'second_stale', 'second_probe', 'insufficient_cap', 'second_put_failure'])
def test_whole_multi_file_preflight_and_explicit_partial_write_failure(monkeypatch, case):
    s2, old2, new2 = S + '2', OLD + '2', NEW + '2'
    pairs = [PAIR, {'page': s2, 'from': old2, 'to': new2}]
    observations = pages() + [page(s2, 'fr', s2, [('fr', s2), ('en', old2)]),
        page(old2, 'en', new2, [('fr', s2), ('en', new2)], served=None), page(new2, 'en', new2, [('fr', s2), ('en', new2)])]
    sources = {'fr.html': source(), 'fr2.html': source().replace(S, s2).replace(OLD, old2).replace(NEW, new2)}
    if case == 'second_stale':
        sources['fr2.html'] = sources['fr2.html'].replace('hreflang="en"', 'hreflang="de"')
    probes, writes = [], []
    def get(api, **kw):
        return {'sha': 'original', 'encoding': 'base64', 'content': base64.b64encode(sources[api.split('/contents/', 1)[1]].encode()).decode()}
    def probe(url, canonical, observed):
        probes.append(url)
        return not (case == 'second_probe' and url == new2)
    def put(api, **kw):
        assert probes == [S, OLD, NEW, s2, old2, new2] and kw['json_body']['sha'] == 'original'
        path = api.split('/contents/', 1)[1]
        if case == 'second_put_failure' and path == 'fr2.html':
            raise OSError('second write failed')
        writes.append(path)
        return {'content': {'sha': 'new'}}
    monkeypatch.setattr(m, '_github_api_get', get)
    monkeypatch.setattr(m, '_github_api_put', put)
    monkeypatch.setattr(m, '_hreflang_canonical_page', probe)
    for name in ('_openai_generate_file_patch', '_ai_map_urls_to_files', '_ai_pick_repo_files', '_github_tarball_grep'):
        monkeypatch.setattr(m, name, lambda *a, **kw: pytest.fail('No heuristic or model fallback'))
    prep = prepare(observations, pairs, [S, s2])
    result = m._apply_prepared_issue_fix(owner='fixture', repo_name='fixture', branch='baseline', token='unused', fix_branch='qa',
        all_paths=list(sources), issue_key=KEY, issue_label=KEY, impacted=[S, s2], site_name='site.test', file_state={},
        max_files=1 if case == 'insufficient_cap' else 2, prep=prep, pages=observations,
        index=repo_index.build_repo_index(list(sources)), allow_ai_targeting=True)
    expected = ['fr.html', 'fr2.html'] if case == 'two_verified' else ['fr.html'] if case == 'second_put_failure' else []
    assert result['patched'] == writes == expected and not result['ai_files']
    if case == 'second_put_failure':
        assert result['skipped'] == ['fr2.html']
    elif case in {'second_stale', 'insufficient_cap'}:
        assert not probes
