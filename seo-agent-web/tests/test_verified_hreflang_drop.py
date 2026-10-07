"""Remove a false language only with a current, independently observed group."""
import base64
from types import SimpleNamespace

import pytest

from backend import app as m, repo_index
from tests.test_verified_hreflang_canonical import page, source

KEY = 'page_referenced_for_more_than_one_language_in_hreflang'
S, T = 'https://site.test/fr', 'https://site.test/en'
ITEM = {'page': S, 'field': 'fr', 'value': T}


def pages():
    return [page(S, 'fr', S, [('fr', T), ('en', T)]),
            page(T, 'en', T, [('fr', S), ('en', T)])]


def raw():
    return source(url=S, language='fr', canonical=S, values=[('fr', T), ('en', T)])


def expected(text=None):
    text = raw() if text is None else text
    return text.replace('<link rel="alternate" hreflang="fr" href="' + T + '" />', '', 1)


def prepare(observations=None, items=None, impacted=None, key=KEY):
    return m._prepare_issue_fix(issue_key=key, issues={key: {'count': 1, 'examples': [S],
        'evidence': {'kind': 'page_values', 'items': [ITEM] if items is None else items}}},
        impacted=[S] if impacted is None else impacted, all_paths=['fr.html', 'en.html'],
        site_name='site.test', owner='fixture', repo_name='fixture', branch='baseline', token='unused',
        pages=pages() if observations is None else observations)


@pytest.mark.parametrize('key', [KEY, KEY + '_indexable', KEY + '_not_indexable', ' ' + KEY.upper() + ' '])
def test_independently_observed_bilingual_group_has_a_protected_plan(key):
    plan = prepare(key=key)
    assert not plan['refusal'] and plan.get('hreflang_drop_items') == [ITEM]
    assert plan['link_rewriter'] is None and not plan['rewriter_ai_fallback']


@pytest.mark.parametrize('which', [0, 1])
@pytest.mark.parametrize('change', [{'status_code': True}, {'status_code': '200'}, {'status_code': 404},
    {'content_type': 'application/json'}, {'error': 'timeout'}, {'blocked_by_host': True}, {'redirect_chain': [S]},
    {'redirect_statuses': [302]}, {'meta_robots': 'noindex'}, {'x_robots_tag': 'none'}, {'final_url': T + '?other'},
    {'canonical': None}, {'canonical': T + '#fragment'}, {'url': S + '#fragment'}, {'final_url': None},
    {'lang': None}, {'lang': False}, {'served_lang': None}, {'hreflang': {}}, {'hreflang_raw': []}])
def test_both_observations_are_typed_direct_indexable_and_self_canonical(which, change):
    rows = pages()
    rows[which].update(change)
    plan = prepare(rows)
    assert plan['refusal'] and plan.get('hreflang_drop_items') == [] and not plan['rewriter_ai_fallback']


@pytest.mark.parametrize('which', [0, 1])
@pytest.mark.parametrize('field', ['lang', 'served_lang', 'canonical', 'hreflang', 'hreflang_raw'])
def test_unknown_fields_refuse(which, field):
    rows = pages()
    del rows[which][field]
    assert prepare(rows)['refusal']


@pytest.mark.parametrize('case', ['no_target', 'final_alias', 'contradictory_duplicate', 'wrong_target_language',
    'missing_return', 'other_group', 'wrong_source_code', 'same_primary_language', 'x_default', 'third_language',
    'duplicate_code', 'other_destination', 'wrong_evidence_code', 'conflicting_items', 'http', 'foreign',
    'fragment', 'credentials', 'port', 'no_evidence'])
def test_language_group_and_removal_cannot_be_guessed(case):
    rows, items = pages(), [dict(ITEM)]
    if case == 'no_target':
        rows.pop()
    elif case == 'final_alias':
        rows[1]['url'] = T + '?alias'
    elif case == 'contradictory_duplicate':
        rows.append(dict(rows[1], lang='de'))
    elif case == 'wrong_target_language':
        rows[1].update(lang='de', served_lang='de')
    elif case in {'missing_return', 'other_group'}:
        values = [('en', T)] if case == 'missing_return' else [('fr', S + '?other'), ('en', T)]
        rows[1] = page(T, 'en', T, values)
    elif case in {'wrong_source_code', 'same_primary_language', 'x_default', 'third_language', 'duplicate_code', 'other_destination'}:
        values = [('de', T), ('en', T)] if case == 'wrong_source_code' else [('fr-ca', T), ('fr-fr', T)] if case == 'same_primary_language' else [('fr', T), ('en', T)]
        if case in {'x_default', 'third_language', 'duplicate_code'}:
            values.append(('x-default' if case == 'x_default' else 'de' if case == 'third_language' else 'fr', T))
        if case == 'other_destination':
            values[0] = ('fr', S)
        rows[0] = page(S, 'fr', S, values)
    elif case == 'wrong_evidence_code':
        items[0]['field'] = 'en'
    elif case == 'conflicting_items':
        items.append(dict(ITEM, field='en'))
    elif case == 'no_evidence':
        items = []
    else:
        items[0]['value'] = {'http': 'http://site.test/en', 'foreign': 'https://other.test/en',
            'fragment': T + '#x', 'credentials': 'https://user:secret@site.test/en', 'port': 'https://site.test:443/en'}[case]
    assert prepare(rows, items)['refusal']


def test_verified_subset_names_the_unproved_source_without_fallback():
    plan = prepare(items=[ITEM, dict(ITEM, page=S + '?other')], impacted=[S, S + '?other'])
    assert not plan['refusal'] and plan['hreflang_drop_items'] == [ITEM]
    assert S + '?other' in plan['side_effects']


def test_only_one_actual_tag_is_removed_and_reapplication_is_idle():
    from backend import hreflang_drop as h
    text = raw()
    assert h.rewrite(text, ITEM, pages()[0], m._duplicate_html_document, m._verification_url) == (expected(), 1)
    assert h.rewrite(expected(), ITEM, pages()[0], m._duplicate_html_document, m._verification_url) == (expected(), 0)


@pytest.mark.parametrize('style', ['single_quotes', 'crlf', 'order', 'upper_case', 'comment', 'inline'])
def test_literal_styles_preserve_all_other_bytes(style):
    from backend import hreflang_drop as h
    text, tag = raw(), '<link rel="alternate" hreflang="fr" href="' + T + '" />'
    if style == 'single_quotes':
        text, tag = text.replace('"', "'"), tag.replace('"', "'")
    elif style == 'crlf':
        text = text.replace('\n', '\r\n')
    elif style == 'order':
        changed = '<link href="' + T + '" hreflang="fr" rel="alternate" />'
        text, tag = text.replace(tag, changed), changed
    elif style == 'upper_case':
        text, tag = text.replace('<link', '<LINK').replace('hreflang=', 'HREFLANG='), tag.replace('<link', '<LINK').replace('hreflang=', 'HREFLANG=')
    elif style == 'comment':
        text = text.replace('</body>', '<!-- ' + tag + ' --></body>')
    else:
        text = text.replace('\n', '')
    assert h.rewrite(text, ITEM, pages()[0], m._duplicate_html_document, m._verification_url) == (text.replace(tag, '', 1), 1)


@pytest.mark.parametrize('case', ['stale_code', 'stale_destination', 'wrong_canonical', 'wrong_root_lang', 'nested',
    'body', 'duplicate', 'duplicate_attr', 'unquoted', 'script', 'template', 'base', 'refresh', 'noindex', 'offset', 'oversize'])
def test_stale_or_ambiguous_source_is_preserved(case):
    from backend import hreflang_drop as h
    text, tag = raw(), '<link rel="alternate" hreflang="fr" href="' + T + '" />'
    mutations = {'stale_code': (tag, tag.replace('"fr"', '"de"')), 'stale_destination': (tag, tag.replace(T, S)),
        'wrong_canonical': ('rel="canonical" href="' + S, 'rel="canonical" href="' + T), 'wrong_root_lang': ('lang="fr"', 'lang="en"'),
        'nested': (tag, '<template>' + tag + '</template>'), 'body': (tag, ''), 'duplicate': (tag, tag + tag),
        'duplicate_attr': (tag, tag.replace('href=', 'href="other" href=')), 'unquoted': (tag, tag.replace('href="' + T + '"', 'href=' + T)),
        'script': ('</body>', '<script src="/app.js"></script></body>'), 'template': ('</body>', '{{ value }}</body>'),
        'base': ('</head>', '<base href="' + S + '" /></head>'), 'refresh': ('</head>', '<meta http-equiv="refresh" content="0" /></head>'),
        'noindex': ('</head>', '<meta name="robots" content="none" /></head>')}
    if case in mutations:
        text = text.replace(*mutations[case], 1)
        if case == 'body':
            text = text.replace('</body>', tag + '</body>')
    elif case == 'offset':
        text = '<!-- offset\u2028 -->\n' + text
    else:
        text += 'x' * 80_001
    assert h.rewrite(text, ITEM, pages()[0], m._duplicate_html_document, m._verification_url) == (text, 0)


@pytest.mark.parametrize('case', ['verified', 'stale', 'no_sha', 'encoding', 'routing', 'ambiguous', 'budget',
    'source_probe', 'target_probe', 'put_failure'])
def test_pipeline_completes_both_fresh_probes_before_sha_guarded_write(monkeypatch, case):
    sources, probes, writes = {'fr.html': raw()}, [], {}
    if case == 'stale':
        sources['fr.html'] = expected()
    elif case == 'routing':
        sources['_redirects'] = '/fr /en 301\n'
    elif case == 'ambiguous':
        sources['fr/index.html'] = raw()
    def get(api, **kw):
        return {'sha': '' if case == 'no_sha' else 'original', 'encoding': 'none' if case == 'encoding' else 'base64',
            'content': base64.b64encode(sources[api.split('/contents/', 1)[1]].encode()).decode()}
    def probe(url, canonical, observed):
        probes.append(url)
        assert canonical == url and observed in pages()
        return url != {'source_probe': S, 'target_probe': T}.get(case)
    def put(api, **kw):
        assert probes == [S, T] and kw['json_body']['sha'] == 'original'
        if case == 'put_failure':
            raise OSError('write failed')
        writes[api.split('/contents/', 1)[1]] = base64.b64decode(kw['json_body']['content']).decode()
        return {'content': {'sha': 'new'}}
    monkeypatch.setattr(m, '_github_api_get', get)
    monkeypatch.setattr(m, '_github_api_put', put)
    monkeypatch.setattr(m, '_hreflang_drop_page', probe, raising=False)
    for name in ('_openai_generate_file_patch', '_ai_map_urls_to_files', '_ai_pick_repo_files', '_github_tarball_grep'):
        monkeypatch.setattr(m, name, lambda *a, **kw: pytest.fail('No model or heuristic fallback'))
    result = m._apply_prepared_issue_fix(owner='fixture', repo_name='fixture', branch='baseline', token='unused', fix_branch='qa',
        all_paths=list(sources), issue_key=KEY, issue_label=KEY, impacted=[S], site_name='site.test', file_state={},
        max_files=0 if case == 'budget' else 1, prep=prepare(), pages=pages(),
        index=repo_index.build_repo_index(list(sources)), allow_ai_targeting=True)
    assert not result['ai_files'] and result['patched'] == (['fr.html'] if case == 'verified' else [])
    assert writes == ({'fr.html': expected()} if case == 'verified' else {})


@pytest.mark.parametrize('case', ['verified', 'wrong_lang', 'stale_tags', 'noindex', 'none', 'redirect',
    'wrong_content_type', 'bad_status_type', 'fragment', 'base', 'unknown', 'oversize', 'unreadable'])
def test_fresh_streamed_https_probe_uses_actual_body_and_keeps_preview_policy(monkeypatch, case):
    text, closed, calls = raw(), [], []
    status, headers = 200, {'content-type': 'text/html; charset=utf-8'}
    url, history = S, []
    if case == 'wrong_lang':
        text = text.replace('lang="fr"', 'lang="en"')
    elif case == 'stale_tags':
        text = expected()
    elif case in {'noindex', 'none'}:
        headers['x-robots-tag'] = case
    elif case == 'redirect':
        history = ['redirect']
    elif case == 'wrong_content_type':
        headers['content-type'] = 'application/json'
    elif case == 'bad_status_type':
        status = True
    elif case == 'fragment':
        text = text.replace('rel="canonical" href="' + S, 'rel="canonical" href="' + S + '#fragment')
    elif case == 'base':
        text = text.replace('</head>', '<base href="' + T + '" /></head>')
    elif case == 'oversize':
        text += 'x' * 80_001
    elif case == 'unreadable':
        text = None
    observed = pages()[0]
    if case == 'unknown':
        del observed['hreflang_raw']
    def get(value, **kw):
        calls.append(value)
        assert kw['allow_redirects'] is False and kw['stream'] is True
        return SimpleNamespace(status_code=status, url=url, history=history, headers=headers,
            iter_content=lambda **k: [text.encode() if text is not None else b'\xff'], close=lambda: closed.append(True))
    monkeypatch.setattr(m, '_validate_public_crawl_target', lambda *a: '')
    monkeypatch.setattr(m.requests, 'get', get)
    assert m._hreflang_drop_page(S, S, observed) is (case == 'verified')
    assert calls == [S] and closed == [True]


@pytest.mark.parametrize('case', ['both', 'second_stale', 'second_probe', 'cap', 'partial_put'])
def test_multi_file_preflight_and_partial_writes_are_explicit(monkeypatch, case):
    s2, t2 = S + '2', T + '2'
    items = [ITEM, {'page': s2, 'field': 'fr', 'value': t2}]
    rows = pages() + [page(s2, 'fr', s2, [('fr', t2), ('en', t2)]), page(t2, 'en', t2, [('fr', s2), ('en', t2)])]
    sources = {'fr.html': raw(), 'fr2.html': raw().replace(S, s2).replace(T, t2)}
    if case == 'second_stale':
        sources['fr2.html'] = expected(sources['fr2.html']).replace('hreflang="fr"', 'hreflang="de"')
    writes, probes = [], []
    def get(api, **kw):
        return {'sha': 'original', 'encoding': 'base64', 'content': base64.b64encode(sources[api.split('/contents/', 1)[1]].encode()).decode()}
    def probe(url, canonical, observed):
        probes.append(url)
        return not (case == 'second_probe' and url == t2)
    def put(api, **kw):
        assert probes == [S, T, s2, t2] and kw['json_body']['sha'] == 'original'
        path = api.split('/contents/', 1)[1]
        if case == 'partial_put' and path == 'fr2.html':
            raise OSError('second write failed')
        writes.append(path)
        return {'content': {'sha': 'new'}}
    monkeypatch.setattr(m, '_github_api_get', get)
    monkeypatch.setattr(m, '_github_api_put', put)
    monkeypatch.setattr(m, '_hreflang_drop_page', probe)
    for name in ('_openai_generate_file_patch', '_ai_map_urls_to_files', '_ai_pick_repo_files', '_github_tarball_grep'):
        monkeypatch.setattr(m, name, lambda *a, **kw: pytest.fail('No model or heuristic fallback'))
    result = m._apply_prepared_issue_fix(owner='fixture', repo_name='fixture', branch='baseline', token='unused', fix_branch='qa',
        all_paths=list(sources), issue_key=KEY, issue_label=KEY, impacted=[S, s2], site_name='site.test', file_state={},
        max_files=1 if case == 'cap' else 2, prep=prepare(rows, items, [S, s2]), pages=rows,
        index=repo_index.build_repo_index(list(sources)), allow_ai_targeting=True)
    wanted = ['fr.html', 'fr2.html'] if case == 'both' else ['fr.html'] if case == 'partial_put' else []
    assert result['patched'] == writes == wanted and not result['ai_files']
    if case == 'partial_put':
        assert result['skipped'] == ['fr2.html']
    elif case in {'cap', 'second_stale'}:
        assert not probes
