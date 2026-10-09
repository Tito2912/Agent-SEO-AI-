"""A short description may select observed text, never invent a full-file repair."""
import base64
from types import SimpleNamespace

import pytest

from backend import app as m, repo_index
from tests.test_verified_anchor_text import page as anchor_page

KEY, S = 'meta_description_too_short', 'https://site.test/'
SHORT = 'Une description courte.'
FACT = 'Cette page explique comment verifier une meta description a partir du contenu publie, sans modifier le titre, les liens ni les autres balises du site.'


def source(url=S, absent=False):
    return ('<!doctype html>\n<html lang="fr"><head>\n<title>Accueil | Site</title>\n'
        + ('' if absent else '<meta name="description" content="' + SHORT + '" />\n')
        + '<link rel="canonical" href="' + url + '" />\n</head><body><h1>Accueil</h1>\n'
        + '<p>' + FACT + '</p>\n<!-- <meta name="description" content="' + SHORT + '" /> -->\n</body></html>\n')


def page(url=S, absent=False):
    result = anchor_page(url)
    result.update(meta_description=None if absent else SHORT, meta_description_tag_count=0 if absent else 1)
    return result


def prepare(rows=None, samples=None, key=KEY, impacted=None):
    samples = {S: {'rendered': SHORT, 'len': len(SHORT)}} if samples is None else samples
    return m._prepare_issue_fix(issue_key=key, issues={key: {'count': 1, 'examples': [S], 'length_samples': samples}},
        impacted=[S] if impacted is None else impacted, all_paths=['index.html'], site_name='site.test',
        owner='fixture', repo_name='fixture', branch='baseline', token='unused', pages=[page()] if rows is None else rows)


@pytest.mark.parametrize('key', [KEY, KEY + '_indexable', KEY + '_not_indexable', ' ' + KEY.upper() + ' '])
def test_short_description_uses_an_explicit_ai_selection_plan_not_a_freeform_callback(key):
    plan = prepare(key=key)
    assert not plan['refusal'] and plan['short_description_urls'] == [S]
    assert plan['link_rewriter'] is None and plan['rewriter_is_ai'] and not plan['rewriter_ai_fallback']


@pytest.mark.parametrize('change', [{'status_code': True}, {'status_code': '200'}, {'status_code': 404},
    {'content_type': 'application/json'}, {'canonical': None}, {'canonical': S + '#x'}, {'final_url': None},
    {'final_url': S + '?other'}, {'lang': None}, {'h1_tag_count': True}, {'title_tag_count': True},
    {'meta_description_tag_count': True}, {'meta_description_tag_count': 2}, {'meta_description': False},
    {'meta_description': 'Changed'}, {'meta_description': 'x' * 100}, {'x_robots_tag': 'noindex'},
    {'meta_robots': 'none'}, {'error': 'fetch failed'}, {'blocked_by_host': True}, {'redirect_chain': [S]}])
def test_incoherent_or_unproved_page_metadata_refuses(change):
    observed = page()
    observed.update(change)
    assert prepare([observed])['refusal']


@pytest.mark.parametrize('field', ['lang', 'title', 'title_tag_count', 'h1', 'h1_tag_count',
                                  'meta_description', 'meta_description_tag_count'])
def test_unknown_snapshot_fields_refuse(field):
    observed = page()
    del observed[field]
    assert prepare([observed])['refusal']


@pytest.mark.parametrize('sample', [None, {}, {'rendered': SHORT, 'len': True}, {'rendered': SHORT, 'len': '23'},
    {'rendered': 42, 'len': 2}, {'rendered': SHORT, 'len': 1}, {'rendered': 'x' * 99, 'len': 99},
    {'rendered': 'x' * 100, 'len': 100}])
def test_length_sample_is_typed_complete_and_matches_the_actual_description(sample):
    assert prepare(samples={S: sample})['refusal']


def test_absent_description_and_proved_subset_have_explicit_plans():
    plan = prepare([page(absent=True)], {S: {'rendered': '', 'len': 0}}, impacted=[S, S + 'unknown'])
    assert not plan['refusal'] and plan['short_description_urls'] == [S]
    assert S + 'unknown' in plan['side_effects'] and not plan['rewriter_ai_fallback']


@pytest.mark.parametrize('bad', ['alias', 'fragment', 'duplicate_disagrees', 'unobserved'])
def test_requested_exact_identity_and_duplicate_coherence_are_required(bad):
    rows = [page()]
    if bad == 'unobserved':
        rows = []
    elif bad == 'duplicate_disagrees':
        rows.append(dict(rows[0], meta_description='Other'))
    else:
        rows[0]['url'] += '?alias' if bad == 'alias' else '#fragment'
    assert prepare(rows)['refusal']


@pytest.mark.parametrize('absent', [False, True])
@pytest.mark.parametrize('case', ['exact', 'crlf', 'single_quotes', 'entity', 'description_changed', 'title_changed',
    'canonical_fragment', 'script', 'base', 'duplicate_description', 'body_meta', 'unquoted', 'data_content',
    'hidden_text', 'navigation_text', 'no_candidate', 'invented_value', 'oversize', 'candidate_over_ceiling'])
def test_only_a_literal_description_receives_an_existing_complete_text_excerpt(absent, case):
    from backend import short_description as a
    raw, observed, value = source(absent=absent), page(absent=absent), FACT
    if case == 'crlf':
        raw = raw.replace('\n', '\r\n')
    elif case == 'single_quotes':
        raw = raw.replace('"', "'")
    elif case == 'entity':
        value = FACT.replace('le titre', 'titre & texte')
        raw = raw.replace(FACT, value.replace('&', '&amp;'))
    elif case == 'candidate_over_ceiling':
        value = FACT + ' Texte ajoute.'
        raw = raw.replace(FACT, value)
    elif case == 'invented_value':
        value = FACT.replace('le titre', 'les prix garantis')
    else:
        changes = {'description_changed': ('content="' + SHORT, 'content="Changed'),
            'title_changed': ('Accueil | Site', 'Other title'), 'canonical_fragment': ('href="' + S, 'href="' + S + '#x'),
            'script': ('</body>', '<script src="/app.js"></script></body>'),
            'base': ('</head>', '<base href="' + S + '" /></head>'),
            'duplicate_description': ('</head>', '<meta name="description" content="Other" /></head>'),
            'body_meta': ('<meta name="description" content="' + SHORT + '" />\n', ''),
            'unquoted': ('content="' + SHORT + '"', 'content=short'),
            'data_content': ('content="' + SHORT + '"', 'data-content="' + SHORT + '"'),
            'hidden_text': ('<p>', '<p hidden>'), 'navigation_text': ('<p>' + FACT + '</p>', '<nav><p>' + FACT + '</p></nav>'),
            'no_candidate': (FACT, 'Un texte court.')}
        if case in changes:
            raw = raw.replace(*changes[case], 1)
        if case == 'body_meta':
            raw = raw.replace('</body>', '<meta name="description" content="' + SHORT + '" /></body>')
        if case == 'oversize':
            raw += 'x' * 80_001
    output, count = a.rewrite(raw, observed, value, m._duplicate_html_document, m._verification_url)
    valid = case in {'exact', 'crlf', 'single_quotes', 'entity'}
    if absent and case in {'description_changed', 'unquoted', 'data_content'}:
        valid = True
    if valid:
        import html
        quote = "'" if case == 'single_quotes' else '"'
        escaped = html.escape(value, quote=True)
        if absent:
            newline = '\r\n' if case == 'crlf' else '\n'
            expected = raw.replace('</head>', '<meta name="description" content="' + escaped + '" />' + newline + '</head>', 1)
        else:
            expected = raw.replace('content=' + quote + SHORT + quote, 'content=' + quote + escaped + quote, 1)
        assert (output, count) == (expected, 1)
        assert a.rewrite(output, observed, value, m._duplicate_html_document, m._verification_url) == (output, 0)
    else:
        assert (output, count) == (raw, 0)


@pytest.mark.parametrize('answer', [{'candidate': 0}, {'candidate': True}, {'candidate': '0'}, {'candidate': -1},
    {'candidate': 99}, {'candidate': 0, 'value': 'invented'}, {'value': FACT}, None, []])
def test_model_selects_a_typed_existing_candidate_without_authoring_any_text(monkeypatch, answer):
    calls = []
    def ai(**kw):
        import json
        calls.append(json.loads(kw['user_msg']))
        return answer
    monkeypatch.setattr(m, '_correction_ai_json', ai)
    value = m._short_description_value(source(), page())
    assert value == (FACT if answer == {'candidate': 0} and type(answer['candidate']) is int else '')
    assert len(calls) == 1 and calls[0]['candidates'] == [FACT]
    assert calls[0]['lang'] == 'fr' and calls[0]['current'] == SHORT


@pytest.mark.parametrize('case', ['verified', 'stale_description', 'stale_context', 'noindex', 'redirect', 'bad_utf8', 'oversize'])
def test_fresh_https_attests_the_actual_candidate_context_and_closes(monkeypatch, case):
    from backend import short_description as a
    raw, observed = source(), page()
    expected = a.document(raw, S, m._duplicate_html_document, m._verification_url)
    actual = raw.replace(SHORT, 'Other', 1) if case == 'stale_description' else raw.replace(FACT, FACT.replace('titre', 'titre actuel')) if case == 'stale_context' else raw
    if case == 'oversize':
        actual += 'x' * 80_001
    closed = []
    response = SimpleNamespace(status_code=302 if case == 'redirect' else 200, url=S, history=[],
        headers={'content-type': 'text/html', **({'x-robots-tag': 'noindex'} if case == 'noindex' else {})},
        iter_content=lambda chunk_size: iter([b'\xff' if case == 'bad_utf8' else actual.encode()]), close=lambda: closed.append(True))
    monkeypatch.setattr(m, '_validate_public_crawl_target', lambda url: None)
    monkeypatch.setattr(m.requests, 'get', lambda *args, **kw: response)
    assert m._short_description_page(S, observed, expected) is (case == 'verified')
    assert closed == [True]


@pytest.mark.parametrize('case', ['verified', 'no_plan', 'source_stale', 'source_probe', 'model_invalid', 'ambiguous',
    'shared', 'routing', 'no_sha', 'no_budget', 'cache_stale', 'put_failure', 'existing_duplicate'])
def test_current_route_source_and_https_precede_ai_and_a_sha_guarded_put(monkeypatch, case):
    raw, calls, writes = source(), [], {}
    sources = {'index.html': raw}
    if case in {'source_stale', 'cache_stale'}:
        sources['index.html'] = raw.replace(SHORT, 'Changed', 1)
    elif case == 'ambiguous':
        sources['public/index.html'] = raw
    elif case == 'shared':
        sources = {'src/components/index.html': raw}
    elif case == 'routing':
        sources['_redirects'] = '/ /other 301\n'
    def get(api, **kw):
        path = api.split('/contents/', 1)[1]
        calls.append(('read', path))
        return {'sha': '' if case == 'no_sha' else 'current', 'content': base64.b64encode(sources[path].encode()).decode()}
    def probe(*args):
        calls.append(('probe', S))
        return case != 'source_probe'
    def ai(**kw):
        assert calls[-1] == ('probe', S)
        calls.append(('ai', S))
        return {'candidate': True if case == 'model_invalid' else 0}
    def put(api, **kw):
        assert calls[-1] == ('ai', S) and kw['json_body']['sha'] == 'current'
        if case == 'put_failure':
            raise OSError('write failed')
        writes['index.html'] = base64.b64decode(kw['json_body']['content']).decode()
        return {'content': {'sha': 'new'}}
    monkeypatch.setattr(m, '_github_api_get', get)
    monkeypatch.setattr(m, '_github_api_put', put)
    monkeypatch.setattr(m, '_short_description_page', probe, raising=False)
    monkeypatch.setattr(m, '_correction_ai_json', ai)
    for name in ('_openai_generate_file_patch', '_ai_map_urls_to_files', '_ai_pick_repo_files'):
        monkeypatch.setattr(m, name, lambda *a, **kw: pytest.fail('No free-form or targeting model'))
    rows = [page()]
    if case == 'existing_duplicate':
        existing = page(S + 'existing')
        existing['meta_description'] = FACT
        rows.append(existing)
    plan = prepare(rows)
    if case == 'no_plan':
        plan.pop('short_description_urls', None)
    result = m._apply_prepared_issue_fix(owner='fixture', repo_name='fixture', branch='baseline', token='unused', fix_branch='qa',
        all_paths=list(sources), issue_key=KEY, issue_label=KEY, impacted=[S], site_name='site.test',
        file_state={'index.html': {'content': raw, 'sha': 'cached'}} if case == 'cache_stale' else {},
        max_files=0 if case == 'no_budget' else 1, prep=plan, pages=rows, index=repo_index.build_repo_index(list(sources)))
    assert result['patched'] == (['index.html'] if case == 'verified' else [])
    assert result['ai_files'] == result['patched']
    assert writes == ({'index.html': raw.replace('content="' + SHORT + '"', 'content="' + FACT + '"', 1)} if case == 'verified' else {})
    if case not in {'verified', 'model_invalid', 'put_failure'}:
        assert not any(kind == 'ai' for kind, _ in calls)


@pytest.mark.parametrize('collision', [False, True])
def test_all_lot_probes_and_ai_selections_finish_before_any_write(monkeypatch, collision):
    other, events = S + 'other', []
    rows, sources = [page(), page(S + 'other')], {'index.html': source(), 'other.html': source(S + 'other')}
    if not collision:
        sources['other.html'] = sources['other.html'].replace('les liens', 'la navigation')
    samples = {row['url']: {'rendered': SHORT, 'len': len(SHORT)} for row in rows}
    monkeypatch.setattr(m, '_github_api_get', lambda api, **kw: {'sha': 'current', 'content': base64.b64encode(sources[api.split('/contents/', 1)[1]].encode()).decode()})
    def probe(url, *args):
        events.append(('probe', url))
        return True
    def ai(**kw):
        events.append(('ai', len(events)))
        return {'candidate': 0}
    def put(api, **kw):
        assert [kind for kind, _ in events[:4]] == ['probe', 'probe', 'ai', 'ai']
        events.append(('put', api))
        return {'content': {'sha': 'new'}}
    monkeypatch.setattr(m, '_short_description_page', probe, raising=False)
    monkeypatch.setattr(m, '_correction_ai_json', ai)
    monkeypatch.setattr(m, '_github_api_put', put)
    plan = prepare(rows, samples, impacted=[S, other])
    result = m._apply_prepared_issue_fix(owner='fixture', repo_name='fixture', branch='baseline', token='unused', fix_branch='qa',
        all_paths=list(sources), issue_key=KEY, issue_label=KEY, impacted=[S, other], site_name='site.test', file_state={},
        max_files=2, prep=plan, pages=rows, index=repo_index.build_repo_index(list(sources)))
    assert result['patched'] == result['ai_files'] == ([] if collision else list(sources))
    if collision:
        assert [kind for kind, _ in events] == ['probe', 'probe', 'ai']


def test_requesting_long_descriptions_cannot_rewrite_short_siblings_via_the_old_callback(monkeypatch):
    long_key = 'meta_description_too_long'
    issues = {KEY: {'length_samples': {S: {'rendered': SHORT, 'len': len(SHORT)}}},
              long_key: {'length_samples': {S + 'long': {'rendered': 'x' * 200, 'len': 200}}}}
    plan = m._prepare_issue_fix(issue_key=long_key, issues=issues, impacted=[S, S + 'long'],
        all_paths=['index.html', 'long.html'], site_name='site.test', owner='fixture', repo_name='fixture',
        branch='baseline', token='unused', pages=[page()])
    monkeypatch.setattr(m, '_length_value_for_page', lambda **kw: pytest.fail('A short sibling needs its protected plan'))
    assert not plan['rewriter_ai_fallback'] and plan['link_rewriter'](source()) == (source(), 0)


@pytest.mark.parametrize('sample', [None, {}, {'rendered': SHORT, 'len': len(SHORT)},
    {'rendered': 'x' * 200, 'len': True}, {'rendered': 'x' * 200, 'len': '200'},
    {'rendered': SHORT, 'len': 200}, 'empty_report'])
def test_unproved_long_requests_cannot_reach_freeform_short_repairs(monkeypatch, sample):
    key = 'meta_description_too_long'
    issues = {} if sample == 'empty_report' else {key: {'count': 1, 'length_samples': {S: sample}}}
    plan = m._prepare_issue_fix(issue_key=key, issues=issues, impacted=[S], all_paths=['index.html'],
        site_name='site.test', owner='fixture', repo_name='fixture', branch='qa', token='unused', pages=[page()])
    assert plan['refusal'] and not plan['rewriter_ai_fallback']
    for name in ('_github_api_get', '_github_api_put', '_openai_generate_file_patch', '_length_value_for_page'):
        monkeypatch.setattr(m, name, lambda *a, **kw: pytest.fail('Refuse an unproved long plan before I/O or AI'))
    result = m._apply_prepared_issue_fix(owner='fixture', repo_name='fixture', branch='qa', token='unused',
        fix_branch='fix', all_paths=['index.html'], issue_key=key, issue_label=key, impacted=[S], site_name='site.test',
        file_state={}, max_files=1, prep=plan, pages=[page()], index=repo_index.build_repo_index(['index.html']))
    assert not result['patched'] and not result['ai_files']


@pytest.mark.parametrize('separator', ['\r', '\r\n', '\u2028', '\u2029', '\x85'])
def test_missing_tag_uses_the_real_parser_offset_not_splitlines(separator):
    from backend import short_description as a
    raw = source(absent=True).replace('\n', separator)
    output, count = a.rewrite(raw, page(absent=True), FACT, m._duplicate_html_document, m._verification_url)
    tag = '<meta name="description" content="' + FACT + '" />'
    expected = raw.replace('</head>', tag + ('\r\n' if separator == '\r\n' else '') + '</head>', 1)
    assert (output, count) == (expected, 1)
    assert output.index('name="description"') < output.index('</head>')


@pytest.mark.parametrize('case', ['late_probe', 'late_source', 'late_selection', 'cap', 'partial_put'])
def test_lot_failures_preflight_without_writes_or_report_partial_put_explicitly(monkeypatch, case):
    other, events = S + 'other', []
    rows = [page(), page(other)]
    sources = {'index.html': source(), 'other.html': source(other).replace('les liens', 'la navigation')}
    if case == 'late_source':
        sources['other.html'] = sources['other.html'].replace(SHORT, 'Changed', 1)
    monkeypatch.setattr(m, '_github_api_get', lambda api, **kw: {'sha': 'current', 'content': base64.b64encode(
        sources[api.split('/contents/', 1)[1]].encode()).decode()})
    def probe(url, *args):
        events.append('probe')
        return not (case == 'late_probe' and url == other)
    def ai(**kw):
        events.append('ai')
        return {'candidate': True if case == 'late_selection' and events.count('ai') == 2 else 0}
    def put(api, **kw):
        events.append('put')
        if case == 'partial_put' and events.count('put') == 2:
            raise OSError('write failed')
        return {'content': {'sha': 'new'}}
    monkeypatch.setattr(m, '_short_description_page', probe)
    monkeypatch.setattr(m, '_correction_ai_json', ai)
    monkeypatch.setattr(m, '_github_api_put', put)
    samples = {row['url']: {'rendered': SHORT, 'len': len(SHORT)} for row in rows}
    result = m._apply_prepared_issue_fix(owner='fixture', repo_name='fixture', branch='baseline', token='unused',
        fix_branch='qa', all_paths=list(sources), issue_key=KEY, issue_label=KEY, impacted=[S, other],
        site_name='site.test', file_state={}, max_files=1 if case == 'cap' else 2,
        prep=prepare(rows, samples, impacted=[S, other]), pages=rows, index=repo_index.build_repo_index(list(sources)))
    assert result['patched'] == result['ai_files'] == (['index.html'] if case == 'partial_put' else [])
    if case == 'partial_put':
        assert result['skipped'] == ['other.html'] and events == ['probe', 'probe', 'ai', 'ai', 'put', 'put']
    else:
        assert 'put' not in events
        if case != 'late_selection':
            assert 'ai' not in events
