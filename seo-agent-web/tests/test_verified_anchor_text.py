"""Anchor names require actual source, target, routing and fresh HTTPS witnesses."""
import base64
from types import SimpleNamespace

import pytest

from backend import app as m, repo_index
from tests.test_verified_sitemap_https import row

KEY = 'links_with_no_anchor_text'
S, T = 'https://site.test/', 'https://site.test/contact'
ITEM = {'page': S, 'field': '/contact', 'value': 'Contactez-nous'}
OPENING = '<a href="/contact">'
LINK = OPENING + '<svg class="icon" /></a>'


def source(url=S, target=False):
    return ('<!doctype html>\n<html lang="fr"><head>\n<title>'
        + ('Contact | Site' if target else 'Accueil | Site') + '</title>\n'
        + '<link rel="canonical" href="' + url + '" />\n</head><body><h1>'
        + ('Contactez-nous' if target else 'Accueil') + '</h1>\n'
        + ('<p>Nous contacter.</p>' if target else LINK + '\n<!-- ' + LINK + ' -->')
        + '\n</body></html>\n')


def page(url, target=False, **changes):
    item = {'source_url': url, 'target_url': T, 'rel': '', 'internal': True, 'anchor_text': '',
            'title': '', 'aria_label': '', 'href': '/contact'}
    return row(url, canonical=url, lang='fr', title='Contact | Site' if target else 'Accueil | Site',
        title_tag_count=1, h1=['Contactez-nous' if target else 'Accueil'], h1_tag_count=1,
        links_without_anchor_text=[] if target else [item], **changes)


def pages():
    return [page(S), page(T, True)]


def prepare(observations=None, items=None, impacted=None, key=KEY):
    return m._prepare_issue_fix(issue_key=key, issues={key: {'count': 1, 'examples': [S],
        'evidence': {'kind': 'page_values', 'items': [ITEM] if items is None else items}}},
        impacted=[S] if impacted is None else impacted, all_paths=['index.html', 'contact.html'],
        site_name='site.test', owner='fixture', repo_name='fixture', branch='baseline', token='unused',
        pages=pages() if observations is None else observations)


@pytest.mark.parametrize('key', [KEY, KEY + '_indexable', KEY + '_not_indexable', ' ' + KEY.upper() + ' '])
def test_verified_anchor_name_has_a_protected_plan_without_a_generic_callback(key):
    plan = prepare(key=key)
    assert not plan['refusal'] and plan.get('anchor_text_items') == [ITEM]
    assert plan['link_rewriter'] is None and not plan['rewriter_ai_fallback'] and not plan['rewriter_is_ai']


@pytest.mark.parametrize('which', [0, 1])
@pytest.mark.parametrize('change', [{'status_code': True}, {'status_code': '200'}, {'status_code': 404},
    {'content_type': 'application/json'}, {'error': 'timeout'}, {'blocked_by_host': True}, {'redirect_chain': [T]},
    {'redirect_statuses': [301]}, {'meta_robots': 'noindex'}, {'x_robots_tag': 'none'}, {'final_url': T + '?alias'},
    {'canonical': None}, {'canonical': T + '#fragment'}, {'url': S + '#fragment'}, {'final_url': None},
    {'lang': None}, {'title_tag_count': True}, {'h1_tag_count': True}, {'h1': 'Contactez-nous'}, {'title': False}])
def test_both_observations_are_typed_direct_indexable_and_self_canonical(which, change):
    rows = pages()
    rows[which].update(change)
    assert prepare(rows)['refusal']


@pytest.mark.parametrize('which', [0, 1])
@pytest.mark.parametrize('field', ['canonical', 'lang', 'title', 'title_tag_count', 'h1', 'h1_tag_count'])
def test_unknown_metadata_cannot_name_a_link(which, field):
    rows = pages()
    del rows[which][field]
    assert prepare(rows)['refusal']


@pytest.mark.parametrize('case', ['missing_target', 'target_alias_only', 'duplicate_disagrees', 'wrong_name',
    'source_links_unknown', 'source_links_empty', 'source_target_wrong', 'source_href_wrong', 'source_named',
    'source_external', 'source_identity_wrong', 'source_link_untyped', 'no_evidence', 'conflicting_names',
    'typed_name', 'typed_href', 'typed_page', 'empty_target', 'duplicate_title', 'fragment', 'http', 'foreign',
    'port', 'credentials', 'backslash', 'self_link'])
def test_unproved_evidence_or_target_names_are_refused(case):
    rows, items = pages(), [dict(ITEM)]
    if case == 'missing_target':
        rows.pop()
    elif case == 'target_alias_only':
        rows[1]['url'] = T + '?alias'
    elif case == 'duplicate_disagrees':
        rows.append(dict(rows[1], h1=['Autre nom']))
    elif case == 'source_links_unknown':
        del rows[0]['links_without_anchor_text']
    elif case == 'source_links_empty':
        rows[0]['links_without_anchor_text'] = []
    elif case.startswith('source_'):
        link = rows[0]['links_without_anchor_text'][0]
        key, value = {'source_target_wrong': ('target_url', T + '?other'), 'source_href_wrong': ('href', '../other'),
            'source_named': ('anchor_text', 'Contact'), 'source_external': ('internal', False),
            'source_identity_wrong': ('source_url', T), 'source_link_untyped': ('aria_label', False)}[case]
        link[key] = value
    elif case == 'wrong_name':
        items[0]['value'] = 'Autre nom'
    elif case == 'no_evidence':
        items = []
    elif case == 'conflicting_names':
        items.append(dict(ITEM, value='Autre nom'))
    elif case.startswith('typed_'):
        items[0][{'typed_name': 'value', 'typed_href': 'field', 'typed_page': 'page'}[case]] = 42
    elif case == 'empty_target':
        rows[1].update(h1=[], h1_tag_count=0, title=None, title_tag_count=0)
    elif case == 'duplicate_title':
        rows[1]['title_tag_count'] = 2
    elif case == 'self_link':
        items[0].update(field='/', value='Accueil')
        rows[0]['links_without_anchor_text'][0].update(href='/', target_url=S)
    else:
        items[0]['field'] = {'fragment': '/contact#form', 'http': 'http://site.test/contact',
            'foreign': 'https://other.test/contact', 'port': 'https://site.test:443/contact',
            'credentials': 'https://user:secret@site.test/contact', 'backslash': '/contact\\other'}[case]
    assert prepare(rows, items)['refusal']


def test_a_unique_title_is_the_fallback_when_h1_is_absent_or_multiple():
    for headings in ([], ['Deux', 'Noms']):
        rows = pages()
        rows[1].update(h1=headings, h1_tag_count=len(headings))
        plan = prepare(rows, [dict(ITEM, value='Contact | Site')])
        assert not plan['refusal'] and plan['anchor_text_items'] == [dict(ITEM, value='Contact | Site')]


def test_verified_subset_names_the_unproved_page_without_model_fallback():
    other = S + 'unproved'
    plan = prepare(items=[ITEM, dict(ITEM, page=other)], impacted=[S, other])
    assert not plan['refusal'] and plan['anchor_text_items'] == [ITEM]
    assert other in plan['side_effects'] and not plan['rewriter_ai_fallback']


@pytest.mark.parametrize('case', ['exact', 'single_quotes', 'crlf', 'entity', 'absolute_href', 'relative_href',
                                 'current_named', 'changed_source_title', 'different_source', 'canonical_fragment',
                                 'noindex', 'script', 'base', 'template', 'duplicate_attr', 'named_equivalent'])
def test_current_source_rewrite_is_exact_and_refuses_stale_or_ambiguous_markup(case):
    from backend import anchor_proof as a
    text, item, observed = source(), dict(ITEM), pages()[0]
    if case == 'single_quotes':
        text = text.replace('"', "'")
    elif case == 'crlf':
        text = text.replace('\n', '\r\n')
    elif case in {'absolute_href', 'relative_href'}:
        href = T if case == 'absolute_href' else './contact'
        text = text.replace('href="/contact"', 'href="' + href + '"')
        item['field'] = observed['links_without_anchor_text'][0]['href'] = href
    elif case == 'entity':
        href = '/contact?a=1&b=2'
        text = text.replace('href="/contact"', 'href="/contact?a=1&amp;b=2"')
        item['field'] = observed['links_without_anchor_text'][0]['href'] = href
        observed['links_without_anchor_text'][0]['target_url'] = S + 'contact?a=1&b=2'
    else:
        changes = {'current_named': (OPENING, OPENING[:-1] + ' title="Contact">'),
            'changed_source_title': ('Accueil | Site', 'Autre accueil'), 'different_source': (S + '"', S + 'other"'),
            'canonical_fragment': ('rel="canonical" href="' + S, 'rel="canonical" href="' + S + '#x'),
            'noindex': ('</head>', '<meta name="robots" content="noindex" /></head>'),
            'script': ('</body>', '<script src="/app.js"></script></body>'),
            'base': ('</head>', '<base href="' + S + '" /></head>'), 'template': ('</body>', '{{ value }}</body>'),
            'duplicate_attr': (OPENING, '<a href="/contact" href="/other">'),
            'named_equivalent': ('</body>', '<a href="' + T + '">Contact</a></body>')}
        if case in changes:
            text = text.replace(*changes[case], 1)
    output, count = a.rewrite(text, [item], observed, m._duplicate_html_document, m._verification_url)
    if case in {'exact', 'single_quotes', 'crlf', 'entity', 'absolute_href', 'relative_href'}:
        quote = "'" if case == 'single_quotes' else '"'
        actual = text.index('<a href=')
        end = text.index('>', actual)
        assert count == 1 and output == text[:end] + ' aria-label=' + quote + ITEM['value'] + quote + text[end:]
        assert a.rewrite(output, [item], observed, m._duplicate_html_document, m._verification_url) == (output, 0)
    else:
        assert (output, count) == (text, 0)


@pytest.mark.parametrize('which', ['source', 'target'])
@pytest.mark.parametrize('case', ['verified', 'changed_name', 'noindex', 'redirect', 'history', 'wrong_url',
                                 'json', 'oversize', 'bad_utf8', 'timeout', 'canonical_fragment'])
def test_fresh_probe_checks_the_actual_markup_and_always_closes(monkeypatch, which, case):
    url, observed = (S, pages()[0]) if which == 'source' else (T, pages()[1])
    text = source(url, target=which == 'target')
    if case == 'changed_name':
        text = text.replace('Contactez-nous' if which == 'target' else 'Accueil', 'Autre nom')
    elif case == 'oversize':
        text += 'x' * 80_001
    elif case == 'canonical_fragment':
        text = text.replace('rel="canonical" href="' + url, 'rel="canonical" href="' + url + '#x')
    closed, fetched = [], []
    headers = {'content-type': 'application/json' if case == 'json' else 'text/html'}
    if case == 'noindex':
        headers['x-robots-tag'] = 'noindex'
    response = SimpleNamespace(status_code=302 if case == 'redirect' else 200, url=T if case == 'wrong_url' and which == 'source' else S if case == 'wrong_url' else url,
        history=[object()] if case == 'history' else [], headers=headers, close=lambda: closed.append(True),
        iter_content=lambda chunk_size: iter([b'\xff' if case == 'bad_utf8' else text.encode()]))
    def get(value, **kw):
        assert kw == {'allow_redirects': False, 'stream': True, 'timeout': (5, 10)}
        fetched.append(value)
        if case == 'timeout':
            raise TimeoutError()
        return response
    monkeypatch.setattr(m, '_validate_public_crawl_target', lambda value: None)
    monkeypatch.setattr(m.requests, 'get', get)
    assert m._anchor_text_page(url, url, observed, [ITEM] if which == 'source' else []) is (case == 'verified')
    assert fetched == [url] and closed == ([] if case == 'timeout' else [True])


@pytest.mark.parametrize('case', ['verified', 'source_stale', 'target_stale', 'target_missing', 'target_shared',
    'target_ambiguous', 'source_ambiguous', 'source_sha', 'target_sha', 'target_encoding', 'target_routing',
    'source_probe', 'target_probe', 'put_failure', 'budget'])
def test_source_and_destination_repository_and_https_preflight_precede_the_only_put(monkeypatch, case):
    sources = {'index.html': source(), 'contact.html': source(T, True)}
    if case == 'source_stale':
        sources['index.html'] = sources['index.html'].replace(OPENING, OPENING[:-1] + ' title="Contact">')
    elif case == 'target_stale':
        sources['contact.html'] = sources['contact.html'].replace('Contactez-nous', 'Autre nom')
    elif case == 'target_missing':
        del sources['contact.html']
    elif case == 'target_shared':
        sources['src/components/contact.html'] = sources.pop('contact.html')
    elif case in {'target_ambiguous', 'source_ambiguous'}:
        sources['contact/index.html' if case == 'target_ambiguous' else 'public/index.html'] = source(T, True) if case == 'target_ambiguous' else source()
    elif case == 'target_routing':
        sources['_redirects'] = '/contact /elsewhere 301\n'
    reads, probes, writes = [], [], {}
    def get(api, **kw):
        path = api.split('/contents/', 1)[1]
        reads.append(path)
        return {'sha': '' if case == 'target_sha' and path == 'contact.html' or case == 'source_sha' and path == 'index.html' else 'original',
            'encoding': 'none' if case == 'target_encoding' and path == 'contact.html' else 'base64',
            'content': base64.b64encode(sources[path].encode()).decode()}
    def probe(url, canonical, observed, items):
        probes.append(url)
        assert canonical == url and observed in pages()
        assert items == ([ITEM] if url == S else [])
        return url != {'source_probe': S, 'target_probe': T}.get(case)
    def put(api, **kw):
        assert probes == [S, T] and set(reads) == {'index.html', 'contact.html'}
        assert kw['json_body']['sha'] == 'original'
        if case == 'put_failure':
            raise OSError('write failed')
        writes[api.split('/contents/', 1)[1]] = base64.b64decode(kw['json_body']['content']).decode()
        return {'content': {'sha': 'new'}}
    monkeypatch.setattr(m, '_github_api_get', get)
    monkeypatch.setattr(m, '_github_api_put', put)
    monkeypatch.setattr(m, '_anchor_text_page', probe, raising=False)
    for name in ('_openai_generate_file_patch', '_ai_map_urls_to_files', '_ai_pick_repo_files', '_github_tarball_grep'):
        monkeypatch.setattr(m, name, lambda *a, **kw: pytest.fail('No model or heuristic fallback'))
    result = m._apply_prepared_issue_fix(owner='fixture', repo_name='fixture', branch='baseline', token='unused', fix_branch='qa',
        all_paths=list(sources), issue_key=KEY, issue_label=KEY, impacted=[S], site_name='site.test', file_state={},
        max_files=0 if case == 'budget' else 1, prep=prepare(), pages=pages(), index=repo_index.build_repo_index(list(sources)))
    assert not result['ai_files'] and result['patched'] == (['index.html'] if case == 'verified' else [])
    assert writes == ({'index.html': source().replace(OPENING, OPENING[:-1] + ' aria-label="Contactez-nous">', 1)} if case == 'verified' else {})


def test_the_old_generic_callback_cannot_bypass_the_required_plan(monkeypatch):
    for name in ('_github_api_get', '_github_api_put', '_openai_generate_file_patch', '_ai_map_urls_to_files', '_ai_pick_repo_files'):
        monkeypatch.setattr(m, name, lambda *a, **kw: pytest.fail('A legacy callback must not reach GitHub or AI'))
    result = m._deep_patch_issue_files(owner='fixture', repo_name='fixture', branch='baseline', token='unused', fix_branch='qa',
        all_paths=['index.html'], issue_key=KEY, issue_label=KEY, impacted_urls=[S], site_name='site.test', file_state={},
        link_rewriter=lambda text: m._poser_aria_label_sur_liens_sans_ancre(text, [ITEM]))
    assert not result[0] and result[1] and not result[2] and not result[3]


@pytest.mark.parametrize('items', [[None], [{}], [False]])
def test_direct_literal_rewrite_refuses_malformed_items_without_raising(items):
    from backend import anchor_proof
    text = source()
    assert anchor_proof.rewrite(text, items, pages()[0], m._duplicate_html_document, m._verification_url) == (text, 0)


@pytest.mark.parametrize('case', ['two_sources', 'second_stale', 'second_probe', 'insufficient_cap', 'second_put_failure', 'cached_target_stale'])
def test_all_source_target_preflights_finish_before_a_multi_file_write(monkeypatch, case):
    other = S + 'other'
    items, rows = [ITEM, dict(ITEM, page=other)], pages() + [page(other)]
    sources = {'index.html': source(), 'other.html': source(other), 'contact.html': source(T, True)}
    if case == 'second_stale':
        sources['other.html'] = sources['other.html'].replace(OPENING, OPENING[:-1] + ' title="Contact">')
    elif case == 'cached_target_stale':
        sources['contact.html'] = sources['contact.html'].replace('Contactez-nous', 'Autre nom')
    probes, writes, reads = [], [], []
    def get(api, **kw):
        path = api.split('/contents/', 1)[1]
        reads.append(path)
        return {'sha': 'current', 'encoding': 'base64', 'content': base64.b64encode(sources[path].encode()).decode()}
    def probe(url, *a):
        probes.append(url)
        return not (case == 'second_probe' and url == other)
    def put(api, **kw):
        assert probes == [S, T, other, T] and set(reads) == set(sources)
        assert kw['json_body']['sha'] == 'current'
        path = api.split('/contents/', 1)[1]
        if case == 'second_put_failure' and path == 'other.html':
            raise OSError('partial write')
        writes.append(path)
        return {'content': {'sha': 'new'}}
    monkeypatch.setattr(m, '_github_api_get', get)
    monkeypatch.setattr(m, '_github_api_put', put)
    monkeypatch.setattr(m, '_anchor_text_page', probe)
    for method in ('_openai_generate_file_patch', '_ai_map_urls_to_files', '_ai_pick_repo_files'):
        monkeypatch.setattr(m, method, lambda *a, **kw: pytest.fail('No model targeting'))
    state = {'contact.html': {'content': source(T, True), 'sha': 'old-cached'}}
    result = m._apply_prepared_issue_fix(owner='fixture', repo_name='fixture', branch='baseline', token='unused', fix_branch='qa',
        all_paths=list(sources), issue_key=KEY, issue_label=KEY, impacted=[S, other], site_name='site.test', file_state=state,
        max_files=1 if case == 'insufficient_cap' else 2, prep=prepare(rows, items, [S, other]), pages=rows,
        index=repo_index.build_repo_index(list(sources)))
    expected = ['index.html', 'other.html'] if case == 'two_sources' else ['index.html'] if case == 'second_put_failure' else []
    assert result['patched'] == writes == expected and not result['ai_files']
    if case == 'second_put_failure':
        assert result['skipped'] == ['other.html']


@pytest.mark.parametrize('items', [[], {}, [None], [{'page': S}], [dict(ITEM, value=False)]])
def test_malformed_explicit_plans_fail_closed_without_a_model(monkeypatch, items):
    for method in ('_github_api_get', '_github_api_put', '_openai_generate_file_patch', '_ai_map_urls_to_files', '_ai_pick_repo_files'):
        monkeypatch.setattr(m, method, lambda *a, **kw: pytest.fail('Malformed plans must not reach I/O'))
    result = m._deep_patch_issue_files(owner='fixture', repo_name='fixture', branch='baseline', token='unused', fix_branch='qa',
        all_paths=['index.html', 'contact.html'], issue_key=KEY, issue_label=KEY, impacted_urls=[S], site_name='site.test',
        file_state={}, anchor_text_items=items, pages=pages())
    assert not result[0] and result[1] and not result[2] and not result[3]


@pytest.mark.parametrize('case', ['canonical_body', 'hidden_h1', 'hidden_parent', 'duplicate_title', 'empty_h1', 'unquoted_href'])
def test_literal_identity_and_names_cannot_come_from_an_ambiguous_document(case):
    from backend import anchor_proof as a
    raw = source(T, True)
    changes = {'canonical_body': ('<link rel="canonical" href="' + T + '" />', ''),
        'hidden_h1': ('<h1>', '<h1 hidden>'), 'hidden_parent': ('<body>', '<body hidden>'),
        'duplicate_title': ('</head>', '<title>Other</title></head>'), 'empty_h1': ('Contactez-nous</h1>', '</h1>')}
    if case == 'unquoted_href':
        raw = source().replace('href="/contact"', 'href=/contact')
        assert not a.matches(raw, S, pages()[0], m._duplicate_html_document, m._verification_url, [ITEM])
    else:
        raw = raw.replace(*changes[case], 1)
        if case == 'canonical_body':
            raw = raw.replace('</body>', '<link rel="canonical" href="' + T + '" /></body>')
        assert not a.matches(raw, T, pages()[1], m._duplicate_html_document, m._verification_url, [])
