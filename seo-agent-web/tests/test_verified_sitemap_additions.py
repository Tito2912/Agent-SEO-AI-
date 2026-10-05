"""Sitemap additions must prove indexability, not rewrite a complete file through AI."""

import base64
from urllib.parse import unquote

import pytest

from backend import app as m, repo_index
from tests.test_verified_sitemap_canonical import entry, sitemap
from tests.test_verified_sitemap_https import row, HTML, A, B

KEY = 'indexable_page_not_in_sitemap'


def prepare(rows, urls=None, paths=None):
    urls = [A] if urls is None else urls
    return m._prepare_issue_fix(issue_key=KEY, issues={KEY: {'count': len(urls), 'examples': urls}}, impacted=urls,
        all_paths=['sitemap.xml', 'a.html'] if paths is None else paths, site_name='site.test', owner='fixture',
        repo_name='fixture', branch='baseline', token='unused', pages=rows)


@pytest.mark.parametrize('change', [{'status_code': 404}, {'status_code': 301}, {'status_code': '200'},
    {'status_code': True}, {'status_code': 200.0}, {'content_type': 'application/json'}, {'error': 'timeout'},
    {'blocked_by_host': True}, {'final_url': B}, {'canonical': B}, {'meta_robots': 'noindex'}, {'meta_robots': 'none'},
    {'x_robots_tag': 'googlebot: noindex'}, {'redirect_chain': [A]}, {'redirect_statuses': [302]}])
def test_nonindexable_or_unproven_page_refuses(change):
    prep = prepare([row(**change)])
    assert prep['refusal'] and prep.get('sitemap_add_urls') == [] and not prep['rewriter_ai_fallback']


def test_missing_conflicting_or_indirect_observations_refuse():
    for rows in ([], [row(B, final_url=A)], [row(), row(status_code=404)], [row(), row(canonical=None)]):
        assert prepare(rows)['refusal']


@pytest.mark.parametrize('url', ['http://site.test/a', 'https://other.test/a', 'https://site.test/a#section',
    'https://user:secret@site.test/a', 'https://site.test:443/a', 'https://site.test/unknown'])
def test_foreign_ambiguous_or_unobserved_url_refuses(url):
    rows = [row()] if url.endswith('/unknown') else [row(url)]
    assert prepare(rows, [url])['refusal']


@pytest.mark.parametrize('paths', [[], ['app/sitemap.ts'], ['sitemap.xml', 'app/sitemap.ts'],
    ['sitemap.xml', 'backup-sitemap.xml'], ['archive/sitemap.xml'], ['static/sitemap.xml', 'hugo.toml']])
def test_generated_or_ambiguous_sitemap_refuses(paths):
    assert prepare([row()], paths=paths)['refusal']


def test_verified_subset_is_minimal_preserves_old_entries_and_names_refusals():
    prep = prepare([row(), row(B, meta_robots='noindex')], [A, B])
    assert not prep['refusal'] and prep.get('sitemap_add_urls') == [A] and not prep['rewriter_ai_fallback']
    assert B in prep['side_effects'] and prep['targets_override'] == ['sitemap.xml']
    old = entry(B, '<lastmod>2001-01-01</lastmod><priority>0.1</priority>')
    source = sitemap(old)
    result, count = prep['link_rewriter'](source)
    assert count == 1 and result == source.replace('</urlset>', entry(A) + '\r\n</urlset>')
    assert result.count('<lastmod>') == result.count('<priority>') == 1 and 'hreflang' not in result
    assert prep['link_rewriter'](result) == (result, 0)


@pytest.mark.parametrize('namespace', ['', ' xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"',
    ' xmlns:sm="http://www.sitemaps.org/schemas/sitemap/0.9"'])
def test_empty_literal_root_and_namespace_prefix_are_preserved(namespace):
    prefix = 'sm:' if ':sm=' in namespace else ''
    source = '<' + prefix + 'urlset' + namespace + '><!-- control --></' + prefix + 'urlset>'
    new, count = prepare([row()])['link_rewriter'](source)
    block = '<' + prefix + 'url><' + prefix + 'loc>' + A + '</' + prefix + 'loc></' + prefix + 'url>\n'
    assert (new, count) == (source.replace('</' + prefix + 'urlset>', block + '</' + prefix + 'urlset>'), 1)


def test_exact_case_query_slash_fragments_duplicates_comments_and_extensions_survive():
    url = A + '?x=1&y=2'
    source = sitemap(entry(A), entry(A), entry(A + '/'), entry(A + '#section'), entry('https://site.test/A'),
        entry(B, '<image xmlns="urn:image"><loc>' + url.replace('&', '&amp;') + '</loc></image>'))
    source = source.replace('</urlset>', '<!-- ' + entry(url) + ' -->\r\n</urlset>')
    new, count = prepare([row(url)], [url])['link_rewriter'](source)
    assert (new, count) == (source.replace('</urlset>', entry(url) + '\r\n</urlset>'), 1)
    assert prepare([row()])['link_rewriter'](source) == (source, 0)


@pytest.mark.parametrize('source', ['<urlset/>', '<urlset><url><loc>' + B + '</loc>',
    '<!DOCTYPE urlset><urlset></urlset>', '<sitemapindex></sitemapindex>', '<urlset xmlns="urn:wrong"></urlset>',
    '<urlset><url><loc>' + B + '</loc><loc>' + B + '</loc></url></urlset>',
    '<urlset><url><loc><![CDATA[' + B + ']]></loc></url></urlset>',
    '<urlset>not a literal sitemap<url><loc>' + B + '</loc></url></urlset>',
    '<urlset><url>unexpected text<loc>' + B + '</loc></url></urlset>'])
def test_unverifiable_xml_refuses_without_fallback(source):
    assert prepare([row()])['link_rewriter'](source) == (source, 0)


@pytest.mark.parametrize('case', ['verified', 'already_present', 'unobserved', 'changed_canonical', 'current_noindex',
    'current_none', 'current_refresh', 'current_script', 'ambiguous_route', 'missing_route', 'generated_package',
    'missing_sha', 'invalid_base64', 'changed_rules', 'wildcard_rules', 'toml_rules', 'fresh_http_refused', 'budget', 'second_stale'])
def test_pipeline_revalidates_current_sources_and_every_http_witness_before_one_put(monkeypatch, case):
    xml = sitemap(entry(B))
    sources = {'sitemap.xml': xml, 'a.html': HTML}
    rows, urls = [row()], [A]
    if case == 'already_present':
        sources['sitemap.xml'] = xml = sitemap(entry(A), entry(B))
    elif case == 'unobserved':
        rows = []
    elif case == 'changed_canonical':
        sources['a.html'] = HTML.replace(A, B)
    elif case in {'current_noindex', 'current_none'}:
        sources['a.html'] = HTML.replace('</head>', '<meta name="robots" content="' + ('none' if case == 'current_none' else 'noindex') + '" /></head>')
    elif case == 'current_refresh':
        sources['a.html'] = HTML.replace('</head>', '<meta http-equiv="refresh" content="0;url=/control" /></head>')
    elif case == 'current_script':
        sources['a.html'] = HTML.replace('</body>', '<script src="/seo.js"></script></body>')
    elif case == 'ambiguous_route':
        sources['a/index.html'] = HTML
    elif case == 'missing_route':
        del sources['a.html']
    elif case == 'generated_package':
        sources['package.json'] = '{"dependencies":{"next-sitemap":"4.0.0"}}'
    elif case in {'changed_rules', 'wildcard_rules'}:
        sources['_redirects'] = '/a /control 301\n' if case == 'changed_rules' else '/* /index.html 200\n'
    elif case == 'toml_rules':
        sources['netlify.toml'] = '[[redirects]]\nfrom="/a"\nto="/control"\nstatus=200\n'
    elif case == 'second_stale':
        sources['control.html'] = HTML.replace(A, B)
        sources['sitemap.xml'] = xml = sitemap()
        rows, urls = [row(), row(B)], [A, B]
    writes, probes = {}, []
    def get(api, **kw):
        name = unquote(api.split('/contents/', 1)[1])
        return {'sha': '' if case == 'missing_sha' else 'original', 'content': '!!!!' if case == 'invalid_base64'
                else base64.b64encode(sources[name].encode()).decode()}
    def put(api, **kw):
        assert probes == [(A, A)] and kw['json_body']['sha'] == 'original'
        writes[unquote(api.split('/contents/', 1)[1])] = base64.b64decode(kw['json_body']['content']).decode()
        return {'content': {'sha': 'patched'}}
    def probe(url, canonical):
        probes.append((url, canonical))
        return case != 'fresh_http_refused' and not (case == 'second_stale' and url == B)
    def forbidden(*a, **kw):
        pytest.fail('No model, inferred generator edit or heuristic target for verified additions')
    monkeypatch.setattr(m, '_github_api_get', get)
    monkeypatch.setattr(m, '_github_api_put', put)
    monkeypatch.setattr(m, '_sitemap_https_page', probe)
    for name in ('_openai_generate_file_patch', '_ai_map_urls_to_files', '_ai_pick_repo_files', '_github_tarball_grep'):
        monkeypatch.setattr(m, name, forbidden)
    result = m._apply_prepared_issue_fix(owner='fixture', repo_name='fixture', branch='baseline', token='unused', fix_branch='qa',
        all_paths=list(sources), issue_key=KEY, issue_label=KEY, impacted=urls, site_name='site.test', file_state={},
        max_files=0 if case == 'budget' else 1, prep=prepare(rows, urls, list(sources)), pages=rows,
        index=repo_index.build_repo_index(list(sources)))
    assert not result['ai_files']
    if case == 'verified':
        assert writes == {'sitemap.xml': xml.replace('</urlset>', entry(A) + '\r\n</urlset>')}
    else:
        assert not writes
    if case == 'second_stale':
        assert probes == [(A, A), (B, B)]
