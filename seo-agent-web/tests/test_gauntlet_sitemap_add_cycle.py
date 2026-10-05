"""Missing-page sitemap acceptance cannot pass by losing pages or hiding other defects."""

import base64
import copy

import pytest

from ops.gauntlet import sitemap_add_cycle as b
from ops.gauntlet.ai_budget import ClaudeBudget
from tests.test_gauntlet_sitemap_https_cycle import reports as previous_reports


def source():
    blocks = [b.entry(b.SITE + 'gauntlet/'), b.entry(b.previous.MASTER, b.previous.GOOD_META),
              b.entry(b.previous.CONTROL, b.previous.OLD_META)]
    blocks += [b.entry(url) for url in sorted(b.previous.NEGATIVES)]
    blocks += [b.entry(b.SITE + 'control-' + str(i)) for i in range(44)]
    return '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\r\n' + '\r\n'.join(blocks) + '\r\n</urlset>\r\n'


def reports():
    _, after = previous_reports()
    pages = b.live._html_pages(after)
    targets = {url.removeprefix(b.SITE.rstrip('/')): url for url in b.TARGETS}
    extra, replaced = [], set()
    for route, url in targets.items():
        if route in pages:
            old = pages[route]
            replaced.add(old['url'])
        else:
            old = next(row for row in after['pages'] if row['status_code'] == 200 and row['url'] not in replaced)
            replaced.add(old['url'])
        extra.append(dict(old, url=url, final_url=url, status_code=200, canonical=url, meta_robots=None,
                          x_robots_tag=None, redirect_chain=[], redirect_statuses=[]))
    after['pages'] = [row for row in after['pages'] if row['url'] not in replaced] + extra
    after['issues'][b.KEY] = {'count': 0, 'examples': []}
    before = copy.deepcopy(after)
    before['issues'][b.KEY] = {'count': 3, 'examples': sorted(b.TARGETS)}
    return before, after


def test_nonzero_provider_budget_refuses_before_remote_io(tmp_path):
    with pytest.raises(ValueError):
        b.cycle(None, 'unused', tmp_path, ClaudeBudget(1))
    assert not list(tmp_path.iterdir())


def test_setup_removes_only_three_entries_and_repair_adds_only_minimal_loc_nodes():
    from backend import app as m, sitemap_rewrite
    original = source()
    before = b.setup_source(original)
    urls = sorted(b.TARGETS)
    expected = b.expected_source(before, urls)
    assert len(b.locs(before)) == 46 and len(b.locs(expected)) == 49
    assert sitemap_rewrite.add_urls(before, urls, m._verification_url) == (expected, 3)
    assert all(b.entry(url) in expected for url in b.TARGETS | b.previous.NEGATIVES)
    assert '<lastmod>' not in expected and '<priority>' not in expected and 'hreflang' not in expected


@pytest.mark.parametrize('bad', ['count', 'missing', 'duplicate', 'negative', 'metadata', 'namespace'])
def test_drifted_setup_refuses(bad):
    value = source()
    if bad == 'count':
        value = value.replace('</urlset>', b.entry(b.SITE + 'extra') + '</urlset>')
    elif bad == 'missing':
        value = value.replace(b.previous.MASTER, b.SITE + 'other')
    elif bad == 'duplicate':
        value = value.replace(b.SITE + 'control-0', b.previous.MASTER)
    elif bad == 'negative':
        value = value.replace(next(iter(b.previous.NEGATIVES)), b.SITE + 'other')
    elif bad == 'metadata':
        value = value.replace(b.previous.GOOD_META, b.previous.OLD_META)
    else:
        value = value.replace('http://www.sitemaps.org/schemas/sitemap/0.9', 'urn:wrong')
    with pytest.raises(ValueError):
        b.setup_source(value)


@pytest.mark.parametrize('bad', ['count', 'already_present', 'wrong_targets', 'duplicate_targets'])
def test_drifted_expected_source_refuses(bad):
    value = b.setup_source(source())
    urls = sorted(b.TARGETS)
    if bad == 'count':
        value = value.replace('</urlset>', b.entry(b.SITE + 'extra') + '</urlset>')
    elif bad == 'already_present':
        value = value.replace(b.SITE + 'control-0', urls[0])
    elif bad == 'wrong_targets':
        urls[0] = b.SITE + 'other'
    else:
        urls += [urls[0]]
    with pytest.raises(ValueError):
        b.expected_source(value, urls)


@pytest.mark.parametrize('bad', [None, 'file', 'main', 'branch', 'retry', 'sha', 'bytes', 'base64'])
def test_write_guard_requires_exact_qa_scope_sha_and_bytes(bad):
    path, branch, attempts = 'sitemap.xml', b.PREFIX + 'correction-test', 0
    body = {'branch': branch, 'sha': 'original', 'content': base64.b64encode(b'expected').decode()}
    if bad == 'file':
        path = 'index.html'
    elif bad == 'main':
        branch = body['branch'] = 'main'
    elif bad == 'branch':
        body['branch'] += 'other'
    elif bad == 'retry':
        attempts = 1
    elif bad == 'sha':
        body['sha'] = 'stale'
    elif bad == 'bytes':
        body['content'] = base64.b64encode(b'collateral').decode()
    elif bad == 'base64':
        body['content'] = '!!!!'
    assert b.write_allowed(path, body, branch, attempts, 'expected', 'original') is (bad is None)


def test_measured_zero_preserves_healthy_pages_and_previous_negative_controls():
    before, after = reports()
    result = b.checks(before, after)
    assert result['target_counts'] == {b.KEY: [3, 0]} and result['html_routes'] == [52, 52]
    assert not result['increased_counts'] and result['previous_repairs_and_negative_controls_preserved']


@pytest.mark.parametrize('bad', ['false_zero', 'lost_page', 'title', 'noindex', 'redirect', 'status', 'canonical',
    'old_count', 'old_examples', 'incomplete', 'settings', 'new_issue'])
def test_false_success_or_collateral_change_refuses(bad):
    before, after = reports()
    target = next(row for row in after['pages'] if row['url'] in b.TARGETS)
    if bad == 'false_zero':
        after['issues'][b.KEY] = {'count': 1, 'examples': [target['url']]}
    elif bad == 'lost_page':
        after['pages'].remove(target)
    elif bad == 'title':
        target['title'] = 'changed'
    elif bad == 'noindex':
        target['meta_robots'] = 'noindex'
    elif bad == 'redirect':
        target['redirect_chain'] = [target['url']]
    elif bad == 'status':
        target['status_code'] = 404
    elif bad == 'canonical':
        target['canonical'] = b.SITE + 'other'
    elif bad == 'old_count':
        after['issues'][b.previous.KEY]['count'] = 0
    elif bad == 'old_examples':
        after['issues'][b.previous.KEY]['examples'] = ['other']
    elif bad == 'incomplete':
        after['meta']['urls_uncrawled'] = 1
    elif bad == 'settings':
        after['meta']['max_pages'] = 89
    else:
        after['issues']['new_issue'] = {'count': 1, 'examples': ['new']}
    with pytest.raises(ValueError):
        b.checks(before, after)


@pytest.mark.parametrize('scheme,www', [('http', ''), ('http', 'www.'), ('https', 'www.')])
def test_only_exact_owned_root_probe_hostname_may_vary(scheme, www):
    before, after = reports()
    bases = [f'https://deploy-preview-{number}--{b.REPO}.netlify.app/' for number in (73, 74)]
    for report, base in zip((before, after), bases):
        report['issues']['alias_probe'] = {'count': 1, 'examples': [base.replace('https://', scheme + '://' + www)]}
    with pytest.raises(ValueError, match='other_issue_counts_or_examples_changed'):
        b.checks(before, after)
    result = b.checks(before, after, before_preview=bases[0], after_preview=bases[1])
    assert result['owned_preview_root_examples_normalized']


@pytest.mark.parametrize('bad', ['scheme', 'www', 'path', 'query', 'fragment', 'host', 'port', 'credentials',
    'foreign_preview', 'count', 'production_url', 'duplicate'])
def test_preview_normalization_cannot_hide_changed_probe_or_production_examples(bad):
    before, after = reports()
    bases = [f'https://deploy-preview-{number}--{b.REPO}.netlify.app/' for number in (73, 74)]
    for report, base in zip((before, after), bases):
        report['issues']['alias_probe'] = {'count': 1, 'examples': [base.replace('https://', 'http://')]}
    value = after['issues']['alias_probe']['examples'][0]
    if bad == 'count':
        after['issues']['alias_probe']['count'] = 2
    elif bad == 'duplicate':
        after['issues']['alias_probe']['examples'].append(value)
    elif bad == 'production_url':
        before['issues']['alias_probe']['examples'] = [b.SITE + 'old']
        after['issues']['alias_probe']['examples'] = [b.SITE + 'new']
    else:
        replacements = {'scheme': value.replace('http://', 'https://'), 'www': value.replace('http://', 'http://www.'),
            'path': value + 'different', 'query': value + '?different', 'fragment': value + '#different',
            'host': value.replace(b.REPO, 'foreign'), 'port': value.replace('.app/', '.app:80/'),
            'credentials': value.replace('http://', 'http://user@'), 'foreign_preview': value.replace('-74--', '-75--')}
        after['issues']['alias_probe']['examples'] = [replacements[bad]]
    with pytest.raises(ValueError, match='other_issue_counts_or_examples_changed'):
        b.checks(before, after, before_preview=bases[0], after_preview=bases[1])


@pytest.mark.parametrize('bad', ['one_missing', 'identical', 'http', 'foreign', 'credentials', 'port', 'path', 'query',
    'fragment', 'not_numeric', 'leading_zero', 'zero'])
def test_untrusted_or_incomplete_preview_context_refuses(bad):
    before, after = reports()
    before['issues']['alias_probe'] = {'count': 1, 'examples': ['http://deploy-preview-73--' + b.REPO + '.netlify.app/']}
    after['issues']['alias_probe'] = {'count': 1, 'examples': ['http://deploy-preview-74--' + b.REPO + '.netlify.app/']}
    left, right = [f'https://deploy-preview-{number}--{b.REPO}.netlify.app/' for number in (73, 74)]
    contexts = {'one_missing': '', 'identical': left, 'http': right.replace('https://', 'http://'),
        'foreign': right.replace(b.REPO, 'foreign'), 'credentials': right.replace('https://', 'https://user@'),
        'port': right.replace('.app/', '.app:443/'), 'path': right + 'other', 'query': right + '?other',
        'fragment': right + '#other', 'not_numeric': right.replace('-74--', '-text--'),
        'leading_zero': right.replace('-74--', '-074--'), 'zero': right.replace('-74--', '-0--')}
    with pytest.raises(ValueError):
        b.checks(before, after, before_preview=left, after_preview=contexts[bad])
