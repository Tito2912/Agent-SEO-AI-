"""An owned HTTPS sitemap benchmark must keep refused URLs and collateral fields."""

import base64
import copy

import pytest

from ops.gauntlet import sitemap_https_cycle as b
from ops.gauntlet.ai_budget import ClaudeBudget
from tests.test_gauntlet_sitemap_error_cycle import reports as previous_reports


def source():
    urls = [b.HTTP + 'gauntlet/', b.SITE + 'gauntlet/', b.MASTER, b.CONTROL]
    urls += [b.SITE + 'control-' + str(i) for i in range(44)]
    return '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\r\n' + '\r\n'.join(b.entry(u) for u in urls) + '\r\n</urlset>\r\n'


def reports():
    _, after = previous_reports()
    before = copy.deepcopy(after)
    before['issues'][b.KEY] = {'count': 5, 'examples': sorted(b.TARGETS | b.NEGATIVES)}
    after['issues'][b.KEY] = {'count': 2, 'examples': sorted(b.NEGATIVES)}
    return before, after


def test_nonzero_budget_refuses_before_remote_io(tmp_path):
    with pytest.raises(ValueError):
        b.cycle(None, 'unused', tmp_path, ClaudeBudget(1))
    assert not list(tmp_path.iterdir())


def test_setup_and_expected_xml_preserve_metadata_and_negative_entries():
    from backend import app as m, sitemap_rewrite
    baseline = b.setup_source(source())
    expected = b.expected_source(baseline)
    pairs = [{'from': url, 'to': 'https:' + url[5:]} for url in sorted(b.TARGETS)]
    assert sitemap_rewrite.upgrade_schemes(baseline, pairs, m._verification_url) == (expected, 3)
    assert len(b.locs(baseline)) == 51 and len(b.locs(expected)) == 49
    assert b.entry(b.MASTER, b.GOOD_META) in expected and b.entry(b.CONTROL, b.OLD_META) in expected
    assert all(b.entry(url) in expected for url in b.NEGATIVES)


@pytest.mark.parametrize('bad', ['count', 'missing_master', 'duplicate_control', 'already_negative', 'namespace', 'bytes'])
def test_drifted_setup_refuses(bad):
    value = source()
    if bad == 'count':
        value = value.replace('</urlset>', b.entry(b.SITE + 'extra') + '</urlset>')
    elif bad == 'missing_master':
        value = value.replace(b.MASTER, b.SITE + 'other')
    elif bad == 'duplicate_control':
        value = value.replace(b.SITE + 'control-0', b.CONTROL)
    elif bad == 'already_negative':
        value = value.replace(b.SITE + 'control-0', next(iter(b.NEGATIVES)))
    elif bad == 'namespace':
        value = value.replace('http://www.sitemaps.org/schemas/sitemap/0.9', 'urn:wrong')
    else:
        value = value.replace('<loc>' + b.MASTER, '<loc> ' + b.MASTER)
    with pytest.raises(ValueError):
        b.setup_source(value)


@pytest.mark.parametrize('bad', ['count', 'missing', 'duplicate', 'metadata'])
def test_drifted_expected_source_refuses(bad):
    value = b.setup_source(source())
    url = 'http:' + b.MASTER[6:]
    if bad == 'count':
        value = value.replace('</urlset>', b.entry(b.SITE + 'extra') + '</urlset>')
    elif bad == 'missing':
        value = value.replace(url, b.HTTP + 'other')
    elif bad == 'duplicate':
        value = value.replace(b.SITE + 'control-0', url)
    else:
        value = value.replace(b.GOOD_META, b.OLD_META)
    with pytest.raises(ValueError):
        b.expected_source(value)


@pytest.mark.parametrize('bad', [None, 'file', 'main', 'branch', 'retry', 'sha', 'bytes', 'base64'])
def test_write_guard_requires_exact_scope_sha_and_bytes(bad):
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


def test_measurement_keeps_two_negative_http_entries_and_reports_increases():
    before, after = reports()
    assert b.checks(before, after)['target_counts'] == {b.KEY: [5, 2]}
    after['issues']['example'] = {'count': 1}
    assert b.checks(before, after)['increased_counts'] == {'example': [0, 1]}


@pytest.mark.parametrize('bad', ['false_zero', 'lost_negative', 'lost_page', 'title', 'missing_witness', 'missing_code',
    'noindex_family', 'redirect_family', 'negative_examples', 'incomplete', 'settings'])
def test_false_success_or_collateral_change_refuses(bad):
    before, after = reports()
    if bad == 'false_zero':
        after['issues'][b.KEY] = {'count': 0, 'examples': []}
    elif bad == 'lost_negative':
        after['issues'][b.KEY]['examples'].pop()
    elif bad == 'lost_page':
        after['pages'].pop(0)
    elif bad == 'title':
        after['pages'][0]['title'] = 'changed'
    elif bad == 'missing_witness':
        after['pages'] = [row for row in after['pages'] if row['url'] != next(iter(b.previous.TARGETS))]
    elif bad == 'missing_code':
        next(row for row in after['pages'] if row['url'] in b.previous.TARGETS)['status_code'] = 403
    elif bad == 'noindex_family':
        after['issues'][b.previous.previous.KEY]['count'] = 1
    elif bad == 'redirect_family':
        after['issues'][b.previous.previous.previous.KEY]['count'] = 0
    elif bad == 'negative_examples':
        after['issues'][b.previous.previous.previous.previous.KEY]['examples'] = ['unrelated']
    elif bad == 'incomplete':
        after['meta']['urls_uncrawled'] = 1
    else:
        after['meta']['max_pages'] = 89
    with pytest.raises(ValueError):
        b.checks(before, after)
