"""A viewport benchmark cannot pass by losing a page or changing a healthy head."""

import base64
import copy

import pytest

from ops.gauntlet import viewport_cycle as b
from ops.gauntlet.ai_budget import ClaudeBudget
from tests.test_gauntlet_sitemap_add_cycle import reports as previous_reports

BASES = [f'https://deploy-preview-{n}--{b.REPO}.netlify.app/' for n in (77, 78)]


def source():
    return '<html lang="fr"><head>\n  <link rel="canonical" href="' + b.SOURCE + '" />\n  </head><body>Control</body></html>\n'


def reports():
    _, after = previous_reports()
    old = next(row for row in after['pages'] if row['status_code'] == 200 and row['url'] not in b.previous.TARGETS)
    after['pages'] = [row for row in after['pages'] if row['url'] != old['url']]
    target = dict(old, url=b.SOURCE, final_url=b.SOURCE, canonical=b.SOURCE, meta_viewport=b.VALUE,
                  meta_viewport_tag_count=1, redirect_chain=[], redirect_statuses=[], meta_robots=None, x_robots_tag=None)
    after['pages'].extend([target, dict(target)])
    after['issues'][b.KEY] = {'count': 0, 'examples': []}
    before = copy.deepcopy(after)
    for row in before['pages']:
        if row['url'] == b.SOURCE:
            row.update(meta_viewport=None, meta_viewport_tag_count=0)
    before['issues'][b.KEY] = {'count': 1, 'examples': [b.SOURCE]}
    return before, after


def check(before, after):
    return b.checks(before, after, before_preview=BASES[0], after_preview=BASES[1])


def test_nonzero_budget_refuses_before_io(tmp_path):
    with pytest.raises(ValueError):
        b.cycle(None, 'unused', tmp_path, ClaudeBudget(1))
    assert not list(tmp_path.iterdir())


def test_exact_expected_source_and_product_addition_match_without_other_changes():
    from backend import app as m, viewport
    expected = b.expected_source(source())
    assert expected == source().replace('  </head>', '  ' + b.TAG + '\n  </head>')
    assert viewport.add(source(), b.SOURCE, m._duplicate_html_document, m._verification_url) == (expected, 1)


@pytest.mark.parametrize('bad', ['missing_close', 'second_head', 'other_canonical', 'already_present'])
def test_drifted_pinned_source_refuses(bad):
    raw = source()
    if bad == 'missing_close':
        raw = raw.replace('</head>', '')
    elif bad == 'second_head':
        raw = raw.replace('</body>', '<head></head></body>')
    elif bad == 'other_canonical':
        raw = raw.replace(b.SOURCE, b.SITE + 'other')
    else:
        raw = raw.replace('</head>', b.TAG + '</head>')
    with pytest.raises(ValueError):
        b.expected_source(raw)


@pytest.mark.parametrize('bad', [None, 'file', 'main', 'branch', 'retry', 'sha', 'bytes', 'base64'])
def test_write_guard_permits_only_exact_single_qa_html_write(bad):
    path, branch, attempts = b.FILE, b.PREFIX + 'correction-test', 0
    body = {'branch': branch, 'sha': 'original', 'content': base64.b64encode(b'expected').decode()}
    if bad == 'file':
        path = 'sitemap.xml'
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


def test_one_tag_is_measured_with_all_healthy_routes_and_previous_repairs_preserved():
    result = check(*reports())
    assert result['target_counts'] == {b.KEY: [1, 0]} and result['viewport_tags'] == [0, 1]
    assert not result['increased_counts'] and result['healthy_viewports_and_previous_repairs_preserved']


@pytest.mark.parametrize('bad', ['false_zero', 'lost_route', 'lost_observation', 'healthy_viewport', 'title', 'status',
    'redirect', 'canonical', 'noindex', 'viewport_value', 'viewport_count', 'typed_count', 'typed_status',
    'old_count', 'old_examples', 'new_issue', 'settings', 'incomplete'])
def test_false_success_and_collateral_changes_refuse(bad):
    before, after = reports()
    target = next(row for row in after['pages'] if row['url'] == b.SOURCE)
    if bad == 'false_zero':
        after['issues'][b.KEY] = {'count': 1, 'examples': [b.SOURCE]}
    elif bad == 'lost_route':
        after['pages'] = [row for row in after['pages'] if row['url'] != b.SOURCE]
    elif bad == 'lost_observation':
        after['pages'].remove(target)
    elif bad == 'healthy_viewport':
        next(row for row in after['pages'] if row['url'] != b.SOURCE)['meta_viewport'] = b.VALUE
    elif bad in {'title', 'viewport_value'}:
        target['title' if bad == 'title' else 'meta_viewport'] = 'changed'
    elif bad in {'viewport_count', 'typed_count', 'status', 'typed_status'}:
        target['meta_viewport_tag_count' if 'count' in bad else 'status_code'] = (
            True if bad.startswith('typed') else 2 if bad == 'viewport_count' else 404)
    elif bad == 'redirect':
        target['redirect_chain'] = [b.SOURCE]
    elif bad == 'canonical':
        target['canonical'] = b.SITE + 'other'
    elif bad == 'noindex':
        target['meta_robots'] = 'noindex'
    elif bad in {'old_count', 'old_examples'}:
        after['issues'][b.previous.KEY]['count' if bad == 'old_count' else 'examples'] = 1 if bad == 'old_count' else ['other']
    elif bad == 'new_issue':
        after['issues']['new_issue'] = {'count': 1, 'examples': ['new']}
    elif bad == 'settings':
        after['meta']['max_pages'] = 89
    else:
        after['meta']['urls_uncrawled'] = 1
    with pytest.raises(ValueError):
        check(before, after)
