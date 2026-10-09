"""A Twitter benchmark must reject false zeros, incomplete cards and lost observations."""

import base64
import copy

import pytest

from ops.gauntlet import twitter_card_cycle as b
from ops.gauntlet.ai_budget import ClaudeBudget
from tests.test_gauntlet_viewport_cycle import reports as previous_reports
from ops.gauntlet import viewport_cycle

BASES = [f'https://deploy-preview-{n}--{b.REPO}.netlify.app/' for n in (79, 80)]


def source():
    return ('<html lang="fr"><head>\n  <link rel="canonical" href="' + b.SOURCE + '" />\n'
        '  <meta property="og:description" content="' + b.DESCRIPTION + '" />\n'
        '  <meta name="twitter:title" content="' + b.TITLE + '" />\n'
        '  <meta name="twitter:image" content="' + b.IMAGE + '" />\n  </head><body>Control</body></html>\n')


def reports():
    _, after = previous_reports()
    old = next(row for row in after['pages'] if row['status_code'] == 200
               and row['url'] not in b.previous.TARGETS | {viewport_cycle.SOURCE})
    after['pages'] = [row for row in after['pages'] if row['url'] != old['url']]
    target = dict(old, url=b.SOURCE, final_url=b.SOURCE, canonical=b.SOURCE, twitter_card=b.VALUE,
        twitter_title=b.TITLE, twitter_description=b.DESCRIPTION, twitter_image=b.IMAGE, og_description=b.DESCRIPTION,
        redirect_chain=[], redirect_statuses=[], meta_robots=None, x_robots_tag=None)
    after['pages'].extend([target, dict(target)])
    after['issues'][b.KEY] = {'count': 0, 'examples': []}
    before = copy.deepcopy(after)
    for row in before['pages']:
        if row['url'] == b.SOURCE:
            row.update(twitter_card=None, twitter_description=None)
    before['issues'][b.KEY] = {'count': 1, 'examples': [b.SOURCE]}
    return before, after


def check(before, after):
    return b.checks(before, after, before_preview=BASES[0], after_preview=BASES[1])


def test_nonzero_budget_refuses_before_io(tmp_path):
    with pytest.raises(ValueError):
        b.cycle(None, 'unused', tmp_path, ClaudeBudget(1))
    assert not list(tmp_path.iterdir())


def test_exact_expected_source_and_product_addition_match_without_other_changes():
    from backend import app as m, twitter_card
    expected = b.expected_source(source())
    assert expected == source().replace('  </head>', ''.join('  ' + tag + '\n' for tag in b.TAGS) + '  </head>')
    assert twitter_card.add(source(), b.SOURCE, m._duplicate_html_document, m._verification_url) == (expected, 2)


@pytest.mark.parametrize('bad', ['missing_close', 'second_head', 'other_canonical', 'already_present', 'wrong_description',
    'wrong_title', 'wrong_image'])
def test_drifted_pinned_source_refuses(bad):
    raw = source()
    if bad == 'missing_close':
        raw = raw.replace('</head>', '')
    elif bad == 'second_head':
        raw = raw.replace('</body>', '<head></head></body>')
    elif bad == 'other_canonical':
        raw = raw.replace(b.SOURCE, b.SITE + 'other')
    elif bad == 'already_present':
        raw = raw.replace('</head>', b.TAGS[0] + '</head>')
    else:
        raw = raw.replace({'wrong_description': b.DESCRIPTION, 'wrong_title': b.TITLE, 'wrong_image': b.IMAGE}[bad], 'changed')
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


def test_two_minimal_tags_are_measured_with_all_previous_repairs_and_healthy_heads_preserved():
    result = check(*reports())
    assert result['target_counts'] == {b.KEY: [1, 0]} and result['minimal_new_tags'] == 2
    assert not result['increased_counts'] and result['no_incomplete_card_created']


@pytest.mark.parametrize('bad', ['false_zero', 'lost_route', 'lost_observation', 'healthy_card', 'healthy_description',
    'og_title', 'og_description', 'title', 'status', 'typed_status', 'typed_count', 'redirect', 'canonical', 'noindex',
    'twitter_card', 'twitter_description', 'twitter_title', 'twitter_image', 'previous_viewport',
    'old_count', 'old_examples', 'new_issue', 'incomplete_card', 'settings', 'incomplete'])
def test_false_success_and_collateral_changes_refuse(bad):
    before, after = reports()
    target = next(row for row in after['pages'] if row['url'] == b.SOURCE)
    if bad == 'false_zero':
        after['issues'][b.KEY] = {'count': 1, 'examples': [b.SOURCE]}
    elif bad == 'lost_route':
        after['pages'] = [row for row in after['pages'] if row['url'] != b.SOURCE]
    elif bad == 'lost_observation':
        after['pages'].remove(target)
    elif bad in {'healthy_card', 'healthy_description'}:
        next(row for row in after['pages'] if row['url'] != b.SOURCE)[
            'twitter_card' if bad == 'healthy_card' else 'twitter_description'] = 'changed'
    elif bad in {'title', 'og_title', 'og_description', 'twitter_card', 'twitter_description', 'twitter_title', 'twitter_image'}:
        target[bad] = 'changed'
    elif bad in {'status', 'typed_status'}:
        target['status_code'] = True if bad == 'typed_status' else 404
    elif bad == 'typed_count':
        after['issues'][b.KEY]['count'] = False
    elif bad == 'redirect':
        target['redirect_chain'] = [b.SOURCE]
    elif bad == 'canonical':
        target['canonical'] = b.SITE + 'other'
    elif bad == 'noindex':
        target['meta_robots'] = 'noindex'
    elif bad == 'previous_viewport':
        next(row for row in after['pages'] if row['url'] == viewport_cycle.SOURCE)['meta_viewport'] = None
    elif bad in {'old_count', 'old_examples'}:
        after['issues'][b.previous.KEY]['count' if bad == 'old_count' else 'examples'] = 1 if bad == 'old_count' else ['other']
    elif bad in {'new_issue', 'incomplete_card'}:
        after['issues']['twitter_card_incomplete' if bad == 'incomplete_card' else 'new_issue'] = {'count': 1, 'examples': [b.SOURCE]}
    elif bad == 'settings':
        after['meta']['max_pages'] = 89
    else:
        after['meta']['urls_uncrawled'] = 1
    with pytest.raises(ValueError):
        check(before, after)
