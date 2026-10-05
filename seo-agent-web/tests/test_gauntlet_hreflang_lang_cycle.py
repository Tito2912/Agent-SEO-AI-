"""A root-language benchmark cannot pass by rewriting translation links or losing pages."""

import base64
import copy

import pytest

from ops.gauntlet import hreflang_lang_cycle as b, twitter_card_cycle, viewport_cycle
from ops.gauntlet.ai_budget import ClaudeBudget
from tests.test_gauntlet_twitter_card_cycle import reports as previous_reports

BASES = [f'https://deploy-preview-{n}--{b.REPO}.netlify.app/' for n in (81, 82)]


def source():
    return ('<!doctype html>\n<html>\n<head><!-- <html> in a comment -->\n'
        '<link rel="canonical" href="' + b.SOURCE + '" />\n'
        '<link rel="alternate" hreflang="fr" href="' + b.SOURCE + '" />\n'
        '<link rel="alternate" hreflang="en" href="' + b.TRANSLATION + '" />\n'
        '</head><body>Control</body></html>\n')


def reports():
    _, after = previous_reports()
    protected = b.previous.TARGETS | {twitter_card_cycle.SOURCE, viewport_cycle.SOURCE}
    old = next(row for row in after['pages'] if row['status_code'] == 200
               and row['url'].startswith(b.SITE + 'gauntlet/') and row['final_url'] == row['url']
               and all(item['url'] == row['url'] for item in after['pages'] if item.get('final_url') == row['url'])
               and row['url'] not in protected)
    after['pages'] = [row for row in after['pages'] if row['url'] != old['url']]
    target = dict(old, url=b.SOURCE, final_url=b.SOURCE, canonical=b.SOURCE, lang=b.VALUE, served_lang=b.VALUE,
        hreflang_raw=copy.deepcopy(b.ALTERNATES), hreflang={'fr': b.SOURCE, 'en': b.TRANSLATION},
        redirect_chain=[], redirect_statuses=[], meta_robots=None, x_robots_tag=None)
    after['pages'].extend([target, dict(target)])
    other = next(row for row in after['pages'] if row['status_code'] == 200
                 and row['url'].startswith(b.SITE + 'gauntlet/') and row['final_url'] == row['url']
                 and all(item['url'] == row['url'] for item in after['pages'] if item.get('final_url') == row['url'])
                 and row['url'] not in protected | {b.SOURCE})
    old_url = other['url']
    after['pages'] = [row for row in after['pages'] if row['url'] != old_url]
    after['pages'].append(dict(other, url=b.OTHER, final_url=b.OTHER, canonical=b.OTHER, lang=None, served_lang=None))
    after['issues'][b.KEY] = {'count': 0, 'examples': []}
    after['issues'][b.SECOND] = {'count': 1, 'examples': [b.OTHER]}
    before = copy.deepcopy(after)
    for row in before['pages']:
        if row['url'] == b.SOURCE:
            row.update(lang=None, served_lang=None)
    before['issues'][b.KEY] = {'count': 1, 'examples': [b.SOURCE]}
    before['issues'][b.SECOND] = {'count': 2, 'examples': [b.SOURCE, b.OTHER]}
    return before, after


def check(before, after):
    return b.checks(before, after, before_preview=BASES[0], after_preview=BASES[1])


def test_nonzero_budget_refuses_before_io(tmp_path):
    with pytest.raises(ValueError):
        b.cycle(None, 'unused', tmp_path, ClaudeBudget(1))
    assert not list(tmp_path.iterdir())


def test_only_root_attribute_changes_while_the_comment_and_hreflang_tags_stay_identical():
    from backend import app as m, hreflang_lang
    expected = b.expected_source(source())
    assert expected == source().replace('\n<html>\n', '\n<html lang="fr">\n', 1)
    assert '<!-- <html> in a comment -->' in expected
    assert hreflang_lang.add(source(), b.SOURCE, m._duplicate_html_document, m._verification_url) == (expected, 1)


@pytest.mark.parametrize('bad', ['missing_root', 'second_root', 'other_canonical', 'already_present', 'wrong_self', 'wrong_translation'])
def test_drifted_pinned_source_refuses(bad):
    raw = source()
    if bad == 'missing_root':
        raw = raw.replace('\n<html>\n', '\n')
    elif bad == 'second_root':
        raw = raw.replace('</html>', '</html>\n<html>\n')
    elif bad == 'other_canonical':
        raw = raw.replace('rel="canonical" href="' + b.SOURCE, 'rel="canonical" href="' + b.SITE + 'other')
    elif bad == 'already_present':
        raw = raw.replace('\n<html>\n', '\n<html lang="fr">\n')
    elif bad == 'wrong_self':
        raw = raw.replace('hreflang="fr"', 'hreflang="en"')
    else:
        raw = raw.replace(b.TRANSLATION, b.SITE + 'other')
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


def test_one_language_attribute_is_measured_with_the_other_missing_lang_and_old_repairs_preserved():
    result = check(*reports())
    assert result['target_counts'] == {b.KEY: [1, 0], b.SECOND: [2, 1]} and result['minimal_new_root_attributes'] == 1
    assert not result['increased_counts'] and result['all_hreflang_values_preserved']


@pytest.mark.parametrize('bad', ['false_zero', 'lost_route', 'lost_observation', 'healthy_language', 'other_missing_lang',
    'og_title', 'twitter_description', 'title', 'status', 'typed_status', 'typed_count', 'redirect', 'canonical', 'noindex',
    'lang', 'served_lang', 'hreflang', 'hreflang_raw', 'previous_viewport', 'previous_twitter',
    'old_count', 'old_examples', 'new_issue', 'secondary_count', 'secondary_examples', 'settings', 'incomplete'])
def test_false_success_and_collateral_changes_refuse(bad):
    before, after = reports()
    target = next(row for row in after['pages'] if row['url'] == b.SOURCE)
    if bad == 'false_zero':
        after['issues'][b.KEY] = {'count': 1, 'examples': [b.SOURCE]}
    elif bad == 'lost_route':
        after['pages'] = [row for row in after['pages'] if row['url'] != b.SOURCE]
    elif bad == 'lost_observation':
        after['pages'].remove(target)
    elif bad in {'healthy_language', 'other_missing_lang'}:
        next(row for row in after['pages'] if
             (row['url'] == b.OTHER if bad == 'other_missing_lang' else row['url'] not in {b.SOURCE, b.OTHER}))['lang'] = 'changed'
    elif bad in {'title', 'og_title', 'twitter_description', 'lang', 'served_lang', 'hreflang', 'hreflang_raw'}:
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
    elif bad == 'previous_twitter':
        next(row for row in after['pages'] if row['url'] == twitter_card_cycle.SOURCE)['twitter_card'] = None
    elif bad in {'old_count', 'old_examples', 'secondary_count', 'secondary_examples'}:
        key = b.SECOND if bad.startswith('secondary') else b.previous.KEY
        after['issues'][key]['count' if bad.endswith('count') else 'examples'] = 9 if bad.endswith('count') else ['other']
    elif bad == 'new_issue':
        after['issues']['new_issue'] = {'count': 1, 'examples': [b.SOURCE]}
    elif bad == 'settings':
        after['meta']['max_pages'] = 89
    else:
        after['meta']['urls_uncrawled'] = 1
    with pytest.raises(ValueError):
        check(before, after)
