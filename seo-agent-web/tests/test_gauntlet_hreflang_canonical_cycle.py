"""The measured subset must retain languages, returns and ambiguous defects."""
import base64
import copy

import pytest

from ops.gauntlet import hreflang_canonical_cycle as b
from tests.test_gauntlet_hreflang_lang_cycle import reports as previous_reports

BASES = [f'https://deploy-preview-{n}--{b.REPO}.netlify.app/' for n in (83, 84)]


def reports():
    _, after = previous_reports()
    sample = next(row for row in after['pages'] if row['url'] == b.previous.SOURCE)
    for url in (b.SOURCE, b.OLD, b.NEW):
        language, canonical = ('fr', b.SOURCE) if url == b.SOURCE else ('en', b.NEW)
        values = [('fr', b.SOURCE), ('en', b.NEW)]
        target = dict(sample, url=url, final_url=url, canonical=canonical, lang=language,
            served_lang=language if url != b.OLD else None, hreflang=dict(values),
            meta_viewport='width=device-width, initial-scale=1', meta_viewport_tag_count=1,
            hreflang_raw=[{'hreflang': code, 'href': href} for code, href in values])
        after['pages'].append(target)
    after['issues'][b.KEY] = {'count': 2, 'examples': sorted(b.NEGATIVES)}
    after['issues'][b.SECOND] = {'count': 1, 'examples': [b.previous.SOURCE]}
    before = copy.deepcopy(after)
    before['issues'][b.KEY] = {'count': 3, 'examples': sorted(b.NEGATIVES | {b.SOURCE})}
    before['issues'][b.SECOND] = {'count': 2, 'examples': [b.previous.SOURCE, b.NEW]}
    for row in before['pages']:
        if row['url'] == b.SOURCE:
            row['hreflang']['en'] = b.OLD
            row['hreflang_raw'][1]['href'] = b.OLD
    return before, after


def check(before, after):
    return b.checks(before, after, before_preview=BASES[0], after_preview=BASES[1])


def test_nonzero_budget_refuses_without_io(tmp_path):
    with pytest.raises(ValueError):
        b.cycle(None, 'unused', tmp_path, b.ClaudeBudget(1))
    assert not list(tmp_path.iterdir())


def test_setup_is_exactly_three_controls_and_three_discovery_links_not_a_sitemap_change():
    raw = '<html><body><ul>\n    </ul></body></html>'
    values = b.setup_sources(raw)
    assert set(values) == {b.INDEX, *b.FILES}
    assert all(values[b.INDEX].count('href="' + route + '"') == 1 for route in b.ROUTES)
    assert values[b.FILES[1]] == values[b.FILES[2]] and 'sitemap.xml' not in values


def test_the_real_source_rewriter_matches_the_independent_one_href_golden():
    from backend import app as m, hreflang_canonical
    raw = b.fixture('fr', b.SOURCE, True)
    before, _ = reports()
    observed = next(row for row in before['pages'] if row['url'] == b.SOURCE)
    expected = b.expected_source(raw)
    assert hreflang_canonical.rewrite(raw, b.PAIR, observed, m._duplicate_html_document, m._verification_url) == (expected, 1)
    assert hreflang_canonical.rewrite(expected, b.PAIR, observed, m._duplicate_html_document, m._verification_url) == (expected, 0)
    assert expected.replace('hreflang="en" href="' + b.NEW + '"', 'hreflang="en" href="' + b.OLD + '"', 1) == raw


@pytest.mark.parametrize('bad', ['no_list', 'duplicate_list', 'existing_control'])
def test_drifted_seed_index_refuses(bad):
    raw = '    </ul>'
    raw = '' if bad == 'no_list' else raw * 2 if bad == 'duplicate_list' else raw + b.ROUTES[0]
    with pytest.raises(ValueError):
        b.setup_sources(raw)


@pytest.mark.parametrize('bad', ['other_lang', 'other_canonical', 'already_fixed', 'changed_navigation'])
def test_drifted_control_source_refuses(bad):
    raw = b.fixture('fr', b.SOURCE, True)
    if bad == 'other_lang':
        raw = raw.replace('lang="fr"', 'lang="en"')
    elif bad == 'other_canonical':
        raw = raw.replace('rel="canonical" href="' + b.SOURCE, 'rel="canonical" href="' + b.NEW)
    elif bad == 'already_fixed':
        raw = b.expected_source(raw)
    else:
        raw = raw.replace('href="/gauntlet/"', 'href="/"')
    with pytest.raises(ValueError):
        b.expected_source(raw)


@pytest.mark.parametrize('kind', ['setup', 'correction'])
@pytest.mark.parametrize('bad', [None, 'path', 'main', 'branch', 'retry', 'sha', 'bytes', 'base64'])
def test_exact_per_stage_bytes_branch_sha_and_no_retry_required(kind, bad):
    branch = b.PREFIX + kind + '-test'
    path = b.FILES[1] if kind == 'setup' else b.FILE
    allowed, shas, attempts = {path: 'expected'}, {} if kind == 'setup' else {path: 'original'}, {}
    body = {'branch': branch, 'content': base64.b64encode(b'expected').decode()}
    if kind == 'correction':
        body['sha'] = 'original'
    if bad == 'path':
        path = 'sitemap.xml'
    elif bad == 'main':
        branch = body['branch'] = 'main'
    elif bad == 'branch':
        body['branch'] += 'other'
    elif bad == 'retry':
        attempts[path] = 1
    elif bad == 'sha':
        body['sha'] = 'stale'
    elif bad == 'bytes':
        body['content'] = base64.b64encode(b'collateral').decode()
    elif bad == 'base64':
        body['content'] = '!!!!'
    assert b.write_allowed(path, body, branch, allowed, shas, attempts) is (bad is None)


def test_measured_subset_retains_all_pages_and_the_two_explicit_unsafe_controls():
    result = check(*reports())
    assert result['whole_family_counts'] == [3, 2] and result['selected_counts'] == [1, 0]
    assert result['ambiguous_controls'] == [2, 2] and result['reciprocal_counts'] == [2, 1] and not result['increased_counts']


@pytest.mark.parametrize('bad', ['false_zero', 'false_three', 'lost_route', 'lost_observation', 'typed_count', 'typed_status',
    'canonical', 'lang', 'served_lang', 'return_link', 'alias_annotation', 'healthy_lang', 'healthy_hreflang',
    'og_title', 'title', 'viewport', 'twitter_description', 'status', 'robots', 'negative_examples', 'old_count',
    'old_examples', 'new_issue', 'settings', 'incomplete', 'secondary_count', 'secondary_example'])
def test_false_success_or_collateral_changes_refuse(bad):
    before, after = reports()
    target = next(row for row in after['pages'] if row['url'] == b.SOURCE)
    if bad == 'false_zero':
        after['issues'][b.KEY]['count'] = 0
    elif bad == 'false_three':
        after['issues'][b.KEY]['count'] = 3
    elif bad == 'lost_route':
        after['pages'] = [row for row in after['pages'] if row['url'] != b.SOURCE]
    elif bad == 'lost_observation':
        after['pages'].append(dict(target))
    elif bad == 'typed_count':
        after['issues'][b.KEY]['count'] = '2'
    elif bad == 'typed_status':
        target['status_code'] = True
    elif bad in {'canonical', 'lang', 'served_lang', 'og_title', 'title', 'twitter_description'}:
        target[bad] = 'changed'
    elif bad == 'viewport':
        target['meta_viewport'] = None
    elif bad in {'return_link', 'alias_annotation'}:
        next(row for row in after['pages'] if row['url'] == (b.NEW if bad == 'return_link' else b.OLD))['hreflang']['fr'] = 'changed'
    elif bad in {'healthy_lang', 'healthy_hreflang'}:
        next(row for row in after['pages'] if row['url'] == b.previous.SOURCE)['lang' if bad == 'healthy_lang' else 'hreflang'] = 'changed'
    elif bad == 'status':
        target['status_code'] = 404
    elif bad == 'robots':
        target['meta_robots'] = 'noindex'
    elif bad == 'negative_examples':
        after['issues'][b.KEY]['examples'] = [b.SOURCE, next(iter(b.NEGATIVES))]
    elif bad in {'old_count', 'old_examples'}:
        after['issues'][b.previous.KEY]['count' if bad == 'old_count' else 'examples'] = 9 if bad == 'old_count' else ['changed']
    elif bad == 'new_issue':
        after['issues']['new_issue'] = {'count': 1, 'examples': [b.SOURCE]}
    elif bad in {'secondary_count', 'secondary_example'}:
        after['issues'][b.SECOND]['count' if bad == 'secondary_count' else 'examples'] = 0 if bad == 'secondary_count' else [b.NEW]
    elif bad == 'settings':
        after['meta']['max_pages'] = 89
    else:
        after['meta']['urls_uncrawled'] = 1
    with pytest.raises(ValueError):
        check(before, after)
