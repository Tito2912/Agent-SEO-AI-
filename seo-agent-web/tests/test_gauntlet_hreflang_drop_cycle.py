"""A measured deletion must retain the actual unproved language control."""
import base64
import copy

import pytest

from ops.gauntlet import hreflang_drop_cycle as b
from tests.test_gauntlet_hreflang_canonical_cycle import reports as previous_reports

BASES = [f'https://deploy-preview-{n}--{b.REPO}.netlify.app/' for n in (85, 86)]


def reports():
    _, after = previous_reports()
    sample = next(row for row in after['pages'] if row['url'] == b.previous.SOURCE)
    for url in (b.SOURCE, b.TARGET, b.NEGATIVE):
        values = [('fr', b.SOURCE), ('en', b.TARGET)] if url == b.TARGET else [('fr', b.UNKNOWN), ('en', b.UNKNOWN)] if url == b.NEGATIVE else [('en', b.TARGET)]
        language = 'en' if url == b.TARGET else 'fr'
        after['pages'].append(dict(sample, url=url, final_url=url, canonical=url, lang=language, served_lang=language,
            hreflang=dict(values), hreflang_raw=[{'hreflang': c, 'href': h} for c, h in values]))
    unknown = next(row for row in after['pages'] if row['url'] == b.UNKNOWN)
    unknown.update(lang=None, served_lang=None)
    after['issues'][b.KEY] = {'count': 1, 'examples': [b.NEGATIVE]}
    before = copy.deepcopy(after)
    before['issues'][b.KEY] = {'count': 2, 'examples': sorted([b.SOURCE, b.NEGATIVE])}
    target = next(row for row in before['pages'] if row['url'] == b.SOURCE)
    target.update(hreflang={'fr': b.TARGET, 'en': b.TARGET},
        hreflang_raw=[{'hreflang': c, 'href': b.TARGET} for c in ('fr', 'en')])
    return before, after


def check(before, after):
    return b.checks(before, after, before_preview=BASES[0], after_preview=BASES[1])


def test_nonzero_budget_refuses_without_io(tmp_path):
    with pytest.raises(ValueError):
        b.cycle(None, 'unused', tmp_path, b.ClaudeBudget(1))
    assert not list(tmp_path.iterdir())


def test_setup_adds_only_three_controls_and_three_discovery_links():
    text = '<html><body><ul>\n    </ul></body></html>'
    values = b.setup_sources(text)
    assert set(values) == {b.INDEX, *b.FILES}
    assert all(values[b.INDEX].count('href="' + route + '"') == 1 for route in b.ROUTES)
    assert all(values[path] == b.fixture(kind) for path, kind in zip(b.FILES, ('fr', 'en', 'unknown')))
    assert 'sitemap.xml' not in values and 'hreflang="fr" href="' + b.UNKNOWN + '"' in values[b.FILES[2]]


def test_real_rewriter_matches_the_independent_single_tag_golden():
    from backend import app as m, hreflang_drop
    before, _ = reports()
    observed = next(row for row in before['pages'] if row['url'] == b.SOURCE)
    text, expected = b.fixture('fr'), b.expected_source(b.fixture('fr'))
    assert hreflang_drop.rewrite(text, b.ITEM, observed, m._duplicate_html_document, m._verification_url) == (expected, 1)
    assert hreflang_drop.rewrite(expected, b.ITEM, observed, m._duplicate_html_document, m._verification_url) == (expected, 0)
    plan = m._prepare_issue_fix(issue_key=b.KEY, issues={b.KEY: {'evidence': {'kind': 'page_values', 'items': [b.ITEM]}}},
        impacted=[b.SOURCE, b.NEGATIVE], all_paths=b.FILES, site_name=b.SITE, owner='fixture', repo_name='fixture',
        branch='qa', token='unused', pages=before['pages'])
    assert not plan['refusal'] and plan['hreflang_drop_items'] == [b.ITEM]
    assert b.NEGATIVE in plan['side_effects'] and not plan['rewriter_ai_fallback']


@pytest.mark.parametrize('bad', ['no_list', 'duplicate_list', 'existing_control'])
def test_drifted_index_refuses(bad):
    text = '' if bad == 'no_list' else '    </ul>' * 2 if bad == 'duplicate_list' else '    </ul>' + b.ROUTES[0]
    with pytest.raises(ValueError):
        b.setup_sources(text)


@pytest.mark.parametrize('bad', ['other_lang', 'other_canonical', 'already_fixed', 'navigation'])
def test_drifted_source_refuses(bad):
    text = b.fixture('fr')
    if bad == 'other_lang':
        text = text.replace('lang="fr"', 'lang="en"')
    elif bad == 'other_canonical':
        text = text.replace('rel="canonical" href="' + b.SOURCE, 'rel="canonical" href="' + b.TARGET)
    elif bad == 'already_fixed':
        text = b.expected_source(text)
    else:
        text = text.replace('href="/gauntlet/"', 'href="/"')
    with pytest.raises(ValueError):
        b.expected_source(text)


@pytest.mark.parametrize('kind', ['setup', 'correction'])
@pytest.mark.parametrize('bad', [None, 'path', 'main', 'branch', 'retry', 'sha', 'bytes', 'base64'])
def test_exact_stage_branch_sha_bytes_and_no_retry(kind, bad):
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


def test_measured_subset_preserves_the_unknown_control_and_all_routes():
    result = check(*reports())
    assert result['whole_family_counts'] == [2, 1] and result['selected_counts'] == [1, 0]
    assert result['unproved_controls'] == [1, 1] and not result['increased_counts']


@pytest.mark.parametrize('bad', ['false_zero', 'false_two', 'lost_route', 'lost_observation', 'typed_count', 'typed_status',
    'canonical', 'lang', 'served_lang', 'return', 'negative_annotation', 'unknown_lang', 'unknown_served',
    'healthy_lang', 'healthy_hreflang', 'og_title', 'title', 'viewport', 'twitter_description', 'status', 'robots',
    'negative_examples', 'old_count', 'old_examples', 'new_issue', 'settings', 'incomplete'])
def test_false_success_and_collateral_changes_refuse(bad):
    before, after = reports()
    target = next(row for row in after['pages'] if row['url'] == b.SOURCE)
    if bad in {'false_zero', 'false_two', 'typed_count'}:
        after['issues'][b.KEY]['count'] = 0 if bad == 'false_zero' else 2 if bad == 'false_two' else '1'
    elif bad == 'lost_route':
        after['pages'] = [row for row in after['pages'] if row['url'] != b.SOURCE]
    elif bad == 'lost_observation':
        after['pages'].append(dict(target))
    elif bad == 'typed_status':
        target['status_code'] = True
    elif bad in {'canonical', 'lang', 'served_lang', 'og_title', 'title', 'twitter_description'}:
        target[bad] = 'changed'
    elif bad == 'viewport':
        target['meta_viewport'] = None
    elif bad in {'return', 'negative_annotation'}:
        next(row for row in after['pages'] if row['url'] == (b.TARGET if bad == 'return' else b.NEGATIVE))['hreflang']['fr'] = 'changed'
    elif bad in {'unknown_lang', 'unknown_served'}:
        next(row for row in after['pages'] if row['url'] == b.UNKNOWN)['lang' if bad == 'unknown_lang' else 'served_lang'] = 'fr'
    elif bad in {'healthy_lang', 'healthy_hreflang'}:
        next(row for row in after['pages'] if row['url'] == b.previous.SOURCE)['lang' if bad == 'healthy_lang' else 'hreflang'] = 'changed'
    elif bad == 'status':
        target['status_code'] = 404
    elif bad == 'robots':
        target['meta_robots'] = 'noindex'
    elif bad == 'negative_examples':
        after['issues'][b.KEY]['examples'] = [b.SOURCE]
    elif bad in {'old_count', 'old_examples'}:
        after['issues'][b.previous.KEY]['count' if bad == 'old_count' else 'examples'] = 9 if bad == 'old_count' else ['changed']
    elif bad == 'new_issue':
        after['issues']['new_issue'] = {'count': 1, 'examples': [b.SOURCE]}
    elif bad == 'settings':
        after['meta']['max_pages'] = 89
    else:
        after['meta']['urls_uncrawled'] = 1
    with pytest.raises(ValueError):
        check(before, after)
