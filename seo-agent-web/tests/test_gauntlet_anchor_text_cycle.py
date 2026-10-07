"""Reject a false success, an invented destination, and collateral fixture edits."""
import base64
import copy

import pytest

from ops.gauntlet import anchor_text_cycle as b
from tests.test_gauntlet_hreflang_drop_cycle import reports as previous_reports

BASES = [f'https://deploy-preview-{n}--{b.REPO}.netlify.app/' for n in (87, 88)]


def reports():
    _, after = previous_reports()
    sample = next(row for row in after['pages'] if row['url'] == b.previous.SOURCE)
    for url, kind in zip((b.SOURCE, b.TARGET, b.NEGATIVE), ('source', 'target', 'unproved')):
        links = [b.empty_link(b.NEGATIVE, b.UNKNOWN, '/gauntlet/html-lang-missing')] if url == b.NEGATIVE else []
        title = 'Accessible name control ' + kind.upper()
        after['pages'].append(dict(sample, url=url, final_url=url, canonical=url, lang='fr', served_lang=None,
            title=title, title_tag_count=1, h1=[title], h1_tag_count=1, hreflang={}, hreflang_raw=[], links_without_anchor_text=links))
    after['issues'][b.KEY] = {'count': 1, 'examples': [b.NEGATIVE]}
    before = copy.deepcopy(after)
    before['issues'][b.KEY] = {'count': 2, 'examples': sorted([b.SOURCE, b.NEGATIVE])}
    next(row for row in before['pages'] if row['url'] == b.SOURCE)['links_without_anchor_text'] = [b.empty_link(b.SOURCE, b.TARGET, b.ROUTES[1])]
    return before, after


def check(before, after):
    return b.checks(before, after, before_preview=BASES[0], after_preview=BASES[1])


def test_nonzero_budget_refuses_without_io(tmp_path):
    with pytest.raises(ValueError):
        b.cycle(None, 'unused', tmp_path, b.ClaudeBudget(1))
    assert not list(tmp_path.iterdir())


def test_setup_adds_only_three_controls_and_discovery_links():
    values = b.setup_sources('<html><body><ul>\n    </ul></body></html>')
    assert set(values) == {b.INDEX, *b.FILES}
    assert all(values[b.INDEX].count('href="' + route + '"') == 1 for route in b.ROUTES)
    assert all(values[path] == b.fixture(kind) for path, kind in zip(b.FILES, ('source', 'target', 'unproved')))
    assert all('hreflang=' not in values[path] for path in b.FILES)
    assert 'sitemap.xml' not in values


def test_real_rewriter_and_guarded_plan_match_single_attribute_golden():
    from backend import app as m, anchor_proof
    from tests.test_verified_anchor_text import page
    text, expected = b.fixture('source'), b.expected_source(b.fixture('source'))
    observed = page(b.SOURCE)
    observed.update(h1=['Accessible name control SOURCE'], title='Accessible name control SOURCE',
                    links_without_anchor_text=[b.empty_link(b.SOURCE, b.TARGET, b.ROUTES[1])])
    target = page(b.TARGET, target=True)
    target.update(h1=[b.NAME], title=b.NAME)
    assert anchor_proof.rewrite(text, [b.ITEM], observed, m._duplicate_html_document, m._verification_url) == (expected, 1)
    assert anchor_proof.rewrite(expected, [b.ITEM], observed, m._duplicate_html_document, m._verification_url) == (expected, 0)
    assert '<!-- decoy ' + b.LINK + ' -->' in expected
    plan = m._prepare_issue_fix(issue_key=b.KEY, issues={b.KEY: {'evidence': {'kind': 'page_values', 'items': [b.ITEM]}}},
        impacted=[b.SOURCE, b.NEGATIVE], all_paths=b.FILES, site_name=b.SITE, owner='fixture', repo_name='fixture',
        branch='qa', token='unused', pages=[observed, target])
    assert not plan['refusal'] and plan['anchor_text_items'] == [b.ITEM]
    assert b.NEGATIVE in plan['side_effects'] and not plan['rewriter_ai_fallback']


@pytest.mark.parametrize('bad', ['no_list', 'duplicate_list', 'existing_control'])
def test_drifted_index_refuses(bad):
    text = '' if bad == 'no_list' else '    </ul>' * 2 if bad == 'duplicate_list' else '    </ul>' + b.ROUTES[0]
    with pytest.raises(ValueError):
        b.setup_sources(text)


@pytest.mark.parametrize('bad', ['other_lang', 'other_canonical', 'already_fixed', 'navigation'])
def test_drifted_source_refuses(bad):
    text = b.fixture('source')
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


def test_measured_subset_preserves_unproved_link_and_all_routes():
    result = check(*reports())
    assert result['whole_family_counts'] == [2, 1] and result['selected_counts'] == [1, 0]
    assert result['unproved_controls'] == [1, 1] and not result['increased_counts']


@pytest.mark.parametrize('bad', ['false_zero', 'false_two', 'lost_route', 'lost_observation', 'typed_count', 'typed_status',
    'canonical', 'lang', 'served_lang', 'h1', 'h1_tag_count', 'source_link', 'negative_link', 'unknown_lang',
    'unknown_served', 'healthy_lang', 'healthy_hreflang', 'og_title', 'title', 'viewport', 'twitter_description',
    'status', 'robots', 'negative_examples', 'old_count', 'old_examples', 'new_issue', 'settings', 'incomplete',
    'typed_title_count', 'typed_h1_count'])
def test_false_success_or_collateral_changes_refuse(bad):
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
    elif bad in {'typed_title_count', 'typed_h1_count'}:
        target['title_tag_count' if bad == 'typed_title_count' else 'h1_tag_count'] = True
    elif bad in {'canonical', 'lang', 'served_lang', 'og_title', 'title', 'twitter_description', 'h1_tag_count'}:
        target[bad] = 'changed'
    elif bad == 'h1':
        target['h1'] = ['changed']
    elif bad == 'viewport':
        target['meta_viewport'] = None
    elif bad == 'source_link':
        target['links_without_anchor_text'] = [b.empty_link(b.SOURCE, b.TARGET, b.ROUTES[1])]
    elif bad == 'negative_link':
        next(row for row in after['pages'] if row['url'] == b.NEGATIVE)['links_without_anchor_text'] = []
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
