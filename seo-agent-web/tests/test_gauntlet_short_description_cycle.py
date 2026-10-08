"""An excerpt bench cannot certify invented text or changed collateral observations."""
import base64
import copy

import pytest

from ops.gauntlet import short_description_cycle as b
from tests.test_gauntlet_anchor_text_cycle import reports as previous_reports

BASES = [f'https://deploy-preview-{n}--{b.REPO}.netlify.app/' for n in (91, 92)]


def reports():
    _, after = previous_reports()
    sample = next(row for row in after['pages'] if row['url'] == b.previous.SOURCE)
    for url, kind in zip((b.SOURCE, b.ABSENT, b.NEGATIVE), b.KINDS):
        title = 'Published excerpt acceptance control ' + kind.upper()
        after['pages'].append(dict(sample, url=url, final_url=url, canonical=url,
            lang=None if kind == 'unproved' else 'fr', served_lang=None, title=title, title_tag_count=1,
            h1=[title], h1_tag_count=1, hreflang={}, hreflang_raw=[], links_without_anchor_text=[],
            meta_description=b.SHORTS[kind] if kind == 'unproved' else b.FACTS[kind], meta_description_tag_count=1))
    for key, examples in ((b.KEY, sorted(b.LEGACY_SHORT + [b.NEGATIVE])),
                          (b.KEY + '_indexable', sorted(b.LEGACY_SHORT + [b.NEGATIVE])),
                          (b.KEY + '_not_indexable', [])):
        after['issues'][key] = {'count': len(examples), 'examples': examples}
    before = copy.deepcopy(after)
    for key in (b.KEY, b.KEY + '_indexable'):
        before['issues'][key] = {'count': 5, 'examples': sorted(b.LEGACY_SHORT + [b.SOURCE, b.ABSENT, b.NEGATIVE])}
    for url, kind in zip((b.SOURCE, b.ABSENT), b.KINDS[:2]):
        row = next(row for row in before['pages'] if row['url'] == url)
        row.update(meta_description=b.SHORTS[kind], meta_description_tag_count=0 if kind == 'absent' else 1)
    return before, after


def check(before, after):
    return b.checks(before, after, before_preview=BASES[0], after_preview=BASES[1])


@pytest.mark.parametrize('limit', [0, 1, 3, 12])
def test_exact_two_provider_call_budget_refuses_without_io(tmp_path, limit):
    with pytest.raises(ValueError):
        b.cycle(None, 'unused', tmp_path, b.ClaudeBudget(limit))
    assert not list(tmp_path.iterdir())


def test_setup_adds_only_three_owned_controls_and_discovery_links():
    values = b.setup_sources('<html><body><ul>\n    </ul></body></html>')
    assert set(values) == {b.INDEX, *b.FILES}
    assert all(values[b.INDEX].count('href="' + route + '"') == 1 for route in b.ROUTES)
    assert all(values[path] == b.fixture(kind) for path, kind in zip(b.FILES, b.KINDS))
    assert all('hreflang=' not in values[path] for path in b.FILES)
    assert 'sitemap.xml' not in values


@pytest.mark.parametrize('kind', b.KINDS[:2])
def test_real_rewriter_matches_an_independently_bounded_golden(kind):
    from backend import app as m, short_description
    from tests.test_verified_short_description import page
    url = b.SOURCE if kind == 'present' else b.ABSENT
    title = 'Published excerpt acceptance control ' + kind.upper()
    observed = page(url, absent=kind == 'absent')
    observed.update(title=title, h1=[title], meta_description=b.SHORTS[kind])
    raw, expected = b.fixture(kind), b.expected_source(b.fixture(kind), kind)
    assert short_description.document(raw, url, m._duplicate_html_document, m._verification_url)['candidates'] == [b.FACTS[kind]]
    assert short_description.rewrite(raw, observed, b.FACTS[kind], m._duplicate_html_document, m._verification_url) == (expected, 1)
    assert short_description.rewrite(expected, observed, b.FACTS[kind], m._duplicate_html_document, m._verification_url) == (expected, 0)
    assert '<!-- decoy <meta name="description" content="not a description" /> -->' in expected


@pytest.mark.parametrize('bad', ['no_list', 'duplicate_list', 'existing_control'])
def test_drifted_index_refuses(bad):
    text = '' if bad == 'no_list' else '    </ul>' * 2 if bad == 'duplicate_list' else '    </ul>' + b.ROUTES[0]
    with pytest.raises(ValueError):
        b.setup_sources(text)


@pytest.mark.parametrize('bad', ['lang', 'canonical', 'already_fixed', 'paragraph', 'navigation'])
def test_drifted_source_refuses(bad):
    raw = b.fixture('present')
    changes = {'lang': ('lang="fr"', 'lang="en"'), 'canonical': (b.SOURCE, b.ABSENT),
               'paragraph': (b.FACTS['present'], 'Invented'), 'navigation': ('href="/gauntlet/"', 'href="/"')}
    raw = b.expected_source(raw, 'present') if bad == 'already_fixed' else raw.replace(*changes[bad], 1)
    with pytest.raises(ValueError):
        b.expected_source(raw, 'present')


@pytest.mark.parametrize('kind', ['setup', 'correction'])
@pytest.mark.parametrize('bad', [None, 'path', 'main', 'branch', 'retry', 'sha', 'bytes', 'base64'])
def test_exact_stage_branch_sha_bytes_and_no_retry(kind, bad):
    branch, path = b.PREFIX + kind + '-test', b.FILES[0]
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


def test_verified_subset_leaves_unknown_language_and_preexisting_occurrences():
    result = check(*reports())
    assert result['whole_family_counts'] == [5, 3] and result['selected_counts'] == [2, 0]
    assert result['unproved_controls'] == [1, 1] and not result['increased_counts']


@pytest.mark.parametrize('bad', ['false_zero', 'typed_count', 'variant_count', 'lost_route', 'lost_observation',
    'typed_status', 'canonical', 'lang', 'served_lang', 'h1', 'title', 'title_count', 'h1_count', 'description',
    'description_count', 'negative_description', 'invented_negative_lang', 'viewport', 'twitter_description',
    'body', 'navigation', 'old_count', 'old_examples', 'new_issue', 'settings', 'incomplete'])
def test_false_success_or_collateral_changes_refuse(bad):
    before, after = reports()
    target = next(row for row in after['pages'] if row['url'] == b.SOURCE)
    if bad in {'false_zero', 'typed_count', 'variant_count'}:
        after['issues'][b.KEY + ('_indexable' if bad == 'variant_count' else '')]['count'] = '3' if bad == 'typed_count' else 0
    elif bad == 'lost_route':
        after['pages'] = [row for row in after['pages'] if row['url'] != b.SOURCE]
    elif bad == 'lost_observation':
        after['pages'].append(dict(target))
    elif bad == 'typed_status':
        target['status_code'] = True
    elif bad in {'canonical', 'lang', 'served_lang', 'title', 'twitter_description'}:
        target[bad] = 'changed'
    elif bad == 'h1':
        target['h1'] = ['changed']
    elif bad in {'title_count', 'h1_count', 'description_count'}:
        target[{'title_count': 'title_tag_count', 'h1_count': 'h1_tag_count',
                'description_count': 'meta_description_tag_count'}[bad]] = True
    elif bad == 'description':
        target['meta_description'] = 'x' * 150
    elif bad in {'negative_description', 'invented_negative_lang'}:
        negative = next(row for row in after['pages'] if row['url'] == b.NEGATIVE)
        negative['meta_description' if bad == 'negative_description' else 'lang'] = b.FACTS['unproved'] if bad == 'negative_description' else 'fr'
    elif bad == 'viewport':
        target['meta_viewport'] = None
    elif bad == 'body':
        target['content_sketch'] = 'changed'
    elif bad == 'navigation':
        target['internal_link_items'] = ['changed']
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
