"""Measure one minimal viewport addition on a pinned owned static HTML fixture."""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ops.gauntlet import sitemap_add_cycle as previous  # noqa: E402
from ops.gauntlet.ai_budget import ClaudeBudget  # noqa: E402

live, titles = previous.live, previous.titles
REPO, MAIN, SITE = previous.REPO, previous.MAIN, previous.SITE
SEED_BRANCH = 'gauntlet-static-html-sitemap-add-correction-20261005-102033'
SEED, SEED_PR = 'e42e0f58eb69128317c2ec912f3ce92d33c7e82a', 76
PREFIX, KEY = 'gauntlet-static-html-viewport-', 'viewport_not_set'
FILE, ROUTE = 'gauntlet/viewport-not-set.html', '/gauntlet/viewport-not-set'
SOURCE = SITE.rstrip('/') + ROUTE
VALUE = 'width=device-width, initial-scale=1'
TAG = '<meta name="viewport" content="' + VALUE + '" />'


def expected_source(raw):
    if (raw.count('  </head>') != 1 or raw.count('</head>') != 1 or raw.count('<head>') != 1
            or raw.count('rel="canonical" href="' + SOURCE + '"') != 1 or 'name="viewport"' in raw):
        raise ValueError('unexpected_pinned_viewport_source')
    return raw.replace('  </head>', '  ' + TAG + '\n  </head>')


def write_allowed(path, body, branch, attempts, expected, sha):
    if (path != FILE or not branch.startswith(PREFIX + 'correction-') or body.get('branch') != branch
            or attempts != 0 or not sha or body.get('sha') != sha):
        return False
    try:
        return base64.b64decode(body.get('content', ''), validate=True).decode('utf-8') == expected
    except (ValueError, TypeError, UnicodeError):
        return False


def checks(before, after, *, before_preview, after_preview):
    previous.complete(before)
    previous.complete(after)
    bp, ap = live._html_pages(before), live._html_pages(after)
    if bp.keys() != ap.keys() or len(bp) != 52:
        raise ValueError('html_routes_lost')
    for report, count, value in ((before, 0, None), (after, 1, VALUE)):
        block = report['issues'][KEY]
        if block.get('count') != 1 - count or block.get('examples') != ([SOURCE] if not count else []):
            raise ValueError('viewport_family_not_measured')
        rows = [row for row in report['pages'] if row['url'] == SOURCE]
        if not rows or any(type(row.get('meta_viewport_tag_count')) is not int or row['meta_viewport_tag_count'] != count
                or row.get('meta_viewport') != value or type(row.get('status_code')) is not int or row['status_code'] != 200
                or row.get('canonical') != SOURCE or row.get('final_url') != SOURCE or row.get('redirect_chain')
                or row.get('redirect_statuses') or row.get('meta_robots') or row.get('x_robots_tag') for row in rows):
            raise ValueError('viewport_witness_changed_or_missing')
    fields = titles.PRESERVED + ('title', 'title_tag_count', 'meta_description', 'meta_description_tag_count',
        'h1_tag_count', 'h2_tag_count', 'content_sketch', 'text_word_count', 'internal_link_items', 'ld_json_blocks',
        'meta_robots_tag_count', 'status_code', 'content_type', 'error', 'blocked_by_host')
    if any(old.get(field) != ap[route].get(field) for route, old in bp.items() for field in fields):
        raise ValueError('collateral_html_fields_changed')
    if any(old.get(field) != ap[route].get(field) for route, old in bp.items() if route != ROUTE
           for field in ('meta_viewport', 'meta_viewport_tag_count')):
        raise ValueError('healthy_viewport_changed')
    def observations(report):
        rows = []
        for row in report['pages']:
            if not row['url'].startswith(SITE):
                continue
            selected = fields + ('url', 'final_url', 'redirect_chain', 'redirect_statuses')
            if row['url'] != SOURCE:
                selected += ('meta_viewport', 'meta_viewport_tag_count')
            rows.append(json.dumps({field: row.get(field) for field in selected}, sort_keys=True))
        return sorted(rows)
    if observations(before) != observations(after):
        raise ValueError('requested_observations_changed')
    for key, block in before['issues'].items():
        if key == KEY:
            continue
        new = after['issues'].get(key, {})
        if (block.get('count', 0) != new.get('count', 0)
                or previous.preview_examples(block.get('examples', []), before_preview)
                != previous.preview_examples(new.get('examples', []), after_preview)):
            raise ValueError('other_issue_counts_or_examples_changed')
    if any(block.get('count', 0) > before['issues'].get(key, {}).get('count', 0) for key, block in after['issues'].items()):
        raise ValueError('issue_count_increased')
    return {'html_routes': [52, 52], 'target_counts': {KEY: [1, 0]}, 'viewport_tags': [0, 1],
        'minimal_new_tags': 1, 'observed_html_fields_preserved': True, 'requested_observation_multiset_preserved': True,
        'healthy_viewports_and_previous_repairs_preserved': True, 'increased_counts': {}}


def cycle(m, token, work, budget):
    if budget.limit != 0:
        raise ValueError('nonzero_provider_budget')
    result = {'status': 'unverified', 'stage': 'identity', 'repository': f'{live.OWNER}/{REPO}', 'seed_sha': SEED,
        'pull_requests': [], 'writes': [], 'write_attempts': 0, 'http_rechecks': [], 'preview_urls': {}, 'universal_certification': False}
    original_put, original_probe = m._github_api_put, m._sitemap_https_page
    correction, expected, blob, baseline_preview = '', '', '', ''

    def get(*parts, **kw):
        return m._github_api_get(m._github_api_path('repos', live.OWNER, REPO, *parts), token=token, **kw)

    def ref(branch):
        return get('git', 'ref', 'heads', branch)['object']['sha']

    def read(path, sha):
        row = get('contents', path, params={'ref': sha})
        return row['sha'], base64.b64decode(row['content']).decode('utf-8')

    def put(api, **kw):
        if (api != m._github_content_api_path(live.OWNER, REPO, FILE)
                or not write_allowed(FILE, kw.get('json_body', {}), correction, result['write_attempts'], expected, blob)):
            raise ValueError('outside_exact_qa_write_scope')
        result['write_attempts'] += 1
        live.save(work / 'cycle.json', result)
        response = original_put(api, **kw)
        result['writes'].append({'branch': correction, 'path': FILE})
        return response

    def probe(url, canonical):
        if url != SOURCE or not baseline_preview:
            raise ValueError('outside_exact_qa_http_scope')
        actual = baseline_preview.rstrip('/') + ROUTE
        original_get = m.requests.get
        def projected_get(value, **kw):
            if value != SOURCE:
                raise ValueError('unexpected_probe_url')
            response = original_get(actual, **kw)
            if response.url != actual or response.headers.get('x-robots-tag') != 'noindex' or response.history:
                response.close()
                raise ValueError('unexpected_native_preview_response')
            headers = response.headers.copy()
            del headers['x-robots-tag']
            return SimpleNamespace(status_code=response.status_code, url=SOURCE, history=response.history,
                headers=headers, close=response.close, iter_content=response.iter_content)
        m.requests.get = projected_get
        try:
            valid = original_probe(url, canonical)
        finally:
            m.requests.get = original_get
        result['http_rechecks'].append({'source': SOURCE, 'actual_preview_url': actual, 'verified': valid,
            'mode': 'owned_exact_host_and_native_noindex_header_projection'})
        return valid

    def branch(name):
        if not name.startswith(PREFIX) or not m._github_branch_allowed(name):
            raise ValueError('outside_owned_qa_branch')
        m._github_api_post(m._github_api_path('repos', live.OWNER, REPO, 'git', 'refs'), token=token,
            json_body={'ref': 'refs/heads/' + name, 'sha': SEED})

    def preview(kind, head, sha):
        nonlocal baseline_preview
        result['stage'] = kind
        print('STAGE=' + kind, flush=True)
        pr = m._ouvrir_pull_request(owner=live.OWNER, repo=REPO, token=token, head=head, base='main', draft=True,
            title='QA minimal viewport ' + kind + ' (never merge)', body='Owned fixture only. Close without merging.')
        row = {'number': pr['number'], 'url': pr['html_url'], 'kind': kind}
        result['pull_requests'].append(row)
        live.save(work / 'cycle.json', result)
        row['build'] = live.wait_build(m, REPO, token, pr['number'], expected_sha=sha)
        if ref(head) != sha or row['build']['head_sha'] != sha:
            raise ValueError('exact_head_changed')
        base = f"https://deploy-preview-{pr['number']}--{REPO}.netlify.app/"
        result['preview_urls'][kind] = base
        if kind == 'baseline':
            baseline_preview = base
        out = work / kind
        out.mkdir()
        xml = titles.preview_sitemap(base)
        (out / 'sitemap.xml').write_bytes(xml)
        response = live.requests.get(base.rstrip('/') + ROUTE, timeout=30, allow_redirects=False)
        try:
            (out / 'viewport.http').write_bytes(response.content)
            live.save(out / 'http.json', {'status': response.status_code, 'location': response.headers.get('location', ''),
                'content_type': response.headers.get('content-type', ''), 'x_robots_tag': response.headers.get('x-robots-tag', '')})
            if response.status_code != 200 or response.headers.get('location') or response.headers.get('x-robots-tag') != 'noindex':
                raise ValueError('native_preview_policy_changed')
        finally:
            response.close()
        live.crawl('static-html', base, out / 'raw', 90, preview=True)
        return previous.previous.rescore(out / 'raw/report.json', base, xml, out / 'controlled')

    m._github_api_put, m._sitemap_https_page = put, probe
    try:
        if ref('main') != MAIN or ref(SEED_BRANCH) != SEED:
            raise ValueError('fixture_identity_changed')
        parent = get('pulls', str(SEED_PR))
        if parent['state'] != 'closed' or parent['merged'] or not parent['draft'] or parent['head']['sha'] != SEED:
            raise ValueError('unverified_seed_pr')
        live.wait_build(m, REPO, token, SEED_PR, expected_sha=SEED)
        if get('compare', MAIN + '...' + SEED)['merge_base_commit']['sha'] != MAIN:
            raise ValueError('unverified_seed_ancestry')
        tree = get('git', 'trees', SEED, params={'recursive': '1'})
        if tree.get('truncated'):
            raise ValueError('incomplete_fixture_tree')
        paths = [row['path'] for row in tree['tree'] if row['type'] == 'blob']
        blob, raw = read(FILE, SEED)
        expected = expected_source(raw)
        stamp = dt.datetime.now(dt.UTC).strftime('%Y%m%d-%H%M%S')
        baseline, correction = (PREFIX + kind + '-' + stamp for kind in ('baseline', 'correction'))
        result.update(main_sha=MAIN, baseline_branch=baseline, correction_branch=correction, baseline_sha=SEED)
        branch(baseline)
        before = preview('baseline', baseline, SEED)
        prep = m._prepare_issue_fix(issue_key=KEY, issues=before['issues'], impacted=[SOURCE], all_paths=paths,
            site_name=urlsplit(SITE).netloc, owner=live.OWNER, repo_name=REPO, branch=baseline, token=token, pages=before['pages'])
        if prep['refusal'] or prep['rewriter_ai_fallback'] or prep.get('viewport_urls') != [SOURCE]:
            raise ValueError('missing_viewport_not_verified')
        branch(correction)
        from backend import repo_index
        result['repair'] = m._apply_prepared_issue_fix(owner=live.OWNER, repo_name=REPO, branch=baseline, token=token,
            fix_branch=correction, all_paths=paths, issue_key=KEY, issue_label=KEY, impacted=[SOURCE],
            site_name=urlsplit(SITE).netloc, file_state={}, max_files=1, prep=prep, pages=before['pages'],
            index=repo_index.build_repo_index(paths), allow_ai_targeting=False)
        repair = result['repair']
        if repair['patched'] != [FILE] or repair['skipped'] or repair['ai_files'] or repair['config_changes']:
            raise ValueError('unexpected_repair_scope')
        if len(result['http_rechecks']) != 1 or not result['http_rechecks'][0]['verified']:
            raise ValueError('missing_current_https_proof')
        final_sha = ref(correction)
        result['final_sha'] = final_sha
        compare = get('compare', SEED + '...' + final_sha)
        if compare['total_commits'] != 1 or [row['filename'] for row in compare['files']] != [FILE]:
            raise ValueError('collateral_source_changed')
        if read(FILE, final_sha)[1] != expected or read('sitemap.xml', SEED)[1] != read('sitemap.xml', final_sha)[1]:
            raise ValueError('exact_source_check_failed')
        after = preview('final', correction, final_sha)
        result.update(status='measured_minimal_viewport_addition', checks=checks(before, after,
            before_preview=result['preview_urls']['baseline'], after_preview=result['preview_urls']['final']))
    except Exception as exc:
        result.update(status='failed', error_type=type(exc).__name__)
        if isinstance(exc, ValueError) and str(exc).replace('_', '').isalnum():
            result['failure_code'] = str(exc)
        print(result['status'], result['stage'], result.get('failure_code', result['error_type']), flush=True)
    finally:
        m._github_api_put, m._sitemap_https_page = original_put, original_probe
        result['ai_budget'] = budget.summary()
        result['cleanup'] = live.close_prs(REPO, token, result['pull_requests'])
        result['prs_closed_unmerged'] = previous.closed_cleanup(result['pull_requests'], result['cleanup'])
        result['main_unchanged'] = ref('main') == MAIN
        if not result['prs_closed_unmerged'] or not result['main_unchanged'] or budget.summary()['attempted'] or budget.summary()['denied']:
            result['status'] = 'unverified'
        live.save(work / 'cycle.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workdir', type=Path, required=True)
    args = parser.parse_args()
    args.workdir.mkdir(parents=True, exist_ok=True)
    if any(args.workdir.iterdir()):
        parser.error('Use an empty temporary directory, never customer data.')
    m, token = live._load_backend(args.workdir)
    budget = ClaudeBudget(0)
    budget.install(m)
    result = cycle(m, token, args.workdir, budget)
    print(result['status'], flush=True)
    return int(result['status'] != 'measured_minimal_viewport_addition')


if __name__ == '__main__':
    raise SystemExit(main())
