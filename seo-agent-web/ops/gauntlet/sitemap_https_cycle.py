"""Measure verified scheme upgrades and two refused controls on an owned fixture."""

from __future__ import annotations

import argparse
import base64
import datetime as dt
from pathlib import Path
import sys
from types import SimpleNamespace
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ops.gauntlet import sitemap_error_cycle as previous  # noqa: E402
from ops.gauntlet.ai_budget import ClaudeBudget  # noqa: E402
from ops.gauntlet.https_canonical_cycle import complete  # noqa: E402
from ops.gauntlet.canonical_completion_cycle import closed_cleanup  # noqa: E402

live, projection, titles = previous.live, previous.projection, previous.titles
REPO, MAIN, SITE, locs = previous.REPO, previous.MAIN, previous.SITE, previous.locs
SEED_BRANCH = "gauntlet-static-html-sitemap-error-correction-20261005-064430"
SEED, SEED_PR = "7a29cdf61167d4613d714abae6638b011bfe3716", 70
PREFIX, KEY = "gauntlet-static-html-sitemap-https-", "sitemap_http_urls_for_https"
HTTP = "http:" + SITE[6:]
MASTER, CONTROL = SITE + "gauntlet/qa-master", SITE + "gauntlet/qa-control"
TARGETS = {HTTP + "gauntlet/", "http:" + MASTER[6:], "http:" + CONTROL[6:]}
NEGATIVES = {HTTP + "gauntlet/noindex-long", HTTP + "gauntlet/qa-sitemap-missing-a"}
OLD_META, GOOD_META = '<lastmod>2001-01-01</lastmod><priority>0.1</priority>', '<lastmod>2026-10-05</lastmod><priority>0.9</priority>'


def entry(url, meta=""):
    return '<url><loc>' + url + '</loc>' + meta + '</url>'


def setup_source(source):
    values = locs(source)
    if (len(values) != 48 or values.count(HTTP + "gauntlet/") != 1 or values.count(MASTER) != 1
            or values.count(CONTROL) != 1 or set(values) & (NEGATIVES | (TARGETS - {HTTP + "gauntlet/"}))
            or source.count(entry(MASTER)) != 1 or source.count(entry(CONTROL)) != 1 or source.count('</urlset>') != 1):
        raise ValueError("unexpected_pinned_sitemap_shape")
    source = source.replace(entry(MASTER), entry("http:" + MASTER[6:], OLD_META) + entry(MASTER, GOOD_META))
    source = source.replace(entry(CONTROL), entry("http:" + CONTROL[6:], OLD_META))
    return source.replace('</urlset>', '\n'.join(entry(url) for url in sorted(NEGATIVES)) + '\n</urlset>')


def expected_source(source):
    values = locs(source)
    if len(values) != 51 or any(values.count(url) != 1 for url in TARGETS | NEGATIVES):
        raise ValueError("unexpected_sitemap_witnesses")
    blocks = {HTTP + "gauntlet/": entry(HTTP + "gauntlet/"), "http:" + MASTER[6:]: entry("http:" + MASTER[6:], OLD_META),
              "http:" + CONTROL[6:]: entry("http:" + CONTROL[6:], OLD_META)}
    if any(source.count(block) != 1 for block in blocks.values()) or source.count(entry(MASTER, GOOD_META)) != 1:
        raise ValueError("unexpected_sitemap_metadata")
    output = source
    for url, block in blocks.items():
        output = output.replace(block, entry(CONTROL, OLD_META) if url == "http:" + CONTROL[6:] else '')
    if len(locs(output)) != 49 or {url for url in locs(output) if url.startswith('http://')} != NEGATIVES:
        raise ValueError("collateral_sitemap_change")
    return output


def write_allowed(path, body, branch, attempts, expected, sha):
    if (not branch.startswith(PREFIX) or body.get('branch') != branch or attempts != 0 or path != 'sitemap.xml'
            or not sha or body.get('sha') != sha):
        return False
    try:
        return base64.b64decode(body.get('content', ''), validate=True).decode('utf-8') == expected
    except (ValueError, TypeError, UnicodeError):
        return False


def rescore(report_path, preview, sitemap, output):
    report = projection.rescore(report_path, preview, SITE, sitemap, output)
    # This system check runs outside _score_issues in the auditor; use actual served locs.
    urls = sorted({url for url in locs(sitemap.decode('utf-8')) if url.startswith('http://')})
    report['issues'][KEY] = {'count': len(urls), 'examples': urls[:200]}
    live.save(output / 'report.json', report)
    return report


def checks(before, after):
    complete(before)
    complete(after)
    bp, ap = live._html_pages(before), live._html_pages(after)
    if bp.keys() != ap.keys() or len(bp) != 52:
        raise ValueError('html_routes_lost')
    for report, expected in ((before, TARGETS | NEGATIVES), (after, NEGATIVES)):
        block = report['issues'][KEY]
        if block.get('count') != len(expected) or set(block.get('examples', [])) != expected:
            raise ValueError('scheme_family_not_measured')
        for bench, count in ((previous, 0), (previous.previous, 0), (previous.previous.previous, 1),
                             (previous.previous.previous.previous, 2), (previous.previous.previous.previous.previous, 2)):
            if report['issues'][bench.KEY]['count'] != count:
                raise ValueError('previous_repair_or_negative_control_changed')
        rows = {row['url']: row for row in report['pages']}
        if not previous.TARGETS <= rows.keys() or any(rows[url].get('status_code') != 404 for url in previous.TARGETS):
            raise ValueError('missing_witnesses_lost')
    fields = titles.PRESERVED + ('title', 'title_tag_count', 'meta_description', 'meta_description_tag_count',
        'h1_tag_count', 'h2_tag_count', 'content_sketch', 'text_word_count', 'internal_link_items', 'ld_json_blocks',
        'meta_robots_tag_count', 'status_code', 'content_type', 'error', 'blocked_by_host')
    if any(old.get(field) != ap[route].get(field) for route, old in bp.items() for field in fields):
        raise ValueError('collateral_html_fields_changed')
    for key in (previous.previous.previous.KEY, previous.previous.previous.previous.KEY,
                previous.previous.previous.previous.previous.KEY):
        if set(before['issues'][key]['examples']) != set(after['issues'][key]['examples']):
            raise ValueError('negative_examples_changed')
    return {'html_routes': [52, 52], 'selected_occurrences': [3, 0], 'target_counts': {KEY: [5, 2]},
        'negative_http_entries_preserved': True, 'observed_html_fields_preserved': True,
        'previous_repairs_and_negative_controls_preserved': True,
        'increased_counts': {key: [before['issues'].get(key, {}).get('count', 0), value.get('count', 0)]
            for key, value in after['issues'].items() if value.get('count', 0) > before['issues'].get(key, {}).get('count', 0)}}


def cycle(m, token, work, budget):
    if budget.limit != 0:
        raise ValueError('nonzero_provider_budget')
    result = {'status': 'unverified', 'stage': 'identity', 'repository': f'{live.OWNER}/{REPO}', 'seed_sha': SEED,
        'pull_requests': [], 'writes': [], 'http_rechecks': [], 'universal_certification': False}
    original_put, original_probe = m._github_api_put, m._sitemap_https_page
    writable, expected, blob, baseline_preview = '', '', '', ''
    attempts = set()

    def get(*parts, **kwargs):
        return m._github_api_get(m._github_api_path('repos', live.OWNER, REPO, *parts), token=token, **kwargs)

    def ref(branch):
        return get('git', 'ref', 'heads', branch)['object']['sha']

    def read(path, sha):
        row = get('contents', path, params={'ref': sha})
        return row['sha'], base64.b64decode(row['content']).decode('utf-8')

    def put(api, **kwargs):
        if (api != m._github_content_api_path(live.OWNER, REPO, 'sitemap.xml') or len(attempts) >= 2
                or not write_allowed('sitemap.xml', kwargs.get('json_body', {}), writable, int(writable in attempts), expected, blob)):
            raise ValueError('outside_exact_qa_write_scope')
        attempts.add(writable)
        result['write_attempts'] = sorted(attempts)
        live.save(work / 'cycle.json', result)
        response = original_put(api, **kwargs)
        result['writes'].append({'branch': writable, 'path': 'sitemap.xml'})
        return response

    def probe(url, canonical):
        if url not in {'https:' + source[5:] for source in TARGETS} or not baseline_preview:
            raise ValueError('outside_exact_qa_http_scope')
        actual = baseline_preview.rstrip('/') + urlsplit(url).path
        original_get = m.requests.get
        def projected_get(value, **kwargs):
            if value != url:
                raise ValueError('unexpected_probe_url')
            response = original_get(actual, **kwargs)
            if response.url != actual or response.headers.get('x-robots-tag') != 'noindex' or response.history:
                response.close()
                raise ValueError('unexpected_native_preview_response')
            headers = response.headers.copy()
            del headers['x-robots-tag']
            return SimpleNamespace(status_code=response.status_code, url=url, history=response.history,
                headers=headers,
                close=response.close, iter_content=response.iter_content)
        m.requests.get = projected_get
        try:
            valid = original_probe(url, canonical)
        finally:
            m.requests.get = original_get
        result['http_rechecks'].append({'source': url, 'actual_preview_url': actual, 'verified': valid,
            'mode': 'owned_exact_host_and_native_noindex_header_projection'})
        return valid

    def branch(name, sha):
        if not name.startswith(PREFIX) or not m._github_branch_allowed(name):
            raise ValueError('outside_owned_qa_branch')
        m._github_api_post(m._github_api_path('repos', live.OWNER, REPO, 'git', 'refs'), token=token,
                          json_body={'ref': 'refs/heads/' + name, 'sha': sha})

    def preview(kind, head, sha):
        nonlocal baseline_preview
        result['stage'] = kind
        print('STAGE=' + kind, flush=True)
        pr = m._ouvrir_pull_request(owner=live.OWNER, repo=REPO, token=token, head=head, base='main', draft=True,
            title='QA sitemap HTTPS ' + kind + ' (never merge)', body='Owned fixture only. Keep negative entries. Close without merging.')
        row = {'number': pr['number'], 'url': pr['html_url'], 'kind': kind}
        result['pull_requests'].append(row)
        live.save(work / 'cycle.json', result)
        row['build'] = live.wait_build(m, REPO, token, pr['number'], expected_sha=sha)
        if ref(head) != sha or row['build']['head_sha'] != sha:
            raise ValueError('exact_head_changed')
        url = f"https://deploy-preview-{pr['number']}--{REPO}.netlify.app/"
        if kind == 'baseline':
            baseline_preview = url
        out = work / kind
        out.mkdir()
        xml = titles.preview_sitemap(url)
        (out / 'sitemap.xml').write_bytes(xml)
        observations = {}
        for source in sorted({'https:' + source[5:] for source in TARGETS | NEGATIVES}):
            response = live.requests.get(url.rstrip('/') + urlsplit(source).path, timeout=30, allow_redirects=False)
            name = urlsplit(source).path.rstrip('/').rsplit('/', 1)[-1]
            (out / (name + '.http')).write_bytes(response.content)
            observations[source] = {'status': response.status_code, 'location': response.headers.get('location', ''),
                'content_type': response.headers.get('content-type', ''), 'x_robots_tag': response.headers.get('x-robots-tag', '')}
        live.save(out / 'http.json', observations)
        live.crawl('static-html', url, out / 'raw', 90, preview=True)
        return rescore(out / 'raw/report.json', url, xml, out / 'controlled')

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
        paths = [p['path'] for p in tree['tree'] if p['type'] == 'blob']
        from backend import repo_index
        index = repo_index.build_repo_index(paths)
        blob, source = read('sitemap.xml', SEED)
        stamp = dt.datetime.now(dt.UTC).strftime('%Y%m%d-%H%M%S')
        baseline, correction = (PREFIX + kind + '-' + stamp for kind in ('baseline', 'correction'))
        result.update(main_sha=MAIN, baseline_branch=baseline, correction_branch=correction)
        branch(baseline, SEED)
        writable, expected = baseline, setup_source(source)
        put(m._github_content_api_path(live.OWNER, REPO, 'sitemap.xml'), token=token,
            json_body={'branch': baseline, 'sha': blob, 'message': 'QA sitemap HTTPS witnesses (never merge)',
                       'content': base64.b64encode(expected.encode()).decode()})
        baseline_sha = ref(baseline)
        result['baseline_sha'] = baseline_sha
        before = preview('baseline', baseline, baseline_sha)
        urls = before['issues'][KEY]['examples']
        prep = m._prepare_issue_fix(issue_key=KEY, issues=before['issues'], impacted=urls, all_paths=paths,
            site_name=urlsplit(SITE).netloc, owner=live.OWNER, repo_name=REPO, branch=baseline, token=token, pages=before['pages'])
        if prep['refusal'] or prep['rewriter_ai_fallback'] or {p['from'] for p in prep.get('sitemap_https_pairs', [])} != TARGETS:
            raise ValueError('https_destinations_not_verified')
        if any(url not in prep['side_effects'] for url in NEGATIVES):
            raise ValueError('refused_entries_not_named')
        branch(correction, baseline_sha)
        blob, source = read('sitemap.xml', baseline_sha)
        writable, expected = correction, expected_source(source)
        repair = m._apply_prepared_issue_fix(owner=live.OWNER, repo_name=REPO, branch=baseline, token=token, fix_branch=correction,
            all_paths=paths, issue_key=KEY, issue_label=KEY, impacted=urls, site_name=urlsplit(SITE).netloc, file_state={},
            max_files=1, prep=prep, pages=before['pages'], index=index, allow_ai_targeting=False)
        result['repair'] = repair
        if repair['patched'] != ['sitemap.xml'] or repair['skipped'] or repair['ai_files'] or repair['config_changes']:
            raise ValueError('unexpected_correction_scope')
        if len(result['http_rechecks']) != 3 or not all(row['verified'] for row in result['http_rechecks']):
            raise ValueError('missing_current_https_proof')
        final_sha = ref(correction)
        compare = get('compare', baseline_sha + '...' + final_sha)
        if compare['total_commits'] != 1 or [row['filename'] for row in compare['files']] != ['sitemap.xml']:
            raise ValueError('collateral_source_changed')
        if read('sitemap.xml', final_sha)[1] != expected or read('_redirects', final_sha)[1] != read('_redirects', SEED)[1]:
            raise ValueError('exact_source_check_failed')
        after = preview('final', correction, final_sha)
        result.update(status='measured_verified_sitemap_https_upgrades', final_sha=final_sha, checks=checks(before, after), sitemap_entries=[51, 49])
    except Exception as exc:
        result.update(status='failed', error_type=type(exc).__name__)
        if isinstance(exc, ValueError) and str(exc).replace('_', '').isalnum():
            result['failure_code'] = str(exc)
        print(result['status'], result['stage'], result.get('failure_code', result['error_type']), flush=True)
    finally:
        m._github_api_put, m._sitemap_https_page = original_put, original_probe
        result['ai_budget'] = budget.summary()
        result['cleanup'] = live.close_prs(REPO, token, result['pull_requests'])
        result['prs_closed_unmerged'] = closed_cleanup(result['pull_requests'], result['cleanup'])
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
    return int(result['status'] != 'measured_verified_sitemap_https_upgrades')


if __name__ == '__main__':
    raise SystemExit(main())
