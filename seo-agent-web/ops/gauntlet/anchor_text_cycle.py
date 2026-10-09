"""Measure one proved accessible name while retaining an unproved empty link."""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import html
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ops.gauntlet import hreflang_drop_cycle as previous  # noqa: E402
from ops.gauntlet.ai_budget import ClaudeBudget  # noqa: E402

live, titles, shared = previous.live, previous.titles, previous.shared
REPO, MAIN, SITE = previous.REPO, previous.MAIN, previous.SITE
SEED_BRANCH = 'gauntlet-static-html-href-drop-correction-20261007-164104'
SEED, SEED_PR = 'd949b4d86a2eacc123a4ec475bf2def4f35df863', 86
PREFIX, KEY = 'gauntlet-static-html-anchor-', 'links_with_no_anchor_text'
ROUTES = ['/gauntlet/qa-anchor-' + kind for kind in ('source', 'target', 'unproved')]
SOURCE, TARGET, NEGATIVE = [SITE.rstrip('/') + route for route in ROUTES]
UNKNOWN = previous.UNKNOWN
FILES = [route.lstrip('/') + '.html' for route in ROUTES]
FILE, INDEX = FILES[0], 'gauntlet/index.html'
NAME = 'Accessible name control TARGET'
ITEM = {'page': SOURCE, 'field': ROUTES[1], 'value': NAME}
LINK = '<a href="' + ROUTES[1] + '"><svg class="icon" /></a>'


def fixture(kind):
    canonical = {'source': SOURCE, 'target': TARGET, 'unproved': NEGATIVE}[kind]
    old = previous.previous
    raw = old.fixture('fr', canonical, False).replace(
        'Canonical translation acceptance control FR', 'Accessible name control ' + kind.upper())
    for code, href in (('fr', old.SOURCE), ('en', old.NEW)):
        line = '  <link rel="alternate" hreflang="' + code + '" href="' + href + '" />\n'
        if raw.count(line) != 1:
            raise ValueError('unexpected_fixture_alternate')
        raw = raw.replace(line, '', 1)
    if kind != 'target':
        link = LINK if kind == 'source' else LINK.replace(ROUTES[1], urlsplit(UNKNOWN).path)
        raw = raw.replace('</body>', '  ' + link + '\n  <!-- decoy ' + link + ' -->\n</body>', 1)
    return raw


def setup_sources(index):
    if index.count('    </ul>') != 1 or any(route in index for route in ROUTES):
        raise ValueError('unexpected_pinned_fixture_index')
    links = ''.join('      <li><a href="' + route + '">' + route.rsplit('/', 1)[1] + '</a></li>\n' for route in ROUTES)
    return {INDEX: index.replace('    </ul>', links + '    </ul>', 1),
            **{path: fixture(kind) for path, kind in zip(FILES, ('source', 'target', 'unproved'))}}


def expected_source(raw):
    if raw != fixture('source'):
        raise ValueError('unexpected_anchor_control_source')
    opening = '<a href="' + ROUTES[1] + '"'
    return raw.replace(opening, opening + ' aria-label="' + html.escape(NAME, quote=True) + '"', 1)


def write_allowed(path, body, branch, allowed, shas, attempts):
    if (not branch.startswith(PREFIX) or body.get('branch') != branch or path not in allowed
            or attempts.get(path, 0) or body.get('sha') != shas.get(path)):
        return False
    try:
        return base64.b64decode(body.get('content', ''), validate=True).decode('utf-8') == allowed[path]
    except (ValueError, TypeError, UnicodeError):
        return False


def empty_link(page, target, href):
    return {'source_url': page, 'target_url': target, 'href': href, 'internal': True,
            'anchor_text': '', 'title': '', 'aria_label': '', 'rel': ''}


def checks(before, after, *, before_preview, after_preview):
    shared.complete(before)
    shared.complete(after)
    bp, ap = live._html_pages(before), live._html_pages(after)
    if bp.keys() != ap.keys() or len(bp) != 61:
        raise ValueError('html_routes_lost')
    for report, count in ((before, 2), (after, 1)):
        block = report['issues'][KEY]
        if (type(block.get('count')) is not int or block['count'] != count
                or sorted(block.get('examples', [])) != sorted([NEGATIVE] + ([SOURCE] if count == 2 else []))):
            raise ValueError('family_or_unproved_control_changed')
        for url, kind in zip((SOURCE, TARGET, NEGATIVE), ('source', 'target', 'unproved')):
            rows = [row for row in report['pages'] if row['url'] == url]
            expected_links = ([empty_link(SOURCE, TARGET, ROUTES[1])] if count == 2 else []) if url == SOURCE else (
                [empty_link(NEGATIVE, UNKNOWN, urlsplit(UNKNOWN).path)] if url == NEGATIVE else [])
            if not rows or any(type(row.get('status_code')) is not int or row['status_code'] != 200
                    or row.get('canonical') != url or row.get('final_url') != url
                    or row.get('lang') != 'fr' or row.get('served_lang') is not None
                    or row.get('h1') != ['Accessible name control ' + kind.upper()]
                    or type(row.get('h1_tag_count')) is not int or row['h1_tag_count'] != 1
                    or row.get('title') != 'Accessible name control ' + kind.upper()
                    or type(row.get('title_tag_count')) is not int or row['title_tag_count'] != 1
                    or row.get('links_without_anchor_text') != expected_links for row in rows):
                raise ValueError('literal_or_unproved_control_changed')
        unknown = [row for row in report['pages'] if row['url'] == UNKNOWN]
        if not unknown or any(row.get('lang') is not None or row.get('served_lang') is not None for row in unknown):
            raise ValueError('unknown_target_language_invented')
    fields = titles.PRESERVED + (
        'title', 'title_tag_count', 'meta_description', 'meta_description_tag_count', 'h1_tag_count', 'h2_tag_count',
        'content_sketch', 'text_word_count', 'internal_link_items', 'ld_json_blocks', 'meta_robots_tag_count',
        'status_code', 'content_type', 'error', 'blocked_by_host', 'meta_viewport', 'meta_viewport_tag_count',
        'og_title', 'og_description', 'twitter_title', 'twitter_description')
    def observations(report):
        rows = []
        for row in report['pages']:
            if row['url'].startswith(SITE):
                selected = fields + ('url', 'final_url', 'redirect_chain', 'redirect_statuses')
                if row['url'] != SOURCE:
                    selected += ('links_without_anchor_text',)
                rows.append(json.dumps({key: row.get(key) for key in selected}, sort_keys=True))
        return sorted(rows)
    if observations(before) != observations(after):
        raise ValueError('collateral_observations_changed')
    for key, block in before['issues'].items():
        if key == KEY:
            continue
        new = after['issues'].get(key, {})
        if (block.get('count', 0) != new.get('count', 0)
                or shared.preview_examples(block.get('examples', []), before_preview) != shared.preview_examples(new.get('examples', []), after_preview)):
            raise ValueError('other_issue_counts_or_examples_changed')
    if any(block.get('count', 0) > before['issues'].get(key, {}).get('count', 0) for key, block in after['issues'].items()):
        raise ValueError('issue_count_increased')
    return {'html_routes': [61, 61], 'whole_family_counts': [2, 1], 'selected_counts': [1, 0],
        'unproved_controls': [1, 1], 'one_aria_label_only': True,
        'requested_observation_multiset_preserved': True, 'increased_counts': {}}


def cycle(m, token, work, budget):
    if budget.limit != 0:
        raise ValueError('nonzero_provider_budget')
    result = {'status': 'unverified', 'repository': f'{live.OWNER}/{REPO}', 'seed_sha': SEED,
        'pull_requests': [], 'writes': [], 'http_rechecks': [], 'preview_urls': {}, 'universal_certification': False}
    original_put, original_probe = m._github_api_put, m._anchor_text_page
    writable, allowed, shas, attempts, baseline_preview = '', {}, {}, {}, ''
    def get(*parts, **kw):
        return m._github_api_get(m._github_api_path('repos', live.OWNER, REPO, *parts), token=token, **kw)
    def ref(branch):
        return get('git', 'ref', 'heads', branch)['object']['sha']
    def read(path, sha):
        row = get('contents', path, params={'ref': sha})
        return row['sha'], base64.b64decode(row['content']).decode('utf-8')
    def branch(name, sha):
        if not name.startswith(PREFIX) or not m._github_branch_allowed(name):
            raise ValueError('outside_owned_qa_branch')
        m._github_api_post(m._github_api_path('repos', live.OWNER, REPO, 'git', 'refs'), token=token,
            json_body={'ref': 'refs/heads/' + name, 'sha': sha})
    def put(api, **kw):
        path = next((path for path in allowed if api == m._github_content_api_path(live.OWNER, REPO, path)), None)
        if not write_allowed(path, kw.get('json_body', {}), writable, allowed, shas, attempts):
            raise ValueError('outside_exact_qa_write_scope')
        attempts[path] = attempts.get(path, 0) + 1
        result['write_attempts'] = result.get('write_attempts', 0) + 1
        live.save(work / 'cycle.json', result)
        response = original_put(api, **kw)
        result['writes'].append({'branch': writable, 'path': path, 'stage': result['stage']})
        return response
    def probe(url, canonical, observed, items):
        if url not in {SOURCE, TARGET} or not baseline_preview:
            raise ValueError('outside_exact_qa_http_scope')
        actual, original_get = baseline_preview.rstrip('/') + urlsplit(url).path, m.requests.get
        def projected_get(value, **kw):
            if value != url:
                raise ValueError('unexpected_probe_url')
            response = original_get(actual, **kw)
            if response.url != actual or response.headers.get('x-robots-tag') != 'noindex' or response.history:
                response.close()
                raise ValueError('unexpected_native_preview_response')
            headers = response.headers.copy()
            del headers['x-robots-tag']
            return SimpleNamespace(status_code=response.status_code, url=url, history=response.history,
                headers=headers, close=response.close, iter_content=response.iter_content)
        m.requests.get = projected_get
        try:
            valid = original_probe(url, canonical, observed, items)
        finally:
            m.requests.get = original_get
        result['http_rechecks'].append({'source': url, 'actual_preview_url': actual, 'verified': valid,
            'mode': 'owned_exact_host_and_native_noindex_header_projection'})
        return valid
    def preview(kind, head, sha):
        nonlocal baseline_preview
        result['stage'] = kind
        print('STAGE=' + kind, flush=True)
        pr = m._ouvrir_pull_request(owner=live.OWNER, repo=REPO, token=token, head=head, base='main', draft=True,
            title='QA anchor accessible name ' + kind + ' (never merge)', body='Owned bounded controls only. Close without merging.')
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
        for number, route in enumerate(ROUTES):
            response = live.requests.get(base.rstrip('/') + route, timeout=30, allow_redirects=False)
            try:
                if response.status_code != 200 or response.headers.get('location') or response.headers.get('x-robots-tag') != 'noindex':
                    raise ValueError('native_preview_policy_changed')
                (out / (str(number) + '.http')).write_bytes(response.content)
            finally:
                response.close()
        live.crawl('static-html', base, out / 'raw', 90, preview=True)
        return shared.previous.rescore(out / 'raw/report.json', base, xml, out / 'controlled')
    m._github_api_put, m._anchor_text_page = put, probe
    try:
        result['stage'] = 'identity'
        if ref('main') != MAIN or ref(SEED_BRANCH) != SEED:
            raise ValueError('fixture_identity_changed')
        parent = get('pulls', str(SEED_PR))
        if parent['state'] != 'closed' or parent['merged'] or not parent['draft'] or parent['head']['sha'] != SEED:
            raise ValueError('unverified_seed_pr')
        live.wait_build(m, REPO, token, SEED_PR, expected_sha=SEED)
        tree = get('git', 'trees', SEED, params={'recursive': '1'})
        if tree.get('truncated') or get('compare', MAIN + '...' + SEED)['merge_base_commit']['sha'] != MAIN:
            raise ValueError('unverified_seed_tree_or_ancestry')
        paths = [row['path'] for row in tree['tree'] if row['type'] == 'blob']
        if any(path in paths for path in FILES):
            raise ValueError('control_sources_already_exist')
        index_sha, index_raw = read(INDEX, SEED)
        allowed, shas = setup_sources(index_raw), {INDEX: index_sha}
        stamp = dt.datetime.now(dt.UTC).strftime('%Y%m%d-%H%M%S')
        baseline, correction = (PREFIX + kind + '-' + stamp for kind in ('baseline', 'correction'))
        result.update(main_sha=MAIN, baseline_branch=baseline, correction_branch=correction, stage='setup')
        branch(baseline, SEED)
        writable = baseline
        for path, raw in allowed.items():
            body = {'branch': baseline, 'message': 'test(seo): bounded empty anchor controls', 'content': base64.b64encode(raw.encode()).decode()}
            if path in shas:
                body['sha'] = shas[path]
            m._github_api_put(m._github_content_api_path(live.OWNER, REPO, path), token=token, json_body=body)
        baseline_sha = ref(baseline)
        result['baseline_sha'] = baseline_sha
        before = preview('baseline', baseline, baseline_sha)
        paths += FILES
        prep = m._prepare_issue_fix(issue_key=KEY, issues=before['issues'], impacted=[SOURCE, NEGATIVE], all_paths=paths,
            site_name=urlsplit(SITE).netloc, owner=live.OWNER, repo_name=REPO, branch=baseline, token=token, pages=before['pages'])
        if prep['refusal'] or prep['rewriter_ai_fallback'] or prep.get('anchor_text_items') != [ITEM] or NEGATIVE not in prep['side_effects']:
            raise ValueError('proved_group_or_named_refusal_missing')
        branch(correction, baseline_sha)
        source_sha, source_raw = read(FILE, baseline_sha)
        writable, allowed, shas, attempts = correction, {FILE: expected_source(source_raw)}, {FILE: source_sha}, {}
        result['stage'] = 'correction'
        from backend import repo_index
        repair = m._apply_prepared_issue_fix(owner=live.OWNER, repo_name=REPO, branch=baseline, token=token, fix_branch=correction,
            all_paths=paths, issue_key=KEY, issue_label=KEY, impacted=[SOURCE, NEGATIVE], site_name=urlsplit(SITE).netloc,
            file_state={}, max_files=1, prep=prep, pages=before['pages'], index=repo_index.build_repo_index(paths), allow_ai_targeting=False)
        result['repair'] = repair
        if repair['patched'] != [FILE] or repair['skipped'] or repair['ai_files'] or repair['config_changes']:
            raise ValueError('unexpected_repair_scope')
        if [row['source'] for row in result['http_rechecks']] != [SOURCE, TARGET] or not all(row['verified'] for row in result['http_rechecks']):
            raise ValueError('missing_current_https_proofs')
        final_sha = ref(correction)
        result['final_sha'] = final_sha
        compare = get('compare', baseline_sha + '...' + final_sha)
        if compare['total_commits'] != 1 or [row['filename'] for row in compare['files']] != [FILE] or read(FILE, final_sha)[1] != allowed[FILE]:
            raise ValueError('collateral_source_changed')
        after = preview('final', correction, final_sha)
        result.update(status='measured_verified_anchor_name_subset', checks=checks(before, after,
            before_preview=result['preview_urls']['baseline'], after_preview=result['preview_urls']['final']))
    except Exception as exc:
        result.update(status='failed', error_type=type(exc).__name__)
        if isinstance(exc, ValueError) and str(exc).replace('_', '').isalnum():
            result['failure_code'] = str(exc)
        print(result['status'], result['stage'], result.get('failure_code', result['error_type']), flush=True)
    finally:
        m._github_api_put, m._anchor_text_page = original_put, original_probe
        result['ai_budget'] = budget.summary()
        result['cleanup'] = live.close_prs(REPO, token, result['pull_requests'])
        result['prs_closed_unmerged'] = shared.closed_cleanup(result['pull_requests'], result['cleanup'])
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
    return int(result['status'] != 'measured_verified_anchor_name_subset')


if __name__ == '__main__':
    raise SystemExit(main())
