"""Assistance queues distinguish observed failures, normal progress and unknown history."""
from __future__ import annotations

import csv
import io
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlsplit

import pytest
from bs4 import BeautifulSoup
from sqlalchemy import event, select

from backend import app as m, customer_activation, customer_follow_up as support
from backend.models import AccountActivation, AccountMember, AuditLog, IssueTask, JobRecord, Project
from test_customer_activation import client, lab as lab, user
from test_current_plan_counts import subscription


def owner(lab, *, hours=96, **kwargs):
    return user(lab['db'], created=datetime.now(timezone.utc) - timedelta(hours=hours), **kwargs)


def site(lab, account, *, name='Atelier', slug=None, age=72):
    with lab['db'].session() as db:
        row = Project(owner_user_id=account.id, slug=slug or uuid.uuid4().hex, site_name=name,
                      base_url='https://atelier.example/', settings={},
                      created_at=datetime.now(timezone.utc) - timedelta(hours=age))
        db.add(row)
        db.commit()
        return row


def job(lab, account, project, *, status='failed', age=2, stamp=None, kind='crawl', finished=None):
    when = datetime.now(timezone.utc) - timedelta(hours=age)
    with lab['db'].session() as db:
        row = JobRecord(id=uuid.uuid4().hex, owner_user_id=account.id, slug=project.slug, kind=kind,
                        status=status, created_at=when.timestamp(), finished_at=finished,
                        result={'timestamp': stamp} if stamp else {'private': 'private-result'},
                        stdout='private-stdout', stderr='private-stderr')
        db.add(row)
        db.commit()
        return row


def report(lab, account, project, *, positive=True, partial=False, stamp=None):
    stamp = stamp or (datetime.now(timezone.utc) - timedelta(hours=1)).strftime('%Y%m%d-%H%M%S')
    payload = {'meta': {'base_url': project.base_url, 'stopped_on_time_budget': partial},
               'pages': [{'url': project.base_url, 'status_code': 200, 'content_type': 'text/html'}],
               'issues': {'missing_title': {'count': 1, 'examples': [project.base_url]}} if positive else {}}
    customer_activation.observe_report(lab['db'], project.slug, payload, user_id=account.id, crawl_ts=stamp)
    return stamp, payload


def counts(lab, **kwargs):
    data = support.snapshot(lab['db'], **kwargs)
    return data, {row['key']: row['count'] for row in data['groups']}


def task(lab, account, project, *, status='todo', key='missing_title', url='https://atelier.example/', age=0):
    with lab['db'].session() as db:
        db.add(IssueTask(project_id=project.id, user_id=account.id, issue_key=key, url=url,
                        status=status, crawl_ts=(datetime.now(timezone.utc) - timedelta(hours=1)).strftime('%Y%m%d-%H%M%S'),
                        note='private-user-note', created_at=datetime.now(timezone.utc) - timedelta(hours=age)))
        db.commit()


def test_four_categories_count_unique_accounts_and_keep_overlaps_explicit(lab):
    old, young, waiting, failed, clean, pending, selected = [owner(lab) for _ in range(7)]
    with lab['db'].session() as db:
        db.merge(young).created_at = datetime.now(timezone.utc) - timedelta(hours=1)
        db.commit()
    site(lab, waiting)
    failed_site = site(lab, failed)
    job(lab, failed, failed_site)
    for person, positive in [(clean, False), (pending, True), (selected, True)]:
        p = site(lab, person)
        stamp, _ = report(lab, person, p, positive=positive)
        job(lab, person, p, status='done', stamp=stamp)
        if person == selected:
            task(lab, selected, p)
    data, actual = counts(lab)
    assert actual == {'no_site_48h': 1, 'no_first_audit': 2, 'failed_analysis': 1, 'no_correction': 1}
    assert data['unique_count'] == 4 and data['unknown_count'] == 0 and data['total'] == 7
    assert {item['key'] for item in data['reasons'][failed.id]} == {'no_first_audit', 'failed_analysis'}
    assert not any(person.id in data['reasons'] for person in [young, clean, selected])


def test_48_hour_boundary_uses_registration_and_current_sites_not_missing_logins(lab):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    old, boundary, recent, future, deleted = [owner(lab) for _ in range(5)]
    with lab['db'].session() as db:
        for person, when in [(old, now - timedelta(hours=49)), (boundary, now - timedelta(hours=48)),
                             (recent, now - timedelta(hours=48) + timedelta(seconds=1)),
                             (future, now + timedelta(days=1))]:
            db.get(m.User, person.id).created_at = when
        db.get(AccountActivation, deleted.id).project_added_at = now - timedelta(hours=70)
        db.commit()
    data, actual = counts(lab, now=now)
    assert actual['no_site_48h'] == 3
    assert {uid for uid, reasons in data['reasons'].items() if any(r['key'] == 'no_site_48h' for r in reasons)} == {old.id, boundary.id, deleted.id}


def test_unknown_history_and_completed_jobs_without_report_evidence_are_not_failed_audits(lab):
    legacy, finished, active, canceled = [owner(lab) for _ in range(4)]
    with lab['db'].session() as db:
        db.delete(db.get(AccountActivation, legacy.id))
        db.commit()
    site(lab, legacy)
    job(lab, finished, site(lab, finished), status='done')
    job(lab, active, site(lab, active), status='queued')
    job(lab, canceled, site(lab, canceled), status='canceled')
    data, actual = counts(lab)
    assert actual == {'no_site_48h': 0, 'no_first_audit': 1, 'failed_analysis': 0, 'no_correction': 0}
    assert data['unknown_count'] == 2 and set(data['reasons']) == {canceled.id}


@pytest.mark.parametrize('new_status', ['queued', 'running', 'cancel_requested', 'done', 'canceled'])
def test_new_attempt_replaces_failed_attempt_for_the_same_site(lab, new_status):
    account = owner(lab)
    p = site(lab, account)
    job(lab, account, p, age=5)
    job(lab, account, p, status=new_status, age=1)
    assert counts(lab)[1]['failed_analysis'] == 0


def test_multiple_sites_owner_scope_job_kind_future_and_deleted_sites_are_respected(lab):
    first, second = owner(lab), owner(lab)
    a, b = site(lab, first, slug='same'), site(lab, second, slug='same')
    job(lab, first, a)
    job(lab, first, a, status='done', kind='competitor_crawl', age=1)
    job(lab, first, a, status='done', age=-5)
    job(lab, second, b, status='done')
    extra = site(lab, first)
    job(lab, first, extra)
    data, actual = counts(lab)
    assert actual['failed_analysis'] == 1
    assert len([r for r in data['reasons'][first.id] if r['key'] == 'failed_analysis']) == 2
    with lab['db'].session() as db:
        db.delete(db.get(Project, a.id))
        db.delete(db.get(Project, extra.id))
        db.commit()
    replacement = site(lab, first, slug='same', age=0)
    assert replacement.id != a.id and counts(lab)[1]['failed_analysis'] == 0


@pytest.mark.parametrize('option', ['clean', 'partial_empty', 'chosen', 'ignored', 'internal', 'invalid_url', 'foreign_url', 'future_task'])
def test_correction_queue_requires_positive_report_and_no_valid_technical_choice(lab, option):
    account = owner(lab)
    p = site(lab, account)
    stamp, _ = report(lab, account, p, positive=option not in {'clean', 'partial_empty'}, partial=option == 'partial_empty')
    job(lab, account, p, status='done', stamp=stamp)
    if option in {'chosen', 'ignored', 'internal', 'invalid_url', 'foreign_url', 'future_task'}:
        task(lab, account, p, status='ignored' if option == 'ignored' else 'todo',
             key='_article' if option == 'internal' else 'missing_title',
             url='javascript:private' if option == 'invalid_url' else ('https://foreign.example/' if option == 'foreign_url' else p.base_url),
             age=-5 if option == 'future_task' else 0)
    assert counts(lab)[1]['no_correction'] == int(option in {'ignored', 'internal', 'invalid_url', 'foreign_url', 'future_task'})


def test_invalid_issue_counts_remain_unknown_and_never_become_a_correction_to_choose(lab):
    account = owner(lab)
    p = site(lab, account)
    stamp, payload = report(lab, account, p)
    payload['issues']['missing_title']['count'] = 'not-a-count'
    customer_activation.observe_report(lab['db'], p.slug, payload, user_id=account.id,
        crawl_ts=(datetime.now(timezone.utc) - timedelta(minutes=30)).strftime('%Y%m%d-%H%M%S'))
    data, actual = counts(lab)
    assert actual['no_correction'] == 0 and data['unknown_count'] == 1


def test_old_or_out_of_order_report_does_not_replace_a_newer_clean_report(lab):
    account = owner(lab)
    p = site(lab, account)
    newest, _ = report(lab, account, p, positive=False)
    report(lab, account, p, stamp=(datetime.now(timezone.utc) - timedelta(hours=3)).strftime('%Y%m%d-%H%M%S'))
    job(lab, account, p, status='done', stamp=newest)
    assert counts(lab)[1]['no_correction'] == 0
    # A newer completed job without validated evidence makes its outcome unknown.
    job(lab, account, p, status='done', age=0, stamp='missing')
    assert counts(lab)[0]['unknown_count'] == 1


@pytest.mark.parametrize('new_address', ['https://changed.example/', 'https://[invalid'])
def test_changed_or_invalid_site_address_does_not_reuse_another_targets_report(lab, new_address):
    account = owner(lab)
    p = site(lab, account)
    stamp, _ = report(lab, account, p)
    job(lab, account, p, status='done', stamp=stamp)
    task(lab, account, p, url='https://foreign.example/')
    with lab['db'].session() as db:
        db.get(Project, p.id).base_url = new_address
        db.commit()
    data, actual = counts(lab)
    assert actual['no_correction'] == 0 and data['unknown_count'] == 1


def test_verifier_stores_only_minimal_facts_and_failure_cannot_rollback_milestones(lab, monkeypatch):
    account = owner(lab)
    p = site(lab, account)
    stamp, payload = report(lab, account, p)
    with lab['db'].session() as db:
        record = db.scalar(select(AuditLog).where(AuditLog.action == support.ACTION))
        assert record.actor_user_id == account.id and record.target_id == p.id
        assert record.meta == {'version': 1, 'crawl_ts': stamp, 'positive_issues': True, 'clean': False,
                               'target_hash': support.target_hash(p.base_url)}
        assert record.actor_email is None and record.ip_address is None and record.user_agent is None
    monkeypatch.setattr(support, 'observe', lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError('isolated telemetry failure')))
    m._verify_corrections_after_crawl(p.slug, payload, user_id=account.id, crawl_ts=stamp)
    with lab['db'].session() as db:
        assert db.get(AccountActivation, account.id).audit_available_at


@pytest.mark.parametrize('case', ['invalid_report', 'foreign_host', 'future_stamp', 'older_than_site', 'ambiguous_slug'])
def test_unusable_or_invalid_evidence_never_enters_the_correction_queue(lab, case):
    account = owner(lab)
    p = site(lab, account, slug='same')
    stamp = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
    payload = {'issues': {'missing_title': {'count': 1}}, 'pages': [{'url': p.base_url, 'status_code': 200, 'content_type': 'text/html'}]}
    if case == 'invalid_report':
        payload['pages'] = []
    elif case == 'foreign_host':
        payload['pages'][0]['url'] = 'https://other.example/'
    elif case == 'future_stamp':
        stamp = (datetime.now(timezone.utc) + timedelta(days=1)).strftime('%Y%m%d-%H%M%S')
    elif case == 'older_than_site':
        stamp = (datetime.now(timezone.utc) - timedelta(days=5)).strftime('%Y%m%d-%H%M%S')
    else:
        site(lab, owner(lab), slug='same')
    customer_activation.observe_report(lab['db'], p.slug, payload,
                                       user_id=None if case == 'ambiguous_slug' else account.id, crawl_ts=stamp)
    assert counts(lab)[1]['no_correction'] == 0
    with lab['db'].session() as db:
        assert not db.scalar(select(AuditLog.id).where(AuditLog.action == support.ACTION))


def test_count_links_filter_table_and_csv_with_country_plan_dates_and_pagination(lab):
    for index in range(29):
        person = owner(lab, email=f'follow-up-{index:02d}@example.invalid')
        if index == 0:
            subscription(lab['db'], person, 'solo')
    data, actual = counts(lab)
    c = client(lab['admin'])
    link = next(g['url'] for g in data['groups'] if g['key'] == 'no_site_48h')
    first = c.get(link)
    assert first.status_code == 200 and actual['no_site_48h'] == first.context['inventory']['matched'] == 29
    inventory = first.context['inventory']
    assert len(inventory['rows']) == 25
    second = c.get(inventory['next_url'])
    assert len(second.context['inventory']['rows']) == 4
    assert parse_qs(urlsplit(inventory['next_url']).query)['attention'] == ['no_site_48h']
    response = c.get(inventory['export_url'])
    rows = list(csv.DictReader(io.StringIO(response.content.decode('utf-8-sig')), delimiter=';'))
    assert len(rows) == 29 and all('Sans site après 48 h' in row['Situations à accompagner, sites et dates (Paris)'] for row in rows)
    assert all(row['Nom du pays'] == 'Non renseigné' for row in rows)
    assert len(c.get(link + '&plan=solo&q=follow-up-00&usage=without_site').context['inventory']['rows']) == 1
    assert response.headers['cache-control'] == 'no-store' and response.headers['referrer-policy'] == 'no-referrer'


def test_owner_only_views_api_separation_and_escaped_sites_and_formula_safe_exports(lab):
    account = owner(lab)
    p = site(lab, account, name='=2+3<script>private-site</script>')
    job(lab, account, p)
    c = client(lab['admin'])
    page = c.get('/settings/activation/accounts?attention=failed_analysis')
    assert '&lt;script&gt;' in page.text and '<script>private-site</script>' not in page.text
    assert account.email not in c.get('/settings/activation').text
    for url in ['/api/settings/activation', '/api/settings/activation/plans', '/settings/activation.csv']:
        body = c.get(url).text
        assert account.email not in body and 'private-site' not in body
    for url in ['/settings/activation', '/settings/activation/accounts?attention=failed_analysis',
                '/settings/activation/accounts.csv?attention=failed_analysis']:
        for outsider in [None, account, user(lab['db'], admin=True)]:
            response = client(outsider).get(url, follow_redirects=False)
            assert response.status_code in (303, 403) and account.email not in response.text
    text = c.get('/settings/activation/accounts.csv?attention=failed_analysis').text
    assert all(secret not in text for secret in ['private-stdout', 'private-stderr', 'private-result', 'private-user-note'])


def test_population_excludes_admins_and_team_only_members_and_deletions_are_current(lab):
    host, member, personal = owner(lab), owner(lab), owner(lab)
    site(lab, host)
    site(lab, personal)
    with lab['db'].session() as db:
        db.add_all([AccountMember(owner_user_id=host.id, member_user_id=member.id),
                    AccountMember(owner_user_id=host.id, member_user_id=personal.id)])
        db.commit()
    data, _ = counts(lab)
    assert data['total'] == 2 and member.id not in data['reasons'] and lab['admin'].id not in data['reasons']
    with lab['db'].session() as db:
        db.delete(db.get(m.User, personal.id))
        db.commit()
    assert counts(lab)[0]['total'] == 1


def test_reader_uses_five_selects_without_report_files_providers_or_sensitive_columns(lab):
    for _ in range(8):
        person = owner(lab)
        p = site(lab, person)
        report(lab, person, p)
        job(lab, person, p)
        task(lab, person, p)
    statements = []
    def observed(connection, cursor, statement, parameters, context, many):
        statements.append(statement)
    event.listen(lab['db'].engine, 'before_cursor_execute', observed)
    try:
        data, _ = counts(lab)
    finally:
        event.remove(lab['db'].engine, 'before_cursor_execute', observed)
    assert data['total'] == 8 and len(statements) == 5
    assert all(sql.lstrip().upper().startswith('SELECT') for sql in statements)
    sql = '\n'.join(statements)
    assert all(name not in sql for name in ['password_hash', 'secret_value', 'stdout', 'stderr', 'token_hash', 'stripe_data', 'issue_tasks.note'])


def test_invalid_filter_and_unavailable_data_never_become_zero_counts_or_leak_details(lab, monkeypatch):
    c = client(lab['admin'])
    for path in ['/settings/activation/accounts', '/settings/activation/accounts.csv']:
        assert c.get(path + '?attention=unknown').status_code == 400
    monkeypatch.setattr(support, 'snapshot', lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError('private-database-location')))
    page = c.get('/settings/activation')
    assert page.status_code == 200 and page.context['follow_up'] is None
    soup = BeautifulSoup(page.text, 'html.parser')
    assert 'momentanément indisponibles' in soup.select_one('.customer-follow-up').text
    assert not soup.select('.follow-up-group') and 'private-database-location' not in page.text
    for path in ['/settings/activation/accounts', '/settings/activation/accounts.csv']:
        response = c.get(path + '?attention=any')
        assert response.status_code == 503 and 'private-database-location' not in response.text
