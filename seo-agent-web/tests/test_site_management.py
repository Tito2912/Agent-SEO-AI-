"""Site creation retains input and context without running crawls or expanding ownership."""
from urllib.parse import parse_qs, urlsplit

from bs4 import BeautifulSoup
import pytest
from sqlalchemy import select

from backend import app as m
from backend.models import AccountMember, JobRecord, Project, UsageEvent
from test_customer_activation import client, lab as lab, project, user


@pytest.fixture()
def sites(lab, monkeypatch):
    monkeypatch.setattr(m.billing, 'plan_limits', lambda *a, **kw: {'projects': 3})
    return lab


def submit(c, **values):
    c.get('/')
    return c.post('/projects/add', data={'_csrf': c.cookies.get(m._CSRF_COOKIE_NAME),
                  'source': 'site-list', 'mode': 'domain', 'domain': 'atelier.example', **values}, follow_redirects=False)


def owned(database, owner):
    with database.session() as db:
        return list(db.scalars(select(Project).where(Project.owner_user_id == owner.id)))


@pytest.mark.parametrize('lang', ['fr', 'en'])
def test_add_form_has_a_single_address_explicit_scope_and_real_personal_limit(sites, lang):
    owner, host = user(sites['db']), user(sites['db'])
    project(sites['db'], owner, slug='own')
    project(sites['db'], host, slug='shared')
    with sites['db'].session() as db:
        db.add(AccountMember(owner_user_id=host.id, member_user_id=owner.id)); db.commit()
    page = client(owner).get('/?add_site=1&lang='+lang)
    assert page.status_code == 200 and page.headers['cache-control'] == 'no-store'
    soup = BeautifulSoup(page.text, 'html.parser')
    dialog = soup.select_one('#add-project-dialog')
    assert dialog['aria-labelledby'] == 'site-add-title' and dialog['data-auto-open'] == '1'
    assert len(dialog.select('input[name=domain]')) == 1 and not dialog.select('input[name=url]')
    assert dialog.select_one('input[name=_csrf]')['value']
    assert page.context['site_add']['count'] == 1 and page.context['site_add']['limit'] == 3
    assert soup.select_one('.project-select[value=own]')['data-owned'] == '1'
    assert soup.select_one('.project-select[value=shared]')['data-owned'] == '0'
    assert not dialog.select_one('#gsc-load')  # Existing administrative UI boundary.
    assert 'URL précise' not in dialog.get_text()


@pytest.mark.parametrize('address', ['atelier.example', 'https://atelier.example/service?q=1#contact'])
def test_add_opens_the_recorded_root_project_without_a_job_or_usage(sites, address):
    owner = user(sites['db'])
    response = submit(client(owner), domain=address, site_name='Atelier & Réparation')
    assert response.status_code == 303
    location = urlsplit(response.headers['location'])
    assert location.path == '/projects/atelier-example'
    assert parse_qs(location.query)['msg'] == ['Votre site est ajouté']
    rows = owned(sites['db'], owner)
    assert len(rows) == 1 and rows[0].base_url == 'https://atelier.example/'
    assert rows[0].site_name == 'Atelier & Réparation'
    with sites['db'].session() as db:
        assert list(db.scalars(select(JobRecord))) == list(db.scalars(select(UsageEvent))) == []


@pytest.mark.parametrize('address', ['https://atelier.example:wrong/', 'https://[broken/', 'https://name:password@atelier.example/', 'https://atelier.example/a b', 'ftp://atelier.example/', '', 'x'*2049])
def test_invalid_input_keeps_the_form_without_writing_or_exposing_a_trace(sites, address):
    owner = user(sites['db'])
    response = submit(client(owner), domain=address, site_name='Nom <script>test</script>')
    assert response.status_code == 400
    assert response.headers['cache-control'] == 'no-store' and response.headers['referrer-policy'] == 'no-referrer'
    soup = BeautifulSoup(response.text, 'html.parser')
    dialog = soup.select_one('#add-project-dialog')
    assert dialog['data-auto-open'] == '1' and dialog.select_one('[role=alert]')
    assert dialog.select_one('[name=domain]')['value'] == address[:2048]
    assert dialog.select_one('[name=site_name]')['value'] == 'Nom <script>test</script>'
    assert not dialog.select('script') and 'Traceback' not in response.text
    assert not owned(sites['db'], owner)


def test_refused_public_target_retains_input_and_does_not_write(sites, monkeypatch):
    owner = user(sites['db'])
    monkeypatch.setattr(m, '_validate_public_crawl_target', lambda value: 'Host non autorisé (adresse IP).')
    response = submit(client(owner), domain='127.0.0.1')
    assert response.status_code == 400 and not owned(sites['db'], owner)


def test_repeat_add_opens_existing_project_without_renaming_or_replacing_settings(sites):
    owner = user(sites['db']); c = client(owner)
    submit(c, site_name='Mon nom conservé')
    with sites['db'].session() as db:
        row = db.scalar(select(Project).where(Project.owner_user_id == owner.id))
        row.settings = {'github_repo': 'atelier/site', 'custom': 'kept'}; db.commit()
    second = submit(c, site_name='Ne pas renommer')
    assert second.headers['location'] == '/projects/atelier-example'
    rows = owned(sites['db'], owner)
    assert len(rows) == 1 and rows[0].site_name == 'Mon nom conservé' and rows[0].settings['custom'] == 'kept'


@pytest.mark.parametrize('mode', ['domain', 'url', 'gsc', 'bing', 'bulk'])
def test_shared_project_cannot_bypass_the_personal_site_limit(sites, monkeypatch, mode):
    owner, host = user(sites['db']), user(sites['db'])
    project(sites['db'], owner, slug='personal')
    project(sites['db'], host, slug='shared-example')
    with sites['db'].session() as db:
        db.add(AccountMember(owner_user_id=host.id, member_user_id=owner.id)); db.commit()
    monkeypatch.setattr(m.billing, 'plan_limits', lambda *a, **kw: {'projects': 1})
    response = submit(client(owner), mode=mode, domain='shared.example', url='https://shared.example/',
                      gsc_urls=['sc-domain:shared.example'] if mode=='gsc' else [],
                      bulk_urls=['https://shared.example/'] if mode in ('bing','bulk') else [])
    assert response.status_code == (409 if mode in ('domain','url') else 303)
    assert len(owned(sites['db'], owner)) == len(owned(sites['db'], host)) == 1
    if response.status_code == 303:
        assert parse_qs(urlsplit(response.headers['location']).query)['msg'] == ['Une limite de votre formule est atteinte']
    if response.status_code == 409:
        soup=BeautifulSoup(response.text, 'html.parser')
        assert soup.select_one('.site-add-form .btn.primary').has_attr('disabled')
        assert soup.select_one('.site-add-allowance a')['href'] == '/billing'


def test_bulk_ignores_malformed_addresses_and_counts_only_successful_new_records(sites):
    owner = user(sites['db'])
    response=submit(client(owner), mode='bulk', bulk_urls=['https://atelier.example:bad/', 'https://atelier.example/'])
    assert response.status_code == 303 and len(owned(sites['db'], owner)) == 1


def test_admin_keeps_import_and_bypasses_the_client_site_limit(sites, monkeypatch):
    admin=sites['admin']; project(sites['db'], admin)
    monkeypatch.setattr(m.billing, 'plan_limits', lambda *a, **kw: {'projects': 1})
    c=client(admin)
    page=c.get('/')
    assert BeautifulSoup(page.text,'html.parser').select_one('.site-add-import #gsc-load')
    assert submit(c).status_code == 303 and len(owned(sites['db'], admin)) == 2


def test_failed_recording_is_not_announced_as_success(sites, monkeypatch):
    owner=user(sites['db'])
    monkeypatch.setattr(m, '_db_upsert_project', lambda **kw: None)
    response=submit(client(owner))
    assert response.status_code == 503 and not owned(sites['db'], owner)
    assert BeautifulSoup(response.text,'html.parser').select_one('.site-add-form [role=alert]')


def test_legacy_import_returns_to_its_safe_account_screen(sites):
    owner=user(sites['db'])
    response=submit(client(owner), source='', mode='url', url='https://atelier.example/', next='/settings/accounts#github')
    assert urlsplit(response.headers['location']).path == '/settings/accounts'
    assert urlsplit(response.headers['location']).fragment == 'github'


def test_language_navigation_from_an_error_has_a_safe_get_entry(sites):
    owner=user(sites['db']); c=client(owner)
    response=submit(c, domain='https://atelier.example:bad/')
    soup=BeautifulSoup(response.text,'html.parser')
    english=next(a['href'] for a in soup.select('a[href]') if 'lang=en' in a['href'])
    result=c.get(english)
    assert result.status_code == 200 and result.context['site_add']['open']
    assert BeautifulSoup(result.text,'html.parser').html['lang']=='en'


@pytest.mark.parametrize('path', ['/projects/add','/projects/delete'])
def test_site_mutations_still_require_csrf(sites, path):
    owner=user(sites['db']); row=project(sites['db'],owner)
    c=client(owner);c.get('/')
    response=c.post(path,data={'mode':'domain','domain':'atelier.example','slugs':[row.slug]},follow_redirects=False)
    assert response.status_code == 403 and len(owned(sites['db'],owner))==1


def test_delete_keeps_shared_projects_and_does_not_move_report_files(sites):
    owner,host=user(sites['db']),user(sites['db'])
    own=project(sites['db'],owner,slug='personal'); shared=project(sites['db'],host,slug='shared')
    with sites['db'].session() as db:
        db.add(AccountMember(owner_user_id=host.id,member_user_id=owner.id));db.commit()
    report=m._runs_dir_for_user(owner.id)/own.slug/'audit'/'report.json';report.parent.mkdir(parents=True);report.write_text('{}')
    c=client(owner);c.get('/')
    response=c.post('/projects/delete',data={'_csrf':c.cookies.get(m._CSRF_COOKIE_NAME),'slugs':[own.slug,shared.slug]},follow_redirects=False)
    assert response.status_code==303 and not owned(sites['db'],owner)
    assert len(owned(sites['db'],host))==1 and report.exists()
