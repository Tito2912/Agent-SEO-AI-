"""A public guide may resume an explicitly authorized site, never choose a write target."""
import json
import os
import uuid
from urllib.parse import parse_qs, urlsplit

from bs4 import BeautifulSoup
from fastapi.testclient import TestClient
import pytest

from backend import app as m, auth, content_library, help_navigation
from backend.models import AccountMember, Project, User

STAMP = "20261101-090000"


@pytest.fixture()
def help_site(tmp_path, monkeypatch):
    m.DB.create_tables()
    tag = uuid.uuid4().hex
    with m.DB.session() as db:
        users = [User(email=f"help-{kind}-{tag}@example.invalid", password_hash="fixture", is_admin=False)
                 for kind in ("owner", "member", "outsider")]
        db.add_all(users)
        db.commit()
        owner, member, outsider = [str(user.id) for user in users]
        site = Project(owner_user_id=owner, slug=f"atelier-{tag}", site_name='Atelier <clair> & net',
                       base_url="https://atelier.example/", settings={})
        db.add(site)
        db.add(AccountMember(owner_user_id=owner, member_user_id=member))
        db.commit()
        slug = site.slug
    monkeypatch.setattr(m, "_runs_dir_for_user", lambda uid: tmp_path / uid)
    report = tmp_path / owner / slug / STAMP / "audit" / "report.json"
    report.parent.mkdir(parents=True)
    report.write_text(json.dumps({"pages": [], "issues": {}}))

    def client(uid=None):
        result = TestClient(m.app)
        if uid:
            result.cookies.set(auth.SESSION_COOKIE_NAME,
                auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
        return result

    return {"owner": client(owner), "member": client(member), "outsider": client(outsider),
            "anonymous": client(), "slug": slug, "owner_id": owner, "member_id": member, "report": report}


@pytest.mark.parametrize("who", ["owner", "member"])
@pytest.mark.parametrize("lang", ["fr", "en"])
def test_help_returns_to_the_same_authorized_site_and_analysis(help_site, who, lang):
    client, slug = help_site[who], help_site["slug"]
    source = client.get(f"/projects/{slug}/issues?crawl={STAMP}&lang={lang}")
    soup = BeautifulSoup(source.text, "html.parser")
    help_link = next(a["href"] for a in soup.select('main a[href]') if "/docs/anomalies-et-priorites" in a["href"])
    assert parse_qs(urlsplit(help_link).query) == {"project": [slug], "crawl": [STAMP]}
    article = client.get(help_link)
    soup = BeautifulSoup(article.text, "html.parser")
    assert article.status_code == 200 and soup.html["lang"] == lang
    assert soup.select_one('.content-header .btn')["href"] == f"/projects/{slug}/issues?crawl={STAMP}"
    assert soup.select_one('[data-help-project] strong').get_text() == 'Atelier <clair> & net'
    assert not soup.select('[data-help-project] clair')
    assert article.headers["cache-control"] == "private, no-store"
    assert article.headers["referrer-policy"] == "same-origin"
    for link in soup.select('.content-aside a[href], .content-related a[href], .content-body a[href], .lang-switch a[href]'):
        if help_navigation.is_help_path(urlsplit(link["href"]).path):
            query = parse_qs(urlsplit(link["href"]).query)
            assert query["project"] == [slug] and query["crawl"] == [STAMP]
    metadata = str(soup.head)
    assert slug not in metadata and STAMP not in metadata


@pytest.mark.parametrize("who", ["anonymous", "outsider"])
def test_public_help_does_not_disclose_an_inaccessible_site(help_site, who):
    response = help_site[who].get(f'/docs/anomalies-et-priorites?project={help_site["slug"]}&crawl={STAMP}')
    soup = BeautifulSoup(response.text, "html.parser")
    assert response.status_code == 200 and not soup.select('[data-help-project]')
    assert help_site["slug"] not in response.text and "Atelier &lt;clair&gt;" not in response.text
    if who != "anonymous":
        assert soup.select_one('.content-header .btn')["href"] == "/"


def test_revoked_membership_is_rechecked_on_the_next_help_request(help_site):
    path = f'/docs/lire-le-rapport?project={help_site["slug"]}'
    assert help_site["member"].get(path).context["help_project"]
    with m.DB.session() as db:
        for member in db.query(AccountMember).filter_by(owner_user_id=help_site["owner_id"], member_user_id=help_site["member_id"]):
            db.delete(member)
        db.commit()
    assert help_site["member"].get(path).context["help_project"] is None


@pytest.mark.parametrize("query", ["project=unknown", "project=//outside.example", "project={slug}&project={slug}"])
def test_unknown_or_ambiguous_context_keeps_the_generic_help(help_site, query):
    response = help_site["owner"].get('/docs/lire-le-rapport?' + query.format(slug=help_site["slug"]))
    assert response.status_code == 200 and response.context["help_project"] is None
    assert BeautifulSoup(response.text, "html.parser").select_one('.content-header .btn')["href"] == "/"


@pytest.mark.parametrize("stamp", ["20260101-010101", "../../private", STAMP + "&token=secret"])
def test_only_an_existing_report_of_the_authorized_site_can_travel(help_site, stamp):
    response = help_site["owner"].get('/docs/lire-le-rapport', params={"project": help_site["slug"], "crawl": stamp, "token": "private-sentinel"})
    assert response.context["help_project"]["crawl"] == ""
    assert "private-sentinel" not in response.text
    assert BeautifulSoup(response.text, "html.parser").select_one('.content-header .btn')["href"] == f'/projects/{help_site["slug"]}'


@pytest.mark.parametrize('who', ['owner', 'member'])
def test_persisted_report_is_restored_once_before_preserving_the_selected_analysis(help_site, monkeypatch, who):
    report = help_site['report']
    report.unlink()
    restored = []

    def restore(path):
        assert path == report
        restored.append(path)
        path.write_text(json.dumps({'pages': [], 'issues': {}}))
        return True

    monkeypatch.setattr(m, '_ensure_runs_artifact_local', restore)
    response = help_site[who].get(f'/docs/anomalies-et-priorites?project={help_site["slug"]}&crawl={STAMP}')
    assert response.status_code == 200 and response.context['help_project']['crawl'] == STAMP
    assert len(restored) == 1
    assert BeautifulSoup(response.text, 'html.parser').select_one('.content-header .btn')['href'] == f'/projects/{help_site["slug"]}/issues?crawl={STAMP}'


@pytest.mark.parametrize('who', ['anonymous', 'outsider'])
def test_persisted_artifact_is_never_requested_before_project_authorization(help_site, monkeypatch, who):
    help_site['report'].unlink()

    def forbidden(path):
        raise AssertionError('Unauthorized request must not access object storage')

    monkeypatch.setattr(m, '_ensure_runs_artifact_local', forbidden)
    response = help_site[who].get(f'/docs/lire-le-rapport?project={help_site["slug"]}&crawl={STAMP}')
    assert response.status_code == 200 and response.context['help_project'] is None


@pytest.mark.parametrize("slug,screen", [
    ("lire-le-rapport", ""), ("corrections-automatiques", "/corrections"),
    ("corriger-dans-wordpress", "/cms"), ("verifier-une-correction", "/first-correction"),
    ("lancer-un-crawl", "/settings/crawl"), ("analyser-les-concurrents", "/competitors"),
    ("rediger-du-contenu", "/content"), ("suivre-la-performance", "/performance"),
])
def test_doc_ctas_open_read_screens_without_starting_operations(help_site, slug, screen):
    response = help_site["owner"].get(f'/docs/{slug}?project={help_site["slug"]}')
    assert BeautifulSoup(response.text, "html.parser").select_one('.content-header .btn')["href"] == f'/projects/{help_site["slug"]}{screen}'


def test_account_settings_are_distinct_from_project_settings_and_new_project_entry(help_site):
    for doc, destination in [("connecter-github", "/settings/accounts#github-connect-card"),
                             ("connecter-search-console", "/settings/accounts#gsc-oauth-card"),
                             ("comptes-d-equipe", "/settings/team"), ("ajouter-un-projet", "/")]:
        response = help_site["owner"].get(f'/docs/{doc}?project={help_site["slug"]}')
        assert BeautifulSoup(response.text, "html.parser").select_one('.content-header .btn')["href"] == destination


def test_guide_directory_related_pages_and_language_keep_only_the_bounded_context(help_site):
    client, slug = help_site["owner"], help_site["slug"]
    response = client.get(f'/en/docs?project={slug}&crawl={STAMP}&email=private-sentinel')
    soup = BeautifulSoup(response.text, "html.parser")
    guide_link = next(a['href'] for a in soup.select('main a[href]') if urlsplit(a['href']).path.endswith('/ressources-seo'))
    directory = client.get(guide_link)
    soup = BeautifulSoup(directory.text, "html.parser")
    article_link = next(a['href'] for a in soup.select('main a[href]') if '/ressources-seo/audit-seo-technique-checklist-priorites' in a['href'])
    article = client.get(article_link)
    soup = BeautifulSoup(article.text, "html.parser")
    assert soup.select_one('.content-cta .btn.primary')['href'] == f'/projects/{slug}/issues?crawl={STAMP}'
    assert 'private-sentinel' not in response.text + directory.text + article.text
    assert not soup.select('form[action*="crawl"], form[action*="fix"]')


def test_project_context_never_modifies_the_shared_article_or_leaks_into_the_next_request(help_site):
    raw = content_library.get_doc('anomalies-et-priorites')
    original = raw['body_html']
    contextual = help_site['owner'].get('/docs/anomalies-et-priorites', params={'project': help_site['slug']})
    assert contextual.context['help_project']
    assert raw['body_html'] == original and content_library.get_doc(raw['slug'])['body_html'] == original
    generic = help_site['owner'].get('/docs/anomalies-et-priorites')
    assert help_site['slug'] not in generic.text and generic.context['help_project'] is None


def test_missing_translation_keeps_the_authorized_context_on_the_french_fallback(help_site, monkeypatch):
    original = content_library.get_doc
    monkeypatch.setattr(content_library, 'get_doc', lambda slug, lang='fr': None if lang == 'en' and slug == 'lire-le-rapport' else original(slug, lang))
    response = help_site['owner'].get(f'/en/docs/lire-le-rapport?project={help_site["slug"]}&crawl={STAMP}', follow_redirects=False)
    assert response.status_code == 302
    assert response.headers['cache-control'] == 'private, no-store'
    assert response.headers['location'] == f'/docs/lire-le-rapport?project={help_site["slug"]}&crawl={STAMP}'


@pytest.mark.parametrize('guide,screen', [
    ('audit-seo-technique-checklist-priorites', '/issues'),
    ('corriger-title-meta-description-grande-echelle', '/corrections'),
    ('corriger-le-seo-dans-le-code-pas-dans-un-rapport', '/corrections'),
    ('regression-seo-apres-mise-en-production', '/crawls'),
    ('frequence-crawl-seo-site-vitrine-ecommerce-blog', '/settings/crawl'),
    ('connecter-google-search-console-audit-mensuel', '/settings/search'),
    ('core-web-vitals-lire-signaux-seo', ''),
    ('ce-que-vos-concurrents-publient-et-pas-vous', '/competitors'),
    ('netlinking-opportunites-backlinks-sans-spam', '/backlinks/opportunities'),
])
def test_all_seo_guides_lead_to_the_relevant_project_screen(help_site, guide, screen):
    response = help_site['owner'].get(f'/ressources-seo/{guide}?project={help_site["slug"]}')
    assert response.status_code == 200
    assert BeautifulSoup(response.text, 'html.parser').select_one('.content-cta .btn.primary')['href'] == f'/projects/{help_site["slug"]}{screen}'


def test_external_links_anchors_and_code_examples_never_receive_the_site():
    context = {'slug': 'atelier', 'crawl': STAMP}
    body = '<a href="/en/docs/lire-le-rapport?lang=en#score">Aide</a><a href="https://outside.example/docs/foo">Externe</a><a href="#score">Score</a><code>href="/docs/lire-le-rapport"</code>'
    rendered = help_navigation.body_links(body, context)
    soup = BeautifulSoup(rendered, 'html.parser')
    assert soup.select_one('code').get_text() == 'href="/docs/lire-le-rapport"'
    assert soup.select('a')[1]['href'] == 'https://outside.example/docs/foo'
    assert soup.select('a')[2]['href'] == '#score'
    target = urlsplit(soup.select('a')[0]['href'])
    assert target.fragment == 'score' and parse_qs(target.query) == {'lang': ['en'], 'project': ['atelier'], 'crawl': [STAMP]}
