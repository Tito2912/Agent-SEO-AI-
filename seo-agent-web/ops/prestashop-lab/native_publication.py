"""Real native CMS-page recipe; accepts only a freshly prepared loopback laboratory."""
from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import secrets
import sys
from tempfile import TemporaryDirectory
from unittest.mock import patch
from urllib.parse import urljoin, urlsplit

from defusedxml import ElementTree as ET
import requests
from sqlalchemy import select


def check_publication_consents(root, adapter, calls, config, public, nonce):
    from backend import cms_publication as workflow
    from backend.cms_content_adapters import ArticleDraft
    from backend.db import Database
    from backend.models import CmsPublicationConsent, IssueTask, Project, User

    with TemporaryDirectory(dir=root) as temporary:
        data_dir = Path(temporary)
        with patch.dict(os.environ, {"DATABASE_URL": "sqlite:///" + str(data_dir / "recipe.db")}):
            database = Database(data_dir=data_dir)
        try:
            database.create_tables()
            with database.session() as db:
                user = User(email="prestashop-recipe@example.invalid", password_hash="fixture")
                db.add(user)
                db.flush()
                project = Project(owner_user_id=user.id, slug="prestashop-recipe", site_name="Recette locale", base_url=public)
                db.add(project)
                db.flush()
                task = IssueTask(project_id=project.id, user_id=user.id, issue_key="ai_content_article", url=public)
                db.add(task)
                db.commit()
            article = ArticleDraft(title="Accords de recette " + nonce, html="<p>Page fictive approuvée.</p>",
                                   slug="consent-native-" + nonce)
            fingerprint = "fictional-prestashop-lab-connection"
            start = len(calls)
            prepared = workflow.prepare(database, project_id=project.id, task_id=task.id, user_id=user.id,
                                        platform="prestashop", language="fr", draft=article, config=config,
                                        connection_fingerprint=fingerprint)
            assert len(calls) == start
            execution = {"project_id": project.id, "publication_id": prepared.id, "adapter": adapter,
                         "connection_fingerprint": fingerprint, "actor_id": user.id,
                         "current_article_digest": workflow.digest(prepared.data["article"])}
            for operation, expected in [("create", "draft"), ("publish", "published")]:
                start = len(calls)
                try:
                    workflow.execute(database, operation=operation, **execution)
                except workflow.PublicationError as error:
                    assert error.code == "approval"
                else:
                    raise AssertionError("The workflow must require a new explicit approval")
                assert len(calls) == start
                result = workflow.execute(database, operation=operation, approved=True, **execution)
                assert result.state == expected
                assert sum(method != "GET" for method in calls[start:]) == 1
            with database.session() as db:
                receipts = list(db.scalars(select(CmsPublicationConsent)))
                assert sorted(receipt.operation for receipt in receipts) == ["create", "publish"]
                assert all(receipt.publication_id == prepared.id and receipt.actor_id == user.id for receipt in receipts)
        finally:
            database.engine.dispose()


def run(root: Path, server: str, report: Path | None):
    parsed = urlsplit(server)
    if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or not parsed.port:
        raise RuntimeError("Only the disposable loopback fixture is allowed")
    metadata = json.loads((root / "fixture.json").read_text(encoding="utf-8"))
    version = metadata.get("version")
    shop = root / "shop"
    if (version not in {"8.2.8", "9.2.1"}
            or metadata.get("server_url") not in {server, f"http://localhost:{parsed.port}"}
            or (shop / "install").exists() or (version == "8.2.8" and not (shop / "install-disabled").is_dir())):
        raise RuntimeError("Expected the explicitly prepared disposable PrestaShop fixture")
    key = (root / "native-key.txt").read_text(encoding="utf-8").strip()
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from backend import cms_content_transport as transport
    from backend.cms_content_adapters import ArticleDraft, AdapterError, get_adapter

    public = "https://prestashop.noyaru.test"
    calls = []

    class LocalConnection(http.client.HTTPConnection):
        def __init__(self, host, address):
            if host != "prestashop.noyaru.test" or address != "127.0.0.1":
                raise RuntimeError("Unexpected fixture destination")
            super().__init__("127.0.0.1", parsed.port, timeout=60)

        def request(self, method, path, body=None, headers=None):
            calls.append(method)
            super().request(method, path, body=body, headers={**(headers or {}), "Host": parsed.netloc})

    def address(host, _service):
        if host != "prestashop.noyaru.test":
            raise RuntimeError("Unexpected fixture hostname")
        return "127.0.0.1"

    transport._PinnedHTTPS = LocalConnection
    transport.adresse_publique = address
    connection = transport.ContentTransport("prestashop", {"base_url": public, "key": key}, project_url=public)
    assert connection.check()["verified"]
    default = str(metadata["default_lang"])
    english = str(metadata["english_lang"])
    assert default != english
    config = {"id_lang": default, "id_shop": "1"}
    adapter = get_adapter("prestashop", transport=connection, config=config)
    en_adapter = get_adapter("prestashop", transport=connection, config={"id_lang": english, "id_shop": "1"})
    nonce = secrets.token_hex(5)
    draft = ArticleDraft(title="Recette native Noyaru " + nonce,
                         html="<h2>Un contrôle utile</h2>\n<p>Le texte fictif de cette page doit rester intact.</p>",
                         slug="native-recipe-" + nonce, excerpt="Page de recette locale fictive.")

    def snapshot(ident):
        response = connection("GET", f"/api/content_management_system/{ident}?output_format=XML&id_shop=1")
        content = ET.fromstring(response.body).find("content")
        assert content is not None and content.find("date_upd") is None
        return {element.tag: {lang.get("id"): lang.text or "" for lang in element.findall("language")}
                if element.findall("language") else element.text or "" for element in content}

    created = adapter.create_draft(draft)
    assert created.status == "draft" and len(created.revision) == 64
    assert adapter.read(created.id).revision == created.revision
    before = snapshot(created.id)
    assert before["content"][default] == draft.html
    checks = ["Native CMS document has no date_upd; draft identity and stable content revision confirmed"]
    en_draft = ArticleDraft(title="English native recipe " + nonce,
                            html="<p>An independently written English text.</p>",
                            slug="english-native-recipe-" + nonce, excerpt="English local test.", language="en")
    updated = en_adapter.update_draft(created.id, en_draft, expected_revision=created.revision)
    after = snapshot(created.id)
    for field in ("meta_title", "link_rewrite", "meta_description", "content"):
        assert before[field][default] == after[field][default], field
    assert after["content"][english] == en_draft.html and after["active"] == "0"
    checks.append("Updating the second language preserves every original-language field and remains a draft")
    published = adapter.publish(created.id, expected_revision=updated.revision)
    final = snapshot(created.id)
    assert published.status == "published"
    assert final == {**after, "active": "1"}
    for language, content in [(default, "texte fictif de cette page"), (english, "independently written English text")]:
        with requests.Session() as session:
            session.trust_env = False
            url = f"{server}/index.php?controller=cms&id_cms={created.id}&id_lang={language}"
            response = session.get(url, timeout=30, allow_redirects=False)
            for _ in range(3):
                if response.status_code not in {301, 302, 303}:
                    break
                url = urljoin(url, response.headers["Location"])
                assert urlsplit(url).netloc == parsed.netloc
                response = session.get(url, timeout=30, allow_redirects=False)
            assert response.status_code == 200 and content in response.text, language
    checks.append("Activation preserves the complete API document except for active; public French and English HTML confirmed")
    old_revision = published.revision
    changed = en_adapter.update_draft(created.id, ArticleDraft(title="External edit", html="<p>Changed text.</p>",
        slug="external-native-" + nonce), expected_revision=old_revision)
    assert changed.revision != old_revision
    start = len(calls)
    try:
        adapter.publish(created.id, expected_revision=old_revision)
        raise AssertionError("A stale CMS version must not be activated")
    except AdapterError as error:
        assert error.code == "conflict" and calls[start:] == ["GET"]
    checks.append("A stale revision is rejected before any new write")
    # A non-default-language creation is observed separately; never populate another
    # language with an invented translation merely to make the request pass.
    try:
        nondefault = en_adapter.create_draft(ArticleDraft(title="Second language only", html="<p>English only.</p>",
                                               slug="only-english-" + nonce, language="en"))
        nondefault_result = {"created": True, "state": nondefault.status}
    except AdapterError as error:
        nondefault_result = {"created": False, "error": error.code, "uncertain": error.uncertain}
    check_publication_consents(root, adapter, calls, config, public, nonce)
    checks.append("Noyaru draft and publication each require their own approval and durable receipt; no approval means no request")
    sources = {name: hashlib.sha256((Path(__file__).resolve().parents[2] / name).read_bytes()).hexdigest()
               for name in ["backend/cms_content_adapters/prestashop.py", "backend/cms_content_transport.py", "backend/cms_publication.py"]}
    result = {"prestashop": metadata["version"], "sources": sources, "languages": [default, english], "checks": checks,
              "nondefault_creation": nondefault_result, "requests": len(calls),
              "writes": sum(method != "GET" for method in calls), "outcome": "passed",
              "scope": "New fictitious CMS pages in a disposable PrestaShop shop; test-only DNS/TLS mapping; no external HTTPS"}
    if report:
        report.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lab-root", type=Path, required=True)
    parser.add_argument("--server", default="http://127.0.0.1:8091")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    print(json.dumps(run(args.lab_root.resolve(), args.server, args.report), ensure_ascii=False, indent=2))
