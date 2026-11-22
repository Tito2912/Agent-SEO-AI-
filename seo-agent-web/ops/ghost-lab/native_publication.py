"""Disposable real Ghost recipe for the native editorial workflow, never a customer site.

Requires an extracted official Ghost release with its dependencies installed.
The only replacement in Noyaru is the last DNS/TLS hop to this loopback process.
"""
from __future__ import annotations

import argparse
import hashlib
from contextlib import contextmanager
from dataclasses import asdict, replace
import http.client
import io
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlsplit

from PIL import Image
import requests


def recipe(ghost_root: Path, node: str, report: Path | None):
    package = json.loads((ghost_root / "package.json").read_text(encoding="utf-8"))
    if package.get("name") != "ghost" or not (ghost_root / "index.js").is_file():
        raise RuntimeError("An official installed Ghost release is required")
    with tempfile.TemporaryDirectory(prefix="noyaru-ghost-native-") as directory:
        root = Path(directory)
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        local = f"http://127.0.0.1:{port}"
        public = "https://ghost.noyaru.test"
        (root / "current").symlink_to(ghost_root, target_is_directory=True)
        content = root / "content"
        (content / "themes").mkdir(parents=True)
        themes = ghost_root / "content/themes"
        if themes.is_dir():
            for theme in themes.iterdir():
                if theme.is_dir():
                    (content / "themes" / theme.name).symlink_to(theme, target_is_directory=True)
        config = {"url": local, "server": {"host": "127.0.0.1", "port": port},
                  "database": {"client": "sqlite3", "connection": {"filename": str(root / "ghost.db")}},
                  "paths": {"contentPath": str(content)}, "logging": {"transports": ["stdout"], "level": "error"},
                  "mail": {"transport": "SMTP", "options": {"host": "127.0.0.1", "port": 9}},
                  "privacy": {"useUpdateCheck": False, "useGravatar": False, "useRpcPing": False}}
        (root / "config.development.json").write_text(json.dumps(config), encoding="utf-8")
        env = {**os.environ, "NODE_ENV": "development"}
        with (root / "ghost-runtime.log").open("w", encoding="utf-8") as log:
            process = subprocess.Popen([node, str(ghost_root / "index.js")], cwd=root, env=env,
                                       stdout=log, stderr=subprocess.STDOUT)
            try:
                try:
                    result = exercise(local, public, root, process, package["version"])
                except Exception:
                    # Private diagnostic retained only outside the repository, never a report.
                    diagnostic = Path(tempfile.gettempdir()) / "noyaru-ghost-native-diagnostic.log"
                    shutil.copyfile(root / "ghost-runtime.log", diagnostic)
                    diagnostic.chmod(0o600)
                    raise
            finally:
                process.terminate()
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
        if report:
            report.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return result


def exercise(local, public, root, process, version):
    if urlsplit(local).hostname != "127.0.0.1":
        raise RuntimeError("This recipe only accepts its disposable loopback Ghost")
    session = requests.Session()
    session.trust_env = False
    base = local + "/ghost/api/admin/"
    headers = {"Origin": local, "Accept-Version": "v5.0"}

    def api(method, path, body=None):
        response = session.request(method, base + path, json=body, headers=headers,
                                   allow_redirects=False, timeout=30)
        if response.status_code not in {200, 201, 204}:
            raise RuntimeError("Disposable Ghost setup API refused: " + str(response.status_code))
        return response.json() if response.content and "json" in response.headers.get("Content-Type", "") else {}

    for _ in range(120):
        if process.poll() is not None:
            raise RuntimeError("Disposable Ghost stopped before readiness")
        try:
            response = session.get(base + "authentication/setup/", timeout=2, allow_redirects=False)
            if response.status_code == 200:
                break
        except requests.RequestException:
            pass
        time.sleep(1)
    else:
        raise RuntimeError("Disposable Ghost readiness deadline reached")
    state = api("GET", "authentication/setup/")
    if state["setup"][0]["status"]:
        raise RuntimeError("Refusing to change an already initialized Ghost")
    password = secrets.token_urlsafe(36)
    api("POST", "authentication/setup/", {"setup": [{"name": "Noyaru Local Lab",
        "email": "ghost-lab@example.invalid", "password": password, "blogTitle": "Noyaru Local Lab"}]})
    api("POST", "session/", {"username": "ghost-lab@example.invalid", "password": password})
    integration = api("POST", "integrations/?include=api_keys", {"integrations": [{"name": "Noyaru native lab"}]})
    key = next(k["secret"] for k in integration["integrations"][0]["api_keys"] if k["type"] == "admin")

    os.environ.update({"DATABASE_URL": "sqlite:///" + str(root / "noyaru.db"),
                       "SEO_AGENT_DATA_DIR": str(root / "data"), "SEO_AGENT_RUNS_DIR": str(root / "runs"),
                       "SEO_AGENT_SECRET_KEY": secrets.token_urlsafe(40),
                       "SEO_AGENT_ENCRYPTION_KEY": secrets.token_urlsafe(40),
                       "SEO_AGENT_DISABLE_WORKER": "true", "SENTRY_DSN": ""})
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from sqlalchemy import create_engine, select
    from sqlalchemy.orm import sessionmaker
    from types import SimpleNamespace
    from backend import cms_content_transport as transport, cms_publication as workflow, cms_media_workflow
    from backend.cms_content_adapters import ArticleDraft, AdapterError, get_adapter
    from backend.models import Base, CmsPublicationConsent, CmsMediaTransfer, IssueTask, Project, User

    calls = []
    host = urlsplit(public).hostname
    port = urlsplit(local).port

    class LocalResponse:
        def __init__(self, response):
            self.response, self.status = response, response.status

        def read(self, limit):
            return self.response.read(limit).replace(local.encode("utf-8"), public.encode("utf-8"))

        def getheader(self, name, default=None):
            return self.response.getheader(name, default)

    class LocalConnection(http.client.HTTPConnection):
        def __init__(self, target, address):
            if target != host or address != "127.0.0.1":
                raise RuntimeError("Unexpected test destination")
            super().__init__("127.0.0.1", port, timeout=30)

        def request(self, method, path, body=None, headers=None):
            calls.append(method)
            super().request(method, path, body=body, headers={**(headers or {}), "Host": f"127.0.0.1:{port}"})

        def getresponse(self):
            return LocalResponse(super().getresponse())

    def address(target, _service):
        if target != host:
            raise RuntimeError("Unexpected test hostname")
        return "127.0.0.1"

    transport.adresse_publique = address
    transport._PinnedHTTPS = LocalConnection
    connection = transport.ContentTransport("ghost", {"base_url": public, "key": key}, project_url=public)
    assert connection.check()["verified"]
    adapter = get_adapter("ghost", transport=connection, config={})
    engine = create_engine("sqlite:///" + str(root / "noyaru.db"))
    Base.metadata.create_all(engine)
    factory = sessionmaker(engine, expire_on_commit=False)

    @contextmanager
    def database_session():
        with factory() as db:
            yield db

    database = SimpleNamespace(session=database_session)
    with database.session() as db:
        actor = User(email="native-lab@example.invalid", password_hash="disposable")
        db.add(actor)
        db.flush()
        project = Project(owner_user_id=actor.id, slug="ghost-native-lab", site_name="Ghost Native Lab", base_url=public)
        db.add(project)
        db.flush()
        task = IssueTask(project_id=project.id, user_id=actor.id, issue_key="ai_content_article", url=public + "/native-recipe/")
        db.add(task)
        db.commit()
    checks = []
    data = io.BytesIO()
    Image.new("RGB", (32, 32), "#759e83").save(data, "WEBP")
    upload_args = {"project_id": project.id, "task_id": task.id, "platform": "ghost",
                   "connection_fingerprint": "local-test-connection", "name": "native-recipe.webp",
                   "data": data.getvalue(), "transport": connection, "actor_id": actor.id}
    before = len(calls)
    try:
        cms_media_workflow.image_url(database, **upload_args)
        raise AssertionError("Image upload without approval must fail")
    except workflow.PublicationError as error:
        assert error.code == "media_approval" and len(calls) == before
    image_url = cms_media_workflow.image_url(database, **upload_args, approved=True)
    assert cms_media_workflow.image_url(database, **upload_args) == image_url
    with database.session() as db:
        transfer = db.scalar(select(CmsMediaTransfer))
        assert transfer.state == "ready" and transfer.approved_by == actor.id
    checks.append("Native WebP upload: explicit consent persisted, confirmed URL reused without another upload")
    draft = ArticleDraft(title="Entretenir son vélo — recette Noyaru", slug="native-recipe", language="fr",
        html='<h2>Le contrôle de la chaîne</h2><p>Un entretien adapté protège les pièces de votre vélo.</p>'
             f'<img src="{image_url}" alt="Image fictive de recette">')
    draft = replace(draft, html=workflow.clean_html(draft.html))
    row = workflow.prepare(database, project_id=project.id, task_id=task.id, user_id=actor.id,
            platform="ghost", language="fr", draft=draft, config={}, connection_fingerprint="local-test-connection")
    execute_args = {"project_id": project.id, "publication_id": row.id, "adapter": adapter,
                    "connection_fingerprint": "local-test-connection",
                    "current_article_digest": workflow.digest(asdict(draft)), "actor_id": actor.id}
    before = len(calls)
    try:
        workflow.execute(database, operation="create", **execute_args)
        raise AssertionError("Draft without consent must fail")
    except workflow.PublicationError as error:
        assert error.code == "approval" and len(calls) == before
    saved = workflow.execute(database, operation="create", approved=True, **execute_args)
    assert saved.state == "draft" and adapter.read(saved.remote_id).status == "draft"
    snapshot = api("GET", "posts/" + saved.remote_id + "/?formats=html")["posts"][0]
    from bs4 import BeautifulSoup
    assert "protège les pièces" in BeautifulSoup(snapshot["html"], "html.parser").get_text()
    assert image_url in snapshot["html"] or image_url.replace(public, local) in snapshot["html"]
    try:
        workflow.execute(database, operation="create", approved=True, **execute_args)
        raise AssertionError("A repeated create must fail")
    except workflow.PublicationError as error:
        assert error.code == "state"
    published = workflow.execute(database, operation="publish", approved=True, **execute_args)
    assert published.state == "published"
    response = session.get(published.remote_url.replace(public, local), allow_redirects=False, timeout=20)
    assert response.status_code == 200 and "protège les pièces" in response.text
    page = api("GET", "posts/" + saved.remote_id + "/?formats=html")["posts"][0]
    assert page.get("email") is None and page.get("newsletter_id") is None
    with database.session() as db:
        consents = list(db.scalars(select(CmsPublicationConsent)))
        assert len(consents) == 3 and {c.operation for c in consents} == {"create", "publish", "media_upload"}
        assert all(c.actor_id == actor.id and c.actor_email == actor.email and c.expires_at for c in consents)
        assert len([c for c in consents if c.publication_id == saved.id]) == 2
    checks.append("Draft and publication require separate persisted approvals; public local HTML verified; no newsletter attached")
    changed = adapter.create_draft(ArticleDraft(title="Concurrent edit", html="<p>Version initiale.</p>", slug="native-conflict"))
    initial_post = api("GET", "posts/" + changed.id + "/?formats=html")["posts"][0]
    api("PUT", "posts/" + changed.id + "/", {"posts": [{"title": "Modification externe", "updated_at": initial_post["updated_at"]}]})
    observed = api("GET", "posts/" + changed.id + "/?formats=html")["posts"][0]
    assert observed["title"] == "Modification externe"
    print(json.dumps({"rapid_external_edit_confirmed": True,
                      "same_updated_at": initial_post["updated_at"] == observed["updated_at"]}), flush=True)
    before = len(calls)
    try:
        adapter.publish(changed.id, expected_revision=changed.revision)
        raise AssertionError("A stale remote draft must not be published")
    except AdapterError as error:
        assert error.code == "conflict" and calls[before:] == ["GET"]
    checks.append("Concurrent remote modification blocks publication before any additional write")
    engine.dispose()
    session.close()
    sources = {name: hashlib.sha256((Path(__file__).resolve().parents[2] / name).read_bytes()).hexdigest()
               for name in ["backend/cms_content_adapters/ghost.py", "backend/cms_content_transport.py", "backend/cms_publication.py", "backend/cms_media_workflow.py", "backend/cms_consent.py", "backend/models.py"]}
    return {"ghost": version, "sources": sources, "checks": checks, "requests": len(calls),
            "writes": sum(method != "GET" for method in calls), "outcome": "passed",
            "scope": "Real disposable loopback Ghost, test-only URL/DNS/TLS mapping; no external HTTPS or customer site"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ghost-root", type=Path, required=True)
    parser.add_argument("--node", default=shutil.which("node"))
    parser.add_argument("--report", type=Path)
    options = parser.parse_args()
    if not options.node:
        parser.error("A supported Node executable is required")
    print(json.dumps(recipe(options.ghost_root.resolve(), options.node, options.report), ensure_ascii=False, indent=2))
