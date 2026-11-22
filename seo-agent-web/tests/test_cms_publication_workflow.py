"""A publication is durable and consented before any remote write is attempted."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace
from xml.etree.ElementTree import SubElement, tostring

import pytest
from defusedxml import ElementTree
from sqlalchemy import create_engine, delete, event, func, select
from sqlalchemy.orm import sessionmaker

from backend import cms_consent, cms_publication as workflow
from backend.cms_commun import CmsError
from backend.cms_content_adapters import AdapterError, ArticleDraft, RemoteArticle, TransportResponse
from backend.cms_content_adapters.prestashop import PrestaShopAdapter
from backend.models import AgentPageEvent, Base, CmsPublication, CmsPublicationConsent, IssueTask, Project, User

URL = "https://atelier.example/articles/entretenir-un-velo"
CONNECTION = "hmac:fictional-connection-fingerprint"


@pytest.fixture()
def store(tmp_path):
    engine = create_engine("sqlite:///" + str(tmp_path / "publications.sqlite"))

    @event.listens_for(engine, "connect")
    def enable_foreign_keys(connection, _record):
        connection.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)

    @contextmanager
    def session():
        with factory() as current:
            yield current

    database = SimpleNamespace(session=session)
    with session() as db:
        owner = User(email="publication-owner@example.invalid", password_hash="test")
        other = User(email="publication-other@example.invalid", password_hash="test")
        db.add_all([owner, other])
        db.commit()
        project = Project(owner_user_id=owner.id, slug="atelier", site_name="Atelier", base_url="https://atelier.example/")
        other_project = Project(owner_user_id=other.id, slug="ailleurs", site_name="Ailleurs", base_url="https://ailleurs.example/")
        db.add_all([project, other_project])
        db.commit()
        task = IssueTask(project_id=project.id, user_id=owner.id, issue_key="ai_content_article", url=URL)
        other_task = IssueTask(project_id=other_project.id, user_id=other.id, issue_key="ai_content_article", url="https://ailleurs.example/article")
        db.add_all([task, other_task])
        db.commit()
    data = SimpleNamespace(database=database, project_id=project.id, other_project_id=other_project.id,
                           user_id=owner.id, task_id=task.id, other_task_id=other_task.id)
    yield data
    engine.dispose()


def article(**changes):
    draft = ArticleDraft(title="Entretenir un vélo", html="<p>Nettoyez la chaîne avec un chiffon.</p>",
                         slug="entretenir-un-velo", excerpt="Les gestes utiles.", language="fr", author="Atelier")
    return replace(draft, **changes)


def prepare(data, **changes):
    values = {"project_id": data.project_id, "task_id": data.task_id, "user_id": data.user_id,
              "platform": "wordpress", "language": "fr", "draft": article(), "config": {},
              "connection_fingerprint": CONNECTION}
    values.update(changes)
    return workflow.prepare(data.database, **values)


def load(data, publication_id):
    return workflow.load(data.database, project_id=data.project_id, publication_id=publication_id)


class FakeAdapter:
    def __init__(self):
        self.calls = []
        self.current = RemoteArticle("remote-article-1", "draft", URL, "revision-1")
        self.on_create = None
        self.create_error = None
        self.read_error = None
        self.publish_error = None
        self.publish_status = "published"

    def create_draft(self, draft):
        self.calls.append(("create", draft))
        if self.on_create:
            self.on_create()
        if self.create_error:
            raise self.create_error
        return self.current

    def read(self, remote_id):
        self.calls.append(("read", remote_id))
        if self.read_error:
            raise self.read_error
        assert remote_id == self.current.id
        return self.current

    def publish(self, remote_id, *, expected_revision):
        self.calls.append(("publish", remote_id, expected_revision))
        if self.publish_error:
            raise self.publish_error
        assert expected_revision == self.current.revision
        self.current = replace(self.current, status=self.publish_status, revision="revision-2")
        return self.current


def execute(data, row, adapter, operation="create", **changes):
    values = {"project_id": data.project_id, "publication_id": row.id, "operation": operation,
              "adapter": adapter, "connection_fingerprint": CONNECTION,
              "current_article_digest": workflow.digest(row.data["article"]), "approved": True}
    values["actor_id"] = data.user_id
    values.update(changes)
    return workflow.execute(data.database, **values)


def count(data, model):
    with data.database.session() as db:
        return db.scalar(select(func.count()).select_from(model))


def test_repeated_preview_reuses_intent_without_remote_write(store):
    first = prepare(store)
    second = prepare(store)
    assert first.id == second.id
    assert first.state == "prepared" and first.remote_id is None
    assert count(store, CmsPublication) == 1
    assert count(store, AgentPageEvent) == 0


@pytest.mark.parametrize("change", [
    {"draft": article(html="<p>Un autre texte.</p>")},
    {"config": {"category_id": "42"}},
    {"connection_fingerprint": "hmac:another-fictional-connection"},
], ids=["new-text", "new-destination", "new-connection"])
def test_changed_approved_material_requires_a_separate_intent(store, change):
    first = prepare(store)
    second = prepare(store, **change)
    assert first.id != second.id
    assert count(store, CmsPublication) == 2


@pytest.mark.parametrize("change,code", [
    ({"approved": False}, "approval"),
    ({"connection_fingerprint": "hmac:changed"}, "connection_changed"),
    ({"current_article_digest": "changed-content"}, "content_changed"),
    ({"operation": "delete"}, "operation"),
], ids=["no-consent", "connection-changed", "text-changed", "unknown-operation"])
def test_invalid_execution_never_claims_or_calls_the_cms(store, change, code):
    row, adapter = prepare(store), FakeAdapter()
    with pytest.raises(workflow.PublicationError) as failure:
        execute(store, row, adapter, **change)
    assert failure.value.code == code
    assert adapter.calls == []
    assert load(store, row.id).state == "prepared"


def test_publication_requires_a_second_explicit_request_and_readback(store):
    row, adapter = prepare(store), FakeAdapter()
    draft = execute(store, row, adapter)
    assert draft.state == "draft"
    assert draft.remote_id == "remote-article-1"
    assert draft.remote_revision == "revision-1"
    assert [call[0] for call in adapter.calls] == ["create", "read"]
    assert count(store, AgentPageEvent) == 0

    with pytest.raises(workflow.PublicationError) as failure:
        execute(store, draft, adapter, "publish", approved=False)
    assert failure.value.code == "approval"
    assert [call[0] for call in adapter.calls] == ["create", "read"]

    published = execute(store, draft, adapter, "publish")
    assert published.state == "published" and published.remote_revision == "revision-2"
    assert adapter.calls[2:] == [("publish", "remote-article-1", "revision-1"), ("read", "remote-article-1")]
    with store.database.session() as db:
        proof = db.scalar(select(AgentPageEvent))
        assert proof.project_id == store.project_id and proof.page_url == URL
        assert proof.kind == "publication_accepted"
        assert proof.data == {"publication_id": row.id, "platform": "wordpress", "public_page_verified": False}


def test_publish_cannot_skip_the_draft_step(store):
    row, adapter = prepare(store), FakeAdapter()
    with pytest.raises(workflow.PublicationError) as failure:
        execute(store, row, adapter, "publish")
    assert failure.value.code == "state" and adapter.calls == []
    assert load(store, row.id).state == "prepared"


def test_durable_claim_blocks_an_overlapping_create_before_remote_result(store):
    row, adapter = prepare(store), FakeAdapter()

    def another_request_arrives():
        assert load(store, row.id).state == "creating"
        competing = FakeAdapter()
        with pytest.raises(workflow.PublicationError) as failure:
            execute(store, row, competing)
        assert failure.value.code == "state"
        assert competing.calls == []

    adapter.on_create = another_request_arrives
    assert execute(store, row, adapter).state == "draft"
    assert sum(call[0] == "create" for call in adapter.calls) == 1


@pytest.mark.parametrize("operation", ["create", "publish"], ids=["draft", "publication"])
def test_repeated_completed_request_cannot_duplicate_remote_write(store, operation):
    row, adapter = prepare(store), FakeAdapter()
    draft = execute(store, row, adapter)
    completed = execute(store, draft, adapter, "publish") if operation == "publish" else draft
    calls = list(adapter.calls)
    with pytest.raises(workflow.PublicationError) as failure:
        execute(store, completed, adapter, operation)
    assert failure.value.code == "state"
    assert adapter.calls == calls
    assert count(store, AgentPageEvent) == (1 if operation == "publish" else 0)


@pytest.mark.parametrize("remote_error", [
    AdapterError("Résultat inconnu", code="timeout", uncertain=True),
    CmsError("Résultat inconnu", code="timeout", uncertain=True),
    TimeoutError("Réponse interrompue"),
], ids=["adapter-timeout", "transport-timeout", "socket-timeout"])
def test_ambiguous_create_is_not_retried(store, remote_error):
    row, adapter = prepare(store), FakeAdapter()
    adapter.create_error = remote_error
    with pytest.raises(workflow.PublicationError) as failure:
        execute(store, row, adapter)
    saved = load(store, row.id)
    assert failure.value.code == "uncertain"
    assert saved.state == "uncertain" and saved.remote_id is None
    with pytest.raises(workflow.PublicationError) as repeated:
        execute(store, saved, adapter)
    assert repeated.value.code == "state"
    assert [call[0] for call in adapter.calls] == ["create"]
    assert count(store, AgentPageEvent) == 0


def test_failed_readback_keeps_returned_identity_and_blocks_duplicate_creation(store):
    row, adapter = prepare(store), FakeAdapter()
    adapter.read_error = CmsError("Lecture indisponible", code="read_failed")
    with pytest.raises(workflow.PublicationError) as failure:
        execute(store, row, adapter)
    saved = load(store, row.id)
    assert failure.value.code == "uncertain" and saved.state == "uncertain"
    assert saved.remote_id == "remote-article-1" and saved.remote_revision == "revision-1"
    assert saved.error_code == "read_failed"
    with pytest.raises(workflow.PublicationError):
        execute(store, saved, adapter)
    assert [call[0] for call in adapter.calls] == ["create", "read"]


@pytest.mark.parametrize("remote_state", ["matching", "edited", "translated", "published"])
def test_prestashop_recovers_a_known_create_by_read_only(store, remote_state):
    config = {"id_shop": "1", "id_lang": "1"}
    row = prepare(store, platform="prestashop", config=config)
    calls = []
    document = None

    def transport(method, path, payload=None, headers=None):
        nonlocal document
        calls.append(method)
        if method == "POST":
            document = ElementTree.fromstring(payload)
            SubElement(document.find("content"), "id").text = "12"
            return TransportResponse(201, {}, tostring(document))
        assert method == "GET" and "/content_management_system/12?" in path
        if len(calls) == 2:
            return TransportResponse(503, {}, b"")
        return TransportResponse(200, {}, tostring(document))

    adapter = PrestaShopAdapter(transport=transport, config=config)
    with pytest.raises(workflow.PublicationError, match="Le résultat doit être vérifié") as failure:
        execute(store, row, adapter)
    assert failure.value.code == "uncertain"
    saved = load(store, row.id)
    assert saved.remote_id == "12" and saved.remote_revision is None
    assert saved.state == "uncertain"
    with store.database.session() as db:
        receipt = db.get(CmsPublicationConsent, saved.data["last_consent_id"])
        assert receipt.targets["result"]["remote_id"] == "12"
    with pytest.raises(workflow.PublicationError) as repeated:
        execute(store, saved, adapter)
    assert repeated.value.code == "state"

    content = document.find("content")
    if remote_state == "edited":
        content.find("content/language").text = "<p>Brouillon modifié</p>"
    if remote_state == "translated":
        SubElement(content.find("content"), "language", id="2").text = "<p>Unapproved translation</p>"
    if remote_state == "published":
        content.find("active").text = "1"
    args = {"project_id": store.project_id, "publication_id": row.id, "adapter": adapter}
    if remote_state in {"edited", "translated"}:
        with pytest.raises(workflow.PublicationError) as changed:
            workflow.reconcile(store.database, **args)
        assert changed.value.code == "remote_changed"
        assert load(store, row.id).state == "uncertain"
    else:
        restored = workflow.reconcile(store.database, **args)
        assert restored.state == ("draft" if remote_state == "matching" else "published_unverified")
        assert bool(restored.remote_revision) == (remote_state == "matching")
    assert calls == ["POST", "GET", "GET"]
    assert count(store, CmsPublicationConsent) == 1
    assert count(store, AgentPageEvent) == 0


@pytest.mark.parametrize("operation", ["create", "publish"], ids=["draft-readback", "publish-readback"])
def test_a_revision_changed_during_readback_never_becomes_an_approved_success(store, operation):
    row, adapter = prepare(store), FakeAdapter()
    if operation == "publish":
        row = execute(store, row, adapter)
    original_read = adapter.read
    def changed_read(remote_id):
        adapter.current = replace(adapter.current, revision="concurrent-edit")
        return original_read(remote_id)
    adapter.read = changed_read
    with pytest.raises(workflow.PublicationError) as error:
        execute(store, row, adapter, operation)
    assert error.value.code == "uncertain"
    assert load(store, row.id).state == "uncertain" and count(store, AgentPageEvent) == 0


@pytest.mark.parametrize("code", ["unreadable", "identity", "partial"], ids=["unreadable-body", "missing-identity", "partial-response"])
def test_an_unusable_write_response_never_allows_a_second_creation(store, code):
    row, adapter = prepare(store), FakeAdapter()
    adapter.create_error = AdapterError("The successful HTTP response cannot be confirmed", code=code)
    with pytest.raises(workflow.PublicationError) as error:
        execute(store, row, adapter)
    assert error.value.code == "uncertain"
    assert prepare(store).state == "uncertain"
    with pytest.raises(workflow.PublicationError):
        execute(store, row, adapter)
    assert [call[0] for call in adapter.calls] == ["create"]


def test_known_refusal_is_failed_not_published_or_uncertain(store):
    row, adapter = prepare(store), FakeAdapter()
    adapter.create_error = AdapterError("Droits insuffisants", code="forbidden")
    with pytest.raises(workflow.PublicationError) as failure:
        execute(store, row, adapter)
    saved = load(store, row.id)
    assert failure.value.code == "remote_error"
    assert saved.state == "failed" and saved.error_code == "forbidden"
    assert count(store, AgentPageEvent) == 0


def test_a_consent_receipt_precedes_the_remote_write_and_contains_exact_targets(store):
    row, adapter = prepare(store), FakeAdapter()
    def witness():
        with store.database.session() as db:
            consent = db.scalar(select(CmsPublicationConsent))
            assert consent.actor_id == store.user_id and consent.approved_at
            assert consent.operation == "create" and consent.publication_id == row.id
            assert consent.content_digest == workflow.digest(row.data["article"])
            assert consent.targets["slug"] == row.data["article"]["slug"]
    adapter.on_create = witness
    execute(store, row, adapter)


def test_an_actor_from_another_account_cannot_supply_the_consent(store):
    row, adapter = prepare(store), FakeAdapter()
    with store.database.session() as db:
        other_actor = db.get(Project, store.other_project_id).owner_user_id
    with pytest.raises(workflow.PublicationError) as error:
        execute(store, row, adapter, actor_id=other_actor)
    assert error.value.code == "approval_actor" and not adapter.calls
    assert count(store, CmsPublicationConsent) == 0 and load(store, row.id).state == "prepared"


def test_a_certain_refusal_requires_a_new_preview_and_fresh_consent_before_retry(store):
    row, adapter = prepare(store), FakeAdapter()
    adapter.create_error = AdapterError("Insufficient permissions", code="forbidden")
    with pytest.raises(workflow.PublicationError):
        execute(store, row, adapter)
    with pytest.raises(workflow.PublicationError):
        execute(store, row, adapter)
    assert count(store, CmsPublicationConsent) == 1
    refreshed = prepare(store)
    assert refreshed.id == row.id and refreshed.state == "prepared"
    adapter.create_error = None
    execute(store, refreshed, adapter)
    assert count(store, CmsPublicationConsent) == 2


def test_read_only_reconciliation_recovers_an_existing_draft_without_creating_again(store):
    row, adapter = prepare(store), FakeAdapter()
    adapter.read_error = CmsError("Unavailable")
    with pytest.raises(workflow.PublicationError):
        execute(store, row, adapter)
    adapter.read_error = None
    restored = workflow.reconcile(store.database, project_id=store.project_id, publication_id=row.id, adapter=adapter)
    assert restored.state == "draft"
    assert [call[0] for call in adapter.calls].count("create") == 1
    assert [call[0] for call in adapter.calls].count("publish") == 0


def test_reconciliation_can_confirm_a_returned_publish_receipt_after_failed_read(store):
    row, adapter = prepare(store), FakeAdapter()
    draft = execute(store, row, adapter)
    adapter.read_error = CmsError("Unavailable")
    with pytest.raises(workflow.PublicationError):
        execute(store, draft, adapter, operation="publish")
    saved_receipt = load(store, row.id).data["publish_accepted_at"]
    adapter.read_error = None
    restored = workflow.reconcile(store.database, project_id=store.project_id, publication_id=row.id, adapter=adapter)
    assert restored.state == "published" and count(store, AgentPageEvent) == 1
    with store.database.session() as db:
        from datetime import datetime
        recorded = db.scalar(select(AgentPageEvent)).created_at
        assert cms_consent.utc(recorded) == datetime.fromisoformat(saved_receipt)
    assert [call[0] for call in adapter.calls].count("publish") == 1


@pytest.mark.parametrize("operation,final_state", [("create", "draft"), ("publish", "published")],
                         ids=["create", "publish"])
@pytest.mark.parametrize("legacy", [False, True], ids=["current", "legacy"])
def test_reconcile_recovers_a_process_interrupted_after_the_write(store, operation, final_state, legacy):
    row, adapter = prepare(store), FakeAdapter()
    if operation == "publish":
        row = execute(store, row, adapter)
    original_read = adapter.read

    def interrupted(remote_id):
        raise RuntimeError("Simulated worker shutdown before read-back")

    adapter.read = interrupted
    with pytest.raises(RuntimeError, match="shutdown"):
        execute(store, row, adapter, operation)
    saved = load(store, row.id)
    assert saved.state == ("creating" if operation == "create" else "publishing")
    assert saved.remote_id == adapter.current.id and saved.data["write_phase"] == "answered"
    if legacy:
        with store.database.session() as db:
            stored = db.get(CmsPublication, row.id)
            stored.data = {key: value for key, value in stored.data.items() if key != "write_phase"}
            db.commit()
    adapter.read = original_read
    for _ in range(2):
        restored = workflow.reconcile(store.database, project_id=store.project_id, publication_id=row.id, adapter=adapter)
        assert restored.state == final_state
    assert sum(call[0] == operation for call in adapter.calls) == 1
    assert count(store, AgentPageEvent) == (1 if operation == "publish" else 0)


@pytest.mark.parametrize("times_out", [False, True], ids=["returns", "timeout"])
def test_reading_the_old_draft_cannot_rearm_an_inflight_or_ambiguous_publish(store, times_out):
    row, adapter = prepare(store), FakeAdapter()
    draft = execute(store, row, adapter)
    original_publish = adapter.publish

    def in_flight(remote_id, *, expected_revision):
        with pytest.raises(workflow.PublicationError) as error:
            workflow.reconcile(store.database, project_id=store.project_id, publication_id=row.id, adapter=adapter)
        assert error.value.code == "write_pending" and load(store, row.id).state == "publishing"
        competing = FakeAdapter()
        with pytest.raises(workflow.PublicationError):
            execute(store, draft, competing, operation="publish")
        assert competing.calls == []
        if times_out:
            raise TimeoutError("The CMS may still be processing this write")
        return original_publish(remote_id, expected_revision=expected_revision)

    adapter.publish = in_flight
    if times_out:
        with pytest.raises(workflow.PublicationError) as error:
            execute(store, draft, adapter, operation="publish")
        assert error.value.code == "uncertain"
        with pytest.raises(workflow.PublicationError) as pending:
            workflow.reconcile(store.database, project_id=store.project_id, publication_id=row.id, adapter=adapter)
        assert pending.value.code == "write_pending" and load(store, row.id).state == "uncertain"
    else:
        assert execute(store, draft, adapter, operation="publish").state == "published"


@pytest.mark.parametrize("read_fails", [False, True], ids=["late-success", "late-error"])
def test_a_late_read_back_cannot_overwrite_recovery_or_duplicate_the_event(store, read_fails):
    row, adapter = prepare(store), FakeAdapter()
    draft = execute(store, row, adapter)
    original_read = adapter.read
    reader = SimpleNamespace(read=original_read)

    def late_read(remote_id):
        restored = workflow.reconcile(store.database, project_id=store.project_id, publication_id=row.id, adapter=reader)
        assert restored.state == "published"
        if read_fails:
            raise CmsError("Late read failed")
        return original_read(remote_id)

    adapter.read = late_read
    with pytest.raises(workflow.PublicationError) as error:
        execute(store, draft, adapter, operation="publish")
    assert error.value.code == "state"
    assert load(store, row.id).state == "published" and count(store, AgentPageEvent) == 1
    assert sum(call[0] == "publish" for call in adapter.calls) == 1


def test_an_interrupted_create_without_id_cannot_be_reconciled_or_replayed(store):
    row, adapter = prepare(store), FakeAdapter()

    def interrupted():
        raise RuntimeError("Simulated worker shutdown before receiving the ID")

    adapter.on_create = interrupted
    with pytest.raises(RuntimeError, match="shutdown"):
        execute(store, row, adapter)
    with pytest.raises(workflow.PublicationError) as error:
        workflow.reconcile(store.database, project_id=store.project_id, publication_id=row.id, adapter=adapter)
    assert error.value.code == "unresolved"
    assert prepare(store).state == "creating"
    with pytest.raises(workflow.PublicationError):
        execute(store, row, adapter)
    assert [call[0] for call in adapter.calls] == ["create"]


def test_a_changed_remote_draft_cannot_silently_replace_the_approved_preview(store):
    row, adapter = prepare(store), FakeAdapter()
    execute(store, row, adapter)
    adapter.current = replace(adapter.current, revision="someone-else-edited")
    with pytest.raises(workflow.PublicationError) as error:
        workflow.reconcile(store.database, project_id=store.project_id, publication_id=row.id, adapter=adapter)
    assert error.value.code == "remote_changed"
    assert load(store, row.id).remote_revision == "revision-1"


def test_a_publication_without_a_matching_receipt_is_only_observed_not_attributed(store):
    row, adapter = prepare(store), FakeAdapter()
    execute(store, row, adapter)
    adapter.current = replace(adapter.current, status="published", revision="outside-change")
    restored = workflow.reconcile(store.database, project_id=store.project_id, publication_id=row.id, adapter=adapter)
    assert restored.state == "published_unverified" and count(store, AgentPageEvent) == 0
    assert not any(call[0] == "publish" for call in adapter.calls)


@pytest.mark.parametrize("operation", ["create", "publish"], ids=["create-not-draft", "publish-not-live"])
def test_unconfirmed_remote_status_never_becomes_success(store, operation):
    row, adapter = prepare(store), FakeAdapter()
    if operation == "create":
        adapter.current = replace(adapter.current, status="published")
    else:
        row = execute(store, row, adapter)
        adapter.publish_status = "draft"
    with pytest.raises(workflow.PublicationError) as failure:
        execute(store, row, adapter, operation)
    assert failure.value.code == "uncertain"
    assert load(store, row.id).state == "uncertain"
    assert count(store, AgentPageEvent) == 0


def test_another_project_cannot_read_or_execute_an_intent(store):
    row, adapter = prepare(store), FakeAdapter()
    with pytest.raises(workflow.PublicationError) as read_failure:
        workflow.load(store.database, project_id=store.other_project_id, publication_id=row.id)
    assert read_failure.value.code == "not_found"
    with pytest.raises(workflow.PublicationError) as execute_failure:
        execute(store, row, adapter, project_id=store.other_project_id)
    assert execute_failure.value.code == "not_found" and adapter.calls == []


@pytest.mark.parametrize("reference", ["other-project", "missing"], ids=["other-project", "missing"])
def test_an_intent_cannot_reference_an_unavailable_article(store, reference):
    with pytest.raises(workflow.PublicationError) as failure:
        prepare(store, task_id=store.other_task_id if reference == "other-project" else "missing-task")
    assert failure.value.code == "not_found"
    assert count(store, CmsPublication) == 0


@pytest.mark.parametrize("markup", [
    '<script>alert(1)</script><p>Texte.</p>',
    '<iframe src="https://ailleurs.example/"></iframe>',
    '<p onclick="alert(1)">Texte.</p>',
    '<a href="javascript:alert(1)">Cliquer</a>',
    '<a href="https://identifiant:secret@ailleurs.example/">Cliquer</a>',
    '<a href="https://[bad">Cliquer</a>',
    '<a href="https://ailleurs.example:not-a-port/">Cliquer</a>',
    '<img src="/api/projects/atelier/content/image/privee">',
    '<img src="https://atelier.example/api/projects/atelier/content/image/privee">',
    '<img src="data:image/png;base64,AAAA">',
    '<img src="https://cdn.example/photo.jpg" srcset="https://cdn.example/photo-2.jpg 2x">',
], ids=["script", "frame", "handler", "javascript", "url-credentials", "broken-url", "bad-port", "private-relative", "private-api", "data-image", "image-variants"])
def test_unsafe_or_unready_content_is_refused_before_an_intent_is_saved(store, markup):
    with pytest.raises(workflow.PublicationError):
        prepare(store, draft=article(html=markup))
    assert count(store, CmsPublication) == 0


def test_public_https_media_and_useful_markup_are_kept(store):
    markup = '<h2>La chaîne</h2><p>Un <strong>conseil</strong> utile.</p><img src="https://cdn.example/chaine.webp" alt="Une chaîne de vélo">'
    row = prepare(store, draft=article(html=markup))
    assert "https://cdn.example/chaine.webp" in row.data["article"]["html"]
    assert '<strong>conseil</strong>' in row.data["article"]["html"]


@pytest.mark.parametrize("secret_key", ["token", "password", "authorization"], ids=["token", "password", "authorization"])
def test_credentials_never_enter_the_publication_preview(store, secret_key):
    with pytest.raises(workflow.PublicationError) as failure:
        prepare(store, config={secret_key: "fictional-only"})
    assert failure.value.code == "config"
    assert count(store, CmsPublication) == 0


def test_project_deletion_removes_publication_history(store):
    row, adapter = prepare(store), FakeAdapter()
    draft = execute(store, row, adapter)
    execute(store, draft, adapter, "publish")
    with store.database.session() as db:
        db.execute(delete(Project).where(Project.id == store.project_id))
        db.commit()
    assert count(store, CmsPublication) == 0
    assert count(store, AgentPageEvent) == 0
