"""An article's private media is consented, project-scoped and journaled before upload."""
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from backend import cms_consent, cms_media_workflow
from backend.cms_commun import CmsError
from backend.cms_media_workflow import image_url
from backend.cms_publication import PublicationError
from backend.models import CmsMediaTransfer, CmsPublicationConsent
import test_cms_publication_workflow as publication

store = publication.store


def values(store, transport, **changes):
    out = {"project_id": store.project_id, "task_id": store.task_id, "platform": "wordpress",
           "connection_fingerprint": "fictional-hmac", "name": "velo-1.webp", "data": b"validated-by-transport",
           "transport": transport, "approved": True, "actor_id": store.user_id}
    out.update(changes)
    return out


def test_consent_is_required_before_a_media_intent_or_upload(store):
    transport = SimpleNamespace(upload_image=lambda **kwargs: pytest.fail("Consent is absent"))
    with pytest.raises(PublicationError, match="Confirmez"):
        image_url(store.database, **values(store, transport, approved=False))
    with store.database.session() as db:
        assert not list(db.scalars(select(CmsMediaTransfer)))


def test_an_intent_is_durable_before_upload_and_completed_media_is_reused(store):
    calls = []
    def upload(**kwargs):
        with store.database.session() as db:
            row = db.scalar(select(CmsMediaTransfer))
            assert row.state == "uploading" and row.source_name == "velo-1.webp"
            assert row.approved_by == store.user_id and row.consent_digest
        calls.append(kwargs)
        return {"id": "123", "url": "https://atelier.example/images/velo.webp"}
    transport = SimpleNamespace(upload_image=upload)
    expected = "https://atelier.example/images/velo.webp"
    assert image_url(store.database, **values(store, transport)) == expected
    assert image_url(store.database, **values(store, transport, approved=False)) == expected
    assert len(calls) == 1


@pytest.mark.parametrize("error,state", [(TimeoutError(), "uncertain"),
    (CmsError("Timeout", code="network", uncertain=True), "uncertain"),
    (CmsError("Refused", code="rights", uncertain=False), "failed")], ids=["timeout", "ambiguous", "refused"])
def test_failed_uploads_are_not_repeated_and_certainty_is_preserved(store, error, state):
    calls = []
    def upload(**kwargs):
        calls.append(kwargs)
        raise error
    transport = SimpleNamespace(upload_image=upload)
    for _ in range(2):
        with pytest.raises(PublicationError):
            image_url(store.database, **values(store, transport))
    assert len(calls) == 1
    with store.database.session() as db:
        row = db.scalar(select(CmsMediaTransfer))
        assert row.state == state and row.remote_url is None


def test_another_projects_article_cannot_upload_under_this_project(store):
    transport = SimpleNamespace(upload_image=lambda **kwargs: pytest.fail("Wrong project"))
    with pytest.raises(PublicationError):
        image_url(store.database, **values(store, transport, task_id=store.other_task_id))


def test_native_media_support_is_not_claimed_for_every_platform(store):
    transport = SimpleNamespace(upload_image=lambda **kwargs: pytest.fail("Unsupported provider"))
    with pytest.raises(PublicationError) as error:
        image_url(store.database, **values(store, transport, platform="webflow"))
    assert error.value.code == "media_unsupported"


def test_unusable_public_url_preserves_known_media_identity_and_never_retries(store):
    error = CmsError("Public URL rejected", code="image_url", uncertain=True)
    error.remote_id = "42"
    calls = []
    def upload(**kwargs):
        calls.append(True)
        raise error
    for _ in range(2):
        with pytest.raises(PublicationError):
            image_url(store.database, **values(store, SimpleNamespace(upload_image=upload)))
    with store.database.session() as db:
        row, = db.scalars(select(CmsMediaTransfer))
        assert row.state == "uncertain" and row.remote_id == "42" and row.remote_url is None
        proof, = db.scalars(select(CmsPublicationConsent))
        assert proof.targets["result"]["remote_id"] == "42" and proof.targets["result"]["url"] is None
    assert calls == [True]


def refuse_upload(**kwargs):
    raise CmsError("Refused", code="rights")


def refused_attempt(store):
    with pytest.raises(PublicationError) as error:
        image_url(store.database, **values(store, SimpleNamespace(upload_image=refuse_upload)))
    assert error.value.code == "media_failed"
    with store.database.session() as db:
        return db.scalar(select(CmsMediaTransfer))


@pytest.mark.parametrize("legacy", [False, True], ids=["current", "legacy"])
def test_explicit_retry_uses_a_new_receipt_and_keeps_the_first(store, legacy):
    transfer = refused_attempt(store)
    first_id = cms_consent.media_receipt_id(transfer.id)
    with store.database.session() as db:
        first_targets = db.get(CmsPublicationConsent, first_id).targets
        if legacy:
            db.get(CmsMediaTransfer, transfer.id).attempt_id = None
            db.commit()
    expected = "https://atelier.example/images/velo.webp"

    def upload(**kwargs):
        with store.database.session() as db:
            current = db.get(CmsMediaTransfer, transfer.id)
            proof = db.get(CmsPublicationConsent, current.attempt_id)
            assert current.state == "uploading" and proof.id != first_id
            assert proof.targets == first_targets
        return {"id": "123", "url": expected}

    transport = SimpleNamespace(upload_image=upload)
    assert image_url(store.database, **values(store, transport, approved=False, retry_attempts=(first_id,))) == expected
    with store.database.session() as db:
        proofs = {proof.id: proof.targets for proof in db.scalars(select(CmsPublicationConsent))}
        latest = db.get(CmsMediaTransfer, transfer.id)
        assert latest.state == "ready" and len(proofs) == 2
        assert proofs[first_id] == first_targets
        assert proofs[latest.attempt_id]["result"]["remote_id"] == "123"


def test_overlapping_retries_and_a_replayed_form_cannot_send_twice(store):
    previous = refused_attempt(store)
    started, release = Event(), Event()

    def upload(**kwargs):
        started.set()
        assert release.wait(5)
        refuse_upload()

    kwargs = values(store, SimpleNamespace(upload_image=upload), retry_attempts=(previous.attempt_id,))
    with ThreadPoolExecutor(max_workers=1) as pool:
        first = pool.submit(image_url, store.database, **kwargs)
        try:
            assert started.wait(5)
            with pytest.raises(PublicationError) as overlapping:
                image_url(store.database, **kwargs)
            assert overlapping.value.code == "media_uncertain"
        finally:
            release.set()
        with pytest.raises(PublicationError) as completed:
            first.result(timeout=5)
        assert completed.value.code == "media_failed"
    with pytest.raises(PublicationError) as replayed:
        image_url(store.database, **kwargs)
    assert replayed.value.code == "media_retry_approval"
    assert publication.count(store, CmsPublicationConsent) == 2


def test_storage_failure_rolls_back_retry_without_contacting_the_cms(store, monkeypatch):
    previous = refused_attempt(store)

    def failed_capture(*args, **kwargs):
        raise SQLAlchemyError("test storage failure")

    monkeypatch.setattr(cms_media_workflow.cms_consent, "capture", failed_capture)
    transport = SimpleNamespace(upload_image=lambda **kwargs: pytest.fail("Approval was not saved"))
    with pytest.raises(PublicationError) as refused:
        image_url(store.database, **values(store, transport, retry_attempts=(previous.attempt_id,)))
    assert refused.value.code == "media_storage"
    with store.database.session() as db:
        current = db.get(CmsMediaTransfer, previous.id)
        assert current.state == "failed" and current.attempt_id == previous.attempt_id
    assert publication.count(store, CmsPublicationConsent) == 1


def test_failure_to_record_a_sent_upload_never_rearms_it(store, monkeypatch):
    previous = refused_attempt(store)
    sent = []

    def uploaded(**kwargs):
        sent.append(True)
        return {"id": "42", "url": "https://atelier.example/images/velo.webp"}

    def failed_result(*args, **kwargs):
        raise SQLAlchemyError("test storage failure after upload")

    monkeypatch.setattr(cms_media_workflow.cms_consent, "record_result", failed_result)
    kwargs = values(store, SimpleNamespace(upload_image=uploaded), retry_attempts=(previous.attempt_id,))
    for _ in range(2):
        with pytest.raises(PublicationError) as error:
            image_url(store.database, **kwargs)
        assert error.value.code == "media_uncertain"
    assert sent == [True]
    with store.database.session() as db:
        assert db.get(CmsMediaTransfer, previous.id).state == "uploading"
    assert publication.count(store, CmsPublicationConsent) == 2
