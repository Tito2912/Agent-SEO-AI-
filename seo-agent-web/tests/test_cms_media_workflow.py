"""An article's private media is consented, project-scoped and journaled before upload."""
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from backend.cms_commun import CmsError
from backend.cms_media_workflow import image_url
from backend.cms_publication import PublicationError
from backend.models import CmsMediaTransfer
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
    from backend.models import CmsPublicationConsent
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
