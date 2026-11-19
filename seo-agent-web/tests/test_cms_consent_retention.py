"""Approval evidence survives deletion, stays isolated and expires at its original anniversary."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from backend import cms_consent, cms_media_workflow, export_compte
from backend.models import (AccountMember, CmsMediaTransfer, CmsPublication, CmsPublicationConsent,
                            IssueTask, Project, User)
import test_cms_publication_workflow as publication

store = publication.store


def receipts(store):
    with store.database.session() as db:
        return list(db.scalars(select(CmsPublicationConsent).order_by(CmsPublicationConsent.approved_at)))


def media(store, *, upload=None):
    return cms_media_workflow.image_url(store.database, project_id=store.project_id,
        task_id=store.task_id, platform="wordpress", connection_fingerprint="fictional",
        name="private-source.webp", data=b"fictitious-normalized-image", approved=True,
        actor_id=store.user_id, transport=SimpleNamespace(upload_image=upload or (
            lambda **kwargs: {"id": "image-1", "url": "https://atelier.example/images/one.webp"})))


def completed(store):
    row = publication.prepare(store)
    adapter = publication.FakeAdapter()
    draft = publication.execute(store, row, adapter)
    publication.execute(store, draft, adapter, operation="publish")
    media(store)
    return row.id


def test_exact_minimal_snapshot_precedes_write_and_observation_keeps_approval_date(store):
    row = publication.prepare(store, config={"collection_id": "collection-1", "destination_label": "PRIVATE-LABEL",
                                           "unused": "DO-NOT-RETAIN"})
    adapter = publication.FakeAdapter()
    approved_dates = []
    def witness():
        proof, = receipts(store)
        assert proof.owner_user_id == store.user_id and proof.actor_id == store.user_id
        assert proof.actor_email == "publication-owner@example.invalid"
        assert proof.site_url == "https://atelier.example/" and proof.snapshot_source == "captured"
        assert proof.targets["destination"] == {"collection_id": "collection-1"}
        assert "result" not in proof.targets
        approved_dates.append(proof.approved_at)
        assert cms_consent.utc(proof.expires_at) == cms_consent.expiration(proof.approved_at)
    adapter.on_create = witness
    publication.execute(store, row, adapter)
    proof, = receipts(store)
    assert proof.approved_at == approved_dates[0]
    assert proof.targets["result"]["remote_id"] == adapter.current.id
    assert proof.targets["result"]["url"] == adapter.current.url
    assert proof.content_digest == publication.workflow.digest(row.data["article"])
    data = json.dumps(proof.targets)
    for forbidden in ("html", "chiffon", "PRIVATE-LABEL", "DO-NOT-RETAIN", "fictional-connection"):
        assert forbidden not in data


def test_media_has_its_own_durable_byte_digest_before_upload(store):
    def upload(**kwargs):
        proof, = receipts(store)
        assert proof.operation == "media_upload" and proof.publication_id is None
        assert proof.content_digest == hashlib.sha256(kwargs["data"]).hexdigest()
        assert proof.actor_email and proof.site_url and proof.approved_at
        assert "private-source.webp" not in json.dumps(proof.targets)
        return {"id": "media-1", "url": "https://atelier.example/images/one.webp?token=PRIVATE#preview"}
    media(store, upload=upload)
    proof, = receipts(store)
    assert proof.targets["result"]["url"] == "https://atelier.example/images/one.webp"
    assert "PRIVATE" not in json.dumps(proof.targets)
    media(store)  # Reuse does not fabricate a second approval or upload.
    assert len(receipts(store)) == 1


@pytest.mark.parametrize("parent", ["project", "task", "publication", "account"])
def test_receipts_survive_actual_foreign_key_deletions(store, parent):
    publication_id = completed(store)
    before = receipts(store)
    assert len(before) == 3
    model, ident = {"project": (Project, store.project_id), "task": (IssueTask, store.task_id),
                    "publication": (CmsPublication, publication_id), "account": (User, store.user_id)}[parent]
    with store.database.session() as db:
        db.delete(db.get(model, ident))
        db.commit()
    after = receipts(store)
    assert [p.id for p in before] == [p.id for p in after]
    for old, new in zip(before, after):
        assert (new.actor_email, new.site_url, new.content_digest, new.approved_at, new.expires_at, new.targets) == (
            old.actor_email, old.site_url, old.content_digest, old.approved_at, old.expires_at, old.targets)
        if parent in {"project", "account"}:
            assert new.project_id is None
        if parent in {"project", "task", "publication", "account"}:
            assert new.publication_id is None
        if parent == "account":
            assert new.owner_user_id is None and new.actor_id is None
    with store.database.session() as db:
        if parent in {"project", "task", "account"}:
            assert not list(db.scalars(select(CmsMediaTransfer)))


def test_member_approval_retains_real_author_and_exports_only_to_project_owner(store):
    with store.database.session() as db:
        member = User(email="member@example.invalid", password_hash="test")
        db.add(member); db.flush()
        db.add(AccountMember(owner_user_id=store.user_id, member_user_id=member.id))
        db.commit()
        member_id = member.id
    row = publication.prepare(store)
    publication.execute(store, row, publication.FakeAdapter(), actor_id=member_id)
    with store.database.session() as db:
        assert "cms_publication_consents" not in export_compte.lignes_du_compte(db, member_id)
        db.delete(db.get(Project, store.project_id)); db.commit()
        exported = export_compte.lignes_du_compte(db, store.user_id)["cms_publication_consents"]
        assert len(exported) == 1 and exported[0]["actor_email"] == "member@example.invalid"
        db.delete(db.get(User, member_id)); db.commit()
    proof, = receipts(store)
    assert proof.actor_id is None and proof.actor_email == "member@example.invalid"
    assert proof.owner_user_id == store.user_id


def test_export_does_not_follow_actor_or_reassigned_email_to_another_account(store):
    completed(store)
    with store.database.session() as db:
        other = db.get(Project, store.other_project_id)
        db.add(AccountMember(owner_user_id=other.owner_user_id, member_user_id=store.user_id))
        db.commit()
    row = publication.prepare(store, project_id=store.other_project_id, task_id=store.other_task_id)
    publication.workflow.execute(store.database, project_id=store.other_project_id, publication_id=row.id,
        operation="create", actor_id=store.user_id, approved=True, adapter=publication.FakeAdapter(),
        connection_fingerprint=publication.CONNECTION, current_article_digest=publication.workflow.digest(row.data["article"]))
    with store.database.session() as db:
        db.delete(db.get(Project, store.project_id))
        db.delete(db.get(Project, store.other_project_id)); db.commit()
        exported = export_compte.lignes_du_compte(db, store.user_id)["cms_publication_consents"]
        assert len(exported) == 3 and all(p["site_url"] == "https://atelier.example/" for p in exported)
        old_email = db.get(User, store.user_id).email
        db.delete(db.get(User, store.user_id)); db.commit()
        new = User(email=old_email, password_hash="new-account")
        db.add(new); db.commit()
        assert "cms_publication_consents" not in export_compte.lignes_du_compte(db, new.id)


def test_later_profile_and_site_changes_do_not_rewrite_the_original_snapshot(store):
    row = publication.prepare(store)
    publication.execute(store, row, publication.FakeAdapter())
    with store.database.session() as db:
        db.get(User, store.user_id).email = "changed@example.invalid"
        db.get(Project, store.project_id).base_url = "https://different.example/"
        db.commit()
    proof, = receipts(store)
    assert proof.actor_email == "publication-owner@example.invalid" and proof.site_url == "https://atelier.example/"


def test_purge_uses_original_expiry_including_active_and_deleted_projects(store):
    completed(store)
    now = datetime(2031, 11, 19, 12, tzinfo=timezone.utc)
    with store.database.session() as db:
        rows = list(db.scalars(select(CmsPublicationConsent).order_by(CmsPublicationConsent.approved_at)))
        for proof, offset in zip(rows, [-1, 0, 1]):
            proof.approved_at = now.replace(year=2026) + timedelta(microseconds=offset)
            proof.expires_at = cms_consent.expiration(proof.approved_at)
        db.commit()
        assert cms_consent.purger(db, now) == 2
        remaining, = list(db.scalars(select(CmsPublicationConsent)))
        stamp = remaining.approved_at
        expiry = remaining.expires_at
        db.delete(db.get(Project, store.project_id)); db.commit()
        assert remaining.approved_at == stamp and remaining.expires_at == expiry
        assert cms_consent.purger(db, now + timedelta(microseconds=1)) == 1
        assert db.get(User, store.user_id) is not None


def test_february_29_expires_on_february_28_without_an_extra_day(store):
    stamp = datetime(2024, 2, 29, 12, 30, tzinfo=timezone.utc)
    assert cms_consent.expiration(stamp) == datetime(2029, 2, 28, 12, 30, tzinfo=timezone.utc)
    with store.database.session() as db:
        proof = CmsPublicationConsent(actor_email="legacy@example.invalid", operation="create",
            content_digest="a" * 64, targets={}, approved_at=stamp)  # Incomplete legacy expiry is derived, not renewed.
        db.add(proof); db.commit()
        assert cms_consent.purger(db, datetime(2029, 2, 28, 12, 29, tzinfo=timezone.utc)) == 0
        assert cms_consent.purger(db, datetime(2029, 2, 28, 12, 30, tzinfo=timezone.utc)) == 1


@pytest.mark.parametrize("operation", ["publication", "media"])
def test_failed_evidence_commit_never_sends_a_remote_write(store, monkeypatch, operation):
    row = publication.prepare(store)
    calls = []
    from sqlalchemy.orm import Session
    original = Session.commit
    def reject_proof(session):
        if any(isinstance(obj, CmsPublicationConsent) for obj in session.new) or session.scalar(select(CmsPublicationConsent.id).limit(1)):
            raise SQLAlchemyError("Fictitious evidence storage failure")
        return original(session)
    monkeypatch.setattr(Session, "commit", reject_proof)
    with pytest.raises(SQLAlchemyError):
        if operation == "publication":
            adapter = publication.FakeAdapter()
            adapter.on_create = lambda: calls.append("create")
            publication.execute(store, row, adapter)
        else:
            media(store, upload=lambda **kwargs: calls.append("upload"))
    assert not calls and not receipts(store)
    assert publication.load(store, row.id).state == "prepared"
    with store.database.session() as db:
        assert db.scalar(select(func.count()).select_from(CmsMediaTransfer)) == 0


@pytest.mark.parametrize("value, expected", [
    ("https://example.test/index.php?controller=cms&id_cms=12&preview=SECRET#x", "https://example.test/index.php?controller=cms&id_cms=12"),
    ("https://user:SECRET@example.test/page", None), ("javascript:SECRET", None),
    ("https://example.test/page?token=SECRET", "https://example.test/page"),
], ids=["cms-id", "userinfo", "scheme", "preview-token"])
def test_retained_urls_keep_identity_without_preview_secrets(value, expected):
    assert cms_consent.public_url(value) == expected


def test_retention_loop_purges_cms_evidence_even_without_other_cleanup(store, monkeypatch):
    from backend import app as m
    completed(store)
    with store.database.session() as db:
        for proof in db.scalars(select(CmsPublicationConsent)):
            proof.approved_at = datetime(2000, 1, 1, tzinfo=timezone.utc)
            proof.expires_at = cms_consent.expiration(proof.approved_at)
        db.commit()
    monkeypatch.setattr(m, "DB", store.database)
    monkeypatch.setattr(m, "_cleanup_old_jobs", lambda: None)
    monkeypatch.setattr(m, "_cleanup_old_runs", lambda: None)
    monkeypatch.setattr(m, "_purger_preuves_dpa", lambda: None)
    class Stop:
        done = False
        def is_set(self):
            return self.done
        def wait(self, timeout):
            self.done = True
    monkeypatch.setattr(m, "_WORKER_STOP", Stop())
    m._retention_loop()
    assert not receipts(store)


def test_purge_failure_is_logged_without_private_payload_and_next_cycle_retries(store, monkeypatch, caplog):
    from backend import app as m
    completed(store)
    with store.database.session() as db:
        for proof in db.scalars(select(CmsPublicationConsent)):
            proof.expires_at = datetime(2000, 1, 1, tzinfo=timezone.utc)
        db.commit()
    monkeypatch.setattr(m, "DB", store.database)
    original = cms_consent.purger
    def fail(*args):
        raise RuntimeError("PRIVATE-DATABASE-PAYLOAD")
    monkeypatch.setattr(cms_consent, "purger", fail)
    m._purger_preuves_cms()
    assert len(receipts(store)) == 3
    assert "RuntimeError" in caplog.text and "PRIVATE-DATABASE-PAYLOAD" not in caplog.text
    monkeypatch.setattr(cms_consent, "purger", original)
    m._purger_preuves_cms()
    assert not receipts(store)


@pytest.mark.parametrize("operation", ["project", "account"])
def test_real_delete_routes_preserve_proofs_and_project_export_still_finds_them(store, tmp_path, monkeypatch, operation):
    import io
    import os
    import zipfile
    from fastapi.testclient import TestClient
    from backend import app as m, auth
    completed(store)
    monkeypatch.setattr(m, "DB", store.database)
    monkeypatch.setattr(m, "DEFAULT_RUNS_DIR", tmp_path / "runs")
    monkeypatch.setattr(m, "_initialize_service", lambda **kwargs: None)
    monkeypatch.setattr(m, "_rate_limit_retry_after", lambda **kwargs: None)
    monkeypatch.setattr(m.billing, "cancel_and_purge_customer", lambda *args, **kwargs: {})
    monkeypatch.setattr(m.telegram, "notifier", lambda *args, **kwargs: None)
    monkeypatch.setattr(m.object_store, "s3_enabled", lambda: False)
    monkeypatch.setenv("SEO_AGENT_DISABLE_WORKER", "true")
    monkeypatch.setenv("SEO_AGENT_SECRET_KEY", "fictional-session-seed-" + "r" * 32)
    with TestClient(m.app) as client:
        client.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(
            user_id=store.user_id, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
        page = client.get("/settings/accounts")
        assert page.status_code == 200
        csrf = page.text.split('name="_csrf" value="', 1)[1].split('"', 1)[0]
        if operation == "project":
            result = client.post("/projects/delete", data={"slugs": "atelier", "_csrf": csrf}, follow_redirects=False)
            assert result.status_code == 303
            exported = client.get("/settings/account/export.zip")
            assert exported.status_code == 200
            with zipfile.ZipFile(io.BytesIO(exported.content)) as archive:
                proofs = json.loads(archive.read("donnees/cms_publication_consents.json"))
                assert len(proofs) == 3 and all(p["project_id"] is None for p in proofs)
                assert all(p["site_url"] == "https://atelier.example/" for p in proofs)
                assert "donnees/cms_publications.json" not in archive.namelist()
        else:
            result = client.post("/settings/account/delete", data={"confirm": "DELETE", "_csrf": csrf}, follow_redirects=False)
            assert result.status_code == 303 and "Compte" in result.headers["location"]
    proofs = receipts(store)
    assert len(proofs) == 3 and all(p.project_id is None for p in proofs)
    with store.database.session() as db:
        assert db.get(Project, store.project_id) is None
        if operation == "account":
            assert db.get(User, store.user_id) is None
            assert all(p.owner_user_id is None and p.actor_id is None and p.actor_email for p in proofs)


def test_later_reads_cannot_rewrite_the_original_returned_identity_or_date(store):
    row = publication.prepare(store)
    publication.execute(store, row, publication.FakeAdapter())
    proof, = receipts(store)
    original = proof.targets["result"]
    with store.database.session() as db:
        cms_consent.record_result(db, proof.id, remote_id="other-article", remote_url="https://different.example/")
        cms_consent.record_result(db, proof.id, remote_id=original["remote_id"], remote_url="https://changed.example/")
        db.commit()
    assert receipts(store)[0].targets["result"] == original


def test_missing_result_url_can_be_completed_without_moving_the_original_observation_date(store):
    row = publication.prepare(store)
    adapter = publication.FakeAdapter()
    from dataclasses import replace
    adapter.current = replace(adapter.current, url="")
    publication.execute(store, row, adapter)
    proof, = receipts(store)
    original = proof.targets["result"]["observed_at"]
    with store.database.session() as db:
        cms_consent.record_result(db, proof.id, remote_id=adapter.current.id, remote_url=publication.URL)
        db.commit()
    proof, = receipts(store)
    assert proof.targets["result"]["observed_at"] == original
    assert proof.targets["result"]["url"] == publication.URL and proof.targets["result"]["url_observed_at"]
