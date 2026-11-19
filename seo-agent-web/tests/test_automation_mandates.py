"""Mode activation is explicit, signed proposals are bound, and withdrawal stops later transports."""
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
import json
import os
from pathlib import Path
import tempfile

import pytest
from sqlalchemy import select

TEST_ROOT = Path(tempfile.mkdtemp(prefix="noyaru-mode-mandates-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m, automation_mandates as mandates, correction_execution as execution, export_compte
from backend.models import AutomationMandate, AutomationRun, CmsPublicationConsent, Project
import test_cms_publication_workflow as publication

store = publication.store


@pytest.fixture()
def grant(store, monkeypatch):
    monkeypatch.setattr(m, "DB", store.database)
    with store.database.session() as db:
        project = db.get(Project, store.project_id)
        project.settings = {"github_repo": "fixture/site", "github_branch": "main", "github_mode": "auto",
                            "content_auto": {"enabled": True, "section": "/blog", "publication": "auto"}}
        row = mandates.change(db, project=project, actor_id=store.user_id, mode="github_merge", consent=True, active=True)
        db.commit()
    return SimpleNamespace(store=store, key=row.id)


def revoke(g, *, reenable=False):
    with g.store.database.session() as db:
        project = db.get(Project, g.store.project_id)
        project.settings = {**project.settings, "github_mode": "review"}
        mandates.change(db, project=project, actor_id=g.store.user_id, mode="github_merge", consent=False, active=False)
        db.commit()
        if reenable:
            project.settings = {**project.settings, "github_mode": "auto"}
            row = mandates.change(db, project=project, actor_id=g.store.user_id, mode="github_merge", consent=True, active=True)
            db.commit()
            return row.id


def proof_rows(g):
    with g.store.database.session() as db:
        return list(db.scalars(select(CmsPublicationConsent)))


def fence(g, key="one-operation"):
    return mandates.fenced(project_id=g.store.project_id, mode="github_merge", mandate_key=g.key,
                           request_key=publication.workflow.digest(key), data={"test_target": "one"})


def response():
    return SimpleNamespace(status_code=200, text="", json=lambda: {"ok": True})


def test_repeated_unchanged_setting_keeps_one_real_approval_and_no_secret_in_export(grant):
    g = grant
    original, = proof_rows(g)
    with g.store.database.session() as db:
        project = db.get(Project, g.store.project_id)
        assert mandates.change(db, project=project, actor_id=g.store.user_id, mode="github_merge", consent=False, active=True).id == g.key
        db.commit()
        exported = export_compte.lignes_du_compte(db, g.store.user_id)
        functional, = exported["automation_mandates"]
        assert "signing_secret" not in functional
        assert functional["scope_key"]
    current, = proof_rows(g)
    assert current.id == original.id and current.approved_at == original.approved_at and current.expires_at == original.expires_at
    assert current.operation == "mode_activation" and current.actor_email and current.site_url


def test_legacy_enabled_setting_is_not_an_activation(grant):
    g = grant
    with g.store.database.session() as db:
        db.delete(db.get(AutomationMandate, g.key)); db.commit()
        project = db.get(Project, g.store.project_id)
        assert mandates.current_grant(db, project, "github_merge") is None
        with pytest.raises(mandates.ConsentRequired):
            mandates.change(db, project=project, actor_id=g.store.user_id, mode="github_merge", consent=False, active=True)
    with pytest.raises(execution.ExecutionStopped, match="automation_revoked"):
        with fence(g):
            pytest.fail("An old setting must not supply permission")


def test_withdrawal_then_reactivation_never_reauthorizes_an_old_task(grant):
    g = grant
    new = revoke(g, reenable=True)
    assert new != g.key
    assert sorted(p.operation for p in proof_rows(g)) == ["mode_activation", "mode_activation", "mode_revocation"]
    with pytest.raises(execution.ExecutionStopped, match="automation_revoked"):
        with fence(g):
            pytest.fail("Old activation must remain withdrawn")


def test_actual_github_transport_rechecks_revocation_between_requests(grant, monkeypatch):
    g = grant; calls = []
    def put(*args, **kwargs):
        calls.append(args[0])
        revoke(g)  # The first request has already left; the next must be stopped.
        return response()
    monkeypatch.setattr(m.requests, "put", put)
    with pytest.raises(execution.ExecutionStopped, match="automation_revoked"):
        with fence(g):
            m._github_api_put("/repos/fixture/site/contents/a", token="fictional", json_body={})
            m._github_api_put("/repos/fixture/site/contents/b", token="fictional", json_body={})
    assert len(calls) == 1
    with g.store.database.session() as db:
        run, = list(db.scalars(select(AutomationRun)))
        assert run.writes == 1


def test_another_repository_cannot_use_the_grant(grant, monkeypatch):
    monkeypatch.setattr(m.requests, "put", lambda *a, **kw: pytest.fail("Wrong repository"))
    with pytest.raises(execution.ExecutionStopped, match="automation_target_changed"):
        with fence(grant):
            m._github_api_put("/repos/fixture/other/contents/a", token="fictional", json_body={})


def test_provider_guard_propagates_into_explicitly_copied_thread_context(grant):
    with fence(grant):
        worker = m._dans_ce_contexte(lambda: execution.provider_request(system="fictional", user_msg="data", max_tokens=5))
        revoke(grant)
        with ThreadPoolExecutor(max_workers=1) as pool:
            with pytest.raises(execution.ExecutionStopped, match="automation_revoked"):
                pool.submit(worker).result(timeout=10)
    with grant.store.database.session() as db:
        assert not list(db.scalars(select(AutomationRun)))


def test_unknown_write_is_reserved_once_and_cannot_be_replayed(grant, monkeypatch):
    calls = []
    def uncertain(*args, **kwargs):
        calls.append(True)
        raise TimeoutError("fictitious response lost")
    monkeypatch.setattr(m.requests, "put", uncertain)
    with pytest.raises(TimeoutError):
        with fence(grant) as active:
            m._github_api_put("/repos/fixture/site/pulls/7/merge", token="fictional", json_body={})
    mandates.finish(active.run_id, reason="unconfirmed")
    with grant.store.database.session() as db:
        assert db.get(AutomationRun, active.run_id).state == "uncertain"
    with pytest.raises(execution.ExecutionStopped, match="automation_already_started"):
        with fence(grant):
            m._github_api_put("/repos/fixture/site/pulls/7/merge", token="fictional", json_body={})
    assert len(calls) == 1


def test_read_only_poll_does_not_consume_the_future_merge_intent(grant, monkeypatch):
    monkeypatch.setattr(m.requests, "get", lambda *args, **kwargs: response())
    for _ in range(2):
        with fence(grant):
            m._github_api_get("/repos/fixture/site/pulls/7", token="fictional")
    with grant.store.database.session() as db:
        assert not list(db.scalars(select(AutomationRun)))


@pytest.mark.parametrize("changed", ["sha", "node_id", "number", "signature"])
def test_editable_task_metadata_cannot_forge_a_signed_proposal(grant, changed):
    with grant.store.database.session() as db:
        project = db.get(Project, grant.store.project_id)
    pr = {"head": {"sha": "a" * 40}, "node_id": "PR_fictional", "number": 7}
    verification = m._bloc_verification(pr, fusion_auto=True, project=project)
    number = 7
    if changed == "number":
        number = 8
    elif changed == "signature":
        verification["automation"]["signature"] = "0" * 64
    else:
        verification[changed] = "different"
    with grant.store.database.session() as db:
        with pytest.raises(execution.ExecutionStopped, match="automation_target_changed"):
            mandates.verify(db, project_id=project.id, verification=verification, number=number)


def test_valid_signature_binds_the_exact_pr_and_activation(grant):
    with grant.store.database.session() as db:
        project = db.get(Project, grant.store.project_id)
    pr = {"head": {"sha": "a" * 40}, "node_id": "PR_fictional", "number": 7}
    verification = m._bloc_verification(pr, fusion_auto=True, project=project)
    with grant.store.database.session() as db:
        assert mandates.verify(db, project_id=project.id, verification=verification, number=7).id == grant.key
    assert "signing_secret" not in json.dumps(verification)


def test_reducing_to_human_review_does_not_fabricate_an_activation_or_renew_original_proof(grant):
    g = grant
    with g.store.database.session() as db:
        project = db.get(Project, g.store.project_id)
        original = mandates.change(db, project=project, actor_id=g.store.user_id, mode="content_schedule", consent=True, active=True)
        db.commit()
        proof_id = original.receipt_id
        expiry = db.get(CmsPublicationConsent, proof_id).expires_at
        settings = {**project.settings, "content_auto": {**project.settings["content_auto"], "publication": "verification"}}
        project.settings = settings
        restricted = mandates.change(db, project=project, actor_id=g.store.user_id, mode="content_schedule", consent=False, active=True)
        db.commit()
        assert restricted.id != original.id and restricted.receipt_id == proof_id
        assert db.get(CmsPublicationConsent, proof_id).expires_at == expiry
    assert len([p for p in proof_rows(g) if p.operation == "mode_activation"]) == 2  # GitHub + initial content mode.
    assert len([p for p in proof_rows(g) if p.operation == "mode_restriction"]) == 1


@pytest.mark.parametrize("tamper", ["permission", "sitemap"], ids=["draft-to-publish", "remove-control"])
def test_proposal_permissions_and_required_controls_cannot_be_edited(grant, tamper):
    with grant.store.database.session() as db:
        project = db.get(Project, grant.store.project_id)
    verification = m._bloc_verification({"head": {"sha": "a" * 40}, "node_id": "PR_fictional", "number": 7},
        fusion_auto=False, project=project, controle_sitemap={"source": "/sitemap.xml"})
    if tamper == "permission":
        verification["fusion_auto"] = True
    else:
        verification.pop("controle_sitemap")
    with grant.store.database.session() as db:
        with pytest.raises(execution.ExecutionStopped, match="automation_target_changed"):
            mandates.verify(db, project_id=project.id, verification=verification, number=7)


def pending_task(g, *, content=False):
    from backend.models import IssueTask
    with g.store.database.session() as db:
        project = db.get(Project, g.store.project_id)
        if content:
            project.settings = {**project.settings, "content_auto": {**project.settings["content_auto"], "publication": "verification"}}
            row = mandates.change(db, project=project, actor_id=g.store.user_id, mode="content_schedule", consent=True, active=True)
            db.commit()
            with mandates.fenced(project_id=project.id, mode="content_schedule", mandate_key=row.id):
                verification = m._bloc_verification({"head": {"sha": "a" * 40}, "node_id": "PR_fixture", "number": 7}, fusion_auto=False)
        else:
            verification = m._bloc_verification({"head": {"sha": "a" * 40}, "node_id": "PR_fixture", "number": 7}, fusion_auto=True, project=project)
        task = db.get(IssueTask, g.store.task_id)
        task.note = json.dumps({"verification": verification, "pr_number": 7, "pr_title": "Fictional SEO proposal"})
        db.commit()
        return task


def merge_transport(g, monkeypatch, *, after_ready=None, merge_result=None):
    calls = []
    monkeypatch.setattr(m, "_effective_user_connection_value", lambda **kw: ("fictional", "user"))
    monkeypatch.setattr(m, "_github_pr_is_open", lambda *a: True)
    monkeypatch.setattr(m, "_verifications_du_commit", lambda **kw: {"total": 1, "en_cours": 0, "echoues": [], "reussis": 1})
    def post(url, **kw):
        calls.append(("POST", url, kw["json"]))
        if after_ready:
            after_ready()
        return SimpleNamespace(status_code=200, text="", json=lambda: {"data": {"markPullRequestReadyForReview": {"pullRequest": {"isDraft": False}}}})
    def put(url, **kw):
        calls.append(("PUT", url, kw["json"]))
        return SimpleNamespace(status_code=200, text="", json=lambda: {"merged": True} if merge_result is None else merge_result)
    monkeypatch.setattr(m.requests, "get", lambda *a, **kw: SimpleNamespace(status_code=200, text="", json=lambda: {
        "state": "open", "number": 7, "node_id": "PR_fixture", "head": {"sha": "a" * 40},
        "base": {"ref": "main", "repo": {"full_name": "fixture/site"}}}))
    monkeypatch.setattr(m.requests, "post", post)
    monkeypatch.setattr(m.requests, "put", put)
    return calls


@pytest.mark.parametrize("reenable", [False, True], ids=["withdraw", "off-on"])
def test_pending_merge_withdrawal_blocks_all_transports(grant, monkeypatch, reenable):
    task = pending_task(grant)
    calls = merge_transport(grant, monkeypatch)
    revoke(grant, reenable=reenable)
    assert m._reprendre_une_verification(task) == "autorisation_retiree"
    assert calls == []


def test_withdrawal_between_ready_and_merge_stops_the_next_real_transport(grant, monkeypatch):
    task = pending_task(grant)
    calls = merge_transport(grant, monkeypatch, after_ready=lambda: revoke(grant))
    assert m._reprendre_une_verification(task) == "autorisation_retiree"
    assert [x[0] for x in calls] == ["POST"]
    with grant.store.database.session() as db:
        run, = db.scalars(select(AutomationRun))
        assert run.state == "uncertain" and run.writes == 1


def test_merge_sends_only_the_attested_sha_and_completed_run(grant, monkeypatch):
    task = pending_task(grant)
    calls = merge_transport(grant, monkeypatch)
    assert m._reprendre_une_verification(task) == "fusionnee"
    assert [x[0] for x in calls] == ["POST", "PUT"]
    assert calls[-1][2]["sha"] == "a" * 40
    with grant.store.database.session() as db:
        run, = db.scalars(select(AutomationRun))
        assert run.state == "completed" and run.writes == 2


@pytest.mark.parametrize("result", [{}, {"merged": False}], ids=["missing-ack", "refused"])
def test_unconfirmed_merge_is_never_marked_done_or_replayed(grant, monkeypatch, result):
    from backend.models import IssueTask
    task = pending_task(grant)
    original = task.note
    calls = merge_transport(grant, monkeypatch, merge_result=result)
    assert m._reprendre_une_verification(task) != "fusionnee"
    with grant.store.database.session() as db:
        assert db.get(IssueTask, task.id).status != "done"
        run, = db.scalars(select(AutomationRun))
        assert run.state == "uncertain"
        # Replaying the original signed note cannot repeat an unconfirmed remote mutation.
        replay = json.loads(original)
        replay["pr_number"] = str(replay["pr_number"])  # Same attested identity, different JSON type.
        task.note = json.dumps(replay)
    assert m._reprendre_une_verification(task) == "resultat_a_verifier"
    assert [x[0] for x in calls] == ["POST", "PUT"]


def test_scheduled_draft_promotion_still_obeys_its_own_activation(grant, monkeypatch):
    task = pending_task(grant, content=True)
    calls = merge_transport(grant, monkeypatch)
    with grant.store.database.session() as db:
        project = db.get(Project, grant.store.project_id)
        project.settings = {**project.settings, "content_auto": {**project.settings["content_auto"], "enabled": False}}
        mandates.change(db, project=project, actor_id=grant.store.user_id, mode="content_schedule", consent=False, active=False)
        db.commit()
    assert m._reprendre_une_verification(task) == "autorisation_retiree"
    assert calls == []


def test_editable_manual_flag_cannot_bypass_the_activation(grant, monkeypatch):
    task = pending_task(grant)
    note = json.loads(task.note)
    note["verification"]["valide_par_client"] = True
    task.note = json.dumps(note)
    calls = merge_transport(grant, monkeypatch)
    assert m._reprendre_une_verification(task) == "autorisation_retiree"
    assert calls == []


def test_erasing_a_scheduled_origin_in_the_note_does_not_make_it_manual(grant, monkeypatch):
    task = pending_task(grant, content=True)
    with grant.store.database.session() as db:
        project = db.get(Project, grant.store.project_id)
        active = mandates.current_grant(db, project, "content_schedule")
        db.add(AutomationRun(project_id=project.id, mode="content_schedule", mandate_key=active.id,
            request_key="e" * 64, state="completed", data={"task_id": task.id}))
        db.commit()
    note = json.loads(task.note)
    note["verification"].pop("automation")
    task.note = json.dumps(note)
    calls = merge_transport(grant, monkeypatch)
    assert m._reprendre_une_verification(task) == "autorisation_retiree"
    assert calls == []


def test_scheduled_draft_success_does_not_claim_uncertainty_or_permission_to_merge(grant, monkeypatch):
    task = pending_task(grant, content=True)
    calls = merge_transport(grant, monkeypatch)
    assert m._reprendre_une_verification(task) == "verifiee"
    assert [x[0] for x in calls] == ["POST"]
    with grant.store.database.session() as db:
        run, = db.scalars(select(AutomationRun))
        assert run.state == "completed" and run.writes == 1


@pytest.mark.parametrize("field", ["branch", "head", "repository"], ids=["new-base", "new-head", "new-repo"])
def test_remote_target_change_after_ready_cannot_be_merged(grant, monkeypatch, field):
    task = pending_task(grant)
    remote = {"state": "open", "number": 7, "node_id": "PR_fixture", "head": {"sha": "a" * 40},
              "base": {"ref": "main", "repo": {"full_name": "fixture/site"}}}
    def change():
        if field == "branch": remote["base"]["ref"] = "release"
        elif field == "head": remote["head"]["sha"] = "b" * 40
        else: remote["base"]["repo"]["full_name"] = "fixture/other"
    calls = merge_transport(grant, monkeypatch, after_ready=change)
    monkeypatch.setattr(m.requests, "get", lambda *a, **kw: SimpleNamespace(status_code=200, text="", json=lambda: remote))
    assert m._reprendre_une_verification(task) == "autorisation_retiree"
    assert [x[0] for x in calls] == ["POST"]
