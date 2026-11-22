"""The scheduler never restores stale settings and never repeats an uncertain route."""

import pytest
from sqlalchemy import select

from backend import app as m, automation_mandates as mandates, correction_execution as execution
from backend.models import AutomationRun, Project
import test_le_mode_automatique_est_etroit as original

projet, plan, github, modele = original.projet, original.plan, original.github, original.modele


def withdraw(pid, uid):
    with m._automation_settings_transaction(pid) as (db, project):
        project.settings = {**project.settings, "content_auto": {**project.settings["content_auto"], "enabled": False},
                            "concurrent_setting": "preserved"}
        mandates.change(db, project=project, actor_id=uid, mode="content_schedule", consent=False, active=False)


def test_withdrawal_after_selection_never_restores_enabled_or_last_run(projet, plan, github, modele, monkeypatch):
    _slug, pid, uid = projet
    pick = m._sujets_non_couverts
    def selected(*args, **kwargs):
        subjects = pick(*args, **kwargs)
        withdraw(pid, uid)
        return subjects
    monkeypatch.setattr(m, "_sujets_non_couverts", selected)
    result = m._balayer_contenu_auto()
    assert result["proposees"] == 0 and result["refusees"] == 1
    assert modele == [] and github["post"] == [] and github["put"] == []
    with m.DB.session() as db:
        project = db.get(Project, pid)
        assert project.settings["concurrent_setting"] == "preserved"
        assert project.settings["content_auto"]["enabled"] is False
        assert project.settings["content_auto"]["last_run"] == 0
        assert db.scalar(select(AutomationRun.id).where(AutomationRun.project_id == pid)) is None


def test_withdrawal_when_provider_returns_stops_before_any_branch_write(projet, plan, github, modele, monkeypatch):
    _slug, pid, uid = projet
    generate = m._correction_ai_json
    def provider(**kwargs):
        execution.provider_request(system=kwargs["system"], user_msg=kwargs["user_msg"], max_tokens=2048)
        result = generate(**kwargs)
        withdraw(pid, uid)
        return result
    monkeypatch.setattr(m, "_correction_ai_json", provider)
    result = m._balayer_contenu_auto()
    assert result["proposees"] == 0 and result["refusees"] == 1
    assert len(modele) == 1 and github["post"] == [] and github["put"] == []
    with m.DB.session() as db:
        run, = db.scalars(select(AutomationRun).where(AutomationRun.project_id == pid))
        assert run.state == "uncertain" and run.provider_calls == 1 and run.writes == 0
        assert db.get(Project, pid).settings["content_auto"]["enabled"] is False


def test_started_unknown_route_survives_restart_and_later_activation(projet, plan, github, modele, monkeypatch):
    _slug, pid, uid = projet
    def interrupted(*args, **kwargs):
        execution.provider_request(system="fixture", user_msg="fixture", max_tokens=50)
        raise RuntimeError("unknown provider result")
    monkeypatch.setattr(m, "_proposer_une_page", interrupted)
    assert m._balayer_contenu_auto()["refusees"] == 1
    withdraw(pid, uid)
    with m._automation_settings_transaction(pid) as (db, project):
        project.settings = {**project.settings, "content_auto": {**project.settings["content_auto"], "enabled": True, "last_run": 0}}
        mandates.change(db, project=project, actor_id=uid, mode="content_schedule", consent=True, active=True)
    monkeypatch.setattr(m, "_proposer_une_page", lambda *a, **kw: pytest.fail("Uncertain route must not be retried"))
    assert m._balayer_contenu_auto()["refusees"] == 1
    with m.DB.session() as db:
        run, = db.scalars(select(AutomationRun).where(AutomationRun.project_id == pid))
        assert run.state == "uncertain" and run.provider_calls == 1
    assert modele == [] and not github["post"] and not github["put"]
