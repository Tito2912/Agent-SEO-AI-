"""Activation grants, durable evidence and per-transport revocation for existing modes.

No consent is fabricated from a setting or from an old audit entry. A task carries
one activation identity; switching a mode back on never reauthorizes an old task.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
import os
import hashlib
import hmac
import json
import secrets
import sys
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from . import cms_consent
from .cms_publication import digest, require_actor
from .models import AutomationMandate, AutomationRun, Project, User

_CURRENT = ContextVar("noyaru_automation_authorization", default=None)
MODES = {"github_merge", "content_schedule"}


class ConsentRequired(ValueError):
    pass


def module():
    if "app" in sys.modules:
        return sys.modules["app"]
    from . import app
    return app


def configuration(project, mode, settings=None):
    if mode not in MODES:
        raise ValueError("Unsupported automation mode")
    m = module()
    if settings is not None:
        project = SimpleNamespace(owner_user_id=project.owner_user_id, base_url=project.base_url, settings=settings)
    cfg = m._project_github_cfg(project)
    scope = {"owner_user_id": str(project.owner_user_id), "site_url": cms_consent.public_url(project.base_url),
             "repo": str(cfg["repo"]).casefold(), "branch": cfg["branch"],
             "warning_version": m.legal.AUTO_MODE_WARNING_VERSION}
    if mode == "content_schedule":
        auto = m._reglages_contenu_auto(project.settings)
        scope["content"] = {key: auto[key] for key in (
            "section", "publication", "taille", "ton", "langues", "frequence", "jour", "heure", "fuseau")}
    return scope


def enabled(project, mode):
    m = module()
    if mode == "github_merge":
        return m._project_github_cfg(project)["mode"] == "auto"
    return bool(mode == "content_schedule" and m._reglages_contenu_auto(project.settings)["enabled"])


def current_grant(db, project, mode):
    if not project or not enabled(project, mode):
        return None
    grant = db.scalar(select(AutomationMandate).where(
        AutomationMandate.project_id == project.id, AutomationMandate.mode == mode))
    if grant and grant.scope_key == digest(configuration(project, mode)):
        return grant
    return None


def approved(project, mode):
    with module().DB.session() as db:
        return current_grant(db, project, mode) is not None


def change(db, *, project, actor_id, mode, consent, active):
    """Called in the settings transaction; proof and mode commit or roll back together."""
    _, actor = require_actor(db, str(project.id), str(actor_id))
    previous = db.scalar(select(AutomationMandate).where(
        AutomationMandate.project_id == project.id, AutomationMandate.mode == mode))
    config = configuration(project, mode)
    key = digest(config)
    if active and previous and previous.scope_key == key:
        return previous
    restriction = False
    if active and previous and mode == "content_schedule" and not consent:
        old, new = dict(previous.config), dict(config)
        old_content, new_content = dict(old.pop("content", {})), dict(new.pop("content", {}))
        old_pub, new_pub = old_content.pop("publication", None), new_content.pop("publication", None)
        restriction = old == new and old_content == new_content and old_pub == "auto" and new_pub == "verification"
    if active and not consent and not restriction:
        raise ConsentRequired()
    original_receipt = previous.receipt_id if previous else None
    original_actor = previous.actor_id if previous else actor.id
    if previous:
        cms_consent.capture(db, project=project, actor=actor, operation="mode_revocation",
            content_digest=previous.scope_key,
            targets={"platform": "github", "mode": mode, "mandate_key": previous.id,
                     "configuration": previous.config})
        db.delete(previous)
        db.flush()
    if not active:
        return None
    ident = str(uuid4())
    receipt = cms_consent.capture(db, project=project, actor=actor, operation="mode_restriction" if restriction else "mode_activation",
        content_digest=key, targets={"platform": "github", "mode": mode,
                                   "mandate_key": ident, "configuration": config})
    db.flush()  # Persist the evidence FK before inserting the functional grant.
    grant = AutomationMandate(id=ident, project_id=project.id, mode=mode, scope_key=key, signing_secret=secrets.token_hex(32),
        config=config, actor_id=original_actor if restriction else actor.id,
        receipt_id=original_receipt if restriction else receipt.id)
    db.add(grant)
    return grant


def check(db, *, project_id, mode, mandate_key):
    ExecutionStopped = module().correction_execution.ExecutionStopped
    project = db.get(Project, str(project_id))
    grant = current_grant(db, project, mode)
    if (not grant or not mandate_key or grant.id != mandate_key
            or db.get(User, str(project.owner_user_id)) is None):
        raise ExecutionStopped("automation_revoked")
    return grant


def binding(project=None, mode="github_merge"):
    if entry := current():
        return {"mode": entry.mode, "mandate_key": entry.mandate_key, "run_id": entry.run_id}
    if project is not None:
        with module().DB.session() as db:
            grant = current_grant(db, project, mode)
            if grant:
                return {"mode": mode, "mandate_key": grant.id}
    return None


def _signature(grant, *, project_id, sha, node_id, number, policy):
    payload = json.dumps(["noyaru-automation-proposal-v1", str(project_id), grant.mode, grant.id,
                          str(sha), str(node_id), str(number), policy], separators=(",", ":"))
    return hmac.new(grant.signing_secret.encode(), payload.encode(), hashlib.sha256).hexdigest()


def _policy(verification):
    return digest({"fusion_auto": bool(verification.get("fusion_auto")),
                   "controle_sitemap": verification.get("controle_sitemap")})


def seal(project, pr_data, verification):
    info = binding(project)
    if not info:
        return None
    project_id = current().project_id if current() else str(project.id)
    with module().DB.session() as db:
        grant = check(db, project_id=project_id, mode=info["mode"], mandate_key=info["mandate_key"])
        number = str(pr_data.get("number") or "")
        signature = _signature(grant, project_id=project_id,
            sha=(pr_data.get("head") or {}).get("sha", ""), node_id=pr_data.get("node_id", ""), number=number, policy=_policy(verification))
    return {**info, "number": number, "signature": signature}


def verify(db, *, project_id, verification, number):
    ExecutionStopped = module().correction_execution.ExecutionStopped
    info = verification.get("automation") or {}
    grant = check(db, project_id=project_id, mode=info.get("mode", "github_merge"), mandate_key=info.get("mandate_key"))
    signature = _signature(grant, project_id=project_id, sha=verification.get("sha", ""),
                           node_id=verification.get("node_id", ""), number=str(number), policy=_policy(verification))
    if (grant.mode == "content_schedule" and verification.get("fusion_auto")
            and grant.config.get("content", {}).get("publication") != "auto"):
        raise ExecutionStopped("automation_target_changed")
    if not hmac.compare_digest(signature, str(info.get("signature") or "")):
        raise ExecutionStopped("automation_target_changed")
    return grant


def reserve(db, *, project, grant, request_key, data):
    """Reserve before provider calls or writes, under the caller's project/job lock."""
    if db.scalar(select(AutomationRun.id).where(AutomationRun.project_id == project.id,
            AutomationRun.mode == grant.mode, AutomationRun.request_key == request_key)):
        return None
    run = AutomationRun(id=str(uuid4()), project_id=project.id, mode=grant.mode,
        mandate_key=grant.id, request_key=request_key, state="running", data=data,
        provider_calls=0, writes=0)
    db.add(run)
    db.flush()
    return run


def claim(*, project_id, mode, mandate_key, request_key, data):
    ExecutionStopped = module().correction_execution.ExecutionStopped
    m = module()
    try:
        with m._automation_settings_transaction(project_id) as (db, project):
            grant = check(db, project_id=project_id, mode=mode, mandate_key=mandate_key)
            run = reserve(db, project=project, grant=grant, request_key=request_key, data=data)
            if run is None:
                raise ExecutionStopped("automation_already_started")
            return run.id
    except IntegrityError:
        raise ExecutionStopped("automation_already_started") from None


@dataclass
class Fence:
    project_id: str
    mode: str
    mandate_key: str
    run_id: str | None
    request_key: str | None = None
    data: dict | None = None
    reason: str = ""


def current():
    value = _CURRENT.get()
    return value[1] if value and value[0] == os.getpid() else None


@contextmanager
def fenced(*, project_id, mode, mandate_key, run_id=None, request_key=None, data=None):
    value = Fence(str(project_id), mode, mandate_key, run_id, request_key, data)
    token = _CURRENT.set((os.getpid(), value))
    try:
        transport()
        yield value
    finally:
        _CURRENT.reset(token)


def transport(path=None, method=None, *, provider=False, graphql=False):
    """Recheck at the actual transport boundary; never hold a DB lock during HTTP."""
    entry = current()
    if entry is None:
        return
    ExecutionStopped = module().correction_execution.ExecutionStopped
    try:
        with module().DB.session() as db:
            if db.bind.dialect.name == "sqlite":
                db.connection().exec_driver_sql("BEGIN IMMEDIATE")
            grant = check(db, project_id=entry.project_id, mode=entry.mode, mandate_key=entry.mandate_key)
            prefix = "/repos/" + grant.config["repo"] + "/"
            if path and not graphql and not path.casefold().startswith(prefix.casefold()):
                raise ExecutionStopped("automation_target_changed")
            mutating = provider or (method and method != "GET")
            if mutating and not entry.run_id:
                project = db.get(Project, entry.project_id)
                run = reserve(db, project=project, grant=grant, request_key=entry.request_key, data=entry.data or {})
                if run is None:
                    raise ExecutionStopped("automation_already_started")
                entry.run_id = run.id
            if entry.run_id:
                run = db.scalar(select(AutomationRun).where(AutomationRun.id == entry.run_id).with_for_update())
                if not run or run.state != "running" or run.mandate_key != grant.id:
                    raise ExecutionStopped("automation_already_started")
                if provider:
                    run.provider_calls += 1
                elif method and method != "GET":
                    run.writes += 1
            db.commit()
    except ExecutionStopped as error:
        entry.reason = error.reason
        raise


def observe_pr(pr_data, *, branch="", task_id=None):
    entry = current()
    if entry is None or not entry.run_id:
        return
    with module().DB.session() as db:
        run = db.get(AutomationRun, entry.run_id)
        if run and run.project_id == entry.project_id:
            run.data = {**run.data, "pr_number": pr_data.get("number"),
                        "pr_url": cms_consent.public_url(pr_data.get("html_url")),
                        "branch": branch, **({"task_id": str(task_id)} if task_id else {})}
            db.commit()


def finish(run_id, *, ok=False, reason=""):
    with module().DB.session() as db:
        run = db.get(AutomationRun, run_id)
        if not run or run.state != "running":
            return
        run.state = "completed" if ok else "uncertain" if run.writes or run.provider_calls else "stopped"
        run.reason = str(reason or "")[:48] or None
        run.finished_at = datetime.now(timezone.utc)
        db.commit()


def verify_remote_pr(*, owner, repo, number, token, branch, verification):
    """Read the actual destination before mutation; the merge SHA alone does not bind its base."""
    m = module()
    data = m._github_api_get(m._github_api_path("repos", owner, repo, "pulls", str(number)), token=token)
    base = data.get("base") or {} if isinstance(data, dict) else {}
    head = data.get("head") or {} if isinstance(data, dict) else {}
    if (not isinstance(data, dict) or data.get("state") != "open"
            or str(data.get("number")) != str(number)
            or data.get("node_id") != verification.get("node_id")
            or head.get("sha") != verification.get("sha")
            or base.get("ref") != branch
            or str((base.get("repo") or {}).get("full_name") or "").casefold() != (owner + "/" + repo).casefold()):
        error = m.correction_execution.ExecutionStopped("automation_target_changed")
        if entry := current():
            entry.reason = error.reason
        raise error
