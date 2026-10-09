"""Operator-only abandonment of pending work, without changing GitHub or the ledger."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from urllib.parse import quote

import requests
from sqlalchemy import select

from .correction_guard import CorrectionStoreUnavailable, correction_lock, ensure_active
from .correction_journal import Operation
from .models import CorrectionOperation


class ReconciliationRefused(Exception):
    pass


class GitHubReader:
    def __init__(self, token):
        self.token = token

    def get(self, path, *, params=None, missing=False):
        ensure_active()
        try:
            response = requests.get(
                "https://api.github.com" + path,
                headers={"Authorization": "Bearer " + self.token,
                         "Accept": "application/vnd.github+json",
                         "X-GitHub-Api-Version": "2022-11-28",
                         "User-Agent": "seo-agent-reconciliation"},
                params=params, timeout=30, allow_redirects=False,
            )
            ensure_active()
            if missing and response.status_code == 404:
                return None
            if response.status_code != 200:
                raise CorrectionStoreUnavailable()
            return response.json()
        except Exception as exc:
            raise CorrectionStoreUnavailable() from exc


def _path(*parts):
    return "/" + "/".join(quote(part, safe="") for part in parts)


def _load(database, operation_id):
    try:
        with database.session() as db:
            row = db.get(CorrectionOperation, operation_id)
            if row is None:
                raise ReconciliationRefused("operation_not_found")
            return row
    except ReconciliationRefused:
        raise
    except Exception as exc:
        raise CorrectionStoreUnavailable() from exc


def summary(row):
    data = row.data or {}
    return {"operation_id": row.id, "payer_id": row.payer_id, "project_id": row.project_id,
            "action": row.action, "state": row.state, "charged": row.charged,
            "branch": data.get("branch"), "repository": data.get("repository"),
            "saved_preview": bool(data.get("preview")), "saved_pr": bool(data.get("pr"))}


def list_pending(database, *, payer=None):
    try:
        with database.session() as db:
            query = select(CorrectionOperation).where(CorrectionOperation.state.in_(["running", "blocked"]))
            if payer:
                query = query.where(CorrectionOperation.payer_id == payer)
            return [summary(row) for row in db.scalars(query.order_by(CorrectionOperation.created_at)).all()]
    except Exception as exc:
        raise CorrectionStoreUnavailable() from exc


def _inspect(row, reader_factory):
    data = row.data or {}
    result = {**summary(row), "decision": "manual_review", "reason": "missing_repository_snapshot"}
    if row.state not in {"running", "blocked"}:
        return {**result, "reason": "operation_not_pending"}
    if row.charged or data.get("preview") or data.get("pr"):
        return {**result, "reason": "saved_result_requires_recovery"}
    if not data.get("writes_started") and not any(data.get(key) for key in ("branch", "pr_intent")):
        return {**result, "decision": "can_abandon", "reason": "no_external_write_intent"}
    repo = data.get("repository")
    branch = data.get("branch")
    if (not isinstance(repo, dict)
            or not all(isinstance(repo.get(key), str) and repo[key] for key in ("owner", "repo", "base", "base_sha"))
            or not re.fullmatch(r"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}", repo["base_sha"])
            or not isinstance(branch, str) or not branch.startswith(("seo-fix/", "seo-keyword/"))
            or not branch.endswith("-" + row.id[:8]) or branch == repo["base"]):
        return result
    intent = data.get("pr_intent")
    if intent and (not isinstance(intent, dict) or any(intent.get(key) != repo[key] for key in ("owner", "repo", "base"))):
        return {**result, "reason": "repository_intent_mismatch"}
    reader = reader_factory(row.payer_id)
    prefix = _path("repos", repo["owner"], repo["repo"])
    metadata = reader.get(prefix)
    if (not isinstance(metadata, dict)
            or str(metadata.get("full_name", "")).casefold() != (repo["owner"] + "/" + repo["repo"]).casefold()):
        raise CorrectionStoreUnavailable()
    # No base filter: a PR targeting another branch must also prevent abandonment.
    pulls = reader.get(prefix + "/pulls", params={
        "state": "all", "head": repo["owner"] + ":" + branch, "per_page": 100, "page": 1})
    if not isinstance(pulls, list):
        raise CorrectionStoreUnavailable()
    if pulls:
        return {**result, "reason": "pull_request_exists", "pull_request_count_at_least": len(pulls)}
    # Repository metadata alone does not prove contents access: GitHub can mask it with 404.
    commit = reader.get(prefix + "/git/commits/" + quote(repo["base_sha"], safe=""))
    if not isinstance(commit, dict) or str(commit.get("sha", "")).casefold() != repo["base_sha"].casefold():
        raise CorrectionStoreUnavailable()
    ref = reader.get(prefix + "/git/ref/heads/" + "/".join(quote(part, safe="") for part in branch.split("/")),
                     missing=True)
    if ref is None:
        return {**result, "decision": "can_abandon", "reason": "branch_absent", "head_sha": None}
    obj = ref.get("object") if isinstance(ref, dict) else None
    if (not isinstance(obj, dict) or obj.get("type") != "commit"
            or ref.get("ref") != "refs/heads/" + branch
            or not isinstance(obj.get("sha"), str)
            or not re.fullmatch(r"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}", obj["sha"])):
        raise CorrectionStoreUnavailable()
    if obj["sha"].casefold() == repo["base_sha"].casefold():
        return {**result, "decision": "can_abandon", "reason": "unchanged_branch", "head_sha": obj["sha"]}
    return {**result, "decision": "retain_branch_required", "reason": "unpublished_changes", "head_sha": obj["sha"]}


def reconcile(database, operation_id, reader_factory, *, abandon=False, retain_branch=False,
              operator="", reason=""):
    if abandon and (not operator.strip() or not reason.strip()):
        raise ReconciliationRefused("operator_and_reason_required")
    row = _load(database, operation_id)
    with correction_lock(database, row.payer_id):
        fresh = _load(database, operation_id)
        if fresh.payer_id != row.payer_id:
            raise CorrectionStoreUnavailable()
        try:
            result = _inspect(fresh, reader_factory)
        except Exception as exc:
            raise CorrectionStoreUnavailable() from exc
        ensure_active()
        if not abandon:
            return result
        allowed = result["decision"] == "can_abandon" or (retain_branch and result["decision"] == "retain_branch_required")
        if not allowed:
            raise ReconciliationRefused(result["reason"])
        # Retain all remote commits and all billing records. Only release this request's key.
        audit = {"at": datetime.now(timezone.utc).isoformat(), "operator": operator.strip(),
                 "reason": reason.strip(), "evidence": result,
                 "remote_branch_retained": result.get("head_sha") is not None}
        Operation(database, fresh).save(state="abandoned", release=True, reconciliation=audit)
        return {**result, "state": "abandoned", "released": True}
