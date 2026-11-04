"""Publish one approved preview, then verify one observed deployment and control.

The mandate never grants free-form tools, model calls or automatic rollback.
Every external write has a durable intent. Unknown writes are read back, not replayed.
"""

import base64
import hashlib
from datetime import timedelta
import re
import threading
from urllib.parse import urlsplit

from sqlalchemy import select, or_
from starlette.requests import Request

try:
    from . import (
        autocorrection as preparation,
        correction_execution as execution,
        correction_guard,
        correction_journal,
    )
    from . import (
        correction_memory as memory,
        first_correction,
        publication_control as control,
    )
    from .models import (
        AuditLog,
        AutocorrectionRun,
        CorrectionObservation,
        CorrectionOperation,
        CorrectionPublication,
        IssueTask,
        JobRecord,
        Project,
        User,
    )
except ImportError:
    import autocorrection as preparation
    import correction_execution as execution
    import correction_guard
    import correction_journal
    import correction_memory as memory
    import first_correction
    import publication_control as control
    from models import (
        AuditLog,
        AutocorrectionRun,
        CorrectionObservation,
        CorrectionOperation,
        CorrectionPublication,
        IssueTask,
        JobRecord,
        Project,
        User,
    )

VERSION = 1
MAX_READS = 240
MAX_WRITES = 5
TERMINAL = {"verified", "review_required", "revoked", "expired"}
READY_MUTATION = "mutation($id:ID!){markPullRequestReadyForReview(input:{pullRequestId:$id}){pullRequest{isDraft}}}"
_STARTED = False
_GUARD = threading.Lock()


def sha(value):
    return isinstance(value, str) and bool(re.fullmatch("[a-f0-9]{40}", value))


def preview_key(preview):
    return preparation.decisions.digest(
        [preview.get("file"), preview.get("patched_content")]
    )


def _audit(db, pub, action):
    db.add(
        AuditLog(
            actor_user_id=pub.owner_id,
            action="publication." + action,
            target_type="correction_publication",
            target_id=pub.id,
            meta={
                "project_id": pub.project_id,
                "task_id": pub.task_id,
                "run_id": pub.run_id,
                "version": pub.version,
                "phase": pub.phase,
                "reason": pub.reason,
                "pages_budget": pub.pages_budget,
                "job_id": pub.job_id,
                "reads": pub.reads,
                "writes": pub.writes,
                "lang": pub.lang,
            },
        )
    )


def authorize(
    project_id,
    run_id,
    owner_id,
    approved_key,
    *,
    approved,
    consent,
    pages_budget,
    environment,
    lang,
):
    m = preparation.module()
    if not approved or not consent:
        raise execution.ExecutionStopped("consent_required")
    if type(pages_budget) is not int or not 1 <= pages_budget <= 300:
        raise execution.ExecutionStopped("pages_budget")
    if not re.fullmatch(r"[A-Za-z0-9_. -]{1,64}", str(environment)):
        raise execution.ExecutionStopped("environment")
    with correction_guard.correction_lock(m.DB, str(owner_id)):
        with m.DB.session() as db:
            project = db.get(Project, project_id)
            run = db.get(AutocorrectionRun, run_id)
            if not project or project.owner_user_id != owner_id:
                raise execution.ExecutionStopped("owner_required")
            if not run or run.project_id != project_id or run.owner_id != owner_id:
                raise execution.ExecutionStopped("task_missing")
            if existing := db.scalar(
                select(CorrectionPublication).where(
                    CorrectionPublication.run_id == run_id
                )
            ):
                return existing
            task = db.get(IssueTask, run.task_id)
            preview, _ = preparation.saved_preview(db, run)
            if (
                not task
                or not preview
                or not preparation.draft_current(db, run, project, task)
                or preview_key(preview) != approved_key
                or not m._github_file_path_allowed(preview.get("file", ""))
            ):
                raise execution.ExecutionStopped("application_changed")
            reason, _ = preparation.eligibility(db, project, task)
            if reason:
                raise execution.ExecutionStopped(reason)
            now = preparation.clock()
            pub = CorrectionPublication(
                run_id=run.id,
                project_id=project.id,
                task_id=task.id,
                owner_id=owner_id,
                application_key=memory.attempt_key(task),
                config_key=preparation.config_key(project),
                preview_key=approved_key,
                analysis_key=control.analysis_key(project),
                version=VERSION,
                lang="en" if str(lang).startswith("en") else "fr",
                environment=environment,
                pages_budget=pages_budget,
                reserved_pages=0,
                control_settled=False,
                job_issued=False,
                reads=0,
                writes=0,
                phase="queued",
                reason="authorized",
                receipt={},
                expires_at=now + timedelta(hours=24),
                next_check_at=now,
            )
            db.add(pub)
            db.flush()
            _audit(db, pub, "authorized")
            db.commit()
            return pub


def _scope(db, pub, *, allow_expired=False):
    if not pub or pub.version != VERSION:
        raise execution.ExecutionStopped("policy_changed")
    if not allow_expired and pub.revoked_at:
        raise execution.ExecutionStopped("revoked")
    if not allow_expired and preparation.clock() >= first_correction.timestamp(
        pub.expires_at
    ):
        raise execution.ExecutionStopped("expired")
    project = db.get(Project, pub.project_id)
    task = db.get(IssueTask, pub.task_id)
    run = db.get(AutocorrectionRun, pub.run_id)
    if (
        not project
        or not task
        or not run
        or project.owner_user_id != pub.owner_id
        or task.project_id != project.id
    ):
        raise execution.ExecutionStopped("owner_required")
    if preparation.paused() or preparation.project_paused(project):
        raise execution.ExecutionStopped("paused")
    if (
        preparation.config_key(project) != pub.config_key
        or control.analysis_key(project) != pub.analysis_key
    ):
        raise execution.ExecutionStopped("target_changed")
    preview, _ = preparation.saved_preview(db, run)
    if not preview or preview_key(preview) != pub.preview_key:
        raise execution.ExecutionStopped("preview_changed")
    if pub.phase in {"queued", "creating_pr"} and memory.attempt_key(task) == pub.application_key:
        reason, _ = preparation.eligibility(db, project, task)
        if reason:
            raise execution.ExecutionStopped(reason)
    expected = pub.applied_key or pub.receipt.get("proposal_key") or pub.application_key
    if (
        task.status not in {"in_progress", "done"}
        or memory.attempt_key(task) != expected
    ):
        # Reconcile a lost response after the guarded confirmation saved its new receipt.
        note = first_correction.note_data(task.note)
        op = db.get(CorrectionOperation, pub.operation_id) if pub.operation_id else None
        pr = op.data.get("pr", {}) if op else {}
        if (
            pub.phase != "creating_pr"
            or not op
            or not pr.get("number")
            or (note.get("verification") or {}).get("publication_id") != pub.id
            or note.get("pr_number") != pr["number"]
        ):
            raise execution.ExecutionStopped("application_changed")
    return project, task, run, preview


class Policy:
    def __init__(self, pub_id, run_id):
        self.publication_id = pub_id
        self.run_id = run_id
        self.purpose = "publication:" + pub_id

    def _reserve(self, *, write=False):
        m = preparation.module()
        correction_guard.ensure_active()
        with m.DB.session() as db:
            pub = db.get(CorrectionPublication, self.publication_id)
            scope = _scope(db, pub)
            if (write and pub.writes >= MAX_WRITES) or (
                not write and pub.reads >= MAX_READS
            ):
                raise execution.ExecutionStopped("request_budget")
            if write:
                pub.writes += 1
            else:
                pub.reads += 1
            db.commit()
            return pub, scope

    def bind_operation(self, operation):
        m = preparation.module()
        correction_guard.ensure_active()
        with m.DB.session() as db:
            pub = db.get(CorrectionPublication, self.publication_id)
            project, _, _, _ = _scope(db, pub)
            row = operation.row
            if (
                row.project_id != project.id
                or row.payer_id != pub.owner_id
                or row.action != "api_github_fix"
            ):
                raise execution.ExecutionStopped("operation_mismatch")
            if pub.operation_id not in {None, row.id}:
                raise execution.ExecutionStopped("operation_mismatch")
            pub.operation_id = row.id
            db.commit()

    def preview_source_matches(self, *, project, file, patched, original):
        m = preparation.module()
        with m.DB.session() as db:
            pub = db.get(CorrectionPublication, self.publication_id)
            target, _, run, preview = _scope(db, pub)
            source = db.get(CorrectionOperation, run.operation_id) if run.operation_id else None
            return bool(source and str(project) == target.id
                and file == preview['file'] and patched == preview['patched_content']
                and hashlib.sha256(original.encode()).hexdigest() == source.data.get('preview_source'))

    def safe_to_abandon(self):
        return True  # No AI permission; the shared journal still checks write intents.

    def provider_request(self, **kwargs):
        raise execution.ExecutionStopped("ai_not_authorized")

    def github_read(self, path):
        pub, (project, _, _, _) = self._reserve()
        m = preparation.module()
        parts = m._github_repo_parts(m._project_github_cfg(project)["repo"])
        if not path.startswith(m._github_api_path("repos", *parts) + "/"):
            raise execution.ExecutionStopped("target_changed")

    def github_write_intent(self, path, method, body):
        m = preparation.module()
        with m.DB.session() as db:
            pub = db.get(CorrectionPublication, self.publication_id)
            project, task, _, preview = _scope(db, pub)
            prefix = m._github_api_path(
                "repos", *m._github_repo_parts(m._project_github_cfg(project)["repo"])
            )
            allowed = False
            if (
                pub.phase == "creating_pr"
                and memory.attempt_key(task) == pub.application_key
            ):
                op = (
                    db.get(CorrectionOperation, pub.operation_id)
                    if pub.operation_id
                    else None
                )
                data = op.data if op else {}
                branch = data.get("branch")
                repo = data.get("repository") or {}
                if method == "POST" and path == prefix + "/git/refs":
                    allowed = bool(
                        branch
                        and body
                        == {"ref": "refs/heads/" + branch, "sha": repo.get("base_sha")}
                    )
                elif method == "PUT" and path == m._github_content_api_path(
                    *m._github_repo_parts(m._project_github_cfg(project)["repo"]),
                    preview["file"],
                ):
                    try:
                        content = base64.b64decode(
                            body.get("content", ""), validate=True
                        ).decode("utf-8")
                    except (ValueError, UnicodeError):
                        content = ""
                    allowed = bool(
                        branch
                        and body.get("branch") == branch
                        and _text_key(content) == _text_key(preview["patched_content"])
                    )
                elif method == "POST" and path == prefix + "/pulls":
                    allowed = bool(
                        branch
                        and body.get("head") == branch
                        and body.get("base") == m._project_github_cfg(project)["branch"]
                        and body.get("draft") is True
                    )
            elif (
                pub.phase == "merging"
                and method == "PUT"
                and path
                == prefix + "/pulls/" + str(pub.receipt.get("number")) + "/merge"
            ):
                allowed = body == {
                    "sha": pub.receipt.get("head"),
                    "merge_method": "squash",
                } and sha(body.get("sha"))
            if not allowed:
                raise execution.ExecutionStopped("write_not_authorized")
        self._reserve(write=True)

    def github_graphql(self, query, variables):
        m = preparation.module()
        with m.DB.session() as db:
            pub = db.get(CorrectionPublication, self.publication_id)
            _scope(db, pub)
            if (
                pub.phase != "marking_ready"
                or query != READY_MUTATION
                or variables != {"id": pub.receipt.get("node_id")}
            ):
                raise execution.ExecutionStopped("write_not_authorized")
        self._reserve(write=True)


def _text_key(value):
    return preparation.decisions.digest(str(value).replace("\r\n", "\n"))


def _update(pub_id, phase, reason, **receipt):
    m = preparation.module()
    with m.DB.session() as db:
        pub = db.get(CorrectionPublication, pub_id)
        if not pub:
            return
        pub.phase = phase
        pub.reason = reason
        pub.receipt = {**pub.receipt, **receipt}
        pub.next_check_at = preparation.clock() + timedelta(seconds=60)
        _audit(db, pub, "state")
        db.commit()


def _remote(pub):
    m = preparation.module()
    with m.DB.session() as db:
        project, _, _, _ = _scope(db, db.get(CorrectionPublication, pub.id))
    owner, repo = m._github_repo_parts(m._project_github_cfg(project)["repo"])
    token, source = m._effective_user_connection_value(
        user_id=pub.owner_id, key="GITHUB_TOKEN"
    )
    if not token or source != "user":
        raise execution.ExecutionStopped("github_required")
    return m, project, owner, repo, token


def _protection(m, owner, repo, branch, token):
    value = m._github_api_get(
        m._github_api_path("repos", owner, repo, "branches", branch, "protection"),
        token=token,
    )
    checks = value.get("required_status_checks") if isinstance(value, dict) else None
    if (
        not isinstance(checks, dict)
        or checks.get("strict") is not True
        or (value.get("enforce_admins") or {}).get("enabled") is not True
    ):
        raise execution.ExecutionStopped("branch_protection")
    names = set(checks.get("contexts") or [])
    names.update(
        row["context"]
        for row in checks.get("checks", [])
        if isinstance(row, dict) and isinstance(row.get("context"), str)
    )
    if not names or any(not isinstance(name, str) or not name for name in names):
        raise execution.ExecutionStopped("branch_protection")
    return names


def _checks(m, owner, repo, head, token, required):
    runs = m._github_api_get(
        m._github_api_path("repos", owner, repo, "commits", head, "check-runs"),
        token=token,
        params={"per_page": 100, "filter": "latest"},
    )
    statuses = m._github_api_get(
        m._github_api_path("repos", owner, repo, "commits", head, "status"),
        token=token,
        params={"per_page": 100},
    )
    if (
        not isinstance(runs, dict)
        or not isinstance(runs.get("check_runs"), list)
        or type(runs.get("total_count")) is not int
        or runs["total_count"] != len(runs["check_runs"])
        or not isinstance(statuses, dict)
        or not isinstance(statuses.get("statuses"), list)
        or type(statuses.get("total_count")) is not int
        or statuses["total_count"] != len(statuses["statuses"])
    ):
        raise execution.ExecutionStopped("checks_incomplete")
    success = set()
    pending = False
    for row in runs["check_runs"]:
        if row.get("head_sha") != head:
            raise execution.ExecutionStopped("checks_incomplete")
        if row.get("status") != "completed":
            pending = True
        elif row.get("conclusion") == "success":
            success.add(row.get("name"))
        elif row.get("conclusion") not in {"neutral", "skipped"}:
            raise execution.ExecutionStopped("checks_failed")
    for row in statuses["statuses"]:
        if row.get("state") == "success":
            success.add(row.get("context"))
        elif row.get("state") == "pending":
            pending = True
        else:
            raise execution.ExecutionStopped("checks_failed")
    return not pending and required <= success


def _pr(m, pub, owner, repo, token):
    number = pub.receipt.get("number")
    if type(number) is not int or number <= 0:
        raise execution.ExecutionStopped("pr_unconfirmed")
    value = m._github_api_get(
        m._github_api_path("repos", owner, repo, "pulls", str(number)), token=token
    )
    head, base = value.get("head") or {}, value.get("base") or {}
    expected_repo = (owner + "/" + repo).casefold()
    if (
        head.get("sha") != pub.receipt.get("head")
        or head.get("ref") != pub.receipt.get("branch")
        or base.get("ref") != pub.receipt.get("base_branch")
        or any(
            str((part.get("repo") or {}).get("full_name", "")).casefold()
            != expected_repo
            for part in (head, base)
        )
    ):
        raise execution.ExecutionStopped("pr_changed")
    return value


def _validate_code(m, pub, project, owner, repo, token, value):
    with m.DB.session() as db:
        _, _, _, preview = _scope(db, db.get(CorrectionPublication, pub.id))
    if value.get("changed_files") != 1 or value.get("commits") != 1:
        raise execution.ExecutionStopped("pr_changed")
    files = m._github_api_get(
        m._github_api_path(
            "repos", owner, repo, "pulls", str(pub.receipt["number"]), "files"
        ),
        token=token,
        params={"per_page": 2},
    )
    if (
        not isinstance(files, list)
        or len(files) != 1
        or files[0].get("filename") != preview["file"]
        or files[0].get("status") != "modified"
    ):
        raise execution.ExecutionStopped("pr_changed")
    data = m._github_api_get(
        m._github_content_api_path(owner, repo, preview["file"]),
        token=token,
        params={"ref": pub.receipt["head"]},
    )
    try:
        content = base64.b64decode(
            "".join(data["content"].split()), validate=True
        ).decode("utf-8")
    except (KeyError, ValueError, UnicodeError):
        raise execution.ExecutionStopped("pr_changed")
    if _text_key(content) != _text_key(preview["patched_content"]):
        raise execution.ExecutionStopped("pr_changed")
    ref = m._github_api_get(
        m._github_ref_api_path(owner, repo, m._project_github_cfg(project)["branch"]),
        token=token,
    )
    if (value.get("base") or {}).get("sha") != pub.receipt.get("base") or (
        ref.get("object") or {}
    ).get("sha") != pub.receipt.get("base"):
        raise execution.ExecutionStopped("base_changed")


def _create(pub):
    m, project, owner, repo, token = _remote(pub)
    _protection(m, owner, repo, m._project_github_cfg(project)["branch"], token)
    with m.DB.session() as db:
        fresh = db.get(CorrectionPublication, pub.id)
        _, task, _, preview = _scope(db, fresh)
        user = db.get(User, pub.owner_id)
    _update(pub.id, "creating_pr", "creating_pr")
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/",
            "headers": [],
            "scheme": "https",
            "server": ("noyaru.internal", 443),
            "query_string": b"",
        }
    )
    request.state.user = user
    response = m.api_github_fix(
        request,
        project.slug,
        task.issue_key,
        m._GithubFixBody(
            url=task.url,
            crawl_ts=task.crawl_ts,
            confirm=True,
            file_path=preview["file"],
            patched_content=preview["patched_content"],
        ),
    )
    if response.status_code == 409:
        raise execution.ExecutionStopped("source_changed")
    # An HTTP error may conceal a created PR. Recover only its durable intent.
    _capture(pub.id)


def _capture(pub_id):
    m = preparation.module()
    with m.DB.session() as db:
        pub = db.get(CorrectionPublication, pub_id)
        project, task, _, _ = _scope(db, pub)
        op = db.get(CorrectionOperation, pub.operation_id) if pub.operation_id else None
    if not op or not op.data.get("pr_intent"):
        raise execution.ExecutionStopped("pr_unconfirmed")
    if op.state != "completed":
        with m.DB.session() as db:
            user = db.get(User, pub.owner_id)
        with correction_journal.operation(
            m.DB,
            payer=pub.owner_id,
            project=pub.project_id,
            action=op.action,
            key=op.request_key,
            row=op,
        ) as receipt:
            response = m._recover_correction(receipt, user=user, slug=project.slug)
        if response.status_code != 200:
            raise execution.ExecutionStopped("pr_unconfirmed")
        op = receipt.row
    pr = op.data.get("pr") or {}
    remote = op.data.get("repository") or {}
    if (
        not sha((pr.get("head") or {}).get("sha"))
        or not sha(remote.get("base_sha"))
        or not isinstance(pr.get("node_id"), str)
        or not 1 <= len(pr["node_id"]) <= 240
    ):
        raise execution.ExecutionStopped("pr_unconfirmed")
    with m.DB.session() as db:
        task = db.get(IssueTask, pub.task_id)
    _update(
        pub.id,
        "waiting_checks",
        "waiting_checks",
        number=pr["number"],
        head=pr["head"]["sha"],
        node_id=pr["node_id"],
        base=remote["base_sha"],
        base_branch=remote["base"],
        repository=remote["owner"]+"/"+remote["repo"],
        branch=op.data["branch"],
        proposal_key=memory.attempt_key(task),
    )


def _record_merge(pub, value):
    merge_sha = value.get("merge_commit_sha") or value.get("sha")
    if value.get("merged") is not True or not sha(merge_sha):
        raise execution.ExecutionStopped("merge_unconfirmed")
    _update(
        pub.id,
        "waiting_deployment",
        "waiting_deployment",
        merge_sha=merge_sha,
        merged_at=value.get("merged_at")
        or pub.receipt.get("merged_at")
        or preparation.clock().isoformat(),
    )


def _check_and_merge(pub):
    m, project, owner, repo, token = _remote(pub)
    required = _protection(
        m, owner, repo, m._project_github_cfg(project)["branch"], token
    )
    value = _pr(m, pub, owner, repo, token)
    if value.get("merged") is True:
        _record_merge(pub, value)
        return
    if value.get("state") != "open":
        raise execution.ExecutionStopped("pr_closed")
    _validate_code(m, pub, project, owner, repo, token, value)
    if value.get("draft") is True:
        _update(pub.id, "marking_ready", "marking_ready")
        m._pr_marquer_prete(node_id=pub.receipt["node_id"], token=token)
        _update(pub.id, "waiting_checks", "waiting_checks")
        return
    if not _checks(m, owner, repo, pub.receipt["head"], token, required):
        _update(pub.id, "waiting_checks", "waiting_checks")
        return
    if value.get("mergeable") is not True or value.get("mergeable_state") != "clean":
        _update(pub.id, "waiting_checks", "waiting_github")
        return
    # Strict server protection closes the base-update race; the exact reviewed head is pinned.
    latest = _pr(m, pub, owner, repo, token)
    if (latest.get("base") or {}).get("sha") != pub.receipt["base"] or latest.get(
        "draft"
    ) is not False:
        raise execution.ExecutionStopped("pr_changed")
    _update(pub.id, "merging", "merging")
    merged = m._github_api_put(
        m._github_api_path(
            "repos", owner, repo, "pulls", str(pub.receipt["number"]), "merge"
        ),
        token=token,
        json_body={"sha": pub.receipt["head"], "merge_method": "squash"},
    )
    _record_merge(pub, merged)


def _deployment(pub):
    m, project, owner, repo, token = _remote(pub)
    cfg = m._project_github_cfg(project)
    ref = m._github_api_get(
        m._github_ref_api_path(owner, repo, cfg["branch"]), token=token
    )
    if (ref.get("object") or {}).get("sha") != pub.receipt["merge_sha"]:
        raise execution.ExecutionStopped("deployment_superseded")
    rows = m._github_api_get(
        m._github_api_path("repos", owner, repo, "deployments"),
        token=token,
        params={
            "sha": pub.receipt["merge_sha"],
            "environment": pub.environment,
            "per_page": 100,
        },
    )
    if not isinstance(rows, list) or len(rows) >= 100:
        raise execution.ExecutionStopped("deployment_incomplete")
    expected = urlsplit(project.base_url)
    for row in sorted(rows, key=lambda r: int(r.get("id") or 0), reverse=True):
        if (
            row.get("sha") != pub.receipt["merge_sha"]
            or row.get("environment") != pub.environment
            or row.get("production_environment") is not True
            or row.get("transient_environment") is not False
            or type(row.get("id")) is not int
        ):
            continue
        statuses = m._github_api_get(
            m._github_api_path(
                "repos", owner, repo, "deployments", str(row["id"]), "statuses"
            ),
            token=token,
            params={"per_page": 100},
        )
        if not isinstance(statuses, list) or len(statuses) >= 100:
            raise execution.ExecutionStopped("deployment_incomplete")
        latest = max(statuses, key=lambda r: int(r.get("id") or 0), default={})
        if latest.get("state") in {"failure", "error", "inactive"}:
            raise execution.ExecutionStopped("deployment_failed")
        if latest.get("state") != "success":
            _update(pub.id, "waiting_deployment", "waiting_deployment")
            return
        url = urlsplit(str(latest.get("environment_url") or ""))
        when = first_correction.timestamp(latest.get("created_at"))
        if (url.scheme, url.hostname, url.port) != (
            expected.scheme,
            expected.hostname,
            expected.port,
        ):
            raise execution.ExecutionStopped("deployment_target")
        if (
            not when
            or when
            < first_correction.timestamp(pub.receipt["merged_at"])
            - timedelta(minutes=5)
            or when > preparation.clock() + timedelta(minutes=5)
        ):
            raise execution.ExecutionStopped("deployment_incomplete")
        _update(
            pub.id,
            "deployment_confirmed",
            "deployment_confirmed",
            deployment_id=row["id"],
            deployed_at=when.isoformat(),
        )
        return
    _update(pub.id, "waiting_deployment", "waiting_deployment")


def _control_result(pub):
    m = preparation.module()
    with m.DB.session() as db:
        row = db.get(JobRecord, pub.job_id) if pub.job_id else None
        project = db.get(Project, pub.project_id)
        if (
            not row
            or not project
            or row.slug != project.slug
            or (row.result or {}).get("publication_control") != pub.id
            or row.owner_user_id != pub.owner_id
            or row.kind != "crawl"
        ):
            raise execution.ExecutionStopped("control_missing")
        if row.status in {"queued", "running", "cancel_requested"}:
            _update(pub.id, "waiting_control", "waiting_control")
            return
        if row.status != "done":
            raise execution.ExecutionStopped("control_failed")
        control_ts = (row.result or {}).get("timestamp")
        when = first_correction.timestamp(control_ts)
        deployed = first_correction.timestamp(pub.receipt.get("deployed_at"))
        if not when or not deployed or when <= deployed:
            raise execution.ExecutionStopped("control_inconclusive")
        observed = db.scalar(
            select(CorrectionObservation).where(
                CorrectionObservation.project_id == pub.project_id,
                CorrectionObservation.task_id == pub.task_id,
                CorrectionObservation.attempt_key == pub.applied_key,
                CorrectionObservation.control_ts
                == (
                    first_correction.timestamp(control_ts).isoformat()
                    if first_correction.timestamp(control_ts)
                    else ""
                ),
            )
        )
        if not observed:
            if (
                row.finished_at
                and preparation.clock().timestamp() - row.finished_at < 60
            ):
                _update(pub.id, "waiting_control", "waiting_control")
                return
            raise execution.ExecutionStopped("control_inconclusive")
        if observed.result != "resolved" or not observed.baseline_confirmed:
            raise execution.ExecutionStopped(
                "defect_present"
                if observed.result == "still_present"
                else "control_inconclusive"
            )
        fresh = db.get(CorrectionPublication, pub.id)
        fresh.observation_id = observed.id
        db.commit()
    _update(pub.id, "verified", "verified")


def step(pub_id, *, force=False):
    m = preparation.module()
    with m.DB.session() as db:
        pub = db.get(CorrectionPublication, str(pub_id))
    if not pub or pub.phase in TERMINAL:
        return pub.phase if pub else "missing"
    if not force and preparation.clock() < first_correction.timestamp(
        pub.next_check_at
    ):
        return "not_due"
    try:
        with correction_guard.correction_lock(m.DB, pub.owner_id):
            with m.DB.session() as db:
                pub = db.get(CorrectionPublication, str(pub_id))
                if not pub or pub.phase in TERMINAL:
                    return pub.phase if pub else "missing"
                _scope(db, pub)
            with execution.bounded(Policy(pub.id, pub.run_id)):
                if pub.phase == "queued":
                    _create(pub)
                elif pub.phase == "creating_pr":
                    _capture(pub.id)
                elif pub.phase == "waiting_checks":
                    _check_and_merge(pub)
                elif pub.phase == "marking_ready":
                    m, _, owner, repo, token = _remote(pub)
                    value = _pr(m, pub, owner, repo, token)
                    if value.get("draft") is False:
                        _update(pub.id, "waiting_checks", "waiting_checks")
                    else:
                        raise execution.ExecutionStopped("write_unknown")
                elif pub.phase == "merging":
                    m, _, owner, repo, token = _remote(pub)
                    value = _pr(m, pub, owner, repo, token)
                    _record_merge(pub, value)
                elif pub.phase == "waiting_deployment":
                    _deployment(pub)
                elif pub.phase == "deployment_confirmed":
                    job_id = control.enqueue(pub.id)
                    if not job_id:
                        _update(pub.id, "deployment_confirmed", "analysis_busy")
                elif pub.phase == "waiting_control":
                    _control_result(pub)
            with m.DB.session() as db:
                return db.get(CorrectionPublication, pub.id).phase
    except correction_guard.CorrectionBusy:
        return "busy"
    except execution.ExecutionStopped as exc:
        if exc.reason == "paused":
            with m.DB.session() as db:
                current = db.get(CorrectionPublication, pub_id)
            if current:
                _update(pub_id, current.phase, "paused")
            return "paused"
        phase = (
            "revoked"
            if exc.reason == "revoked"
            else "expired"
            if exc.reason == "expired"
            else "review_required"
        )
        _update(pub_id, phase, exc.reason)
        control.cancel(pub_id)
        return phase
    except Exception:
        # Keep write intents for the next read-only reconciliation after uncertain results.
        m.logger.error("[publication] step not confirmed")
        with m.DB.session() as db:
            current = db.get(CorrectionPublication, pub_id)
        if current and current.phase in {"creating_pr", "marking_ready", "merging"}:
            _update(pub_id, current.phase, "write_unknown")
            return current.phase
        _update(pub_id, "review_required", "unavailable")
        return "review_required"


def revoke(pub_id, owner_id):
    m = preparation.module()
    with m.DB.session() as db:
        pub = db.scalar(
            select(CorrectionPublication)
            .where(
                CorrectionPublication.id == pub_id,
                CorrectionPublication.owner_id == owner_id,
            )
            .with_for_update()
        )
        if not pub:
            raise execution.ExecutionStopped("task_missing")
        if not pub.revoked_at:
            pub.revoked_at = preparation.clock()
            _audit(db, pub, "revoked")
        db.commit()
    control.cancel(pub_id)


def sweep(*, limit=1):
    m = preparation.module()
    if preparation.paused() or m._env_bool("SEO_AGENT_DISABLE_SCHEDULERS"):
        return {"paused": True, "examined": 0}
    with m.DB.session() as db:
        ids = list(
            db.scalars(
                select(CorrectionPublication.id)
                .join(Project, Project.id == CorrectionPublication.project_id)
                .where(
                    CorrectionPublication.phase.not_in(TERMINAL),
                    or_(
                        CorrectionPublication.revoked_at.is_not(None),
                        Project.settings["autocorrection_paused"]
                        .as_boolean()
                        .is_not(True),
                    ),
                    CorrectionPublication.next_check_at <= preparation.clock(),
                )
                .order_by(CorrectionPublication.next_check_at)
                .limit(min(max(1, limit), 3))
            )
        )
    states = {}
    for value in ids:
        state = step(value)
        states[state] = states.get(state, 0) + 1
    return {"paused": False, "examined": len(ids), "states": states}


def start():
    global _STARTED
    m = preparation.module()
    with _GUARD:
        if _STARTED or m._env_bool("SEO_AGENT_DISABLE_SCHEDULERS"):
            return
        _STARTED = True

    def loop():
        while not m._WORKER_STOP.wait(30):
            try:
                sweep()
            except Exception:
                m.logger.error("[publication] queue unavailable")

    threading.Thread(target=loop, name="correction-publication", daemon=True).start()
