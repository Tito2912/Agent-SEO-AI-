"""Atomic, single-use crawl reservations for an approved publication."""

import json
import math
import re
import time
import uuid

from sqlalchemy import select

try:
    from . import (
        autocorrection as preparation,
        billing,
        correction_decisions as decisions,
        correction_memory as memory,
        first_correction,
    )
    from .models import (
        CorrectionPublication,
        IssueTask,
        JobRecord,
        Project,
        UsageEvent,
        User,
    )
except ImportError:
    import autocorrection as preparation
    import billing
    import correction_decisions as decisions
    import correction_memory as memory
    import first_correction
    from models import (
        CorrectionPublication,
        IssueTask,
        JobRecord,
        Project,
        UsageEvent,
        User,
    )


def options(project):
    m = preparation.module()
    path = m._resolve_config_path(str(m.DEFAULT_CONFIG))
    if not m._config_path_allowed(path):
        raise ValueError("control_config")
    crawl, gsc, bing = m._effective_project_crawl_settings(
        project.slug,
        config_path=path if path.exists() else None,
        project_settings=project.settings or {},
    )
    return path, crawl, gsc, bing


def fingerprint(project, crawl, gsc, bing):
    return decisions.digest(
        [
            str(project.id),
            str(project.owner_user_id),
            project.base_url,
            crawl,
            gsc,
            bing,
        ]
    )


def analysis_key(project):
    _, crawl, gsc, bing = options(project)
    return fingerprint(project, crawl, gsc, bing)


def valid(db, publication):
    if not publication or publication.revoked_at or preparation.paused():
        return False
    if preparation.clock() >= first_correction.timestamp(publication.expires_at):
        return False
    project = db.get(Project, publication.project_id)
    task = db.get(IssueTask, publication.task_id)
    return bool(
        project
        and task
        and project.owner_user_id == publication.owner_id
        and task.project_id == project.id
        and not preparation.project_paused(project)
        and preparation.config_key(project) == publication.config_key
        and analysis_key(project) == publication.analysis_key
        and publication.applied_key
        and memory.attempt_key(task) == publication.applied_key
    )


def enqueue(publication_id):
    """Quota, job, declaration and mandate link commit together, or not at all."""
    m = preparation.module()
    with m.DB.session() as db:
        pub = db.scalar(
            select(CorrectionPublication)
            .where(CorrectionPublication.id == publication_id)
            .with_for_update()
        )
        if not pub or pub.job_issued:
            return pub.job_id if pub else None
        project = db.get(Project, pub.project_id)
        task = db.get(IssueTask, pub.task_id)
        owner = db.get(User, pub.owner_id)
        if (
            not project
            or not task
            or not owner
            or project.owner_user_id != pub.owner_id
            or pub.phase != "deployment_confirmed"
            or not pub.receipt.get("deployment_id")
            or pub.revoked_at
            or preparation.paused()
            or preparation.project_paused(project)
            or preparation.clock() >= first_correction.timestamp(pub.expires_at)
            or preparation.config_key(project) != pub.config_key
            or analysis_key(project) != pub.analysis_key
        ):
            raise ValueError("control_config")
        if db.scalar(
            select(JobRecord.id)
            .where(
                JobRecord.owner_user_id == pub.owner_id,
                JobRecord.slug == project.slug,
                JobRecord.kind == "crawl",
                JobRecord.status.in_(["queued", "running", "cancel_requested"]),
            )
            .limit(1)
        ):
            return None
        if memory.attempt_key(task) != pub.receipt.get("proposal_key"):
            raise ValueError("application_changed")
        path, crawl, _, _ = options(project)
        requested = max(1, int(crawl.get("max_pages") or 300))
        plan = billing.crawl_config_for_plan(
            billing.effective_plan_key(db, user_id=pub.owner_id)
        )
        planned = min(
            requested,
            pub.pages_budget,
            max(1, int(plan.get("max_pages_per_crawl") or 300)),
        )
        if not owner.is_admin:
            ok, remaining = billing.ensure_within_quota(
                db,
                user_id=pub.owner_id,
                metric="pages_crawled_month",
                planned_amount=planned,
            )
            if not ok and isinstance(remaining, int) and remaining > 0:
                planned = min(planned, remaining)
            elif not ok:
                raise ValueError("quota")
        # Control declarations follow the real deployment receipt, never the merge alone.
        decisions.lock_project(db, project.id)
        note = first_correction.note_data(task.note)
        journey = dict(note.get("first_correction") or {})
        now = preparation.clock()
        journey["declared_at"] = now.isoformat()
        note["first_correction"] = journey
        note.pop("verify", None)
        note["autocorrection_publication"] = str(pub.id)
        task.note = json.dumps(note, ensure_ascii=False)
        task.status = "in_progress"
        pub.applied_key = memory.attempt_key(task)
        job_id = str(
            uuid.uuid5(uuid.NAMESPACE_URL, "noyaru-publication-control:" + str(pub.id))
        )
        result = {
            "type": "crawl",
            "slug": project.slug,
            "user_id": pub.owner_id,
            "started_by": pub.owner_id,
            "requested_max_pages": requested,
            "override_max_pages": planned,
            "quota_reserved_pages": planned,
            "publication_control": str(pub.id),
            "quota_period": billing._period_key(now),
            "job_timeout_s": int(plan.get("job_timeout_s") or 1800),
            "max_pagespeed_urls": max(1, int(plan.get("max_pagespeed_urls") or 1)),
        }
        if owner.is_admin:
            result["skip_billing"] = True
        else:
            billing.usage_add(
                db,
                user_id=pub.owner_id,
                metric="pages_crawled_month",
                amount=planned,
                meta={
                    "kind": "crawl_reserve",
                    "job_id": job_id,
                    "slug": project.slug,
                    "publication_id": pub.id,
                },
                commit=False,
            )
        script = (
            m.REPO_ROOT
            / "skills"
            / "public"
            / "seo-autopilot"
            / "scripts"
            / "seo_audit.py"
        )
        job = JobRecord(
            id=job_id,
            owner_user_id=pub.owner_id,
            slug=project.slug,
            kind="crawl",
            status="queued",
            created_at=time.time(),
            updated_at=time.time(),
            config_path=str(path),
            command=[m.sys.executable, "-u", str(script)],
            result=result,
            attempts=0,
            max_attempts=1,
            run_after=math.ceil(now.timestamp()) + 1,
        )
        db.add(job)
        db.flush()
        pub.job_id = job_id
        pub.job_issued = True
        pub.reserved_pages = planned
        pub.receipt = {**pub.receipt, "quota_period": billing._period_key(now)}
        pub.phase = "waiting_control"
        pub.reason = "control_queued"
        db.commit()
        return job_id


def _settle(db, pub, row, actual):
    data = dict(row.result or {})
    if data.get("publication_settled") or (pub and pub.control_settled):
        return
    reserved = max(0, int(data.get("quota_reserved_pages") or 0))
    actual = min(max(0, int(actual)), reserved)
    period = str(data.get("quota_period") or "")
    if not re.fullmatch(r"\d{4}-\d{2}", period):
        raise ValueError("quota_period")
    delta = actual - reserved
    if delta and not data.get("skip_billing"):
        db.add(
            UsageEvent(
                user_id=row.owner_user_id,
                period=period,
                metric="pages_crawled_month",
                amount=delta,
                meta={
                    "kind": "crawl_adjust",
                    "job_id": row.id,
                    "publication_id": data["publication_control"],
                    "reserved_pages": reserved,
                    "actual_pages_crawled": actual,
                },
            )
        )
    if pub:
        pub.control_settled = True
    row.result = {**data, "quota_reserved_pages": 0, "publication_settled": True}


def settle(job, *, actual=None):
    """Job receipt survives project/mandate deletion; settlement is exactly once."""
    data = job.result if isinstance(job.result, dict) else {}
    pid = data.get("publication_control")
    if not pid:
        return False
    m = preparation.module()
    with m.DB.session() as db:
        pub = db.scalar(
            select(CorrectionPublication)
            .where(CorrectionPublication.id == str(pid))
            .with_for_update()
        )
        row = db.scalar(
            select(JobRecord).where(JobRecord.id == str(job.id)).with_for_update()
        )
        if not row or row.result.get("publication_control") != pid:
            raise ValueError("control_identity")
        if pub and (pub.job_id != row.id or pub.owner_id != row.owner_user_id):
            raise ValueError("control_identity")
        if actual is None:
            return True  # Unreported usage retains its reservation, never guesses a refund.
        _settle(db, pub, row, actual)
        db.commit()
    job.result = {**data, "quota_reserved_pages": 0, "publication_settled": True}
    return True


def cancel(publication_id):
    m = preparation.module()
    with m.DB.session() as db:
        pub = db.scalar(
            select(CorrectionPublication)
            .where(CorrectionPublication.id == publication_id)
            .with_for_update()
        )
        row = (
            db.scalar(
                select(JobRecord).where(JobRecord.id == pub.job_id).with_for_update()
            )
            if pub and pub.job_id
            else None
        )
        if not row:
            return
        if row.status == "queued":
            _settle(db, pub, row, 0)
            row.status = "canceled"
            row.finished_at = time.time()
        elif row.status == "running":
            row.status = "cancel_requested"
        db.commit()


def _stop(job, *, unstarted=True):
    m = preparation.module()
    job.status = "canceled"
    job.finished_at = time.time()
    settle(job, actual=0 if unstarted else None)
    m._save_job(job)


def guard_start(job):
    data = job.result if isinstance(job.result, dict) else {}
    pid = data.get("publication_control")
    if not pid:
        return True
    m = preparation.module()
    with m.DB.session() as db:
        pub = db.get(CorrectionPublication, str(pid))
        row = db.get(JobRecord, str(job.id))
        project = db.get(Project, pub.project_id) if pub else None
        owner = db.get(User, pub.owner_id) if pub else None
        plan = (
            billing.crawl_config_for_plan(
                billing.effective_plan_key(db, user_id=pub.owner_id)
            )
            if owner
            else {}
        )
        first = bool(row and row.attempts == 1)
        good = bool(
            pub
            and project
            and row
            and owner
            and first
            and pub.job_issued
            and not pub.control_settled
            and pub.phase == "waiting_control"
            and pub.job_id == str(job.id)
            and row.owner_user_id == pub.owner_id
            and row.slug == project.slug
            and row.kind == "crawl"
            and row.status == "running"
            and bool(data.get("skip_billing")) == bool(owner.is_admin)
            and int(data.get("override_max_pages") or 0) == pub.reserved_pages
            and pub.reserved_pages <= int(plan.get("max_pages_per_crawl") or 300)
            and valid(db, pub)
        )
    if not good:
        _stop(job, unstarted=first)
    return good


def worker_settings(job, base_url, crawl, gsc, bing):
    pid = (job.result or {}).get("publication_control")
    if not pid:
        return True
    m = preparation.module()
    with m.DB.session() as db:
        pub = db.get(CorrectionPublication, str(pid))
        project = db.get(Project, pub.project_id) if pub else None
        good = bool(
            pub
            and project
            and base_url == project.base_url
            and fingerprint(project, crawl, gsc, bing) == pub.analysis_key
            and valid(db, pub)
        )
    if not good:
        _stop(job)
    return good
