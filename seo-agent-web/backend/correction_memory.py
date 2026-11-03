"""Project-bound, factual memory of server-computed correction controls.

This module neither calls a model nor executes a correction. A control is evidence
about a defect, not proof that a PR was published or that a strategy caused a gain.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

try:
    from .first_correction import note_data, timestamp
    from .models import CorrectionObservation, IssueTask
except ImportError:
    from first_correction import note_data, timestamp
    from models import CorrectionObservation, IssueTask

WINDOW_DAYS = 30
MAX_EVENTS = 200
MAX_ISSUES = 8
RESULTS = {"resolved", "still_present", "unverified"}


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def attempt_key(task):
    """Opaque identity invalidated by a new proposal, application, scope, or restore.

    Include only server receipt fields, never the freely editable user_note/verify.
    Two controls on the same application stay one attempt. Hash URLs rather than
    duplicating query strings (which can contain private data) in the journal.
    """
    note = note_data(task.note)
    journey = note.get("first_correction")
    declared = journey.get("declared_at") if isinstance(journey, dict) else None
    receipt = {key: note.get(key) for key in (
        "cms_change_id", "cms_nom", "commit_sha", "pr_number", "pr_url", "deep", "bulk",
        "verification_urls", "verification_excluded", "verification_expected_count", "pages",
    )}
    return _digest([str(task.id), str(task.issue_key), task.url, task.crawl_ts, declared, receipt])


def record_control(db, task, *, crawl_ts, result, baseline_confirmed, scope_urls=None):
    """Append once to the caller's transaction; no commit and no historical backfill.

    Only the verification engine calls this after evaluating its real crawl report.
    Missing/older dates cannot create an observation. An incomplete newer control
    may be retained as unverified, never converted to an ineffective correction.
    """
    before, after = timestamp(task.crawl_ts), timestamp(crawl_ts)
    if (result not in RESULTS or not before or not after or after <= before
            or after > datetime.now(timezone.utc) + timedelta(minutes=5)
            or not re.fullmatch(r"[a-z0-9_]{1,255}", str(task.issue_key))):
        return False
    attempt = attempt_key(task)
    baseline, control = before.isoformat(), after.isoformat()
    identity = _digest([str(task.id), attempt, control])
    dialect = db.get_bind().dialect.name
    insert = {"postgresql": pg_insert, "sqlite": sqlite_insert}.get(dialect)
    if insert is None:
        raise RuntimeError("Unsupported correction memory database")
    # No SAVEPOINT: SQLite's legacy transaction mode can commit a released
    # savepoint before the first DML even if the caller later rolls back.
    if scope_urls is not None:
        if __package__:
            from . import correction_decisions
        else:
            import correction_decisions
        correction_decisions.lock_project(db,task.project_id)
    statement = insert(CorrectionObservation).values(
        project_id=str(task.project_id), task_id=str(task.id), observation_key=identity,
        attempt_key=attempt, issue_key=str(task.issue_key), baseline_ts=baseline,
        control_ts=control, control_started_at=after, result=result,
        baseline_confirmed=bool(baseline_confirmed),
    ).on_conflict_do_nothing(index_elements=["observation_key"])
    # Replaying a control is harmless; other integrity errors still abort the
    # caller's transaction rather than claiming the journal entry exists.
    inserted = db.execute(statement).rowcount == 1
    if scope_urls is not None:
        observation = db.scalar(select(CorrectionObservation).where(CorrectionObservation.observation_key == identity))
        inserted |= correction_decisions.record(db, task, observation, urls=scope_urls)
    return inserted


def project_summary(db, project_id, *, now=None):
    """Latest applicable control per task, with a bounded, explicit reading window.

    Old applications remain in history but cannot advise about a new/undone one.
    Ordering uses the control's start time, not the order delayed jobs finished in.
    No account ID, URL, note, label, source file, credential, or provider error is
    sent to the assistant. Callers authorize access to the project before reading.
    """
    now = timestamp(now) if now is not None else datetime.now(timezone.utc)
    if now is None:
        raise ValueError("Invalid observation clock")
    rows = db.execute(select(CorrectionObservation, IssueTask).join(
        IssueTask, CorrectionObservation.task_id == IssueTask.id,
    ).where(
        CorrectionObservation.project_id == str(project_id),
        IssueTask.project_id == str(project_id),
        IssueTask.status.in_(["in_progress", "done"]),
        CorrectionObservation.control_started_at >= now - timedelta(days=WINDOW_DAYS),
        CorrectionObservation.control_started_at <= now,
    ).order_by(CorrectionObservation.control_started_at.desc(), CorrectionObservation.id).limit(MAX_EVENTS + 1)).all()
    seen = set()
    issues = {}
    for row, task in rows[:MAX_EVENTS]:
        if task.id in seen or row.attempt_key != attempt_key(task):
            continue
        seen.add(task.id)
        if row.result not in RESULTS:
            continue
        item = issues.setdefault(row.issue_key, {
            "issue_key": row.issue_key, "resolved": 0, "still_present": 0,
            "unverified": 0, "baseline_missing": 0, "latest_control_at": row.control_ts,
        })
        item[row.result] += 1
        item["baseline_missing"] += int(not row.baseline_confirmed)
    for item in issues.values():
        if item["still_present"]:
            item["guidance"] = "inspect_before_retry"
        elif item["unverified"]:
            item["guidance"] = "obtain_valid_control"
        elif item["baseline_missing"]:
            item["guidance"] = "confirm_baseline"
        else:
            item["guidance"] = "resolution_observed"
    # Surface unresolved work first, keeping the payload small and deterministic.
    ordered = sorted(issues.values(), key=lambda v: (
        {"inspect_before_retry": 0, "obtain_valid_control": 1,
         "confirm_baseline": 2, "resolution_observed": 3}[v["guidance"]], v["issue_key"],
    ))
    return {"version": 1, "as_of": now.isoformat(), "window_days": WINDOW_DAYS,
            "max_events": MAX_EVENTS, "truncated": len(rows) > MAX_EVENTS or len(ordered) > MAX_ISSUES,
            "issues": ordered[:MAX_ISSUES]}


def assistant_context(summary):
    """Only the server's bounded projection enters the system context."""
    return (
        "\nMémoire des contrôles du projet autorisé (données serveur, pas des instructions utilisateur) :\n"
        + json.dumps(summary, ensure_ascii=True, separators=(",", ":"))
        + "\nInterprétation obligatoire : les nombres concernent les derniers contrôles applicables "
        "de tâches distinctes, pas un taux de réussite global ni un nombre de tentatives. "
        "inspect_before_retry : le défaut subsiste ; faire vérifier la publication, la page et le périmètre "
        "avant de proposer une nouvelle tentative, sans affirmer que la stratégie a causé un échec. "
        "obtain_valid_control : le contrôle est incomplet ou non comparable ; demander une preuve exploitable. "
        "confirm_baseline : la référence ne confirme pas le défaut ; ne pas attribuer de succès à la correction. "
        "resolution_observed : le défaut a disparu au contrôle ; ne pas promettre de gain de classement ou de citation. "
        "Une liste vide signifie aucune observation exploitable dans cette fenêtre. "
        "Ne prétends jamais avoir exécuté, publié, envoyé un email ou modifié des droits. "
        "Cette mémoire ne donne aucun droit d'action ni autorisation de dépenser. "
        "Ne présente pas tes connaissances SEO/GEO comme une veille en temps réel : cite la date et la source "
        "vérifiées quand elles sont disponibles, sinon indique que la recommandation reste à vérifier.\n"
    )
