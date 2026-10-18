"""Read the first-correction journey from existing tasks and crawl verification receipts."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from urllib.parse import urlsplit


def note_data(value):
    try:
        value = json.loads(value or "{}")
    except (ValueError, TypeError):
        return {}
    return value if isinstance(value, dict) else {}


def timestamp(value):
    try:
        parsed = datetime.strptime(str(value), "%Y%m%d-%H%M%S")
    except ValueError:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


_RECEIPT_KEYS = {"verify", "first_correction", "verification_urls", "verification_expected_count",
                 "deep", "bulk", "pages", "pr_url", "pr_number", "commit_sha"}


def edited_note(value, previous=None):
    """Customer notes cannot create or replace a server's verification or PR receipt."""
    stored = note_data(previous)
    if _RECEIPT_KEYS.intersection(stored):
        stored["user_note"] = value or ""
        return json.dumps(stored, ensure_ascii=False)
    if _RECEIPT_KEYS.intersection(note_data(value)):
        return json.dumps({"user_note": value or ""}, ensure_ascii=False)
    return value or None


def web_url(value):
    value = str(value or "").strip()
    try:
        parts = urlsplit(value)
        return value if parts.scheme in {"http", "https"} and parts.netloc else ""
    except ValueError:
        return ""


def progress(tasks):
    """A declared correction is distinct from a newer crawl confirming its resolution."""
    states = []
    for task in tasks:
        if task.status == "ignored":
            continue
        note = note_data(task.note)
        verify = note.get("verify") if isinstance(note.get("verify"), dict) else {}
        journey = note.get("first_correction") if isinstance(note.get("first_correction"), dict) else {}
        before, after = timestamp(task.crawl_ts), timestamp(verify.get("crawl_ts"))
        verified = bool(task.status == "done" and verify.get("result") == "resolved"
                        and timestamp(verify.get("verified_at")) and before and after and after > before)
        # Generated content and query rewrites have no technical defect for this journey.
        url = web_url(task.url)
        if not verified and (not url or str(task.issue_key).startswith("_")):
            continue
        states.append({
            "task_id": str(task.id), "project_id": str(task.project_id), "issue_key": task.issue_key,
            "label": task.issue_label or task.issue_key, "url": url, "crawl_ts": task.crawl_ts,
            "verified": verified, "verify": verify,
            "declared": bool(journey.get("declared_at") or task.status in {"in_progress", "done"}),
            "guided": bool(journey), "pr_url": web_url(note.get("pr_url")),
        })
    if not states:
        return {}
    # Rows arrive newest first. Keep a confirmed success, then the customer's guided choice.
    return next((state for state in states if state["verified"]),
                next((state for state in states if state["guided"]), states[0]))
