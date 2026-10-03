"""Durable receipts under the payer lease; never store connection credentials."""

from __future__ import annotations

import contextvars
import hashlib
import json
import os
import threading
from contextlib import contextmanager

from sqlalchemy import select

try:
    from .correction_guard import CorrectionStoreUnavailable, ensure_active
    from .models import CorrectionOperation
except ImportError:
    from correction_guard import CorrectionStoreUnavailable, ensure_active
    from models import CorrectionOperation

_CURRENT = contextvars.ContextVar("correction_operation", default=None)


def current():
    entry = _CURRENT.get()
    return entry[2] if entry and entry[:2] == (os.getpid(), threading.get_ident()) else None


def request_key(payer, project, action, payload):
    raw = json.dumps([payer, project, action, payload], sort_keys=True, ensure_ascii=True,
                     separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


def find(database, *, key):
    try:
        with database.session() as db:
            return db.scalar(select(CorrectionOperation).where(CorrectionOperation.request_key == key))
    except Exception as exc:
        raise CorrectionStoreUnavailable() from exc


def pending(database, *, payer):
    try:
        with database.session() as db:
            return db.scalar(select(CorrectionOperation).where(
                CorrectionOperation.payer_id == payer,
                CorrectionOperation.state.in_(["running", "blocked"])))
    except Exception as exc:
        raise CorrectionStoreUnavailable() from exc


class Operation:
    def __init__(self, database, row):
        self.database = database
        self.row = row

    def save(self, *, state=None, release=False, **data):
        ensure_active()
        try:
            with self.database.session() as db:
                row = db.get(CorrectionOperation, self.row.id)
                if row is None:
                    raise CorrectionStoreUnavailable()
                row.data = {**row.data, **data}
                if state:
                    row.state = state
                if release:
                    row.request_key = None
                db.commit()
                self.row = row
        except Exception as exc:
            raise CorrectionStoreUnavailable() from exc

    def finish(self, response):
        body = json.loads(response.body)
        if 200 <= response.status_code < 300:
            self.save(state="completed", response={"body": body, "status": response.status_code})
        elif self.row.data.get("writes_started") or self.row.charged or self.row.data.get("preview"):
            self.save(state="blocked")
        else:
            self.save(state="abandoned", release=True)

    def charge(self, callback):
        ensure_active()
        try:
            with self.database.session() as db:
                row = db.get(CorrectionOperation, self.row.id)
                if row is None:
                    raise CorrectionStoreUnavailable()
                if not row.charged:
                    callback(db)
                    row.charged = True
                    db.commit()
                self.row = row
        except Exception as exc:
            raise CorrectionStoreUnavailable() from exc


@contextmanager
def operation(database, *, payer, project, action, key, row=None):
    ensure_active()
    if row is None:
        try:
            with database.session() as db:
                row = CorrectionOperation(payer_id=payer, project_id=project, action=action,
                                          request_key=key, state="running", data={})
                db.add(row)
                db.commit()
        except Exception as exc:
            raise CorrectionStoreUnavailable() from exc
    op = Operation(database, row)
    token = _CURRENT.set((os.getpid(), threading.get_ident(), op))
    try:
        yield op
    finally:
        _CURRENT.reset(token)


def branch(value):
    op = current()
    if not op:
        return value
    value += "-" + op.row.id[:8]
    op.save(branch=value, writes_started=True)
    return value


def write_intent():
    op = current()
    if op and not op.row.data.get("writes_started"):
        op.save(writes_started=True)


def preview(body, *, original=None):
    op = current()
    if op:
        data = {"preview": body}
        if original is not None:
            data["preview_source"] = hashlib.sha256(original.encode()).hexdigest()
        op.save(**data)


def unchanged_preview_source(database, *, project, file, patched, original):
    try:
        with database.session() as db:
            rows = db.scalars(select(CorrectionOperation).where(
                CorrectionOperation.project_id == project).order_by(CorrectionOperation.created_at.desc())).all()
            for row in rows:
                body = row.data.get("preview", {})
                if body.get("file") == file and body.get("patched_content") == patched and row.data.get("preview_source"):
                    return hashlib.sha256(original.encode()).hexdigest() == row.data["preview_source"]
    except Exception as exc:
        raise CorrectionStoreUnavailable() from exc
    return True


def pr_intent(*, owner, repo, base, tasks, billable, motif):
    op = current()
    if op:
        op.save(pr_intent={"owner": owner, "repo": repo, "base": base,
                           "tasks": tasks, "billable": billable, "motif": motif})


def pr_received(data):
    op = current()
    if op:
        intent = op.row.data.get("pr_intent", {})
        if not valid_pr(data, intent):
            raise CorrectionStoreUnavailable()
        previous = op.row.data.get("pr", {})
        pr = dict(previous) if previous.get("number") == data["number"] else {}
        pr.update(number=data["number"], html_url=data["html_url"])
        if isinstance(data.get("head"), dict) and data["head"].get("sha"):
            pr["head"] = {"sha": data["head"]["sha"]}
        if isinstance(data.get("node_id"), str):
            pr["node_id"] = data["node_id"]
        op.save(pr=pr)


def valid_pr(data, intent):
    if not isinstance(intent, dict) or not all(isinstance(intent.get(k), str) and intent[k] for k in ("owner", "repo")):
        return False
    if not isinstance(data, dict) or type(data.get("number")) is not int or data["number"] <= 0:
        return False
    expected = "https://github.com/{owner}/{repo}/pull/{number}".format(**intent, number=data["number"])
    return isinstance(data.get("html_url"), str) and data["html_url"].casefold() == expected.casefold()


def matches_pr(data, intent, branch):
    if not valid_pr(data, intent) or data.get("state") not in {"open", "closed"}:
        return False
    head, base = data.get("head"), data.get("base")
    if not isinstance(head, dict) or not isinstance(base, dict):
        return False
    expected = (intent["owner"] + "/" + intent["repo"]).casefold()
    return (head.get("ref") == branch and base.get("ref") == intent["base"]
            and all(isinstance(part.get("repo"), dict)
                    and str(part["repo"].get("full_name", "")).casefold() == expected for part in (head, base)))


def known_pr(database, *, project, issue, url):
    try:
        with database.session() as db:
            rows = db.scalars(select(CorrectionOperation).where(
                CorrectionOperation.project_id == project,
                CorrectionOperation.state.in_(["completed", "blocked", "running"])
            ).order_by(CorrectionOperation.created_at.desc())).all()
            for row in rows:
                pr = row.data.get("pr") or row.data.get("response", {}).get("body", {})
                number = pr.get("number") or pr.get("pr_number")
                link = pr.get("html_url") or pr.get("pr_url")
                if not number or not link:
                    continue
                tasks = row.data.get("pr_intent", {}).get("tasks", [])
                if any(t.get("issue_key") == issue and t.get("url") == url for t in tasks):
                    return int(number), str(link)
    except Exception as exc:
        raise CorrectionStoreUnavailable() from exc
    return None
