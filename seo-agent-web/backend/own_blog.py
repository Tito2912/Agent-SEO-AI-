"""Owner's own blog: reviewed files -> draft PR -> separate merge approval -> observation."""

from datetime import timedelta
import copy
import re
import threading
import uuid

import requests
from sqlalchemy import select, or_

try:
    from . import own_seo as s, own_writing as w, own_blog_content as c
    from .own_blog_github import GitHub, Stop
    from .models import OwnBlogPublication as Publication, AuditLog, UserConnection
except ImportError:
    import own_seo as s
    import own_writing as w
    import own_blog_content as c
    from own_blog_github import GitHub, Stop
    from models import OwnBlogPublication as Publication, AuditLog, UserConnection

PATH = "/settings/system/own-seo/publication"
MAX_READS = 600
MAX_WRITES = 10
RUNNABLE = {
    "queued_prepare",
    "preparing",
    "reviewing",
    "queued_publish",
    "publishing",
    "observing",
    "uncertain",
}
_STOP = threading.Event()
_THREAD = None
_LOCK = threading.Lock()


def now():
    return s.clock()


def audit(db, row, action):
    db.add(
        AuditLog(
            actor_user_id=row.owner_id,
            action="own_blog." + action,
            target_type="own_blog_publication",
            target_id=row.id,
            meta={
                "state": row.state,
                "generation": row.generation,
                "reads": row.reads,
                "writes": row.writes,
            },
        )
    )


def connection(owner, *, db):
    # The general connection helper can lazy-rotate a legacy secret and commit its
    # session. Decisions must stay atomic, and SQLite must not open a second writer
    # under this transaction's lock. Decode here without migrating or committing.
    stored = db.scalar(
        select(UserConnection).where(
            UserConnection.user_id == owner, UserConnection.key == "GITHUB_TOKEN"
        )
    )
    try:
        token, _ = (
            s.module()._decrypt_secret_with_rotation(
                str(stored.secret_value or "").strip()
            )
            if stored
            else ("", False)
        )
    except Exception:
        token = ""
    if not token:
        raise Stop("github_required")
    return token, s.digest(["own-blog", token])


def scope(db, owner, ident, *, fresh=True):
    row = db.get(Publication, ident)
    if not row or row.owner_id != owner:
        raise Stop("missing")
    workspace, project = s.scope(db, owner)
    if (
        not workspace
        or not project
        or workspace.paused
        or row.workspace_token != workspace.token
        or row.project_id != project.id
    ):
        raise Stop("paused")
    drafts = c.drafts(db, owner, row.fr_id, row.en_id, current=fresh)
    if row.proposal["drafts"] != [[d.id, d.input_hash, d.content_hash] for d in drafts]:
        raise Stop("draft_required")
    if fresh:
        slug, files = c.files(db, owner, row.fr_id, row.en_id, row.proposal["date"])
        if slug != row.slug or files != row.proposal["files"]:
            raise Stop("draft_required")
    for file in row.proposal["files"]:
        prefix = "en/" if file["lang"] == "en" else ""
        url = (
            s.BASE.rstrip("/")
            + ("/en" if prefix else "")
            + "/ressources-seo/"
            + row.slug
        )
        if (
            file["path"] != c.ROOT + prefix + row.slug + ".md"
            or file["url"] != url
            or c.sha(file["text"]) != file["sha256"]
            or c.blob(file["text"]) != file["blob"]
        ):
            raise Stop("target_changed")
    token, fingerprint = connection(owner, db=db)
    if fingerprint != row.connection_hash:
        raise Stop("connection_changed")
    return row, token


def version(row, kind):
    return s.digest(
        [
            row.id,
            row.generation,
            kind,
            row.proposal,
            row.receipt.get("head") if kind == "blog_publish" else None,
            row.receipt.get("checks") if kind == "blog_publish" else None,
        ]
    )


def prepare(owner, fr_id, en_id, key, confirm):
    if not confirm:
        raise Stop("confirm_required")
    if not re.fullmatch("[a-f0-9]{32}", key):
        raise Stop("changed")
    # Reserve before network. A duplicate form cannot fan out GitHub operations.
    with s.module().DB.session() as db:
        s.locked(db)
        workspace, project = s.scope(db, owner)
        if not workspace or not project or workspace.paused:
            raise Stop("paused")
        rows = c.drafts(db, owner, fr_id, en_id)
        slug, files = c.files(db, owner, fr_id, en_id, now().date().isoformat())
        existing = db.scalar(
            select(Publication)
            .where(
                Publication.owner_id == owner,
                or_(Publication.authorization_key == key, Publication.slug == slug),
            )
            .order_by(Publication.created_at.desc())
            .limit(1)
        )
        if existing:
            same_key = existing.authorization_key == key
            if existing.state == "cancelled" and not same_key:
                if any(not intent.get("done") for intent in existing.intents.values()):
                    raise Stop("github_uncertain")
                # Only a new explicit web preparation can create another request.
                # The scheduler never revives a refused proposal or its buttons.
            elif existing.fr_id != fr_id or existing.en_id != en_id:
                raise Stop("article_exists")
            else:
                return existing.id
        _, fingerprint = connection(owner, db=db)
        row = Publication(
            owner_id=owner,
            project_id=project.id,
            fr_id=fr_id,
            en_id=en_id,
            workspace_token=workspace.token,
            authorization_key=key,
            connection_hash=fingerprint,
            slug=slug,
            proposal={
                "drafts": [[d.id, d.input_hash, d.content_hash] for d in rows],
                "files": files,
                "base": None,
                "date": now().date().isoformat(),
            },
            receipt={},
            intents={},
            state="checking",
            generation=1,
            reads=0,
            writes=0,
            expires_at=now() + timedelta(hours=24),
        )
        db.add(row)
        db.flush()
        ident = row.id
        audit(db, row, "proposal")
        db.commit()
    return ident


class Worker:
    def __init__(self, owner, ident, lease, *, observation=False):
        self.owner, self.ident, self.lease, self.observation = (
            owner,
            ident,
            lease,
            observation,
        )
        self.api = GitHub(self.guard, self.finish)

    def scoped(self, db, *, write=False):
        existing = db.get(Publication, self.ident)
        historical = self.observation or bool(
            existing and existing.intents.get("merge")
        )
        return scope(db, self.owner, self.ident, fresh=write or not historical)

    def guard(self, action, *, key=None, body=None):
        with s.module().DB.session() as db:
            s.locked(db)
            row, token = self.scoped(db)
            if (
                row.lease_token != self.lease
                or not row.lease_until
                or w.utc(row.lease_until) <= now()
            ):
                raise Stop("lease_ended")
            if w.utc(row.expires_at) <= now():
                raise Stop("expired")
            if action == "intent":
                if row.state not in {"preparing", "publishing", "uncertain"}:
                    raise Stop("authorization_required")
                fingerprint = s.digest(body)
                known = row.intents.get(key)
                if known:
                    if known["hash"] != fingerprint:
                        raise Stop("changed")
                    return copy.deepcopy(known)
                # Every *new* effect needs current sources, draft, binding and mandate.
                self.scoped(db, write=True)
                if row.state == "uncertain":
                    if any(not v.get("done") for v in row.intents.values()):
                        raise Stop("github_uncertain")
                    row.state = (
                        "publishing"
                        if row.receipt.get("phase") == "publish"
                        else "preparing"
                    )
                if row.writes >= MAX_WRITES:
                    raise Stop("budget")
                row.writes += 1
                row.intents = {
                    **row.intents,
                    key: {"hash": fingerprint, "issued": True},
                }
                audit(db, row, "write_intent")
                db.commit()
                return {}
            if row.reads >= MAX_READS:
                raise Stop("budget")
            row.reads += 1
            db.commit()
            return token

    def finish(self, key, result):
        with s.module().DB.session() as db:
            s.locked(db)
            row, _ = self.scoped(db)
            if row.lease_token != self.lease:
                raise Stop("lease_ended")
            # Store only selected receipts, never provider payloads / authentication data.
            small = {
                k: result[k]
                for k in ("sha", "merged", "number", "node_id", "confirmed")
                if k in result
            }
            if "commit" in result:
                small["commit"] = {"sha": (result.get("commit") or {}).get("sha")}
            row.intents = {
                **row.intents,
                key: {**row.intents[key], "done": True, "result": small},
            }
            audit(db, row, "write_receipt")
            db.commit()

    def row(self):
        with s.module().DB.session() as db:
            row, _ = self.scoped(db)
            if row.lease_token != self.lease:
                raise Stop("lease_ended")
            return row

    def save(self, *, state=None, receipt=None, proposal=None):
        with s.module().DB.session() as db:
            s.locked(db)
            row, _ = self.scoped(db)
            if row.lease_token != self.lease:
                raise Stop("lease_ended")
            if state:
                row.state = state
            if receipt is not None:
                row.receipt = {**row.receipt, **receipt}
            if proposal is not None:
                row.proposal = proposal
            row.error_code = None
            audit(db, row, "progress")
            db.commit()


def claim(owner, ident, *, refresh=False):
    with s.module().DB.session() as db:
        s.locked(db)
        row, _ = scope(db, owner, ident, fresh=False)
        if row.lease_until and w.utc(row.lease_until) > now():
            raise Stop("busy")
        if refresh:
            if row.state not in {
                "checking",
                "proposal",
                "review_ready",
                "reviewing",
                "needs_review",
            } or any(not v.get("done") for v in row.intents.values()):
                raise Stop("changed")
        elif row.state not in RUNNABLE:
            raise Stop("changed")
        if refresh and w.utc(row.expires_at) <= now():
            row.expires_at = now() + timedelta(hours=24)
        elif w.utc(row.expires_at) <= now():
            raise Stop("expired")
        row.lease_token = uuid.uuid4().hex
        row.lease_until = now() + timedelta(minutes=4)
        row.next_check_at = now() + timedelta(minutes=10)
        if row.state == "queued_prepare":
            row.state = "preparing"
        if row.state == "queued_publish":
            row.state = "publishing"
        token = row.lease_token
        db.commit()
        return Worker(owner, ident, token, observation=row.state == "observing")


def release(worker, error=None):
    with s.module().DB.session() as db:
        s.locked(db)
        row = db.get(Publication, worker.ident)
        if not row or row.lease_token != worker.lease:
            return
        if error:
            allowed = {
                "github_required",
                "connection_changed",
                "draft_required",
                "paused",
                "github_denied",
                "github_unconfirmed",
                "repository_required",
                "article_exists",
                "diff_changed",
                "branch_changed",
                "pr_changed",
                "checks_pending",
                "checks_failed",
                "checks_incomplete",
                "base_changed",
                "github_uncertain",
                "budget",
                "expired",
                "deploy_pending",
                "deployment_changed",
                "response_limit",
                "translation_required",
                "lease_ended",
                "branch_protection",
            }
            row.error_code = error if error in allowed else "changed"
            if error == "github_uncertain" and row.state in {
                "preparing",
                "publishing",
                "uncertain",
            }:
                row.receipt = {
                    **row.receipt,
                    "phase": row.receipt.get("phase")
                    or ("publish" if row.state == "publishing" else "prepare"),
                }
                row.state = "uncertain"
            elif row.state == "publishing" or error not in {
                "checks_pending",
                "checks_failed",
                "checks_incomplete",
                "deploy_pending",
            }:
                row.state = "needs_review"
            audit(db, row, "stopped")
        row.lease_token = None
        row.lease_until = None
        db.commit()


def refresh(owner, ident):
    try:
        worker = claim(owner, ident, refresh=True)
    except ValueError:
        return
    error = None
    try:
        row = worker.row()
        api = worker.api
        api.repository()
        base = api.head()
        api.absent(row.proposal["files"], base)
        proposal = {**row.proposal, "base": base}
        if row.receipt.get("number"):
            value = api.pr(
                row.receipt["number"], row.receipt["branch"], row.receipt["head"]
            )
            if value.get("state") != "open" or value.get("merged"):
                raise Stop("pr_changed")
            # Never bless additions to the branch that were not reviewed in Noyaru.
            api.diff(row.receipt["base"], row.receipt["head"], proposal["files"])
        with s.module().DB.session() as db:
            s.locked(db)
            live, _ = scope(db, owner, ident)
            if live.lease_token != worker.lease:
                raise Stop("lease_ended")
            changed = live.proposal["base"] is not None
            if changed:
                if live.generation >= 4:
                    raise Stop("budget")
                live.generation += 1
            live.proposal = proposal
            live.state = "proposal"
            live.error_code = None
            audit(db, live, "refresh")
            db.commit()
    except ValueError as exc:
        error = str(exc)
    finally:
        release(worker, error)


def decision_in_session(db, owner, ident, kind, approve, fingerprint):
    row, _ = scope(db, owner, ident)
    expected = "proposal" if kind == "blog_pr" else "review_ready"
    if (
        kind not in {"blog_pr", "blog_publish"}
        or row.state != expected
        or version(row, kind) != fingerprint
    ):
        raise Stop("changed")
    if w.utc(row.expires_at) <= now():
        raise Stop("expired")
    if row.lease_until and w.utc(row.lease_until) > now():
        raise Stop("busy")
    row.state = (
        ("queued_prepare" if kind == "blog_pr" else "queued_publish")
        if approve
        else "cancelled"
    )
    row.receipt = {
        **row.receipt,
        "phase": "prepare" if kind == "blog_pr" else "publish",
    }
    row.next_check_at = now()
    audit(db, row, "approve" if approve else "refuse")
    return row.id if approve else None


def approval_document(db, owner, ident, kind, lang):
    row, _ = scope(db, owner, ident)
    if (
        row.state != ("proposal" if kind == "blog_pr" else "review_ready")
        or w.utc(row.expires_at) <= now()
    ):
        raise Stop("changed")
    en = lang == "en"
    effect = (
        (
            "Create or update only this draft PR. No AI request or publication."
            if en
            else "Créer ou actualiser uniquement cette PR en brouillon. Aucun appel IA ni publication."
        )
        if kind == "blog_pr"
        else (
            "Mark this PR ready and merge its exact head. The automatic deployment can publish these articles. No new AI request."
            if en
            else "Rendre cette PR prête puis fusionner sa tête exacte. Le déploiement automatique pourra publier ces articles. Aucun nouvel appel IA."
        )
    )
    lines = [
        c.REPO + " · main",
        "Base: " + row.proposal["base"],
        (
            "FR/EN: confirm both versions cover the same topic."
            if en
            else "FR/EN : confirmer que les deux versions couvrent le même sujet."
        ),
    ]
    if kind == "blog_publish":
        lines += [
            row.receipt["url"],
            "Commit: " + row.receipt["head"],
            "CI: " + row.receipt["checks"]["url"],
        ]
    for file in row.proposal["files"]:
        lines += [file["path"], file["url"], file["text"]]
    return dict(
        kind=kind,
        target=ident,
        version=version(row, kind),
        title=row.slug,
        text=row.slug + "\n\n" + effect + "\n\n" + "\n\n".join(lines),
        effect=effect,
        path=PATH + "?publication=" + ident,
        ready=True,
        expires_at=row.expires_at,
    )


def prepare_pr(worker):
    row = worker.row()
    api = worker.api
    api.repository()
    if api.head() != row.proposal["base"]:
        raise Stop("base_changed")
    files = row.proposal["files"]
    branch = row.receipt.get("branch") or "noyaru-blog/" + row.id
    if row.receipt.get("number"):
        sync_key = "sync-" + str(row.generation)
        syncing = (
            bool(row.intents.get(sync_key))
            and row.receipt["base"] != row.proposal["base"]
        )
        api.pr(row.receipt["number"], branch, None if syncing else row.receipt["head"])
        head = row.receipt["head"]
        if row.receipt["base"] != row.proposal["base"]:
            head = api.sync(branch, row.proposal["base"], row.generation, files)
        api.diff(row.proposal["base"], head, files)
        api.pr(row.receipt["number"], branch, head)
        worker.save(
            state="reviewing",
            receipt={"base": row.proposal["base"], "head": head, "checks": None},
        )
        return
    if not row.receipt.get("head"):
        api.absent(files, row.proposal["base"])
        api.branch(branch, row.proposal["base"])
        worker.save(
            receipt={
                "branch": branch,
                "base": row.proposal["base"],
                "head": row.proposal["base"],
                "files_done": 0,
            }
        )
    row = worker.row()
    head = row.receipt["head"]
    for index, file in enumerate(files):
        if index < row.receipt.get("files_done", 0):
            continue
        head = api.add(branch, file, index, head)
        worker.save(receipt={"head": head, "files_done": index + 1})
    api.diff(row.proposal["base"], head, files)
    pr = api.create_pr(
        branch,
        ("Article Noyaru : " + row.slug)[:160],
        "<!-- noyaru-blog:" + row.id + " -->",
    )
    number = pr.get("number")
    value = api.pr(number, branch, head)
    if value.get("draft") is not True or value.get("state") != "open":
        raise Stop("pr_changed")
    worker.save(
        state="reviewing",
        receipt={
            "number": number,
            "url": "https://github.com/" + c.REPO + "/pull/" + str(number),
            "head": head,
        },
    )


def inspect_pr(worker, *, merge=False):
    # Read an already merged PR even if deployment has changed the local catalogue.
    worker.observation = True
    row = worker.row()
    api = worker.api
    api.repository()
    value = api.pr(row.receipt["number"], row.receipt["branch"], row.receipt["head"])
    if value.get("merged") is True:
        # Observe a manual/existing merge, never infer a new merge mandate.
        if row.intents.get("merge") and not row.intents["merge"].get("done"):
            worker.finish(
                "merge", {"merged": True, "sha": value.get("merge_commit_sha")}
            )
        worker.save(
            state="observing",
            receipt={
                "merge": value.get("merge_commit_sha"),
                "external": not bool(row.intents.get("merge")),
            },
        )
        return
    worker.observation = False
    row = worker.row()
    if value.get("state") != "open":
        raise Stop("pr_changed")
    if api.head() != row.proposal["base"]:
        raise Stop("base_changed")
    api.diff(row.proposal["base"], row.receipt["head"], row.proposal["files"])
    checks = api.checks(row.receipt["number"], row.receipt["head"])
    if not merge:
        worker.save(state="review_ready", receipt={"checks": checks})
        return
    if checks != row.receipt.get("checks"):
        raise Stop("checks_incomplete")
    api.ready(
        row.receipt["number"],
        row.receipt["branch"],
        row.receipt["head"],
        attempted=bool(row.intents.get("ready")),
    )
    if api.head() != row.proposal["base"]:
        raise Stop("base_changed")
    merged = api.merge(
        row.receipt["number"], row.receipt["branch"], row.receipt["head"]
    )
    worker.save(state="observing", receipt={"merge": merged, "external": False})


def observe(worker):
    row = worker.row()
    api = worker.api
    merged = row.receipt.get("merge")
    from_sha = row.proposal["base"]
    # Merge receipt alone is not proof of the content that was actually merged.
    commit = (
        api.get("/git/commits/" + merged)
        if isinstance(merged, str) and re.fullmatch("[a-f0-9]{40}", merged)
        else {}
    )
    if [p.get("sha") for p in commit.get("parents", [])] != [
        from_sha,
        row.receipt["head"],
    ]:
        raise Stop("deployment_changed")
    api.diff(from_sha, merged, row.proposal["files"])
    observed = []
    for file in row.proposal["files"]:
        worker.guard("read")
        try:
            with requests.get(
                file["url"],
                timeout=(3, 12),
                allow_redirects=False,
                stream=True,
                headers={"Cache-Control": "no-cache"},
            ) as response:
                if (
                    response.status_code != 200
                    or response.headers.get("X-Noyaru-Article-SHA256") != file["sha256"]
                ):
                    raise Stop("deploy_pending")
                release_sha = response.headers.get("X-Noyaru-Release", "")
                if not re.fullmatch("[a-f0-9]{40}", release_sha):
                    raise Stop("deploy_pending")
        except Stop:
            raise
        except Exception:
            raise Stop("deploy_pending") from None
        if release_sha != merged:
            ancestor = api.get("/compare/" + merged + "..." + release_sha)
            if (ancestor.get("merge_base_commit") or {}).get("sha") != merged:
                raise Stop("deployment_changed")
        item = api.content(file["path"], release_sha)
        if not item or item.get("type") != "file" or item.get("sha") != file["blob"]:
            raise Stop("deployment_changed")
        observed.append(
            {"url": file["url"], "release": release_sha, "sha256": file["sha256"]}
        )
    worker.save(
        state="published",
        receipt={"observed": observed, "observed_at": now().isoformat()},
    )


def run(ident):
    with s.module().DB.session() as db:
        row = db.get(Publication, ident)
        if not row:
            return
        owner = row.owner_id
    try:
        worker = claim(owner, ident)
    except ValueError as exc:
        if str(exc) != "busy":
            with s.module().DB.session() as db:
                s.locked(db)
                live = db.get(Publication, ident)
                if (
                    live
                    and live.state in RUNNABLE
                    and (not live.lease_until or w.utc(live.lease_until) <= now())
                ):
                    live.state = "needs_review"
                    live.error_code = (
                        str(exc)
                        if str(exc)
                        in {"expired", "paused", "draft_required", "connection_changed"}
                        else "changed"
                    )
                    audit(db, live, "stopped")
                    db.commit()
        return
    error = None
    try:
        row = worker.row()
        phase = row.receipt.get("phase")
        if row.state == "preparing" or (
            row.state == "uncertain" and phase == "prepare"
        ):
            prepare_pr(worker)
        elif row.state == "publishing" or (
            row.state == "uncertain" and phase == "publish"
        ):
            inspect_pr(worker, merge=True)
        elif row.state == "reviewing":
            inspect_pr(worker)
        elif row.state == "observing":
            observe(worker)
    except ValueError as exc:
        error = str(exc)
    except Exception:
        error = "github_uncertain"
    finally:
        release(worker, error)


def tick():
    with s.module().DB.session() as db:
        ident = db.scalar(
            select(Publication.id)
            .where(
                Publication.state.in_(RUNNABLE),
                or_(
                    Publication.next_check_at.is_(None),
                    Publication.next_check_at <= now(),
                ),
                or_(
                    Publication.lease_until.is_(None), Publication.lease_until <= now()
                ),
            )
            .order_by(Publication.created_at)
            .limit(1)
        )
    if ident:
        run(ident)
    return int(bool(ident))


def start():
    global _THREAD
    if s.module()._env_bool("SEO_AGENT_DISABLE_SCHEDULERS"):
        return
    with _LOCK:
        if _THREAD and _THREAD.is_alive():
            return
        _STOP.clear()

        def loop():
            while not _STOP.wait(30):
                try:
                    tick()
                except Exception:
                    pass  # No provider payload/URL or token is logged.

        _THREAD = threading.Thread(
            target=loop, name="own-blog-publication", daemon=True
        )
        _THREAD.start()


def stop():
    _STOP.set()
