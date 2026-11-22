"""Durable publication workflow. A saved draft, an accepted publish and a live page differ."""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import datetime, timezone
from urllib.parse import urlsplit

from bs4 import BeautifulSoup
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

from . import cms_consent
from .cms_commun import CmsError
from .cms_content_adapters import AdapterError, ArticleDraft
from .models import AccountMember, AgentPageEvent, CmsPublication, CmsPublicationConsent, IssueTask, Project, User


class PublicationError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":")).encode("utf-8")).hexdigest()


def clean_html(value):
    """Refuse executable or private media content instead of publishing a broken approximation."""
    value = str(value or "")
    if not value.strip() or len(value.encode("utf-8")) > 350_000:
        raise PublicationError("content", "Le contenu est vide ou trop volumineux.")
    soup = BeautifulSoup(value, "html.parser")
    for node in soup.find_all(True):
        if node.name in {"script", "iframe", "object", "embed", "form", "input", "style", "svg", "math", "base", "link", "meta"}:
            raise PublicationError("unsafe_html", "Le contenu comporte un élément qui ne peut pas être publié.")
        for key, attribute in node.attrs.items():
            if key.lower().startswith("on") or key.lower() in {"srcdoc", "style"}:
                raise PublicationError("unsafe_html", "Le contenu comporte un attribut qui ne peut pas être publié.")
            if key.lower() in {"src", "href", "poster", "action", "srcset"}:
                if key.lower() == "srcset":
                    raise PublicationError("media", "Vérifiez les variantes d’images avant publication.")
                target = str(attribute or "").strip()
                try:
                    parts = urlsplit(target)
                    _ = parts.port
                except ValueError:
                    raise PublicationError("unsafe_html", "Une adresse du contenu est invalide.") from None
                if parts.username or parts.password or "\\" in target or any(ord(c) < 32 for c in target):
                    raise PublicationError("unsafe_html", "Une adresse du contenu est invalide.")
                if parts.scheme and parts.scheme.lower() not in {"https", "http", "mailto", "tel"}:
                    raise PublicationError("unsafe_html", "Un protocole du contenu n’est pas autorisé.")
                if node.name == "img" and key.lower() == "src":
                    if parts.scheme != "https" or not parts.hostname or "/api/projects/" in parts.path:
                        raise PublicationError("media_not_uploaded", "Les images doivent être transférées dans votre CMS avant de publier cet article.")
    from .cms_content_adapters.html_safe import validate_html
    try:
        validate_html(str(soup))
    except AdapterError as error:
        raise PublicationError("unsafe_html", "Le contenu comporte un élément ou un attribut non pris en charge.") from error
    return str(soup)


def prepare(database, *, project_id, task_id, user_id, platform, language, draft, config, connection_fingerprint):
    payload = asdict(draft)
    payload["html"] = clean_html(payload["html"])
    # Configuration only contains destination IDs/mapping; credentials must never enter this row.
    forbidden = {"token", "password", "key", "secret", "credentials", "authorization"}
    if any(str(k).lower() in forbidden for k in config):
        raise PublicationError("config", "Les identifiants de connexion ne font pas partie de l’aperçu.")
    data = {"article": payload, "config": config, "connection": connection_fingerprint}
    intent = digest({"task": task_id, "platform": platform, "language": language, **data})
    with database.session() as db:
        task = db.get(IssueTask, task_id)
        if task is None or task.project_id != project_id:
            raise PublicationError("not_found", "Article introuvable dans ce projet.")
        existing = db.scalar(select(CmsPublication).where(CmsPublication.project_id == project_id,
                                                         CmsPublication.intent_key == intent))
        if existing:
            # A new, explicit preview request may retry a certain refusal with no remote ID.
            # An interrupted/ambiguous creation remains blocked, never silently retried.
            if existing.state == "failed" and existing.remote_id is None:
                db.execute(update(CmsPublication).where(CmsPublication.id == existing.id,
                            CmsPublication.state == "failed", CmsPublication.remote_id.is_(None))
                            .values(state="prepared", error_code=None))
                db.commit()
                db.refresh(existing)
            return existing
        row = CmsPublication(project_id=project_id, task_id=task_id, created_by=user_id,
                             platform=platform, language=language, intent_key=intent,
                             state="prepared", data=data)
        db.add(row)
        try:
            db.commit()
            return row
        except IntegrityError:
            db.rollback()
            existing = db.scalar(select(CmsPublication).where(CmsPublication.project_id == project_id,
                                                               CmsPublication.intent_key == intent))
            if existing is None:
                raise PublicationError("storage", "La demande de publication n’a pas pu être enregistrée.") from None
            return existing


def load(database, *, project_id, publication_id):
    with database.session() as db:
        row = db.get(CmsPublication, publication_id)
        if row is None or row.project_id != project_id:
            raise PublicationError("not_found", "Publication introuvable.")
        return row


def require_actor(db, project_id, actor_id):
    project = db.get(Project, project_id)
    if not actor_id or not project or db.get(User, actor_id) is None:
        raise PublicationError("approval_actor", "Une session authentifiée est nécessaire pour cet accord.")
    if actor_id != project.owner_user_id and db.scalar(select(AccountMember.member_user_id).where(
            AccountMember.owner_user_id == project.owner_user_id, AccountMember.member_user_id == actor_id)) is None:
        raise PublicationError("approval_actor", "Cet accord ne concerne pas un compte autorisé sur ce projet.")
    return project, db.get(User, actor_id)


def _claim(database, row, expected, running, operation, actor_id):
    with database.session() as db:
        project, actor = require_actor(db, row.project_id, actor_id)
        receipt = cms_consent.capture(db, project=project, actor=actor, publication_id=row.id,
            operation=operation, content_digest=digest(row.data["article"]),
            targets={"platform": row.platform, "language": row.language,
                     "destination": cms_consent.destination(row.data.get("config") or {}),
                     "slug": row.data["article"]["slug"], "remote_id": row.remote_id,
                     "remote_url": cms_consent.public_url(row.remote_url)})
        if row.state != expected:
            raise PublicationError("state", "Cette opération a déjà commencé. Actualisez son suivi avant toute nouvelle demande.")
        data = {**row.data, "last_operation": operation, "last_consent_id": receipt.id, "write_phase": "writing"}
        data.pop("publish_receipt_revision", None)
        data.pop("publish_accepted_at", None)
        _advance(db, row, state=running, error_code=None, data=data)
        db.commit()
        return receipt.id


def _advance(db, row, **values):
    """Apply a change only to the state and write attempt the caller observed."""
    result = db.execute(update(CmsPublication).where(
        CmsPublication.id == row.id, CmsPublication.project_id == row.project_id,
        CmsPublication.state == row.state,
        CmsPublication.data["last_consent_id"].as_string().is_not_distinct_from(row.data.get("last_consent_id")),
        CmsPublication.data["write_phase"].as_string().is_not_distinct_from(row.data.get("write_phase")),
        CmsPublication.remote_id.is_not_distinct_from(row.remote_id),
        CmsPublication.remote_revision.is_not_distinct_from(row.remote_revision),
        CmsPublication.remote_url.is_not_distinct_from(row.remote_url),
    ).values(**values))
    if result.rowcount != 1:
        raise PublicationError("state", "Le suivi a changé pendant l’opération. Actualisez-le.")


def _running_attempt(db, row, running, receipt_id):
    saved = db.get(CmsPublication, row.id)
    if saved is None or saved.state != running or saved.data.get("last_consent_id") != receipt_id:
        raise PublicationError("state", "Le suivi a changé pendant l’opération. Actualisez-le.")
    return saved


def execute(database, *, project_id, publication_id, operation, adapter, connection_fingerprint,
            current_article_digest, approved=False, actor_id=None):
    row = load(database, project_id=project_id, publication_id=publication_id)
    if not approved:
        raise PublicationError("approval", "Relisez et confirmez cette opération.")
    if row.data["connection"] != connection_fingerprint:
        raise PublicationError("connection_changed", "La connexion a changé. Préparez un nouvel aperçu.")
    if current_article_digest != digest(row.data["article"]):
        raise PublicationError("content_changed", "L’article a changé. Préparez un nouvel aperçu.")
    if operation not in {"create", "publish"}:
        raise PublicationError("operation", "Opération inconnue.")
    expected, running = ("prepared", "creating") if operation == "create" else ("draft", "publishing")
    receipt_id = _claim(database, row, expected, running, operation, actor_id)
    remote = None
    try:
        if operation == "create":
            payload = dict(row.data["article"])
            payload.pop("images", None)  # Inline image URLs are already present in the approved HTML.
            remote = adapter.create_draft(ArticleDraft(**payload))
            if not remote.id or not remote.revision:
                raise AdapterError("Le CMS n’a pas confirmé l’identité et la version du brouillon.", code="identity", uncertain=True)
            expected_id, expected_revision = remote.id, remote.revision
            # Store the returned identity before the read-back; a failed read must not lose it.
            with database.session() as db:
                saved = _running_attempt(db, row, running, receipt_id)
                _advance(db, saved, remote_id=remote.id, remote_revision=remote.revision, remote_url=remote.url,
                         data={**saved.data, "write_phase": "answered"})
                cms_consent.record_result(db, receipt_id, remote_id=remote.id, remote_url=remote.url)
                db.commit()
            remote = adapter.read(remote.id)
            if remote.status != "draft" or remote.id != expected_id or remote.revision != expected_revision:
                raise AdapterError("Le CMS n’a pas confirmé le brouillon.", code="draft_unconfirmed", uncertain=True)
        else:
            if not row.remote_id or not row.remote_revision:
                raise AdapterError("La version du brouillon est inconnue.", code="revision")
            remote = adapter.publish(row.remote_id, expected_revision=row.remote_revision)
            if remote.id != row.remote_id or not remote.revision:
                raise AdapterError("Le CMS n’a pas confirmé l’identité de l’article.", code="identity", uncertain=True)
            with database.session() as db:
                saved = _running_attempt(db, row, running, receipt_id)
                accepted_at = datetime.now(timezone.utc)
                _advance(db, saved, remote_revision=remote.revision, remote_url=remote.url,
                         data={**saved.data, "write_phase": "answered", "publish_receipt_revision": remote.revision,
                               "publish_accepted_at": accepted_at.isoformat()})
                expected_revision = remote.revision
                cms_consent.record_result(db, receipt_id, remote_id=remote.id, remote_url=remote.url)
                db.commit()
            remote = adapter.read(remote.id)
            if remote.status != "published" or remote.id != row.remote_id or remote.revision != expected_revision:
                raise AdapterError("La publication n’est pas confirmée.", code="publish_unconfirmed", uncertain=True)
        with database.session() as db:
            saved = _running_attempt(db, row, running, receipt_id)
            _advance(db, saved, state="draft" if operation == "create" else "published",
                     remote_id=remote.id, remote_revision=remote.revision, remote_url=remote.url)
            if operation == "publish":
                db.add(AgentPageEvent(project_id=project_id, event_key="cms:" + row.id + ":published",
                                     page_url=remote.url or "", kind="publication_accepted", created_at=accepted_at,
                                     data={"publication_id": row.id, "platform": row.platform,
                                           "public_page_verified": False}))
            db.commit()
    except (AdapterError, CmsError, OSError, TimeoutError) as error:
        with database.session() as db:
            saved = _running_attempt(db, row, running, receipt_id)
            # Once a write was sent, uncertainty is conservative even when a read-back fails.
            uncertain = bool(getattr(error, "uncertain", False) or remote is not None
                             or getattr(error, "code", "") in {"unreadable", "identity", "partial", "server"}
                             or isinstance(error, (OSError, TimeoutError)))
            _advance(db, saved, state="uncertain" if uncertain else "failed",
                     error_code=str(getattr(error, "code", "") or "remote_error")[:80])
            db.commit()
        raise PublicationError("uncertain" if uncertain else "remote_error",
                               "Le résultat doit être vérifié dans le CMS avant toute nouvelle tentative."
                               if uncertain else "Le CMS a refusé l’opération. Vérifiez la connexion et ses droits.") from None
    return load(database, project_id=project_id, publication_id=publication_id)


def _write_answered(database, row):
    phase = row.data.get("write_phase")
    if phase is not None:
        return phase == "answered"
    # Before phase tracking, a create only got an ID after its write returned.
    if row.state == "creating":
        return bool(row.remote_id and row.remote_revision)
    # A publish already had an ID. Its own receipt must show that it got a response.
    if row.data.get("publish_receipt_revision") != row.remote_revision or not row.data.get("publish_accepted_at"):
        return False
    receipt_id = row.data.get("last_consent_id")
    if not receipt_id:
        return False
    with database.session() as db:
        receipt = db.get(CmsPublicationConsent, receipt_id)
        return bool(receipt and receipt.publication_id == row.id and receipt.operation == "publish"
                    and (receipt.targets.get("result") or {}).get("remote_id") == row.remote_id)


def reconcile(database, *, project_id, publication_id, adapter):
    """A read-only remote check can recover a known ID, never create another article."""
    row = load(database, project_id=project_id, publication_id=publication_id)
    if row.state not in {"creating", "publishing", "uncertain", "failed", "draft", "published", "published_unverified"} or not row.remote_id:
        raise PublicationError("unresolved", "L’identifiant du résultat n’est pas encore confirmé. Vérifiez directement dans votre CMS.")
    try:
        remote = adapter.read(row.remote_id)
    except (AdapterError, CmsError, OSError):
        raise PublicationError("read_unavailable", "Le CMS ne permet pas encore de confirmer ce résultat.") from None
    if remote.id != row.remote_id or remote.status not in {"draft", "published"}:
        raise PublicationError("unresolved", "Le CMS ne confirme pas encore un brouillon ou une publication.")
    if row.state in {"creating", "publishing"} and not _write_answered(database, row):
        message = ("Le CMS indique un brouillon. L’écriture en cours reste à confirmer."
                   if remote.status == "draft" else
                   "Le CMS indique une publication. L’écriture en cours reste à confirmer.")
        raise PublicationError("write_pending", message)
    if remote.status == "draft" and (
            row.state in {"publishing", "published", "published_unverified"}
            or (row.state == "uncertain" and row.data.get("last_operation") != "create")):
        raise PublicationError("write_pending", "Le CMS indique un brouillon. La publication précédente reste à vérifier dans votre CMS.")
    if remote.status == "draft" and remote.revision != row.remote_revision:
        raise PublicationError("remote_changed", "Le brouillon a changé dans le CMS. Relisez-le et poursuivez directement dans votre CMS.")
    confirmed = bool(remote.status == "published" and (
        row.state == "published" and remote.revision == row.remote_revision
        or row.data.get("publish_receipt_revision") == remote.revision))
    with database.session() as db:
        state = "draft" if remote.status == "draft" else "published" if confirmed else "published_unverified"
        _advance(db, row, state=state, remote_url=remote.url,
                 remote_revision=remote.revision if confirmed else row.remote_revision, error_code=None)
        cms_consent.record_result(db, row.data.get("last_consent_id"), remote_id=remote.id, remote_url=remote.url)
        if confirmed:
            key = "cms:" + row.id + ":published"
            if db.scalar(select(AgentPageEvent.id).where(AgentPageEvent.project_id == project_id,
                                                        AgentPageEvent.event_key == key)) is None:
                stamp = row.data.get("publish_accepted_at")
                try:
                    accepted_at = datetime.fromisoformat(stamp)
                    if accepted_at.tzinfo is None:
                        raise ValueError()
                except (TypeError, ValueError):
                    # Never invent an original publication date from a later read-back.
                    accepted_at = None
                if accepted_at is not None:
                    db.add(AgentPageEvent(project_id=project_id, event_key=key, page_url=remote.url,
                        kind="publication_accepted", created_at=accepted_at,
                        data={"publication_id": row.id, "platform": row.platform,
                              "public_page_verified": False}))
        db.commit()
    return load(database, project_id=project_id, publication_id=publication_id)
