"""Upload only an article's authorized local bytes, once, with a durable intent."""
from __future__ import annotations

import hashlib
from uuid import uuid4

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from . import cms_consent
from .cms_commun import CmsError
from .cms_publication import PublicationError, digest, require_actor
from .models import CmsMediaTransfer, IssueTask


def retry_token(transfer):
    """Old rows used the deterministic ID of their first receipt."""
    return transfer.attempt_id or cms_consent.media_receipt_id(transfer.id)


def image_url(database, *, project_id, task_id, platform, connection_fingerprint,
              name, data, transport, approved=False, actor_id=None, retry_attempts=()):
    content_digest = hashlib.sha256(data).hexdigest()
    key = digest({"task": task_id, "platform": platform, "connection": connection_fingerprint,
                  "name": name, "sha": content_digest})
    with database.session() as db:
        task = db.get(IssueTask, task_id)
        if task is None or task.project_id != project_id:
            raise PublicationError("not_found", "Article introuvable dans ce projet.")
        previous = db.scalar(select(CmsMediaTransfer).where(CmsMediaTransfer.project_id == project_id,
                                                           CmsMediaTransfer.media_key == key))
        if previous and previous.state == "ready" and previous.remote_url:
            return previous.remote_url
        if previous and (previous.state != "failed" or previous.remote_id or previous.remote_url):
            raise PublicationError("media_uncertain", "Vérifiez le transfert déjà commencé dans votre CMS.")
        if previous and retry_token(previous) not in retry_attempts:
            raise PublicationError("media_retry_approval", "Actualisez le formulaire et confirmez la reprise du transfert refusé.")
        if previous is None and not approved:
            raise PublicationError("media_approval", "Confirmez le transfert des images de cet article vers votre CMS.")
        if platform not in {"wordpress", "ghost"}:
            raise PublicationError("media_unsupported", "Le transfert de ces images privées n’est pas encore disponible pour ce CMS. Utilisez des images déjà hébergées dans votre site.")
        project, actor = require_actor(db, project_id, actor_id)
        try:
            if previous is None:
                transfer = CmsMediaTransfer(project_id=project_id, task_id=task_id, media_key=key,
                                            platform=platform, source_name=name, state="uploading",
                                            approved_by=actor_id, consent_digest=key)
                db.add(transfer)
                db.flush()
                transfer_id = transfer.id
                attempt_id = transfer.attempt_id = cms_consent.media_receipt_id(transfer_id)
            else:
                transfer_id = previous.id
                attempt_id = str(uuid4())
                result = db.execute(update(CmsMediaTransfer).where(
                    CmsMediaTransfer.id == transfer_id, CmsMediaTransfer.state == "failed",
                    CmsMediaTransfer.remote_id.is_(None), CmsMediaTransfer.remote_url.is_(None),
                    CmsMediaTransfer.attempt_id.is_not_distinct_from(previous.attempt_id),
                ).values(state="uploading", attempt_id=attempt_id, approved_by=actor_id, consent_digest=key))
                if result.rowcount != 1:
                    raise PublicationError("media_busy", "Ce transfert a déjà commencé. Actualisez le suivi.")
            cms_consent.capture(db, project=project, actor=actor,
                receipt_id=attempt_id, operation="media_upload",
                content_digest=content_digest,
                targets={"platform": platform, "media_key": key})
            db.commit()
        except IntegrityError:
            db.rollback()
            raise PublicationError("media_busy", "Ce transfert a déjà commencé. Actualisez le suivi.") from None
        except SQLAlchemyError:
            db.rollback()
            raise PublicationError("media_storage", "L’accord de transfert n’a pas pu être enregistré.") from None
    try:
        # Noyaru's article assets have already been normalized to WebP by the editor.
        result = transport.upload_image(data=data, filename=name, content_type="image/webp")
        if not result.get("url"):
            raise CmsError("Transfert non confirmé.", uncertain=True)
    except (CmsError, OSError, TimeoutError) as error:
        known_id = str(getattr(error, "remote_id", "") or "")
        uncertain = not isinstance(error, CmsError) or bool(error.uncertain) or bool(known_id)
        remote_id = None
        if platform == "wordpress" and known_id.isascii() and known_id.isdigit() and int(known_id) > 0:
            remote_id = known_id
        _finish(database, transfer_id, attempt_id, state="uncertain" if uncertain else "failed", remote_id=remote_id)
        raise PublicationError("media_uncertain" if uncertain else "media_failed",
                "Le transfert n’a pas pu être confirmé. Vérifiez les médias de votre CMS."
                if uncertain else "Le CMS n’a pas accepté cette image. Vérifiez son format et les droits de connexion avant de confirmer un nouvel essai.") from None
    _finish(database, transfer_id, attempt_id, state="ready", remote_id=str(result.get("id") or ""), remote_url=result["url"])
    return result["url"]


def _finish(database, transfer_id, attempt_id, *, state, remote_id, remote_url=None):
    try:
        with database.session() as db:
            result = db.execute(update(CmsMediaTransfer).where(
                CmsMediaTransfer.id == transfer_id, CmsMediaTransfer.state == "uploading",
                CmsMediaTransfer.attempt_id == attempt_id,
            ).values(state=state, remote_id=remote_id, remote_url=remote_url))
            if result.rowcount != 1:
                raise PublicationError("media_busy", "Le suivi du transfert a changé. Actualisez-le.")
            if remote_id or remote_url:
                cms_consent.record_result(db, attempt_id, remote_id=remote_id, remote_url=remote_url)
            db.commit()
    except SQLAlchemyError:
        raise PublicationError("media_uncertain", "Le transfert n’a pas pu être confirmé. Vérifiez les médias de votre CMS.") from None
