"""Upload only an article's authorized local bytes, once, with a durable intent."""
from __future__ import annotations

import hashlib

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from . import cms_consent
from .cms_commun import CmsError
from .cms_publication import PublicationError, digest, require_actor
from .models import CmsMediaTransfer, IssueTask


def image_url(database, *, project_id, task_id, platform, connection_fingerprint,
              name, data, transport, approved=False, actor_id=None):
    key = digest({"task": task_id, "platform": platform, "connection": connection_fingerprint,
                  "name": name, "sha": hashlib.sha256(data).hexdigest()})
    with database.session() as db:
        task = db.get(IssueTask, task_id)
        if task is None or task.project_id != project_id:
            raise PublicationError("not_found", "Article introuvable dans ce projet.")
        previous = db.scalar(select(CmsMediaTransfer).where(CmsMediaTransfer.project_id == project_id,
                                                           CmsMediaTransfer.media_key == key))
        if previous:
            if previous.state == "ready" and previous.remote_url:
                return previous.remote_url
            raise PublicationError("media_uncertain", "Vérifiez le transfert déjà commencé dans votre CMS avant de le relancer.")
        if not approved:
            raise PublicationError("media_approval", "Confirmez le transfert des images de cet article vers votre CMS.")
        if platform not in {"wordpress", "ghost"}:
            raise PublicationError("media_unsupported", "Le transfert de ces images privées n’est pas encore disponible pour ce CMS. Utilisez des images déjà hébergées dans votre site.")
        project, actor = require_actor(db, project_id, actor_id)
        transfer = CmsMediaTransfer(project_id=project_id, task_id=task_id, media_key=key,
                                    platform=platform, source_name=name, state="uploading",
                                    approved_by=actor_id, consent_digest=key)
        db.add(transfer)
        try:
            db.flush()
            receipt = cms_consent.capture(db, project=project, actor=actor,
                receipt_id=cms_consent.media_receipt_id(transfer.id), operation="media_upload",
                content_digest=hashlib.sha256(data).hexdigest(),
                targets={"platform": platform, "media_key": key})
            db.commit()
        except IntegrityError:
            db.rollback()
            raise PublicationError("media_busy", "Ce transfert a déjà commencé. Actualisez le suivi.") from None
        transfer_id = transfer.id
    try:
        # Noyaru's article assets have already been normalized to WebP by the editor.
        result = transport.upload_image(data=data, filename=name, content_type="image/webp")
        if not result.get("url"):
            raise CmsError("Transfert non confirmé.", uncertain=True)
        with database.session() as db:
            transfer = db.get(CmsMediaTransfer, transfer_id)
            transfer.state, transfer.remote_url = "ready", result["url"]
            transfer.remote_id = str(result.get("id") or "")
            cms_consent.record_result(db, receipt.id, remote_id=transfer.remote_id, remote_url=transfer.remote_url)
            db.commit()
        return result["url"]
    except (CmsError, OSError, TimeoutError) as error:
        uncertain = not isinstance(error, CmsError) or bool(error.uncertain)
        with database.session() as db:
            db.get(CmsMediaTransfer, transfer_id).state = "uncertain" if uncertain else "failed"
            db.commit()
        raise PublicationError("media_uncertain" if uncertain else "media_failed",
                "Le transfert n’a pas pu être confirmé. Vérifiez les médias de votre CMS avant de recommencer."
                if uncertain else "Le CMS n’a pas accepté cette image. Vérifiez son format et les droits de connexion.") from None
