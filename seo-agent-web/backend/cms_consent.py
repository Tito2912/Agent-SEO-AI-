"""Minimal CMS approval evidence; no article body, asset bytes or connection secret.

New evidence is captured before writing. Legacy snapshots are explicitly marked as
backfilled from current records, never misrepresented as historical copies.
"""
from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import NAMESPACE_URL, uuid4, uuid5

from sqlalchemy import delete, select

try:
    from .models import CmsPublicationConsent
except ImportError:  # uvicorn app:app from backend/
    from models import CmsPublicationConsent  # type: ignore

CONSERVATION_ANS = 5


def public_url(value):
    """Keep a public target, stripping fragments and preview/token query strings."""
    try:
        value = str(value or "")
        parts = urlsplit(value)
        if (parts.scheme not in {"http", "https"} or not parts.hostname
                or parts.username or parts.password or "\\" in value
                or any(ord(c) < 32 for c in value)):
            return None
        _ = parts.port
        # CMS query IDs can identify a page; arbitrary preview/access parameters cannot stay.
        query = urlencode([(k, v) for k, v in parse_qsl(parts.query)
                           if (k in {"id_cms", "id_lang", "p", "page_id"} and v.isdigit()) or (k == "controller" and v == "cms")])
        clean = urlunsplit((parts.scheme, parts.netloc, parts.path, query, ""))
        return clean if len(clean) <= 2048 else None
    except ValueError:
        return None


def destination(config):
    """Retain IDs/mapping only, not display labels or arbitrary connection configuration."""
    out = {key: str(config[key]) for key in ("blog_id", "collection_id", "id_shop", "id_lang", "id_cms_category")
           if config.get(key) is not None}
    mapping = config.get("field_mapping")
    if isinstance(mapping, dict):
        out["field_mapping"] = {key: str(mapping[key]) for key in ("title", "slug", "body", "excerpt") if key in mapping}
    return out


def media_receipt_id(transfer_id):
    return str(uuid5(NAMESPACE_URL, "noyaru:cms-media:" + str(transfer_id)))


def capture(db, *, project, actor, operation, content_digest, targets, publication_id=None, receipt_id=None):
    approved_at = datetime.now(timezone.utc)
    receipt = CmsPublicationConsent(
        id=receipt_id or str(uuid4()), project_id=project.id, publication_id=publication_id,
        owner_user_id=project.owner_user_id, actor_id=actor.id, actor_email=actor.email,
        site_url=public_url(project.base_url), snapshot_source="captured", operation=operation,
        content_digest=content_digest, targets=targets, approved_at=approved_at, expires_at=expiration(approved_at))
    db.add(receipt)
    return receipt


def record_result(db, receipt_id, *, remote_id, remote_url):
    """Append an observed identity; never change the original approved targets or date."""
    if not receipt_id:
        return
    receipt = db.get(CmsPublicationConsent, receipt_id)
    if receipt is None:  # The receipt may already have reached its retention limit.
        return
    ident, url = str(remote_id or "")[:2048], public_url(remote_url)
    previous = receipt.targets.get("result")
    if isinstance(previous, dict):
        # A later read cannot rewrite the original returned identity/date/address.
        if previous.get("remote_id") == ident and not previous.get("url") and url:
            receipt.targets = {**receipt.targets, "result": {
                **previous, "url": url, "url_observed_at": datetime.now(timezone.utc).isoformat()}}
        return
    receipt.targets = {**receipt.targets, "result": {
        "remote_id": ident, "url": url, "observed_at": datetime.now(timezone.utc).isoformat()}}


def utc(instant):
    return instant.replace(tzinfo=timezone.utc) if instant.tzinfo is None else instant.astimezone(timezone.utc)


def expiration(approved_at):
    instant = utc(approved_at)
    try:
        return instant.replace(year=instant.year + CONSERVATION_ANS)
    except ValueError:  # Approval on February 29 expires on February 28 five years later.
        return instant.replace(year=instant.year + CONSERVATION_ANS, day=28)


def purger(db, maintenant):
    """Remove evidence at its original anniversary, without extending it on deletion."""
    # Normally filled by capture/migration. Handle an incomplete legacy row without
    # silently retaining it forever or treating a later backfill as a new approval.
    for row in db.scalars(select(CmsPublicationConsent).where(CmsPublicationConsent.expires_at.is_(None))):
        row.expires_at = expiration(row.approved_at)
    db.flush()
    result = db.execute(delete(CmsPublicationConsent).where(CmsPublicationConsent.expires_at <= utc(maintenant)))
    db.commit()
    return int(result.rowcount or 0)
