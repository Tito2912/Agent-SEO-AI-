"""Private owner support primitives. No customer account or site authority."""

from datetime import timedelta
import json
import re

import requests

try:
    from . import own_seo as s, seo_watch
    from .models import (
        OwnSupportChannel as Channel,
        OwnSupportTicket as Ticket,
        AuditLog,
    )
except ImportError:
    import own_seo as s
    import seo_watch
    from models import (
        OwnSupportChannel as Channel,
        OwnSupportTicket as Ticket,
        AuditLog,
    )

KEY = "owner"
PATH = "/settings/system/support"
CONTACT = "contact@noyaru.com"
SCOPE = "https://www.googleapis.com/auth/gmail.readonly"
DAILY_AI = 2
DAILY_IMPORT = 20
DAILY_SENDS = 10
DAILY_READS = 1800
RETENTION = timedelta(days=30)


class Stop(ValueError):
    pass


def module():
    return s.module()


def now():
    return s.clock()


def utc(value):
    return seo_watch.utc(value)


def seal(value):
    m = module()
    if not m._encryption_ready():
        raise Stop("encryption_required")
    result = m._encrypt_secret(json.dumps(value, ensure_ascii=True, sort_keys=True))
    if not result.startswith(m._SECRET_PREFIX):
        raise Stop("encryption_required")
    return result


def reveal(value):
    try:
        raw, _ = module()._decrypt_secret_with_rotation(value or "")
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError()
        return data
    except Exception:
        raise Stop("credentials_unavailable") from None


def audit(db, owner, action, ident=None, meta=None):
    db.add(
        AuditLog(
            actor_user_id=owner,
            action="own_support." + action,
            target_type="own_support",
            target_id=ident,
            meta=meta or {},
        )
    )


def channel(db, owner, *, active=True):
    s.actor(db, owner)
    row = db.get(Channel, KEY)
    if not row or row.owner_id != owner:
        raise Stop("channel_required")
    if active and (not row.active or not row.cursor or not row.label_id):
        raise Stop("paused")
    return row


def ticket(db, owner, ident, *, active=True):
    cfg = channel(db, owner, active=active)
    row = db.get(Ticket, ident)
    if not row or row.owner_id != owner:
        raise Stop("missing")
    if row.created_at and utc(row.created_at) + RETENTION <= now():
        raise Stop("missing")
    if active and row.binding != cfg.binding:
        raise Stop("binding_changed")
    return cfg, row


def http(
    method,
    url,
    *,
    headers=None,
    params=None,
    data=None,
    body=None,
    statuses=(200,),
    limit=200000,
):
    """Callers supply compiled URLs. No redirect, fallback or logged provider payload."""
    try:
        with requests.request(
            method,
            url,
            headers=headers,
            params=params,
            data=data,
            json=body,
            timeout=(3, 12),
            allow_redirects=False,
            stream=True,
        ) as response:
            if response.status_code not in statuses:
                reason = {
                    401: "connection_denied",
                    403: "connection_denied",
                    404: "not_found",
                    429: "rate_limited",
                }.get(response.status_code, "provider_refused")
                raise Stop(reason)
            raw = bytearray()
            for chunk in response.iter_content(32768):
                raw.extend(chunk)
                if len(raw) > limit:
                    raise Stop("response_limit")
            result = json.loads(raw) if raw else {}
            if not isinstance(result, dict):
                raise Stop("provider_unconfirmed")
            return response.status_code, result, dict(response.headers)
    except Stop:
        raise
    except Exception:
        raise Stop("transport_uncertain") from None


def google_id(value):
    if not isinstance(value, str) or not re.fullmatch("[a-f0-9]{1,40}", value):
        raise Stop("message_changed")
    return value


def history_id(value):
    if not isinstance(value, str) or not re.fullmatch("[1-9][0-9]{0,30}", value):
        raise Stop("cursor_changed")
    return value
