"""One attempted support email, fixed recipient and transport, durable acceptance receipt."""

import re
from datetime import timedelta
from sqlalchemy import select, func, or_, and_

try:
    from . import (
        own_support_common as c,
        own_support_message as msg,
        own_support_writing as writing,
    )
except ImportError:
    import own_support_common as c
    import own_support_message as msg
    import own_support_writing as writing


def reserved(db, owner):
    return (
        db.scalar(
            select(func.count())
            .select_from(c.Ticket)
            .where(
                c.Ticket.owner_id == owner,
                or_(
                    c.Ticket.send_intent_at >= c.now() - timedelta(hours=24),
                    and_(
                        c.Ticket.state.in_({"send_queued", "sending"}),
                        c.Ticket.send_intent_at.is_(None),
                        c.Ticket.authorized_until > c.now(),
                    ),
                ),
            )
        )
        or 0
    )


def sender(db, owner):
    cfg = c.channel(db, owner, active=False)
    if cfg.sender_credentials:
        value = c.reveal(cfg.sender_credentials)
    else:
        m = c.module()
        config = m._mail_config()
        if not config:
            raise c.Stop("sender_required")
        provider, key = m._mail_api_transport(config)
        value = {
            "provider": provider,
            "key": key,
            "from": str(config.get("from", "")).casefold(),
            "name": "Noyaru",
        }
    if (
        value.get("provider") not in {"brevo", "resend", "sendgrid"}
        or not value.get("key")
        or value.get("from") != c.CONTACT
    ):
        raise c.Stop("sender_required")
    if any(ch in value["key"] for ch in "\r\n\x00"):
        raise c.Stop("sender_required")
    return value, c.s.digest(["support-sender-v1", value])


def configure(owner, provider, key, confirm):
    if not confirm:
        raise c.Stop("confirm_required")
    if (
        provider not in {"brevo", "resend", "sendgrid"}
        or not 8 <= len(key) <= 1000
        or any(ord(ch) < 33 for ch in key)
    ):
        raise c.Stop("sender_required")
    value = c.seal(
        {"provider": provider, "key": key, "from": c.CONTACT, "name": "Noyaru"}
    )
    with c.module().DB.session() as db:
        c.s.locked(db)
        cfg = c.channel(db, owner, active=False)
        cfg.sender_credentials = value
        c.audit(db, owner, "sender_configured", meta={"provider": provider})
        db.commit()


def manifest(row):
    payload = c.reveal(row.payload_encrypted)
    draft = c.reveal(row.draft_encrypted)
    if c.s.digest(payload) != row.input_hash or c.s.digest(draft) != row.draft_hash:
        raise c.Stop("changed")
    if msg.addresses(payload["from"], single=True) != [payload["from"]]:
        raise c.Stop("sender_unconfirmed")
    writing.validate(draft, {"sources": row.dossier["sources"]})
    sources = {s["id"]: s for s in row.dossier["sources"]}
    en = row.lang == "en"
    text = (
        draft["body"]
        + "\n\n"
        + ("Useful documentation:" if en else "Documentation utile :")
    )
    for ident in draft["sources"]:
        source = sources[ident]
        expected = "https://noyaru.com" + ("/en" if en else "") + "/docs/" + ident
        if source["url"] != expected:
            raise c.Stop("sources_changed")
        text += "\n" + source["title"] + " : " + source["url"]
    text += "\n\n" + (
        "Noyaru support\nReply to this email to continue your support request."
        if en
        else "Assistance Noyaru\nRépondez à cet email pour poursuivre votre demande d’assistance."
    )
    return {
        "from": c.CONTACT,
        "name": "Noyaru",
        "to": payload["from"],
        "subject": "Re: " + payload["subject"],
        "text": text,
        "reply_to": c.CONTACT,
        "in_reply_to": payload["message_id"],
    }


def send(worker):
    row = worker.row()
    if row.send_intent_at:
        # The provider may have accepted the first call. Never fall back or replay it.
        raise c.Stop("send_uncertain")
    worker.verify_inbox()
    with c.module().DB.session() as db:
        c.s.locked(db)
        _, live = c.ticket(db, row.owner_id, row.id)
        worker.check_lease(live)
        if (
            live.state != "sending"
            or not live.authorized_until
            or c.utc(live.authorized_until) <= c.now()
        ):
            raise c.Stop("expired")
        if not writing.current(db, live):
            raise c.Stop("sources_changed")
        config, fingerprint = sender(db, row.owner_id)
        if fingerprint != live.sender_hash:
            raise c.Stop("sender_changed")
        if live.send_intent_at:
            raise c.Stop("send_uncertain")
        if reserved(db, row.owner_id) > c.DAILY_SENDS:
            raise c.Stop("budget")
        message = c.reveal(live.outbox_encrypted)
        if message != manifest(live):
            raise c.Stop("changed")
        # This immutable marker precedes the only HTTP attempt, including process crashes.
        live.send_intent_at = c.now()
        live.receipt = {
            "manifest_hash": c.s.digest(message),
            "provider": config["provider"],
        }
        c.audit(db, row.owner_id, "send_intent", row.id)
        db.commit()
    worker.row()  # Recheck revocation/lease immediately before transport; no second effect.
    provider = config["provider"]
    key = config["key"]
    mail_headers = {
        "In-Reply-To": message["in_reply_to"],
        "References": message["in_reply_to"],
        "Auto-Submitted": "auto-replied",
        "X-Auto-Response-Suppress": "All",
    }
    if provider == "brevo":
        body = {
            "sender": {"email": c.CONTACT, "name": "Noyaru"},
            "to": [{"email": message["to"]}],
            "replyTo": {"email": c.CONTACT},
            "subject": message["subject"],
            "textContent": message["text"],
            "headers": {
                "X-Noyaru-SAV-ID": row.id,
                "IdempotencyKey": "noyaru-support-" + row.id,
            },
        }
        status, value, headers = c.http(
            "POST",
            "https://api.brevo.com/v3/smtp/email",
            headers={"api-key": key},
            body=body,
            statuses=(201,),
        )
        receipt = value.get("messageId")
    elif provider == "resend":
        body = {
            "from": "Noyaru <" + c.CONTACT + ">",
            "to": [message["to"]],
            "reply_to": c.CONTACT,
            "subject": message["subject"],
            "text": message["text"],
            "headers": mail_headers,
        }
        status, value, headers = c.http(
            "POST",
            "https://api.resend.com/emails",
            headers={
                "Authorization": "Bearer " + key,
                "Idempotency-Key": "noyaru-support/" + row.id + "/" + row.draft_hash,
            },
            body=body,
            statuses=(200,),
        )
        receipt = value.get("id")
    else:
        body = {
            "personalizations": [{"to": [{"email": message["to"]}]}],
            "from": {"email": c.CONTACT, "name": "Noyaru"},
            "reply_to": {"email": c.CONTACT},
            "subject": message["subject"],
            "content": [{"type": "text/plain", "value": message["text"]}],
            "headers": mail_headers,
        }
        status, value, headers = c.http(
            "POST",
            "https://api.sendgrid.com/v3/mail/send",
            headers={"Authorization": "Bearer " + key},
            body=body,
            statuses=(202,),
        )
        receipt = next(
            (v for k, v in headers.items() if k.casefold() == "x-message-id"), None
        )
    if not isinstance(receipt, str) or not re.fullmatch(
        r"[A-Za-z0-9_@.<>+:/=-]{4,240}", receipt
    ):
        raise c.Stop("send_uncertain")
    with c.module().DB.session() as db:
        c.s.locked(db)
        _, live = c.ticket(db, row.owner_id, row.id, active=False)
        worker.check_lease(live)
        live.receipt = {
            **live.receipt,
            "id": receipt,
            "accepted_at": c.now().isoformat(),
            "status": status,
            "delivered": False,
        }
        live.state = "accepted"
        live.error_code = None
        c.audit(db, row.owner_id, "send_accepted", row.id)
        db.commit()
