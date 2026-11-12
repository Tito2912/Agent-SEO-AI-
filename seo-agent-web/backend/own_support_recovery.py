"""Explicit reread of one known, unread request after a human fixes its Gmail labels."""

try:
    from . import own_support_common as c, own_support_gmail as gmail
except ImportError:
    import own_support_common as c
    import own_support_gmail as gmail


def eligible(row):
    return (
        row.state == "ignored"
        and row.reason == "outside_label"
        and not any(
            (
                row.provider_calls,
                row.provider_started_at,
                row.draft_encrypted,
                row.draft_hash,
                row.outbox_encrypted,
                row.send_intent_at,
                row.authorized_until,
                row.sender_hash,
                row.dossier,
                row.receipt,
            )
        )
    )


def checked(db, owner, ident):
    cfg, row = c.ticket(db, owner, ident)
    if row.binding != cfg.binding or not eligible(row):
        raise c.Stop("recheck_unavailable")
    if row.lease_until and c.utc(row.lease_until) > c.now():
        raise c.Stop("busy")
    if c.reveal(row.payload_encrypted) != {} or row.input_hash != c.s.digest({}):
        raise c.Stop("changed")
    return cfg, row


def version(row):
    return c.s.digest([row.binding, row.id, row.revision, row.input_hash])


def recheck(owner, ident, confirm):
    if not confirm:
        raise c.Stop("confirm_required")
    with c.module().DB.session() as db:
        c.s.locked(db)
        _, row = checked(db, owner, ident)
        expected = version(row)

    reader = gmail.claim(owner)
    error = None
    try:
        with c.module().DB.session() as db:
            c.s.locked(db)
            cfg, row = checked(db, owner, ident)
            if version(row) != expected or cfg.binding != reader.binding:
                raise c.Stop("changed")
            if gmail.intake_reserved(db, owner) >= c.DAILY_IMPORT:
                raise c.Stop("import_limit")
            google_id = row.google_id

        # Same metadata/MIME/body gates as initial intake. No search or Gmail write.
        meta, payload, reason, received = reader.incoming(google_id, cfg)

        with c.module().DB.session() as db:
            c.s.locked(db)
            cfg, row = checked(db, owner, ident)
            if (
                cfg.binding != reader.binding
                or cfg.lease_token != reader.lease
                or not cfg.lease_until
                or c.utc(cfg.lease_until) <= c.now()
                or version(row) != expected
                or meta.get("id") != google_id
            ):
                raise c.Stop("changed")
            if gmail.intake_reserved(db, owner) >= c.DAILY_IMPORT:
                raise c.Stop("import_limit")
            row.thread_id = meta["threadId"]
            row.received_at = received
            row.payload_encrypted = c.seal(payload)
            row.input_hash = c.s.digest(payload)
            row.lang = (
                "en"
                if payload.get("text", "")
                .lower()
                .startswith(("hello", "hi ", "how ", "i "))
                else "fr"
            )
            row.state = "review_required" if reason else "received"
            row.reason = reason
            row.error_code = None
            row.revision += 1
            # Keep created_at: rereading must never extend private-copy retention.
            c.audit(db, owner, "recheck", row.id, {"state": row.state})
            db.commit()
    except c.Stop as exc:
        if str(exc) in {
            "connection_denied",
            "connection_required",
            "scope_changed",
            "gmail_reconnect_required",
            "oauth_client_rejected",
            "label_changed",
            "mailbox_changed",
            "not_found",
            "cursor_changed",
            "read_limit",
            "provider_refused",
            "rate_limited",
            "transport_uncertain",
        }:
            error = str(exc)
        raise
    finally:
        gmail.release(reader, error)
