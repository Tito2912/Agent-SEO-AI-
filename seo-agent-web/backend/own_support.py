"""Support supervision and closed Telegram actions. No inbound email can grant a mandate."""

from datetime import timedelta
import json
import threading
import uuid

from sqlalchemy import select, or_

try:
    from . import (
        own_support_common as c,
        own_support_gmail as gmail,
        own_support_writing as writing,
        own_support_mail as mail,
    )
except ImportError:
    import own_support_common as c
    import own_support_gmail as gmail
    import own_support_writing as writing
    import own_support_mail as mail

PATH = c.PATH
KINDS = {"support_write", "support_send", "support_review"}
RUNNABLE = {"writing_queued", "writing", "send_queued", "sending"}
_STOP = threading.Event()
_THREAD = None
_LOCK = threading.Lock()


def latest(db, row):
    if db.scalar(
        select(c.Ticket.id)
        .where(
            c.Ticket.binding == row.binding,
            c.Ticket.thread_id == row.thread_id,
            c.Ticket.id != row.id,
            c.Ticket.received_at >= row.received_at,
            c.Ticket.state != "ignored",
        )
        .limit(1)
    ):
        raise c.Stop("thread_changed")


def approval_document(db, owner, ident, kind, lang):
    _, row = c.ticket(db, owner, ident)
    latest(db, row)
    payload = c.reveal(row.payload_encrypted)
    en = lang == "en"
    if c.s.digest(payload) != row.input_hash:
        raise c.Stop("changed")
    expected = {
        "support_write": "received",
        "support_send": "draft",
        "support_review": "review_required",
    }
    uncertain = (
        kind == "support_review"
        and row.state == "send_uncertain"
        and not row.receipt.get("review_ack")
    )
    if kind not in expected or row.state != expected[kind] and not uncertain:
        raise c.Stop("changed")
    body = (
        "Sender identity is not verified. No account access is granted."
        if en
        else "L’identité du correspondant n’est pas vérifiée. Aucun accès au compte n’est accordé."
    )
    body += "\n" + payload.get("from", "") + "\n" + payload.get("subject", "")
    ready = True
    expiry = c.now() + timedelta(hours=24)
    if kind == "support_write":
        packet, fingerprint = writing.bundle(db, owner, ident)
        body += "\n\n" + json.dumps(packet, ensure_ascii=False, indent=2)
        effect = (
            "Authorize one paid AI text request, up to 2 per rolling 24 hours, for this reduced question and these public sources. Private reply only; no email is sent."
            if en
            else "Autoriser un appel IA texte payant, au plus 2 par 24 h glissantes, pour cette question réduite et ces sources publiques. Réponse privée ; aucun email envoyé."
        )
        ready = writing.reserved(db, owner) < c.DAILY_AI and c.module()._ai_configured()
        if not ready:
            effect += "\n" + (
                "AI configuration or daily budget needs attention in Noyaru."
                if en
                else "Vérifier la configuration IA ou le plafond quotidien dans Noyaru."
            )
    elif kind == "support_send":
        if not row.draft_encrypted or not writing.current(db, row):
            raise c.Stop("sources_changed")
        config, sender_hash = mail.sender(db, owner)
        value = mail.manifest(row)
        fingerprint = c.s.digest(
            [
                row.binding,
                row.id,
                row.revision,
                row.input_hash,
                row.draft_hash,
                row.dossier["hash"],
                sender_hash,
                value,
            ]
        )
        body += "\n\n" + json.dumps(
            {k: v for k, v in value.items() if k != "in_reply_to"},
            ensure_ascii=False,
            indent=2,
        )
        body += "\n\n" + config["provider"]
        if config["provider"] == "brevo":
            body += "\n" + (
                "Brevo does not guarantee conversation threading with this API."
                if en
                else "Cette API Brevo ne garantit pas le regroupement dans le fil du destinataire."
            )
        effect = (
            "Send this exact reply once, from contact@noyaru.com to the sender above. No CC/BCC, site change or account action. Provider acceptance is not proof of delivery. Unknown result: no automatic resend."
            if en
            else "Envoyer cette réponse exacte une seule fois, depuis contact@noyaru.com au correspondant ci-dessus. Aucun CC/BCC ni action sur un site ou compte. L’acceptation par le fournisseur ne prouve pas la livraison. Résultat inconnu : aucun renvoi automatique."
        )
        spent = mail.reserved(db, owner)
        ready = spent < c.DAILY_SENDS and not row.send_intent_at
    else:
        fingerprint = c.s.digest(
            [row.binding, row.id, row.revision, row.input_hash, row.reason]
        )
        body += "\n\n" + (row.reason or "unclear")
        effect = (
            "Acknowledge human handling in Noyaru, or reject and ignore this request. No AI call or email."
            if en
            else "Confirmer la reprise manuelle dans Noyaru, ou refuser et ignorer cette demande. Aucun appel IA ni email."
        )
        if uncertain:
            effect = (
                "Acknowledge investigation of this uncertain send. Reject dismisses this reminder only. The unknown result is kept; no resend or delivery claim."
                if en
                else "Confirmer l’investigation humaine de cet envoi incertain. Refuser écarte cette relance uniquement. Le résultat reste inconnu ; aucun renvoi ni affirmation de livraison."
            )
    return {
        "kind": kind,
        "target": ident,
        "version": fingerprint,
        "title": payload.get("subject") or ("Support request" if en else "Demande SAV"),
        "text": effect + "\n\n" + body,
        "effect": effect,
        "path": PATH + "?ticket=" + ident,
        "ready": ready,
        "expires_at": expiry,
    }


def decide(db, owner, ident, kind, approve, fingerprint):
    doc = approval_document(db, owner, ident, kind, "fr")
    if doc["version"] != fingerprint or approve and not doc["ready"]:
        raise c.Stop("changed")
    _, row = c.ticket(db, owner, ident)
    if row.lease_until and c.utc(row.lease_until) > c.now():
        raise c.Stop("busy")
    if kind == "support_review" and row.state == "send_uncertain":
        row.receipt = {**row.receipt, "review_ack": True}
    elif not approve:
        row.state = (
            "draft_refused"
            if kind == "support_send"
            else "human"
            if kind == "support_write"
            else "ignored"
        )
        row.lease_token = None
        row.lease_until = None
    elif kind == "support_write":
        writing.authorize(db, owner, ident, fingerprint)
    elif kind == "support_send":
        _, sender_hash = mail.sender(db, owner)
        row.state = "send_queued"
        row.sender_hash = sender_hash
        row.authorized_until = c.now() + timedelta(hours=24)
        row.outbox_encrypted = c.seal(mail.manifest(row))
    else:
        row.state = "human"
    row.next_check_at = c.now()
    c.audit(
        db,
        owner,
        "approve" if approve else "refuse",
        ident,
        {"kind": kind, "version": fingerprint},
    )
    return ident if approve and kind in {"support_write", "support_send"} else None


def approval_pages(row):
    """SAV cards are encrypted at rest and disappear with their 30-day ticket copy."""
    with c.module().DB.session() as db:
        ticket = db.get(c.Ticket, row.target_id)
        if (
            not ticket
            or ticket.owner_id != row.owner_id
            or c.utc(ticket.created_at) + c.RETENTION <= c.now()
        ):
            return ["Données SAV retirées / Support data removed"]
    try:
        return [c.reveal(value)["text"] for value in row.pages]
    except ValueError:
        return ["Données SAV indisponibles / Support data unavailable"]


class Worker:
    def __init__(self, owner, ident, lease):
        self.owner, self.ident, self.lease = owner, ident, lease

    def check_lease(self, row):
        if (
            not row
            or row.lease_token != self.lease
            or not row.lease_until
            or c.utc(row.lease_until) <= c.now()
        ):
            raise c.Stop("lease_ended")

    def row(self):
        with c.module().DB.session() as db:
            _, row = c.ticket(db, self.owner, self.ident)
            self.check_lease(row)
            if (
                row.state not in {"writing", "sending"}
                or not row.authorized_until
                or c.utc(row.authorized_until) <= c.now()
            ):
                raise c.Stop("expired")
            if c.s.digest(c.reveal(row.payload_encrypted)) != row.input_hash:
                raise c.Stop("changed")
            latest(db, row)
            if not writing.current(db, row):
                raise c.Stop("sources_changed")
            if (
                row.state == "sending"
                and mail.sender(db, self.owner)[1] != row.sender_hash
            ):
                raise c.Stop("sender_changed")
            return row

    def verify_inbox(self):
        row = self.row()
        reader = gmail.claim(self.owner)
        error = None
        try:
            cfg, _, _ = reader.verify()
            _, payload, reason, _ = reader.incoming(row.google_id, cfg)
            if (
                reason
                and not row.dossier.get("manual")
                or c.s.digest(payload) != row.input_hash
            ):
                raise c.Stop("message_changed")
            result = reader.get(
                "/history",
                startHistoryId=cfg.cursor,
                labelId=cfg.label_id,
                historyTypes=["messageAdded", "labelAdded"],
                maxResults=10,
                fields="history(messagesAdded(message(id,threadId)),labelsAdded(message(id,threadId))),nextPageToken,historyId",
            )
            if result.get("nextPageToken"):
                raise c.Stop("inbox_sync_required")
            for history in result.get("history", []):
                for change in history.get("messagesAdded", []) + history.get(
                    "labelsAdded", []
                ):
                    msg = change.get("message") or {}
                    if (
                        msg.get("threadId") == row.thread_id
                        and msg.get("id") != row.google_id
                    ):
                        raise c.Stop("thread_changed")
        except c.Stop as exc:
            error = str(exc)
            raise
        finally:
            gmail.release(reader, error)


def claim(ident):
    with c.module().DB.session() as db:
        c.s.locked(db)
        row = db.get(c.Ticket, ident)
        if not row:
            raise c.Stop("missing")
        c.ticket(db, row.owner_id, ident)
        if row.state not in RUNNABLE:
            raise c.Stop("changed")
        if row.lease_until and c.utc(row.lease_until) > c.now():
            raise c.Stop("busy")
        if not row.authorized_until or c.utc(row.authorized_until) <= c.now():
            raise c.Stop("expired")
        if row.send_intent_at:
            raise c.Stop("send_uncertain")
        if row.state == "writing" and row.provider_calls:
            raise c.Stop("provider_uncertain")
        row.state = (
            "writing" if row.state in {"writing", "writing_queued"} else "sending"
        )
        row.lease_token = uuid.uuid4().hex
        row.lease_until = c.now() + timedelta(minutes=4)
        row.next_check_at = c.now() + timedelta(minutes=5)
        db.commit()
        return Worker(row.owner_id, row.id, row.lease_token)


def stop_result(ident, error=None, lease=None):
    with c.module().DB.session() as db:
        c.s.locked(db)
        row = db.get(c.Ticket, ident)
        if not row or lease and row.lease_token != lease:
            return
        if lease is None and row.lease_until and c.utc(row.lease_until) > c.now():
            return
        if error:
            # Never store an exception message from a provider, email or model.
            allowed = {
                "expired",
                "changed",
                "paused",
                "binding_changed",
                "sources_changed",
                "thread_changed",
                "message_changed",
                "provider_uncertain",
                "send_uncertain",
                "credentials_unavailable",
                "human_required",
                "invalid_output",
                "sender_changed",
                "sender_required",
                "provider_required",
                "connection_denied",
                "provider_refused",
                "transport_uncertain",
                "inbox_sync_required",
                "budget",
                "lease_ended",
                "input_limit",
                "response_limit",
            }
            row.error_code = error if error in allowed else "unavailable"
            if row.send_intent_at:
                row.state = "send_uncertain"
            elif row.provider_calls:
                row.state = "review_required"
                row.reason = row.error_code
            else:
                row.state = "review_required"
                row.reason = row.error_code
            c.audit(
                db,
                row.owner_id,
                "stopped",
                row.id,
                {"state": row.state, "reason": row.error_code},
            )
        row.lease_token = None
        row.lease_until = None
        db.commit()


def run(ident):
    try:
        worker = claim(ident)
    except ValueError as exc:
        if str(exc) not in {"busy", "changed", "missing"}:
            stop_result(ident, str(exc))
        return
    error = None
    try:
        row = worker.row()
        if row.state == "writing":
            writing.run(worker)
        else:
            mail.send(worker)
    except writing.execution.ExecutionStopped as exc:
        error = exc.reason
    except ValueError as exc:
        error = str(exc)
    except Exception:
        error = "transport_uncertain"
    finally:
        stop_result(ident, error, worker.lease)


def manual_draft(owner, ident, body, sources, confirm):
    if not confirm:
        raise c.Stop("confirm_required")
    with c.module().DB.session() as db:
        c.s.locked(db)
        _, row = c.ticket(db, owner, ident)
        if (
            row.state
            not in {"received", "draft", "draft_refused", "human", "review_required"}
            or row.send_intent_at
        ):
            raise c.Stop("changed")
        if row.lease_until and c.utc(row.lease_until) > c.now():
            raise c.Stop("busy")
        packet, hash = writing.bundle(db, owner, ident, manual=True)
        value = writing.validate(
            {"needs_human": False, "body": body, "sources": sources}, packet
        )
        row.draft_encrypted = c.seal(value)
        row.outbox_encrypted = None
        row.draft_hash = c.s.digest(value)
        row.dossier = {"hash": hash, "sources": packet["sources"], "manual": True}
        row.state = "draft"
        row.revision += 1
        # Recompute the dossier after the owner's revision bump.
        packet, hash = writing.bundle(db, owner, ident, manual=True)
        row.dossier = {"hash": hash, "sources": packet["sources"], "manual": True}
        c.audit(db, owner, "manual_draft", ident)
        db.commit()


def cancel(owner, ident):
    with c.module().DB.session() as db:
        c.s.locked(db)
        _, row = c.ticket(db, owner, ident, active=False)
        if row.state in {"accepted", "send_uncertain"}:
            raise c.Stop("changed")
        row.state = "ignored"
        row.lease_token = None
        row.lease_until = None
        c.audit(db, owner, "cancel", ident)
        db.commit()


def language(owner, ident, lang):
    if lang not in {"fr", "en"}:
        raise c.Stop("changed")
    with c.module().DB.session() as db:
        c.s.locked(db)
        _, row = c.ticket(db, owner, ident)
        if row.state != "received" or row.provider_calls:
            raise c.Stop("changed")
        row.lang = lang
        row.revision += 1
        c.audit(db, owner, "language", ident)
        db.commit()


def tick():
    with c.module().DB.session() as db:
        cfg = db.get(c.Channel, c.KEY)
        if not cfg:
            return 0
        owner = cfg.owner_id
        expired = list(
            db.scalars(
                select(c.Ticket.id)
                .where(c.Ticket.created_at < c.now() - c.RETENTION)
                .limit(100)
            )
        )
        for ident in expired:
            row = db.get(c.Ticket, ident)
            if not row.lease_until or c.utc(row.lease_until) <= c.now():
                try:
                    from .models import TelegramApprovalRequest
                except ImportError:
                    from models import TelegramApprovalRequest
                for request in db.scalars(
                    select(TelegramApprovalRequest).where(
                        TelegramApprovalRequest.kind.in_(KINDS),
                        TelegramApprovalRequest.target_id == ident,
                    )
                ):
                    request.pages = []
                    request.confirmation = None
                    if request.state == "pending":
                        request.state = "expired"
                db.delete(row)
        db.commit()
        poll = cfg.active and (
            not cfg.next_poll_at or c.utc(cfg.next_poll_at) <= c.now()
        )
        ident = db.scalar(
            select(c.Ticket.id)
            .where(
                c.Ticket.owner_id == owner,
                c.Ticket.state.in_(RUNNABLE),
                or_(
                    c.Ticket.next_check_at.is_(None), c.Ticket.next_check_at <= c.now()
                ),
                or_(c.Ticket.lease_until.is_(None), c.Ticket.lease_until <= c.now()),
            )
            .order_by(c.Ticket.created_at)
            .limit(1)
        )
    if poll:
        gmail.poll(owner)
    if ident:
        run(ident)
    return int(bool(poll)) + int(bool(ident))


def start():
    global _THREAD
    if c.module()._env_bool("SEO_AGENT_DISABLE_SCHEDULERS"):
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
                    pass

        _THREAD = threading.Thread(target=loop, name="noyaru-support", daemon=True)
        _THREAD.start()


def stop():
    _STOP.set()
