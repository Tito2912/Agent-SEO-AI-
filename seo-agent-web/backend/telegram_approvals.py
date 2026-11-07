"""Durable private owner decisions. No public webhook and no arbitrary tool dispatch."""

from datetime import timedelta
from html import unescape
import os
import re
import secrets
import threading
import uuid

import requests
from sqlalchemy import select, or_, and_
from sqlalchemy.exc import SQLAlchemyError

try:
    from . import (
        own_seo as s,
        own_writing as w,
        telegram,
        telegram_approval_adapters as adapters,
        telegram_approval_format as display,
    )
    from .models import (
        TelegramApprovalChannel as Channel,
        TelegramApprovalRequest as Approval,
        AuditLog,
        OwnSeoDraft,
    )
except ImportError:
    import own_seo as s
    import own_writing as w
    import telegram
    import telegram_approval_adapters as adapters
    import telegram_approval_format as display
    from models import (
        TelegramApprovalChannel as Channel,
        TelegramApprovalRequest as Approval,
        AuditLog,
        OwnSeoDraft,
    )

PATH = "/settings/system/telegram/approvals"
KEY = "owner"
_STOP = threading.Event()
_THREAD = None
_THREAD_LOCK = threading.Lock()


def now():
    return s.clock()


def digest_config():
    return s.digest([telegram._jeton(), telegram._conversation()])


def tr(channel, fr, en):
    return en if channel.lang == "en" else fr


def kind_label(channel, kind):
    labels = {
        "test": ("Test", "Test"),
        "knowledge": ("Source SEO/GEO", "SEO/GEO source"),
        "brief": ("Brief éditorial", "Editorial brief"),
        "writing": ("Autoriser la rédaction", "Authorize writing"),
        "draft": ("Relire un brouillon", "Review a draft"),
        "blog_pr": ("Préparer une PR du blog", "Prepare a blog PR"),
        "blog_publish": ("Publier un article", "Publish an article"),
        "support_write": ("Préparer une réponse SAV", "Prepare a support reply"),
        "support_send": ("Envoyer une réponse SAV", "Send a support reply"),
        "support_review": ("Reprendre une demande SAV", "Handle a support request"),
    }
    return tr(channel, *labels[kind])


def api(method, body=None):
    """Only allowlisted methods; provider errors/URLs never enter logs or storage."""
    if method not in {
        "getMe",
        "getChat",
        "getWebhookInfo",
        "getUpdates",
        "sendMessage",
        "editMessageText",
        "answerCallbackQuery",
    }:
        raise ValueError("unsupported")
    if not telegram.configure():
        raise ValueError("not_configured")
    try:
        response = requests.post(
            telegram._API + "/bot" + telegram._jeton() + "/" + method,
            json=body or {},
            timeout=(3, 20 if method == "getUpdates" else 10),
        )
        data = response.json()
    except Exception:
        raise ValueError("transport_uncertain") from None
    if (
        response.status_code != 200
        or not isinstance(data, dict)
        or data.get("ok") is not True
    ):
        code = response.status_code
        raise ValueError(
            {
                409: "receiver_conflict",
                429: "rate_limited",
                401: "bot_denied",
                403: "chat_denied",
            }.get(code, "telegram_refused")
        )
    return data.get("result")


def audit(db, owner, action, target=None, meta=None):
    db.add(
        AuditLog(
            actor_user_id=owner,
            action="telegram_approvals." + action,
            target_type="telegram_approval",
            target_id=target,
            meta=meta or {},
        )
    )


def active(db):
    channel = db.get(Channel, KEY)
    if not channel or not channel.enabled:
        raise ValueError("inactive")
    s.actor(db, channel.owner_id)
    if channel.config_hash != digest_config() or not re.fullmatch(
        "[1-9][0-9]{0,19}", channel.chat_id
    ):
        raise ValueError("binding_changed")
    return channel


def activate(owner, lang, confirm):
    if not confirm:
        raise ValueError("confirm_required")
    fingerprint = digest_config()
    chat = telegram._conversation()
    if not re.fullmatch("[1-9][0-9]{0,19}", chat):
        raise ValueError("private_required")
    webhook = api("getWebhookInfo")
    if not isinstance(webhook, dict) or webhook.get("url"):
        raise ValueError("webhook_exists")
    bot = api("getMe")
    conversation = api("getChat", {"chat_id": chat})
    if (
        not isinstance(bot, dict)
        or bot.get("is_bot") is not True
        or not isinstance(bot.get("id"), int)
        or not isinstance(conversation, dict)
        or conversation.get("type") != "private"
        or str(conversation.get("id")) != chat
    ):
        raise ValueError("private_required")
    with s.module().DB.session() as db:
        s.locked(db)
        s.actor(db, owner)
        if fingerprint != digest_config():
            raise ValueError("binding_changed")
        old = db.get(Channel, KEY)
        if (
            old
            and old.enabled
            and old.owner_id == owner
            and old.config_hash == fingerprint
        ):
            return  # Duplicate form submission preserves the current binding and buttons.
        # A live poll still in flight must finish before this channel is rebound.
        if old and old.lease_until and s.seo_watch.utc(old.lease_until) > now():
            raise ValueError("receiver_busy")
        if old:
            for row in db.scalars(
                select(Approval).where(
                    Approval.binding == old.binding, Approval.state == "pending"
                )
            ):
                row.state = "stale"
                row.confirmation = None
        previous_offset = old.offset if old and old.bot_id == str(bot["id"]) else "0"
        channel = old or Channel(id=KEY)
        channel.owner_id = owner
        channel.enabled = True
        channel.binding = uuid.uuid4().hex
        channel.config_hash = fingerprint
        channel.bot_id = str(bot["id"])
        channel.chat_id = chat
        channel.lang = "en" if lang == "en" else "fr"
        channel.error_code = None
        channel.updated_at = now()
        channel.offset = previous_offset
        channel.lease_token = None
        channel.lease_until = None
        db.add(channel)
        audit(db, owner, "activate")
        db.commit()
    start()


def pause(owner):
    with s.module().DB.session() as db:
        s.locked(db)
        s.actor(db, owner)
        channel = db.get(Channel, KEY)
        if channel:
            channel.enabled = False
            channel.updated_at = now()
            for row in db.scalars(
                select(Approval).where(
                    Approval.binding == channel.binding, Approval.state == "pending"
                )
            ):
                row.state = "stale"
                row.confirmation = None
        audit(db, owner, "pause")
        db.commit()


def split_pages(text):
    pages = display.split_text(text)
    if not pages or len(pages) > 16:
        raise ValueError("document_too_long")
    return pages


def stored_pages(doc):
    pages = display.pages(doc)
    if doc["kind"] in {"support_write", "support_send", "support_review"}:
        try:
            from . import own_support_common
        except ImportError:
            import own_support_common
        pages = [own_support_common.seal({"text": page}) for page in pages]
    return pages


def register(db, channel, doc):
    row = db.scalar(
        select(Approval).where(
            Approval.binding == channel.binding,
            Approval.kind == doc["kind"],
            Approval.target_id == doc["target"],
            Approval.version == doc["version"],
        )
    )
    if row:
        # inspect() has just revalidated this exact version in the current binding.
        # A refresh lease or temporary outage must not strand its existing card.
        if row.state == "stale":
            row.state = (
                "pending" if s.seo_watch.utc(row.expires_at) > now() else "expired"
            )
            row.confirmation = None
            audit(
                db,
                channel.owner_id,
                "recover",
                row.id,
                {"state": row.state, "version": row.version},
            )
        # A refusal stays refused. Expiry requires an explicit web resend, not minute-by-minute spam.
        return row
    pages = stored_pages(doc)
    row = Approval(
        id=uuid.uuid4().hex,
        owner_id=channel.owner_id,
        binding=channel.binding,
        kind=doc["kind"],
        target_id=doc["target"],
        version=doc["version"],
        pages=pages,
        state="pending",
        nonce=secrets.token_hex(6),
        seen=0,
        delivery="queued",
        expires_at=min(now() + timedelta(hours=24), s.seo_watch.utc(doc['expires_at'])) if doc.get('expires_at') else now() + timedelta(hours=24),
        created_at=now(),
    )
    db.add(row)
    db.flush()
    audit(
        db,
        channel.owner_id,
        "request",
        row.id,
        {"kind": row.kind, "version": row.version},
    )
    return row


def inspect(db, channel, row):
    if row.kind == "test":
        return adapters.document(
            "test",
            row.target_id,
            row.version,
            "Noyaru · Test",
            tr(
                channel,
                "Cette décision ne déclenche aucune opération.",
                "This decision triggers no operation.",
            ),
            tr(
                channel,
                "Approuver ou refuser ce test, sans effet métier.",
                "Approve or reject this test, with no business effect.",
            ),
            PATH,
            en=channel.lang == "en",
        )
    return adapters.inspect(db, channel.owner_id, row.kind, row.target_id, channel.lang)


def check(db, channel, row):
    if not row or row.binding != channel.binding or row.owner_id != channel.owner_id:
        raise ValueError("binding_changed")
    if row.state != "pending":
        raise ValueError("already_handled")
    if s.seo_watch.utc(row.expires_at) <= now():
        raise ValueError("expired")
    doc = inspect(db, channel, row)
    if doc["version"] != row.version:
        raise ValueError("changed")
    return doc


def sync():
    with s.module().DB.session() as db:
        s.locked(db)
        channel = active(db)
        for row in db.scalars(
            select(Approval).where(
                Approval.binding == channel.binding, Approval.state == "pending"
            )
        ):
            try:
                check(db, channel, row)
            except ValueError as exc:
                row.state = "expired" if str(exc) == "expired" else "stale"
                row.confirmation = None
            if (
                row.delivery == "sending"
                and row.delivery_until
                and s.seo_watch.utc(row.delivery_until) <= now()
            ):
                row.delivery = "uncertain"
                row.error_code = "transport_uncertain"
        for kind, target in adapters.candidates(db, channel.owner_id, channel.lang):
            try:
                register(
                    db,
                    channel,
                    adapters.inspect(db, channel.owner_id, kind, target, channel.lang),
                )
            except ValueError:
                continue
        channel.updated_at = now()
        db.commit()


def test_request(owner):
    with s.module().DB.session() as db:
        s.locked(db)
        channel = active(db)
        if channel.owner_id != owner:
            raise ValueError("binding_changed")
        # One pending test at a time.
        prior = db.scalar(
            select(Approval).where(
                Approval.binding == channel.binding,
                Approval.kind == "test",
                Approval.state == "pending",
            )
        )
        if prior:
            return prior.id
        ident = uuid.uuid4().hex
        doc = inspect(
            db, channel, Approval(kind="test", target_id=ident, version=s.digest(ident))
        )
        row = register(db, channel, doc)
        db.commit()
        return row.id


def resend(owner, ident):
    with s.module().DB.session() as db:
        s.locked(db)
        channel = active(db)
        row = db.get(Approval, ident)
        if channel.owner_id != owner or not row or row.binding != channel.binding:
            raise ValueError("binding_changed")
        if row.state == "expired":
            doc = inspect(db, channel, row)
            if doc["version"] != row.version:
                raise ValueError("changed")
            row.state = "pending"
            row.expires_at = now() + timedelta(hours=24)
        doc = check(db, channel, row)
        if row.delivery not in {"uncertain", "sent"}:
            raise ValueError("receiver_busy")
        # Only an explicit resend replaces a cached presentation and invalidates old buttons.
        row.pages = stored_pages(doc)
        row.delivery = "queued"
        row.message_id = None
        row.seen = 0
        row.confirmation = None
        row.nonce = secrets.token_hex(6)
        audit(db, owner, "resend", row.id)
        db.commit()


def callback_data(row, verb, nonce=None):
    return "ny:" + row.id + ":" + verb + ":" + (nonce or row.nonce)


def card(channel, row, page=0, doc=None, confirm=False):
    buttons = []
    icons = {
        "test": "🧪",
        "knowledge": "🔎",
        "brief": "🧭",
        "writing": "✍️",
        "draft": "📝",
        "blog_pr": "📦",
        "blog_publish": "🚀",
        "support_write": "💬",
        "support_send": "📨",
        "support_review": "🧑‍💼",
    }
    heading = (
        "<b>"
        + icons[row.kind]
        + " Noyaru · "
        + display.escaped(kind_label(channel, row.kind))
        + "</b>"
    )
    path = doc.get("path", PATH) if doc else PATH
    if not re.fullmatch(
        r"/settings/system/[a-zA-Z0-9/_-]+(?:\?[a-zA-Z0-9=_&%.-]+)?", path
    ):
        path = PATH
    open_button = [
        {
            "text": tr(channel, "Ouvrir dans Noyaru ↗", "Open in Noyaru ↗"),
            "url": "https://noyaru.com" + path,
        }
    ]
    if row.state != "pending":
        labels = {
            "approved": ("✅ Accord enregistré", "✅ Approval recorded"),
            "rejected": ("✋ Refus enregistré", "✋ Rejection recorded"),
            "expired": ("⌛ Demande expirée", "⌛ Request expired"),
            "stale": ("🔄 Demande à actualiser", "🔄 Request needs refreshing"),
        }
        status = tr(
            channel,
            *labels.get(row.state, ("À vérifier dans Noyaru", "Check in Noyaru")),
        )
        text = heading + "\n\n<b>" + display.escaped(status) + "</b>"
        if doc:
            text += "\n" + display.escaped(display.one_line(doc.get("title", "")))
        if row.state == "approved":
            text += "\n" + tr(
                channel,
                "L’accord est enregistré. Le résultat de l’opération reste à vérifier dans Noyaru.",
                "Approval is recorded. Check the operation’s outcome in Noyaru.",
            )
        text += "\n\n" + tr(channel, "Réf. ", "Ref. ") + row.id[:8]
        buttons = [open_button]
    elif confirm:
        text = (
            heading
            + "\n\n<b>"
            + tr(
                channel,
                "Dernière étape · confirmer l’accord"
                if row.choice == "yes"
                else "Dernière étape · confirmer le refus",
                "Last step · confirm approval"
                if row.choice == "yes"
                else "Last step · confirm rejection",
            )
            + "</b>"
        )
        if doc.get("decision_summary"):
            text += "\n\n" + display.escaped(doc["decision_summary"])
        refusal = {
            "knowledge": tr(
                channel,
                "Écarter cette demande. Cette version reste non validée.",
                "Dismiss this request. This version stays unapproved.",
            ),
            "brief": tr(
                channel,
                "Abandonner ce brief. Aucun appel IA ni publication.",
                "Discard this brief. No AI request or publication.",
            ),
            "writing": tr(
                channel,
                "Refuser cette rédaction. Aucun nouvel appel IA autorisé.",
                "Reject this writing request. No new AI request authorized.",
            ),
            "draft": tr(
                channel,
                "Annuler ce brouillon privé. Aucune publication.",
                "Cancel this private draft. No publication.",
            ),
            "blog_pr": tr(
                channel,
                "Arrêter cette préparation. Aucune nouvelle écriture GitHub ; une PR existante reste en place.",
                "Stop this preparation. No new GitHub write; any existing PR is kept.",
            ),
            "blog_publish": tr(
                channel,
                "Refuser la publication. Aucune fusion ; la PR reste en place.",
                "Reject publication. No merge; the PR is kept.",
            ),
            "support_write": tr(
                channel,
                "Refuser cet appel IA ; garder la demande pour une réponse manuelle.",
                "Reject this AI request; keep the request for a manual reply.",
            ),
            "support_send": tr(
                channel,
                "Refuser cet envoi ; conserver le brouillon à corriger. Aucun email envoyé.",
                "Reject sending; keep the draft for revision. No email sent.",
            ),
            "support_review": tr(
                channel,
                "Écarter cette demande ou ce rappel d’envoi incertain. Le résultat inconnu reste conservé ; aucun nouvel email ni appel IA.",
                "Dismiss this request or uncertain-send reminder. The unknown result is kept; no new email or AI request.",
            ),
            "test": tr(
                channel,
                "Refuser le test, sans effet métier.",
                "Reject the test, with no business effect.",
            ),
        }
        effect = doc["effect"] if row.choice == "yes" else refusal[row.kind]
        text += (
            "\n\n"
            + display.escaped(effect)
            + "\n\n"
            + tr(channel, "Réf. ", "Ref. ")
            + row.id[:8]
        )
        buttons = [
            [
                {
                    "text": tr(
                        channel,
                        "✅ Confirmer l’accord"
                        if row.choice == "yes"
                        else "✋ Confirmer le refus",
                        "✅ Confirm approval"
                        if row.choice == "yes"
                        else "✋ Confirm rejection",
                    ),
                    "callback_data": callback_data(row, "c", row.confirmation),
                },
                {
                    "text": tr(channel, "↩️ Relire", "↩️ Read again"),
                    "callback_data": callback_data(row, "p0"),
                },
            ]
        ]
    else:
        page = max(0, min(page, len(row.pages) - 1))
        shown = row.seen | (1 << page)
        all_shown = shown == (1 << len(row.pages)) - 1
        progress = " ".join(
            "●" if shown & (1 << p) else "○" for p in range(len(row.pages))
        )
        stage = {
            "support_write": (
                "Étape 1/2 · Préparer le brouillon",
                "Step 1/2 · Prepare the draft",
            ),
            "support_send": (
                "Étape 2/2 · Autoriser l’envoi",
                "Step 2/2 · Authorize sending",
            ),
            "blog_pr": ("Étape 1/2 · Préparer la PR", "Step 1/2 · Prepare the PR"),
            "blog_publish": (
                "Étape 2/2 · Autoriser la publication",
                "Step 2/2 · Authorize publication",
            ),
        }.get(row.kind)
        text = heading
        if stage:
            text += "\n<i>" + tr(channel, *stage) + "</i>"
        if doc:
            text += "\n" + display.escaped(display.one_line(doc.get("title", "")))
        text += (
            "\n\n"
            + tr(channel, "Page ", "Page ")
            + f"{page + 1}/{len(row.pages)} · "
            + progress
        )
        text += "\n\n" + display.escaped(pages_for(row)[page])
        if not all_shown:
            text += "\n\n" + tr(
                channel,
                "📖 Parcours toutes les pages, puis choisis ta décision.",
                "📖 View every page, then choose your decision.",
            )
        if doc and not doc["ready"]:
            text += "\n\n" + tr(
                channel,
                "⏸ Cette demande doit d’abord être complétée dans Noyaru.",
                "⏸ Complete this request in Noyaru first.",
            )
        text += (
            "\n\n"
            + tr(channel, "⏳ Valable jusqu’au ", "⏳ Valid until ")
            + s.seo_watch.utc(row.expires_at).strftime("%Y-%m-%d %H:%M UTC")
            + " · "
            + tr(channel, "Réf. ", "Ref. ")
            + row.id[:8]
        )
        nav = []
        if page:
            nav.append(
                {
                    "text": tr(channel, "← Précédent", "← Previous"),
                    "callback_data": callback_data(row, "p" + str(page - 1)),
                }
            )
        if page + 1 < len(row.pages):
            nav.append(
                {
                    "text": tr(channel, "Suite →", "Next →"),
                    "callback_data": callback_data(row, "p" + str(page + 1)),
                }
            )
        if nav:
            buttons.append(nav)
        choices = []
        if all_shown and (doc is None or doc["ready"]):
            choices.append(
                {
                    "text": tr(channel, "✅ Approuver", "✅ Approve"),
                    "callback_data": callback_data(row, "y"),
                }
            )
        choices.append(
            {
                "text": tr(channel, "✋ Refuser", "✋ Reject"),
                "callback_data": callback_data(row, "n"),
            }
        )
        buttons.append(choices)
        buttons.append(open_button)
    # All variable text is escaped before HTML formatting; legacy plain pages stay safe too.
    plain = (
        text.replace("<b>", "")
        .replace("</b>", "")
        .replace("<i>", "")
        .replace("</i>", "")
    )
    if display.units(unescape(plain)) > 4096:
        raise ValueError("document_too_long")
    return {
        "text": text,
        "parse_mode": "HTML",
        "reply_markup": {"inline_keyboard": buttons},
        "link_preview_options": {"is_disabled": True},
    }


def deliver_one():
    with s.module().DB.session() as db:
        s.locked(db)
        channel = active(db)
        row = db.scalar(
            select(Approval)
            .where(
                Approval.binding == channel.binding,
                Approval.state == "pending",
                Approval.delivery == "queued",
            )
            .order_by(Approval.created_at)
            .limit(1)
        )
        if not row:
            return False
        try:
            doc = check(db, channel, row)
        except ValueError:
            row.state = "stale"
            db.commit()
            return False
        body = card(channel, row, doc=doc)
        body["chat_id"] = channel.chat_id
        ident = row.id
        binding = channel.binding
        row.delivery = "sending"
        row.delivery_until = now() + timedelta(seconds=40)
        db.commit()
    # No DB transaction is held over a network request. Unknown outcomes are never auto-retried.
    error = None
    result = None
    try:
        result = api("sendMessage", body)
    except ValueError as exc:
        error = str(exc)
    with s.module().DB.session() as db:
        s.locked(db)
        channel = active(db)
        row = db.get(Approval, ident)
        if channel.binding != binding or row.delivery != "sending":
            return False
        if (
            not error
            and isinstance(result, dict)
            and isinstance(result.get("message_id"), int)
        ):
            row.message_id = str(result["message_id"])
            row.delivery = "sent"
            row.seen |= 1
        else:
            row.delivery = "uncertain"
            row.error_code = error or "transport_uncertain"
        db.commit()
    return True


def acknowledge(query_id, text):
    try:
        api("answerCallbackQuery", {"callback_query_id": query_id, "text": text[:180]})
    except ValueError:
        pass


def lease_valid(channel, lease):
    if lease is not None and (
        channel.lease_token != lease
        or not channel.lease_until
        or s.seo_watch.utc(channel.lease_until) <= now()
    ):
        raise ValueError("lease_lost")


def handle_callback(query, lease=None):
    if not isinstance(query, dict):
        return
    data = query.get("data")
    message = query.get("message") or {}
    sender = query.get("from") or {}
    chat = message.get("chat") or {}
    match = re.fullmatch(
        r"ny:([a-f0-9]{32}):(p(?:[0-9]|1[0-5])|y|n|c):([a-f0-9]{12})", data or ""
    )
    if not match:
        return
    ident, verb, nonce = match.groups()
    body = None
    page = None
    result_id = None
    binding = None
    error_message = None
    with s.module().DB.session() as db:
        s.locked(db)
        channel = active(db)
        lease_valid(channel, lease)
        if (
            chat.get("type") != "private"
            or str(chat.get("id")) != channel.chat_id
            or str(sender.get("id")) != channel.chat_id
            or sender.get("is_bot") is not False
            or str((message.get("from") or {}).get("id")) != channel.bot_id
            or message.get("forward_origin")
            or query.get("inline_message_id")
        ):
            return
        row = db.get(Approval, ident)
        if (
            not row
            or row.binding != channel.binding
            or str(message.get("message_id")) != row.message_id
        ):
            return
        expected = row.confirmation if verb == "c" else row.nonce
        if not expected or not secrets.compare_digest(expected, nonce):
            return
        query_id = query.get("id")
        if not isinstance(query_id, str) or len(query_id) > 128:
            return
        # Nothing is approved until this transaction commits.
        try:
            doc = check(db, channel, row)
            if verb.startswith("p"):
                page = int(verb[1:])
                row.confirmation = None
                row.choice = None
                body = card(channel, row, page, doc)
            elif verb in {"y", "n"}:
                if verb == "y" and (
                    not doc["ready"] or row.seen != (1 << len(row.pages)) - 1
                ):
                    raise ValueError("read_all_pages")
                row.choice = "yes" if verb == "y" else "no"
                row.confirmation = secrets.token_hex(6)
                body = card(channel, row, doc=doc, confirm=True)
            else:
                if row.choice not in {"yes", "no"}:
                    raise ValueError("changed")
                if row.choice == "yes" and (
                    not doc["ready"] or row.seen != (1 << len(row.pages)) - 1
                ):
                    raise ValueError("read_all_pages")
                result_id = adapters.apply(
                    db,
                    channel.owner_id,
                    row.kind,
                    row.target_id,
                    row.choice == "yes",
                    row.id,
                    row.version,
                )
                row.state = "approved" if row.choice == "yes" else "rejected"
                row.decided_at = now()
                # Keep the final nonce so a repeated click can be acknowledged as
                # already handled. check() forbids every second business action.
                row.result_id = result_id
                audit(
                    db,
                    channel.owner_id,
                    row.state,
                    row.id,
                    {"kind": row.kind, "version": row.version},
                )
                body = card(channel, row, doc=doc)
            binding = channel.binding
            body.update(chat_id=channel.chat_id, message_id=int(row.message_id))
            db.commit()
        except ValueError as exc:
            code = str(exc)
            # Service failures roll back completely; the card remains actionable where appropriate.
            error_message = {
                "read_all_pages": tr(
                    channel,
                    "Lis toutes les pages avant d’approuver.",
                    "Read every page before approving.",
                ),
                "already_handled": tr(
                    channel,
                    "Cette demande a déjà été traitée.",
                    "This request was already handled.",
                ),
                "budget": tr(
                    channel,
                    "Plafond IA atteint. Réessaie plus tard.",
                    "AI limit reached. Try later.",
                ),
            }.get(
                code,
                tr(
                    channel,
                    "Demande expirée ou modifiée. Consulte Noyaru.",
                    "Request expired or changed. Check Noyaru.",
                ),
            )
            db.rollback()
    if error_message:
        acknowledge(query_id, error_message)
        return
    acknowledge(
        query_id,
        tr(
            channel,
            "Décision enregistrée." if verb == "c" else "Demande reçue.",
            "Decision recorded." if verb == "c" else "Request received.",
        ),
    )
    try:
        api("editMessageText", body)
        if page is not None:
            with s.module().DB.session() as db:
                s.locked(db)
                channel = active(db)
                row = db.get(Approval, ident)
                if (
                    channel.binding == binding
                    and row.state == "pending"
                    and row.message_id == str(body["message_id"])
                ):
                    row.seen |= 1 << page
                    db.commit()
    except ValueError:
        pass
    if result_id:
        threading.Thread(
            target=run_result,
            args=(row.kind, result_id),
            name="telegram-authorized-operation",
            daemon=True,
        ).start()


def run_result(kind, ident):
    if kind in {'support_write','support_send','support_review'}:
        try:
            from . import own_support
        except ImportError:
            import own_support
        own_support.run(ident)
        return
    if kind in {'blog_pr', 'blog_publish'}:
        try:
            from . import own_blog
        except ImportError:
            import own_blog
        own_blog.run(ident)
    elif kind == 'writing':
        w.run(ident)


def pages_for(row):
    if row.kind in {'support_write','support_send','support_review'}:
        try:
            from . import own_support
        except ImportError:
            import own_support
        return own_support.approval_pages(row)
    return row.pages


def process_update(update, lease):
    if not isinstance(update, dict) or not isinstance(update.get("update_id"), int):
        return
    with s.module().DB.session() as db:
        s.locked(db)
        channel = active(db)
        lease_valid(channel, lease)
        channel.lease_until = now() + timedelta(seconds=45)
        db.commit()
    if "callback_query" in update:
        handle_callback(update["callback_query"], lease)
    # Only commands in the pinned private conversation get a response. No arbitrary text is stored.
    message = update.get("message") or {}
    if message.get("text") in {"/start", "/validations", "/aide"}:
        with s.module().DB.session() as db:
            channel = active(db)
            lease_valid(channel, lease)
            if (
                (message.get("chat") or {}).get("type") == "private"
                and str((message.get("chat") or {}).get("id")) == channel.chat_id
                and str((message.get("from") or {}).get("id")) == channel.chat_id
                and not message.get("forward_origin")
            ):
                pending = list(
                    db.scalars(
                        select(Approval)
                        .where(
                            Approval.binding == channel.binding,
                            Approval.state == "pending",
                        )
                        .limit(20)
                    )
                )
                text = (
                    tr(channel, "Validations en attente : ", "Pending approvals: ")
                    + str(len(pending))
                    + "\n"
                )
                text += "\n".join(
                    row.id[:8] + " · " + kind_label(channel, row.kind)
                    for row in pending
                )
                text += "\n" + s.module()._lien_admin(PATH)
            else:
                text = None
        if text:
            try:
                api(
                    "sendMessage",
                    {
                        "chat_id": channel.chat_id,
                        "text": text,
                        "link_preview_options": {"is_disabled": True},
                    },
                )
            except ValueError:
                pass
    with s.module().DB.session() as db:
        s.locked(db)
        channel = active(db)
        lease_valid(channel, lease)
        channel.offset = str(max(int(channel.offset), update["update_id"] + 1))
        db.commit()


def claim():
    token = uuid.uuid4().hex
    with s.module().DB.session() as db:
        s.locked(db)
        channel = active(db)
        if channel.lease_until and s.seo_watch.utc(channel.lease_until) > now():
            return None
        channel.lease_token = token
        channel.lease_until = now() + timedelta(seconds=45)
        db.commit()
    return token


def resume_writing():
    """Resume only a durably accepted writing request after a process restart."""
    with s.module().DB.session() as db:
        channel = active(db)
        ident = db.scalar(
            select(OwnSeoDraft.id)
            .join(Approval, Approval.result_id == OwnSeoDraft.id)
            .where(
                Approval.owner_id == channel.owner_id,
                Approval.kind == "writing",
                Approval.state == "approved",
                or_(
                    OwnSeoDraft.status == "queued",
                    and_(
                        OwnSeoDraft.status == "running",
                        or_(
                            OwnSeoDraft.lease_until.is_(None),
                            OwnSeoDraft.lease_until <= now(),
                        ),
                    ),
                ),
            )
            .order_by(Approval.decided_at)
            .limit(1)
        )
    if ident:
        threading.Thread(
            target=w.run, args=(ident,), name="telegram-resumed-writing", daemon=True
        ).start()


def cycle():
    token = claim()
    if token is None:
        return
    try:
        sync()
        # At most one notification per poll avoids message bursts and provider throttling.
        deliver_one()
        resume_writing()
        with s.module().DB.session() as db:
            s.locked(db)
            channel = active(db)
            lease_valid(channel, token)
            channel.lease_until = now() + timedelta(seconds=45)
            offset = int(channel.offset)
            channel.error_code = None
            db.commit()
        updates = api(
            "getUpdates",
            {
                "offset": offset,
                "timeout": 15,
                "limit": 20,
                "allowed_updates": ["callback_query", "message"],
            },
        )
        if not isinstance(updates, list):
            raise ValueError("telegram_refused")
        for update in updates:
            if _STOP.is_set():
                break
            process_update(update, token)
    except ValueError as exc:
        with s.module().DB.session() as db:
            s.locked(db)
            channel = db.get(Channel, KEY)
            if channel and channel.lease_token == token:
                channel.error_code = str(exc)
                if str(exc) in {
                    "receiver_conflict",
                    "binding_changed",
                    "owner_required",
                }:
                    channel.enabled = False
                db.commit()
    finally:
        with s.module().DB.session() as db:
            s.locked(db)
            channel = db.get(Channel, KEY)
            if channel and channel.lease_token == token:
                channel.lease_token = None
                channel.lease_until = None
                db.commit()


def start():
    global _THREAD
    if os.environ.get("SEO_AGENT_DISABLE_TELEGRAM_RECEIVER", "").lower() in {
        "true",
        "1",
        "yes",
    }:
        return
    with _THREAD_LOCK:
        if _THREAD and _THREAD.is_alive():
            return
        try:
            with s.module().DB.session() as db:
                active(db)
        except (ValueError, SQLAlchemyError):
            return
        _STOP.clear()

        def run():
            while not _STOP.is_set():
                try:
                    cycle()
                except (ValueError, SQLAlchemyError):
                    pass
                except Exception:
                    # Never log exceptions containing provider URLs or message bodies.
                    s.module().logger.warning(
                        "[TELEGRAM APPROVALS] receiver cycle failed"
                    )
                _STOP.wait(3)

        _THREAD = threading.Thread(
            target=run, name="telegram-owner-approvals", daemon=True
        )
        _THREAD.start()


def stop():
    _STOP.set()
