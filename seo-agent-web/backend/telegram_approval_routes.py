"""Owner UI for linking, pausing and inspecting Telegram decisions."""

from urllib.parse import urlencode
from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

try:
    from . import telegram_approvals as a, i18n, own_seo_routes
except ImportError:
    import telegram_approvals as a
    import i18n
    import own_seo_routes

router = APIRouter()


def redirect(error=""):
    return RedirectResponse(
        a.PATH + "?" + urlencode({"err": error}),
        status_code=303,
        headers={"Cache-Control": "no-store"},
    )


ERRORS = {
    "confirm_required": (
        "Confirme le lien avec ta conversation privée.",
        "Confirm the link to your private conversation.",
    ),
    "not_configured": (
        "Configure le bot et ta conversation dans Système.",
        "Configure the bot and your conversation in System.",
    ),
    "private_required": (
        "Les validations nécessitent une conversation privée avec le bot.",
        "Approvals require a private conversation with the bot.",
    ),
    "webhook_exists": (
        "Ce bot reçoit déjà ses messages par un autre service. Aucun réglage Telegram n’a été remplacé.",
        "This bot already receives messages through another service. No Telegram setting was replaced.",
    ),
    "binding_changed": (
        "La configuration ou le propriétaire a changé. Réactive le lien.",
        "The configuration or owner changed. Reactivate the link.",
    ),
    "receiver_conflict": (
        "Un autre service lit les messages de ce bot. Réception mise en pause.",
        "Another service reads this bot’s messages. Reception paused.",
    ),
    "receiver_busy": (
        "La réception termine une opération. Réessaie dans une minute.",
        "The receiver is finishing an operation. Try again in a minute.",
    ),
    "inactive": (
        "Active d’abord les validations Telegram.",
        "Enable Telegram approvals first.",
    ),
    "changed": (
        "Cette demande a changé. Consulte la version actuelle dans Noyaru.",
        "This request changed. Check the current version in Noyaru.",
    ),
    "expired": (
        "La demande a expiré. Renvoie-la après avoir vérifié le contexte.",
        "The request expired. Resend it after checking the context.",
    ),
    "already_handled": (
        "Cette décision a déjà été enregistrée.",
        "This decision was already recorded.",
    ),
    "transport_uncertain": (
        "Réponse Telegram inconnue. Vérifie le bot avant de renvoyer une carte.",
        "Telegram’s response is unknown. Check the bot before resending a card.",
    ),
    "rate_limited": (
        "Telegram limite temporairement les demandes.",
        "Telegram temporarily limits requests.",
    ),
    "bot_denied": (
        "Telegram refuse l’accès du bot. Vérifie sa configuration.",
        "Telegram denies bot access. Check its configuration.",
    ),
    "chat_denied": (
        "Le bot ne peut pas écrire dans cette conversation. Ouvre-le et envoie /start.",
        "The bot cannot write in this conversation. Open it and send /start.",
    ),
    "unavailable": (
        "Le service est momentanément indisponible.",
        "The service is temporarily unavailable.",
    ),
}


def error_text(code, lang):
    return ERRORS.get(code, ERRORS["unavailable"])[lang == "en"] if code else ""


@router.get("/settings/system/telegram/approvals")
def page(request: Request, err: str = ""):
    m = a.s.module()
    user = m._require_system_owner(request)
    lang = i18n.current_lang()
    channel = None
    items = []
    problem = ""
    unavailable = False
    try:
        with m.DB.session() as db:
            channel = db.get(a.Channel, a.KEY)
            if channel and channel.owner_id == user.id:
                items = list(
                    db.scalars(
                        select(a.Approval)
                        .where(a.Approval.owner_id == user.id)
                        .order_by(a.Approval.created_at.desc())
                        .limit(50)
                    )
                )
                if channel.enabled:
                    try:
                        a.active(db)
                    except ValueError as exc:
                        problem = str(exc)
                problem = problem or channel.error_code or ""
    except SQLAlchemyError:
        unavailable = True
    return m.templates.TemplateResponse(
        "settings_telegram_approvals.html",
        dict(
            request=request,
            project=None,
            base=a.PATH,
            channel=channel,
            items=items,
            pages_for=a.pages_for,
            configured=a.telegram.configure(),
            en=lang == "en",
            err=error_text(err or problem, lang),
            unavailable=unavailable,
        ),
        status_code=503 if unavailable else 200,
        headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"},
    )


@router.post("/settings/system/telegram/approvals/{operation}")
def action(
    request: Request, operation: str, confirm: bool = Form(False), ident: str = Form("")
):
    m = a.s.module()
    user = m._require_system_owner(request)
    own_seo_routes.throttle(request)
    try:
        if operation == "activate":
            a.activate(user.id, i18n.current_lang(), confirm)
        elif operation == "pause":
            a.pause(user.id)
        elif operation == "sync":
            a.sync()
        elif operation == "test":
            a.test_request(user.id)
        elif operation == "resend":
            a.resend(user.id, ident)
        else:
            return redirect("changed")
    except ValueError as exc:
        return redirect(str(exc))
    except SQLAlchemyError:
        return redirect("unavailable")
    return redirect()
