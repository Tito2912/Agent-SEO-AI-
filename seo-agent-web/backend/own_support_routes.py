"""Owner-only support UI. GET never reads Gmail, calls AI or sends email."""

import json
from urllib.parse import urlencode

from fastapi import APIRouter, BackgroundTasks, Form, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

try:
    from . import (
        own_support as support,
        own_support_common as c,
        own_support_gmail as gmail,
    )
    from . import (
        own_support_mail as mail,
        own_support_writing as writing,
        own_seo_routes,
        i18n,
    )
    from .own_support_copy import strings
except ImportError:
    import own_support as support
    import own_support_common as c
    import own_support_gmail as gmail
    import own_support_mail as mail
    import own_support_writing as writing
    import own_seo_routes
    import i18n
    from own_support_copy import strings

router = APIRouter()
HEADERS = {
    "Cache-Control": "no-store",
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
}


def redirect(ident="", err=""):
    return RedirectResponse(
        c.PATH + "?" + urlencode({"ticket": ident, "err": err}),
        status_code=303,
        headers=HEADERS,
    )


@router.get("/settings/system/support")
def page(request: Request, ticket: str = "", err: str = ""):
    m = c.module()
    user = m._require_system_owner(request)
    copy = strings(i18n.current_lang())
    cfg = None
    connected = False
    sender = None
    items = []
    unavailable = False
    error = ""
    try:
        with m.DB.session() as db:
            row = db.get(c.Channel, c.KEY)
            if row and row.owner_id == user.id:
                cfg = {
                    key: getattr(row, key)
                    for key in (
                        "mailbox",
                        "label_name",
                        "active",
                        "error_code",
                        "cursor",
                        "binding",
                    )
                }
                try:
                    connected = bool(
                        c.reveal(row.gmail_credentials).get("refresh_token")
                    )
                except ValueError:
                    error = "credentials_unavailable"
                try:
                    value, _ = mail.sender(db, user.id)
                    sender = {"provider": value["provider"], "from": value["from"]}
                except ValueError:
                    pass
            elif row:
                error = "owner_changed"
            for row in db.scalars(
                select(c.Ticket)
                .where(
                    c.Ticket.owner_id == user.id,
                    c.Ticket.created_at > c.now() - c.RETENTION,
                )
                .order_by(c.Ticket.created_at.desc())
                .limit(30)
            ):
                item = {
                    "row": row,
                    "payload": {},
                    "draft": {},
                    "manifest": None,
                    "packet": None,
                    "current": False,
                    "problem": "",
                    "binding_current": bool(cfg and cfg["binding"] == row.binding),
                }
                try:
                    item["payload"] = c.reveal(row.payload_encrypted)
                    if row.draft_encrypted:
                        item["draft"] = c.reveal(row.draft_encrypted)
                    if row.outbox_encrypted:
                        item["manifest"] = c.reveal(row.outbox_encrypted)
                    elif row.draft_encrypted:
                        item["manifest"] = mail.manifest(row)
                    if (
                        cfg
                        and cfg["active"]
                        and item["binding_current"]
                        and row.state
                        in {
                            "received",
                            "draft",
                            "draft_refused",
                            "human",
                            "review_required",
                        }
                        and item["payload"].get("text")
                    ):
                        try:
                            item["packet"], _ = writing.bundle(
                                db, user.id, row.id, manual=True
                            )
                        except ValueError as exc:
                            item["problem"] = str(exc)
                    item["current"] = (
                        writing.current(db, row)
                        if row.dossier
                        else item["binding_current"]
                    )
                    if (
                        cfg
                        and cfg["active"]
                        and item["binding_current"]
                        and row.state == "draft"
                        and not item["current"]
                    ):
                        item["problem"] = "sources_changed"
                except ValueError:
                    item["problem"] = "credentials_unavailable"
                items.append(item)
    except SQLAlchemyError:
        unavailable = True
    code = err or error or (cfg.get("error_code") if cfg else "")
    return m.templates.TemplateResponse(
        "settings_own_support.html",
        dict(
            request=request,
            project=None,
            copy=copy,
            base=c.PATH,
            cfg=cfg,
            connected=connected,
            sender=sender,
            items=items,
            selected=ticket,
            unavailable=unavailable,
            error=copy["errors"].get(code, copy["errors"]["unavailable"])
            if code
            else "",
            callback=m._public_base_url(request) + gmail.CALLBACK,
        ),
        status_code=503 if unavailable else 200,
        headers=HEADERS,
    )


@router.post("/settings/system/support/configure")
def configure(
    request: Request,
    mailbox: str = Form(""),
    label: str = Form("Noyaru-SAV"),
    client_id: str = Form(""),
    client_secret: str = Form(""),
    confirm: bool = Form(False),
):
    user = c.module()._require_system_owner(request)
    own_seo_routes.throttle(request)
    try:
        gmail.configure(user.id, mailbox, label, client_id, client_secret, confirm)
    except ValueError as exc:
        return redirect(err=str(exc))
    except SQLAlchemyError:
        return redirect(err="unavailable")
    return redirect()


@router.post("/settings/system/support/oauth/start")
def oauth_start(request: Request, confirm: bool = Form(False)):
    user = c.module()._require_system_owner(request)
    own_seo_routes.throttle(request)
    if not confirm:
        return redirect(err="confirm_required")
    callback = c.module()._public_base_url(request) + gmail.CALLBACK
    try:
        url, nonce = gmail.start(user.id, callback)
    except ValueError as exc:
        return redirect(err=str(exc))
    except SQLAlchemyError:
        return redirect(err="unavailable")
    response = RedirectResponse(url, status_code=303, headers=HEADERS)
    response.set_cookie(
        gmail.COOKIE,
        nonce,
        max_age=600,
        path=c.PATH + "/oauth",
        secure=callback.startswith("https://"),
        httponly=True,
        samesite="lax",
    )
    return response


@router.get("/settings/system/support/oauth/callback")
def oauth_callback(request: Request, code: str = "", state: str = "", error: str = ""):
    user = c.module()._require_system_owner(request)
    problem = ""
    try:
        if error:
            raise c.Stop("connection_denied")
        gmail.finish(
            user.id,
            code,
            state,
            request.cookies.get(gmail.COOKIE, ""),
            c.module()._public_base_url(request) + gmail.CALLBACK,
        )
    except ValueError as exc:
        problem = str(exc)
    except SQLAlchemyError:
        problem = "unavailable"
    response = redirect(err=problem)
    response.delete_cookie(
        gmail.COOKIE,
        path=c.PATH + "/oauth",
        secure=request.url.scheme == "https",
        httponly=True,
        samesite="lax",
    )
    return response


@router.post("/settings/system/support/channel/{action}")
def channel_action(
    request: Request,
    background: BackgroundTasks,
    action: str,
    confirm: bool = Form(False),
):
    user = c.module()._require_system_owner(request)
    own_seo_routes.throttle(request)
    try:
        if action == "activate":
            gmail.activate(user.id, confirm)
        elif action == "pause":
            gmail.pause(user.id)
        elif action == "poll":
            with c.module().DB.session() as db:
                c.channel(db, user.id)
            background.add_task(gmail.poll, user.id)
        else:
            raise HTTPException(404, "action_missing")
    except ValueError as exc:
        return redirect(err=str(exc))
    except SQLAlchemyError:
        return redirect(err="unavailable")
    return redirect()


@router.post("/settings/system/support/sender")
def sender(
    request: Request,
    provider: str = Form(""),
    key: str = Form(""),
    confirm: bool = Form(False),
):
    user = c.module()._require_system_owner(request)
    own_seo_routes.throttle(request)
    try:
        mail.configure(user.id, provider, key, confirm)
    except ValueError as exc:
        return redirect(err=str(exc))
    except SQLAlchemyError:
        return redirect(err="unavailable")
    return redirect()


@router.post("/settings/system/support/tickets/{ident}/{action}")
def ticket_action(
    request: Request,
    background: BackgroundTasks,
    ident: str,
    action: str,
    lang: str = Form("fr"),
    body: str = Form(""),
    sources: list[str] = Form([]),
    confirm: bool = Form(False),
):
    user = c.module()._require_system_owner(request)
    own_seo_routes.throttle(request)
    try:
        if action == "language":
            support.language(user.id, ident, lang)
        elif action == "draft":
            support.manual_draft(user.id, ident, body, sources, confirm)
        elif action == "cancel":
            support.cancel(user.id, ident)
        elif action == "resume":
            with c.module().DB.session() as db:
                c.ticket(db, user.id, ident)
            background.add_task(support.run, ident)
        else:
            raise HTTPException(404, "action_missing")
    except ValueError as exc:
        return redirect(ident, str(exc))
    except SQLAlchemyError:
        return redirect(ident, "unavailable")
    return redirect(ident)


@router.get("/settings/system/support/tickets/{ident}/download")
def download(request: Request, ident: str):
    user = c.module()._require_system_owner(request)
    with c.module().DB.session() as db:
        try:
            _, row = c.ticket(db, user.id, ident, active=False)
        except ValueError:
            raise HTTPException(404, "request_missing")
        value = {
            "id": row.id,
            "received_at": row.received_at.isoformat(),
            "state": row.state,
            "reason": row.reason,
            "request": c.reveal(row.payload_encrypted),
            "draft": c.reveal(row.draft_encrypted) if row.draft_encrypted else None,
            "outbox": c.reveal(row.outbox_encrypted) if row.outbox_encrypted else None,
            "sources": row.dossier.get("sources", []),
            "receipt": row.receipt,
        }
    return Response(
        json.dumps(value, ensure_ascii=False, indent=2),
        media_type="application/json",
        headers={
            **HEADERS,
            "Content-Disposition": 'attachment; filename="noyaru-support-'
            + row.id
            + '.json"',
        },
    )


@router.post("/cron/own-support")
def cron(request: Request):
    if not c.module()._cron_auth_check(request):
        return JSONResponse({"ok": False}, status_code=401)
    try:
        count = support.tick()
    except SQLAlchemyError:
        return JSONResponse({"ok": False}, status_code=503)
    return JSONResponse(
        {"ok": True, "examined": count}, headers={"Cache-Control": "no-store"}
    )
