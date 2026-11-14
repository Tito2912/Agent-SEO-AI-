"""Private media preview. Only Telegram can authorize production and final review."""

import uuid
from urllib.parse import urlencode

from fastapi import APIRouter, Request, Form, BackgroundTasks, HTTPException, Query
from fastapi.responses import RedirectResponse, Response, JSONResponse
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

try:
    from . import own_social_media as media, i18n, own_seo_routes
    from .own_social_media_copy import strings
except ImportError:
    import own_social_media as media
    import i18n
    import own_seo_routes
    from own_social_media_copy import strings

router = APIRouter()
HEADERS = {
    "Cache-Control": "no-store",
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
}


def redirect(ident="", error=""):
    return RedirectResponse(
        media.PATH
        + "?"
        + urlencode({"media": ident, "err": error, "lang": i18n.current_lang()}),
        status_code=303,
        headers=HEADERS,
    )


def notify():
    try:
        media.approvals().sync()
        media.approvals().deliver_one()
    except (ValueError, SQLAlchemyError):
        pass  # Durable candidate remains visible to the receiver and in the owner's page.


@router.get("/settings/system/social/media")
def page(
    request: Request,
    media_id: str = Query("", alias="media", max_length=36),
    goal: str = "discovery",
    audience: str = "freelance",
    err: str = "",
):
    m = media
    user = m.s.module()._require_system_owner(request)
    row, packet, recent, issue, unavailable = None, None, [], "", False
    try:
        with m.s.module().DB.session() as db:
            recent = list(
                db.execute(
                    select(m.Media.id, m.Media.state, m.Media.created_at)
                    .where(m.Media.owner_id == user.id)
                    .order_by(m.Media.created_at.desc())
                    .limit(10)
                )
            )
            if media_id:
                row = m.scope(db, user.id, media_id, current=False)
                packet = row.packet
                if row.state not in {"cancelled", "stopped", "uncertain"}:
                    try:
                        m.scope(db, user.id, row.id)
                        if (
                            row.state != "approved"
                            and m.s.seo_watch.utc(row.expires_at) <= m.s.clock()
                        ):
                            raise ValueError("expired")
                    except ValueError as exc:
                        issue = str(exc)
            else:
                packet = m.packet(db, i18n.current_lang(), goal, audience)
            if not row:
                try:
                    channel = m.approvals().active(db)
                    if channel.owner_id != user.id:
                        raise ValueError("binding_changed")
                    m.credentials()
                    m.render.executable()
                except ValueError as exc:
                    issue = str(exc)
    except ValueError as exc:
        if str(exc) == "missing":
            raise HTTPException(404, "media_missing")
        issue = str(exc)
    except SQLAlchemyError:
        unavailable = True
    copy = strings(i18n.current_lang())
    error = (
        copy["errors"].get(err or issue, copy["errors"]["changed"])
        if err or issue
        else ""
    )
    return m.s.module().templates.TemplateResponse(
        "settings_own_social_media.html",
        dict(
            request=request,
            project=None,
            copy=copy,
            item=row,
            packet=packet,
            recent=recent,
            error=error,
            unavailable=unavailable,
            blocked=bool(issue),
            base=m.PATH,
            key=uuid.uuid4().hex,
            lang=i18n.current_lang(),
        ),
        status_code=503 if unavailable else 200,
        headers=HEADERS,
    )


@router.post("/settings/system/social/media/prepare")
def prepare(
    request: Request,
    background: BackgroundTasks,
    goal: str = Form("discovery"),
    audience: str = Form("freelance"),
    key: str = Form(""),
    confirm: bool = Form(False),
):
    user = media.s.module()._require_system_owner(request)
    own_seo_routes.throttle(request)
    try:
        ident = media.prepare(
            user.id, i18n.current_lang(), goal, audience, key, confirm
        )
        background.add_task(notify)
        return redirect(ident)
    except ValueError as exc:
        return redirect(error=str(exc))
    except SQLAlchemyError:
        return redirect(error="unavailable")


@router.post("/settings/system/social/media/{ident}/cancel")
def cancel(request: Request, ident: str):
    user = media.s.module()._require_system_owner(request)
    own_seo_routes.throttle(request)
    try:
        with media.s.module().DB.session() as db:
            media.s.locked(db)
            row = media.scope(db, user.id, ident, current=False)
            row.state = "cancelled"
            row.lease_token = None
            row.result = {}
            media.audit(db, row, "cancelled")
            db.commit()
        return redirect(ident)
    except ValueError:
        raise HTTPException(404, "media_missing")
    except SQLAlchemyError:
        raise HTTPException(503, "media_unavailable")


@router.get("/settings/system/social/media/frame/{index}")
def frame(
    request: Request, index: int, goal: str = "discovery", audience: str = "freelance"
):
    media.s.module()._require_system_owner(request)
    if index not in range(3):
        raise HTTPException(404, "frame_missing")
    try:
        with media.s.module().DB.session() as db:
            value = media.packet(db, i18n.current_lang(), goal, audience)
        return Response(
            media.render.frame(value, index), media_type="image/png", headers=HEADERS
        )
    except ValueError:
        raise HTTPException(409, "captures_changed")
    except SQLAlchemyError:
        raise HTTPException(503, "media_unavailable")


@router.get("/settings/system/social/media/{ident}/video")
def video(request: Request, ident: str):
    user = media.s.module()._require_system_owner(request)
    try:
        with media.s.module().DB.session() as db:
            row = media.scope(db, user.id, ident, current=False)
            data = media.video(row)
        return Response(
            data,
            media_type="video/mp4",
            headers={
                **HEADERS,
                "Content-Disposition": 'inline; filename="noyaru-demo.mp4"',
            },
        )
    except ValueError as exc:
        raise HTTPException(404 if str(exc) == "missing" else 409, "video_unavailable")
    except SQLAlchemyError:
        raise HTTPException(503, "media_unavailable")


@router.get("/settings/system/social/media/{ident}/status")
def status(request: Request, ident: str):
    user = media.s.module()._require_system_owner(request)
    try:
        with media.s.module().DB.session() as db:
            media.s.actor(db, user.id)
            row = db.execute(
                select(media.Media.state, media.Media.error_code).where(
                    media.Media.id == ident, media.Media.owner_id == user.id
                )
            ).first()
            if not row:
                raise ValueError("missing")
            return JSONResponse(
                {"state": row.state, "error": row.error_code}, headers=HEADERS
            )
    except ValueError:
        raise HTTPException(404, "media_missing")
    except SQLAlchemyError:
        raise HTTPException(503, "media_unavailable")
