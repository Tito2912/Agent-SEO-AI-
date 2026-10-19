"""CMS guidance and an explicitly approved, journaled WordPress/Yoast connector."""
from __future__ import annotations

import html
import json
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlsplit

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy import select

try:
    from . import cms_guidance, correction_guard, first_correction, wordpress_cms as wp
    from .models import AuditLog, CmsChange, IssueTask, JobRecord, Project
except ImportError:
    import cms_guidance
    import correction_guard
    import first_correction
    import wordpress_cms as wp
    from models import AuditLog, CmsChange, IssueTask, JobRecord, Project

router = APIRouter()


def app_module():
    if __package__:
        from . import app
    else:
        import app
    return app


def connection_key(project):
    return "CMS_WORDPRESS:" + str(project.id)


def credentials(project):
    m = app_module()
    site = cms_guidance.settings(project.settings)
    if site["platform"] != "wordpress" or site["seo_plugin"] not in {"", "yoast"}:
        raise wp.CmsError("Choisissez WordPress avec Yoast pour utiliser cette connexion.")
    row = m._user_connection_row(user_id=str(project.owner_user_id), key=connection_key(project))
    if row is None:
        raise wp.CmsError("Connectez WordPress pour préparer cette correction.")
    try:
        value = json.loads(m._decrypt_secret(str(row.secret_value)))
        value["site_url"] = wp.site_url(value["site_url"], project.base_url)
        if not value.get("username") or not value.get("password"):
            raise ValueError()
        return value
    except Exception:
        raise wp.CmsError("La connexion WordPress doit être rétablie.") from None


def fingerprint_key():
    key = app_module()._safe_env("SEO_AGENT_SECRET_KEY")
    if not key:
        raise wp.CmsError("La connexion WordPress n’est pas disponible pour le moment.")
    return key.encode()


def connection_state(project):
    m = app_module()
    row = m._user_connection_row(user_id=str(project.owner_user_id), key=connection_key(project))
    return {"connected": bool(row), "label": str((row.meta or {}).get("label") or "WordPress + Yoast") if row else ""}


def redirect(slug, *, msg="", err="", path="cms"):
    m = app_module()
    return RedirectResponse(m._path_with_flash(f"/projects/{slug}/{path}", msg=msg, err=err), status_code=303)


def task_for_project(project, task_id):
    m = app_module()
    with m.DB.session() as db:
        task = db.scalar(select(IssueTask).where(IssueTask.project_id == str(project.id), IssueTask.id == task_id))
    if task is None:
        raise HTTPException(404, "correction_not_found")
    if not cms_guidance.field_for_issue(task.issue_key):
        raise wp.CmsError("Cette intégration prend en charge les titres SEO et méta-descriptions des pages et articles publiés.")
    runs = m._runs_dir_for_user(str(project.owner_user_id))
    if task.crawl_ts not in m.dash.list_project_crawls(runs, project.slug):
        raise wp.CmsError("Le rapport de référence n’est plus disponible. Choisissez une correction dans une nouvelle analyse.")
    report = m.dash.load_report_json(runs, project.slug, task.crawl_ts) or {}
    issue = (report.get("issues") or {}).get(task.issue_key)
    if not isinstance(issue, dict) or int(issue.get("count") or 0) <= 0 or task.url not in m.dash.extract_impacted_pages(task.issue_key, issue):
        raise wp.CmsError("Cette page n’est pas identifiée dans le rapport de référence de la correction.")
    return task


@router.get("/projects/{slug}/cms")
def cms_settings(request: Request, slug: str, msg: str = "", err: str = ""):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    with m.DB.session() as db:
        history = list(db.scalars(select(CmsChange).where(CmsChange.project_id == str(project.id))
                                 .order_by(CmsChange.created_at.desc()).limit(20)))
    response = m.templates.TemplateResponse("cms_settings.html", {
        "request": request, "project": project, "slug": slug, "msg": msg, "err": err,
        "site": cms_guidance.settings(project.settings), "platforms": cms_guidance.PLATFORMS,
        "seo_plugins": cms_guidance.SEO_PLUGINS, "wordpress": connection_state(project), "history": history,
    })
    response.headers["Cache-Control"] = "no-store"
    return response


@router.post("/projects/{slug}/cms/platform")
def cms_platform(request: Request, slug: str, platform: str = Form(default=""), seo_plugin: str = Form(default=""), next: str = Form(default="")):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    if platform not in cms_guidance.PLATFORMS or seo_plugin not in cms_guidance.SEO_PLUGINS:
        raise HTTPException(400, "unknown_cms")
    try:
        with correction_guard.correction_lock(m.DB, str(project.owner_user_id)):
            with m.DB.session() as db:
                row = db.get(Project, project.id)
                previous = cms_guidance.settings(row.settings)
                row.settings = {**(row.settings or {}), "cms": {"platform": platform, "seo_plugin": seo_plugin if platform == "wordpress" else ""}}
                db.commit()
            if previous["platform"] == "wordpress" and (platform != "wordpress" or seo_plugin not in {"", "yoast"}):
                m._delete_user_connection(user_id=str(project.owner_user_id), key=connection_key(project))
    except (correction_guard.CorrectionBusy, correction_guard.CorrectionStoreUnavailable):
        return redirect(slug, err="Une opération est en cours. Réessayez dans un instant.")
    destination = m._safe_next_path(next or f"/projects/{slug}/cms")
    return RedirectResponse(m._path_with_flash(destination, msg="Plateforme enregistrée. Les instructions sont adaptées à votre site."), status_code=303)


@router.post("/projects/{slug}/cms/wordpress/connect")
def wordpress_connect(request: Request, slug: str, site_url: str = Form(...), username: str = Form(...), password: str = Form(...)):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    try:
        if not username.strip() or len(username) > 200 or len(password) > 200 or ":" in username or any(c in username + password for c in "\r\n\0") or not password.strip():
            raise wp.CmsError("Renseignez votre identifiant WordPress et un mot de passe d’application valide.", code="saisie")
        c = {"site_url": wp.site_url(site_url, project.base_url), "username": username.strip(), "password": password.strip()}
        with correction_guard.correction_lock(m.DB, str(project.owner_user_id)):
            label = wp.WordPressClient(c).check()
            m._upsert_user_connection(user_id=str(project.owner_user_id), key=connection_key(project),
                                      value=json.dumps(c), meta={"label": label, "provider": "wordpress_yoast"})
            with m.DB.session() as db:
                row = db.get(Project, project.id)
                row.settings = {**(row.settings or {}), "cms": {"platform": "wordpress", "seo_plugin": "yoast"}}
                db.commit()
        m._audit_log(request, action="cms.wordpress.connect", status="ok", target_type="project", target_id=slug)
        return redirect(slug, msg="WordPress est connecté. Vous pouvez préparer une correction de titre ou de description.")
    except wp.CmsError as error:
        # The pilot needs recent WordPress and Yoast: count who hits which wall.
        m._audit_log(request, action="cms.wordpress.connect", status="declined", target_type="project",
                     target_id=slug, meta={"reason": error.code or "autre"})
        return redirect(slug, err=str(error))
    except (correction_guard.CorrectionBusy, correction_guard.CorrectionStoreUnavailable):
        return redirect(slug, err="Une opération est en cours sur ce compte. Réessayez dans un instant.")
    except Exception:
        m._audit_log(request, action="cms.wordpress.connect", status="error", target_type="project",
                     target_id=slug, meta={"reason": "enregistrement"})
        return redirect(slug, err="La connexion n’a pas pu être enregistrée. Réessayez dans un instant.")


def connection_report(days=30):
    """Connection attempts for the operations page: outcome, and why WordPress declined."""
    m = app_module()
    since = datetime.now(timezone.utc) - timedelta(days=days)
    with m.DB.session() as db:
        rows = list(db.scalars(select(AuditLog).where(AuditLog.action == "cms.wordpress.connect",
                                                      AuditLog.created_at >= since)))
    reasons = {}
    for row in rows:
        if row.status != "ok":
            code = str((row.meta or {}).get("reason") or "autre")
            reasons[code] = reasons.get(code, 0) + 1
    return {
        "days": days,
        "connected": sum(1 for row in rows if row.status == "ok"),
        "declined": sum(reasons.values()),
        "projects": len({row.target_id for row in rows if row.target_id}),
        "reasons": [{"code": code, "label": wp.REASONS.get(code, code), "count": count}
                    for code, count in sorted(reasons.items(), key=lambda item: -item[1])],
    }


@router.post("/projects/{slug}/cms/wordpress/disconnect")
def wordpress_disconnect(request: Request, slug: str):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    try:
        with correction_guard.correction_lock(m.DB, str(project.owner_user_id)):
            m._delete_user_connection(user_id=str(project.owner_user_id), key=connection_key(project))
    except (correction_guard.CorrectionBusy, correction_guard.CorrectionStoreUnavailable):
        return redirect(slug, err="Une opération est en cours. Réessayez dans un instant.")
    m._audit_log(request, action="cms.wordpress.disconnect", status="ok", target_type="project", target_id=slug)
    return redirect(slug, msg="Connexion retirée de Noyaru pour ce projet. Vous pouvez aussi révoquer le mot de passe d’application dans votre profil WordPress.")


@router.get("/projects/{slug}/cms/wordpress/edit")
def wordpress_edit(request: Request, slug: str, task_id: str):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    try:
        task = task_for_project(project, task_id)
        page = wp.WordPressClient(credentials(project)).read(task.url)
        field = cms_guidance.field_for_issue(task.issue_key)
        response = m.templates.TemplateResponse("cms_wordpress_edit.html", {
            "request": request, "project": project, "slug": slug, "task": task, "page": page,
            "field": field, "field_label": "Titre SEO" if field == "seo_title" else "Méta-description",
        })
        response.headers["Cache-Control"] = "no-store"
        return response
    except wp.CmsError as error:
        return redirect(slug, err=str(error))


@router.post("/projects/{slug}/cms/wordpress/preview")
def wordpress_preview(request: Request, slug: str, task_id: str = Form(...), value: str = Form(...)):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    try:
        task = task_for_project(project, task_id)
        c = credentials(project)
        page = wp.WordPressClient(c).read(task.url)
        value = " ".join(value.split())
        field = cms_guidance.field_for_issue(task.issue_key)
        if not value or len(value) > (400 if field == "seo_title" else 2000) or "<" in value or ">" in value or "\0" in value:
            raise wp.CmsError("Saisissez un texte simple, sans balise HTML, pour préparer la correction.")
        before = wp.values(page)
        if before[field] == value:
            raise wp.CmsError("Cette valeur est déjà enregistrée dans WordPress.")
        with m.DB.session() as db:
            change = CmsChange(project_id=str(project.id), task_id=str(task.id), created_by=str(request.state.user.id),
                               state="preview", data={"url": task.url, "post_id": page["post_id"], "field": field,
                               "before": before, "after": {**before, field: value}, "connection": wp.fingerprint(c, fingerprint_key()),
                               "rendered_before": page.get(field + "_rendered")})
            db.add(change)
            db.commit()
            change_id = str(change.id)
        return redirect(slug, path=f"cms/wordpress/changes/{change_id}")
    except wp.CmsError as error:
        return redirect(slug, err=str(error))


def load_change(project, change_id):
    m = app_module()
    with m.DB.session() as db:
        row = db.scalar(select(CmsChange).where(CmsChange.id == change_id, CmsChange.project_id == str(project.id)))
    if row is None:
        raise HTTPException(404, "cms_change_not_found")
    return row


@router.get("/projects/{slug}/cms/wordpress/changes/{change_id}")
def wordpress_change(request: Request, slug: str, change_id: str, err: str = ""):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    change = load_change(project, change_id)
    verification = verification_state(project, change)
    response = m.templates.TemplateResponse("cms_wordpress_change.html", {
        "request": request, "project": project, "slug": slug, "change": change, "err": err,
        "field_label": "Titre SEO" if change.data["field"] == "seo_title" else "Méta-description",
        "verification": verification,
        "published": published_value(project, change, verification["receipt"])
                     if verification["state"] == "still_present" else {},
    })
    response.headers["Cache-Control"] = "no-store"
    return response


def project_crawl_job(project, job_id):
    m = app_module()
    job = m._load_job(str(job_id)) if job_id else None
    result = job.result if job and isinstance(job.result, dict) else {}
    if (result.get("type") != "crawl" or result.get("slug") != project.slug
            or str(result.get("user_id") or "") != str(project.owner_user_id)):
        return None
    m._finalize_stale_job(job)
    return job


def verification_state(project, change):
    """A successful WordPress write or finished job cannot stand in for a crawl receipt."""
    state = {"state": "pending", "poll": False, "job_id": "", "receipt": {}}
    if change.state != "applied":
        return state
    m = app_module()
    with m.DB.session() as db:
        task = db.get(IssueTask, change.task_id)
    note = first_correction.note_data(task.note) if task and str(task.project_id) == str(project.id) else {}
    if note.get("cms_change_id") != str(change.id):
        return {**state, "state": "superseded"}
    journey = note.get("first_correction") if isinstance(note.get("first_correction"), dict) else {}
    declared = first_correction.timestamp(journey.get("declared_at"))
    with m.DB.session() as db:
        scoped = select(JobRecord).where(JobRecord.owner_user_id == str(project.owner_user_id),
                                         JobRecord.slug == project.slug, JobRecord.kind == "crawl")
        active = db.scalar(scoped.where(JobRecord.status.in_(["queued", "running", "cancel_requested"]))
                           .order_by(JobRecord.created_at.desc()).limit(1))
        recent = db.scalar(scoped.where(JobRecord.created_at >= declared.timestamp())
                           .order_by(JobRecord.created_at.desc()).limit(1)) if declared else None
    bound_job = project_crawl_job(project, change.data.get("verification_job_id"))
    job = project_crawl_job(project, active.id if active else None) or bound_job or project_crawl_job(project, recent.id if recent else None)
    if job:
        state["job_id"] = str(job.id)
    receipt = note.get("verify") if isinstance(note.get("verify"), dict) else {}
    crawled = first_correction.timestamp(receipt.get("crawl_ts"))
    received = first_correction.timestamp(receipt.get("verified_at"))
    fresh = bool(declared and received and received >= declared
                 and (not bound_job or received.timestamp() >= bound_job.created_at))
    if (fresh and crawled and crawled > declared and receipt.get("result") == "resolved"
            and first_correction.progress([task]).get("verified")):
        return {**state, "state": "verified", "receipt": receipt}
    if job and job.status in {"queued", "running", "cancel_requested"}:
        return {**state, "state": "running", "poll": True}
    if fresh and receipt.get("result") in {"still_present", "unverified"}:
        return {**state, "state": receipt["result"], "receipt": receipt}
    if job and job.status in {"failed", "canceled"}:
        return {**state, "state": "failed"}
    if job and job.status == "done":
        # The worker closes the crawl before writing correction receipts. Keep following
        # that short interval, then expose a retry instead of spinning indefinitely.
        finished = job.finished_at or job.updated_at or job.created_at
        if datetime.now(timezone.utc).timestamp() - finished < 90:
            return {**state, "state": "finishing", "poll": True}
        return {**state, "state": "unverified"}
    return state


def _same_text(left, right):
    def norm(value):
        return " ".join(html.unescape(str(value or "")).split()).casefold()
    return bool(norm(left)) and norm(left) == norm(right)


def published_value(project, change, receipt):
    """What the control crawl actually saw, against what WordPress holds.

    WordPress confirmed the new value on write, so a crawl still reading the OLD rendered value
    is served a stale page: a cache plugin or a CDN, not a failed correction. Read only for the
    change page (not the status poll), since it loads the control report."""
    m = app_module()
    field = change.data["field"]
    report = m.dash.load_report_json(m._runs_dir_for_user(str(project.owner_user_id)), project.slug,
                                     str(receipt.get("crawl_ts") or "")) or {}
    target = m._verification_url(change.data["url"])
    row = next((page for page in report.get("pages") or [] if isinstance(page, dict)
                and target in {m._verification_url(str(page.get(key) or "")) for key in ("url", "final_url")}), None)
    if row is None:
        return {}
    seen = row.get("title" if field == "seo_title" else "meta_description")
    if _same_text(seen, change.data.get("rendered_before") or change.data["before"].get(field)):
        if not _same_text(seen, change.data["after"][field]):
            return {"kind": "stale", "seen": seen}
    if _same_text(seen, change.data["after"][field]):
        return {"kind": "live", "seen": seen}
    return {"kind": "other", "seen": seen or ""}


@router.get("/projects/{slug}/cms/wordpress/changes/{change_id}/status")
def wordpress_change_status(request: Request, slug: str, change_id: str):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    state = verification_state(project, load_change(project, change_id))
    return JSONResponse({"state": state["state"], "poll": state["poll"]}, headers={"Cache-Control": "no-store"})


def start_verification(request, project, change):
    m = app_module()
    path = f"cms/wordpress/changes/{change.id}"
    if change.state != "applied":
        raise wp.CmsError("Appliquez la correction avant de vérifier son résultat.")
    state = verification_state(project, change)
    if state["poll"] or state["state"] == "verified":
        return redirect(project.slug, path=path)
    if state["state"] == "superseded":
        raise wp.CmsError("Une autre opération a remplacé le suivi de cette correction. Reprenez votre correction actuelle.")
    # Reuse the actual crawl entry point: owner billing, quota, rate limit and worker
    # settings remain identical to a normal analysis. No WordPress write is performed.
    response = m.crawl_project(request, project.slug, config_path=str(m.DEFAULT_CONFIG))
    if isinstance(response, RedirectResponse):
        params = parse_qs(urlsplit(response.headers["location"]).query)
        job_id = (params.get("job") or [""])[0]
        error = (params.get("err") or [""])[0]
    else:
        payload = json.loads(response.body)
        job_id, error = payload.get("job_id", ""), payload.get("error", "")
    if not project_crawl_job(project, job_id):
        raise wp.CmsError(error or "La vérification n’a pas pu démarrer. Réessayez dans un instant.")
    with m.DB.session() as db:
        row = db.get(CmsChange, change.id)
        row.data = {**row.data, "verification_job_id": job_id}
        db.commit()
    return redirect(project.slug, path=path)


def save_state(change_id, state, **data):
    m = app_module()
    with m.DB.session() as db:
        row = db.get(CmsChange, change_id)
        row.state = state
        row.data = {**row.data, **data}
        db.commit()


def confirmed(change, state):
    m = app_module()
    with m.DB.session() as db:
        row = db.get(CmsChange, change.id)
        row.state = state
        row.data = {key: value for key, value in row.data.items() if key != "verification_job_id"}
        task = db.get(IssueTask, row.task_id)
        note = first_correction.note_data(task.note)
        if task.note and not note:
            note["user_note"] = task.note
        journey = note.get("first_correction") if isinstance(note.get("first_correction"), dict) else {}
        note["first_correction"] = {**journey, "declared_at": datetime.now(timezone.utc).isoformat()}
        note["cms_change_id"] = str(row.id)
        note.pop("verify", None)
        task.status = "in_progress"
        task.note = json.dumps(note, ensure_ascii=False)
        db.commit()


@router.post("/projects/{slug}/cms/wordpress/changes/{change_id}/{action}")
def wordpress_change_action(request: Request, slug: str, change_id: str, action: str):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    if action not in {"apply", "restore", "check", "verify"}:
        raise HTTPException(400, "unknown_cms_action")
    path = f"cms/wordpress/changes/{change_id}"
    sending = False
    try:
        with correction_guard.correction_lock(m.DB, str(project.owner_user_id)):
            change = load_change(project, change_id)
            if action == "verify":
                return start_verification(request, project, change)
            c = credentials(project)
            key = fingerprint_key()
            if not wp.same_connection(c, change.data["connection"], key):
                raise wp.CmsError("La connexion WordPress a changé. Préparez un nouvel aperçu avec la connexion actuelle.")
            client = wp.WordPressClient(c)
            live = client.read(change.data["url"])
            if live["post_id"] != change.data["post_id"]:
                raise wp.CmsError("Cette adresse ne correspond plus à la page prévue dans l’aperçu.")
            current = wp.values(live)
            if action == "check":
                if change.state not in {"applying", "restoring", "unknown"}:
                    return redirect(slug, path=path)
                pending = change.data.get("pending") or "apply"
                expected = change.data["after" if pending == "apply" else "before"]
                if current != expected:
                    raise wp.CmsError("Les valeurs attendues ne sont pas confirmées. Vérifiez la page dans WordPress ; aucune nouvelle écriture n’est relancée automatiquement.")
                confirmed(change, "applied" if pending == "apply" else "restored")
                return redirect(slug, path=path)
            if (action == "apply" and change.state == "applied") or (action == "restore" and change.state == "restored"):
                return redirect(slug, path=path)
            if change.state != ("preview" if action == "apply" else "applied"):
                raise wp.CmsError("Cette opération ne peut pas être relancée. Vérifiez son état avant de poursuivre.")
            created = change.created_at.replace(tzinfo=timezone.utc) if change.created_at.tzinfo is None else change.created_at
            if action == "apply" and datetime.now(timezone.utc) - created > timedelta(minutes=20):
                raise wp.CmsError("Cet aperçu a expiré. Préparez un nouvel aperçu avec les valeurs actuelles de la page.")
            expected = change.data["before" if action == "apply" else "after"]
            if current != expected:
                raise wp.CmsError("La page a changé depuis l’aperçu. L’opération est arrêtée pour conserver les modifications actuelles.")
            field = change.data["field"]
            destination = change.data["after" if action == "apply" else "before"]
            save_state(change.id, "applying" if action == "apply" else "restoring", pending=action,
                       connection=wp.fingerprint(c, key))
            correction_guard.ensure_active()
            sending = True
            result = client.update(live, {field: destination[field]})
            if wp.values(result) != destination:
                raise wp.CmsError("WordPress a renvoyé des valeurs différentes de celles attendues.", uncertain=True)
            confirmed(change, "applied" if action == "apply" else "restored")
        m._audit_log(request, action=f"cms.wordpress.{action}", status="ok", target_type="project", target_id=slug,
                     meta={"change_id": change_id})
        return redirect(slug, path=path)
    except wp.CmsError as error:
        if sending:
            save_state(change_id, "unknown")
        return redirect(slug, path=path, err=str(error))
    except (correction_guard.CorrectionBusy, correction_guard.CorrectionStoreUnavailable):
        return redirect(slug, path=path, err="Une opération est en cours ou le suivi n’est pas disponible. Vérifiez l’état avant de poursuivre.")
    except HTTPException:
        raise
    except Exception:
        # The persisted applying/restoring state fences retries after a lost response or DB failure.
        return redirect(slug, path=path, err="Le résultat n’a pas pu être enregistré. Vérifiez l’état de la page avant de poursuivre.")
