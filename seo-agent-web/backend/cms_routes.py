"""CMS guidance and an explicitly approved, journaled WordPress/Yoast connector."""
from __future__ import annotations

import html
import json
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlencode, urlsplit

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

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
    return task, report


# Families where Noyaru writes the value itself, with the corrector's own length writer. A
# duplicate needs a text that differs from OTHER pages, which that writer cannot see: the
# customer writes those.
SUGGESTED = {"missing_title", "title_too_long", "title_too_short",
             "missing_meta_description", "meta_description_too_long", "meta_description_too_short"}


def issue_family(key):
    return re.sub(r"_(?:not_)?indexable$", "", key or "")


def page_context(report, url, post_title):
    """What the page is about, from the crawl that flagged it: the writer must not invent."""
    m = app_module()
    target = m._verification_url(url)
    row = next((page for page in report.get("pages") or [] if isinstance(page, dict)
                and target in {m._verification_url(str(page.get(key) or "")) for key in ("url", "final_url")}), {})
    parts = [("Titre de l’article", post_title), ("H1", " / ".join(row.get("h1") or [])),
             ("Intertitres", " / ".join((row.get("h2") or [])[:8])), ("Description sociale", row.get("og_description"))]
    return ("\n".join(f"{label} : {value}" for label, value in parts if value),
            str(row.get("lang") or "")[:2].lower())


def create_preview(request, project, task, c, page, field, value, *, suggested=False):
    m = app_module()
    before = wp.values(page)
    with m.DB.session() as db:
        change = CmsChange(project_id=str(project.id), task_id=str(task.id), created_by=str(request.state.user.id),
                           state="preview", data={"url": task.url, "post_id": page["post_id"], "field": field,
                           "before": before, "after": {**before, field: value}, "connection": wp.fingerprint(c, fingerprint_key()),
                           "rendered_before": page.get(field + "_rendered"), **({"suggested": True} if suggested else {})})
        db.add(change)
        db.commit()
        return str(change.id)


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
def wordpress_edit(request: Request, slug: str, task_id: str, change: str = "", err: str = ""):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    try:
        task, _ = task_for_project(project, task_id)
        page = wp.WordPressClient(credentials(project)).read(task.url)
        field = cms_guidance.field_for_issue(task.issue_key)
        draft = ""
        if change:
            # Reopening a preview edits ITS text (a proposal included), not WordPress's current one.
            row = load_change(project, change)
            if str(row.task_id) == str(task.id) and row.state == "preview":
                draft = str(row.data["after"].get(field) or "")
        response = m.templates.TemplateResponse("cms_wordpress_edit.html", {
            "request": request, "project": project, "slug": slug, "task": task, "page": page,
            "field": field, "field_label": "Titre SEO" if field == "seo_title" else "Méta-description",
            "draft": draft, "err": err, "can_suggest": issue_family(task.issue_key) in SUGGESTED,
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
        task, _ = task_for_project(project, task_id)
        c = credentials(project)
        page = wp.WordPressClient(c).read(task.url)
        value = " ".join(value.split())
        field = cms_guidance.field_for_issue(task.issue_key)
        if not value or len(value) > (400 if field == "seo_title" else 2000) or "<" in value or ">" in value or "\0" in value:
            raise wp.CmsError("Saisissez un texte simple, sans balise HTML, pour préparer la correction.")
        if wp.values(page)[field] == value:
            raise wp.CmsError("Cette valeur est déjà enregistrée dans WordPress.")
        change_id = create_preview(request, project, task, c, page, field, value)
        return redirect(slug, path=f"cms/wordpress/changes/{change_id}")
    except wp.CmsError as error:
        return redirect(slug, err=str(error))


def propose_value(request, project, task, report):
    """Write one value with the corrector's length writer, bill it, land it as a preview.

    Runs under the payer's correction lock. Raises CmsError — `code="plafond"` when the plan has
    no correction left, which is what stops a batch instead of failing its pages one by one."""
    m = app_module()
    user = request.state.user
    family = issue_family(task.issue_key)
    if family not in SUGGESTED:
        raise wp.CmsError("Noyaru ne rédige pas ce texte : il doit se distinguer des autres pages. Écrivez une valeur propre à cette page.")
    allowed, message, _, model = m._correction_gate(user, slug=project.slug)
    if not allowed:
        raise wp.CmsError(message, code="plafond")
    c = credentials(project)
    page = wp.WordPressClient(c).read(task.url)
    field = cms_guidance.field_for_issue(task.issue_key)
    kind = "title" if field == "seo_title" else "description"
    current = "" if family.startswith("missing_") else str(page.get(field) or page.get(field + "_rendered") or "")
    contexte, langue = page_context(report, task.url, page.get("post_title"))
    value = " ".join(str(m._length_value_for_page(
        current=current, kind=kind, url=task.url, site_name=project.site_name or project.slug,
        model_override=model, contexte=contexte, langue=langue) or "").split())
    if (not value or "<" in value or ">" in value or value == wp.values(page)[field]
            or not m._LENGTH_FLOORS[kind] <= m._rendered_len(value) <= m._LENGTH_CEILINGS[kind]):
        raise wp.CmsError("Noyaru n’a pas trouvé de texte fidèle à la page dans la bonne longueur. Rien n’a été décompté : écrivez-le vous-même.")
    m._correction_charge(user, 1, slug=project.slug, motif=task.issue_key)
    return create_preview(request, project, task, c, page, field, value, suggested=True)


@router.post("/projects/{slug}/cms/wordpress/suggest")
def wordpress_suggest(request: Request, slug: str, task_id: str = Form(...)):
    """Noyaru writes the value, as the corrector does in a repository, and bills one correction.

    The proposal lands as an ordinary preview: nothing reaches WordPress before the customer
    reads it and approves. A value that misses the crawler's bounds is not offered, nor billed."""
    m = app_module()
    project = m._db_project_or_404(request, slug)
    try:
        task, report = task_for_project(project, task_id)
        with correction_guard.correction_lock(m.DB, str(project.owner_user_id)):
            change_id = propose_value(request, project, task, report)
        return redirect(slug, path=f"cms/wordpress/changes/{change_id}")
    except wp.CmsError as error:
        return redirect(slug, path="cms/wordpress/edit?" + urlencode({"task_id": task_id}), err=str(error))
    except (correction_guard.CorrectionBusy, correction_guard.CorrectionStoreUnavailable):
        return redirect(slug, path="cms/wordpress/edit?" + urlencode({"task_id": task_id}),
                        err="Une opération est en cours sur ce compte. Réessayez dans un instant.")


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


def launch_control_crawl(request, project):
    # Reuse the actual crawl entry point: owner billing, quota, rate limit and worker
    # settings remain identical to a normal analysis. No WordPress write is performed.
    m = app_module()
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
    return job_id


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
    job_id = launch_control_crawl(request, project)
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


def write_change(change, action, client, live, c, key, *, approved_at=None):
    """Apply or restore one change on the page just read (`live`), fenced as always.

    `approved_at`: when the customer approved a batch screen, which may be later than the
    preview itself; the 20-minute freshness runs from the latest of the two. A write already
    sent that fails is left "unknown", never retried."""
    current = wp.values(live)
    if (action == "apply" and change.state == "applied") or (action == "restore" and change.state == "restored"):
        return False
    if change.state != ("preview" if action == "apply" else "applied"):
        raise wp.CmsError("Cette opération ne peut pas être relancée. Vérifiez son état avant de poursuivre.")
    created = change.created_at.replace(tzinfo=timezone.utc) if change.created_at.tzinfo is None else change.created_at
    if action == "apply" and datetime.now(timezone.utc) - max(created, approved_at or created) > timedelta(minutes=20):
        raise wp.CmsError("Cet aperçu a expiré. Préparez un nouvel aperçu avec les valeurs actuelles de la page.")
    expected = change.data["before" if action == "apply" else "after"]
    if current != expected:
        raise wp.CmsError("La page a changé depuis l’aperçu. L’opération est arrêtée pour conserver les modifications actuelles.")
    field = change.data["field"]
    destination = change.data["after" if action == "apply" else "before"]
    save_state(change.id, "applying" if action == "apply" else "restoring", pending=action,
               connection=wp.fingerprint(c, key))
    correction_guard.ensure_active()
    try:
        result = client.update(live, {field: destination[field]})
        if wp.values(result) != destination:
            raise wp.CmsError("WordPress a renvoyé des valeurs différentes de celles attendues.", uncertain=True)
    except wp.CmsError:
        save_state(change.id, "unknown")
        raise
    confirmed(change, "applied" if action == "apply" else "restored")
    return True


@router.post("/projects/{slug}/cms/wordpress/changes/{change_id}/{action}")
def wordpress_change_action(request: Request, slug: str, change_id: str, action: str):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    if action not in {"apply", "restore", "check", "verify"}:
        raise HTTPException(400, "unknown_cms_action")
    path = f"cms/wordpress/changes/{change_id}"
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
            if not write_change(change, action, client, live, c, key):
                return redirect(slug, path=path)
        m._audit_log(request, action=f"cms.wordpress.{action}", status="ok", target_type="project", target_id=slug,
                     meta={"change_id": change_id})
        return redirect(slug, path=path)
    except wp.CmsError as error:
        return redirect(slug, path=path, err=str(error))
    except (correction_guard.CorrectionBusy, correction_guard.CorrectionStoreUnavailable):
        return redirect(slug, path=path, err="Une opération est en cours ou le suivi n’est pas disponible. Vérifiez l’état avant de poursuivre.")
    except HTTPException:
        raise
    except Exception:
        # The persisted applying/restoring state fences retries after a lost response or DB failure.
        return redirect(slug, path=path, err="Le résultat n’a pas pu être enregistré. Vérifiez l’état de la page avant de poursuivre.")


BATCH_ROWS = 100
ACTIVE = {"preview", "applying", "restoring", "unknown", "applied"}


def batch_href(project, issue_key):
    """The batch screen, when this anomaly can be corrected there; "" otherwise."""
    # The issue page reads its project defensively (`getattr`): so does this.
    if issue_family(issue_key) not in SUGGESTED or not all(getattr(project, name, None) for name in ("id", "slug", "owner_user_id")):
        return ""
    site = cms_guidance.settings(getattr(project, "settings", None))
    if site["platform"] != "wordpress" or site["seo_plugin"] not in {"", "yoast"}:
        return ""
    if not app_module()._user_connection_row(user_id=str(project.owner_user_id), key=connection_key(project)):
        return ""
    return f"/projects/{project.slug}/cms/wordpress/issues/{issue_key}"


def latest_report(project):
    m = app_module()
    runs = m._runs_dir_for_user(str(project.owner_user_id))
    for ts in reversed(m.dash.list_project_crawls(runs, project.slug)):
        report = m.dash.load_report_json(runs, project.slug, ts)
        if isinstance(report, dict) and report:
            return ts, report
    return "", {}


def batch_scope(project, issue_key):
    """The pages of this anomaly in the latest analysis that the connected WordPress holds."""
    m = app_module()
    if issue_family(issue_key) not in SUGGESTED or not cms_guidance.field_for_issue(issue_key):
        raise HTTPException(404, "cms_batch_not_available")
    ts, report = latest_report(project)
    issue = (report.get("issues") or {}).get(issue_key)
    client = wp.WordPressClient(credentials(project))
    urls = [url for url in m.dash.extract_impacted_pages(issue_key, issue) if client.contains(url)] if isinstance(issue, dict) else []
    return ts, report, urls[:BATCH_ROWS], max(0, len(urls) - BATCH_ROWS)


def ensure_task(project, user, issue_key, url, ts):
    m = app_module()
    meta = m.dash.issue_meta(issue_key)
    with m.DB.session() as db:
        query = select(IssueTask).where(IssueTask.project_id == str(project.id), IssueTask.issue_key == issue_key,
                                        IssueTask.url == url)
        task = db.scalar(query)
        if task is None:
            db.add(IssueTask(project_id=str(project.id), user_id=str(project.owner_user_id), created_by=str(user.id),
                             issue_key=issue_key, issue_label=meta.label, url=url, crawl_ts=ts,
                             status="todo", severity=meta.severity))
            try:
                db.commit()
            except IntegrityError:
                db.rollback()
            task = db.scalar(query)
        elif task.status in {"todo", "ignored"} and task.crawl_ts != ts:
            # A new attempt starts from the analysis that flags the page now; a task in progress
            # keeps the baseline its receipt is measured against.
            task.crawl_ts, task.status = ts, "todo"
            db.commit()
        return str(task.id)


def followed(project, issue_key, urls, days=30):
    """Pages written for this anomaly lately but no longer flagged: a resolved page leaves the
    report, and its verdict must not leave the batch screen with it."""
    m = app_module()
    since = datetime.now(timezone.utc) - timedelta(days=days)
    with m.DB.session() as db:
        rows = db.execute(select(IssueTask.url).join(CmsChange, CmsChange.task_id == IssueTask.id).where(
            IssueTask.project_id == str(project.id), IssueTask.issue_key == issue_key,
            CmsChange.state.in_(["applying", "restoring", "unknown", "applied"]), CmsChange.created_at >= since))
        return [url for url in dict.fromkeys(row[0] for row in rows) if url not in urls]


def batch_rows(project, issue_key, urls):
    m = app_module()
    with m.DB.session() as db:
        tasks = {task.url: task for task in db.scalars(select(IssueTask).where(
            IssueTask.project_id == str(project.id), IssueTask.issue_key == issue_key, IssueTask.url.in_(urls)))}
        changes = {}
        for change in db.scalars(select(CmsChange).where(CmsChange.project_id == str(project.id),
                                                         CmsChange.task_id.in_([str(t.id) for t in tasks.values()]))
                                 .order_by(CmsChange.created_at.desc())):
            changes.setdefault(change.task_id, change)
    rows = []
    for url in urls:
        task = tasks.get(url)
        change = changes.get(str(task.id)) if task else None
        if change is not None and change.state not in ACTIVE:
            change = None  # restored: the page is back to square one
        rows.append({"url": url, "task": task, "change": change,
                     "verification": verification_state(project, change) if change is not None and change.state == "applied" else None})
    return rows


@router.get("/projects/{slug}/cms/wordpress/issues/{issue_key}")
def wordpress_batch(request: Request, slug: str, issue_key: str, msg: str = "", err: str = ""):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    try:
        ts, _, urls, beyond = batch_scope(project, issue_key)
    except wp.CmsError as error:
        return redirect(slug, err=str(error))
    rows = batch_rows(project, issue_key, urls + followed(project, issue_key, urls))
    plafond = m._plafond_de_correction(request.state.user, slug=slug)
    response = m.templates.TemplateResponse("cms_wordpress_batch.html", {
        "request": request, "project": project, "slug": slug, "issue_key": issue_key, "crawl_ts": ts,
        "label": m.dash.issue_meta(issue_key).label, "rows": rows, "beyond": beyond,
        "field": cms_guidance.field_for_issue(issue_key),
        "remaining": None if plafond.illimite else plafond.applique, "now": datetime.now(timezone.utc).isoformat(),
        "msg": msg, "err": err, "poll": any((row["verification"] or {}).get("poll") for row in rows),
    })
    response.headers["Cache-Control"] = "no-store"
    return response


def _batch_answer(request, slug, issue_key, *, ok, msg="", err="", **extra):
    if "application/json" in request.headers.get("accept", ""):
        return JSONResponse({"ok": ok, "error": err, **extra}, status_code=200 if ok else 422)
    return redirect(slug, path=f"cms/wordpress/issues/{issue_key}", msg=msg, err=err)


@router.post("/projects/{slug}/cms/wordpress/issues/{issue_key}/propose")
def wordpress_batch_propose(request: Request, slug: str, issue_key: str, url: str = Form(...)):
    """One page of the batch. The browser calls it page after page: forty AI writes in a single
    request would outlast any proxy, and one stuck page would take the others with it."""
    m = app_module()
    project = m._db_project_or_404(request, slug)
    try:
        ts, _, urls, _ = batch_scope(project, issue_key)
        if url not in urls:
            raise wp.CmsError("Cette page n’est pas signalée pour cette anomalie dans la dernière analyse.")
        task, report = task_for_project(project, ensure_task(project, request.state.user, issue_key, url, ts))
        with correction_guard.correction_lock(m.DB, str(project.owner_user_id)):
            change_id = propose_value(request, project, task, report)
        return _batch_answer(request, slug, issue_key, ok=True, change_id=change_id)
    except wp.CmsError as error:
        return _batch_answer(request, slug, issue_key, ok=False, err=str(error), stop=error.code == "plafond")
    except (correction_guard.CorrectionBusy, correction_guard.CorrectionStoreUnavailable):
        return _batch_answer(request, slug, issue_key, ok=False, stop=True,
                             err="Une opération est en cours sur ce compte. Réessayez dans un instant.")


@router.post("/projects/{slug}/cms/wordpress/issues/{issue_key}/apply")
def wordpress_batch_apply(request: Request, slug: str, issue_key: str, pick: list[str] = Form(default=[]),
                          ids: list[str] = Form(default=[]), values: list[str] = Form(default=[]),
                          shown_at: str = Form(default="")):
    """One approval for the selected previews; each page is still written and read back alone."""
    m = app_module()
    project = m._db_project_or_404(request, slug)
    if not pick:
        return redirect(slug, path=f"cms/wordpress/issues/{issue_key}", err="Cochez au moins une page à enregistrer.")
    edited = dict(zip(ids, values))
    shown = first_correction.timestamp(shown_at)
    approved_at = min(shown, datetime.now(timezone.utc)) if shown else None
    written, failures = 0, []
    try:
        with correction_guard.correction_lock(m.DB, str(project.owner_user_id)):
            c = credentials(project)
            key = fingerprint_key()
            client = wp.WordPressClient(c)
            for change_id in dict.fromkeys(pick):
                change = load_change(project, change_id)
                try:
                    with m.DB.session() as db:
                        task = db.get(IssueTask, change.task_id)
                    if task is None or task.issue_key != issue_key or change.state != "preview":
                        raise wp.CmsError("Cet aperçu ne fait plus partie de ce lot.")
                    field = change.data["field"]
                    value = " ".join(str(edited.get(change_id, change.data["after"][field]) or "").split())
                    if value != change.data["after"][field]:
                        if (not value or len(value) > (400 if field == "seo_title" else 2000)
                                or "<" in value or ">" in value or "\0" in value):
                            raise wp.CmsError("Saisissez un texte simple, sans balise HTML.")
                        save_state(change.id, "preview", after={**change.data["after"], field: value}, edited=True)
                        change = load_change(project, change_id)
                    if not wp.same_connection(c, change.data["connection"], key):
                        raise wp.CmsError("La connexion WordPress a changé. Préparez un nouvel aperçu avec la connexion actuelle.")
                    live = client.read(change.data["url"])
                    if live["post_id"] != change.data["post_id"]:
                        raise wp.CmsError("Cette adresse ne correspond plus à la page prévue dans l’aperçu.")
                    written += bool(write_change(change, "apply", client, live, c, key, approved_at=approved_at))
                except wp.CmsError as error:
                    failures.append(f"{change.data['url']} : {error}")
    except (correction_guard.CorrectionBusy, correction_guard.CorrectionStoreUnavailable):
        failures.append("Une opération est en cours ou le suivi n’est pas disponible. Vérifiez l’état avant de poursuivre.")
    except wp.CmsError as error:
        failures.append(str(error))
    m._audit_log(request, action="cms.wordpress.batch_apply", status="ok" if not failures else "partial",
                 target_type="project", target_id=slug, meta={"issue_key": issue_key, "written": written,
                                                              "failed": len(failures)})
    return redirect(slug, path=f"cms/wordpress/issues/{issue_key}",
                    msg=f"{written} page(s) enregistrée(s) dans WordPress." if written else "",
                    err=" · ".join(failures[:3]) + (f" (+{len(failures) - 3})" if len(failures) > 3 else ""))


@router.post("/projects/{slug}/cms/wordpress/issues/{issue_key}/verify")
def wordpress_batch_verify(request: Request, slug: str, issue_key: str):
    """ONE control crawl for every page of the batch, not one per page."""
    m = app_module()
    project = m._db_project_or_404(request, slug)
    try:
        with correction_guard.correction_lock(m.DB, str(project.owner_user_id)):
            _, _, urls, _ = batch_scope(project, issue_key)
            rows = [row for row in batch_rows(project, issue_key, urls + followed(project, issue_key, urls))
                    if row["verification"]]
            pending = [row for row in rows if row["verification"]["state"] not in {"verified", "superseded"}]
            if not pending:
                raise wp.CmsError("Aucune modification enregistrée n’attend de vérification.")
            if any(row["verification"]["poll"] for row in rows):
                return redirect(slug, path=f"cms/wordpress/issues/{issue_key}")
            job_id = launch_control_crawl(request, project)
            for row in pending:
                save_state(row["change"].id, "applied", verification_job_id=job_id)
        return redirect(slug, path=f"cms/wordpress/issues/{issue_key}",
                        msg="L’analyse de contrôle est lancée pour toutes les pages enregistrées.")
    except wp.CmsError as error:
        return redirect(slug, path=f"cms/wordpress/issues/{issue_key}", err=str(error))
    except (correction_guard.CorrectionBusy, correction_guard.CorrectionStoreUnavailable):
        return redirect(slug, path=f"cms/wordpress/issues/{issue_key}",
                        err="Une opération est en cours sur ce compte. Réessayez dans un instant.")
