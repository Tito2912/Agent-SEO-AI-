"""Explicit article review, draft creation and publication on a verified CMS connection."""
from __future__ import annotations

import json
from dataclasses import asdict

from bs4 import BeautifulSoup
from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select

from . import cms_publication as workflow, cms_media_workflow
from .cms_commun import CmsError, fingerprint
from .cms_content_adapters import AdapterError, ArticleDraft, get_adapter
from .cms_content_transport import ContentTransport
from .i18n import JSONResponse
from .models import IssueTask

router = APIRouter()
LABELS = {"wordpress": "WordPress", "ghost": "Ghost", "webflow": "Webflow",
          "wix": "Wix Blog", "prestashop": "PrestaShop · pages CMS"}


def policy(m, platform):
    # C079: Webflow publishes selected items only; Shopify has no customer surface yet.
    return {"enabled": platform in LABELS, "publish": platform in LABELS}


def require_enabled(m, platform):
    if not policy(m, platform)["enabled"]:
        raise workflow.PublicationError("pilot", "Cette intégration est en préparation. Elle n’est pas encore activée pour les comptes clients.")


def limit_operation(m, project, kind):
    retry = m._rate_limit_retry_after(bucket="cms_content_" + kind, subject=str(project.id), limit=12, window_s=3600)
    if isinstance(retry, int):
        raise workflow.PublicationError("rate_limit", "Plusieurs opérations viennent d’être demandées. Actualisez leur suivi avant de réessayer.")


def app_module():
    from . import app
    return app


def connection_key(project, platform):
    return "CMS_CONTENT_" + platform.upper() + ":" + str(project.id)


def connection(m, project, platform):
    if platform not in LABELS:
        raise HTTPException(404, "unknown_content_platform")
    row = m._user_connection_row(user_id=str(project.owner_user_id), key=connection_key(project, platform))
    if row is None:
        raise workflow.PublicationError("connection", "Connectez votre CMS pour préparer la publication.")
    credentials = json.loads(m._decrypt_secret(row.secret_value))
    return credentials, dict(row.meta or {})


def bound_transport(project, platform, credentials):
    transport = ContentTransport(platform, credentials, project_url=project.base_url)
    checked = transport.check()
    if not checked.get("verified"):
        raise workflow.PublicationError("connection", "La connexion ne confirme pas l’accès au site de ce projet.")
    return transport, checked


def connection_fingerprint(m, credentials):
    return fingerprint(credentials, m._safe_env("SEO_AGENT_SECRET_KEY").encode("utf-8"))


def source_draft(m, request, slug, task_id, language, author, resolve_image=None):
    note = m._note_d_article(request, slug, task_id)
    if note is None:
        raise workflow.PublicationError("not_found", "Article introuvable.")
    versions = m._versions_servies(note.get("versions") or [], m._dimensions(note.get("images")))
    version = next((v for v in versions if v.get("code") == language), None)
    if version is None:
        raise workflow.PublicationError("language", "Cette version linguistique n’existe plus.")
    body = BeautifulSoup(version["html"], "html.parser")
    for image in body.find_all("img"):
        source = str(image.get("src") or "")
        for name in m._dimensions(note.get("images")):
            allowed = {"images/" + name, f"/api/projects/{slug}/content/article/{task_id}/images/{name}"}
            if source in allowed:
                if resolve_image is None:
                    raise workflow.PublicationError("media_not_uploaded", "Les images de cet article doivent être transférées dans votre CMS.")
                content = m._image_d_article(request, slug, note, name)
                if content is None:
                    raise workflow.PublicationError("media_missing", "Une image de cet article est introuvable.")
                image["src"] = resolve_image(name, content)
                break
    heading = body.find("h1")
    if heading and heading.get_text().strip() == str(version["titre"]).strip():
        heading.decompose()  # The CMS/theme supplies the article's title.
    return ArticleDraft(title=version["titre"], html=workflow.clean_html(str(body)), slug=version["slug"],
                        excerpt=version.get("description") or "", language=language, author=author)


def redirect(m, slug, publication_id="", platform="", **message):
    path = f"/projects/{slug}/content/publication" + ("/" + publication_id if publication_id else "")
    if not publication_id and platform in LABELS:
        path += "?platform=" + platform
    return RedirectResponse(m._path_with_flash(path, **message), status_code=303)


@router.get("/projects/{slug}/content/publication")
def publication_home(request: Request, slug: str, platform: str = "wordpress", msg: str = "", err: str = ""):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    if platform not in LABELS:
        raise HTTPException(404, "unknown_content_platform")
    with m.DB.session() as db:
        articles = list(db.scalars(select(IssueTask).where(IssueTask.project_id == project.id,
                         IssueTask.issue_key == m._CONTENT_ARTICLE_KEY).order_by(IssueTask.updated_at.desc()).limit(50)))
    versions = []
    for article in articles:
        try:
            note = json.loads(article.note or "{}")
        except ValueError:
            continue
        versions.extend({"task_id": article.id, "language": v.get("code"), "title": v.get("titre"),
                         "label": v.get("langue") or v.get("code")}
                        for v in note.get("versions", []) if isinstance(v, dict))
    try:
        _, meta = connection(m, project, platform)
    except workflow.PublicationError:
        meta = {}
    response = m.templates.TemplateResponse("cms_publication.html", {
        "request": request, "project": project, "slug": slug, "platform": platform, "platforms": LABELS,
        "meta": meta, "versions": versions, "msg": msg, "err": err, "policy": policy(m, platform),
    })
    response.headers["Cache-Control"] = "no-store"
    return response


@router.post("/projects/{slug}/content/publication/connect")
async def publication_connect(request: Request, slug: str):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    form = await request.form()
    platform = str(form.get("platform") or "")
    if platform not in LABELS:
        raise HTTPException(404, "unknown_content_platform")
    if not policy(m, platform)["enabled"]:
        return redirect(m, slug, platform=platform, err="Cette intégration n’est pas encore activée pour les comptes clients.")
    # Only these credentials may be encrypted; no browser-supplied verification state.
    keys = {"wordpress": ("base_url", "username", "password"), "ghost": ("base_url", "key"),
            "shopify": ("shop", "token"), "webflow": ("token", "site_id"),
            "wix": ("token", "site_id"), "prestashop": ("base_url", "key")}[platform]
    credentials = {key: str(form.get(key) or "").strip() for key in keys}
    if any(not value or len(value) > 4000 for value in credentials.values()):
        return redirect(m, slug, platform=platform, err="Complétez les informations de connexion.")
    try:
        limit_operation(m, project, "connect")
        _, checked = bound_transport(project, platform, credentials)
        m._upsert_user_connection(user_id=str(project.owner_user_id), key=connection_key(project, platform),
                                  value=json.dumps(credentials), meta={"provider": platform, "label": checked.get("label", ""),
                                       "destinations": checked.get("destinations") or []})
        m._audit_log(request, action="cms_content.connect", status="ok", target_type="project", target_id=slug,
                     meta={"platform": platform})
    except (CmsError, AdapterError, workflow.PublicationError):
        return redirect(m, slug, platform=platform, err="La connexion n’a pas pu être vérifiée pour ce site. Vérifiez les droits et l’adresse du CMS.")
    return redirect(m, slug, platform=platform, msg="Connexion vérifiée. Choisissez maintenant l’article et sa destination.")


@router.post("/projects/{slug}/content/publication/disconnect")
def publication_disconnect(request: Request, slug: str, platform: str = Form(default="")):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    if platform not in LABELS:
        raise HTTPException(404, "unknown_content_platform")
    m._delete_user_connection(user_id=str(project.owner_user_id), key=connection_key(project, platform))
    return redirect(m, slug, msg="La connexion de publication a été retirée.")


@router.post("/projects/{slug}/content/publication/prepare")
async def publication_prepare(request: Request, slug: str):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    form = await request.form()
    platform = str(form.get("platform") or "")
    if platform not in LABELS:
        raise HTTPException(404, "unknown_content_platform")
    try:
        limit_operation(m, project, "prepare")
        require_enabled(m, platform)
        task_id, language = str(form.get("version") or "").split("|", 1)
        credentials, _ = connection(m, project, platform)
        transport, checked = bound_transport(project, platform, credentials)
        destination = str(form.get("destination") or "")
        destinations = {str(d.get("id")): d for d in checked.get("destinations", [])}
        config = {"destination_label": LABELS[platform]}
        if platform in {"shopify", "webflow"}:
            if destination not in destinations:
                raise workflow.PublicationError("destination", "Choisissez une destination de ce site.")
            if platform == "shopify":
                config = {"blog_id": destination, "destination_label": destinations[destination].get("title") or LABELS[platform]}
            else:
                schema = transport.describe_destination(destination)
                fields = {f["slug"]: f for f in schema.get("fields", []) if f.get("editable", True)}
                body_field = str(form.get("body_field") or "")
                if body_field not in fields or str(fields[body_field].get("type", "")).lower() not in {"richtext", "rich-text"}:
                    raise workflow.PublicationError("mapping", "Choisissez le champ de contenu riche de cette collection.")
                config = {"collection_id": destination,
                          "destination_label": destinations[destination].get("title") or LABELS[platform],
                          "field_mapping": {"title": "name", "slug": "slug", "body": body_field}}
        elif platform == "prestashop":
            config = {**config, "id_lang": str(form.get("id_lang") or ""), "id_shop": str(form.get("id_shop") or "")}
        author = str(form.get("author") or "").strip()[:255]
        approved_media = str(form.get("approved_media") or "") == "true"
        fp = connection_fingerprint(m, credentials)
        def resolve_image(name, content):
            return cms_media_workflow.image_url(m.DB, project_id=str(project.id), task_id=task_id, platform=platform,
                       connection_fingerprint=fp, name=name, data=content, transport=transport, approved=approved_media,
                       actor_id=str(request.state.user.id))
        draft = source_draft(m, request, slug, task_id, language, author, resolve_image=resolve_image)
        get_adapter(platform, transport=transport, config=config)  # Validate mapping before persisting an intent.
        row = workflow.prepare(m.DB, project_id=str(project.id), task_id=task_id, user_id=str(request.state.user.id),
                               platform=platform, language=language, draft=draft, config=config,
                               connection_fingerprint=fp)
        return redirect(m, slug, row.id)
    except (ValueError, KeyError, CmsError, AdapterError, workflow.PublicationError) as error:
        message = str(error) if isinstance(error, workflow.PublicationError) else "Vérifiez l’article, la destination et les informations requises par votre CMS."
        return redirect(m, slug, platform=platform, err=message)


@router.get("/projects/{slug}/content/publication/{publication_id}")
def publication_review(request: Request, slug: str, publication_id: str, msg: str = "", err: str = ""):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    try:
        row = workflow.load(m.DB, project_id=str(project.id), publication_id=publication_id)
        require_enabled(m, row.platform)
    except workflow.PublicationError:
        raise HTTPException(404, "publication_not_found") from None
    response = m.templates.TemplateResponse("cms_publication_review.html", {
        "request": request, "project": project, "slug": slug, "publication": row,
        "platform_label": LABELS.get(row.platform, row.platform), "msg": msg, "err": err,
        "can_publish": policy(m, row.platform)["publish"],
    })
    response.headers["Cache-Control"] = "no-store"
    return response


@router.get("/api/projects/{slug}/content/destinations/{destination}")
def publication_destination_fields(request: Request, slug: str, destination: str):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    try:
        credentials, _ = connection(m, project, "webflow")
        transport, _ = bound_transport(project, "webflow", credentials)
        result = transport.describe_destination(destination)
    except (CmsError, AdapterError, workflow.PublicationError):
        raise HTTPException(400, "destination_unavailable") from None
    return JSONResponse(result, headers={"Cache-Control": "no-store"})


@router.get("/projects/{slug}/content/publication/{publication_id}/preview")
def publication_preview(request: Request, slug: str, publication_id: str):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    try:
        row = workflow.load(m.DB, project_id=str(project.id), publication_id=publication_id)
        require_enabled(m, row.platform)
    except workflow.PublicationError:
        raise HTTPException(404, "publication_not_found") from None
    return HTMLResponse(row.data["article"]["html"], headers={"Cache-Control": "no-store",
        "Content-Security-Policy": "sandbox; default-src 'none'; img-src https:; frame-ancestors 'self'",
        "X-Frame-Options": "SAMEORIGIN", "X-Robots-Tag": "noindex, nofollow", "Referrer-Policy": "no-referrer"})


@router.post("/projects/{slug}/content/publication/{publication_id}/execute")
def publication_execute(request: Request, slug: str, publication_id: str,
                        operation: str = Form(default=""), approved: bool = Form(default=False)):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    try:
        limit_operation(m, project, "execute")
        row = workflow.load(m.DB, project_id=str(project.id), publication_id=publication_id)
        require_enabled(m, row.platform)
        if operation == "publish" and not policy(m, row.platform)["publish"]:
            raise workflow.PublicationError("publish_policy", "Le brouillon est enregistré. Effectuez sa publication dans Webflow.")
        credentials, _ = connection(m, project, row.platform)
        transport, _ = bound_transport(project, row.platform, credentials)
        fp = connection_fingerprint(m, credentials)
        def resolve_image(name, content):
            return cms_media_workflow.image_url(m.DB, project_id=str(project.id), task_id=row.task_id, platform=row.platform,
                       connection_fingerprint=fp, name=name, data=content, transport=transport, approved=False)
        draft = source_draft(m, request, slug, row.task_id, row.language, row.data["article"].get("author", ""),
                             resolve_image=resolve_image)
        adapter = get_adapter(row.platform, transport=transport, config=row.data["config"])
        workflow.execute(m.DB, project_id=str(project.id), publication_id=publication_id, operation=operation,
                         adapter=adapter, connection_fingerprint=fp,
                         current_article_digest=workflow.digest(asdict(draft)), approved=approved,
                         actor_id=str(request.state.user.id))
        m._audit_log(request, action="cms_content." + operation, status="ok", target_type="project", target_id=slug)
        return redirect(m, slug, publication_id, msg="Le suivi de publication a été actualisé.")
    except (CmsError, AdapterError, workflow.PublicationError) as error:
        if isinstance(error, workflow.PublicationError) and error.code == "not_found":
            raise HTTPException(404, "publication_not_found") from None
        return redirect(m, slug, publication_id, err=str(error) if isinstance(error, workflow.PublicationError)
                        else "La connexion au CMS n’a pas pu être vérifiée.")


@router.post("/projects/{slug}/content/publication/{publication_id}/refresh")
def publication_refresh(request: Request, slug: str, publication_id: str):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    try:
        limit_operation(m, project, "refresh")
        row = workflow.load(m.DB, project_id=str(project.id), publication_id=publication_id)
        require_enabled(m, row.platform)
        credentials, _ = connection(m, project, row.platform)
        transport, _ = bound_transport(project, row.platform, credentials)
        adapter = get_adapter(row.platform, transport=transport, config=row.data["config"])
        workflow.reconcile(m.DB, project_id=str(project.id), publication_id=publication_id, adapter=adapter)
        return redirect(m, slug, publication_id, msg="Le résultat a été relu dans votre CMS, sans nouvelle écriture.")
    except (CmsError, AdapterError, workflow.PublicationError) as error:
        if isinstance(error, workflow.PublicationError) and error.code == "not_found":
            raise HTTPException(404, "publication_not_found") from None
        return redirect(m, slug, publication_id, err=str(error) if isinstance(error, workflow.PublicationError)
                        else "Le CMS n’a pas pu être relu.")
