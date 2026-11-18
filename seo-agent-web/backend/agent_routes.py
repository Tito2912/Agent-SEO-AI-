"""One explained project plan from existing crawl/search evidence; no writes on GET."""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from urllib.parse import unquote, urlsplit
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select

from . import agent_strategy, agent_gsc, agent_results
from .cms_publication import digest
from .models import AgentActionChoice, AgentPageEvent, CmsPublication, Project

router = APIRouter()


def app_module():
    from . import app
    return app


def project_page(project, value):
    try:
        root, target = urlsplit(project.base_url), urlsplit(value)
        if (target.scheme not in {"http", "https"} or target.scheme != root.scheme
                or target.hostname != root.hostname or target.port != root.port or target.username
                or target.password or target.fragment or "\\" in value or len(value) > 2048
                or any(c.isspace() or ord(c) < 32 for c in value)):
            return False
        decoded = target.path
        for _ in range(4):
            decoded = unquote(decoded)
        if any(part in {".", ".."} for part in decoded.split("/")) or "\\" in decoded:
            return False
        return not root.path.rstrip("/") or target.path.startswith(root.path.rstrip("/") + "/") or target.path == root.path.rstrip("/")
    except ValueError:
        return False


@router.get("/projects/{slug}/agent/results")
def agent_page_results(request: Request, slug: str, msg: str = "", err: str = ""):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    with m.DB.session() as db:
        events = list(db.scalars(select(AgentPageEvent).where(AgentPageEvent.project_id == project.id,
                                AgentPageEvent.kind == "search_comparison")
                                .order_by(AgentPageEvent.created_at.desc()).limit(20)))
        publications = list(db.scalars(select(CmsPublication).where(CmsPublication.project_id == project.id,
                                      CmsPublication.state == "published").order_by(CmsPublication.updated_at.desc()).limit(30)))
    response = m.templates.TemplateResponse("agent_results.html", {"request": request, "project": project,
            "slug": slug, "events": events, "publications": publications, "msg": msg, "err": err})
    response.headers["Cache-Control"] = "no-store"
    return response


@router.post("/projects/{slug}/agent/results")
def agent_measure_page(request: Request, slug: str, page_url: str = Form(default=""),
                       change_date: str = Form(default=""), days: int = Form(default=28),
                       publication_id: str = Form(default="")):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    change_source = "customer_date"
    if publication_id:
        with m.DB.session() as db:
            publication = db.get(CmsPublication, publication_id)
            if not publication or publication.project_id != project.id or publication.state != "published":
                raise HTTPException(404, "publication_not_found")
            page_url = publication.remote_url or ""
            event = db.scalar(select(AgentPageEvent).where(
                AgentPageEvent.project_id == project.id,
                AgentPageEvent.event_key == "cms:" + publication.id + ":published",
                AgentPageEvent.kind == "publication_accepted"))
            if event is None:
                raise HTTPException(409, "publication_date_unavailable")
            changed_at = event.created_at
            if changed_at.tzinfo is None:
                changed_at = changed_at.replace(tzinfo=timezone.utc)
            change_date = changed_at.astimezone(ZoneInfo("America/Los_Angeles")).date().isoformat()
            change_source = "cms_publication"
    try:
        changed = date.fromisoformat(change_date)
        if days not in {7, 28} or not project_page(project, page_url):
            raise ValueError()
    except ValueError:
        return RedirectResponse(m._path_with_flash(f"/projects/{slug}/agent/results",
                err="Choisissez une page de ce site, une date et une durée valide."), status_code=303)
    retry = m._rate_limit_retry_after(bucket="agent_page_results", subject=str(project.id), limit=10, window_s=86400)
    if isinstance(retry, int):
        return RedirectResponse(m._path_with_flash(f"/projects/{slug}/agent/results",
                err="Le nombre de mesures de la journée est atteint. Retrouvez les résultats déjà enregistrés."), status_code=303)
    before_start, before_end = changed - timedelta(days=days), changed - timedelta(days=1)
    after_start, after_end = changed + timedelta(days=1), changed + timedelta(days=days)
    comparison = {"comparable": False, "reason": "period_not_final", "before": {}, "after": {}, "changes": {}}
    _, cfg, _ = m._effective_project_crawl_settings(slug, config_path=m.DEFAULT_CONFIG if m.DEFAULT_CONFIG.exists() else None,
                                                   project_settings=project.settings or {})
    if cfg.get("enabled") is False:
        comparison["reason"] = "search_disabled"
    elif after_end <= agent_gsc._today_pacific() - timedelta(days=agent_gsc.FINAL_DATA_LAG_DAYS):
        with m._gsc_live_credentials(user_id=str(project.owner_user_id), slug=slug) as (credentials, _, _):
            if credentials:
                fetch = m._load_gsc_fetch_module()
                for prop in m._gsc_property_candidates(project.base_url, str(cfg.get("property_url") or "")):
                    kwargs = {"fetch_module": fetch, "credentials_path": credentials, "property_url": prop,
                              "page_url": page_url, "search_type": str(cfg.get("search_type") or "web")}
                    before = agent_gsc.fetch_page_period(**kwargs, start_date=before_start, end_date=before_end)
                    after = agent_gsc.fetch_page_period(**kwargs, start_date=after_start, end_date=after_end)
                    change_at = datetime(changed.year, changed.month, changed.day, 12,
                                         tzinfo=ZoneInfo("America/Los_Angeles")).isoformat()
                    comparison = agent_results.compare_periods(before, after, page_url=page_url, change_at=change_at)
                    if before.get("ok") and after.get("ok"):
                        break
            else:
                comparison["reason"] = "authorization_unavailable"
    stamp = datetime.now(timezone.utc).date().isoformat()
    event_key = "gsc:" + digest({"page": page_url, "date": change_date, "days": days, "fetched": stamp})
    data = {"comparison": comparison, "change_date": change_date, "change_source": change_source, "days": days,
            "before_start": before_start.isoformat(), "before_end": before_end.isoformat(),
            "after_start": after_start.isoformat(), "after_end": after_end.isoformat(),
            "available_from": (after_end + timedelta(days=agent_gsc.FINAL_DATA_LAG_DAYS)).isoformat()}
    with m.DB.session() as db:
        event = db.scalar(select(AgentPageEvent).where(AgentPageEvent.project_id == project.id,
                                                      AgentPageEvent.event_key == event_key))
        if event is None:
            db.add(AgentPageEvent(project_id=project.id, event_key=event_key, page_url=page_url,
                                  kind="search_comparison", data=data))
        else:
            event.data = data
        db.commit()
    return RedirectResponse(m._path_with_flash(f"/projects/{slug}/agent/results",
                            msg="Les données disponibles pour cette page ont été enregistrées."), status_code=303)


def project_inputs(m, project):
    runs = m._runs_dir_for_user(str(project.owner_user_id))
    timestamps = m.dash.list_project_crawls(runs, project.slug)
    report = m.dash.load_report_json(runs, project.slug, timestamps[-1]) if timestamps else {}
    return report or {}


def plan(m, project):
    settings = (project.settings or {}).get("agent") or {}
    report = project_inputs(m, project)
    topics = []
    with m.DB.session() as db:
        choices = list(db.scalars(select(AgentActionChoice).where(AgentActionChoice.project_id == project.id)))
        if m._competitor_has_access(db, user_id=str(project.owner_user_id)):
            for competitor in m._competitor_rows(db, str(project.id)):
                if competitor.status == "ready" and isinstance(competitor.pages, list):
                    topics.extend(m.competitors_mod.compare(report.get("pages") or [], competitor.pages, limit=25))
    return agent_strategy.recommend_actions(
        base_url=project.base_url, slug=project.slug, report=report,
        query_rows=settings.get("query_rows") or [], existing_pages=report.get("pages") or [],
        competitor_topics=topics, business_context=settings.get("business_context") or {},
        decisions={r.action_key: r.state for r in choices}, limit=3,
    )


@router.get("/projects/{slug}/agent")
def agent_board(request: Request, slug: str, msg: str = "", err: str = ""):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    settings = (project.settings or {}).get("agent") or {}
    with m.DB.session() as db:
        events = list(db.scalars(select(AgentPageEvent).where(AgentPageEvent.project_id == project.id,
                                AgentPageEvent.kind == "publication_accepted")
                                .order_by(AgentPageEvent.created_at.desc()).limit(30)))
        publications = list(db.scalars(select(CmsPublication).where(CmsPublication.project_id == project.id)
                                      .order_by(CmsPublication.created_at.desc()).limit(20)))
        dismissed = list(db.scalars(select(AgentActionChoice).where(AgentActionChoice.project_id == project.id,
                                                                  AgentActionChoice.state == "dismissed")))
    response = m.templates.TemplateResponse("agent_board.html", {
        "request": request, "project": project, "slug": slug, "actions": plan(m, project),
        "profile": settings.get("business_context") or {}, "search": settings.get("search") or {},
        "events": events, "publications": publications, "dismissed": dismissed, "msg": msg, "err": err,
    })
    response.headers["Cache-Control"] = "no-store"
    return response


def redirect(m, slug, **message):
    return RedirectResponse(m._path_with_flash(f"/projects/{slug}/agent", **message), status_code=303)


@router.post("/projects/{slug}/agent/context")
def agent_context(request: Request, slug: str, activity: str = Form(default=""),
                  services: str = Form(default=""), priority_urls: str = Form(default="")):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    if len(activity) > 500 or len(services) > 3000 or len(priority_urls) > 12000:
        raise HTTPException(400, "context_too_long")
    urls = [line.strip() for line in priority_urls.splitlines() if line.strip()]
    origin = urlsplit(project.base_url)
    try:
        if len(urls) > 20 or any(urlsplit(url).scheme != "https" or urlsplit(url).netloc != origin.netloc
                                or urlsplit(url).username or urlsplit(url).fragment for url in urls):
            raise ValueError()
    except ValueError:
        return redirect(m, slug, err="Choisissez au plus vingt adresses HTTPS appartenant à ce site.")
    with m.DB.session() as db:
        row = db.get(Project, project.id)
        agent = dict((row.settings or {}).get("agent") or {})
        agent["business_context"] = {"activity": activity.strip(),
            "services": [s.strip() for s in services.splitlines() if s.strip()][:30], "priority_urls": urls}
        row.settings = {**(row.settings or {}), "agent": agent}
        db.commit()
    return redirect(m, slug, msg="Vos priorités sont enregistrées.")


@router.post("/projects/{slug}/agent/decision")
def agent_decision(request: Request, slug: str, action_key: str = Form(default=""), state: str = Form(default="")):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    if not re.fullmatch(r"[a-f0-9]{32}", action_key) or state not in {"dismissed", "reset"}:
        raise HTTPException(400, "invalid_action_decision")
    with m.DB.session() as db:
        choice = db.scalar(select(AgentActionChoice).where(AgentActionChoice.project_id == project.id,
                                                          AgentActionChoice.action_key == action_key))
        if state == "reset":
            if choice:
                db.delete(choice)
        elif choice:
            choice.state = state
        else:
            db.add(AgentActionChoice(project_id=project.id, action_key=action_key, state=state,
                                     created_by=str(request.state.user.id)))
        db.commit()
    return redirect(m, slug, msg="Votre choix est enregistré.")


@router.post("/projects/{slug}/agent/refresh")
def agent_refresh(request: Request, slug: str):
    m = app_module()
    project = m._db_project_or_404(request, slug)
    retry = m._rate_limit_retry_after(bucket="agent_search_refresh", subject=str(project.id), limit=3, window_s=3600)
    if isinstance(retry, int):
        return redirect(m, slug, err="Les données viennent d’être actualisées. Réessayez plus tard.")
    _, gsc, _ = m._effective_project_crawl_settings(slug, config_path=m.DEFAULT_CONFIG if m.DEFAULT_CONFIG.exists() else None,
                                                   project_settings=project.settings or {})
    payload = m._fetch_gsc_live_items(user_id=str(project.owner_user_id), slug=slug, base_url=project.base_url,
                                    gsc_cfg=gsc, days=28, dim="query_page", limit=1000)
    with m.DB.session() as db:
        row = db.get(Project, project.id)
        agent = dict((row.settings or {}).get("agent") or {})
        agent["search"] = {k: payload.get(k) for k in ("ok", "reason", "start_date", "end_date", "property", "data_delay_hint")}
        # A failed refresh must not silently leave stale search rows in the current plan.
        agent["query_rows"] = payload.get("items", []) if payload.get("ok") else []
        row.settings = {**(row.settings or {}), "agent": agent}
        db.commit()
    return redirect(m, slug, msg="Les données disponibles ont été actualisées." if payload.get("ok") else
                    "Search Console ne fournit pas encore de données pour ce projet.")
