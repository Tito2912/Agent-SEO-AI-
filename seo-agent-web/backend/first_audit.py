"""Create a personal site and use the existing, billed crawl entry point."""
from __future__ import annotations

from urllib.parse import urlsplit

from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import func, select

try:
    from .models import JobRecord, Project
except ImportError:
    from models import JobRecord, Project

router = APIRouter()
LIVE = {"queued", "running", "cancel_requested"}


def module():
    if __package__:
        from . import app
    else:
        import app
    return app


def allowance(m, user):
    """A read-only preview; the crawl handler checks the quota again when starting."""
    if user.is_admin:
        return {"admin": True, "can_start": True}
    with m.DB.session() as db:
        plan = m.billing.effective_plan_key(db, user_id=str(user.id))
        limits = (m.billing.plan_catalog().get(plan) or {}).get("limits") or {}
        remaining = m.billing.remaining_quota(db, user_id=str(user.id), metric="pages_crawled_month")
        site_count = int(db.scalar(select(func.count()).select_from(Project).where(
            Project.owner_user_id == str(user.id))) or 0)
    page_cap = m.billing.crawl_config_for_plan(plan)["max_pages_per_crawl"]
    site_limit = int(limits.get("projects") or 0)
    return {
        "admin": False, "plan_label": (m.billing.plan_catalog().get(plan) or {}).get("label", plan),
        "page_cap": min(page_cap, remaining) if remaining is not None else page_cap,
        "remaining": remaining, "monthly_limit": int(limits.get("pages_crawled_month") or 0),
        "site_limit": site_limit, "site_count": site_count,
        "can_start": remaining != 0 and (site_limit <= 0 or site_count < site_limit),
    }


def failure(m, request, raw, message, status=400):
    response = m.projects(request, err=message, first_audit_url=raw)
    response.status_code = status
    return response


def resume(selected, state, live, last_job, has_crawl, clean):
    slug = str(selected.get("slug") or "")
    if not slug:
        return None
    base = f"/projects/{slug}"
    result = {"site_name": selected.get("site_name") or slug, "href": base, "post": False}
    if live:
        title = {"queued": "Votre analyse va démarrer", "running": "Votre analyse est en cours",
                 "cancel_requested": "L’annulation est en cours"}.get(live.get("status"), "Votre analyse est en cours")
        result.update(title=title, label="Retrouver mon analyse", href=f"{base}?job={live['id']}",
                      detail="L’analyse continue même si vous quittez cette page.")
        if live.get("status") == "cancel_requested":
            result["detail"] = "Attendez la confirmation de l’arrêt avant de relancer une analyse."
    elif has_crawl and selected.get("used_fallback"):
        result.update(title="Votre dernier rapport est indisponible", label="Comprendre le bilan affiché",
                      detail="Un bilan précédent reste disponible. Retrouvez le projet pour comprendre pourquoi le dernier rapport manque.")
    elif not has_crawl:
        retry = last_job.get("status") in {"failed", "canceled", "done"} or selected.get("site_injoignable")
        result.update(title="Retrouvons un bilan exploitable" if retry else "Votre site est prêt à être analysé",
                      label="Réessayer l’analyse" if retry else "Lancer mon analyse", post=True,
                      href=base + "/crawl", detail="Votre site est enregistré. Lancez l’analyse pour obtenir votre bilan.")
        if retry:
            result["detail"] = "La dernière analyse n’a pas fourni de bilan exploitable. Votre site est conservé."
    elif state and not state.get("verified"):
        result.update(title="Poursuivez votre correction", label="Reprendre ma correction", href=base + "/first-correction",
                      detail="Retrouvez la modification choisie et faites vérifier le résultat.")
        if (state.get("verify") or {}).get("result") == "still_present":
            result.update(title="La correction reste à ajuster",
                          detail="Le dernier contrôle retrouve encore ce problème. Reprenez la modification avant de la faire vérifier.")
        elif state.get("declared"):
            result.update(title="Votre modification attend son contrôle",
                          detail="La modification est déclarée. Une nouvelle analyse doit confirmer que le problème a disparu.")
        elif state.get("pr_url"):
            result.update(title="Votre proposition attend sa publication",
                          detail="Relisez et fusionnez la proposition sur GitHub. Une fois la modification en ligne, signalez-la dans le suivi pour faire vérifier le résultat.")
    elif state.get("verified"):
        remaining = any(int(v or 0) for v in (selected.get("issues_distribution") or {}).values())
        result.update(title="Votre première correction est vérifiée", href=base + "/first-correction",
                      label="Voir le résultat", detail="Le résultat a été confirmé par une nouvelle analyse.")
        if remaining:
            result.update(label="Choisir la correction suivante", href=base + "/first-correction?choose=1",
                          detail="Une correction confirmée, une étape franchie. Choisissez maintenant un autre problème à traiter.")
    elif clean:
        result.update(title="Votre premier bilan est prêt", label="Voir mes résultats",
                      detail="Aucune anomalie détectée sur les pages analysées. Retrouvez le détail et le périmètre de ce bilan.")
    elif any(int(v or 0) for v in (selected.get("issues_distribution") or {}).values()):
        result.update(title="Commencez par une seule correction", label="Choisir ma première correction",
                      href=base + "/first-correction",
                      detail="Votre bilan est disponible. Choisissez une page, préparez la modification, puis faites vérifier le résultat.")
    else:
        result.update(title="Votre bilan reste à compléter", label="Comprendre mon bilan",
                      detail="Aucun problème n’est remonté, mais cette analyse ne permet pas de conclure que tout va bien. Consultez ses limites.")
    return result


def report_resume(project, state, live):
    """Consultation of an older report must lead back to the current journey."""
    current = project.get("current") or {}
    summary = current.get("summary") or {}
    selected = {**summary, "slug": project["slug"], "site_name": project.get("site_name")}
    usable = bool(summary.get("pages_crawled") and not summary.get("site_injoignable"))
    clean = bool(summary.get("analysis_complete") and not summary.get("issues_total"))
    if not current or not usable:
        return None  # The existing empty/unreachable report explains how to start again.
    result = resume(selected, state, live, {}, usable, clean)
    latest = (project.get("crawls") or [current["timestamp"]])[0]
    if not live and current.get("used_fallback"):
        result.update(title="Un bilan précédent est affiché", label="Voir l’historique des analyses",
                      href=f"/projects/{project['slug']}/crawls", post=False,
                      detail="Le dernier rapport manque. Consultez les analyses disponibles avant de réessayer depuis le projet.")
    elif not live and current["timestamp"] != latest:
        result.update(title="Retrouvez votre parcours actuel", label="Ouvrir le bilan actuel",
                      href=f"/projects/{project['slug']}", post=False,
                      detail="Ce rapport reste consultable. Retrouvez l’analyse disponible et la correction en cours avant de poursuivre.")
    return result


@router.post("/projects/first-audit")
def start(request: Request, site_url: str = Form(default="")):
    m = module()
    user = getattr(request.state, "user", None)
    if not user:
        return RedirectResponse("/auth/login", status_code=303)
    raw = str(site_url or "").strip()
    try:
        parsed = urlsplit(raw if "://" in raw else "https://" + raw)
        if (not raw or len(raw) > 2048 or any(c.isspace() for c in raw)
                or parsed.scheme.lower() not in {"http", "https"}
                or not parsed.hostname or parsed.username is not None or parsed.password is not None):
            raise ValueError
        base = m._normalize_base_url(raw)
        error = m._validate_public_crawl_target(base) if base else "URL invalide."
    except ValueError:
        return failure(m, request, raw[:2048], "Saisissez un domaine ou une adresse http/https valide.")
    if error:
        return failure(m, request, raw, error)
    slug = m._slug_from_base_url(base)
    if not slug:
        return failure(m, request, raw, "Cette adresse ne permet pas de créer un site.")

    # Serialize double clicks in this web process, then consult persistent owner-scoped jobs.
    # This lock does not replace the worker's existing job claim mechanism.
    with m._job_lock("first-audit:" + str(user.id)):
        with m.DB.session() as db:
            project = db.scalar(select(Project).where(
                Project.owner_user_id == str(user.id), Project.slug == slug))
            active_id = db.scalar(select(JobRecord.id).where(
                JobRecord.owner_user_id == str(user.id), JobRecord.slug == slug,
                JobRecord.kind == "crawl", JobRecord.status.in_(LIVE))
                .order_by(JobRecord.created_at.desc()).limit(1)) if project else None
        if project:
            if m._normalize_base_url(project.base_url) != base:
                return failure(m, request, raw, "Un autre site utilise déjà ce nom. Ouvrez-le depuis vos projets.")
            if active_id:
                return RedirectResponse(f"/projects/{slug}?job={active_id}", status_code=303)
            runs = m._runs_dir_for_user(str(user.id))
            summary = m.dash.project_latest_summary(runs, slug) if runs.exists() else None
            if summary and summary.get("timestamp"):
                return RedirectResponse(f"/projects/{slug}", status_code=303)
        else:
            retry = m._rate_limit_retry_after(bucket="first_audit_user", subject=str(user.id), limit=8, window_s=60)
            if isinstance(retry, int):
                response = failure(m, request, raw, "Trop de tentatives. Réessayez dans un instant.", 429)
                response.headers["Retry-After"] = str(retry)
                return response
            available = allowance(m, user)
            if not available["can_start"]:
                return failure(m, request, raw, "Votre limite de sites ou de pages est atteinte. Consultez votre abonnement.")
            # Preserve the existing site quota, target validation, creation audit and milestone.
            created = m.add_project(request, mode="url", domain="", url=raw, site_name="",
                                    gsc_urls=[], bulk_urls=[], next="/")
            if "err=" in created.headers.get("location", ""):
                return created
            with m.DB.session() as db:
                project = db.scalar(select(Project).where(
                    Project.owner_user_id == str(user.id), Project.slug == slug))
            if not project:
                return failure(m, request, raw, "Le site n’a pas pu être ajouté. Réessayez.", 503)
        # Explicit config avoids FastAPI Form defaults on a direct Python call. This handler
        # owns plan bounds, rate limits, page reservation, payer identity and job persistence.
        return m.crawl_project(request, slug, config_path=str(m.DEFAULT_CONFIG))
