"""Owner-only publication proposals. GET never contacts GitHub or publishes."""

import uuid
from urllib.parse import urlencode

from fastapi import APIRouter, BackgroundTasks, Form, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

try:
    from . import own_blog as b, own_seo_routes, i18n
    from .models import OwnSeoDraft
except ImportError:
    import own_blog as b
    import own_seo_routes
    import i18n
    from models import OwnSeoDraft

router = APIRouter()


def redirect(ident="", error=""):
    return RedirectResponse(
        b.PATH + "?" + urlencode({"publication": ident, "err": error}),
        status_code=303,
        headers={"Cache-Control": "no-store"},
    )


def copy(lang):
    def t(fr, en):
        return en if lang == "en" else fr

    return dict(
        title=t("Publication du blog de Noyaru", "Noyaru blog publication"),
        details=t("Détails de la publication", "Publication details"),
        legacy=t("Cette ancienne opération conserve ses deux accords : créer la PR, puis publier.", "This existing operation keeps two approvals: create the PR, then publish."),
        topic=t("1. Sujet", "1. Topic"), article=t("2. Article", "2. Article"), publication_step=t("3. Publication", "3. Publication"),
        final_decision=t("Décision 2/2 : relire et publier", "Decision 2/2: review and publish"),
        automatic=t("Le suivi continue automatiquement. Vous pouvez quitter cet écran.", "Tracking continues automatically. You can leave this page."),
        source_draft=t("Relire l’article et ses sources", "Read the article and its sources"),
        updates=t("Un nouvel état est disponible. Afficher la mise à jour", "A new status is available. Show update"),
        intro=t(
            "Après le sujet et la rédaction, une dernière décision permet de relire et publier cet article.",
            "After topic and writing approval, one final decision lets you review and publish this article.",
        ),
        scope=t(
            "Seuls de nouveaux articles du blog de Noyaru peuvent être ajoutés. Aucun site client ni nouvel appel IA.",
            "Only new Noyaru blog articles can be added. No client site or new AI request.",
        ),
        choose=t("Préparer une proposition", "Prepare a proposal"),
        fr=t("Article français à publier", "French article to publish"),
        en=t(
            "Version anglaise du même sujet (facultatif)",
            "English version of the same topic (optional)",
        ),
        none=t("Aucune version anglaise", "No English version"),
        empty=t(
            "Aucun article français actuel à préparer. Retrouvez votre sujet dans Rédaction sourcée.",
            "No current French article to prepare. Continue with your topic in Sourced writing.",
        ),
        consent=t(
            "Préparer la proposition et vérifier GitHub en lecture seule. La décision Telegram permettra ensuite de relire l’article et d’autoriser sa publication. Si je choisis une version anglaise, elle couvre le même sujet.",
            "Prepare the proposal and check GitHub read-only. The Telegram decision will then let me review the article and authorize publication. Any selected English version covers the same topic.",
        ),
        prepare=t("Examiner la proposition", "Review the proposal"),
        history=t("Suivi des publications", "Publication history"),
        bot=t("Ouvrir les validations Telegram", "Open Telegram approvals"),
        writing=t("Rédaction sourcée", "Sourced writing"),
        reload=t("Actualiser la page", "Reload the page"),
        check=t("Vérifier GitHub et la publication", "Check GitHub and publication"),
        refresh=t("Préparer une nouvelle validation", "Prepare a fresh approval"),
        cancel=t("Arrêter cette opération", "Stop this operation"),
        next=t(
            "La prochaine décision est envoyée au bot activé. Lisez toutes les pages puis confirmez votre choix. Vous pouvez refuser ; la PR éventuelle reste en place.",
            "The next decision is sent to the enabled bot. Read every page and confirm your choice. You can reject; any existing PR is kept.",
        ),
        limits=t(
            "Nouvelles propositions : la décision finale couvre la relecture, la création de PR, les contrôles et la publication du contenu exact. Les anciennes opérations gardent leurs deux accords distincts. Un changement de base, de contenu ou d’accès exige une nouvelle décision.",
            "New proposals: the final decision covers review, PR creation, checks and publication of the exact content. Existing operations keep their two separate approvals. A base, content or access change requires a fresh decision.",
        ),
        proof=t(
            "Publication technique observée. Cela ne prouve ni indexation, ni progression du référencement, ni visibilité dans les réponses IA.",
            "Technical publication observed. This does not prove indexing, better rankings or visibility in AI answers.",
        ),
        files=t(
            "Fichiers proposés et diff complet (ajouts)",
            "Proposed files and full diff (additions)",
        ),
        status=t("État", "State"),
        expires=t("Échéance UTC", "UTC deadline"),
        statuses={
            k: t(fr, en)
            for k, fr, en in [
                ("checking", "Vérification de la proposition", "Checking proposal"),
                (
                    "proposal",
                    "Accord attendu : PR en brouillon",
                    "Approval needed: draft PR",
                ),
                ("queued_prepare", "Préparation autorisée", "Preparation authorized"),
                ("preparing", "Création de la PR en cours", "Creating PR"),
                (
                    "reviewing",
                    "Contrôles GitHub en attente",
                    "Waiting for GitHub checks",
                ),
                (
                    "review_ready",
                    "Accord attendu : publication",
                    "Approval needed: publication",
                ),
                ("queued_publish", "Publication autorisée", "Publication authorized"),
                ("publishing", "Fusion en cours", "Merging"),
                (
                    "observing",
                    "Fusion observée, publication à confirmer",
                    "Merge observed, awaiting deployment",
                ),
                (
                    "published",
                    "Article publié et observé",
                    "Article published and observed",
                ),
                ("needs_review", "Intervention nécessaire", "Action needed"),
                (
                    "uncertain",
                    "Résultat GitHub incertain — relecture uniquement",
                    "Uncertain GitHub result — reconciliation only",
                ),
                ("cancelled", "Opération arrêtée", "Operation stopped"),
            ]
        },
        errors={
            k: t(fr, en)
            for k, fr, en in [
                (
                    "unavailable",
                    "Le suivi est momentanément indisponible. Aucune nouvelle action n’a été autorisée.",
                    "Tracking is temporarily unavailable. No new action was authorized.",
                ),
                (
                    "github_required",
                    "Connectez votre compte GitHub dans Mon compte → Connexions.",
                    "Connect your GitHub account in My account → Connections.",
                ),
                (
                    "repository_required",
                    "L’accès GitHub doit permettre de contribuer au dépôt de Noyaru, avec les commits de fusion autorisés.",
                    "GitHub access must allow contributions to the Noyaru repository, with merge commits enabled.",
                ),
                (
                    "draft_required",
                    "Le brouillon doit être inchangé et fondé sur des sources toujours valides ; la publication attend sa relecture.",
                    "The draft must be unchanged and based on valid sources; publication requires review.",
                ),
                (
                    "translation_required",
                    "Choisissez une version anglaise relue de la même opportunité éditoriale.",
                    "Choose a reviewed English version of the same editorial opportunity.",
                ),
                (
                    "base_changed",
                    "La base main a avancé. Préparez une nouvelle validation pour synchroniser cette PR, puis attendez les nouveaux contrôles.",
                    "The main branch has advanced. Prepare a fresh approval to sync this PR, then wait for new checks.",
                ),
                (
                    "article_exists",
                    "Un article ou une proposition existe déjà pour cette adresse. Aucun fichier ne sera écrasé.",
                    "An article or proposal already exists at this address. No file will be overwritten.",
                ),
                (
                    "checks_pending",
                    "Les contrôles GitHub ne sont pas encore terminés.",
                    "GitHub checks are still running.",
                ),
                (
                    "checks_failed",
                    "Les contrôles GitHub ont échoué. Consultez la PR avant de poursuivre.",
                    "GitHub checks failed. Review the PR before continuing.",
                ),
                (
                    "checks_incomplete",
                    "Les preuves de contrôle sont incomplètes ou ont changé. Une nouvelle vérification est nécessaire.",
                    "Check evidence is incomplete or changed. A fresh check is required.",
                ),
                (
                    "branch_protection",
                    "Les règles de fusion exigent une intervention dans GitHub. La fusion automatique ne les contourne pas.",
                    "Merge rules require action in GitHub. Automatic merging does not bypass them.",
                ),
                (
                    "github_uncertain",
                    "Une écriture a un résultat incertain. Le suivi relit GitHub sans répéter cette écriture. Si le résultat reste absent, une intervention est nécessaire.",
                    "A write has an uncertain result. Tracking reads GitHub without repeating that write. If the result remains absent, intervention is needed.",
                ),
                (
                    "deploy_pending",
                    "Le contenu exact n’est pas encore observé sur toutes les pages publiques.",
                    "The exact content has not yet been observed on all public pages.",
                ),
                (
                    "expired",
                    "La demande a expiré. Une nouvelle préparation est nécessaire ; aucun accord n’est prolongé automatiquement.",
                    "The request expired. Fresh preparation is required; approvals are never extended automatically.",
                ),
                (
                    "budget",
                    "La limite de cette opération est atteinte. Consultez la PR avant toute nouvelle opération.",
                    "This operation reached its limit. Review the PR before starting another operation.",
                ),
                (
                    "changed",
                    "La proposition ou ses accès ont changé. Consultez le suivi avant de poursuivre.",
                    "The proposal or access changed. Review tracking before continuing.",
                ),
                (
                    "confirm_required",
                    "Cochez la confirmation pour préparer la proposition.",
                    "Confirm before preparing the proposal.",
                ),
                (
                    "busy",
                    "Une vérification est déjà en cours.",
                    "A check is already running.",
                ),
            ]
        },
    )


@router.get("/settings/system/own-seo/publication")
def page(request: Request, publication: str = "", err: str = "", draft: str = ""):
    m = b.s.module()
    user = m._require_system_owner(request)
    strings = copy(i18n.current_lang())
    items = []
    eligible = []
    briefs = {}
    titles = {}
    unavailable = False
    try:
        with m.DB.session() as db:
            for candidate in db.scalars(
                select(OwnSeoDraft)
                .where(
                    OwnSeoDraft.owner_id == user.id, OwnSeoDraft.status.in_({'draft','reviewed'})
                )
                .order_by(OwnSeoDraft.created_at.desc())
                .limit(40)
            ):
                if b.w.current(db, candidate):
                    eligible.append(candidate)
            for row in db.scalars(
                select(b.Publication)
                .where(b.Publication.owner_id == user.id)
                .order_by(b.Publication.created_at.desc())
                .limit(20)
            ):
                items.append(row)
            if publication and not any(r.id==publication for r in items):
                selected_row=db.get(b.Publication,publication)
                if not selected_row or selected_row.owner_id!=user.id:
                    raise HTTPException(404,'publication_missing')
                items.insert(0,selected_row)
            for row in items:
                source=db.get(OwnSeoDraft,row.fr_id)
                if source and source.owner_id==user.id:
                    briefs[row.id]=source.brief_id
                    titles[row.id]=source.content.get('title',row.slug)
    except SQLAlchemyError:
        unavailable = True
    return m.templates.TemplateResponse(
        "settings_own_blog.html",
        dict(
            request=request,
            project=None,
            copy=strings,
            items=items,
            eligible=eligible,
            unavailable=unavailable,
            base=b.PATH,
            selected=publication,
            briefs=briefs, titles=titles,
            selected_draft=draft,
            journey_version=b.s.digest([(r.id,r.state,r.error_code,r.generation) for r in items]),
            watch=any(r.state in b.RUNNABLE|{'checking'} for r in items),
            key=uuid.uuid4().hex,
            error=strings["errors"].get(err, strings["errors"]["changed"])
            if err
            else "",
        ),
        status_code=503 if unavailable else 200,
        headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"},
    )


@router.post("/settings/system/own-seo/publication/prepare")
def prepare(
    request: Request,
    background: BackgroundTasks,
    fr: str = Form(""),
    en: str = Form(""),
    key: str = Form(""),
    confirm: bool = Form(False),
):
    user = b.s.module()._require_system_owner(request)
    own_seo_routes.throttle(request)
    try:
        ident = b.prepare(user.id, fr, en or None, key, confirm, article_journey=True)
    except ValueError as exc:
        return redirect(error=str(exc))
    except SQLAlchemyError:
        return redirect(error="unavailable")
    with b.s.module().DB.session() as db:
        if db.get(b.Publication, ident).state == "checking":
            background.add_task(b.refresh, user.id, ident)
    return redirect(ident)


@router.post("/settings/system/own-seo/publication/{ident}/{action}")
def action(request: Request, background: BackgroundTasks, ident: str, action: str):
    user = b.s.module()._require_system_owner(request)
    own_seo_routes.throttle(request)
    if action not in {"refresh", "check", "cancel"}:
        raise HTTPException(404, "action_missing")
    try:
        with b.s.module().DB.session() as db:
            b.s.locked(db)
            row, _ = b.scope(db, user.id, ident, fresh=False)
            if action == "cancel":
                row.state = "cancelled"
                row.lease_token = None
                row.lease_until = None
                b.audit(db, row, "cancel")
                db.commit()
        if action == "refresh":
            background.add_task(b.refresh, user.id, ident)
        elif action == "check":
            background.add_task(b.run, ident)
    except ValueError as exc:
        return redirect(ident, str(exc))
    except SQLAlchemyError:
        return redirect(ident, "unavailable")
    return redirect(ident)


@router.post("/cron/own-blog-publication")
def cron(request: Request):
    if not b.s.module()._cron_auth_check(request):
        return JSONResponse({"ok": False}, status_code=401)
    try:
        count = b.tick()
    except SQLAlchemyError:
        return JSONResponse({"ok": False}, status_code=503)
    return JSONResponse(
        {"ok": True, "examined": count}, headers={"Cache-Control": "no-store"}
    )
