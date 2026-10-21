"""Customer activity milestones describe observed execution and report availability separately."""

from __future__ import annotations


def _french(text, **values):
    return text.format(**values) if values else text


def view(job, *, site_name="", project_url="", report_url="", kind="", tr=_french):
    """`tr` writes each sentence in the customer's language (the route passes i18n.tr_current)."""
    status = str(job.status or "")
    active = status in {"queued", "running", "cancel_requested"}
    progress = job.progress if isinstance(job.progress, dict) else {}
    try:
        finalizing = (
            active
            and float(progress.get("total") or 0) > 0
            and float(progress.get("current") or 0) >= float(progress["total"])
        )
    except (ValueError, TypeError, KeyError):
        finalizing = False
    stage = 1 if status == "queued" else (3 if finalizing else 2)
    ready = status == "done" and bool(report_url)
    title = (
        tr("Analyse de {site}", site=site_name)
        if kind == "crawl" and site_name
        else (
            tr("Audit de plusieurs sites") if kind == "autopilot" else tr("Suivi du traitement")
        )
    )
    if status == "queued":
        headline, message = (
            tr("Votre demande est en attente"),
            tr("Elle attend un créneau disponible. Vous pouvez revenir plus tard pour consulter sa progression."),
        )
    elif status == "cancel_requested":
        headline, message = (
            tr("Arrêt demandé"),
            tr("Le traitement termine son étape en cours avant de s’arrêter. Attendez la confirmation avant de faire une nouvelle demande."),
        )
    elif active:
        headline = (
            tr("Préparation du bilan")
            if finalizing and kind == "crawl"
            else tr("Finalisation en cours")
            if finalizing
            else tr("Analyse en cours")
            if kind == "crawl"
            else tr("Traitement en cours")
        )
        message = (
            tr("Les pages ont été parcourues ; le bilan est encore en préparation.")
            if finalizing and kind == "crawl"
            else tr("Le traitement finalise ses résultats ; ils ne sont pas encore disponibles.")
            if finalizing
            else tr("Le suivi affiche les informations reçues du moteur. Vous pouvez consulter vos autres sites pendant ce temps.")
        )
    elif ready:
        stage, headline, message = (
            4,
            tr("Votre bilan est disponible"),
            tr("Consultez les résultats de cette analyse pour choisir la prochaine étape."),
        )
    elif status == "done" and kind == "crawl":
        stage, headline, message = (
            3,
            tr("Traitement terminé · bilan indisponible"),
            tr("Le rapport de cette analyse n’est pas lisible pour le moment. Consultez le site ou actualisez cet état avant de lancer une nouvelle analyse."),
        )
    elif status == "done":
        stage, headline, message = (
            4,
            tr("Traitement terminé"),
            tr("L’exécution est terminée. Consultez les sites concernés pour retrouver les résultats disponibles."),
        )
    elif status == "canceled":
        headline, message = (
            tr("Analyse annulée") if kind == "crawl" else tr("Traitement annulé"),
            tr("Aucun nouveau résultat n’est confirmé par cet arrêt. Les bilans précédents restent accessibles depuis le site."),
        )
    elif status == "failed":
        headline, message = (
            tr("L’analyse n’a pas abouti")
            if kind == "crawl"
            else tr("Le traitement n’a pas abouti"),
            tr("Consultez les détails si nécessaire. Une relance crée une nouvelle demande et peut consommer le quota du compte qui gère le site."),
        )
    else:
        headline, message = (
            tr("État non reconnu"),
            tr("Actualisez cet état avant d’entreprendre une nouvelle action."),
        )
    return {
        "kind": kind,
        "title": title,
        "site_name": site_name,
        "project_url": project_url,
        "report_url": report_url if ready else "",
        "report_ready": ready,
        "active": active,
        "stage": stage,
        "headline": headline,
        "message": message,
    }
