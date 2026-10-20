"""Customer activity milestones describe observed execution and report availability separately."""

from __future__ import annotations


def view(job, *, site_name="", project_url="", report_url="", kind=""):
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
        "Analyse de " + site_name
        if kind == "crawl" and site_name
        else (
            "Audit de plusieurs sites" if kind == "autopilot" else "Suivi du traitement"
        )
    )
    if status == "queued":
        headline, message = (
            "Votre demande est en attente",
            "Elle attend un créneau disponible. Vous pouvez revenir plus tard pour consulter sa progression.",
        )
    elif status == "cancel_requested":
        headline, message = (
            "Arrêt demandé",
            "Le traitement termine son étape en cours avant de s’arrêter. Attendez la confirmation avant de faire une nouvelle demande.",
        )
    elif active:
        headline = (
            "Préparation du bilan"
            if finalizing
            else "Analyse en cours"
            if kind == "crawl"
            else "Traitement en cours"
        )
        message = (
            "Les pages ont été parcourues ; le bilan est encore en préparation."
            if finalizing
            else "Le suivi affiche les informations reçues du moteur. Vous pouvez consulter vos autres sites pendant ce temps."
        )
    elif ready:
        stage, headline, message = (
            4,
            "Votre bilan est disponible",
            "Consultez les résultats de cette analyse pour choisir la prochaine étape.",
        )
    elif status == "done" and kind == "crawl":
        stage, headline, message = (
            3,
            "Traitement terminé · bilan indisponible",
            "Le rapport de cette analyse n’est pas lisible pour le moment. Consultez le site ou actualisez cet état avant de lancer une nouvelle analyse.",
        )
    elif status == "done":
        stage, headline, message = (
            4,
            "Traitement terminé",
            "L’exécution est terminée. Consultez les sites concernés pour retrouver les résultats disponibles.",
        )
    elif status == "canceled":
        headline, message = (
            "Analyse annulée" if kind == "crawl" else "Traitement annulé",
            "Aucun nouveau résultat n’est confirmé par cet arrêt. Les bilans précédents restent accessibles depuis le site.",
        )
    elif status == "failed":
        headline, message = (
            "L’analyse n’a pas abouti"
            if kind == "crawl"
            else "Le traitement n’a pas abouti",
            "Consultez les détails si nécessaire. Une relance crée une nouvelle demande et peut consommer le quota du compte qui gère le site.",
        )
    else:
        headline, message = (
            "État non reconnu",
            "Actualisez cet état avant d’entreprendre une nouvelle action.",
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
