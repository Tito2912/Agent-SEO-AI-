"""Ce que Sentry reçoit d'une erreur : où et pourquoi ça casse, jamais les données des clients.

Relevé du 05/11/2026, en préparant l'accord de traitement des données (DPA) : Sentry était
initialisé sans aucun filtre. Le SDK 2.x envoyait alors, par défaut, les variables locales de
chaque fonction de la pile (contenu de pages, extraits de dépôts, adresses e-mail), le corps des
formulaires reçus (l'e-mail d'une inscription), l'adresse complète de la requête avec ses
paramètres (`/auth/verify?token=…`), et un fil des journaux et des appels sortants — dont la clé
SerpAPI, passée en paramètre d'adresse, et le jeton Telegram, écrit dans l'adresse même de l'API.
La politique de confidentialité, elle, annonce des « traces d'erreur, sans adresse IP ni
identifiant de compte ».

Décision du propriétaire, même jour : FILTRER, plutôt que déclarer Sentry comme destinataire de
données des clients. Ce qui reste suffit à corriger : le type de l'erreur, la pile (fichiers,
lignes, code du service), le chemin de la requête, l'environnement et la version.
"""
from __future__ import annotations

import re
from typing import Any

COURRIEL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}")
# Le jeton d'un bot Telegram (« 123456789:ABC… »), tel qu'il figure dans l'adresse de son API.
# Il suit « bot » sans séparation : pas de limite de mot avant lui.
JETON_TELEGRAM = re.compile(r"(?<!\d)\d{5,15}:[A-Za-z0-9_-]{30,64}(?![A-Za-z0-9_-])")
# Ce qui suit « ? » ou « # » dans une adresse : jetons de vérification, clés d'API, e-mails.
PARAMETRES = re.compile(r"(https?://[^\s?#\"']+)[?#][^\s\"']*")
# Les paramètres d'adresse que le SDK range à part dans le fil des appels sortants.
CLES_RETIREES = {"http.query", "http.fragment"}
# Ce que la requête reçue emporte et qui ne sert pas à corriger : formulaire, cookies, paramètres.
REQUETE_RETIREE = ("data", "cookies", "query_string")


def nettoyer(valeur: Any) -> Any:
    """La même valeur, sans adresse e-mail, jeton Telegram ni paramètre d'adresse, à toute
    profondeur."""
    if isinstance(valeur, str):
        valeur = PARAMETRES.sub(r"\1", valeur)
        valeur = JETON_TELEGRAM.sub("[jeton]", valeur)
        return COURRIEL.sub("[e-mail]", valeur)
    if isinstance(valeur, dict):
        return {cle: nettoyer(v) for cle, v in valeur.items() if cle not in CLES_RETIREES}
    if isinstance(valeur, (list, tuple)):
        return [nettoyer(v) for v in valeur]
    return valeur


def nettoyer_evenement(evenement: dict, _indice: Any = None) -> dict:
    """Une erreur ou une mesure de performance, juste avant son départ. Le fil des journaux et des
    appels sortants voyage DANS l'événement : il est nettoyé avec lui."""
    requete = evenement.get("request")
    if isinstance(requete, dict):
        for cle in REQUETE_RETIREE:
            requete.pop(cle, None)
    return nettoyer(evenement)


def options() -> dict[str, Any]:
    """Ce qu'ajoute Noyaru à `sentry_sdk.init` : rien de ce que le SDK collecte par défaut sur
    les personnes, et un nettoyage de tout ce qui part malgré tout (messages, fil, contextes)."""
    return {
        "send_default_pii": False,
        "include_local_variables": False,
        "max_request_body_size": "never",
        "before_send": nettoyer_evenement,
        "before_send_transaction": nettoyer_evenement,
    }
