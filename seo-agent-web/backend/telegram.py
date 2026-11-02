"""Les notifications du proprietaire sur Telegram : ce qui arrive sur la plateforme, en direct.

Demande du proprietaire, 02/11/2026 : etre prevenu des evenements importants — un compte cree,
un abonnement souscrit, etc. Un bot Telegram, et un seul destinataire : la conversation de
`TELEGRAM_CHAT_ID`, avec le jeton `TELEGRAM_BOT_TOKEN`. Les deux se reglent dans la page Systeme
(ou dans l'environnement) ; le jeton ne transite jamais ailleurs.

TROIS REGLES :
  * NON CONFIGURE, RIEN NE PART — et rien ne casse : c'est le cas des tests et du developpement ;
  * UNE NOTIFICATION NE FAIT JAMAIS ECHOUER L'ACTION QUI LA DECLENCHE : une inscription ou un
    webhook Stripe ne doit ni attendre Telegram, ni tomber avec lui. L'envoi part dans un fil ;
  * le texte est ECHAPPE : une adresse ou un nom de forfait n'est jamais du balisage.

Ces messages portent l'adresse email des clients : ils vont dans la conversation privee du
proprietaire, qui en est le destinataire legitime.
"""
from __future__ import annotations

import html
import logging
import os
import threading
from typing import Any

import requests

logger = logging.getLogger("seo-agent")

_API = "https://api.telegram.org"
_DELAI_S = 10


def _jeton() -> str:
    return str(os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()


def _conversation() -> str:
    return str(os.environ.get("TELEGRAM_CHAT_ID") or "").strip()


def configure() -> bool:
    return bool(_jeton() and _conversation())


def echapper(texte: Any) -> str:
    return html.escape(str(texte if texte is not None else ""), quote=False)


def envoyer_maintenant(texte: str, *, jeton: str = "", conversation: str = "") -> tuple[bool, str]:
    """Envoie et ATTEND la reponse (bouton « tester »). Rend (reussi, raison de l'echec)."""
    jeton = jeton or _jeton()
    conversation = conversation or _conversation()
    if not jeton or not conversation:
        return False, "jeton ou identifiant de conversation manquant"
    try:
        reponse = requests.post(f"{_API}/bot{jeton}/sendMessage", timeout=_DELAI_S, json={
            "chat_id": conversation, "text": texte[:4000], "parse_mode": "HTML",
            "disable_web_page_preview": True})
        donnees = reponse.json() if reponse.content else {}
    except Exception as e:
        # Le jeton est DANS l'adresse : on ne rapporte jamais le message brut d'une exception
        # de requests, qui la cite.
        return False, "Telegram injoignable (%s)" % type(e).__name__
    if reponse.status_code != 200 or not donnees.get("ok"):
        return False, "Telegram refuse : %s" % str(donnees.get("description") or reponse.status_code)[:200]
    return True, ""


def notifier(texte: str) -> None:
    """Envoie sans attendre, dans un fil ; ne leve jamais. Ne fait rien si non configure."""
    if not configure():
        return

    def _envoi() -> None:
        reussi, raison = envoyer_maintenant(texte)
        if not reussi:
            logger.warning("[TELEGRAM] notification non envoyee : %s", raison)

    threading.Thread(target=_envoi, name="notification-telegram", daemon=True).start()


def conversations_recentes(jeton: str = "") -> tuple[list[dict[str, str]], str]:
    """Les conversations qui ont ecrit au bot recemment : [{id, nom}], ou ([], raison).

    Pour trouver son identifiant : ecrire au bot, puis demander cette liste. Le jeton reste sur
    le serveur ; l'identifiant, lui, n'est pas un secret.
    """
    jeton = jeton or _jeton()
    if not jeton:
        return [], "jeton du bot manquant"
    try:
        reponse = requests.get(f"{_API}/bot{jeton}/getUpdates", timeout=_DELAI_S)
        donnees = reponse.json() if reponse.content else {}
    except Exception as e:
        return [], "Telegram injoignable (%s)" % type(e).__name__
    if reponse.status_code != 200 or not donnees.get("ok"):
        return [], "Telegram refuse : %s" % str(donnees.get("description") or reponse.status_code)[:200]
    vues: dict[str, str] = {}
    for maj in donnees.get("result") or []:
        message = (maj or {}).get("message") or (maj or {}).get("my_chat_member") or {}
        chat = message.get("chat") or {}
        if chat.get("id") is None:
            continue
        nom = chat.get("title") or " ".join(x for x in (chat.get("first_name"), chat.get("last_name")) if x) \
            or chat.get("username") or str(chat.get("type") or "")
        vues[str(chat["id"])] = str(nom)
    return [{"id": k, "nom": v} for k, v in vues.items()], ""
