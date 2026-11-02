"""Les inscriptions de robots : les reconnaitre sans rien demander a une personne.

Releve du 02/11/2026, a la demande du proprietaire : 39 des 44 comptes de la plateforme etaient
des inscriptions de robots, faites du 11 au 18/10 avec les adresses REELLES de personnes qui
n'avaient rien demande. Chacune leur a fait envoyer un email de Noyaru — c'est la reputation
d'envoi qui paie, et ces personnes qui recoivent un message qu'elles n'ont pas sollicite. Depuis
le 18/10, le pays obligatoire les arrete PAR ACCIDENT (les robots laissent le menu vide) : le
journal d'audit montre encore des tentatives chaque nuit.

Deux signes, invisibles pour une personne, et aucun captcha :
  * un champ PIEGE, hors de l'ecran et hors de la tabulation, qu'un robot remplit comme les
    autres et qu'une personne ne voit jamais ;
  * le DELAI : le formulaire porte l'heure de son ouverture, signee ; une personne met plus de
    quelques secondes a ecrire une adresse, un mot de passe et choisir un pays, un robot non.
    Sans heure signee, le formulaire n'a pas ete servi par Noyaru.

Un refus ne cree rien et n'envoie rien. Il dit a une personne de reessayer : un faux positif
(gestionnaire de mots de passe tres rapide) coute un second clic, pas un compte.
"""
from __future__ import annotations

import hashlib
import hmac
import time

CHAMP_PIEGE = "site_web"
CHAMP_JETON = "formulaire_ouvert"
DELAI_MIN_S = 3.0
# Un jeton se reutilise au plus deux heures : au-dela, un robot pourrait en garder un seul et
# l'envoyer indefiniment.
AGE_MAX_S = 2 * 3600


def _signature(secret: str, horodatage: str) -> str:
    return hmac.new(str(secret).encode("utf-8"), ("inscription:" + horodatage).encode("ascii"),
                    hashlib.sha256).hexdigest()[:32]


def jeton_d_ouverture(secret: str, *, maintenant: "float | None" = None) -> str:
    """L'heure d'ouverture du formulaire, signee : `<horodatage>.<signature>`."""
    horodatage = "%d" % int(time.time() if maintenant is None else maintenant)
    return "%s.%s" % (horodatage, _signature(secret, horodatage))


def raison_de_refus(secret: str, *, piege: str, jeton: str,
                    maintenant: "float | None" = None) -> str:
    """Pourquoi cet envoi vient d'un robot, ou "" s'il peut venir d'une personne."""
    if str(piege or "").strip():
        return "champ piège rempli"
    horodatage, _, signature = str(jeton or "").partition(".")
    if not horodatage.isdigit():
        return "formulaire sans heure d'ouverture"
    if not hmac.compare_digest(signature, _signature(secret, horodatage)):
        return "heure d'ouverture falsifiée"
    ecoule = (time.time() if maintenant is None else maintenant) - int(horodatage)
    if ecoule < DELAI_MIN_S:
        return "envoyé %.1f s après l'ouverture" % max(0.0, ecoule)
    if ecoule > AGE_MAX_S:
        return "formulaire ouvert depuis plus de deux heures"
    return ""
