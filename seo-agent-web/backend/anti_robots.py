"""Les inscriptions de robots : les reconnaitre sans rien demander a une personne.

Releve du 02/11/2026, a la demande du proprietaire : 39 des 44 comptes de la plateforme etaient
des inscriptions de robots, faites du 11 au 18/10 avec les adresses REELLES de personnes qui
n'avaient rien demande. Chacune leur a fait envoyer un email de Noyaru — c'est la reputation
d'envoi qui paie, et ces personnes qui recoivent un message qu'elles n'ont pas sollicite. Depuis
le 18/10, le pays obligatoire les arrete PAR ACCIDENT (les robots laissent le menu vide) : le
journal d'audit montre encore des tentatives chaque nuit.

Deux signes, invisibles pour une personne :
  * un champ PIEGE, hors de l'ecran et hors de la tabulation, qu'un robot remplit comme les
    autres et qu'une personne ne voit jamais ;
  * le DELAI : le formulaire porte l'heure de son ouverture, signee ; une personne met plus de
    quelques secondes a ecrire une adresse, un mot de passe et choisir un pays, un robot non.
    Sans heure signee, le formulaire n'a pas ete servi par Noyaru.

Un refus ne cree rien et n'envoie rien. Il dit a une personne de reessayer : un faux positif
(gestionnaire de mots de passe tres rapide) coute un second clic, pas un compte.

LE CAPTCHA, EN PLUS (05/11/2026, a la demande du proprietaire) : Cloudflare Turnstile, le plus
souvent une simple coche qui se valide seule. Il vise ce que les deux signes ne voient pas : un
robot qui pilote un VRAI navigateur, attend et laisse le piege vide. Ce jour-la, les deux signes
arretaient encore tout (15 tentatives en deux jours, aucun compte cree). Sans ses DEUX cles
(Systeme -> Anti-robots), il n'existe pas : ni case a l'ecran, ni appel a Cloudflare.
"""
from __future__ import annotations

import hashlib
import hmac
import re
import time

import requests

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


# ── Le captcha Cloudflare Turnstile ──────────────────────────────────────────────────────────

CHAMP_CAPTCHA = "cf-turnstile-response"
VERIFICATION = "https://challenges.cloudflare.com/turnstile/v0/siteverify"
# Les cles de Cloudflare : « 0x… » en production, « 1x… », « 2x… », « 3x… » pour ses cles de test.
FORME_CLE = r"[0-3]x[A-Za-z0-9_-]{16,100}"
JETON_MAX = 2048
# Ce que Cloudflare repond quand c'est le JETON qui ne vaut rien : absent, faux, perime (5 min)
# ou deja servi. Ses autres erreurs — cle secrete refusee, requete mal formee, panne — sont les
# notres ou les siennes, jamais celles de la personne qui s'inscrit.
JETON_REFUSE = {"missing-input-response", "invalid-input-response", "timeout-or-duplicate"}


def cles_captcha(cle_site: str, cle_secrete: str) -> tuple[str, str]:
    """Les deux cles si elles sont la et bien formees, sinon ("", "").

    Jamais l'une sans l'autre : une case sans cle secrete ne verifierait rien, et une cle
    secrete sans case refuserait TOUTES les inscriptions (aucun jeton n'arriverait)."""
    cle_site, cle_secrete = str(cle_site or "").strip(), str(cle_secrete or "").strip()
    if re.fullmatch(FORME_CLE, cle_site) and re.fullmatch(FORME_CLE, cle_secrete):
        return cle_site, cle_secrete
    return "", ""


def _demander_a_cloudflare(donnees: dict) -> dict:
    reponse = requests.post(VERIFICATION, data=donnees, timeout=8)
    return reponse.json()


def verifier_captcha(cle_secrete: str, jeton: str, ip: str = "") -> tuple[str, str]:
    """`(raison de refus, incident)` ; deux chaines vides : la personne passe.

    Un REFUS arrete l'inscription : pas de jeton, ou un jeton que Cloudflare declare faux,
    perime ou deja servi. Un INCIDENT la laisse aux deux autres signes, et se journalise :
    Cloudflare injoignable, en panne, ou qui refuse notre cle secrete. Fermer la porte a ce
    moment-la refuserait toutes les inscriptions pour une panne que personne ne verrait — le piege
    et le delai, eux, tiennent toujours.
    """
    jeton = str(jeton or "").strip()
    if not jeton:
        return "captcha non résolu", ""
    if len(jeton) > JETON_MAX:
        return "jeton de captcha trop long", ""
    donnees = {"secret": cle_secrete, "response": jeton}
    if ip and ip != "unknown":
        donnees["remoteip"] = ip
    try:
        corps = _demander_a_cloudflare(donnees)
    except Exception as e:  # noqa: BLE001 — le TYPE seul : jamais le message, jamais la cle
        return "", "Cloudflare injoignable (%s)" % type(e).__name__
    if not isinstance(corps, dict):
        return "", "réponse de Cloudflare illisible"
    if corps.get("success") is True:
        return "", ""
    codes = [str(c) for c in (corps.get("error-codes") or [])]
    if not codes or set(codes) & JETON_REFUSE:
        return "captcha refusé par Cloudflare (%s)" % (", ".join(codes) or "sans motif"), ""
    return "", "Cloudflare n'a pas pu vérifier le captcha (%s)" % ", ".join(codes)
