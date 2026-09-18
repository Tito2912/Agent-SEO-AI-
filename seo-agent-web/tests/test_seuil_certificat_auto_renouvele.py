# -*- coding: utf-8 -*-
"""Un certificat auto-renouvele passe sous les 30 jours a chaque cycle : ce n'est pas une alerte.

Faux positif SYSTEMATIQUE trouve le 18/09/2026 en recrawlant les sites de reference.
easyshopbuilder.com montrait `certificate_expiration` = 1 quand Ahrefs n'affichait rien.

Let's Encrypt emet des certificats de 90 jours et les renouvelle par machine quand il en reste une
trentaine. Un seuil fixe a 30 jours accuse donc TOUT site qui en depend, a chaque cycle, sans que
rien n'aille mal — c'est-a-dire la quasi-totalite des sites hebergés sur Netlify, Vercel ou
Cloudflare. Mesure du jour, quatre hotes reels :

    easyshopbuilder.com    reste 30 j   certificat de 89 j   Let's Encrypt        -> accuse a tort
    creativeai-tools.com   reste 55 j   certificat de 89 j   Let's Encrypt
    vidforges.com          reste 48 j   certificat de 89 j   Let's Encrypt
    noyaru.com             reste 33 j   certificat de 90 j   Google Trust          -> a 3 jours de
                                                                                     s'accuser

Le dernier chiffre est celui qui tranche : le produit etait a trois jours de signaler son propre
site. Une alerte qui se declenche sur tout le monde a intervalle regulier n'apprend plus rien a
personne, et la seule chose qu'elle entraine est qu'on cesse de la lire.

ON NE DEVINE PAS si le renouvellement est automatique : on lit la DUREE du certificat, qui la dit.
Quatre-vingt-dix jours est la signature des autorites qui renouvellent par machine ; un an, celle
d'un achat qu'une personne doit refaire. COMPTER bat RECONNAITRE, ici comme ailleurs.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_SRC = (Path(__file__).resolve().parents[2]
        / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py")
_spec = importlib.util.spec_from_file_location("seo_audit_cert", _SRC)
assert _spec and _spec.loader
audit = importlib.util.module_from_spec(_spec)
sys.modules["seo_audit_cert"] = audit
_spec.loader.exec_module(audit)


def _alerte(validity_days, days_left: int) -> bool:
    return days_left <= audit._seuil_alerte_certificat(validity_days)


def test_les_quatre_certificats_REELS_du_jour_restent_silencieux() -> None:
    """Les valeurs mesurees le 18/09/2026, y compris celle qui declenchait l'alerte."""
    assert not _alerte(89, 30), "easyshopbuilder : c'est ce cas qui a ouvert le chantier"
    assert not _alerte(89, 55)
    assert not _alerte(89, 48)
    assert not _alerte(90, 33), "noyaru.com allait s'accuser lui-meme sous trois jours"


def test_un_renouvellement_automatique_qui_a_VRAIMENT_echoue_est_signale() -> None:
    """A moins de 7 jours sur un certificat de 90, la machine a echoue trois semaines durant."""
    assert _alerte(89, 5)
    assert _alerte(89, 0)


def test_un_certificat_ANNUEL_garde_le_seuil_large() -> None:
    """Une personne doit s'en occuper : un mois de preavis n'est pas de trop."""
    assert _alerte(365, 20)
    assert not _alerte(365, 45)


def test_une_duree_INCONNUE_retombe_sur_le_seuil_prudent() -> None:
    """Ne rien savoir ne justifie pas de se taire : c'est le seul defaut acceptable des deux."""
    assert _alerte(None, 20)
    assert _alerte(0, 20)
    assert not _alerte(None, 45)


def test_la_frontiere_entre_les_deux_regimes_est_bien_a_cent_jours() -> None:
    """Choisie au-dessus des 90 jours reels pour absorber les variantes (89, 90, 97), et sous les
    formules annuelles. Un certificat de six mois compte comme manuel : personne ne connait
    d'autorite qui renouvelle par machine a cette cadence."""
    assert audit._seuil_alerte_certificat(100) == 7
    assert audit._seuil_alerte_certificat(101) == 30
    assert audit._seuil_alerte_certificat(180) == 30
