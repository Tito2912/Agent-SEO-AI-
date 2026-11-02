# -*- coding: utf-8 -*-
"""Ce qu'un formulaire d'inscription rempli par une PERSONNE porte en plus de ses champs.

Depuis le 02/11/2026 (backend/anti_robots.py), une inscription sans heure d'ouverture signée,
ou envoyée trop vite, est refusée comme venant d'un robot. Un test qui parle d'autre chose —
invitation, pays, email de vérification — doit envoyer ce qu'enverrait une personne : sinon il
serait refusé par la garde des robots et passerait pour la MAUVAISE raison.
"""
from __future__ import annotations

import os
import time

from backend import anti_robots


def jeton_humain(il_y_a_s: float = 30.0) -> str:
    """Le formulaire « ouvert il y a 30 secondes », signé avec la clé de la suite de tests."""
    return anti_robots.jeton_d_ouverture(os.environ["SEO_AGENT_SECRET_KEY"],
                                         maintenant=time.time() - il_y_a_s)
