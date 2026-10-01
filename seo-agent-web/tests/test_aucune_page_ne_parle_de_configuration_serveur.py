# -*- coding: utf-8 -*-
"""Aucune page ne demande au lecteur une manipulation que seul l'hébergeur peut faire.

01/10/2026 : la page Backlinks d'un client disait « Pour la synchro API : AHREFS_API_TOKEN dans
.env (plan Enterprise) » — un fichier qu'il n'a pas, et un « plan Enterprise » que Noyaru ne vend
pas (c'est l'offre d'Ahrefs). La page Abonnement, elle, disait « Ajoute les variables STRIPE_*
sur Render puis redeploy ». On lit le TEXTE VISIBLE des gabarits — sans balises, commentaires,
scripts ni expressions Jinja — et on y cherche ce vocabulaire d'exploitation.
"""
from __future__ import annotations

import re
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
MOTIF = re.compile(r"\.env\b|\b[A-Z][A-Z0-9]{2,}_[A-Z0-9_]*(?:TOKEN|KEY|SECRET|ID)\b|\bredeploy\b|\bsur Render\b|Render env")


def _texte_visible(source: str) -> str:
    s = re.sub(r"\{#.*?#\}", " ", source, flags=re.S)
    s = re.sub(r"<(script|style)\b.*?</\1>", " ", s, flags=re.S)
    s = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\{\{.*?\}\}|\{%.*?%\}", " ", s, flags=re.S)


def test_aucun_gabarit_n_affiche_de_configuration_serveur() -> None:
    trouves = []
    for f in sorted((WEB_ROOT / "templates").glob("*.html")):
        for m in MOTIF.finditer(_texte_visible(f.read_text(encoding="utf-8"))):
            trouves.append("%s : %s" % (f.name, m.group(0)))
    assert not trouves, trouves


def test_le_temoin_reconnait_la_phrase_d_origine() -> None:
    phrase = 'Pour la synchro API : <span class="mono">AHREFS_API_TOKEN</span> dans <span class="mono">.env</span>'
    assert MOTIF.search(_texte_visible(phrase))
    assert MOTIF.search(_texte_visible("Ajoute les variables `STRIPE_SECRET_KEY` sur Render puis redeploy."))
    assert not MOTIF.search(_texte_visible('<input type="hidden" name="key" value="GITHUB_TOKEN" />'))
