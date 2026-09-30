# -*- coding: utf-8 -*-
"""Aucun plan payant ne descend sous 40 % de marge, même dans le pire cas.

Décision du propriétaire, 30/09/2026 : « la marge est inacceptable, je ne peux pas risquer des
pertes ». À quota plein, 900 corrections en Opus coûtaient à Business 78 à 256 € pour 166 € HT.
Règle retenue : dans le PIRE cas — tous les quotas consommés, uniquement sur de gros fichiers —
chaque plan payant garde au moins 40 % de marge (25 % a d'abord été proposé, jugé trop juste :
une erreur d'estimation de 50 % aurait suffi à faire perdre de l'argent).

Ce test refait le calcul à partir du CATALOGUE réel (quotas et modèle de chaque plan) et de la
TABLE DE PRIX du code (`_PRIX_IA_USD_MTOK`). Relever un quota, ou remettre un plan sur un modèle
plus cher, le fait échouer : la décision se prend alors en connaissance de cause.

Les tailles d'appel « pire cas » sont des estimations tirées des plafonds du code ; le tableau
« Coût réel des IA » de /settings/operations dira ce qu'elles valent vraiment.
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
os.environ.setdefault("SEO_AGENT_DATA_DIR", tempfile.mkdtemp(prefix="seo-agent-marge-"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", tempfile.mkdtemp(prefix="seo-agent-marge-runs-"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402
from backend import billing  # noqa: E402

MARGE_MINIMALE = 0.40
# Pire cas par unité : (tokens d'entrée, tokens de sortie). Correction : gros fichier et
# réécriture complète (plafond 8 000) ; article : version longue ; message : historique plein.
PIRE = {"correction": (20_000, 6_000), "article": (10_000, 7_400), "reponse": (2_000, 1_000),
        "message": (10_000, 1_000)}
SERPAPI_USD = 0.025          # formule Starter, la plus chère par recherche
CONCURRENT_USD = 0.08        # par concurrent et par mois : découverte + sujets
STRIPE = (0.027, 0.25)       # carte UE + Billing + Tax, et fixe par paiement


def _usd(modele: str, unite: str) -> float:
    entree, sortie = PIRE[unite]
    cout = m._cout_ia_usd(modele, entree=entree, sortie=sortie)
    assert cout is not None, "modèle sans prix dans la table : %s" % modele
    return cout


def _pire_cas_eur(plan: str) -> float:
    cat = billing.plan_catalog()[plan]
    lim = cat["limits"]
    modele = cat["correction"]["model"]
    defaut = m._correction_ai_model("anthropic")          # réponses de backlinks, ciblage
    assistant = m._assistant_model("claude")
    usd = (lim.get("ai_corrections_month", 0) * _usd(modele, "correction")
           + lim.get("ai_articles_month", 0) * _usd(modele, "article")
           + lim.get("backlink_replies_month", 0) * _usd(defaut, "reponse")
           + lim.get("assistant_messages_month", 0) * _usd(assistant, "message")
           + lim.get("backlink_searches_month", 0) * SERPAPI_USD)
    if billing.plan_rank(plan) >= billing.plan_rank("pro"):
        usd += lim.get("projects", 0) * m._COMPETITOR_MAX_PER_PROJECT * CONCURRENT_USD
    ttc = m._prix_ht_du_plan(plan) * 1.2
    return usd * m._TAUX_USD_EUR + ttc * STRIPE[0] + STRIPE[1]


@pytest.fixture(autouse=True)
def _sans_surcharge(monkeypatch):
    for v in ("PLAN_CONFIG_JSON", "SEO_CORRECTION_ANTHROPIC_MODEL", "SEO_AUDIT_ASSISTANT_CLAUDE_MODEL"):
        monkeypatch.delenv(v, raising=False)


@pytest.mark.parametrize("plan", ["solo", "pro", "business"])
def test_marge_du_pire_cas_au_moins_40_pourcent(plan) -> None:
    ht = m._prix_ht_du_plan(plan)
    cout = _pire_cas_eur(plan)
    marge = (ht - cout) / ht
    assert marge >= MARGE_MINIMALE, "%s : pire cas %.2f € pour %.2f € HT, marge %.0f %%" % (
        plan, cout, ht, marge * 100)


def test_aucun_plan_client_n_est_sur_OPUS_par_defaut() -> None:
    """Le double de Sonnet par correction : c'est lui qui mettait Business en perte."""
    cat = billing.plan_catalog()
    for plan in ("free", "solo", "pro", "business"):
        assert "opus" not in cat[plan]["correction"]["model"].lower(), plan
    assert "opus" not in m._correction_ai_model("anthropic").lower(), \
        "les appels sans modèle précisé (réponses de backlinks, ciblage) ne passent pas sur Opus"


def test_le_temoin_Business_en_Opus_a_900_corrections_ECHOUE(monkeypatch) -> None:
    """Le témoin : l'ancien Business doit être refusé par ce calcul, sinon il ne prouve rien."""
    import json
    monkeypatch.setenv("PLAN_CONFIG_JSON", json.dumps({"business": {
        "limits": {"ai_corrections_month": 900, "assistant_messages_month": 6000,
                   "backlink_searches_month": 1000, "backlink_replies_month": 1000},
        "correction": {"model": "claude-opus-4-8"}}}))
    ht = m._prix_ht_du_plan("business")
    assert (ht - _pire_cas_eur("business")) / ht < 0, "l'ancien Business perdait de l'argent"
