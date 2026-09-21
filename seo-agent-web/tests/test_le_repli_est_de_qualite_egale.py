# -*- coding: utf-8 -*-
"""Quand Claude n'est pas joignable, le client ne doit pas recevoir un moteur au rabais.

DÉCISION DU PROPRIÉTAIRE, 21/09/2026 : *« le repli doit être de qualité égale »*.

CE QUI L'A DÉCLENCHÉE. Les crédits Anthropic étaient épuisés depuis plusieurs jours et
personne ne le savait : `_correction_ai_json` basculait en silence sur OpenAI. Deux défauts se
cachaient derrière ce silence.

    1. Le modèle de repli était `gpt-4o-mini`, écrit en dur — un petit modèle bon marché, là
       où le forfait promet Sonnet ou Opus.
    2. Le PALIER disparaissait avec le fournisseur : `model_override` n'était honoré que du
       côté Anthropic. Un client Business à 199 € et un client Solo à 49 € recevaient le même
       moteur. Le code le disait lui-même, dans un commentaire, sans que personne n'agisse.

ET LA VRAIE CAUSE N'ÉTAIT NI L'UN NI L'AUTRE. Mesuré le 21/09 : **toute la génération gpt-5
refuse le paramètre `max_tokens`** que ce client envoyait. Un modèle moderne rejetait l'appel
avant même de le lire. Le défaut n'était donc pas un mauvais choix de nom — c'était un client
figé sur une forme d'API périmée, et le `mini` était la seule chose qui marchait encore.

C'est pour ça que ce fichier teste surtout la FORME DE L'APPEL. Un nom de modèle se change
sans déployer (`PLAN_CONFIG_JSON`) ; une forme d'appel périmée, non.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-repli-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402
from backend import billing  # noqa: E402


class _Reponse:
    def __init__(self, code: int, texte: str = "", contenu: str = '{"ok": 1}') -> None:
        self.status_code = code
        self.text = texte
        self._contenu = contenu

    def json(self) -> dict:
        return {"choices": [{"message": {"content": self._contenu}}]}


@pytest.fixture()
def openai(monkeypatch):
    """Enregistre chaque corps envoyé à OpenAI, et laisse le test choisir les réponses."""
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.delenv("OPENAI_CHAT_MODEL", raising=False)
    monkeypatch.delenv("OPENAI_MODEL", raising=False)
    m._OPENAI_SANS_TEMPERATURE.clear()
    etat = {"envois": [], "reponses": []}

    def _post(url, headers=None, json=None, timeout=None):
        # UNE COPIE, et c'est le test qui l'a appris : le code retire `temperature` du MEME
        # dictionnaire avant de rejouer. En l'enregistrant par reference, les deux envois
        # n'en faisaient qu'un et le test affirmait le contraire de ce qui s'etait passe.
        etat["envois"].append(dict(json or {}))
        return etat["reponses"].pop(0) if etat["reponses"] else _Reponse(200)

    monkeypatch.setattr(m.requests, "post", _post)
    yield etat
    m._OPENAI_SANS_TEMPERATURE.clear()


# ── la forme de l'appel, qui était la vraie cause ────────────────────────────────────────────

def test_le_plafond_de_jetons_est_envoye_sous_sa_forme_MODERNE(openai) -> None:
    """`max_tokens` est refusé par toute la génération gpt-5 :
    « Unsupported parameter: 'max_tokens' is not supported with this model ».
    `max_completion_tokens` est accepté par les deux générations — vérifié sur gpt-5.5 ET sur
    gpt-4o-mini le 21/09/2026."""
    m._openai_chat_text(system="s", user_msg="u", model="gpt-5.5",
                        max_tokens=1234, temperature=0.05, json_mode=True)
    envoye = openai["envois"][0]
    assert envoye["max_completion_tokens"] == 1234, envoye
    assert "max_tokens" not in envoye, "la forme périmée est toujours envoyée"


def test_une_temperature_REFUSEE_est_retiree_et_l_appel_rejoue(openai) -> None:
    """gpt-5.5 : « Only the default (1) is supported ». On ne devine pas quels modèles le font
    à partir de leur nom — ce serait un pari sur les modèles à venir, et c'est ce pari qui a
    figé ce client. On lit le refus et on réessaie."""
    openai["reponses"] = [_Reponse(400, "Unsupported value: 'temperature' does not support 0.05"),
                          _Reponse(200)]
    texte = m._openai_chat_text(system="s", user_msg="u", model="gpt-5.5",
                                max_tokens=100, temperature=0.05, json_mode=True)
    assert texte == '{"ok": 1}'
    assert len(openai["envois"]) == 2, openai["envois"]
    assert "temperature" in openai["envois"][0] and "temperature" not in openai["envois"][1]


def test_le_modele_qui_a_refuse_est_RETENU_pour_les_appels_suivants(openai) -> None:
    """Sinon chaque appel paie un aller-retour perdu."""
    openai["reponses"] = [_Reponse(400, "'temperature' does not support 0.05"), _Reponse(200)]
    m._openai_chat_text(system="s", user_msg="u", model="gpt-5.5",
                        max_tokens=100, temperature=0.05, json_mode=True)
    m._openai_chat_text(system="s", user_msg="u", model="gpt-5.5",
                        max_tokens=100, temperature=0.05, json_mode=True)
    assert len(openai["envois"]) == 3, "le second appel a re-tenté la température"
    assert "temperature" not in openai["envois"][2]


def test_un_autre_400_n_est_PAS_rejoue(openai) -> None:
    """Le témoin : réessayer une erreur qu'on n'a pas comprise doublerait chaque panne."""
    openai["reponses"] = [_Reponse(400, "context_length_exceeded")]
    with pytest.raises(RuntimeError):
        m._openai_chat_text(system="s", user_msg="u", model="gpt-5.5",
                            max_tokens=100, temperature=0.05, json_mode=True)
    assert len(openai["envois"]) == 1, openai["envois"]


# ── le palier suit le fournisseur ────────────────────────────────────────────────────────────

def test_chaque_forfait_a_un_pair_OpenAI() -> None:
    cat = billing.plan_catalog()
    for plan in ("free", "solo", "pro", "business"):
        assert str(cat[plan]["correction"].get("model_openai") or ""), plan


def test_aucun_pair_n_est_un_modele_MINI() -> None:
    """La garde qui énumère : c'est exactement ce qui était servi, et un `mini` réintroduit un
    jour dans la table repasserait inaperçu."""
    cat = billing.plan_catalog()
    fautifs = [p for p in ("free", "solo", "pro", "business")
               if any(mot in str(cat[p]["correction"].get("model_openai") or "").lower()
                      for mot in ("mini", "nano", "3.5", "instruct"))]
    assert not fautifs, "ces forfaits retombent sur un modèle au rabais : %s" % fautifs


def test_le_PALIER_survit_au_changement_de_fournisseur(openai, monkeypatch) -> None:
    """Le cœur de la décision. `model_override` porte le modèle du forfait ; côté OpenAI on
    sert le pair du MÊME palier, déduit de lui — donc aucun des vingt appelants ne peut
    l'oublier."""
    monkeypatch.setenv("PLAN_CONFIG_JSON", json.dumps(
        {"business": {"correction": {"model_openai": "modele-haut-de-gamme"}},
         "free": {"correction": {"model_openai": "modele-standard"}},
         "solo": {"correction": {"model_openai": "modele-standard"}},
         "pro": {"correction": {"model_openai": "modele-standard"}}}))
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    m._correction_ai_json(system="s", user_msg="u", model_override="claude-opus-4-8")
    assert openai["envois"][-1]["model"] == "modele-haut-de-gamme", openai["envois"][-1]
    m._correction_ai_json(system="s", user_msg="u", model_override="claude-sonnet-4-6")
    assert openai["envois"][-1]["model"] == "modele-standard", openai["envois"][-1]


def test_deux_forfaits_au_MEME_moteur_ont_le_MEME_pair() -> None:
    """La correspondance se fait par MODELE, pas par forfait — et trois forfaits partagent
    Sonnet. Si l'un d'eux recevait un pair different, la recherche inverse rendrait celui du
    premier venu, en silence. Cette garde rend cette configuration impossible a poser sans
    que quelqu'un le voie."""
    cat = billing.plan_catalog()
    par_moteur: dict[str, set] = {}
    for plan in ("free", "solo", "pro", "business"):
        corr = cat[plan]["correction"]
        par_moteur.setdefault(str(corr["model"]), set()).add(str(corr.get("model_openai") or ""))
    incoherents = {moteur: pairs for moteur, pairs in par_moteur.items() if len(pairs) > 1}
    assert not incoherents, (
        "ces moteurs Anthropic ont plusieurs pairs OpenAI selon le forfait : %s" % incoherents)


def test_un_modele_INCONNU_retombe_sur_le_pair_standard_pas_sur_rien() -> None:
    """Rendre "" ferait taire le repli entier. Une panne vaut mieux qu'une dégradation muette
    seulement quand elle est CHOISIE, pas quand elle est accidentelle."""
    assert billing.openai_peer_for_model("un-modele-que-personne-ne-connait")
    assert billing.openai_peer_for_model("") == billing.openai_peer_for_model("inconnu")


def test_le_defaut_sans_palier_vient_du_CATALOGUE(monkeypatch) -> None:
    monkeypatch.delenv("OPENAI_CHAT_MODEL", raising=False)
    monkeypatch.delenv("OPENAI_MODEL", raising=False)
    monkeypatch.setenv("PLAN_CONFIG_JSON", json.dumps(
        {"pro": {"correction": {"model_openai": "modele-du-catalogue"}}}))
    assert m._correction_ai_model("openai") == "modele-du-catalogue"


def test_la_surcharge_d_ENVIRONNEMENT_reste_prioritaire(openai, monkeypatch) -> None:
    """Le levier d'urgence : changer de modèle sans toucher au catalogue ni déployer."""
    monkeypatch.setenv("OPENAI_CHAT_MODEL", "modele-d-urgence")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    m._correction_ai_json(system="s", user_msg="u", model_override="claude-opus-4-8")
    assert openai["envois"][-1]["model"] == "modele-d-urgence", openai["envois"][-1]


def test_les_pairs_se_reglent_SANS_deploiement(monkeypatch) -> None:
    """Un nom de modèle vieillit ; une table qu'on ne peut changer qu'en déployant vieillit
    mal. C'est déjà vrai des quotas, ça doit l'être des moteurs."""
    defaut = billing.plan_catalog()["business"]["correction"]["model_openai"]
    monkeypatch.setenv("PLAN_CONFIG_JSON", json.dumps(
        {"business": {"correction": {"model_openai": "autre-modele"}}}))
    assert billing.plan_catalog()["business"]["correction"]["model_openai"] == "autre-modele"
    monkeypatch.delenv("PLAN_CONFIG_JSON")
    assert billing.plan_catalog()["business"]["correction"]["model_openai"] == defaut


def test_le_modele_ANTHROPIC_du_forfait_n_est_pas_touche() -> None:
    """Le témoin de tout ce qui précède : on a ajouté un pair, on n'a pas déplacé l'original."""
    cat = billing.plan_catalog()
    assert cat["business"]["correction"]["model"] == "claude-opus-4-8"
    assert cat["pro"]["correction"]["model"] == "claude-sonnet-4-6"
