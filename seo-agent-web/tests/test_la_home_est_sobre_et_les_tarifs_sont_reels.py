# -*- coding: utf-8 -*-
"""La page d'accueil et /pricing : sobres, et des quotas RÉELS par fonction et par plan.

Demande du propriétaire, 29/09/2026 : « on dirait un site pour enfant […] ajouter le plan free,
ajouter les quotas réels par fonction et par plan […] les badges et les icônes sont moches ».

Ce que ce fichier garde :

* la grille vient de `plan_catalog()` et des MÊMES portes que l'application — un réglage par
  `PLAN_CONFIG_JSON` change la page sans y toucher ;
* une fonction fermée par le PLAN s'affiche « — », jamais un nombre (pour `remaining_quota`,
  une limite nulle veut dire « pas de plafond » : l'afficher serait mentir dans les deux sens) ;
* ni émoji, ni badge, ni graisse 900 ne reviennent sur ces deux pages.
"""
from __future__ import annotations

import os
import re
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
os.environ.setdefault("SEO_AGENT_DATA_DIR", tempfile.mkdtemp(prefix="seo-agent-tarifs-"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", tempfile.mkdtemp(prefix="seo-agent-tarifs-runs-"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as m  # noqa: E402
from backend import billing  # noqa: E402

GABARITS = [WEB_ROOT / "templates" / n for n in ("home_public.html", "pricing_public.html", "_tarifs.html")]


def _ligne(comparatif: dict, libelle: str) -> list[str]:
    for section in comparatif["sections"]:
        for l in section["lignes"]:
            if l["libelle"].startswith(libelle):
                return l["valeurs"]
    raise AssertionError("ligne introuvable : %s" % libelle)


def test_les_quatre_plans_dans_l_ordre() -> None:
    assert m._comparatif_des_plans()["cles"] == ["free", "solo", "pro", "business"]


def test_les_valeurs_suivent_le_CATALOGUE(monkeypatch) -> None:
    """Un reglage de l'administrateur change la page, sans deploiement ni gabarit."""
    monkeypatch.setenv("PLAN_CONFIG_JSON", '{"pro": {"limits": {"ai_corrections_month": 345, "projects": 12}}}')
    c = m._comparatif_des_plans()
    assert _ligne(c, "Corrections écrites")[2] == "345"
    assert _ligne(c, "Sites suivis")[2] == "12"
    pro = TestClient(m.app).get("/pricing").text
    assert "345 corrections/mois" in pro and "12 sites suivis" in pro


def test_une_fonction_FERMEE_par_le_plan_s_affiche_absente() -> None:
    c = m._comparatif_des_plans()
    # Opportunites : Solo et au-dessus (`_opp_has_access`). Free n'a meme pas la cle.
    assert _ligne(c, "Recherches d'opportunités")[0] == ""
    # Concurrents et redaction : Pro et au-dessus, meme si Solo avait un quota d'articles.
    assert _ligne(c, "Concurrents analysés")[:2] == ["", ""]
    assert _ligne(c, "Articles rédigés")[:2] == ["", ""]
    assert _ligne(c, "Mode automatique")[:2] == ["", ""]


def test_la_porte_de_PLAN_prime_sur_un_quota_mal_regle(monkeypatch) -> None:
    """Un quota d'articles pose par erreur sur Solo n'ouvre pas la redaction, fermee sous Pro :
    la page ne doit pas l'annoncer."""
    monkeypatch.setenv("PLAN_CONFIG_JSON", '{"solo": {"limits": {"ai_articles_month": 3}}}')
    assert _ligne(m._comparatif_des_plans(), "Articles rédigés")[1] == ""


def test_la_porte_SOLO_des_opportunites_tient_meme_avec_un_quota_sur_free(monkeypatch) -> None:
    """Free n'a aucun quota de backlinks, ce qui MASQUAIT la porte Solo+ : sa suppression
    survivait a la mutation. Un quota pose par erreur sur Free la rend visible."""
    monkeypatch.setenv("PLAN_CONFIG_JSON",
                       '{"free": {"limits": {"backlink_searches_month": 10, "backlink_replies_month": 10}}}')
    c = m._comparatif_des_plans()
    assert _ligne(c, "Recherches d'opportunités")[0] == ""
    assert _ligne(c, "Réponses rédigées")[0] == ""


def test_le_moteur_avance_se_deduit_du_MODELE() -> None:
    c = m._comparatif_des_plans()
    modeles = [str(billing.plan_catalog()[k]["correction"]["model"]) for k in c["cles"]]
    attendus = ["Avancé" if "opus" in x.lower() else "Standard" for x in modeles]
    assert _ligne(c, "Moteur de correction") == attendus


def test_la_page_affiche_le_TIRET_et_jamais_zero_pour_l_absent() -> None:
    corps = TestClient(m.app).get("/pricing").text
    assert 'aria-label="Non inclus">—</span>' in corps
    assert "<td>0</td>" not in corps


def test_la_home_montre_la_grille_complete() -> None:
    corps = TestClient(m.app).get("/").text
    for nom in ("Free", "Solo", "Pro", "Business"):
        assert ">%s</h3>" % nom in corps, nom
    assert "Comparer les plans, fonction par fonction" in corps


EMOJI = re.compile("[\U0001F300-\U0001FAFF☀-➿⭐✅]")


def test_ni_EMOJI_ni_BADGE_ni_graisse_900() -> None:
    for f in GABARITS:
        texte = f.read_text(encoding="utf-8")
        assert not EMOJI.findall(texte), (f.name, EMOJI.findall(texte))
        assert 'class="badge' not in texte, f.name
        assert "font-weight: 900" not in texte and "font-weight:900" not in texte, f.name


def test_pas_d_HEBERGEMENT_EN_EUROPE_non_verifie() -> None:
    """L'ancienne home disait « 100 % hébergé en Europe ». `render.yaml` ne fixe aucune région
    (celle par défaut de Render est l'Oregon) et la sauvegarde est explicitement en Oregon :
    l'affirmation ne revient pas tant qu'elle n'est pas établie."""
    for f in GABARITS:
        assert "Europe" not in f.read_text(encoding="utf-8"), f.name
