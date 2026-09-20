# -*- coding: utf-8 -*-
"""Le compteur d'articles : combien, pour qui, et pourquoi si peu.

DEUX DÉCISIONS DU PROPRIÉTAIRE, le 20/09/2026, et elles tiennent dans ce fichier.

*« Un compteur à part, ce n'est pas une correction »* — un article coûte sans commune mesure
avec une réécriture de snippet, et les mélanger rendrait les deux quotas illisibles : un client
qui publie deux articles ne doit pas y perdre ses corrections du mois. Le test qui compte le
plus ici est donc celui qui vérifie que les deux métriques ne se confondent PAS.

*« Pour le plan, on s'aligne aux concurrents »* — d'où Pro et Business seulement. Les plafonds
restent volontairement bas : la politique anti-spam de Google vise le contenu produit en masse
pour le classement, quelle qu'en soit la fabrication. Un plafond généreux ferait de cette
fonction un moyen de NUIRE au client, pas de le servir. Ce fichier ne fige donc pas les nombres
eux-mêmes — ils se règlent sans déploiement — mais l'ORDRE et les zéros, qui sont la décision.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-quota-art-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")

from backend import billing  # noqa: E402

METRIQUE = "ai_articles_month"


def _plafonds() -> dict[str, int]:
    cat = billing.plan_catalog()
    return {k: int(cat[k]["limits"][METRIQUE]) for k in ("free", "solo", "pro", "business")}


def test_les_QUATRE_plans_portent_le_compteur() -> None:
    """Une métrique absente d'un plan n'est pas « zéro » : selon le chemin, elle est illimitée
    ou elle fait planter la lecture. Les quatre plans la portent, explicitement."""
    plafonds = _plafonds()
    assert set(plafonds) == {"free", "solo", "pro", "business"}, plafonds


def test_la_redaction_commence_a_PRO() -> None:
    """Aligné sur l'écran Concurrents, qui fournit les sujets : un client qui ne voit pas ses
    concurrents n'a rien pour alimenter cette fonction."""
    p = _plafonds()
    assert p["free"] == 0 and p["solo"] == 0, p
    assert p["pro"] > 0 and p["business"] > p["pro"], p


def test_le_compteur_d_articles_n_est_PAS_celui_des_corrections() -> None:
    """La décision du propriétaire, mesurée là où elle pourrait se perdre : deux métriques
    distinctes, et des plafonds qui n'ont rien à voir."""
    cat = billing.plan_catalog()
    for plan in ("pro", "business"):
        limites = cat[plan]["limits"]
        assert METRIQUE in limites and "ai_corrections_month" in limites, limites
        assert limites[METRIQUE] != limites["ai_corrections_month"], (
            "%s : un article et une correction coûtent le même quota, les deux compteurs "
            "finiront par être confondus" % plan)


def test_les_plafonds_se_reglent_SANS_deploiement(monkeypatch) -> None:
    """Le nombre juste n'est pas connu d'avance : il se corrigera en production, à chaud.

    Figer 4 et 12 dans un test rendrait ce réglage impossible sans toucher au code — et c'est
    précisément pour ne pas avoir à déployer que `PLAN_CONFIG_JSON` existe.
    """
    defaut = _plafonds()["pro"]
    monkeypatch.setenv("PLAN_CONFIG_JSON", json.dumps({"pro": {"limits": {METRIQUE: 7}}}))
    assert _plafonds()["pro"] == 7, "le réglage à chaud n'atteint pas le compteur d'articles"
    monkeypatch.delenv("PLAN_CONFIG_JSON")
    assert _plafonds()["pro"] == defaut, \
        "le réglage ne se retire plus : le défaut du code n'est plus la source de vérité"
