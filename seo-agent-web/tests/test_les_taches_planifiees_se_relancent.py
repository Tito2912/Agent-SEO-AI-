# -*- coding: utf-8 -*-
"""Un redeploiement ne rend plus une tache planifiee rouge.

Le 28/09/2026 a 16:27, Render redeployait a la minute ou « Verifier les backlinks gagnes »
l'appelait : 502, job rouge, e-mail d'alerte au proprietaire — le code etait sain. Chaque appel
planifie porte desormais une relance BORNEE. Ce test garde les trois proprietes sur tout
`curl` des workflows planifies, y compris ceux qui viendront :

* `--fail-with-body` : un 401 ou un 500 compte comme un echec (sans lui, curl sort avec 0) ;
* `--retry` : une bascule passagere est rejouee ;
* `--retry-max-time` court : un appel LONG qui depasse son delai n'est pas rejoue — il ne
  tiendrait pas dans le `timeout-minutes` du job.

La mecanique elle-meme (le vrai curl contre un serveur qui joue 502, 502, 200 ; une panne
durable ; un 401 ; un appel trop long) a ete prouvee a la main le 29/09/2026.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

WORKFLOWS = Path(__file__).resolve().parents[2] / ".github" / "workflows"
PLANIFIES = sorted(f for f in WORKFLOWS.glob("*.yml")
                   if "schedule" in (yaml.safe_load(f.read_text(encoding="utf-8")).get(True) or {}))


def _appels_curl(texte: str) -> list[str]:
    """Chaque commande curl, lignes de continuation (`\\`) recollees, commentaires retires."""
    lignes = [l for l in texte.splitlines() if not l.strip().startswith("#")]
    appels, courant = [], None
    for l in lignes:
        if courant is None and re.search(r"(^|\s)curl\s", l):
            courant = l
        elif courant is not None:
            courant += " " + l.strip()
        if courant is not None and not l.rstrip().endswith("\\"):
            appels.append(courant)
            courant = None
    return appels


def test_les_workflows_planifies_sont_trouves() -> None:
    """Le temoin : sans lui, un glob vide rendrait les tests suivants vrais pour rien."""
    noms = {f.name for f in PLANIFIES}
    assert {"check-backlinks.yml", "taches-quotidiennes.yml"} <= noms, noms


@pytest.mark.parametrize("workflow", PLANIFIES, ids=lambda f: f.name)
def test_chaque_appel_planifie_se_relance_sans_rejouer_un_appel_long(workflow) -> None:
    appels = _appels_curl(workflow.read_text(encoding="utf-8"))
    assert appels, "aucun curl trouve dans %s" % workflow.name
    for appel in appels:
        assert "--fail-with-body" in appel, appel
        assert re.search(r"--retry \d+", appel), appel
        fenetre = re.search(r"--retry-max-time (\d+)", appel)
        duree = re.search(r"--max-time (\d+)", appel)
        assert fenetre and duree, appel
        assert int(fenetre.group(1)) < int(duree.group(1)), (
            "la fenetre de relance doit etre plus courte que l'appel : sinon un appel qui "
            "depasse son delai serait rejoue — " + appel)


@pytest.mark.parametrize("workflow", PLANIFIES, ids=lambda f: f.name)
def test_le_job_laisse_le_temps_des_relances(workflow) -> None:
    config = yaml.safe_load(workflow.read_text(encoding="utf-8"))
    appels = _appels_curl(workflow.read_text(encoding="utf-8"))
    # Une fonction shell appelee N fois compte N fois.
    n = max(1, len(re.findall(r"^\s*appeler\s+(GET|POST)", workflow.read_text(encoding="utf-8"), re.M)))
    pire = max(int(re.search(r"--max-time (\d+)", a).group(1))
               + int(re.search(r"--retry-max-time (\d+)", a).group(1)) for a in appels) * n
    for job in config["jobs"].values():
        assert job["timeout-minutes"] * 60 >= pire, (workflow.name, job["timeout-minutes"], pire)
