# -*- coding: utf-8 -*-
"""Rejouer la barriere CI en local, en la LISANT plutot qu'en la recopiant.

POURQUOI CE FICHIER EXISTE, et c'est une erreur a moi. Du 19/09/2026 au 20/09, la barriere est
restee ROUGE pendant cinq poussees consecutives, et je l'ai annoncee verte a chaque fois. La
cause n'etait pas un test rate : je lancais bandit sur `seo-agent-web/backend`, la ou le
fichier CI le lance sur `seo-agent-web/backend` ET `skills/public/seo-autopilot/scripts`. Ma
version locale de la barriere etait plus etroite que la vraie, et une barriere plus etroite ne
dit pas « je ne verifie pas tout » — elle dit « tout va bien ».

Recopier les commandes dans une note aurait reproduit le defaut a la premiere modification du
fichier CI. Celui-ci les LIT : si `.github/workflows/ci.yml` change, ce lanceur change avec
lui, y compris ses cibles.

    python seo-agent-web/ops/ci_local.py

Les etapes d'installation sont sautees — l'environnement local est deja installe — et `python`
est remplace par l'interpreteur courant. Tout le reste est execute mot pour mot.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import yaml

RACINE = Path(__file__).resolve().parents[2]
CI = RACINE / ".github" / "workflows" / "ci.yml"
# Ce qui installe plutot que ce qui verifie : l'environnement local existe deja, et refaire un
# `pip install` a chaque passage le ferait diverger de celui dans lequel on travaille.
INSTALLATION = ("set up python", "install dependencies", "checkout")


def etapes() -> list[tuple[str, str]]:
    """(nom, commande) de chaque etape de verification du fichier CI, dans l'ordre."""
    config = yaml.safe_load(CI.read_text(encoding="utf-8"))
    out: list[tuple[str, str]] = []
    for job in (config.get("jobs") or {}).values():
        for etape in job.get("steps") or []:
            commande = str(etape.get("run") or "").strip()
            nom = str(etape.get("name") or "").strip()
            if not commande or nom.lower() in INSTALLATION:
                continue
            out.append((nom or commande[:40], commande))
    return out


def main() -> None:
    liste = etapes()
    if not liste:
        raise SystemExit("aucune etape lue dans %s : le lanceur ne mesure plus rien" % CI)
    print("%d etapes lues dans %s\n" % (len(liste), CI.relative_to(RACINE)))
    rates: list[str] = []
    for nom, commande in liste:
        # `python` du fichier CI -> l'interpreteur courant, pour rester dans le venv local.
        reelle = commande
        if reelle.startswith("python "):
            reelle = '"%s" %s' % (sys.executable, reelle[len("python "):])
        print("== %s" % nom)
        code = subprocess.run(reelle, cwd=str(RACINE), shell=True).returncode
        print("   -> %d\n" % code)
        if code != 0:
            rates.append(nom)
    if rates:
        print("BARRIERE ROUGE : %s" % ", ".join(rates))
        sys.exit(1)
    print("barriere verte, avec les cibles du fichier CI")


if __name__ == "__main__":
    main()
