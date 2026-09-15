# -*- coding: utf-8 -*-
"""Toute anomalie MONTREE au client doit savoir dire ce qu'elle est et quoi en faire.

Mesure du 15/09/2026. Le produit montre 157 familles ; 53 ont un correcteur. Les 104 autres ne
sont pas un manquement en soi — `_github_issue_auto_fixable` est en opt-in, donc aucun bouton
« Corriger » n'est propose sur elles, et beaucoup ne sont tout simplement pas reparables par un
patch (un certificat expire, un score Core Web Vitals, un rapport Search Console).

Le manquement etait ailleurs : SOIXANTE-TROIS d'entre elles tombaient dans le repli generique
— « Issue detectee : <libelle>. Elle peut impacter SEO/UX selon le contexte. » — qui n'apprend
rien a personne. Onze etaient classees `error`, la gravite la plus haute affichee. Le client
voyait une alarme rouge suivie d'une phrase creuse.

Ne pas corriger une anomalie est defendable, et parfois la seule position honnete. Ne rien
savoir en dire ne l'est pas.

Ce test tient la promesse dans l'autre sens : ajouter une cle au catalogue sans ecrire ce qu'on
en sait fait echouer la CI.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "x" * 20)

from backend import audit_dashboard as dash  # noqa: E402
from backend import fix_suggestions as fs  # noqa: E402

REPLI = "Issue détectée:"


def _montrees() -> list[str]:
    return sorted(k for k in dash.ISSUE_CATALOG
                  if k not in dash.NON_ISSUE_KEYS and not dash.is_delta_issue_key(k))


def _conseil(cle: str) -> dict:
    meta = dash.ISSUE_CATALOG[cle]
    return fs.suggest_issue_fix(
        issue_key=cle, label=meta.label, category=meta.category, severity=meta.severity,
        count=1, report={"issues": {}}, site_name="exemple.fr", base_url="https://exemple.fr")


def test_aucune_famille_montree_ne_tombe_dans_le_repli_generique():
    muettes = [k for k in _montrees() if str(_conseil(k).get("why", "")).startswith(REPLI)]
    assert muettes == [], (
        "ces familles sont affichees au client sans rien lui apprendre : " + ", ".join(muettes))


def test_chaque_famille_dit_QUOI_FAIRE_et_COMMENT_VERIFIER():
    sans = [k for k in _montrees()
            if not _conseil(k).get("fix") or not _conseil(k).get("verify")]
    assert sans == [], "familles sans marche a suivre ou sans controle : " + ", ".join(sans)


def test_les_familles_les_plus_graves_sont_couvertes_en_premier():
    """Une `error` muette est le pire cas : une alarme rouge suivie d'une phrase creuse."""
    graves = [k for k in _montrees() if dash.ISSUE_CATALOG[k].severity == "error"]
    assert graves, "le catalogue doit contenir des familles graves"
    muettes = [k for k in graves if str(_conseil(k).get("why", "")).startswith(REPLI)]
    assert muettes == []


def test_le_repli_generique_existe_toujours_pour_une_cle_inconnue():
    """On ne supprime pas le filet : une cle hors catalogue doit encore produire quelque chose."""
    sortie = fs.suggest_issue_fix(
        issue_key="famille_qui_n_existe_pas", label="Inconnue", category="Other",
        severity="notice", count=1, report={"issues": {}}, site_name="exemple.fr",
        base_url="https://exemple.fr")
    assert sortie["why"].startswith(REPLI)
    assert sortie["fix"] and sortie["verify"]
