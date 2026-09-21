# -*- coding: utf-8 -*-
"""La page d'accueil promet ce que le produit fait, et rien qu'on ne puisse prouver.

ÉTAT TROUVÉ LE 21/09/2026. La home datait du 03/09 et vendait un autre produit : « crawl,
audit, dashboard, recommandations priorisées », « arrêtez de jongler entre dix outils ». C'est
le positionnement d'un tableau de bord SEO de plus, face à des outils installés depuis dix ans.
Le correcteur — la seule chose que ce produit fait et que les autres ne font pas — n'était même
pas une des six cartes, et la troisième étape disait *« Suivez les recommandations »*,
c'est-à-dire : à vous de faire le travail.

Sa **meta description disait « SEO Audit vous aide à… »** — l'ancien nom du produit, écrit en
dur, sur la chaîne la plus indexée du site. Le piège qui l'a produite est toujours là : en
local, `_app_name()` retombe sur « SEO Audit », donc quiconque recopie ce qu'il voit s'afficher
grave l'ancien nom dans le gabarit.

CE QUE CE FICHIER GARDE, et il ne garde que ça : **aucune affirmation invérifiable**, et le
chiffre des technologies prises en charge **dérivé du banc** plutôt qu'écrit à la main. Le
reste — le ton, l'ordre des cartes — appartient au propriétaire et n'a rien à faire dans un
test.
"""

from __future__ import annotations

import ast
import os
import re
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

os.environ.setdefault("SEO_AGENT_DATA_DIR", tempfile.mkdtemp(prefix="seo-agent-home-"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", tempfile.mkdtemp(prefix="seo-agent-home-runs-"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

HOME = WEB_ROOT / "templates" / "home_public.html"
EMIT = WEB_ROOT / "ops" / "gauntlet" / "emit_stacks.py"


def _gabarit() -> str:
    return HOME.read_text(encoding="utf-8")


def _stacks_du_banc() -> list[str]:
    """Les technologies que le banc prouve réellement, lues dans `SITES`.

    Par l'arbre syntaxique et non par un import : `emit_stacks` est un outil d'atelier, et un
    test ne doit pas dépendre de ce que son import déclenche.
    """
    arbre = ast.parse(EMIT.read_text(encoding="utf-8"))
    for noeud in ast.walk(arbre):
        cible = None
        if isinstance(noeud, ast.AnnAssign) and isinstance(noeud.target, ast.Name):
            cible, valeur = noeud.target.id, noeud.value
        elif isinstance(noeud, ast.Assign) and len(noeud.targets) == 1 and isinstance(noeud.targets[0], ast.Name):
            cible, valeur = noeud.targets[0].id, noeud.value
        if cible == "SITES" and isinstance(valeur, ast.Dict):
            return [k.value for k in valeur.keys if isinstance(k, ast.Constant)]
    raise AssertionError("SITES est introuvable : ce test ne mesure plus rien")


# ── le nom du produit ────────────────────────────────────────────────────────────────────────

def test_le_nom_du_produit_n_est_JAMAIS_ecrit_en_dur() -> None:
    """« SEO Audit » est le nom d'avant. Il a vécu des mois dans la meta description de la
    home, donc dans les résultats de recherche. Le gabarit doit passer par `{{ app_name }}`,
    qui vaut « Noyaru » en production."""
    fautifs = [n + 1 for n, ligne in enumerate(_gabarit().splitlines())
               if "SEO Audit" in ligne]
    assert not fautifs, (
        "l'ancien nom du produit est écrit en dur lignes %s — utiliser {{ app_name }}" % fautifs)


# ── ce que la home doit dire du produit ──────────────────────────────────────────────────────

def test_la_home_parle_du_CORRECTEUR() -> None:
    """La seule chose que ce produit fait et que les autres ne font pas. Elle a été absente
    des six cartes pendant tout le mois de septembre."""
    texte = _gabarit().lower()
    assert "pull request" in texte, "la home ne dit pas que la correction arrive en pull request"


def test_la_home_parle_de_la_VERIFICATION_par_la_CI_du_client() -> None:
    """L'argument de confiance que personne d'autre ne peut faire : la correction n'est
    proposée qu'une fois le build du client passé."""
    texte = _gabarit().lower()
    # « VOTRE » n'est pas un détail de style : une chaîne d'intégration, tout le monde en a
    # une. Ce qui distingue ce produit, c'est que le verdict vient de CELLE DU CLIENT. Ma
    # première version acceptait le mot « build » n'importe où sur la page, et la mutation qui
    # retirait la promesse y a SURVÉCU parce qu'un autre paragraphe contenait le mot.
    assert re.search(r"votre (propre )?(build|cha[îi]ne d.int[ée]gration|ci)\b", texte), (
        "la home ne dit plus que c'est la CI DU CLIENT qui valide avant proposition")


# ── les affirmations chiffrées doivent être dérivées, pas écrites ────────────────────────────

def test_le_nombre_de_TECHNOLOGIES_annonce_est_celui_que_le_banc_prouve() -> None:
    """Le chiffre se périme tout seul : le jour où le banc gagne ou perd une stack, la home
    ment. Ici le test le dit — et il nomme le bon chiffre."""
    attendu = len(_stacks_du_banc())
    assert attendu >= 5, "la liste du banc a fondu : %d stacks" % attendu
    annonces = [int(n) for n in re.findall(r">(\d+)\s+technologies<", _gabarit())]
    assert annonces, "la home n'annonce plus aucun nombre de technologies"
    faux = [n for n in annonces if n != attendu]
    assert not faux, (
        "la home annonce %s technologies, le banc en prouve %d" % (faux, attendu))


def test_chaque_technologie_NOMMEE_est_couverte_par_le_banc() -> None:
    """Nommer une stack qu'on ne teste pas est la pire des promesses : elle est vérifiable
    par le premier client qui l'utilise."""
    prouvees = set(_stacks_du_banc())
    texte = _gabarit().lower()

    # LES NOMS VIENNENT DU CODE, PAS D'UNE LISTE ÉCRITE ICI. Une liste à la main ne peut pas
    # attraper un nom auquel on n'a pas pensé : la mutation qui annonçait « WordPress » — une
    # stack que `repo_index` sait détecter mais que le banc ne prouve pas — a SURVÉCU à ma
    # première version, parce que ce nom n'y figurait tout simplement pas.
    connues = set(re.findall(r'^STACK_[A-Z_]+ = "([a-z0-9-]+)"',
                             (WEB_ROOT / "backend" / "repo_index.py").read_text(encoding="utf-8"),
                             re.M))
    non_prouvees = {s for s in connues if s not in prouvees} - {"unknown"}
    assert non_prouvees, "toutes les stacks connues sont prouvées : ce test ne mesure plus rien"

    citees_a_tort = [s for s in sorted(non_prouvees)
                     if re.search(r"\b%s\b" % re.escape(s.replace("-", " ")), texte)]
    assert not citees_a_tort, (
        "la home annonce des technologies que le banc ne prouve pas : %s" % citees_a_tort)

    citees = [s for s in sorted(prouvees)
              if re.search(r"\b%s\b" % re.escape(s.split("-")[0]), texte)]
    assert len(citees) >= 5, "la home ne nomme plus les technologies : %s" % citees


def test_la_home_ne_promet_AUCUN_chiffre_de_resultat() -> None:
    """Pas de « +40 % de trafic », pas de « 2 000 clients ». Aucune donnée ne les étaye, et
    une promesse invérifiable sur une page indexée est un risque, pas un argument."""
    texte = _gabarit()
    promesses = re.findall(r"\+\s?\d+\s?%|\b\d[\d\s]{2,}\s*(?:clients|sites|utilisateurs)\b",
                           texte, re.I)
    assert not promesses, "la home promet des résultats invérifiables : %s" % promesses


# ── elle doit continuer de se rendre ─────────────────────────────────────────────────────────

def test_la_home_se_rend_sans_jeton_oublie() -> None:
    from fastapi.testclient import TestClient

    from backend.app import app

    reponse = TestClient(app).get("/")
    assert reponse.status_code == 200
    restants = sorted(set(re.findall(r"\{\{[a-z_]+\}\}", reponse.text)))
    assert not restants, "jetons non résolus sur la home : %s" % restants
