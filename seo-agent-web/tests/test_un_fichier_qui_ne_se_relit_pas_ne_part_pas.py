# -*- coding: utf-8 -*-
"""Avant qu'un fichier parte chez un client, il doit au moins se relire.

Premiere moitie du point 1 de l'audit — « un build avant la PR ». Le build lui-meme demande une
decision d'infrastructure ; ces deux briques-la n'en dependent pas et valent sous n'importe
quelle option.

CE QUI EST VERIFIE, ET SEULEMENT CA : les formats pour lesquels un VRAI analyseur existe —
JSON, TOML, YAML, et l'entete YAML d'un Markdown. HTML, JSX, TypeScript, Vue, Svelte et Astro
ne sont pas couverts, et c'est une decision. Ecrire un pseudo-verificateur pour ces langages
reviendrait a decrire une FORME au lieu d'une GRAMMAIRE : il refuserait du code valide et
laisserait passer du code casse. Un verificateur qui se trompe est pire que pas de
verificateur, parce qu'on lui fait confiance. C'est le build qui juge ces fichiers-la.

LA CLE DUPLIQUEE MERITE SON PROPRE CHARGEUR. `yaml.safe_load` accepte silencieusement la meme
cle deux fois et garde la derniere. C'est exactement le defaut qui est arrive en production :
une correction ajoutait une cle deja presente, le fichier restait valide, et le site publiait
l'autre valeur. Un verificateur bati sur le chargeur par defaut aurait declare ce fichier bon —
il aurait donc rassure sans rien garantir.

LA SECONDE BRIQUE est la porte unique. Les quatre routes qui corrigent du code ouvraient chacune
leur PR avec le meme appel recopie. Tant qu'il n'y a rien a verifier avant, quatre copies ne
coutent rien ; des qu'il y a une porte a poser, elles coutent quatre fois — et la quatrieme est
celle qu'on oublie. Le dernier test de ce fichier ENUMERE les appels a l'API `pulls` et exige
qu'ils passent tous par la porte.
"""

from __future__ import annotations

import ast
import os
import sys
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")

from backend import app as m  # noqa: E402

APP_PY = Path(m.__file__).resolve()


# --- ce que l'analyseur refuse -----------------------------------------------------------------

@pytest.mark.parametrize("chemin, contenu, indice", [
    ("data/site.json", '{"title": "Accueil",}', "json"),
    ("data/site.json", '{"title": ', "json"),
    ("config.toml", 'titre = "Accueil"\ntitre = ', "toml"),
    ("_config.yml", "title: Accueil\n  bad_indent: oui\n", "yml"),
    ("content/page.md", "---\ntitle: Accueil\n  casse: oui\n---\ncorps", "md"),
])
def test_un_fichier_casse_est_REFUSE(chemin, contenu, indice) -> None:
    probleme = m._verifier_la_syntaxe(chemin, contenu)
    assert probleme, "%s casse est passe pour bon" % indice
    assert chemin in probleme, "le message ne nomme pas le fichier : %r" % probleme


@pytest.mark.parametrize("chemin, contenu", [
    ("data/site.json", '{"title": "Accueil"}'),
    ("config.toml", 'titre = "Accueil"\n'),
    ("_config.yml", "title: Accueil\nlang: fr\n"),
    ("content/page.md", "---\ntitle: Accueil\n---\n# Bonjour\n"),
    ("content/page.md", "# Un markdown sans entete\n\ndu texte."),
    ("content/page.mdx", "---\ntitle: Accueil\n---\n<Composant />"),
])
def test_un_fichier_VALIDE_passe(chemin, contenu) -> None:
    assert m._verifier_la_syntaxe(chemin, contenu) is None


def test_la_CLE_DUPLIQUEE_est_refusee_alors_que_yaml_l_accepte() -> None:
    """Le defaut qui est reellement arrive, et la raison d'un chargeur sur mesure.

    L'assertion sur `yaml.safe_load` n'est pas decorative : elle documente que le chargeur par
    defaut declare ce fichier VALIDE. Si un jour quelqu'un remplace le chargeur strict par le
    chargeur standard en pensant simplifier, ce test dira pourquoi il ne faut pas.
    """
    import yaml

    casse = "title: Accueil\ndescription: a\ntitle: Autre\n"
    assert yaml.safe_load(casse) == {"title": "Autre", "description": "a"}, (
        "le chargeur par defaut a change de comportement : relire cette garde")
    probleme = m._verifier_la_syntaxe("_config.yml", casse)
    assert probleme and "dupliqu" in probleme, probleme


def test_la_cle_dupliquee_est_aussi_refusee_dans_une_ENTETE_markdown() -> None:
    """C'est la forme sous laquelle le defaut s'est presente : un article, pas un fichier de
    configuration."""
    casse = "---\ntitle: Ancien titre\ndate: 2026-01-01\ntitle: Nouveau titre\n---\n\nLe corps."
    probleme = m._verifier_la_syntaxe("content/blog/article.md", casse)
    assert probleme and "dupliqu" in probleme, probleme


def test_ce_qui_n_a_pas_d_ANALYSEUR_n_est_pas_juge() -> None:
    """La retenue est le sujet du test, pas un effet de bord.

    Un HTML « mal ferme » est du HTML parfaitement servable, et une heuristique qui le refuserait
    bloquerait des corrections justes. On ne juge que ce qu'un analyseur sait juger.
    """
    for chemin, contenu in [
        ("index.html", "<html><p>jamais ferme"),
        ("src/Page.tsx", "export default function Page( { return <div/> }"),
        ("src/App.vue", "<template><div></template>"),
        ("layouts/base.liquid", "{% if x %}"),
    ]:
        assert m._verifier_la_syntaxe(chemin, contenu) is None, (
            "%s a ete juge alors qu'aucun analyseur ne le couvre" % chemin)


# --- la garde branchee sur le chemin reel -------------------------------------------------------

def test_le_garde_fou_du_CONTENU_refuse_aussi_la_syntaxe() -> None:
    """La fonction que les trois routes appellent deja doit porter la nouvelle verification.

    Ajouter un analyseur que personne n'appelle aurait produit un test vert et aucun effet.
    """
    assert m._github_patched_content_error("", "a.json"), "le vide passe encore"
    assert m._github_patched_content_error('{"a":', "data/a.json"), "un JSON casse passe encore"
    assert m._github_patched_content_error('{"a": 1}', "data/a.json") is None
    # Sans chemin, on ne sait pas quel langage on ecrit : on ne refuse rien sur la syntaxe.
    assert m._github_patched_content_error('{"a":') is None


def test_les_TROIS_appelants_passent_le_chemin() -> None:
    """Sans le chemin, la verification ne sait pas quel langage elle regarde et ne fait rien.

    Le test enumere les appels plutot que d'en reconnaitre la forme : un quatrieme appelant
    ajoute demain sans chemin sera nomme ici.
    """
    arbre = ast.parse(APP_PY.read_text(encoding="utf-8"))
    sans_chemin = []
    for n in ast.walk(arbre):
        if not (isinstance(n, ast.Call) and ast.unparse(n.func) == "_github_patched_content_error"):
            continue
        if len(n.args) < 2 and not any(k.arg == "path" for k in n.keywords):
            sans_chemin.append(n.lineno)
    assert not sans_chemin, (
        "ces appels ne passent pas le chemin, donc ne verifient aucune syntaxe : %s"
        % ", ".join("app.py:%d" % l for l in sans_chemin))


# --- la porte unique ----------------------------------------------------------------------------

def test_AUCUNE_pull_request_ne_part_hors_de_la_porte() -> None:
    """La garde qui rend le point 1 realisable en un seul endroit.

    Elle ENUMERE les appels a l'API `pulls` en POST plutot que de chercher un motif correct.
    Le jour ou une cinquieme route corrigera du code, elle echouera en nommant sa ligne — ce qui
    vaut mieux qu'une pull request qui contourne silencieusement tout ce qu'on posera sur la
    porte.
    """
    arbre = ast.parse(APP_PY.read_text(encoding="utf-8"))
    portees = [(n.lineno, n.end_lineno or n.lineno, n.name)
               for n in ast.walk(arbre)
               if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]

    hors_porte = []
    for n in ast.walk(arbre):
        if not (isinstance(n, ast.Call) and ast.unparse(n.func) == "_github_api_post"):
            continue
        if not n.args:
            continue
        chemin = n.args[0]
        if not (isinstance(chemin, ast.Call) and ast.unparse(chemin.func) == "_github_api_path"):
            continue
        morceaux = [ast.unparse(a) for a in chemin.args]
        if morceaux[-1:] != ["'pulls'"]:
            continue
        englobantes = [p for p in portees if p[0] <= n.lineno <= p[1]]
        nom = max(englobantes, key=lambda p: p[0])[2] if englobantes else "<module>"
        if nom == "_ouvrir_pull_request":
            continue
        hors_porte.append("app.py:%d (%s)" % (n.lineno, nom))

    assert not hors_porte, (
        "ces ouvertures de pull request contournent `_ouvrir_pull_request` :\n  "
        + "\n  ".join(hors_porte))


def test_la_porte_existe_et_elle_est_UTILISEE() -> None:
    """Le temoin : une porte que plus personne n'emprunte rendrait le test precedent vert
    pour la mauvaise raison."""
    arbre = ast.parse(APP_PY.read_text(encoding="utf-8"))
    appels = [n for n in ast.walk(arbre)
              if isinstance(n, ast.Call) and ast.unparse(n.func) == "_ouvrir_pull_request"]
    assert len(appels) == 5, "%d routes ouvrent une PR (attendu 5)" % len(appels)
