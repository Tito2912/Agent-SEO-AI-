# -*- coding: utf-8 -*-
"""Rediger une page neuve dans la FORME du site, et refuser quand elle n'y est pas.

DEUXIEME ETAPE DU CHANTIER CONTENU. L'etape 1 a decide OU poser le fichier ; celle-ci decide ce
qu'on y met. Aucune ecriture GitHub : on rend un contenu, l'appelant en fait un apercu.

LE MODELE NE RECOIT PAS UNE CONSIGNE DE FORMAT, IL RECOIT UNE PAGE. Un blog Hugo en TOML
(`+++`), un blog Astro en YAML (`---`) et une page Next qui exporte `const metadata = {...}`
n'ont aucune forme commune, et aucune liste de cles ecrite d'avance ne survivrait a leur
diversite. La seule source fiable est la page d'a cote. Meme methode que `_inserer_og_complet`,
qui recopie les champs de la mise en page au lieu d'inventer un bloc : **decrire une grammaire
bat decrire une forme**.

ET ON VERIFIE PLUTOT QUE DE CONSIGNER. La consigne dit « les memes cles » ; le controle EXIGE
les memes cles. La difference n'est pas rhetorique : une consigne est une esperance, et ce
projet a deja mesure un modele qui ignore une consigne de longueur quatre fois de suite. Un
front matter ampute casse le build du client, et cette famille ne peut pas etre rattrapee par
un reecriveur borne — il n'y a rien a remplacer sur place dans un fichier qui n'existait pas.
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

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-redaction-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402

SOEUR_YAML = (
    "---\n"
    "title: Premier article\n"
    "description: Une description de la page existante.\n"
    "date: 2026-01-15\n"
    "tags:\n"
    "  - seo\n"
    "---\n\n"
    "# Premier article\n\nDu corps.\n"
)

SOEUR_TOML = (
    "+++\n"
    'title = "Premier article"\n'
    'description = "Une description."\n'
    "+++\n\n"
    "Du corps.\n"
)

SOEUR_TSX = (
    "import type { Metadata } from 'next';\n\n"
    "export const metadata: Metadata = {\n"
    "  title: 'Premier article',\n"
    "  description: 'Une description.',\n"
    "  alternates: { canonical: '/blog/premier' },\n"
    "};\n\n"
    "export default function Page() { return <article>Du corps.</article>; }\n"
)


def _modele(reponse: str):
    return lambda **_kw: {"contenu": reponse}


# --- lire la forme d'une soeur ------------------------------------------------------------------

def test_la_forme_YAML_est_lue_avec_ses_cles() -> None:
    f = m._forme_dune_soeur(SOEUR_YAML, "content/blog/premier.md")
    assert f["bornes"] == "---"
    for cle in ("title", "description", "date", "tags"):
        assert cle in f["cles"], f["cles"]


def test_une_cle_NON_SCALAIRE_compte_aussi() -> None:
    """`tags:` seul sur sa ligne, sa valeur en dessous : le lecteur de scalaires ne le voit
    pas. L'oublier ferait accepter une page sans tags sur un site qui en met partout."""
    assert "tags" in m._forme_dune_soeur(SOEUR_YAML, "a.md")["cles"]


def test_la_forme_TOML_est_lue_aussi() -> None:
    f = m._forme_dune_soeur(SOEUR_TOML, "content/blog/premier.md")
    assert f["bornes"] == "+++"
    assert "title" in f["cles"] and "description" in f["cles"]


def test_la_forme_d_un_export_metadata_est_lue() -> None:
    f = m._forme_dune_soeur(SOEUR_TSX, "app/blog/premier/page.tsx")
    assert f["bornes"] == "metadata"
    assert "title" in f["cles"] and "alternates" in f["cles"]


def test_une_cle_IMBRIQUEE_ne_passe_pas_pour_une_cle_de_tete() -> None:
    """`alternates: { canonical: ... }` : `canonical` est au second niveau. Le confondre
    ferait exiger du modèle une clé de tête qui n'existe pas."""
    assert "canonical" not in m._forme_dune_soeur(SOEUR_TSX, "a.tsx")["cles"]


def test_un_bloc_metadata_SUR_UNE_LIGNE_reste_lisible() -> None:
    """Le cas que le retrait des accolades sert à couvrir, et que je n'avais pas testé.

    `export const metadata = { title: 'X' };` : sans retirer l'accolade ouvrante, l'ancre de
    début de ligne ne trouve plus rien et la page passerait pour illisible — on refuserait
    d'écrire sur un site parfaitement normal. Une mutation supprimant ce retrait a survécu à
    ma première version, qui n'avait que des blocs multi-lignes.

    LIMITE ASSUMÉE, et elle se voit ici : sur une seule ligne, seule la PREMIÈRE clé est lue,
    les suivantes étant en milieu de ligne. On sous-exige donc — jamais on ne sur-exige, ce
    qui refuserait du travail correct.
    """
    une_ligne = ("export const metadata = { title: 'Premier', description: 'Une desc.' };\n"
                 "export default function Page() { return <article>Corps.</article>; }\n")
    f = m._forme_dune_soeur(une_ligne, "app/blog/premier/page.tsx")
    assert f["cles"] == ["title"], f["cles"]


def test_une_page_SANS_forme_lisible_rend_le_vide() -> None:
    assert m._forme_dune_soeur("<p>juste du html</p>", "page.html")["cles"] == []


# --- ce qui sort ---------------------------------------------------------------------------------

def test_une_page_conforme_est_rendue(monkeypatch) -> None:
    attendu = ("---\ntitle: Mon sujet\ndescription: Une description neuve.\n"
               "date: 2026-09-20\ntags:\n  - seo\n---\n\n# Mon sujet\n\nDu corps neuf.\n")
    monkeypatch.setattr(m, "_correction_ai_json", _modele(attendu))
    contenu, refus = m.rediger_une_page(
        sujet="Mon sujet", chemin="content/blog/mon-sujet.md",
        soeur_chemin="content/blog/premier.md", soeur_contenu=SOEUR_YAML)
    assert refus == "" and contenu == attendu


def test_le_fichier_se_termine_par_un_saut_de_ligne(monkeypatch) -> None:
    """Un fichier sans saut final produit un diff bruyant dès la modification suivante."""
    monkeypatch.setattr(m, "_correction_ai_json",
                        _modele("+++\ntitle = \"X\"\ndescription = \"Y\"\n+++\n\nCorps."))
    contenu, refus = m.rediger_une_page(
        sujet="X", chemin="content/blog/x.md",
        soeur_chemin="content/blog/premier.md", soeur_contenu=SOEUR_TOML)
    assert refus == "" and contenu.endswith("\n")


# --- ce qu'on REFUSE de commiter -------------------------------------------------------------------

def test_une_cle_de_tete_MANQUANTE_est_refusee(monkeypatch) -> None:
    """LE contrôle qui justifie cette étape. `date` et `tags` absents : sur un site qui les
    exige, le build casse. Et contrairement aux familles de longueur, aucun réécriveur borné
    ne peut rattraper — il n'y a rien à remplacer dans un fichier qui n'existait pas."""
    monkeypatch.setattr(m, "_correction_ai_json",
                        _modele("---\ntitle: Mon sujet\ndescription: Une desc.\n---\n\nCorps.\n"))
    contenu, refus = m.rediger_une_page(
        sujet="Mon sujet", chemin="content/blog/mon-sujet.md",
        soeur_chemin="content/blog/premier.md", soeur_contenu=SOEUR_YAML)
    assert contenu == ""
    assert "date" in refus and "tags" in refus, refus


def test_un_contenu_au_FORMAT_invalide_est_refuse(monkeypatch) -> None:
    """Le même jeu de refus que les deux chemins qui commitent déjà (`_refus_de_format`).

    LE CONTENU PORTE TOUTES LES CLÉS ATTENDUES, et c'est la condition pour que ce test mesure
    ce qu'il annonce. Ma première version utilisait un littéral TSX cassé — donc illisible,
    donc zéro clé, donc refusé par le contrôle des CLÉS : la mutation qui désactivait le
    contrôle de format y a survécu. Ici le YAML a ses quatre clés et ne se parse pas, une
    valeur étant ouverte par une apostrophe jamais refermée.
    """
    monkeypatch.setattr(m, "_correction_ai_json", _modele(
        "---\ntitle: 'Mon sujet\ndescription: Une desc.\ndate: 2026-09-20\n"
        "tags:\n  - seo\n---\n\nCorps.\n"))
    contenu, refus = m.rediger_une_page(
        sujet="X", chemin="content/blog/x.md",
        soeur_chemin="content/blog/premier.md", soeur_contenu=SOEUR_YAML)
    assert contenu == "", refus
    assert "cles de tete manquantes" not in refus, (
        "refusé par le contrôle des clés, pas par celui du format : %s" % refus)


def test_un_modele_MUET_ne_produit_pas_un_fichier_vide(monkeypatch) -> None:
    monkeypatch.setattr(m, "_correction_ai_json", lambda **_kw: {})
    contenu, refus = m.rediger_une_page(
        sujet="X", chemin="content/blog/x.md",
        soeur_chemin="content/blog/premier.md", soeur_contenu=SOEUR_YAML)
    assert contenu == "" and "rien rendu" in refus


def test_une_SOEUR_illisible_arrete_tout_AVANT_d_appeler_le_modele(monkeypatch) -> None:
    """Sans forme à cloner, il n'y a rien à demander. Appeler le modèle quand même coûterait
    un article facturé pour un résultat qu'on ne saurait pas vérifier."""
    appels = {"n": 0}

    def _compte(**_kw):
        appels["n"] += 1
        return {"contenu": "peu importe"}

    monkeypatch.setattr(m, "_correction_ai_json", _compte)
    contenu, refus = m.rediger_une_page(
        sujet="X", chemin="content/blog/x.md",
        soeur_chemin="content/blog/premier.md", soeur_contenu="<p>html nu</p>")
    assert contenu == "" and "structure illisible" in refus
    assert appels["n"] == 0, "le modèle a été appelé alors qu'on ne pouvait rien vérifier"


# --- ce que le modele recoit -----------------------------------------------------------------------

def test_le_modele_recoit_la_PAGE_SOEUR_et_pas_une_description(monkeypatch) -> None:
    """Le cœur de la méthode : on montre, on ne décrit pas."""
    vu: dict = {}

    def _capture(**kw):
        vu.update(kw)
        return {"contenu": SOEUR_YAML}

    monkeypatch.setattr(m, "_correction_ai_json", _capture)
    m.rediger_une_page(sujet="Mon sujet", chemin="content/blog/mon-sujet.md",
                       soeur_chemin="content/blog/premier.md", soeur_contenu=SOEUR_YAML,
                       site_name="exemple.fr")
    demande = vu["user_msg"]
    assert "Premier article" in demande, "la page sœur n'est pas montrée au modèle"
    assert "Mon sujet" in demande and "exemple.fr" in demande
    # LA LIGNE DE CONTRAINTE, pas les clés perdues dans la page sœur montrée juste au-dessus :
    # elles y figurent forcément, donc les chercher dans tout le message ne prouvait rien, et
    # une mutation qui supprimait la liste explicite y a survécu.
    ligne = [l for l in demande.splitlines() if "toutes presentes" in l]
    assert len(ligne) == 1, demande
    for cle in ("title", "description", "date", "tags"):
        assert cle in ligne[0], ligne[0]


@pytest.mark.parametrize("interdit, motif", [
    ("lien invente", "aucun lien invente"),
    ("langue", "meme langue"),
])
def test_les_consignes_qui_evitent_un_degat_sont_presentes(monkeypatch, interdit, motif) -> None:
    """Deux consignes qu'aucun contrôle automatique ne remplace : un lien vers une page qui
    n'existe pas crée un 404 interne, et une page en anglais sur un site français est pire
    qu'une page absente. Elles ne sont pas vérifiables ici — elles doivent au moins être dites.
    """
    vu: dict = {}
    monkeypatch.setattr(m, "_correction_ai_json",
                        lambda **kw: vu.update(kw) or {"contenu": SOEUR_YAML})
    m.rediger_une_page(sujet="X", chemin="content/blog/x.md",
                       soeur_chemin="content/blog/premier.md", soeur_contenu=SOEUR_YAML)
    assert motif in vu["user_msg"], interdit
