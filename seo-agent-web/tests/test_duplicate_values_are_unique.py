# -*- coding: utf-8 -*-
"""Corriger des doublons en PARALLELE ne peut pas produire des valeurs differentes.

Mesure du 14/09/2026, next-app, famille `duplicate_meta_descriptions` jouee seule. Sur `main`,
`duplicate-a` et `duplicate-b` portent la meme description (104 caracteres). Le correcteur leur a
ecrit, aux deux, EXACTEMENT le meme nouveau texte de 83 caracteres : le doublon restait entier et
les deux valeurs tombaient sous le plancher. Verifie octet par octet sur la branche.

La cause n'est pas le modele mais l'architecture : `_deep_patch_issue_files` lance cinq fichiers
en parallele et aucun appel ne peut savoir ce qu'un autre ecrit. Deux pages qui portent la meme
valeur recoivent la meme consigne et rendent la meme reponse — c'est le comportement attendu d'un
modele, pas un accident, et aucun changement de prompt seul ne peut le garantir.

Ces familles-la sont donc traitees en FILE, chacune recevant les valeurs deja posees. Ce que ce
test verifie est exactement cela : la sequence et la transmission. Ce qu'il ne peut pas verifier,
et lui seul reste hors de portee : que le modele respecte l'interdiction.
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

from backend import app as m  # noqa: E402

PAGE = "export const metadata = {\n  description: 'La meme valeur partout',\n};\n"


def _v(graine: str) -> str:
    """Une valeur de test AU-DESSUS du plancher de longueur.

    Ces tests parlent de SEQUENCE, pas de longueur. Avec des valeurs courtes ils declenchaient la
    relance du plancher et comptaient des appels sans rapport avec ce qu'ils verifient : une
    fixture irrealiste qui fait echouer un test etranger au sien.
    """
    return graine + " " + "complement de texte pour atteindre le plancher. " * 3


class _Faux:
    """Un modele qui repond a la suite, en notant la consigne recue a chaque appel."""

    def __init__(self, reponses: list[str]):
        self.reponses = reponses
        self.consignes: list[str] = []

    def __call__(self, **kw):
        self.consignes.append(kw.get("occurrences_hint") or "")
        valeur = self.reponses[len(self.consignes) - 1]
        return {"patched_content": PAGE.replace("La meme valeur partout", valeur)}


def _jouer(monkeypatch, reponses: list[str], cle: str = "duplicate_meta_descriptions",
           fichiers: int | None = None) -> _Faux:
    """`fichiers` se distingue du nombre de reponses des qu'une RELANCE entre en jeu : un meme
    fichier consomme alors deux reponses."""
    combien = len(reponses) if fichiers is None else fichiers
    faux = _Faux(reponses)
    monkeypatch.setattr(m, "_openai_generate_file_patch", faux)
    monkeypatch.setattr(m, "_resolve_issue_targets",
                        lambda **kw: ["a/page.tsx", "b/page.tsx", "c/page.tsx"][:combien])
    monkeypatch.setattr(m, "_github_api_get", lambda *a, **k: {
        "content": __import__("base64").b64encode(PAGE.encode()).decode(), "sha": "s"})
    monkeypatch.setattr(m, "_github_api_put", lambda *a, **k: {"content": {"sha": "n"}})
    m._deep_patch_issue_files(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="f",
        all_paths=["a/page.tsx", "b/page.tsx", "c/page.tsx"], issue_key=cle,
        issue_label="Doublons", impacted_urls=["https://x.fr/a"], site_name="x.fr",
        file_state={}, max_files=6)
    return faux


def test_le_deuxieme_appel_connait_la_valeur_du_premier(monkeypatch):
    faux = _jouer(monkeypatch, [_v("Premiere valeur"), _v("Deuxieme valeur")])
    assert len(faux.consignes) == 2
    assert _v("Premiere valeur") in faux.consignes[1]


def test_le_premier_appel_n_interdit_rien(monkeypatch):
    faux = _jouer(monkeypatch, [_v("Premiere valeur"), _v("Deuxieme valeur")])
    assert "DEJA ecrites" not in faux.consignes[0]


def test_les_interdictions_s_accumulent(monkeypatch):
    faux = _jouer(monkeypatch, [_v("Une"), _v("Deux"), _v("Trois")])
    assert _v("Une") in faux.consignes[2] and _v("Deux") in faux.consignes[2]


def test_une_valeur_repetee_n_est_listee_qu_une_fois(monkeypatch):
    """Si le modele redonne la meme valeur, l'interdiction ne se duplique pas."""
    faux = _jouer(monkeypatch, [_v("Pareil"), _v("Pareil"), _v("Enfin autre chose")])
    assert faux.consignes[2].count(_v("Pareil")) == 1


def test_une_famille_ordinaire_reste_en_parallele(monkeypatch):
    """La serialisation a un cout : elle ne doit toucher que les familles de doublons."""
    faux = _jouer(monkeypatch, [_v("Une"), _v("Deux")], cle="missing_meta_description")
    assert all("DEJA ecrites" not in c for c in faux.consignes)


def test_une_valeur_trop_courte_declenche_UNE_relance(monkeypatch):
    """La relance dit la longueur manquante, et une seule fois."""
    court = "Trop court."
    long_ok = "x" * (m._LENGTH_FLOORS["description"] + 10)
    faux = _jouer(monkeypatch, [court, long_ok], fichiers=1)
    assert len(faux.consignes) == 2, "un seul fichier, donc un essai puis UNE relance"
    assert "TROP COURT" in faux.consignes[1]
    assert str(m._LENGTH_FLOORS["description"]) in faux.consignes[1]


def test_on_ne_relance_pas_indefiniment(monkeypatch):
    """Si la relance echoue aussi, on garde le premier essai et on passe au suivant."""
    court = "Trop court."
    faux = _jouer(monkeypatch, [court, court, _v("Valeur du fichier suivant")], fichiers=2)
    assert len(faux.consignes) == 3, "essai + relance sur le premier, puis le second fichier"


def test_une_valeur_conforme_ne_declenche_aucune_relance(monkeypatch):
    long_ok = "y" * (m._LENGTH_FLOORS["description"] + 10)
    autre = "z" * (m._LENGTH_FLOORS["description"] + 10)
    faux = _jouer(monkeypatch, [long_ok, autre])
    assert all("TROP COURT" not in c for c in faux.consignes)


def test_une_valeur_deja_posee_n_est_JAMAIS_committee(monkeypatch):
    """La garantie dure, celle qui ne demande rien au modele.

    Mesure du 14/09/2026, next-app : prevenu que la valeur etait prise, le modele a recopie sur
    les SIX pages la description Open Graph generique du site. Deux doublons etaient entres, six
    sont sortis. Une consigne se discute, une verification non : le fichier n'est alors pas
    patche et la page garde son texte. Au pire le doublon subsiste — jamais il ne s'aggrave.
    """
    partagee = _v("La meme pour tout le monde")
    faux = _jouer(monkeypatch, [partagee, partagee, partagee], fichiers=2)
    # Le premier fichier passe ; le second relance puis renonce.
    assert len(faux.consignes) == 3
    assert "DEJA posee" in faux.consignes[2]


def test_le_refus_n_empeche_pas_le_fichier_suivant(monkeypatch):
    """Un fichier abandonne ne doit pas interrompre le lot."""
    partagee = _v("Partagee")
    autre = _v("Bien distincte")
    faux = _jouer(monkeypatch, [partagee, partagee, partagee, autre], fichiers=3)
    assert len(faux.consignes) == 4, "1 pour le premier, 2 pour le refuse, 1 pour le dernier"
