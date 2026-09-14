# -*- coding: utf-8 -*-
"""Chaque fichier doit s'entendre nommer SA page, pas celle d'un autre.

Mesure du 14/09/2026, next-app. `_deep_patch_issue_files` envoyait `url_affectee = impacted_urls[0]`
a TOUS les fichiers du lot : sur six pages corrigees, cinq prompts sur six nommaient une page que
le fichier n'edite pas.

Pourquoi c'est grave pour une famille PER-PAGE — un doublon de titre ou de description : les
appels partent en parallele, avec la meme anomalie, la meme consigne et, sur des pages jumelles,
un contenu quasi identique. L'URL est alors le SEUL element qui distingue deux appels, et elle
etait constante. Le modele rendait donc la meme valeur, ce qui recree exactement le doublon que
la famille corrige.

Ce test verifie l'appariement, pas la qualite de la reponse : ce que le fichier recoit, pas ce
que le modele en fait.
"""

from __future__ import annotations

import base64
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "x" * 20)

from backend import app as m  # noqa: E402
from backend import repo_index  # noqa: E402

PAGE = "export const metadata = {\n  description: 'Valeur',\n};\n"
CHEMINS = ["app/a/page.tsx", "app/b/page.tsx"]
URLS = ["https://x.fr/a", "https://x.fr/b"]


class _Espion:
    """Note l'URL annoncee a chaque fichier."""

    def __init__(self):
        self.vues: dict[str, str] = {}

    def __call__(self, **kw):
        self.vues[kw["file_path"]] = kw.get("url") or ""
        return {"patched_content": PAGE.replace("Valeur", "Valeur de " + kw["file_path"])}


def _jouer(monkeypatch, cle: str) -> _Espion:
    espion = _Espion()
    monkeypatch.setattr(m, "_openai_generate_file_patch", espion)
    monkeypatch.setattr(m, "_resolve_issue_targets", lambda **kw: list(CHEMINS))
    monkeypatch.setattr(m, "_github_api_get", lambda *a, **k: {
        "content": base64.b64encode(PAGE.encode()).decode(), "sha": "s"})
    monkeypatch.setattr(m, "_github_api_put", lambda *a, **k: {"content": {"sha": "n"}})
    m._deep_patch_issue_files(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="f",
        all_paths=CHEMINS, issue_key=cle, issue_label="L", impacted_urls=URLS,
        site_name="x.fr", file_state={}, max_files=6,
        index=repo_index.build_repo_index(CHEMINS))
    return espion


def test_chaque_fichier_recoit_sa_propre_url(monkeypatch):
    espion = _jouer(monkeypatch, "duplicate_meta_descriptions")
    assert espion.vues["app/a/page.tsx"].endswith("/a")
    assert espion.vues["app/b/page.tsx"].endswith("/b")


def test_les_deux_appels_ne_recoivent_plus_la_meme_url(monkeypatch):
    """Sur des pages jumelles, l'URL est le seul element qui les distingue."""
    espion = _jouer(monkeypatch, "duplicate_meta_descriptions")
    assert len(set(espion.vues.values())) == 2


def test_cela_vaut_aussi_pour_une_famille_parallele(monkeypatch):
    """L'appariement ne doit rien devoir a la serialisation des doublons."""
    espion = _jouer(monkeypatch, "missing_meta_description")
    assert len(set(espion.vues.values())) == 2


def test_sans_correspondance_on_retombe_sur_la_premiere_url(monkeypatch):
    """Un fichier que la carte des routes ne connait pas garde l'ancien comportement."""
    espion = _Espion()
    monkeypatch.setattr(m, "_openai_generate_file_patch", espion)
    monkeypatch.setattr(m, "_resolve_issue_targets", lambda **kw: ["composants/partage.tsx"])
    monkeypatch.setattr(m, "_github_api_get", lambda *a, **k: {
        "content": base64.b64encode(PAGE.encode()).decode(), "sha": "s"})
    monkeypatch.setattr(m, "_github_api_put", lambda *a, **k: {"content": {"sha": "n"}})
    m._deep_patch_issue_files(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="f",
        all_paths=CHEMINS + ["composants/partage.tsx"], issue_key="missing_meta_description",
        issue_label="L", impacted_urls=URLS, site_name="x.fr", file_state={}, max_files=6,
        index=repo_index.build_repo_index(CHEMINS))
    assert espion.vues["composants/partage.tsx"] == URLS[0]
