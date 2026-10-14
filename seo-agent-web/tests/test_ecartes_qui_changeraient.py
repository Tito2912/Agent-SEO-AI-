# -*- coding: utf-8 -*-
"""Un fichier ecarte par le plafond n'est « non traite » que s'il aurait change.

PR #2 d'easyshopbuilder, 14/10/2026 : les 3 pages signalees pour « lien vers redirection »
corrigees, puis « Non traites : 4 fichier(s) — relance la correction ». Les quatre avaient ete
trouves parce qu'ils MENTIONNENT /en/ ; aucun ne portait de lien a reecrire. Relancer n'aurait
rien change, et « ces pages resteront signalees » etait faux.

Pour une famille de liens, ou le reecriveur deterministe EST le correctif, le correcteur relit
chaque fichier ecarte (un appel GitHub, aucun appel au modele) et ne garde que ceux que le
reecriveur aurait modifies.
"""
from __future__ import annotations

import base64

from backend import app as m

PAIRES = [{"from": "https://site.fr/x/", "to": "https://site.fr/x"}]
AVEC_LIEN = '<a href="/x/">Accueil</a>\n'
SANS_LIEN = "On parle de /x/ dans le texte, sans lien.\n"


def _reecrire(raw: str, _chemin: str) -> tuple[str, int]:
    return m._rewrite_redirect_links(raw, PAIRES)


def test_seuls_restent_les_fichiers_que_la_reecriture_aurait_changes() -> None:
    contenus = {"a.mdx": AVEC_LIEN, "b.mdx": SANS_LIEN, "c.mdx": None, "d.mdx": "boum"}

    def reecrire(raw, chemin):
        if raw == "boum":
            raise ValueError("refus du reecriveur")
        return _reecrire(raw, chemin)

    assert m._ecartes_qui_changeraient(list(contenus), contenus.get, reecrire) == ["a.mdx", "c.mdx"]
    # a : aurait change ; b : rien a reecrire ; c : illisible, on ne sait pas, il reste annonce ;
    # d : le reecriveur refuse, une relance le refuserait aussi.


def test_au_dela_de_la_limite_rien_n_est_verifie_donc_tout_reste_annonce() -> None:
    chemins = [f"f{i}.mdx" for i in range(35)]
    lus: list[str] = []

    def lire(chemin):
        lus.append(chemin)
        return SANS_LIEN

    assert m._ecartes_qui_changeraient(chemins, lire, _reecrire, limite=30) == chemins[30:]
    assert len(lus) == 30


def _corriger(monkeypatch, issue_key: str, contenus: dict[str, str], *, repli: bool = False,
              deja: list[str] | None = None) -> list[str]:
    monkeypatch.setattr(m, "_github_api_get", lambda chemin, **k: {
        "content": base64.b64encode(b"").decode()})
    monkeypatch.setattr(m, "_github_api_put",
                        lambda chemin, **k: {"content": {"sha": "n"}, "commit": {"sha": "a", "html_url": ""}})
    monkeypatch.setattr(m, "_correction_ai_json", lambda **kw: {})
    ecartes: list[str] = list(deja or [])
    # Les fichiers de la famille precedente sont lisibles et sans lien : refiltres, ils
    # disparaitraient du carnet.
    etat = {p: {"sha": "v", "content": SANS_LIEN} for p in deja or []}
    etat.update({p: {"sha": "v", "content": c} for p, c in contenus.items()})
    m._deep_patch_issue_files(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="fix",
        all_paths=list(etat), issue_key=issue_key, issue_label="L", impacted_urls=[],
        site_name="site.fr", file_state=etat,
        max_files=1, ecartes=ecartes, targets_override=list(contenus), allow_ai_targeting=False,
        link_rewriter=lambda raw: m._rewrite_redirect_links(raw, PAIRES), rewriter_ai_fallback=repli)
    return ecartes


def test_la_boucle_n_annonce_que_le_fichier_qui_porte_un_lien(monkeypatch) -> None:
    contenus = {"p0.mdx": AVEC_LIEN, "p1.mdx": AVEC_LIEN, "p2.mdx": SANS_LIEN, "p3.mdx": SANS_LIEN}

    assert _corriger(monkeypatch, "page_has_links_to_redirect_not_indexable", contenus) == ["p1.mdx"]


def test_le_carnet_d_une_famille_precedente_n_est_pas_retouche(monkeypatch) -> None:
    # « Tout corriger » passe le meme carnet d'une famille a l'autre.
    contenus = {"p0.mdx": AVEC_LIEN, "p1.mdx": SANS_LIEN}

    assert _corriger(monkeypatch, "page_has_links_to_redirect_indexable", contenus,
                     deja=["titre.mdx"]) == ["titre.mdx"]


def test_avec_un_repli_sur_le_modele_le_fichier_reste_annonce(monkeypatch) -> None:
    # Le modele pourrait ecrire ce que le reecriveur ne trouve pas : on ne sait pas.
    contenus = {"p0.mdx": AVEC_LIEN, "p1.mdx": SANS_LIEN}

    assert _corriger(monkeypatch, "page_has_links_to_redirect_indexable", contenus, repli=True) == ["p1.mdx"]


def test_hors_des_familles_de_liens_le_carnet_reste_tel_quel(monkeypatch) -> None:
    # Ailleurs une branche propre ecrit le fichier : le reecriveur ne dit rien de lui.
    contenus = {"p0.mdx": AVEC_LIEN, "p1.mdx": SANS_LIEN, "p2.mdx": SANS_LIEN}

    assert _corriger(monkeypatch, "title_too_long_indexable", contenus) == ["p1.mdx", "p2.mdx"]
