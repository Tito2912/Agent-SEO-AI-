# -*- coding: utf-8 -*-
"""Une correction qui s'arrete au plafond doit le DIRE, et nommer la borne qui a coupe.

MESURE DU 19/09/2026 SUR UN VRAI CLIENT. oryvalo.com : neuf pages portent
`open_graph_url_not_matching_canonical`. La pull request #13 en corrige HUIT — about,
affiliate-disclosure, ai-tools, ai-video, editorial-policy, online-business, privacy-policy,
seo — et laisse `app/contact/page.tsx`, qui a pourtant exactement la forme que le correcteur
sait traiter. La coupe vient de `targets[:max_files]`, et elle ne dit rien.

Le proprietaire a vu son anomalie passer de 9 a 1 occurrence et n'avait aucun moyen de savoir
laquelle des trois explications s'appliquait : une abstention volontaire, un defaut du
correcteur, ou un simple second clic a donner. Il a fallu lire le depot pour trancher.

LE GARDE-FOU EXISTAIT DEJA, ET IL PROTEGEAIT LA MAUVAISE MOITIE DU CHEMIN. `_skipped_note`
ecrit « Non corrigés : N fichier(s) sur M » dans la pull request, et sa docstring enonce mot
pour mot le risque : « A correction that silently drops two thirds of its targets looks like a
complete one. » Mais il ne connait que les fichiers ESSAYES puis refuses. La troncature arrive
un cran plus tot, dans la selection des cibles, hors de sa portee. La lecon n'est pas « il
manquait un garde-fou », c'est **qu'un garde-fou juste peut surveiller le mauvais endroit**.

NOMMER LA BONNE BORNE, et c'est la moitie du travail. « Relance la correction » est le bon
conseil quand c'est le plafond du FORFAIT qui a coupe, et un mauvais quand c'est le QUOTA du
mois : la relance serait refusee a la porte. Un message a moitie juste envoie quelqu'un
cliquer pour rien, ce qui est pire que se taire.
"""

from __future__ import annotations

import ast
import base64
import inspect
import os
import sys
import tempfile
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-troncature-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402


class _Personne:
    def __init__(self, admin: bool = False) -> None:
        self.id = "u-1"
        self.is_admin = admin


def _forfait(monkeypatch, *, plafond: int, restant, illimite: bool = False) -> None:
    # `_compte_payeur` lit la table des projets pour savoir qui paye : sans schéma, le plafond
    # échoue avant d'avoir rien calculé.
    m.DB.create_tables()
    monkeypatch.setattr(m, "_plan_correction_cfg",
                        lambda user, compte="": {"plan": "pro", "model": "modele-x",
                                                 "max_files": plafond, "unlimited": illimite})
    monkeypatch.setattr(m.billing, "remaining_quota",
                        lambda db, *, user_id, metric: restant)


# --- le plafond : un seul calcul, deux nombres ---------------------------------------------

def test_le_forfait_coupe_quand_il_est_le_plus_bas(monkeypatch) -> None:
    _forfait(monkeypatch, plafond=12, restant=40)
    p = m._plafond_de_correction(_Personne(), slug="s")
    assert (p.applique, p.plan, p.restant) == (12, 12, 40)


def test_le_quota_coupe_quand_il_est_le_plus_bas(monkeypatch) -> None:
    """Le cas d'oryvalo : forfait Pro a 12, mais huit corrections restantes."""
    _forfait(monkeypatch, plafond=12, restant=8)
    p = m._plafond_de_correction(_Personne(), slug="s")
    assert (p.applique, p.plan, p.restant) == (8, 12, 8)


def test_les_DEUX_bornes_sont_transportees_meme_quand_une_seule_mord(monkeypatch) -> None:
    """Sans les deux, impossible de dire au client s'il doit relancer ou attendre le mois
    prochain. C'est toute la raison d'etre de ce type de retour."""
    _forfait(monkeypatch, plafond=12, restant=8)
    p = m._plafond_de_correction(_Personne(), slug="s")
    assert p.plan == 12 and p.restant == 8, "la borne qui n'a pas mordu est perdue : %r" % (p,)


def test_un_quota_illisible_ne_ferme_pas_la_porte(monkeypatch) -> None:
    """Une base injoignable ne doit pas transformer un forfait payant en refus."""
    _forfait(monkeypatch, plafond=12, restant=None)
    p = m._plafond_de_correction(_Personne(), slug="s")
    assert p.applique == 12 and p.restant is None


# --- la porte d'entree n'a pas change de comportement ---------------------------------------

@pytest.mark.parametrize("plafond, restant, attendu", [
    (12, 40, (True, 12)),    # le forfait borne
    (12, 8, (True, 8)),      # le quota borne
    (0, 40, (False, 0)),     # corrections hors forfait
    (12, 0, (False, 0)),     # quota epuise
    (12, None, (True, 12)),  # quota illisible
])
def test_le_gate_decide_exactement_comme_avant(monkeypatch, plafond, restant, attendu) -> None:
    """J'ai REECRIT `_correction_gate` pour qu'il lise `_plafond_de_correction` au lieu de
    recalculer. Une refonte qui change une decision de facturation en passant ne se verrait
    pas : ces cinq cas sont la table de verite d'avant, recopiee."""
    _forfait(monkeypatch, plafond=plafond, restant=restant)
    ouvert, _msg, cap, modele = m._correction_gate(_Personne(), slug="s")
    assert (ouvert, cap) == attendu
    assert modele == ("modele-x" if ouvert else "")


def test_un_administrateur_reste_illimite(monkeypatch) -> None:
    _forfait(monkeypatch, plafond=40, restant=0, illimite=True)
    ouvert, _msg, cap, _mod = m._correction_gate(_Personne(admin=True), slug="s")
    assert ouvert and cap == 40, "un quota épuisé a fermé la porte à un administrateur"


# --- la selection retient ce qu'elle jette --------------------------------------------------

def test_le_selecteur_note_les_candidats_ecartes() -> None:
    pages = ["app/p%d/page.tsx" % i for i in range(9)]
    ecartes: list[str] = []
    gardes = m._resolve_issue_targets(
        all_paths=pages, index=None, issue_key="open_graph_url_not_matching_canonical",
        issue_label="OG", impacted_urls=[], located=pages, max_files=8,
        allow_ai=False, ecartes=ecartes)
    assert len(gardes) == 8
    assert len(ecartes) == 1
    assert set(gardes) | set(ecartes) == set(pages), "un candidat a disparu des deux listes"


def test_la_liste_NOMMEE_par_l_appelant_se_coupe_aussi(monkeypatch) -> None:
    """Deux chemins truquent, pas un. `targets_override` court-circuite le selecteur : ne
    l'instrumenter que lui aurait laissé la moitié des corrections muettes."""
    pages = ["app/p%d/page.tsx" % i for i in range(9)]
    ecartes: list[str] = []
    monkeypatch.setattr(m, "_github_api_get",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("lecture inattendue")))
    m._deep_patch_issue_files(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="fix",
        all_paths=pages, issue_key="k", issue_label="L", impacted_urls=[], site_name="s",
        file_state={p: {"sha": "v", "content": "x"} for p in pages}, max_files=8,
        ecartes=ecartes, targets_override=pages, allow_ai_targeting=False,
        link_rewriter=lambda raw: (raw, 0), rewriter_ai_fallback=False)
    assert ecartes == ["app/p8/page.tsx"], ecartes


def test_la_boucle_TEND_son_carnet_au_selecteur(monkeypatch) -> None:
    """Le chaînon que mes premiers tests sautaient tous, et une mutation l'a montré.

    Ils passaient `targets_override`, qui COURT-CIRCUITE le sélecteur. Couper la transmission
    entre la boucle et le sélecteur ne cassait donc rien — alors que c'est le chemin normal
    d'une correction : la route ne nomme presque jamais ses fichiers. Deux morceaux corrects
    reliés par rien, et aucun test pour le voir.
    """
    recu: dict = {}

    def _faux_selecteur(**kw):
        recu.update(kw)
        return []

    monkeypatch.setattr(m, "_resolve_issue_targets", _faux_selecteur)
    ecartes: list[str] = []
    m._deep_patch_issue_files(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="fix",
        all_paths=["app/p0/page.tsx"], issue_key="k", issue_label="L",
        impacted_urls=["https://exemple.fr/p0"], site_name="s",
        file_state={"app/p0/page.tsx": {"sha": "v", "content": PAGE}}, max_files=8,
        ecartes=ecartes, allow_ai_targeting=False,
        link_rewriter=lambda raw: (raw, 0), rewriter_ai_fallback=False)
    assert "ecartes" in recu, "la boucle appelle le sélecteur sans lui tendre son carnet"
    assert recu["ecartes"] is ecartes, "le sélecteur écrit dans un autre carnet que l'appelant"


def test_sans_carnet_le_selecteur_se_comporte_comme_avant() -> None:
    """Le parametre est facultatif : une trentaine de tests et deux routes l'ignorent."""
    pages = ["app/p%d/page.tsx" % i for i in range(9)]
    assert len(m._resolve_issue_targets(
        all_paths=pages, index=None, issue_key="k", issue_label="L", impacted_urls=[],
        located=pages, max_files=8, allow_ai=False)) == 8


# --- le message -----------------------------------------------------------------------------

def test_rien_a_dire_quand_rien_n_a_ete_ecarte() -> None:
    assert m._note_de_troncature([], m._Plafond(12, 12, 40, "m", False)) == ""


def test_le_message_NOMME_le_forfait_quand_c_est_lui_qui_coupe() -> None:
    txt = m._note_de_troncature(["app/contact/page.tsx"], m._Plafond(12, 12, 40, "m", False))
    assert "app/contact/page.tsx" in txt
    assert "12" in txt and "elance" in txt, txt
    assert "ce mois-ci" not in txt, "le quota est cité alors que c'est le forfait qui a coupé"


def test_le_message_NOMME_le_quota_quand_c_est_lui_qui_coupe() -> None:
    """Le cas d'oryvalo, et le piege : conseiller « relance » ici enverrait le client se faire
    refuser a la porte par `_correction_gate`."""
    txt = m._note_de_troncature(["app/contact/page.tsx"], m._Plafond(8, 12, 8, "m", False))
    assert "ce mois-ci" in txt and "8" in txt, txt
    assert "Relance la correction pour traiter les suivants" not in txt, txt


def test_le_message_liste_les_fichiers_restants() -> None:
    restants = ["app/p%d/page.tsx" % i for i in range(20)]
    txt = m._note_de_troncature(restants, m._Plafond(8, 12, 40, "m", False))
    assert "20 fichier(s)" in txt
    assert txt.count("app/p") == 12, "la liste doit être bornée, mais le total doit être dit"


# --- la boucle complete, telle qu'oryvalo l'a vecue -----------------------------------------

LAYOUT = """export const metadata = {
  openGraph: {
    type: "website",
    url: "https://exemple.fr",
    siteName: "Exemple",
    images: [{ url: "/og.png", width: 1200, height: 630 }],
  },
};
"""

PAGE = """export const metadata = {
  title: 'P',
  alternates: { canonical: '/p' },
};
"""


def test_neuf_candidats_huit_traites_le_neuvieme_est_NOMME(monkeypatch) -> None:
    """La reproduction exacte du 19/09/2026, mesuree sur la BOUCLE et non sur une fonction.

    Des tests unitaires verts pendant qu'une correction perd un fichier, ce projet en a deja
    fait l'experience : c'est pourquoi celui-ci fait tourner `_deep_patch_issue_files` et lit
    ce qui sort, plutot que d'interroger le selecteur seul.
    """
    pages = ["app/p%d/page.tsx" % i for i in range(9)]
    monkeypatch.setattr(
        m, "_github_api_get",
        lambda chemin, **k: {"content": base64.b64encode(LAYOUT.encode()).decode()})
    monkeypatch.setattr(
        m, "_github_api_put",
        lambda chemin, **k: {"content": {"sha": "n"}, "commit": {"sha": "a", "html_url": ""}})

    ecartes: list[str] = []
    etat = {p: {"sha": "v", "content": PAGE.replace("/p'", "/p%s'" % p[5])} for p in pages}
    etat["app/layout.tsx"] = {"sha": "v", "content": LAYOUT}
    patched, _skipped, targets, _ai = m._deep_patch_issue_files(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="fix",
        all_paths=pages + ["app/layout.tsx"],
        issue_key="open_graph_url_not_matching_canonical", issue_label="OG",
        impacted_urls=[], site_name="exemple", file_state=etat, max_files=8,
        ecartes=ecartes, targets_override=pages, allow_ai_targeting=False,
        link_rewriter=lambda raw: (raw, 0), rewriter_ai_fallback=False)

    assert len(targets) == 8 and len(patched) == 8
    assert len(ecartes) == 1, "le neuvième candidat doit être nommé, pas perdu : %r" % ecartes
    assert ecartes[0] not in targets


# --- l'enumeration ---------------------------------------------------------------------------

def test_TOUTE_coupe_au_plafond_alimente_le_carnet() -> None:
    """Le garde-fou qui couvre la troisieme troncature, celle que personne n'a encore ecrite.

    Les tests ci-dessus verrouillent les deux coupes que je connais. Celui-ci relit le source
    et refuse qu'une fonction tronque sur `max_files` sans nourrir `ecartes` — c'est la seule
    protection contre la prochaine, et la lecon repetee de la journee : corriger UN site et
    rater les autres ne se voit pas.
    """
    src = Path(m.__file__).read_text(encoding="utf-8")
    arbre = ast.parse(src)
    fautes: list[str] = []
    for n in ast.walk(arbre):
        if not isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        corps = ast.unparse(n)
        if "[:max_files]" not in corps:
            continue
        if "ecartes" not in corps:
            fautes.append("%s (ligne %d)" % (n.name, n.lineno))
    assert not fautes, (
        "ces fonctions tronquent au plafond sans dire ce qu'elles écartent :\n  "
        + "\n  ".join(fautes))


def test_la_pull_request_porte_la_note() -> None:
    """La note doit etre DANS le corps de la PR, pas seulement calculee."""
    src = inspect.getsource(m.api_issue_deep_fix)
    assert "_note_de_troncature(_ecartes" in src, "la note n'est pas ajoutée au corps de la PR"
    assert src.index("_ecartes: list[str] = []") < src.index("_deep_patch_issue_files("), (
        "le carnet doit être ouvert avant la boucle qui le remplit")
