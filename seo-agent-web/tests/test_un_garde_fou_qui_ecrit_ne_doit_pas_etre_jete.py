# -*- coding: utf-8 -*-
"""Un garde-fou qui MODIFIE le fichier doit faire partir la correction, et n'est pas le modele.

Deux defauts de plomberie de la meme boucle, trouves le 19/09/2026 en corrigeant autre chose.
Le geste qui les avait reveles a depuis ete retire — il etait faux pour une raison sans rapport
— mais les deux defauts, eux, sont reels et concernent TOUT garde-fou pose apres le reecriveur.

C'est pourquoi ces tests ne passent par AUCUNE famille : ils remplacent un garde-fou quelconque
par un qui ecrit. La propriete testee appartient a la boucle, pas a une couverture.

DEFAUT 1 — le drapeau du reecriveur jetait le travail des garde-fous.

    if patch.get("no_change") or new_content.strip() == raw.strip():
        continue

`no_change` dit ce que le REECRIVEUR a fait. Il ne dit rien de ce que le fichier est devenu
APRES les garde-fous, qui ecrivent eux aussi. La correction etait calculee, puis oubliee une
instruction plus loin, sans une ligne dans les journaux. La seule question valable est « le
contenu a-t-il change ? », et la comparaison y repondait deja.

DEFAUT 2 — tout ce qui n'etait pas marque deterministe etait mis au compte du modele.

    if not patch.get("deterministic"):
        ai_files.append(path)

`deterministic` ne repond qu'a « le reecriveur a-t-il trouve quelque chose ? ». Son ABSENCE
etait lue comme « c'est donc le modele qui a ecrit » — vrai tant que seuls ces deux-la
ecrivaient, faux des qu'un garde-fou pose la correction apres coup. Consequence mesuree : huit
fichiers factures au quota IA sans un seul appel au modele, et une pull request demandant de
relire une prose que personne n'avait ecrite.
"""

from __future__ import annotations

import base64
import os
import sys
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")

from backend import app as m  # noqa: E402

CHEMIN = "app/about/page.tsx"
PAGE = """export const metadata = {
  title: 'About',
};
"""
MARQUE = "// touche par un garde-fou\n"


def _garde_qui_ecrit(monkeypatch) -> None:
    """Remplace UN garde-fou par un qui modifie toujours le fichier.

    Lequel importe peu — c'est la boucle qu'on teste. Passer par une famille reelle ferait
    dependre ces tests d'une couverture qui peut changer, alors que la propriete, elle, ne
    bouge pas : ce qui a ete ecrit doit partir, et n'est pas l'oeuvre du modele.
    """
    monkeypatch.setattr(m, "_enforce_length_ceilings",
                        lambda new, old: (MARQUE + new, ["garde-fou de test"]))


def _lancer(monkeypatch, *, rewriter=None, patch_du_modele=None, contenu=PAGE):
    """Fait tourner la boucle sur UN fichier, sans reseau.

    `file_state` pre-rempli court-circuite la lecture GitHub ; seule l'ECRITURE est interceptee,
    pour mesurer ce que la boucle a REELLEMENT decide de committer.

    Rend (patched, ai_files, commits). Attention : `_deep_patch_issue_files` rend QUATRE valeurs
    — (patched, skipped, targets, ai_files) — et confondre `targets` avec `ai_files` fait
    echouer le test sur un code correct. C'est arrive.
    """
    commits: dict[str, str] = {}

    def _faux_put(chemin, *, token, json_body):
        commits[chemin] = base64.b64decode(json_body["content"]).decode("utf-8")
        return {"content": {"sha": "neuf"}, "commit": {"sha": "abc", "html_url": ""}}

    monkeypatch.setattr(m, "_github_api_put", _faux_put)
    if patch_du_modele is not None:
        monkeypatch.setattr(m, "_openai_generate_file_patch", lambda **kw: patch_du_modele)

    patched, _skipped, _targets, ai = m._deep_patch_issue_files(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="fix",
        all_paths=[CHEMIN], issue_key="open_graph_url_not_matching_canonical",
        issue_label="OG URL", impacted_urls=["https://exemple.fr/about"],
        site_name="exemple", file_state={CHEMIN: {"sha": "vieux", "content": contenu}},
        max_files=4, link_rewriter=rewriter, rewriter_ai_fallback=False,
        targets_override=[CHEMIN], allow_ai_targeting=False,
    )
    return patched, ai, commits


# --- defaut 1 : ce qui a ete ecrit doit partir -------------------------------------------

def test_le_fichier_PART_meme_si_le_reecriveur_n_a_rien_trouve(monkeypatch) -> None:
    """Le reecriveur rend (contenu inchange, 0) ; un garde-fou ecrit ensuite. La boucle doit
    committer — sinon le travail du garde-fou est perdu en silence."""
    _garde_qui_ecrit(monkeypatch)
    patched, _ai, commits = _lancer(monkeypatch, rewriter=lambda raw: (raw, 0))
    assert patched == [CHEMIN], "le fichier n'a pas été patché : %r" % patched
    ecrits = [v for k, v in commits.items() if k.endswith(CHEMIN)]
    assert ecrits and MARQUE in ecrits[0], "le travail du garde-fou n'a pas été commité"


def test_un_fichier_reellement_INCHANGE_ne_part_pas(monkeypatch) -> None:
    """L'autre moitie : sans elle, la correction precedente ouvrirait des pull requests vides."""
    patched, _ai, commits = _lancer(monkeypatch, rewriter=lambda raw: (raw, 0))
    assert patched == [], "un fichier sans aucune modification a été commité : %r" % patched
    assert not commits


def test_la_decision_de_jeter_ne_consulte_PAS_le_drapeau() -> None:
    """La garde qui empeche le drapeau de revenir.

    Le remettre a l'air d'un raccourci gratuit et casse a nouveau tout garde-fou qui ecrit apres
    un reecriveur bredouille. Le defaut ne se voit nulle part : le produit repond « aucun
    fichier patché », ce qui ressemble a une limite du correcteur.
    """
    import inspect

    source = inspect.getsource(m._deep_patch_issue_files)
    decision = [l.strip() for l in source.splitlines()
                if "new_content.strip() == raw.strip()" in l]
    assert decision, "la comparaison de contenu a disparu de la boucle"
    assert not any("no_change" in l for l in decision), (
        "la décision de jeter consulte à nouveau `no_change` : un garde-fou qui écrit sera "
        "ignoré quand le réécriveur n'a rien trouvé — %s" % decision)


# --- defaut 2 : qui a ECRIT la correction -------------------------------------------------

def test_une_correction_DETERMINISTE_n_est_pas_mise_au_compte_du_modele(monkeypatch) -> None:
    """Mesure du 19/09/2026 : huit fichiers factures sans un seul appel au modele.

    Facturer un travail que le modele n'a pas fait, c'est vendre du calcul qui n'a pas ete
    depense. Et annoncer « redige par le modele » sur un diff mecanique apprend au client a se
    mefier de diffs qu'il pourrait merger les yeux fermes.
    """
    _garde_qui_ecrit(monkeypatch)
    patched, ai, _commits = _lancer(monkeypatch, rewriter=lambda raw: (raw, 0))
    assert patched == [CHEMIN], patched
    assert ai == [], "un fichier que le modèle n'a pas touché est compté comme écrit par lui"


def test_une_ecriture_REELLE_du_modele_reste_comptee(monkeypatch) -> None:
    """Le bord sans lequel la correction precedente cesserait de facturer ce qui doit l'etre.

    MA PREMIERE SIMULATION ETAIT FAUSSE : je faisais rendre au reecriveur un contenu modifie
    avec un compte de zero. Ce contenu est JETE — un compte nul fait prendre la branche « rien
    trouve », qui renvoie le fichier d'origine. Le modele n'ecrit pas par ce chemin-la, mais par
    `_openai_generate_file_patch`.
    """
    ecrit_par_le_modele = PAGE.replace("'About'", "'A propos'")
    patched, ai, _commits = _lancer(
        monkeypatch, rewriter=None,
        patch_du_modele={"patched_content": ecrit_par_le_modele})
    assert patched == [CHEMIN], patched
    assert ai == [CHEMIN], "une écriture réelle du modèle n'est plus comptée : %r" % ai
