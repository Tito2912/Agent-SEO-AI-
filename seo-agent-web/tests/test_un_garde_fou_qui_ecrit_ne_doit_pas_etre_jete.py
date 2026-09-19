# -*- coding: utf-8 -*-
"""Un garde-fou qui MODIFIE le fichier doit faire partir la correction, meme si le reecriveur
n'avait rien trouve.

LE DEFAUT, MESURE LE 19/09/2026 SUR UN SITE CLIENT. Le reecriveur d'og:url ne trouve rien a
remplacer sur une page Next.js qui herite son openGraph de la mise en page racine. Il rend alors
`{"no_change": True, "patched_content": raw}`. Les garde-fous s'executent ensuite — et l'un
d'eux, l'insertion d'og:url, ECRIVAIT bien la ligne manquante. Puis venait :

    if patch.get("no_change") or new_content.strip() == raw.strip():
        continue

Le drapeau `no_change` dit ce que le REECRIVEUR a fait. Il ne dit rien de ce que le fichier est
devenu APRES les garde-fous, qui ecrivent eux aussi. Le tester ici jetait donc le travail d'un
garde-fou au motif que l'etape d'avant n'avait rien fait. Neuf pages signalees, zero fichier
patche, et pas une ligne dans les journaux pour l'expliquer — la correction avait bel et bien
ete calculee, puis oubliee une instruction plus loin.

LA SEULE QUESTION EST « LE CONTENU A-T-IL CHANGE ? », et la comparaison y repond deja. Le
drapeau etait redondant : les deux seuls retours qui le posent dans cette boucle portent
`patched_content: raw`. Les autres `no_change` du produit n'ont pas de `patched_content` et
sortent plus haut.

CE TEST NE VISE PAS QUE og:url. Tout garde-fou pose apres le reecriveur etait concerne — la
completion Open Graph l'est aussi. C'est la boucle qui est testee, pas une famille.
"""

from __future__ import annotations

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
PAGE = """import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'About',
  alternates: { canonical: '/about' },
};

export default function AboutPage() {
  return <article><h1>About</h1></article>;
}
"""


def _lancer(monkeypatch, *, rewriter) -> tuple[list[str], dict[str, str]]:
    """Fait tourner la boucle sur UN fichier, sans reseau, et rend (fichiers patches, commits).

    `file_state` pre-rempli court-circuite la lecture GitHub ; seule l'ECRITURE est interceptee.
    On mesure donc ce que la boucle a reellement decide de committer.
    """
    commits: dict[str, str] = {}

    def _faux_put(chemin, *, token, json_body):
        import base64
        commits[chemin] = base64.b64decode(json_body["content"]).decode("utf-8")
        return {"content": {"sha": "neuf"}, "commit": {"sha": "abc", "html_url": ""}}

    monkeypatch.setattr(m, "_github_api_put", _faux_put)
    monkeypatch.setattr(m, "_github_api_get",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("lecture reseau")))

    patches, _skipped, _ai = m._deep_patch_issue_files(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="fix",
        all_paths=[CHEMIN], issue_key="open_graph_url_not_matching_canonical",
        issue_label="OG URL", impacted_urls=["https://oryvalo.com/about"],
        site_name="oryvalo", file_state={CHEMIN: {"sha": "vieux", "content": PAGE}},
        max_files=4, link_rewriter=rewriter, rewriter_ai_fallback=False,
        targets_override=[CHEMIN], allow_ai_targeting=False,
    )[:3]
    return patches, commits


def test_le_fichier_PART_meme_si_le_reecriveur_n_a_rien_trouve(monkeypatch) -> None:
    """Le defaut exact : la correction etait calculee puis jetee.

    Le reecriveur rend (contenu inchange, 0) — c'est ce qui se produit quand la valeur fautive
    est heritee et n'est ecrite dans aucun fichier de page. L'insertion prend le relais et pose
    la ligne. La boucle doit committer.
    """
    patches, commits = _lancer(monkeypatch, rewriter=lambda raw: (raw, 0))
    assert patches == [CHEMIN], "le fichier n'a pas été patché : %r" % patches
    # La cle est le chemin d'API complet (`/repos/o/r/contents/<fichier>`), pas le chemin nu.
    ecrits = {k: v for k, v in commits.items() if k.endswith(CHEMIN)}
    assert ecrits, "rien n'a été commité : %r" % list(commits)
    contenu = next(iter(ecrits.values()))
    assert "openGraph: { url: '/about' }," in contenu, contenu


def test_un_fichier_reellement_INCHANGE_ne_part_pas(monkeypatch) -> None:
    """L'autre moitie, sans laquelle la correction precedente serait une regression.

    Quand aucun garde-fou n'a rien a ecrire non plus, la boucle doit toujours s'abstenir : une
    pull request au diff vide est pire qu'une absence de pull request.
    """
    sans_canonical = PAGE.replace("  alternates: { canonical: '/about' },\n", "")
    commits: dict[str, str] = {}

    def _faux_put(chemin, *, token, json_body):
        import base64
        commits[chemin] = base64.b64decode(json_body["content"]).decode("utf-8")
        return {"content": {"sha": "neuf"}, "commit": {"sha": "abc", "html_url": ""}}

    monkeypatch.setattr(m, "_github_api_put", _faux_put)
    patches = m._deep_patch_issue_files(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="fix",
        all_paths=[CHEMIN], issue_key="open_graph_url_not_matching_canonical",
        issue_label="OG URL", impacted_urls=["https://oryvalo.com/about"],
        site_name="oryvalo", file_state={CHEMIN: {"sha": "vieux", "content": sans_canonical}},
        max_files=4, link_rewriter=lambda raw: (raw, 0), rewriter_ai_fallback=False,
        targets_override=[CHEMIN], allow_ai_targeting=False,
    )[0]
    assert patches == [], "un fichier sans aucune modification a été commité : %r" % patches
    assert not commits


def test_la_decision_de_jeter_ne_consulte_PLUS_le_drapeau() -> None:
    """La garde qui empeche le drapeau de revenir.

    Le remettre serait tentant — il a l'air d'un raccourci gratuit — et casserait a nouveau
    tout garde-fou qui ecrit apres un reecriveur bredouille. Le defaut ne se voit nulle part :
    le produit repond « aucun fichier patché », ce qui ressemble a une limite du correcteur.
    """
    import inspect

    source = inspect.getsource(m._deep_patch_issue_files)
    ligne = [l.strip() for l in source.splitlines()
             if "continue" in l or "new_content.strip() == raw.strip()" in l]
    decision = [l for l in ligne if "new_content.strip() == raw.strip()" in l]
    assert decision, "la comparaison de contenu a disparu de la boucle"
    assert not any("no_change" in l for l in decision), (
        "la décision de jeter consulte à nouveau `no_change` : un garde-fou qui écrit sera "
        "ignoré quand le réécriveur n'a rien trouvé — %s" % decision)


# --- qui a ECRIT la correction ------------------------------------------------------------

def _lancer_et_compter(monkeypatch, *, rewriter, contenu=PAGE):
    """Rend (patches, ai_files) pour un fichier donne."""
    monkeypatch.setattr(
        m, "_github_api_put",
        lambda chemin, *, token, json_body: {"content": {"sha": "neuf"},
                                             "commit": {"sha": "abc", "html_url": ""}})
    # QUATRE valeurs : (patched, skipped, targets, ai_files). Un `[:3]` faisait lire
    # `targets` en croyant lire `ai_files` — le test mesurait la mauvaise liste et echouait
    # sur un code correct.
    patches, _skipped, _targets, ai = m._deep_patch_issue_files(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="fix",
        all_paths=[CHEMIN], issue_key="open_graph_url_not_matching_canonical",
        issue_label="OG URL", impacted_urls=["https://oryvalo.com/about"],
        site_name="oryvalo", file_state={CHEMIN: {"sha": "vieux", "content": contenu}},
        max_files=4, link_rewriter=rewriter, rewriter_ai_fallback=False,
        targets_override=[CHEMIN], allow_ai_targeting=False,
    )
    return patches, ai


def test_une_correction_DETERMINISTE_n_est_pas_mise_au_compte_du_modele(monkeypatch) -> None:
    """Mesure du 19/09/2026, pull request #12 : huit fichiers factures pour rien.

    Le repli IA de cette famille est desactive au-dela d'une paire — il y en avait neuf — donc
    AUCUN appel au modele n'a eu lieu. Les huit fichiers etaient pourtant comptes comme ecrits
    par lui : factures au quota IA, et annonces dans la pull request comme une prose a relire.

    Deux consequences, et aucune n'est cosmetique. Facturer un travail que le modele n'a pas
    fait, c'est vendre du calcul qui n'a pas ete depense. Et annoncer « redige par le modele »
    sur un diff mecanique apprend au client a se mefier de diffs qu'il pourrait merger les yeux
    fermes.
    """
    patches, ai = _lancer_et_compter(monkeypatch, rewriter=lambda raw: (raw, 0))
    assert patches == [CHEMIN], patches
    assert ai == [], "un fichier que le modèle n'a pas touché est compté comme écrit par lui"


def test_une_correction_REELLEMENT_ecrite_par_le_modele_reste_comptee(monkeypatch) -> None:
    """Le bord sans lequel la correction precedente cesserait de facturer ce qui doit l'etre.

    MA PREMIERE SIMULATION ETAIT FAUSSE et vaut d'etre notee : je faisais rendre au reecriveur
    un contenu modifie avec un compte de zero. Or ce contenu est JETE — un compte nul fait
    prendre la branche « rien trouve », qui renvoie le fichier d'origine. Le modele n'ecrit pas
    par ce chemin-la.

    Le vrai chemin est `_openai_generate_file_patch`, qui rend un contenu SANS drapeau
    deterministe. C'est lui qu'on remplace ici, et cette fois le fichier doit bien etre compte
    comme ecrit par le modele : facture, et annonce comme une prose a relire.
    """
    ecrit_par_le_modele = PAGE.replace("title: 'About'", "title: 'A propos de nous'")

    monkeypatch.setattr(
        m, "_github_api_put",
        lambda chemin, *, token, json_body: {"content": {"sha": "neuf"},
                                             "commit": {"sha": "abc", "html_url": ""}})
    monkeypatch.setattr(
        m, "_openai_generate_file_patch",
        lambda **kw: {"patched_content": ecrit_par_le_modele})

    patches, _skipped, _targets, ai = m._deep_patch_issue_files(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="fix",
        all_paths=[CHEMIN], issue_key="open_graph_url_not_matching_canonical",
        issue_label="OG URL", impacted_urls=["https://oryvalo.com/about"],
        site_name="oryvalo", file_state={CHEMIN: {"sha": "vieux", "content": PAGE}},
        max_files=4, link_rewriter=None, rewriter_ai_fallback=False,
        targets_override=[CHEMIN], allow_ai_targeting=False,
    )
    assert patches == [CHEMIN], patches
    assert ai == [CHEMIN], "une écriture réelle du modèle n'est plus comptée : %r" % ai
