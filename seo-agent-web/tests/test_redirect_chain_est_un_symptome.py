# -*- coding: utf-8 -*-
"""`redirect_chain` ne recevra pas de correcteur, et ce n'est pas faute de valeur mesuree.

Instruite le 18/09/2026 comme premiere candidate « FICHIER » restante. La mesure disait oui : la
detection est `len(p.redirect_statuses) > 1`, et la destination FINALE est connue du crawl, donc
rien ne serait devine. C'est en cherchant QUEL fichier editer que la reponse s'inverse.

Une chaine se declenche par une porte d'entree, et il n'y en a que trois :

  - un LIEN INTERNE pointe vers le premier maillon. Corrige, par `page_has_links_to_redirect` :
    `_rewrite_redirect_links` repointe le lien sur la destination finale mesuree ;
  - une ENTREE DE SITEMAP pointe vers le premier maillon. Corrige, par `sitemap_3xx_redirect` :
    `_rewrite_sitemap_locs` reecrit le <loc> ;
  - les REGLES EMPILEES de la configuration (http->https, puis www->apex, puis slash final).
    Refuse, par `_REDIRECT_CONFIG_KEYS` : c'est la canonicalisation volontaire du site, et le
    fichier qui la porte porte aussi HSTS et CSP.

Il ne reste donc rien a corriger qui ne soit deja fait ou deja refuse. `redirect_chain` mesure la
CONSEQUENCE de trois causes dont chacune a sa famille — la signaler reste utile, lui donner un
correcteur ne le serait pas.

ET CE SERAIT NUISIBLE, pas seulement inutile. `_open_pr_for_issue` garantit une seule PR ouverte
PAR ANOMALIE, pas par fichier. Un correcteur de `redirect_chain` ouvrirait donc une seconde PR sur
les lignes que `page_has_links_to_redirect` vient de modifier, et les deux entreraient en conflit.
C'est exactement l'accident des PR #8 et #9, ouvertes toutes les deux pour une seule correction de
langue, et que ce verrou-la avait ete ecrit pour empecher.

`redirect_chain_too_long` est la meme mesure au-dela de quatre sauts : meme portes d'entree, meme
decision.

CE QUE CE TEST TIENT VRAIMENT. Pas « on ne corrige pas » — cela, un simple refus le dirait. Il
tient le LIEN dont la decision depend : les deux causes reparables restent corrigees ailleurs. Le
jour ou l'une des deux cesserait de l'etre, la chaine deviendrait un vrai trou et la decision
devrait etre rouverte. Sans ce bord, on garderait le refus longtemps apres que sa raison a disparu.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-chaine-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402


def test_la_chaine_de_redirections_reste_signalee_jamais_corrigee() -> None:
    """La decision, et les deux longueurs de chaine qu'elle couvre."""
    assert not m._github_issue_auto_fixable("redirect_chain")
    assert not m._github_issue_auto_fixable("redirect_chain_too_long")


def test_le_lien_qui_MENE_a_la_chaine_est_corrige_lui() -> None:
    """Premiere porte d'entree. Si elle se fermait, la decision ci-dessus ne tiendrait plus."""
    assert m._github_issue_auto_fixable("page_has_links_to_redirect")
    assert "page_has_links_to_redirect" in m._REDIRECT_LINK_KEYS


def test_l_entree_de_sitemap_qui_MENE_a_la_chaine_est_corrigee_elle() -> None:
    """Deuxieme porte d'entree, meme dependance."""
    assert m._github_issue_auto_fixable("sitemap_3xx_redirect")
    assert "sitemap_3xx_redirect" in m._SITEMAP_REWRITE_KEYS


def test_la_configuration_qui_EMPILE_les_regles_est_refusee_avec_un_motif() -> None:
    """Troisieme porte d'entree : refusee, et le refus doit rester PARLANT.

    Un refus muet renverrait le client a « aucun fichier corrigeable », ce qui se lit comme une
    panne alors que c'est une decision. Le motif nomme la canonicalisation volontaire et le
    fichier qu'on ne veut pas voir touche.
    """
    out = m._prepare_issue_fix(
        issue_key="redirect_3xx", issues={"redirect_3xx": {"count": 1, "examples": [
            "https://exemple.fr/a"]}}, impacted=["https://exemple.fr/a"],
        all_paths=["netlify.toml"], site_name="exemple.fr", owner="o", repo_name="r",
        branch="main", token="")
    motif = str(out.get("refusal") or "")
    assert motif, out
    assert "canonicalisation volontaire" in motif
    assert out.get("link_rewriter") is None


def test_une_boucle_sur_ELLE_MEME_reste_la_seule_exception() -> None:
    """Ce que la decision ne recouvre pas : une URL qui se redirige vers elle-meme est un defaut,
    pas une politique, et celle-la est bien reparee."""
    bloc = {"count": 1, "examples": ["https://exemple.fr/a"],
            "evidence": {"kind": "page_values", "items": [
                {"page": "https://exemple.fr/a", "field": m._SELF_LOOP_FIELD,
                 "value": "https://exemple.fr/a"}]}}
    out = m._prepare_issue_fix(
        issue_key="redirect_3xx", issues={"redirect_3xx": bloc},
        impacted=["https://exemple.fr/a"], all_paths=["netlify.toml"], site_name="exemple.fr",
        owner="o", repo_name="r", branch="main", token="")
    assert not str(out.get("refusal") or ""), out
    assert out.get("loop_paths"), out
