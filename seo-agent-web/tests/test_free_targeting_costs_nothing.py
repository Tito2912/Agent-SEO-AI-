"""Un passage annonce sans modele ne doit appeler AUCUN modele — y compris pour CHOISIR le fichier.

Le garde-fou de cout du banc (`GAUNTLET_FREE=1`) ne regardait que l'ECRITURE de la valeur :
« la famille remplace un litteral, elle n'appelle jamais le modele ». C'etait vrai de l'ecriture
et faux du ciblage. `_resolve_issue_targets` se termine par deux selecteurs pilotes par le modele,
et pour les familles d'actifs `want_page_targeting` est faux — donc `index_resolved_all` reste
faux et le mappeur partait a chaque fois, meme quand la preuve avait deja trouve les fichiers.

Mesure du 16/09/2026, static-html, passe `GAUNTLET_FREE=1` : sept appels, un par famille de
redirection d'actif, tous repris par le repli OpenAI donc factures. Environ soixante-trois sur
les neuf idiomes, pour un passage documente « cout nul ».
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-free-targeting-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402

SITE = "https://exemple.fr"
# L'arbre du banc, reduit : des pages statiques et le composant partage qui porte l'actif.
TREE = [
    "public/index.html",
    "public/gauntlet/redirected-image.html",
    "public/gauntlet/redirected-css.html",
    "components/Entete.html",
]


def _resolve(*, allow_ai: bool, located: list[str], journal: list[str]) -> list[str]:
    """Le meme appel que fait `_deep_patch_issue_files`, avec les deux selecteurs mouchardes."""

    # Des chemins qui n'existent nulle part ailleurs : leur presence dans le resultat prouve
    # qu'ils viennent du MODELE, et non d'une etape deterministe qui rendrait la meme reponse.
    def _ai_map() -> list[str]:
        journal.append("ai_map")
        return ["SENTINELLE-mappeur.html"]

    def _ai_pick() -> list[str]:
        journal.append("ai_pick")
        return ["SENTINELLE-selecteur.html"]

    return app_module._resolve_issue_targets(
        all_paths=TREE,
        # `index=None` reproduit le cas mesure : rien ne resout les URL, donc
        # `index_resolved_all` est faux et le mappeur est en droit de partir.
        index=None,
        issue_key="image_redirects",
        issue_label="Images qui redirigent",
        impacted_urls=[f"{SITE}/gauntlet/redirected-image"],
        located=located,
        max_files=6,
        evidence=["/img/ancienne.png"],
        allow_ai=allow_ai,
        ai_map=_ai_map,
        ai_pick=_ai_pick,
    )


def test_le_ciblage_appelle_bien_le_modele_quand_on_l_y_autorise() -> None:
    """Sans cette moitie, le test suivant passerait sur un chemin qui n'appelle jamais rien.

    C'est la verification qui manquait le jour ou le mode gratuit a ete ecrit : on a mesure que
    l'ECRITURE ne coutait rien, sans jamais mesurer ce que faisait le reste de la famille.
    """
    journal: list[str] = []
    cibles = _resolve(allow_ai=True, located=["public/gauntlet/redirected-image.html"], journal=journal)
    assert "ai_map" in journal, journal
    assert "SENTINELLE-mappeur.html" in cibles, cibles


def test_sans_modele_aucun_selecteur_ne_part() -> None:
    journal: list[str] = []
    cibles = _resolve(allow_ai=False, located=["public/gauntlet/redirected-image.html"], journal=journal)
    assert journal == [], f"le mode sans modele a quand meme appele : {journal}"
    # Et le ciblage deterministe repond toujours : la preuve a trouve le fichier, on le garde.
    assert "public/gauntlet/redirected-image.html" in cibles, cibles


def test_sans_modele_le_dernier_recours_tombe_aussi() -> None:
    """Le second selecteur — « que le modele choisisse dans l'arbre » — ne part pas non plus.

    Il ne s'agit PAS d'exiger une liste vide : sans aucun fichier localise, le ciblage
    deterministe propose encore ses candidats codes en dur, et c'est tres bien — ils ne coutent
    rien. Ce qui est exige, c'est qu'aucune des deux cibles ne vienne du modele.
    """
    journal: list[str] = []
    cibles = _resolve(allow_ai=False, located=[], journal=journal)
    assert journal == [], f"le mode sans modele a quand meme appele : {journal}"
    assert not [c for c in cibles if c.startswith("SENTINELLE")], cibles
