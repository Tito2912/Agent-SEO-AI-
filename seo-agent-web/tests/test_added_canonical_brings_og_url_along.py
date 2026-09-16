"""Une correction qui AJOUTE un canonical ne doit pas laisser og:url en desaccord derriere elle.

Troisieme chemin de la meme cause — og:url et canonical qui divergent — et le seul que relire le
canonical sur la branche ne pouvait pas fermer.

Mesure du 16/09/2026, cycle des neuf idiomes, page `no-canonical-b` :

    avant   (aucun canonical)                og:url -> lui-meme
    apres   canonical -> la page maitresse   og:url -> lui-meme, donc DESORMAIS en desaccord

`duplicate_pages_without_canonical` a raison de designer la maitresse. Mais la page n'apparait
dans AUCUNE liste de la famille og:url : sans canonical au crawl il n'y a pas de desaccord a
signaler, donc pas de paire, donc rien a reecrire. L'anomalie n'existe qu'APRES la correction.
C'est donc a la correction qui la cree de la refermer.

Apres le correctif « lire le canonical sur la branche », cette forme etait ce qui restait :
og:url resistait encore sur SIX stacks, a exactement 1 page chacune.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-og-ajout-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402

S = "https://exemple.fr"
B = f"{S}/gauntlet/no-canonical-b"
MAITRESSE = f"{S}/gauntlet/no-canonical-a"


def _page(canonical: str | None, og: str) -> str:
    lignes = ["<!doctype html><html lang=\"fr\"><head>"]
    if canonical:
        lignes.append(f'  <link rel="canonical" href="{canonical}" />')
    lignes.append(f'  <meta property="og:url" content="{og}" />')
    lignes.append("</head><body><h1>Page</h1></body></html>")
    return "\n".join(lignes) + "\n"


def test_le_canonical_ajoute_entraine_og_url() -> None:
    avant = _page(None, B)
    apres = _page(MAITRESSE, B)
    sortie, notes = app_module._align_og_url_with_added_canonical(apres, avant)
    assert f'content="{MAITRESSE}"' in sortie, sortie
    assert notes and "aligne" in notes[0], notes


def test_un_canonical_DEJA_present_ne_declenche_rien() -> None:
    """Ce cas appartient a la famille og:url, qui le traite avec les paires du crawl.

    Sans cette borne, le garde-fou alignerait og:url sur TOUT canonical a chaque patch, y compris
    sur des pages qu'aucun crawl n'a signalees — exactement ce que la famille s'interdit.
    """
    avant = _page(f"{S}/ancien", B)
    apres = _page(MAITRESSE, B)
    sortie, notes = app_module._align_og_url_with_added_canonical(apres, avant)
    assert sortie == apres and notes == [], notes


def test_un_og_url_deja_d_accord_n_est_pas_touche() -> None:
    avant = _page(None, MAITRESSE)
    apres = _page(MAITRESSE, MAITRESSE)
    sortie, notes = app_module._align_og_url_with_added_canonical(apres, avant)
    assert sortie == apres and notes == [], notes


def test_sans_og_url_il_n_y_a_rien_a_aligner() -> None:
    avant = "<head></head>"
    apres = f'<head><link rel="canonical" href="{MAITRESSE}" /></head>'
    sortie, notes = app_module._align_og_url_with_added_canonical(apres, avant)
    assert sortie == apres and notes == [], notes


def test_deux_og_url_dans_un_fichier_font_renoncer() -> None:
    """Un fichier qui porte plusieurs pages : on ne sait pas de laquelle on parle, on s'abstient."""
    avant = "<head></head>"
    apres = (f'<head><link rel="canonical" href="{MAITRESSE}" />'
             f'<meta property="og:url" content="{S}/x" />'
             f'<meta property="og:url" content="{S}/y" /></head>')
    sortie, notes = app_module._align_og_url_with_added_canonical(apres, avant)
    assert sortie == apres and notes == [], notes
