"""Premiere des 96 familles sans correcteur : la politique d'entrainement incoherente.

Elle se leve quand un site refuse UNE PARTIE des robots d'entrainement et laisse passer les
autres — le plus souvent sans l'avoir decide : une regle recopiee quelque part nomme trois agents,
et les six autres apprennent librement du site.

CETTE ANOMALIE A DEUX CORRECTIONS OPPOSEES, et c'est ce qui la distingue d'un canonical qui pointe
une 404. Etre coherent, c'est soit tout bloquer, soit tout ouvrir : l'anomalie est l'INCOHERENCE,
pas le sens. Le code doit donc choisir une direction que la mesure ne dicte pas.

LE CHOIX RETENU EST DE BLOQUER, pour deux raisons :
  - le site a DEJA ecrit des regles de refus, donc son intention exprimee est de refuser ;
  - l'asymetrie du risque : un blocage ajoute se retire en une ligne, un blocage supprime laisse
    un robot apprendre du site entre-temps, et cela ne se reprend pas.

La PR l'annonce au client (`_FIX_PREMISE_NOTES`) et cette famille ne fusionne jamais toute seule —
meme traitement que `sitemap_noindex_page`, dont la premisse est discutable de la meme facon.

Aucun repli modele : la liste des agents et la regle a ecrire sont toutes deux connues, il n'y a
rien a formuler. Un `app/robots.ts` engendre restera donc non corrige — prix plus bas que celui
d'un modele qui reecrit un fichier de politique.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-ia-bots-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402

ROBOTS = """User-agent: *
Allow: /

User-agent: GPTBot
Disallow: /

Sitemap: https://exemple.fr/sitemap.xml
"""


def _agents_refuses(contenu: str) -> set[str]:
    """Les agents qui portent un `Disallow: /`, lus comme le ferait un robot."""
    refuses = set()
    for m in app_module._ROBOTS_BLOC_RE.finditer(contenu):
        if app_module._ROBOTS_DISALLOW_TOUT_RE.search(m.group("regles") or ""):
            refuses.add(m.group("agent").strip())
    return refuses


def test_les_robots_laisses_passer_recoivent_la_meme_regle() -> None:
    sortie, n = app_module._align_ai_training_policy(ROBOTS, ["ClaudeBot", "CCBot"])
    assert n == 2, n
    assert _agents_refuses(sortie) == {"GPTBot", "ClaudeBot", "CCBot"}


def test_aucune_regle_existante_n_est_RETIREE() -> None:
    """Le correctif n'ouvre jamais ce qui etait ferme : c'est le sens de l'asymetrie."""
    sortie, _ = app_module._align_ai_training_policy(ROBOTS, ["ClaudeBot"])
    assert "User-agent: *\nAllow: /" in sortie, sortie
    assert "Sitemap: https://exemple.fr/sitemap.xml" in sortie, sortie
    assert "User-agent: GPTBot\nDisallow: /" in sortie, sortie


def test_rejouer_le_correctif_n_ajoute_rien() -> None:
    """Un correctif qui ne sait pas s'arreter empile les regles a chaque passage."""
    sortie, _ = app_module._align_ai_training_policy(ROBOTS, ["ClaudeBot", "CCBot"])
    encore, n = app_module._align_ai_training_policy(sortie, ["ClaudeBot", "CCBot"])
    assert n == 0 and encore == sortie


def test_un_agent_DEJA_refuse_n_est_pas_redeclare() -> None:
    sortie, n = app_module._align_ai_training_policy(ROBOTS, ["GPTBot", "CCBot"])
    assert n == 1, n
    assert sortie.count("User-agent: GPTBot") == 1, sortie


def test_la_casse_de_l_agent_n_induit_pas_de_doublon() -> None:
    """`gptbot` et `GPTBot` sont le meme agent : un robots.txt ne distingue pas la casse."""
    _, n = app_module._align_ai_training_policy(ROBOTS, ["gptbot"])
    assert n == 0, n


def test_un_fichier_qui_n_est_PAS_un_robots_txt_est_refuse() -> None:
    """Meme precaution qu'`_add_sitemap_to_robots` : une ligne partie dans le mauvais fichier a
    fait echouer cinq deploiements le 15/09/2026."""
    code = "export default function robots() {\n  return { rules: [] };\n}\n"
    sortie, n = app_module._align_ai_training_policy(code, ["CCBot"])
    assert n == 0 and sortie == code


def test_sans_liste_d_agents_on_n_ecrit_rien() -> None:
    sortie, n = app_module._align_ai_training_policy(ROBOTS, [])
    assert n == 0 and sortie == ROBOTS


def test_la_PR_annonce_que_le_SENS_du_correctif_est_discutable() -> None:
    """La direction n'est pas une mesure : le client doit pouvoir la renverser en connaissance."""
    note = app_module._fix_premise_note("inconsistent_ai_training_bot_policy")
    assert note and "bloquer" in note.lower(), note


def test_la_famille_est_desormais_declaree_corrigeable() -> None:
    """Sans cette declaration, le correctif existe mais n'est jamais propose au client."""
    assert "inconsistent_ai_training_bot_policy" in set(app_module._handled_issue_keys())
