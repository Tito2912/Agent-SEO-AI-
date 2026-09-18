# -*- coding: utf-8 -*-
"""`no_hsts` ne recevra pas de correcteur : poser HSTS engage le client, et ne se reprend pas.

Derniere candidate « FICHIER » de la liste, instruite le 18/09/2026 et refusee par le
proprietaire. L'en-tete s'ecrit bel et bien dans un fichier du depot sur un hebergeur statique
(`_headers`, `netlify.toml`, `vercel.json`) et sa valeur est fixe : techniquement, rien ne serait
devine. Quatre raisons s'y opposent, et elles se renforcent.

LA DETECTION NE MESURE QUE LA PRESENCE, jamais la valeur (voir le crawler : `if not sts`). Un
`max-age=300` suffirait donc a faire taire l'anomalie sans proteger personne. C'est la definition
meme de faire taire plutot que corriger, et c'est le premier test ci-dessous qui le tient : si la
detection se mettait un jour a lire le `max-age`, cette raison-la tomberait et la decision
devrait etre reexaminee.

LA VERSION QUI PROTEGE VRAIMENT NE SE REPREND PAS. Un `max-age` de six mois ou plus est grave dans
le navigateur du visiteur : il refusera le HTTP pendant toute cette duree, quoi qu'on republie.
L'asymetrie des robots d'IA jouait dans un sens — un blocage AJOUTE se retire en une ligne — et
elle joue ici a l'envers : c'est l'AJOUT qui est irrattrapable. Certificat expire, sous-domaine
reste en clair, et le client n'a aucun recours immediat.

LE PRODUIT LA CLASSE DEJA `notice`, comme la politique d'entrainement des robots d'IA. Le test
voisin de cette famille-la dit pourquoi : classer un choix legitime en avertissement pousse a
« corriger » une decision du proprietaire.

ET LE FICHIER VISE EST CELUI QU'ON REFUSE DEJA DE TOUCHER. Le refus de `redirect_3xx` le nomme
mot pour mot : « le fichier qui porte aussi tes en-têtes de sécurité et de cache ». Y ecrire
automatiquement un engagement de six mois serait le contraire de ce que ce refus protege.

RESTE UNE QUESTION OUVERTE, qui ne se tranche pas depuis le code : Ahrefs signale-t-il HSTS ? Si
ce n'est pas le cas, `no_hsts` est une anomalie Semrush-only et la bonne action n'est pas de la
refuser mais de la SUPPRIMER, comme les quatre deja supprimees pour parite. A verifier sur un
rapport reel avant d'y revenir.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-hsts-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402
from backend import audit_dashboard as dash  # noqa: E402

CRAWLER = (WEB_ROOT.parent / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py")


def test_hsts_reste_signale_jamais_corrige() -> None:
    assert not m._github_issue_auto_fixable("no_hsts")


def test_la_detection_ne_lit_QUE_la_presence_de_l_en_tete() -> None:
    """La premiere raison du refus, et celle qui peut cesser d'etre vraie.

    Tant que le crawler se contente de `if not sts`, un `max-age=300` symbolique eteint
    l'anomalie sans rien proteger — un correcteur « sur » serait donc un correcteur qui ment. Le
    jour ou la detection lira la duree, ce test tombera, et c'est le signal qu'il faut rouvrir la
    question au lieu de reconduire le refus par habitude.
    """
    source = CRAWLER.read_text(encoding="utf-8")
    debut = source.index('sts = (resp.headers.get("Strict-Transport-Security")')
    bloc = source[debut:debut + 400]
    assert "if not sts:" in bloc, bloc[:200]
    assert "max-age" not in bloc.lower(), (
        "la detection lit desormais la duree : la decision sur no_hsts doit etre reexaminee")


def test_hsts_est_une_REMARQUE_pas_un_defaut() -> None:
    """Meme classement que la politique d'entrainement des robots d'IA, et pour la meme raison."""
    assert dash.ISSUE_CATALOG["no_hsts"].severity == "notice"


def test_le_fichier_vise_est_celui_qu_on_refuse_deja_de_toucher() -> None:
    """Le refus de `redirect_3xx` protege exactement ce fichier ; y ecrire HSTS le contredirait."""
    out = m._prepare_issue_fix(
        issue_key="redirect_3xx",
        issues={"redirect_3xx": {"count": 1, "examples": ["https://exemple.fr/a"]}},
        impacted=["https://exemple.fr/a"], all_paths=["netlify.toml"], site_name="exemple.fr",
        owner="o", repo_name="r", branch="main", token="")
    assert "sécurité" in str(out.get("refusal") or "")
