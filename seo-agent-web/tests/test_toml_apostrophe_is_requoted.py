"""Une apostrophe dans une chaine TOML litterale se repare en requotant, pas en echappant.

En TOML, `'…'` est une chaine LITTERALE : aucun echappement n'y existe. L'apostrophe termine donc
la chaine, et la fin du titre devient de la syntaxe invalide. Le remede JavaScript serait un
poison — un `\\'` resterait tel quel dans le titre servi.

C'est pourquoi `_front_matter_parse_error` REFUSAIT ces fichiers : faute de remede sur, mieux vaut
une correction perdue qu'un deploiement casse. Mesure du 16/09/2026 sur hugo, deux cycles
complets : CINQ refus, tous cette meme cause, tous a la meme erreur —

    TOMLDecodeError: Expected newline or end of document after a statement (at line 2, column 21)

Ils plafonnaient hugo a 85 % quand les autres stacks depassaient 95 %, et laissaient quatre
familles non corrigees.

Il existe pourtant un remede qui ne devine rien : une chaine BASIQUE `"…"` accepte l'apostrophe.
Et surtout il se VERIFIE — la version requotee n'est gardee que si `tomllib` lit desormais le
bloc. Le format garde le dernier mot ; on lui soumet une seconde version avant de renoncer.
"""

from __future__ import annotations

import os
import sys
import tempfile
import tomllib
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-toml-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402

CHEMIN = "content/gauntlet/missing-title.md"
TITRE = "Parcours d'obstacles : pages de test"


def _fichier(ligne_titre: str) -> str:
    """Le front matter d'une page hugo du banc, avec ses chaines MULTILIGNES.

    Ma premiere fixture n'en portait pas, et elle validait une reparation qui detruisait les
    vrais fichiers : le motif d'une ligne voit `raw_head = '''` comme une chaine litterale dont
    la valeur serait une apostrophe, et la requotait en `raw_head = "'"`. Chaque page hugo porte
    `raw_head` et `raw_body` — donc la reparation ne pouvait JAMAIS servir sur cette stack, la
    seule pour laquelle elle a ete ecrite.

    Une fixture plus simple que la realite ne prouve rien sur la realite.
    """
    return ("+++\n"
            'url = "/gauntlet/missing-title/"\n'
            f"{ligne_titre}\n"
            "html_attrs = ' lang=\"fr\"'\n"
            "raw_head = '''\n"
            "  <meta name=\"viewport\" content=\"width=device-width\" />\n"
            "'''\n"
            "raw_body = '''\n"
            "  <p>Le corps de la page.</p>\n"
            "'''\n"
            "+++\n\n"
            "Le corps de la page.\n")


def test_le_cas_mesure_sur_hugo_se_repare() -> None:
    casse = _fichier(f"title = '{TITRE}'")
    assert app_module._front_matter_parse_error(casse, CHEMIN), "la fixture doit etre cassee"
    repare, notes = app_module._requote_toml_apostrophes(casse, CHEMIN)
    assert notes, "aucune reparation tentee"
    assert app_module._front_matter_parse_error(repare, CHEMIN) == "", repare


def test_le_titre_servi_garde_son_apostrophe_et_rien_de_plus() -> None:
    """Le piege du remede JS : un antislash survivrait dans le titre affiche aux visiteurs."""
    repare, _ = app_module._requote_toml_apostrophes(_fichier(f"title = '{TITRE}'"), CHEMIN)
    bloc = repare.split("+++")[1]
    assert tomllib.loads(bloc)["title"] == TITRE
    assert "\\" not in repare, repare


def test_un_front_matter_SAIN_n_est_pas_touche() -> None:
    """On ne reecrit pas ce qui va bien : la requote ne se declenche que sur un bloc illisible."""
    sain = _fichier("title = 'Un titre sans apostrophe'")
    assert app_module._front_matter_parse_error(sain, CHEMIN) == ""
    sortie, notes = app_module._requote_toml_apostrophes(sain, CHEMIN)
    assert sortie == sain and notes == [], notes


def test_une_cassure_D_UNE_AUTRE_NATURE_reste_refusee() -> None:
    """La requote ne doit pas rendre un fichier a moitie repare : si le bloc ne lit toujours pas,
    on rend l'original et le refus ordinaire s'applique."""
    casse = _fichier("title = 'Titre' zzz = = =")
    sortie, notes = app_module._requote_toml_apostrophes(casse, CHEMIN)
    assert sortie == casse and notes == [], notes


def test_une_valeur_contenant_un_guillemet_double_est_echappee() -> None:
    titre = "Le \"meilleur\" guide d'obstacles"
    repare, notes = app_module._requote_toml_apostrophes(_fichier(f"title = '{titre}'"), CHEMIN)
    assert notes, "la reparation aurait du s'appliquer"
    bloc = repare.split("+++")[1]
    assert tomllib.loads(bloc)["title"] == titre, repare


def test_les_chaines_multilignes_ne_sont_JAMAIS_touchees() -> None:
    """`raw_head = '''` n'est pas une chaine litterale d'une ligne, et la requoter detruit tout.

    Mesure du 16/09/2026 : la premiere version de la reparation corrompait `raw_head` et
    `raw_body`, `tomllib` refusait toujours, la fonction rendait donc l'original — et hugo
    continuait de refuser ses fichiers. Le controle final a evite le degat, mais la reparation
    ne servait jamais sur la seule stack pour laquelle elle existe.
    """
    repare, notes = app_module._requote_toml_apostrophes(_fichier(f"title = '{TITRE}'"), CHEMIN)
    assert notes, "la reparation aurait du s'appliquer"
    bloc = repare.split("+++")[1]
    lu = tomllib.loads(bloc)
    assert lu["title"] == TITRE
    assert "viewport" in lu["raw_head"], lu["raw_head"]
    assert "<p>" in lu["raw_body"], lu["raw_body"]
    assert lu["html_attrs"] == ' lang="fr"', lu["html_attrs"]
    assert "raw_head = '''" in repare, "l'ouverture multiligne a ete reecrite"


def test_un_fichier_qui_n_est_pas_du_front_matter_est_ignore() -> None:
    page = "<html><head><title>Parcours d'obstacles</title></head></html>"
    sortie, notes = app_module._requote_toml_apostrophes(page, "public/index.html")
    assert sortie == page and notes == [], notes
