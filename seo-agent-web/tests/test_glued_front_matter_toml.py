"""Le garde-fou de l'espace collee ne connaissait que le YAML : hugo livrait `<htmllang="fr">`.

Il a ete ecrit le 13/09/2026 pour jekyll, dont le front matter est en YAML (`cle: valeur`). Hugo
ecrit le sien en TOML (`cle = valeur`), et le motif exigeait le deux-points — donc le controle se
TAISAIT sur hugo, silencieusement, depuis le jour de sa naissance.

Mesure du 17/09/2026, branche hugo, page `html-lang-missing` : charge d'ajouter l'attribut lang,
le modele a ecrit

    html_attrs = 'lang="fr"'

sans espace initiale. Le gabarit fait `<html{{ .Params.html_attrs }}>`, donc la page servie porte
`<htmllang="fr">` : la balise s'appelle desormais `htmllang`, le document n'a plus d'element
`<html>`, et `html_lang_attribute_missing` restait a 2 pour un verdict « ok ».

ET LA REECRITURE NAIVE AURAIT ETE PIRE QUE LE SILENCE : rendre `cle = 'valeur'` sous la forme
`cle: 'valeur'` casse le front matter TOML, donc la construction du site. Un garde-fou qui repare
dans la mauvaise syntaxe transforme une anomalie en panne. Le separateur est donc CAPTURE et rendu
tel quel.
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

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-glued-toml-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402

COLLE = "html_attrs = 'lang=\"fr\"'"
VIDE = "html_attrs = ''"


def _toml(ligne: str) -> str:
    return "+++\n" + 'url = "/gauntlet/html-lang-missing/"\n' + ligne + "\n+++\n\nLe corps.\n"


def _yaml(ligne: str) -> str:
    return "---\n" + 'url: "/gauntlet/html-lang-missing/"\n' + ligne + "\n---\n\nLe corps.\n"


def _ligne(contenu: str) -> str:
    return next(x for x in contenu.split("\n") if x.startswith("html_attrs"))


def test_le_cas_mesure_sur_hugo_recupere_son_espace() -> None:
    sortie, notes = app_module._space_glued_front_matter(
        _toml(COLLE), _toml(VIDE), {"html_attrs"})
    assert notes, "le controle s'est tu sur du TOML, comme avant"
    assert _ligne(sortie) == 'html_attrs = \' lang="fr"\'', _ligne(sortie)


def test_le_TOML_reste_du_TOML_apres_reparation() -> None:
    """Reparer dans la mauvaise syntaxe casserait la construction du site — pire que le silence."""
    sortie, _ = app_module._space_glued_front_matter(_toml(COLLE), _toml(VIDE), {"html_attrs"})
    bloc = sortie.split("+++")[1]
    assert tomllib.loads(bloc)["html_attrs"] == ' lang="fr"'


def test_jekyll_n_a_pas_regresse() -> None:
    """Le format d'origine du garde-fou doit continuer de marcher, en YAML."""
    sortie, notes = app_module._space_glued_front_matter(
        _yaml("html_attrs: 'lang=\"fr\"'"), _yaml("html_attrs: ''"), {"html_attrs"})
    assert notes, notes
    assert _ligne(sortie) == 'html_attrs: \' lang="fr"\'', _ligne(sortie)


def test_une_valeur_qui_a_DEJA_son_espace_n_est_pas_retouchee() -> None:
    deja = "html_attrs = ' lang=\"fr\"'"
    sortie, notes = app_module._space_glued_front_matter(_toml(deja), _toml(VIDE), {"html_attrs"})
    assert notes == [] and _ligne(sortie) == deja, (notes, _ligne(sortie))


def test_une_clef_hors_liste_n_est_jamais_touchee() -> None:
    """On ne touche qu'une clef que les gabarits du depot injectent COLLEE."""
    sortie, notes = app_module._space_glued_front_matter(_toml(COLLE), _toml(VIDE), {"autre_clef"})
    assert notes == [] and _ligne(sortie) == COLLE, (notes, _ligne(sortie))


def test_une_valeur_QUE_CE_PATCH_N_A_PAS_ECRITE_est_laissee() -> None:
    """Si l'ancienne valeur est deja celle-la, ce patch ne l'a pas produite : on n'y touche pas."""
    sortie, notes = app_module._space_glued_front_matter(_toml(COLLE), _toml(COLLE), {"html_attrs"})
    assert notes == [] and _ligne(sortie) == COLLE, (notes, _ligne(sortie))
