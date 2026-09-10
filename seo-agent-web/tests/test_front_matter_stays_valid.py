"""On ne commite pas un front matter que son propre format refuse de lire.

Mesure sur le cycle complet du 10/09/2026 : le deploiement Hugo echouait, et trois fichiers sur
trente-deux avaient un front matter TOML invalide —

    title = 'Page de test du parcours d'obstacles Noyaru'

l'apostrophe termine la chaine litterale. Meme cause qu'en JavaScript, mais dans un format ou le
garde-fou d'echappement ne s'applique pas et ne DOIT pas s'appliquer : une chaine litterale TOML
n'accepte aucun echappement, `\\'` y serait pris au pied de la lettre et se retrouverait dans le
titre servi.

C'est la QUATRIEME facon dont le modele casse un fichier, trouvee en un jour, apres l'apostrophe
non echappee, la cle dupliquee et la virgule manquante. Guarder chaque forme d'erreur une par
une est une course perdue.

Ici on ne devine rien : le front matter TOML se lit avec `tomllib`, le YAML avec `pyyaml` — les
VRAIS analyseurs des formats, tous deux disponibles en production. Ce qui ne parse pas n'est pas
commite. Un refus se voit ; un deploiement casse, non.
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

from backend import app as app_module  # noqa: E402

TOML_OK = "+++\ntitle = 'Un titre sans apostrophe'\nurl = \"/a/\"\n+++\n\nCorps.\n"
# Le cas HUGO exact, tel qu'il a fait echouer le deploiement.
TOML_KO = "+++\ntitle = 'Page de test du parcours d'obstacles Noyaru'\nurl = \"/a/\"\n+++\n"
YAML_OK = "---\nlayout: gauntlet\ntitle: Un titre\n---\n\n<p>Corps.</p>\n"
YAML_KO = "---\nlayout: gauntlet\ntitle: 'valeur non fermee\n---\n"


def test_valid_toml_passes() -> None:
    assert app_module._front_matter_parse_error(TOML_OK, "content/a.md") == ""


def test_the_hugo_case_is_caught() -> None:
    err = app_module._front_matter_parse_error(TOML_KO, "content/gauntlet/duplicate-b.md")
    assert err, "le front matter invalide de Hugo n'est pas detecte"
    assert "toml" in err.lower()


def test_valid_yaml_passes() -> None:
    assert app_module._front_matter_parse_error(YAML_OK, "gauntlet/a.html") == ""


def test_invalid_yaml_is_caught() -> None:
    assert app_module._front_matter_parse_error(YAML_KO, "gauntlet/a.html")


def test_a_file_without_front_matter_is_not_our_business() -> None:
    assert app_module._front_matter_parse_error("<p>Juste du HTML.</p>\n", "a.html") == ""


def test_an_astro_file_is_never_read_as_yaml() -> None:
    """Un `.astro` commence par `---` et contient du JAVASCRIPT. Le lire comme du YAML le
    ferait refuser a tort — le genre de faux positif qui bloque une correction valide."""
    astro = "---\nconst canonical = 'https://s.fr/a';\nimport Base from '../B.astro';\n---\n<p>x</p>\n"
    assert app_module._front_matter_parse_error(astro, "src/pages/a.astro") == ""


def test_a_javascript_file_is_out_of_scope() -> None:
    """Le JS a ses propres garde-fous ; ce controle-ci ne parle que des formats a front matter."""
    assert app_module._front_matter_parse_error("export const metadata = {\n", "app/page.tsx") == ""


def test_the_check_refuses_the_file_before_commit() -> None:
    import inspect
    src = inspect.getsource(app_module._deep_patch_issue_files)
    assert "_front_matter_parse_error(new_content, path)" in src
    assert src.index("_front_matter_parse_error") < src.index("put_body")
