"""On ne retourne pas la langue d'un site sans regarder le site.

Mesure sur le passage des neuf stacks : sur next-app, `served_html_lang_mismatch` a modifie
`app/layout.tsx` — la RACINE PARTAGEE — et fait passer tout le site de `lang="fr"` a
`lang="en"`. Le crawl disait pourtant, page par page : **34 pages en `fr`, 2 en `en`**. Les deux
`en` etaient le defaut injecte. Le correcteur a aligne la verite du site sur le mensonge d'une
page.

Viser le gabarit n'etait PAS l'erreur : sur un framework, `<html lang>` ne vit nulle part
ailleurs, et `_SHARED_RENDER_FIX_KEYS` l'autorise a juste titre. Sur Jekyll, `_layouts/default.html`
est partage ET c'est l'endroit ou la correction doit avoir lieu — un test l'affirme depuis
longtemps. Le fichier ne dit donc rien : ce qui distingue les deux cas, c'est QUI detient la
verite, et cela se lit dans le crawl, pas dans le fichier.

PREMIERE TENTATIVE, FAUSSE, gardee ici en garde : « dans un fichier partage, ne jamais
RETOURNER un lang existant ». Elle cassait Jekyll. La regle juste s'appuie sur un fait MESURE —
la langue dominante des pages crawlees — et reste inerte quand ce fait manque.
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


def _pages(fr: int, en: int) -> list[dict]:
    return ([{"url": f"https://s.fr/{i}", "lang": "fr"} for i in range(fr)]
            + [{"url": f"https://s.fr/e{i}", "lang": "en"} for i in range(en)])


def test_the_dominant_language_is_read_from_the_crawl() -> None:
    """Le cas next-app exact : 34 pages francaises, 2 anglaises."""
    assert app_module._dominant_site_lang(_pages(34, 2)) == "fr"


def test_a_site_without_a_clear_majority_yields_nothing() -> None:
    """Un site vraiment bilingue n'a pas de langue a defendre — on s'abstient."""
    assert app_module._dominant_site_lang(_pages(5, 5)) == ""


def test_too_few_pages_yield_nothing() -> None:
    """Deux pages ne font pas une mesure."""
    assert app_module._dominant_site_lang(_pages(2, 0)) == ""


def test_no_pages_yield_nothing() -> None:
    """Le rapport du test Jekyll ne porte AUCUN tableau de pages. Sans fait mesure, le
    garde-fou doit etre inerte — sinon il casse une correction legitime."""
    assert app_module._dominant_site_lang(None) == ""
    assert app_module._dominant_site_lang([]) == ""


LAYOUT_FR = '    <html lang="fr">\n      <body>{children}</body>\n'


def test_contradicting_the_site_is_reverted() -> None:
    new = LAYOUT_FR.replace('lang="fr"', 'lang="en"')
    out, notes = app_module._keep_site_lang(new, LAYOUT_FR, "fr")
    assert 'lang="fr"' in out and 'lang="en"' not in out
    assert notes


def test_agreeing_with_the_site_is_allowed() -> None:
    """Le cas Jekyll : le gabarit a tort, le crawl dit `fr`, la correction passe."""
    old = LAYOUT_FR.replace('lang="fr"', 'lang="en"')
    out, notes = app_module._keep_site_lang(LAYOUT_FR, old, "fr")
    assert out == LAYOUT_FR and notes == []


def test_without_a_measured_language_nothing_is_reverted() -> None:
    new = LAYOUT_FR.replace('lang="fr"', 'lang="en"')
    out, notes = app_module._keep_site_lang(new, LAYOUT_FR, "")
    assert out == new and notes == []


def test_a_regional_variant_of_the_site_language_is_allowed() -> None:
    """`fr` -> `fr-FR` precise sans contredire."""
    new = LAYOUT_FR.replace('lang="fr"', 'lang="fr-FR"')
    out, notes = app_module._keep_site_lang(new, LAYOUT_FR, "fr")
    assert out == new and notes == []


def test_the_guard_is_wired_for_shared_files_only() -> None:
    import inspect
    src = inspect.getsource(app_module._deep_patch_issue_files)
    assert "_keep_site_lang" in src
    assert "is_shared_path" in src
