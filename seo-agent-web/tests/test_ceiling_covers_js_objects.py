"""Le plafond de longueur doit aussi tenir dans un objet `metadata` JS.

Depuis le 09/09/2026 le garde-fou de plafond est borne au bloc de front matter de tete, parce
que sa version d'avant CASSAIT les objets JavaScript : le groupe de valeur allait jusqu'en fin
de ligne et emportait le guillemet fermant et la virgule, livrant un fichier non parsable. Le
bornage etait le bon geste dans l'urgence, mais il laissait les titres de Next.js App Router,
d'Astro et de Nuxt SANS AUCUN filet — l'ecriture ou vivent les valeurs de la majorite des
clients modernes.

Ce qui rend la reprise possible aujourd'hui : les guillemets et la virgule restent HORS du
groupe remplace. La classe de corruption ne peut plus revenir, quoi que fasse la coupe.

Et un fait nouveau, qui n'existait pas ce matin : depuis le garde-fou d'echappement, une valeur
peut contenir `\\'`. C'est UN caractere a l'ecran et deux dans le fichier. Le mesurer comme deux
ferait couper des valeurs parfaitement legales, et couper au mauvais endroit laisserait un
antislash orphelin qui casse la chaine — soit exactement le defaut qu'on repare.
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

CEIL = app_module._LENGTH_CEILINGS["title"]
OLD = ("export const metadata = {\n"
       "  title: 'Ancien titre',\n"
       "  description: 'Ancienne description.',\n"
       "};\n")

LONG = ("Chaussures de randonnee impermeables pour homme et femme en cuir pleine fleur - "
        "Boutique Alpine")
# Exactement 70 caracteres : AU plafond, donc parfaitement legal.
AT_CEILING = "Chaussures de randonnee impermeables en cuir pour homme - Alpine Store"


def _line(text: str, needle: str) -> str:
    return next((ln for ln in text.splitlines() if needle in ln), "")


def test_an_over_long_title_in_a_js_object_is_trimmed() -> None:
    new = OLD.replace("Ancien titre", LONG)
    out, notes = app_module._enforce_length_ceilings(new, OLD)
    line = _line(out, "title:")
    assert app_module._rendered_len(line.split("'")[1]) <= CEIL
    assert notes


def test_the_quotes_and_the_comma_survive() -> None:
    """Le defaut d'origine, en un mot : la ligne ressortait sans son guillemet ni sa virgule."""
    new = OLD.replace("Ancien titre", LONG)
    out, _ = app_module._enforce_length_ceilings(new, OLD)
    line = _line(out, "title:").rstrip()
    assert line.endswith("',"), f"chaine JavaScript non terminee : {line!r}"


def test_a_value_at_the_ceiling_is_not_trimmed() -> None:
    """Le second defaut du matin : guillemets et virgule comptes dans la longueur faisaient
    couper un titre de 70 caracteres, pourtant legal."""
    assert app_module._rendered_len(AT_CEILING) == CEIL
    new = OLD.replace("Ancien titre", AT_CEILING)
    out, notes = app_module._enforce_length_ceilings(new, OLD)
    assert out == new and notes == []


def test_an_escaped_apostrophe_counts_for_one_character() -> None:
    """`d\\'obstacles` est UN caractere a l'ecran. Le compter double ferait couper une valeur
    qui tient dans la fenetre."""
    value = "Le parcours d\\'obstacles Noyaru pour tous les niveaux de pratique en France"
    new = OLD.replace("Ancien titre", value)
    out, notes = app_module._enforce_length_ceilings(new, OLD)
    # 73 caracteres dans le fichier, 72 a l'ecran : au-dessus du plafond, donc coupe — mais la
    # mesure doit se faire sur le RENDU, pas sur la source.
    line = _line(out, "title:")
    assert "\\\\" not in line, "un antislash a ete double"
    assert line.rstrip().endswith("',")
    assert notes


def test_a_trim_never_leaves_a_lone_backslash() -> None:
    """Couper au milieu de `\\'` laisserait un antislash orphelin qui ouvre une echappee sur le
    guillemet fermant — le fichier ne compile plus."""
    value = "A" * 66 + " d\\'obstacles et bien davantage encore pour la boutique"
    new = OLD.replace("Ancien titre", value)
    out, _ = app_module._enforce_length_ceilings(new, OLD)
    inner = _line(out, "title:").split(": ", 1)[1].rstrip().rstrip(",").strip("'")
    assert not inner.endswith("\\"), f"antislash orphelin en fin de valeur : {inner!r}"


def test_a_value_the_patch_did_not_write_is_left_alone() -> None:
    already = OLD.replace("Ancien titre", LONG)
    out, notes = app_module._enforce_length_ceilings(already, already)
    assert out == already and notes == []


def test_markdown_prose_is_not_a_js_object() -> None:
    """Une ligne de prose `title: quelque chose` n'a pas de guillemets : elle n'est pas une
    entree d'objet et ne doit pas etre coupee."""
    old = "---\ntitle: Court\n---\n\nEcrivez la ligne suivante :\n\n    title: un exemple\n"
    new = old.replace("    title: un exemple",
                      "    title: un exemple de titre tres long recopie tel quel dans la doc du theme")
    out, _ = app_module._enforce_length_ceilings(new, old)
    assert "recopie tel quel dans la doc du theme" in out


def test_a_jsx_attribute_is_not_a_js_object_entry() -> None:
    old = "<meta name=\"x\" />\n"
    new = ("<script type=\"application/ld+json\" dangerouslySetInnerHTML={{ __html: "
           "`{\"title\":\"" + LONG + "\"}` }} />\n")
    out, notes = app_module._enforce_length_ceilings(new, old)
    assert out == new and notes == []
