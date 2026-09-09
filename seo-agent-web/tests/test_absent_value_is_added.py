"""Une valeur ABSENTE n'est pas une valeur COURTE.

Trouve par le premier passage complet du correcteur sur le parcours static-html, mesure sur la
previsualisation : la page `gauntlet/missing-meta-description` ressort SANS description apres
correction, alors qu'elle etait la PREMIERE cible de sa famille. Le ciblage n'etait pas en
cause.

Les deux moities du produit ne se rencontraient jamais :

* le crawler ne leve `missing_meta_description` que sur une page NON indexable — sur une page
  indexable il replie le cas dans `meta_description_too_short_indexable` (parite Ahrefs) ;
* cette cle appartient au groupe des familles de LONGUEUR, dont le correcteur borne
  `_rewrite_length_values` localise la valeur existante pour la remplacer sur place. Il ne peut
  rien faire d'une valeur qui n'existe pas, et il a raison : ajouter une balise demande de
  savoir OU, ce qui depend de la stack. Le repli IA est le bon outil.

Sauf que l'indice passe au repli decrivait la page comme « meta description RENDU actuel
(0 car.) » — ce qu'un modele lit comme une description a raccourcir, pas comme une balise a
poser. L'echantillon du crawler portait pourtant le fait exact (`len=0`, `rendered=''`).

Encore le meme motif : le correcteur echoue la ou on ne lui a pas donne un fait que le crawler
avait deja mesure — ici, que la valeur est ABSENTE et non COURTE.
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

URL = "https://exemple.fr/gauntlet/sans-description"


def _issues(rendered: str, length: int) -> dict:
    return {"meta_description_too_short_indexable": {
        "count": 1, "length_samples": {URL: {"rendered": rendered, "len": length}}}}


def _hint(rendered: str, length: int) -> str:
    return app_module._build_length_hint(
        _issues(rendered, length),
        app_module._length_family_keys("meta_description_too_short_indexable"),
        "meta")


def test_an_absent_description_is_announced_as_absent() -> None:
    """Le repli IA doit lire « il n'y en a pas », pas « elle fait zero caractere »."""
    hint = _hint("", 0)
    assert URL in hint, "la page ne figure meme pas dans l'indice"
    low = hint.lower()
    assert "aucune" in low or "absente" in low, f"l'indice ne dit pas que la balise manque : {hint}"
    assert "ajoute" in low, f"l'indice ne demande pas d'AJOUTER la balise : {hint}"


def test_the_window_is_still_given_for_an_absent_value() -> None:
    """Ajouter sans dire quelle longueur viser rendrait la correction inutile."""
    low, high = app_module._LENGTH_WINDOWS["description"]
    hint = _hint("", 0)
    assert str(low) in hint and str(high) in hint


def test_a_short_but_present_value_is_not_treated_as_absent() -> None:
    """Une valeur courte se REECRIT ; la confondre avec une absente ferait poser une seconde
    balise a cote de la premiere, soit `multiple_meta_description_tags` en echange."""
    hint = _hint("Une description courte.", 23)
    low = hint.lower()
    assert "aucune" not in low and "ajoute" not in low
    assert "Une description courte." in hint


def test_the_bounded_rewriter_still_refuses_an_absent_value(monkeypatch) -> None:
    """Il ne sait pas OU poser la balise, et cela depend de la stack : neuf idiomes, neuf
    endroits. Son refus est correct — c'est l'indice du repli qui devait changer, pas lui."""
    monkeypatch.setattr(app_module, "_length_value_for_page",
                        lambda **kw: (_ for _ in ()).throw(AssertionError("ne doit pas etre appele")))
    html = "<html><head><title>Un titre</title></head><body><h1>x</h1></body></html>"
    out, n = app_module._rewrite_length_values(html, {URL: {"rendered": "", "len": 0}}, "meta")
    assert n == 0 and out == html
