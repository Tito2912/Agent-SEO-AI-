# -*- coding: utf-8 -*-
"""Une description allongee par le modele n'affirme rien que la page ne dise.

13/10/2026, PR #16 d'oryvalo : la description « trop courte » de /about est revenue allongee en
« …how we keep recommendations transparent, independent, and genuinely helpful ». La page ne dit
nulle part « independent » — elle precise au contraire qu'elle vit d'affiliation. Cause : pour
allonger une valeur, le modele ne recevait QUE la valeur et l'URL ; « developpe ce que la page
traite reellement » ne pouvait s'appuyer sur rien.

Il recoit desormais le contenu de la page, et une affirmation qu'elle ne porte pas (qualite qu'on
se donne, classement, gratuite, garantie…) fait relancer une fois, puis refuser.
"""
from __future__ import annotations

import json

import pytest

from backend import app as m

ABOUT = ("Oryvalo publishes practical, beginner-friendly guides about building online businesses with AI "
         "tools. Honest reviews and comparisons (when relevant). Some pages may contain affiliate links.")


@pytest.mark.parametrize("valeur, fond, attendu", [
    ("transparent, independent, and genuinely helpful", ABOUT, ["independent"]),
    ("our independent reviews", ABOUT + " We are fully independent.", []),
    ("Le meilleur comparatif gratuit des brokers", "Comparatif des brokers en ligne", ["gratuit", "meilleur"]),
    ("Die besten kostenlosen Tools", "Die besten Tools im Test", ["kostenlosen"]),
    ("Guides pratiques sur les outils d'IA", ABOUT, []),     # rien d'affirme : rien a refuser
])
def test_only_claims_the_page_does_not_make_are_reported(valeur, fond, attendu) -> None:
    assert m._affirmations_non_fondees(valeur, fond) == attendu


COURTE = "Learn more about Oryvalo and our approach."


def run(monkeypatch, reponses):
    appels = []

    def faux(**kw):
        appels.append(json.loads(kw["user_msg"]))
        return {"value": reponses[len(appels) - 1]}
    monkeypatch.setattr(m, "_correction_ai_json", faux)
    valeur = m._length_value_for_page(current=COURTE, kind="description", url="https://oryvalo.com/about",
                                      site_name="oryvalo.com", contexte=ABOUT)
    return valeur, appels


FONDEE = ("Learn more about Oryvalo: beginner-friendly guides on building online businesses with AI tools, "
          "honest reviews and comparisons, and our affiliate links.")
INVENTEE = ("Learn more about Oryvalo, our editorial approach, and how we keep recommendations transparent, "
            "independent, and genuinely helpful for every reader online.")


def test_the_model_sees_the_page_and_a_grounded_value_is_kept(monkeypatch) -> None:
    valeur, appels = run(monkeypatch, [FONDEE])
    assert valeur == FONDEE and appels[0]["contenu_de_la_page"].startswith("Oryvalo publishes")


def test_an_invented_claim_is_sent_back_once_then_the_grounded_answer_wins(monkeypatch) -> None:
    valeur, appels = run(monkeypatch, [INVENTEE, FONDEE])
    assert valeur == FONDEE and "independent" in appels[1]["probleme"]


def test_two_invented_answers_leave_the_value_alone(monkeypatch) -> None:
    valeur, _ = run(monkeypatch, [INVENTEE, INVENTEE])
    assert valeur == ""


def test_the_length_family_hands_the_page_text_to_the_model(monkeypatch) -> None:
    vus = {}

    def faux(**kw):
        vus.update(kw)
        return ""
    monkeypatch.setattr(m, "_length_value_for_page", faux)
    page = ("export const metadata = { description: '" + COURTE + "' };\n"
            "export default function About() { return <p>Oryvalo publishes practical guides.</p>; }\n")
    m._rewrite_length_values(page, {"https://oryvalo.com/about": {"rendered": COURTE, "len": len(COURTE)}},
                             "description", site_name="oryvalo.com")
    assert "Oryvalo publishes practical guides." in vus["contexte"] and "<p>" not in vus["contexte"]
