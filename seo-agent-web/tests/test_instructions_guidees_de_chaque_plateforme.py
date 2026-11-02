"""Chaque plateforme proposée a ses instructions, traduites, et la carte « Autres plateformes » la nomme.

Étape 7 des intégrations CMS (02/11/2026) : Systeme.io n'expose pas ces champs dans son API et
Squarespace non plus ; Shopify attend une appli officielle (facturation Shopify). Ils sont guidés :
le client corrige dans sa plateforme, Noyaru contrôle le résultat."""
from __future__ import annotations

from pathlib import Path

import pytest

from backend import cms_guidance, cms_routes, i18n

TEMPLATES = Path(__file__).resolve().parents[1] / "templates"
SANS_CMS = {"", "code", "other"}
GUIDEES = sorted(set(cms_guidance.PLATFORMS) - SANS_CMS - set(cms_routes.CONNECTEURS))


@pytest.mark.parametrize("plateforme", sorted(set(cms_guidance.PLATFORMS) - SANS_CMS))
@pytest.mark.parametrize("anomalie", ["missing_title", "meta_description_too_long", "missing_h1"])
def test_chaque_plateforme_a_des_instructions_traduites(plateforme, anomalie):
    guide = cms_guidance.for_issue({"cms": {"platform": plateforme}}, anomalie)
    assert guide["platform"] == plateforme and len(guide["steps"]) >= 3
    sans_traduction = [etape for etape in guide["steps"] if i18n.translate(etape, "en") == etape]
    assert not sans_traduction


def test_les_instructions_disent_le_bon_champ():
    for plateforme in ("squarespace", "systemeio"):
        titre = " ".join(cms_guidance.for_issue({"cms": {"platform": plateforme}}, "missing_title")["steps"])
        description = " ".join(cms_guidance.for_issue({"cms": {"platform": plateforme}}, "missing_meta_description")["steps"])
        assert "Titre" in titre and "Description" not in titre
        assert "Description" in description


def test_la_carte_autres_plateformes_nomme_chaque_plateforme_guidee():
    reglages = (TEMPLATES / "cms_settings.html").read_text(encoding="utf-8")
    carte = reglages[reglages.index('tr("Autres plateformes")'):]
    carte = carte[:carte.index("</section>")]
    assert GUIDEES and [cms_guidance.PLATFORMS[p] for p in GUIDEES if cms_guidance.PLATFORMS[p] not in carte] == []
