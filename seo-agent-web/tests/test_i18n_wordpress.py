"""Le parcours WordPress en anglais : lot d'une anomalie, aperçu, application et suivi."""
from __future__ import annotations

from backend import app as m
from test_wordpress_lot_d_une_anomalie import URL, _changes, _envoyer, _proposer, _valider, journey as journey, lot as lot


def _english(data, path):
    page = data["client"].get(path, params={"lang": "en"})
    assert page.status_code == 200
    return page.text


def test_wordpress_batch_and_change_screens_in_english(lot):
    batch = _english(lot, lot["lot"])
    assert "1. Have the texts written" in batch and "Status to confirm" in batch
    assert "Faire rédiger les textes" not in batch

    assert _proposer(lot, URL)["ok"]
    review = _english(lot, lot["lot"])
    assert "2. Review and save" in review and "suggested by Noyaru" in review

    _valider(lot, [URL])
    change = _changes(lot)[URL]
    tracking = _english(lot, f'/projects/{lot["slug"]}/cms/wordpress/changes/{change.id}')
    assert "Application confirmed" in tracking
    assert "Rétablir la valeur précédente" not in tracking and "Restore the previous value" in tracking

    settings = _english(lot, lot["cms"])
    assert "Which platform is your site on?" in settings and "Disconnect WordPress" in settings


def test_wordpress_flash_messages_are_translated(lot):
    response = _envoyer(lot, lot["cms"] + "/platform", platform="shopify", seo_plugin="")
    assert response.status_code == 303
    page = lot["client"].get(response.headers["location"] + "&lang=en").text
    assert "Platform saved. The instructions are tailored to your site." in page
    assert m.i18n.translate("3 page(s) enregistrée(s) dans WordPress.", "en") == "3 page(s) saved to WordPress."
