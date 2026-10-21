"""Les derniers écrans et messages traduits (docs/I18N.md, lot 16)."""
from __future__ import annotations

import pytest
from bs4 import BeautifulSoup

from backend import i18n
from test_customer_activation import client, lab as lab, project, user


def test_the_analysis_history_reads_in_english(lab) -> None:
    owner = user(lab['db'])
    site = project(lab['db'], owner)
    page = client(owner).get(f'/projects/{site.slug}/crawls', params={'lang': 'en'})
    assert page.status_code == 200
    text = BeautifulSoup(page.text, 'html.parser').select_one('main').get_text(' ')
    assert 'Analysis history' in text and 'No analysis available for this site.' in text
    assert 'Historique des analyses' not in text and 'Aucune analyse' not in text


@pytest.mark.parametrize('message', [
    'Projet ajouté.',
    'Plan mis à jour. La différence a été facturée.',
    'Quota crawl mensuel atteint. Va sur Abonnement pour upgrade.',
    'Tape DELETE pour confirmer.',
    'Aucun projet n’est connecté à Google Search Console. Connectez-en un dans « Mon compte → Connexions ».',
    'Une PR est déjà ouverte pour cette anomalie.',
    'Email non vérifié. Réessaie dans 30s.',
    'Reconnecte Google pour activer le suivi.',
])
def test_messages_sent_back_to_customers_have_an_english_version(message: str) -> None:
    assert i18n.translate(message, 'en') != message
