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
    # Renvoyées avec les données Search Console et Bing, affichées sous leurs graphiques (02/11/2026).
    'GSC a généralement 48–72h de décalage.',
    'Bing Webmaster Tools peut avoir un léger décalage.',
])
def test_messages_sent_back_to_customers_have_an_english_version(message: str) -> None:
    assert i18n.translate(message, 'en') != message

# Scripts des écrans d'administration, qui restent en français (docs/I18N.md, « Pas encore traduit »).
SCRIPTS_ADMINISTRATION = {'plan-counts.js'}


def test_customer_scripts_format_numbers_in_the_page_language() -> None:
    """02/11/2026 : « 3,2 k » impressions sur la carte Search Console anglaise. Le format des
    nombres suit la langue de la page (`<html lang>`), jamais un « fr-FR » écrit en dur."""
    import re
    from pathlib import Path

    fige = re.compile(r'''(NumberFormat|DateTimeFormat|toLocale\w*String)\(\s*['"]fr-FR['"]''')
    statique = Path(__file__).resolve().parents[1] / 'static'
    figes = sorted(p.name for p in statique.glob('*.js')
                   if p.name not in SCRIPTS_ADMINISTRATION and fige.search(p.read_text(encoding='utf-8')))
    assert figes == [], figes
