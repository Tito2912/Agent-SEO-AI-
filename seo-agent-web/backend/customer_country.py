"""Self-declared country only: no inference from IP, browser, language or email."""
from __future__ import annotations

import json
import unicodedata
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

# Country names from iso-codes (LGPL-2.1+, attribution in docs/licenses/iso-codes.txt).
COUNTRIES = json.loads(Path(__file__).with_name('countries_fr.json').read_text(encoding='utf-8'))
OPTIONS = sorted(COUNTRIES.items(), key=lambda item: unicodedata.normalize('NFKD', item[1]).casefold())


def normalize(value):
    code = str(value or '').strip().upper()
    return code if code in COUNTRIES else None


def with_country(url, country):
    code = normalize(country)
    if not code:
        return url
    parts = urlsplit(url)
    query = [(key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True) if key != 'country']
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query + [('country', code)]), parts.fragment))


def verification_mode(module):
    """Current policy, not an assertion about historical settings or mail delivery."""
    try:
        enabled = module._email_verification_enabled()
        disabled = module._env_bool('EMAIL_VERIFICATION_DISABLED')
    except Exception:
        return {'key': 'unknown', 'label': 'Diagnostic de vérification indisponible',
                'detail': 'Le réglage actuel n’a pas pu être lu. Réessayez dans quelques instants.'}
    if enabled:
        return {'key': 'required', 'label': 'Vérification demandée à l’inscription',
                'detail': 'Les nouveaux comptes par email doivent valider leur adresse pour accéder au service.'}
    if disabled:
        return {'key': 'disabled', 'label': 'Vérification désactivée par réglage',
                'detail': 'Le réglage actuel autorise les comptes par email à entrer sans validation de leur adresse.'}
    return {'key': 'unconfigured', 'label': 'Vérification inactive : aucun service d’envoi configuré',
            'detail': 'Le réglage actuel autorise les comptes par email à entrer sans validation de leur adresse.'}
