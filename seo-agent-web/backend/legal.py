"""Les informations juridiques de Noyaru : l'éditeur, le médiateur, l'hébergeur, les dates.

Révisées avec l'audit juridique d'octobre 2026 (docs/legal/LEGAL-CHANGES.md). Elles vivaient dans
des variables d'environnement (`LEGAL_*`) : une variable vide faisait disparaître une mention
obligatoire des pages, sans que personne le voie. Elles sont désormais écrites ici, une fois, et
les six pages juridiques (français et anglais) les lisent.

Les valeurs ci-dessous sont celles que noyaru.com publiait le 20/10/2026 (mentions légales et CGU),
déclarées par le propriétaire. Une valeur encore inconnue vaut None : sa ligne n'est pas affichée,
`missing()` la nomme, et docs/legal/LEGAL-VALIDATION-REQUIRED.md dit qu'elle bloque la publication.
Ne jamais remplacer None par une valeur supposée.
"""
from __future__ import annotations

from datetime import date
from typing import Any

PUBLISHER: dict[str, str | int | None] = {
    "name": "E-Com Shop",
    "form": "SASU",
    # En euros : celui des statuts du 30/10/2024, confirmé par le propriétaire le 24/10/2026 (le
    # 10 000 € publié du 21 au 24/10/2026 était faux). Écrit dans la langue de la page par
    # share_capital().
    "share_capital": 1,
    "address": "60 rue François 1er, 75008 Paris, France",
    "registration": "934 934 308 R.C.S. Paris",
    "vat_number": "FR18934934308",
    "publication_director": "Tony Caron",
    "phone": "06 14 97 64 01",
}

# Le médiateur de la consommation (art. L612-1 du Code de la consommation), tel que les CGU le
# nommaient le 20/10/2026. L'adhésion d'E-Com Shop reste à confirmer (LEGAL-VALIDATION-REQUIRED.md).
MEDIATOR: dict[str, str] = {
    "name": "CM2C (Centre de la Médiation de la Consommation de Conciliateurs de Justice)",
    "address": "49 rue de Ponthieu, 75008 Paris, France",
    "url": "https://www.cm2c.net/declarer-un-litige.php",
}

# L'hébergeur, tel que Render se nomme dans ses propres conditions (render.com/terms).
HOST: dict[str, str] = {
    "name": "Render Services, Inc.",
    "address": "525 Brannan Street, Suite 300, San Francisco, CA 94107",
    "phone": "+1 415-319-8186",
    "url": "https://render.com",
}

# La date de chaque document : celle de sa publication (validée par le propriétaire le
# 21/10/2026), jamais celle du jour de la visite. À changer à chaque nouvelle version du texte.
UPDATED: dict[str, date] = {
    "terms": date(2026, 10, 24),
    "privacy": date(2026, 10, 21),
    "legal_notice": date(2026, 10, 24),
}

# Version de l'avertissement montré avant d'activer une fusion ou une publication automatique
# (CGU, articles 6 et 7 ; crawl_settings.html, corrections.html, content.html). Elle est gardée
# dans le journal d'audit avec chaque activation : à changer quand ce texte change.
AUTO_MODE_WARNING_VERSION = "2026-10-21"

_MOIS = ("janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août", "septembre",
         "octobre", "novembre", "décembre")
_MONTHS = ("January", "February", "March", "April", "May", "June", "July", "August", "September",
           "October", "November", "December")


def missing() -> list[str]:
    """Les mentions de l'éditeur encore inconnues, qui bloquent la publication."""
    return [key for key, value in PUBLISHER.items() if not value]


def identity() -> str:
    """« E-Com Shop, SASU, 60 rue François 1er, 75008 Paris, France, 934 934 308 R.C.S. Paris »."""
    parts = [PUBLISHER[k] for k in ("name", "form", "address", "registration")]
    return ", ".join(p for p in parts if p)


def share_capital(lang: str) -> str | None:
    """Le capital social, écrit dans la langue de la page : « 10 000 € », "€10,000" ; « 1 € », "€1"."""
    amount = PUBLISHER["share_capital"]
    if not amount:
        return None
    if lang == "en":
        return f"€{amount:,}"
    return f"{amount:,}".replace(",", "\u00a0") + "\u00a0€"


def format_date(day: date, lang: str) -> str:
    """Une date écrite dans sa langue : « 20 octobre 2026 », "October 20, 2026"."""
    if lang == "en":
        return f"{_MONTHS[day.month - 1]} {day.day}, {day.year}"
    return f"{'1er' if day.day == 1 else day.day} {_MOIS[day.month - 1]} {day.year}"


def updated(document: str, lang: str) -> str:
    """La date d'un document, écrite dans sa langue."""
    return format_date(UPDATED[document], lang)


def phone_link(number: str) -> str:
    """`06 14 97 64 01` -> `0614976401` : un lien `tel:` ne porte ni espace ni point."""
    return "".join(c for c in number if c.isdigit() or c == "+")


def context(lang: str) -> dict[str, Any]:
    """Ce que les gabarits juridiques reçoivent, dans la langue de la page."""
    return {
        "publisher": PUBLISHER,
        "publisher_identity": identity(),
        "share_capital": share_capital(lang),
        "publisher_phone_link": phone_link(PUBLISHER["phone"] or ""),
        "mediator": MEDIATOR,
        "host": HOST,
        "updated": {key: updated(key, lang) for key in UPDATED},
    }
