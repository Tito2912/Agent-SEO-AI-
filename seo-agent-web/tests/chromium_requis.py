# -*- coding: utf-8 -*-
"""Les témoins qui font rendre une vraie page par l'auditeur ont besoin d'un Chromium installé.

12/10/2026 : deux témoins du gauntlet (canonical HTTP, données structurées) passaient sur un poste
où `playwright install chromium` avait été lancé, et échouaient sur la CI GitHub, qui n'installe
aucun navigateur — « missing_tracked_http_positive_control », « actual_crawler_control_lost ».
Reproduit à l'identique en pointant PLAYWRIGHT_BROWSERS_PATH sur un dossier vide. Comme les
fixtures absentes ou PostgreSQL, un prérequis d'environnement manquant fait IGNORER le test, il
ne le fait pas échouer ; il tourne partout où le navigateur est là (production, poste de dev).
"""
from __future__ import annotations

from pathlib import Path

import pytest


def chromium_installe() -> bool:
    try:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            return Path(p.chromium.executable_path).is_file()
    except Exception:
        return False


requires_chromium = pytest.mark.skipif(
    not chromium_installe(), reason="requires an installed Playwright Chromium (python -m playwright install chromium)")
