# -*- coding: utf-8 -*-
"""La politique de confidentialité nomme CHAQUE service tiers que le code appelle.

Réécrite le 29/09/2026 (demande du propriétaire). L'ancienne était un « modèle de base à
compléter » publié tel quel : ni les fournisseurs d'IA, ni Google, ni GitHub, ni SerpAPI, un
prestataire d'e-mail faux (SendGrid au lieu de Brevo), aucun mot des transferts hors UE — alors
que l'hébergement est aux États-Unis.

LA LISTE SE DÉRIVE DU CODE. Chaque hôte `https://…` écrit dans le serveur ou le crawler doit
correspondre à un prestataire de la page, ou être explicitement écarté ici avec sa raison. Une
intégration ajoutée demain sans mise à jour de la page fait échouer ce test : c'est la seule
façon qu'une page juridique ne se périme pas en silence.

Au passage, la première version de la page listait Reddit : le code ne l'appelle JAMAIS — ses
recherches passent par SerpAPI avec `site:reddit.com`. Une page de confidentialité qui nomme un
tiers inexistant n'est pas plus exacte qu'une qui en oublie un.
"""
from __future__ import annotations

import os
import re
import sys
import tempfile
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
os.environ.setdefault("SEO_AGENT_DATA_DIR", tempfile.mkdtemp(prefix="seo-agent-privacy-"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", tempfile.mkdtemp(prefix="seo-agent-privacy-runs-"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as m  # noqa: E402

SOURCES = sorted((WEB_ROOT / "backend").glob("*.py")) + sorted(
    (WEB_ROOT.parent / "skills" / "public" / "seo-autopilot" / "scripts").glob("*.py"))

# Hôte appelé -> nom qui doit figurer sur la page.
TIERS = {
    "api.anthropic.com": "Anthropic",
    "api.openai.com": "OpenAI",
    "serpapi.com": "SerpAPI",
    "api.github.com": "GitHub",
    "www.bing.com": "Microsoft Bing",
    "searchconsole.googleapis.com": "Google",
    "oauth2.googleapis.com": "Google",
    "accounts.google.com": "Google",
    "www.googleapis.com": "Google",
    "api.netlify.com": "Netlify",
    "api.brevo.com": "Brevo",
    "checkout.stripe.com": "Stripe",
    "billing.stripe.com": "Stripe",
}
# Appelés seulement quand leur clé est configurée : la page les nomme dans ce cas-là.
CONDITIONNELS = {"api.ahrefs.com": ("Ahrefs", "AHREFS_API_TOKEN"),
                 "generativelanguage.googleapis.com": ("Gemini", "GOOGLE_GEMINI_API_KEY")}
# Écartés, avec leur raison.
ECARTES = {
    # Transports d'e-mail que le code SAIT utiliser mais qui ne le sont pas : Brevo l'est.
    "api.sendgrid.com", "api.resend.com",
    # Liens affichés ou documentation, pas des appels.
    "github.com", "app.netlify.com", "console.cloud.google.com", "developers.google.com",
    "schema.org", "www.cnil.fr",
    # Le produit lui-même, et les exemples de docstrings et de tests.
    "noyaru.com", "example.com", "app.example.com", "exemple.fr", "site.fr", "oryvalo.com",
}


def _hotes_du_code() -> set[str]:
    hotes = set()
    for f in SOURCES:
        for h in re.findall(r"https://([a-z0-9][a-z0-9.-]*\.[a-z]{2,})", f.read_text(encoding="utf-8", errors="replace")):
            hotes.add(h.lower())
    return hotes


def test_chaque_hote_appele_est_DECLARE_ou_ecarte_avec_sa_raison() -> None:
    inconnus = sorted(_hotes_du_code() - set(TIERS) - set(CONDITIONNELS) - ECARTES)
    assert not inconnus, (
        "le code appelle des services absents de la politique de confidentialité : %s — "
        "ajoutez-les à la page (et à TIERS), ou écartez-les ici avec leur raison" % inconnus)


def test_la_liste_des_hotes_n_est_pas_VIDE() -> None:
    """Le témoin : un glob qui ne trouve rien rendrait le test précédent vrai pour rien."""
    assert {"api.anthropic.com", "serpapi.com", "api.github.com"} <= _hotes_du_code()


def _page() -> str:
    return TestClient(m.app).get("/privacy").text


def _lignes_prestataires(page: str) -> list[str]:
    """Les en-tetes de ligne du tableau des prestataires. Chercher le nom N'IMPORTE OU sur la
    page ne suffisait pas : la mutation qui retirait la ligne Netlify survivait, parce que le mot
    restait dans le texte d'une autre case."""
    tableau = page.split("Nos prestataires", 1)[1].split("</table>", 1)[0]
    return re.findall(r'<th scope="row">([^<]+)</th>', tableau)


def test_chaque_tiers_appele_est_NOMME_sur_la_page() -> None:
    page = _page()
    lignes = _lignes_prestataires(page)
    absents = sorted({nom for nom in TIERS.values() if not any(nom in l for l in lignes)})
    assert not absents, (absents, lignes)
    assert "Render" in page and "États-Unis" in page, "l'hébergement et son pays"
    assert "Sentry" in page


def test_un_tiers_INEXISTANT_n_est_pas_nomme() -> None:
    assert "Reddit" not in _page(), "Reddit n'est jamais appelé : ses recherches passent par SerpAPI"


@pytest.mark.parametrize("hote", sorted(CONDITIONNELS))
def test_un_tiers_CONDITIONNEL_n_apparait_que_s_il_est_branche(monkeypatch, hote) -> None:
    nom, variable = CONDITIONNELS[hote]
    for v in ("AHREFS_API_TOKEN", "AHREFS_TOKEN", "AHREFS_API_KEY", "AHREFS_KEY", "cle_api", "GOOGLE_GEMINI_API_KEY"):
        monkeypatch.delenv(v, raising=False)
    assert not any(nom in l for l in _lignes_prestataires(_page()))
    monkeypatch.setenv(variable, "cle-de-test")
    assert any(nom in l for l in _lignes_prestataires(_page()))


@pytest.mark.parametrize("dsn, attendu", [
    ("", "Non utilisé actuellement"),
    ("https://abc@o1.ingest.de.sentry.io/2", "Union européenne (Allemagne)"),
    ("https://abc@o1.ingest.us.sentry.io/2", "États-Unis"),
])
def test_la_region_Sentry_se_LIT_dans_le_DSN(monkeypatch, dsn, attendu) -> None:
    monkeypatch.setenv("SENTRY_DSN", dsn)
    assert m._localisation_sentry() == attendu


def test_l_identite_legale_et_les_sauvegardes_sont_DECLAREES(monkeypatch) -> None:
    monkeypatch.delenv("LEGAL_ENTITY", raising=False)
    monkeypatch.delenv("LEGAL_BACKUP_LOCATION", raising=False)
    page = _page()
    assert "Précisée sur demande" in page, "sans déclaration, la page ne devine pas"
    monkeypatch.setenv("LEGAL_ENTITY", "Exemple SAS, 1 rue de Test, 75000 Paris")
    monkeypatch.setenv("LEGAL_BACKUP_LOCATION", "France")
    page = _page()
    assert "Exemple SAS, 1 rue de Test" in page and ">France</td>" in page


def test_la_date_est_celle_du_DOCUMENT_pas_du_jour() -> None:
    """L'ancienne page affichait la date du jour à chaque visite (`_legal_updated_at`)."""
    page = _page()
    assert "Mise à jour le 29 septembre 2026" in page
    assert "modèle de base à compléter" not in page
