# -*- coding: utf-8 -*-
"""Le site porte ses mentions légales, et la confidentialité nomme le stockage S3.

30/09/2026. noyaru.com n'avait pas de page de mentions légales, alors qu'un site professionnel
doit nommer son éditeur, son directeur de la publication et son hébergeur (LCEN, art. 6 III).
Audit juridique d'octobre 2026 : l'identité de l'éditeur était une déclaration (variables
`LEGAL_*` sur Render) dont l'absence effaçait une mention obligatoire. Elle est désormais écrite
dans backend/legal.py ; seule une valeur encore inconnue (None) n'est pas affichée, et elle est
nommée dans docs/legal/LEGAL-VALIDATION-REQUIRED.md.

Même jour : la politique de confidentialité ne nommait pas Amazon Web Services, alors que les
sauvegardes (backup.py) et, quand le service a un bucket, les rapports d'analyse (object_store.py)
partent dans S3. Le relevé des hôtes du test de confidentialité ne pouvait pas le voir : boto3
n'écrit aucune URL en clair. On lit donc l'import.
"""
from __future__ import annotations

import os
import re
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
os.environ.setdefault("SEO_AGENT_DATA_DIR", tempfile.mkdtemp(prefix="seo-agent-mentions-"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", tempfile.mkdtemp(prefix="seo-agent-mentions-runs-"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as m  # noqa: E402

LEGAL = {
    "LEGAL_ENTITY": "Exemple, SASU, 1 rue de Test, 75008 Paris, 123 456 789 R.C.S. Paris",
    "LEGAL_VAT_NUMBER": "FR00123456789",
    "LEGAL_PUBLICATION_DIRECTOR": "Camille Exemple",
    "LEGAL_PHONE": "06 12 34 56 78",
}


def _page(chemin: str = "/mentions-legales") -> str:
    r = TestClient(m.app).get(chemin, follow_redirects=False)
    assert r.status_code == 200, (chemin, r.status_code, r.headers.get("location"))
    return r.text


def test_la_page_est_PUBLIQUE_sans_connexion() -> None:
    """Deux listes de chemins publics dans l'application (session, puis accès bêta) : un chemin
    oublié dans l'une renvoie le visiteur vers la connexion."""
    _page()


def test_la_page_reste_publique_quand_l_acces_BETA_est_ferme(monkeypatch) -> None:
    monkeypatch.setattr(m, "_beta_basic_auth_expected", lambda: ("beta", "secret"))
    assert TestClient(m.app).get("/pricing").status_code == 200, "le témoin : /pricing est public"
    assert TestClient(m.app).get("/projects").status_code == 401, "le témoin : le reste est fermé"
    _page()


def test_chaque_mention_connue_est_affichee_dans_les_DEUX_langues() -> None:
    for chemin, lang in (("/mentions-legales", "fr"), ("/en/mentions-legales", "en")):
        page = _page(chemin)
        for cle, valeur in m.legal.PUBLISHER.items():
            if cle == "share_capital":
                valeur = m.legal.share_capital(lang)
            if valeur:
                assert valeur in page, (chemin, cle)
        assert 'href="tel:%s"' % m.legal.phone_link(m.legal.PUBLISHER["phone"]) in page, "un lien tel: sans espace"
        assert 'href="mailto:%s"' % m._support_email() in page


def test_une_variable_LEGAL_vide_n_efface_PLUS_rien(monkeypatch) -> None:
    for k in LEGAL:
        monkeypatch.delenv(k, raising=False)
    page = _page()
    for libelle in ("Dénomination sociale", "Siège social", "Immatriculation", "TVA intracommunautaire",
                    "Directeur de la publication", "Téléphone"):
        assert libelle in page, libelle


def test_une_valeur_INCONNUE_n_est_pas_inventee_et_bloque_la_publication(monkeypatch) -> None:
    monkeypatch.setitem(m.legal.PUBLISHER, "share_capital", None)
    assert "Capital social" not in _page() and "share_capital" in m.legal.missing()
    monkeypatch.setitem(m.legal.PUBLISHER, "share_capital", 1_000)
    assert "<dd>1\u00a0000\u00a0€</dd>" in _page()
    assert "<dt>Share Capital</dt><dd>€1,000</dd>" in _page("/en/mentions-legales")


def test_le_capital_social_est_publie_dans_les_deux_langues() -> None:
    # Celui des statuts : 1 €. Le 10 000 € publié du 21 au 24/10/2026 était faux.
    assert "<dt>Capital social</dt><dd>1\u00a0€</dd>" in _page()
    assert "<dt>Share Capital</dt><dd>€1</dd>" in _page("/en/mentions-legales")
    assert "au capital de 1\u00a0€." in _page("/terms")
    assert "with a share capital of €1." in _page("/en/terms")
    assert "10\u00a0000\u00a0€" not in _page() and "€10,000" not in _page("/en/terms")
    assert "share_capital" not in m.legal.missing()


def test_l_hebergeur_est_celui_que_Render_declare() -> None:
    page = _page()
    assert "Render Services, Inc." in page and "525 Brannan Street" in page and "94107" in page


def test_la_page_est_liee_en_PIED_DE_PAGE_et_declaree_au_SITEMAP() -> None:
    assert 'href="/mentions-legales"' in _page("/pricing")
    assert "/mentions-legales</loc>" in _page("/sitemap.xml")
    assert "Allow: /mentions-legales" in _page("/robots.txt")


def test_le_code_parle_a_S3_donc_la_confidentialite_nomme_AMAZON() -> None:
    sources = [f for f in (WEB_ROOT / "backend").glob("*.py")
               if re.search(r"^\s*import boto3\b", f.read_text(encoding="utf-8"), re.M)]
    assert sources, "le témoin : le code importe boto3"
    tableau = _page("/privacy").split("Nos prestataires", 1)[1].split("</table>", 1)[0]
    assert any("Amazon Web Services" in l for l in re.findall(r'<th scope="row">([^<]+)</th>', tableau))


def test_les_rapports_d_analyse_ne_sont_nommes_que_s_ils_vont_dans_S3(monkeypatch) -> None:
    monkeypatch.setattr(m.object_store, "s3_enabled", lambda: False)
    assert "stockage des rapports d'analyse" not in _page("/privacy").replace("&#39;", "'")
    monkeypatch.setattr(m.object_store, "s3_enabled", lambda: True)
    assert "stockage des rapports d'analyse" in _page("/privacy").replace("&#39;", "'")
