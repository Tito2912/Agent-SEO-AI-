# -*- coding: utf-8 -*-
"""Le site porte ses mentions légales, et la confidentialité nomme le stockage S3.

30/09/2026. noyaru.com n'avait pas de page de mentions légales, alors qu'un site professionnel
doit nommer son éditeur, son directeur de la publication et son hébergeur (LCEN, art. 6 III).
L'identité de l'éditeur est une DÉCLARATION du propriétaire (variables `LEGAL_*` sur Render) :
une valeur absente fait disparaître sa ligne, jamais un libellé vide.

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


def test_l_editeur_DECLARE_est_affiche_en_entier(monkeypatch) -> None:
    for k, v in LEGAL.items():
        monkeypatch.setenv(k, v)
    page = _page()
    for v in LEGAL.values():
        assert v in page, v
    assert 'href="tel:0612345678"' in page, "un lien tel: sans espace"


def test_une_valeur_ABSENTE_fait_disparaitre_sa_ligne(monkeypatch) -> None:
    for k in LEGAL:
        monkeypatch.delenv(k, raising=False)
    page = _page()
    assert "TVA intracommunautaire" not in page
    assert "Directeur de la publication" not in page
    assert 'href="tel:' not in page
    assert "<dd>%s</dd>" % m._app_name() in page, "sans déclaration, l'éditeur est le produit"
    assert 'href="mailto:%s"' % m._support_email() in page


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
