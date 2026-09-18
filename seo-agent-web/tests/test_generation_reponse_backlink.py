# -*- coding: utf-8 -*-
"""Une generation de reponse qui echoue ne doit rien couter au client.

Derniere zone ecrite de la couverture backlinks. Cette route est la seule du module qui DEPENSE :
elle appelle le modele, et son resultat part ensuite dans une discussion publique au nom du
client. Deux choses comptent donc, et elles sont independantes — l'ordre des operations, et ce
qu'on facture.

L'ORDRE EST DEJA LE BON, et ces tests sont la pour qu'il le reste : acces au projet, puis plan
Solo+, puis quota, puis validation des entrees, puis seulement l'appel au modele. Facturer se fait
EN DERNIER, apres que le texte est revenu. Un echec du fournisseur ne debite rien — c'est la garde
qui vaut ce fichier, parce qu'elle est invisible tant qu'on ne la casse pas : un client dont le
quota fond sur des erreurs 502 ne comprendrait jamais pourquoi.

Rien n'est corrige ici. La route est saine ; elle n'etait tenue par rien.
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-reply-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as app_module  # noqa: E402
from backend import auth, billing  # noqa: E402
from backend.app import app  # noqa: E402
from backend.models import Project, User  # noqa: E402

CORPS = {"platform": "reddit", "opportunityTitle": "Quel outil SEO choisir ?",
         "opportunityUrl": "https://forum.fr/t/123",
         "targetArticleUrl": "https://site.fr/guide"}


def _client_solo() -> tuple[TestClient, str, str]:
    app_module.DB.create_tables()
    tag = uuid.uuid4().hex[:8]
    with app_module.DB.session() as db:
        user = User(email="solo-%s@exemple.fr" % tag,
                    password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(user)
        db.commit()
        db.refresh(user)
        uid = str(user.id)
        proj = Project(owner_user_id=uid, slug="site-%s" % tag, site_name="site.fr",
                       base_url="https://site.fr/")
        db.add(proj)
        db.commit()
        db.refresh(proj)
        slug = proj.slug
    client = TestClient(app)
    client.cookies.set(auth.SESSION_COOKIE_NAME,
                       auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    return client, slug, uid


def _poster(client: TestClient, chemin: str, corps: dict):
    client.get("/")
    jeton = client.cookies.get(app_module._CSRF_COOKIE_NAME, "") or ""
    return client.post(chemin, json=corps,
                       headers={app_module._CSRF_HEADER_NAME: jeton}, follow_redirects=False)


def _consomme(uid: str) -> int:
    with app_module.DB.session() as db:
        return billing.usage_sum(db, user_id=uid, metric="backlink_replies_month")


@pytest.fixture()
def acces_solo(monkeypatch):
    """Le plan est accorde ici : ce fichier mesure la generation, pas la facturation."""
    monkeypatch.setattr(app_module, "_opp_has_access", lambda db, *, user_id: True)
    monkeypatch.setattr(app_module, "_ai_configured", lambda: True)


def test_un_echec_du_modele_ne_debite_RIEN(acces_solo, monkeypatch) -> None:
    """La garde qui vaut ce fichier.

    Un fournisseur indisponible rend une chaine vide ; la route repond 502. Si le debit avait lieu
    avant, le quota d'un client fondrait sur des erreurs qu'il ne provoque pas et qu'il ne voit
    pas — le genre de perte qu'on ne reclame jamais parce qu'on ne peut pas la constater.
    """
    monkeypatch.setattr(app_module, "_ai_generate_text", lambda **_k: "")
    client, slug, uid = _client_solo()
    avant = _consomme(uid)
    reponse = _poster(client, "/api/projects/%s/backlinks/generate-reply" % slug, CORPS)
    assert reponse.status_code == 502, reponse.text[:200]
    assert _consomme(uid) == avant, "un echec du modele a consomme du quota"


def test_une_generation_reussie_debite_exactement_une_unite(acces_solo, monkeypatch) -> None:
    monkeypatch.setattr(app_module, "_ai_generate_text",
                        lambda **_k: "Bonjour, voici mon retour d'experience...")
    client, slug, uid = _client_solo()
    avant = _consomme(uid)
    reponse = _poster(client, "/api/projects/%s/backlinks/generate-reply" % slug, CORPS)
    assert reponse.status_code == 200, reponse.text[:200]
    assert reponse.json().get("reply")
    assert _consomme(uid) == avant + 1


def test_des_entrees_incompletes_sont_refusees_sans_appeler_le_modele(acces_solo, monkeypatch) -> None:
    """Le refus doit tomber AVANT la depense, pas apres."""
    appels = {"n": 0}

    def _compteur(**_k):
        appels["n"] += 1
        return "texte"

    monkeypatch.setattr(app_module, "_ai_generate_text", _compteur)
    client, slug, uid = _client_solo()
    reponse = _poster(client, "/api/projects/%s/backlinks/generate-reply" % slug,
                      {"platform": "reddit", "opportunityTitle": "", "targetArticleUrl": ""})
    assert reponse.status_code == 400, reponse.text[:200]
    assert appels["n"] == 0, "le modele a ete appele malgre des entrees invalides"
    assert _consomme(uid) == 0


def test_le_quota_epuise_refuse_avant_toute_depense(monkeypatch) -> None:
    """429, et le modele n'est pas appele : un quota atteint ne doit rien couter non plus."""
    appels = {"n": 0}

    def _compteur(**_k):
        appels["n"] += 1
        return "texte"

    monkeypatch.setattr(app_module, "_opp_has_access", lambda db, *, user_id: True)
    monkeypatch.setattr(app_module, "_ai_configured", lambda: True)
    monkeypatch.setattr(app_module, "_ai_generate_text", _compteur)
    monkeypatch.setattr(billing, "ensure_within_quota",
                        lambda db, *, user_id, metric, planned_amount: (False, 0))
    client, slug, _uid = _client_solo()
    reponse = _poster(client, "/api/projects/%s/backlinks/generate-reply" % slug, CORPS)
    assert reponse.status_code == 429, reponse.text[:200]
    assert appels["n"] == 0, "le modele a ete appele alors que le quota etait atteint"


def test_un_plan_sans_la_fonctionnalite_est_refuse_avant_tout(monkeypatch) -> None:
    """Le plan gratuit n'a pas cette route ; le refus arrive avant le quota et avant le modele."""
    appels = {"n": 0}

    def _compteur(**_k):
        appels["n"] += 1
        return "texte"

    monkeypatch.setattr(app_module, "_ai_configured", lambda: True)
    monkeypatch.setattr(app_module, "_ai_generate_text", _compteur)
    client, slug, _uid = _client_solo()          # aucun abonnement : le compte est `free`
    reponse = _poster(client, "/api/projects/%s/backlinks/generate-reply" % slug, CORPS)
    assert reponse.status_code == 403, reponse.text[:200]
    assert appels["n"] == 0


def test_le_detail_du_fournisseur_ne_fuit_pas_vers_un_client(acces_solo, monkeypatch) -> None:
    """Un message d'erreur technique nomme le modele et le fournisseur : reserve aux admins."""
    def _echec(**kwargs):
        sink = kwargs.get("error_sink")
        if isinstance(sink, list):
            sink.append("anthropic 429 sur claude-sonnet-4-6")
        return ""

    monkeypatch.setattr(app_module, "_ai_generate_text", _echec)
    client, slug, _uid = _client_solo()
    reponse = _poster(client, "/api/projects/%s/backlinks/generate-reply" % slug, CORPS)
    message = str(reponse.json().get("error") or "")
    assert "anthropic" not in message.lower() and "claude" not in message.lower(), message
