# -*- coding: utf-8 -*-
"""Le cron qui verifie les backlinks visite des adresses que le CLIENT a fournies.

Dernier maillon de la zone backlinks. Celui-ci ne lit pas un fichier et n'appelle pas le modele :
il SORT. Pour chaque opportunite gagnee, il va chercher la page distante et regarde si le lien y
est encore. L'adresse visitee vient de la base, ou elle est arrivee par un formulaire.

C'EST LA DEFINITION D'UNE SSRF, et la garde existe : `_validate_public_crawl_target` refuse ce qui
n'est pas une adresse publique. Sans elle, n'importe quel compte ferait visiter par le serveur son
propre reseau — `127.0.0.1`, une adresse privee, ou le service de metadonnees d'un hebergeur, qui
distribue des identifiants a qui sait le demander. Ces tests ne reparent rien : ils empechent la
garde de disparaitre, et verifient qu'elle agit AVANT la requete plutot qu'apres.

L'authentification est verifiee ici aussi. Le cron est une route publique ; seul l'en-tete la
protege, et une comparaison en temps constant evite qu'on devine le secret octet par octet.
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

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-cron-bl-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as app_module  # noqa: E402
from backend import auth  # noqa: E402
from backend.app import app  # noqa: E402
from backend.models import BacklinkOpportunity, Project, User  # noqa: E402

SECRET = os.environ["CRON_SECRET"]
CIBLE = "https://client.fr/article"


def _opportunite_gagnee(url_source: str) -> str:
    """Une opportunite « gagnee » : c'est celle que le cron ira verifier."""
    app_module.DB.create_tables()
    tag = uuid.uuid4().hex[:8]
    with app_module.DB.session() as db:
        user = User(email="client-%s@exemple.fr" % tag,
                    password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(user)
        db.commit()
        db.refresh(user)
        proj = Project(owner_user_id=str(user.id), slug="site-%s" % tag,
                       site_name="client.fr", base_url="https://client.fr/")
        db.add(proj)
        db.commit()
        db.refresh(proj)
        opp = BacklinkOpportunity(
            project_id=str(proj.id), user_id=str(user.id), source="web",
            title="Article qui nous cite", url=url_source, snippet="",
            opportunity_score=10, status="won", target_url=CIBLE)
        db.add(opp)
        db.commit()
        db.refresh(opp)
        return str(opp.id)


def _lancer(secret: str | None = SECRET):
    entetes = {"Authorization": "Bearer %s" % secret} if secret is not None else {}
    return TestClient(app).get("/cron/check-backlinks", headers=entetes)


@pytest.fixture()
def visites(monkeypatch) -> list[str]:
    """Enregistre les adresses que le cron essaie REELLEMENT de joindre."""
    vues: list[str] = []

    class _Reponse:
        status_code = 200
        text = "<a href='%s'>toujours la</a>" % CIBLE

    def _get(url, **_kwargs):
        vues.append(str(url))
        return _Reponse()

    monkeypatch.setattr(app_module.requests, "get", _get)
    return vues


# --- l'authentification ----------------------------------------------------------------------

def test_sans_en_tete_le_cron_refuse() -> None:
    assert _lancer(secret=None).status_code == 401


def test_un_mauvais_secret_refuse() -> None:
    assert _lancer(secret="pas-le-bon").status_code == 401


def test_un_secret_presque_bon_refuse_aussi() -> None:
    """La comparaison est en temps constant : un prefixe correct ne doit rien reveler."""
    assert _lancer(secret=SECRET[:-1]).status_code == 401


# --- la garde SSRF ----------------------------------------------------------------------------

@pytest.mark.parametrize("interne", [
    "http://127.0.0.1:8000/page",
    "http://localhost/admin",
    "http://192.168.1.10/",
    "http://10.0.0.5/secret",
    "http://169.254.169.254/latest/meta-data/",      # metadonnees d'hebergeur
])
def test_une_adresse_INTERNE_n_est_jamais_visitee(visites: list[str], interne: str) -> None:
    """La garde doit agir AVANT la requete, pas trier les reponses apres coup.

    On compte les adresses reellement jointes plutot que de lire le code de retour : une garde
    qui laisserait partir la requete et ignorerait sa reponse aurait deja fait le mal.
    """
    _opportunite_gagnee(interne)
    reponse = _lancer()
    assert reponse.status_code == 200, reponse.text[:200]
    assert interne not in visites, "le serveur a visite une adresse interne : %s" % interne
    assert visites == [], visites


def test_une_adresse_PUBLIQUE_est_bien_visitee(visites: list[str]) -> None:
    """Sans ce bord, une garde qui refuserait tout passerait pour une protection."""
    publique = "https://blog-tiers-%s.fr/article" % uuid.uuid4().hex[:6]
    _opportunite_gagnee(publique)
    assert _lancer().status_code == 200
    assert publique in visites, visites


def test_une_adresse_interne_est_RAPPORTEE_et_pas_avalee(visites: list[str]) -> None:
    """Un refus silencieux laisserait croire que le lien a ete verifie.

    Le compte rendu est lu sur MON adresse, pas sur les totaux : le cron balaie toute la base et
    les opportunites des tests voisins y figurent aussi. Une premiere version affirmait
    `checked == 0` et tombait pour cette seule raison — elle mesurait l'isolation de la base, pas
    le comportement du cron.
    """
    interne = "http://127.0.0.1:9000/page-%s" % uuid.uuid4().hex[:6]
    _opportunite_gagnee(interne)
    corps = _lancer().json()
    assert corps.get("ok") is True, corps
    assert interne not in visites, visites
    details = corps.get("error_details") or []
    signalees = [str(d.get("url") or "") for d in details if isinstance(d, dict)]
    assert any(interne.startswith(u) or u.startswith(interne[:40]) for u in signalees), (
        "l'adresse refusee n'apparait pas dans le compte rendu : %s" % signalees[:5])
