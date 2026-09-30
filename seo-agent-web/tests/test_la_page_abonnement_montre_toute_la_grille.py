# -*- coding: utf-8 -*-
"""La page Abonnement montre les quatre plans et TOUTE la grille, la même que la home.

Demande du propriétaire, 30/09/2026 : « il faut ajouter le plan free, ajouter toutes les
fonctions par plan et tous les quotas par fonction et par plan ». Relevé avant correction :
/billing n'affichait que Solo, Pro et Business, chacun avec une liste `features` écrite à la
main — « Monitoring + alertes », « Suggestions IA avancées », « Exports PDF/CSV » réservés à
certains plans — alors que le code ne fait AUCUNE de ces différences. Et la grille de la home
oubliait le fix pack, seule fonction fermée au plan Free en dehors des quotas.

Ces tests relient la grille au code : chaque quota du catalogue y figure, et chaque accès qu'elle
annonce est celui que rendent les VRAIES portes de l'application.
"""
from __future__ import annotations

import html
import os
import re
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-grille-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as m  # noqa: E402
from backend import auth, billing  # noqa: E402
from backend.models import User  # noqa: E402

PLANS = ("free", "solo", "pro", "business")


@pytest.fixture(autouse=True)
def stripe_simule(monkeypatch):
    monkeypatch.setattr(billing, "stripe_enabled", lambda: True)
    monkeypatch.setattr(billing, "stripe_init", lambda: None)
    monkeypatch.setattr(billing, "list_invoices", lambda db, *, user_id, **_k: [])
    monkeypatch.setattr(billing, "price_id_for_plan", lambda k: {"solo": "p_s", "pro": "p_p", "business": "p_b"}.get(k, ""))


def _texte(page: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", html.unescape(page)))


def _abonnement() -> str:
    m.DB.create_tables()
    with m.DB.session() as db:
        user = User(email="grille-%s@exemple.fr" % uuid.uuid4().hex[:8],
                    password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(user)
        db.commit()
        db.refresh(user)
        uid = str(user.id)
    client = TestClient(m.app)
    client.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    return client.get("/billing").text


def _lignes() -> list[str]:
    return [l["libelle"] for s in m._comparatif_des_plans()["sections"] for l in s["lignes"]]


def _ligne(libelle: str) -> list[str]:
    for s in m._comparatif_des_plans()["sections"]:
        for l in s["lignes"]:
            if l["libelle"].startswith(libelle):
                return l["valeurs"]
    raise AssertionError("ligne absente : %s" % libelle)


def test_l_abonnement_montre_les_QUATRE_plans_dont_Free() -> None:
    page = _abonnement()
    for nom in ("Free", "Solo", "Pro", "Business"):
        assert "<h2>%s</h2>" % nom in page, nom


def test_l_abonnement_montre_TOUTE_la_grille_comme_la_home() -> None:
    abonnement, tarifs = _texte(_abonnement()), _texte(TestClient(m.app).get("/pricing").text)
    manquantes = [l for l in _lignes() if l not in abonnement or l not in tarifs]
    assert not manquantes, manquantes
    assert len(_lignes()) >= 24, "le témoin : la grille n'est pas vide"


def test_les_fonctions_FICTIVES_ont_disparu() -> None:
    page = _texte(_abonnement())
    for fiction in ("Monitoring + alertes", "Suggestions IA avancées", "Suggestions IA (limitées)"):
        assert fiction not in page, fiction


def test_chaque_QUOTA_du_catalogue_est_dans_la_grille(monkeypatch) -> None:
    """Un quota ajouté au catalogue et oublié dans la grille serait une promesse muette. Chaque
    limite reçoit ici une valeur unique : elle doit ressortir quelque part dans la grille."""
    vrai = billing.plan_catalog()
    metriques = sorted((vrai["business"].get("limits") or {}).keys())
    assert len(metriques) >= 8, metriques
    faux = {k: {**v, "limits": {mt: 7001 + i for i, mt in enumerate(metriques)}} for k, v in vrai.items()}
    monkeypatch.setattr(billing, "plan_catalog", lambda: faux)
    affiche = {v for s in m._comparatif_des_plans()["sections"] for l in s["lignes"] for v in l["valeurs"]}
    oublies = [mt for i, mt in enumerate(metriques) if m._nombre_fr(7001 + i) not in " ".join(affiche)]
    assert not oublies, oublies


@pytest.mark.parametrize("plan", PLANS)
def test_chaque_ACCES_annonce_est_celui_des_vraies_portes(monkeypatch, plan) -> None:
    i = PLANS.index(plan)
    monkeypatch.setattr(billing, "effective_plan_key", lambda db, *, user_id: plan)
    with m.DB.session() as db:
        assert bool(_ligne("Recherches d'opportunités")[i]) is m._opp_has_access(db, user_id="u")
        assert bool(_ligne("Surveillance des liens obtenus")[i]) is m._opp_has_access(db, user_id="u")
        assert bool(_ligne("Concurrents analysés")[i]) is m._competitor_has_access(db, user_id="u")
    assert bool(_ligne("Fix pack")[i]) is m._fix_pack_ouvert(plan)


def test_le_fix_pack_est_FERME_a_Free_par_sa_route(monkeypatch) -> None:
    """La grille et la route lisent la même fonction : on vérifie la route elle-même."""
    m.DB.create_tables()
    tag = uuid.uuid4().hex[:8]
    with m.DB.session() as db:
        user = User(email="fp-%s@exemple.fr" % tag, password_hash="x", is_admin=False)
        db.add(user)
        db.commit()
        db.refresh(user)
        uid = str(user.id)
        db.add(m.Project(owner_user_id=uid, slug="fp-" + tag, site_name="s.fr", base_url="https://s.fr/", settings={}))
        db.commit()
    client = TestClient(m.app)
    client.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    monkeypatch.setattr(billing, "effective_plan_key", lambda db, *, user_id: "free")
    rep = client.get("/projects/fp-%s/export/fix-pack.zip" % tag, follow_redirects=False)
    assert rep.status_code == 303 and "/billing" in rep.headers["location"]
    monkeypatch.setattr(billing, "effective_plan_key", lambda db, *, user_id: "solo")
    rep = client.get("/projects/fp-%s/export/fix-pack.zip" % tag, follow_redirects=False)
    assert "/billing" not in rep.headers.get("location", ""), "Solo doit passer la porte"


# ── Les cartes listent TOUT (30/09/2026, « il n'y a pas toutes les fonctions dans les badges ») ──

def _cartes(page: str, balise: str) -> dict[str, str]:
    """Le contenu de chaque carte de plan, découpé sur son titre."""
    cartes = {}
    for nom in ("Free", "Solo", "Pro", "Business"):
        cartes[nom] = page.split("<%s>%s</%s>" % (balise, nom, balise), 1)[1].split("</article>", 1)[0]
    return cartes


@pytest.mark.parametrize("chemin, balise", [("/pricing", "h3"), ("/", "h3"), ("/billing", "h2")])
def test_chaque_carte_liste_TOUTES_les_lignes_de_la_grille(chemin, balise) -> None:
    page = _abonnement() if chemin == "/billing" else TestClient(m.app).get(chemin).text
    grille = m._comparatif_des_plans()
    attendu = len(_lignes()) + len(grille["sections"])
    for nom, carte in _cartes(page, balise).items():
        assert carte.count("<li") == attendu, (chemin, nom, carte.count("<li"), attendu)


def test_une_fonction_ABSENTE_est_grisee_et_une_INCLUSE_ne_l_est_pas() -> None:
    cartes = _cartes(TestClient(m.app).get("/pricing").text, "h3")
    fix_free = re.search(r'<li class="absent"><span class="sr">Non inclus : </span>Fix pack sans dépôt Git</li>', cartes["Free"])
    assert fix_free, "Free n'a pas le fix pack : grisé"
    assert "<li>Fix pack sans dépôt Git</li>" in cartes["Solo"]
    assert "absent" not in cartes["Business"], "Business a tout"
    grille = m._comparatif_des_plans()
    for i, carte in enumerate(grille["cartes"]):
        absents = sum(1 for f in carte["fonctions"] if "texte" in f and not f["inclus"])
        vides = sum(1 for s in grille["sections"] for l in s["lignes"] if not l["valeurs"][i])
        assert absents == vides, carte["cle"]


def test_les_valeurs_des_cartes_sont_celles_de_la_GRILLE() -> None:
    cartes = {c["cle"]: [f.get("texte") for f in c["fonctions"]] for c in m._comparatif_des_plans()["cartes"]}
    cat = billing.plan_catalog()
    for k in ("solo", "pro", "business"):
        lim = cat[k]["limits"]
        assert "%s corrections/mois" % m._nombre_fr(lim["ai_corrections_month"]) in cartes[k]
        assert "%s messages à l'assistant IA/mois" % m._nombre_fr(lim["assistant_messages_month"]) in cartes[k]
        assert "%s recherches de backlinks/mois" % m._nombre_fr(lim["backlink_searches_month"]) in cartes[k]
    assert "1 site suivi" in cartes["free"], "le singulier"


def test_le_bouton_est_AU_DESSUS_de_la_liste() -> None:
    carte = _cartes(TestClient(m.app).get("/pricing").text, "h3")["Solo"]
    assert carte.index("Commencer avec Solo") < carte.index('class="tarif-cles"')


def test_la_colonne_mise_en_avant_est_le_PLAN_DU_COMPTE() -> None:
    assert [c["en_avant"] for c in m._comparatif_du_compte("free")["cartes"]] == [True, False, False, False]
    assert [c["en_avant"] for c in m._comparatif_des_plans()["cartes"]] == [False, False, True, False]
    page = _abonnement()
    assert '<th scope="col" class="colonne-avant">Free</th>' in page, "la page Abonnement lit le plan du compte"
    assert '<th scope="col" class="colonne-avant">Pro</th>' not in page


def test_revenir_a_FREE_passe_par_la_resiliation() -> None:
    """Sans abonnement, la carte Free dit « Plan actuel » ; avec, elle mène au parcours de
    résiliation — pas à un paiement."""
    free = _abonnement().split("<h2>Free</h2>", 1)[1].split("</article>", 1)[0]
    assert "Plan actuel" in free and "/billing/checkout" not in free
