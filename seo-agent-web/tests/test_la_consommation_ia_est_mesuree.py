# -*- coding: utf-8 -*-
"""Chaque appel d'IA note ce qu'il coûte, et à qui.

30/09/2026 : le calcul des marges par plan n'a pu se faire que sur des estimations — rien
n'enregistrait les tokens consommés. Chaque appel note désormais les tokens que le fournisseur
facture, leur coût en dollars, la fonction et le compte PAYEUR (le propriétaire du projet, pas la
personne connectée), dans `usage_events`, métrique `ia_cout_microdollars`.
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
TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-ia-cout-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import select  # noqa: E402

from backend import app as m  # noqa: E402
from backend import auth, billing  # noqa: E402
from backend.models import UsageEvent, User  # noqa: E402


class _Reponse:
    status_code = 200
    text = ""

    def __init__(self, usage: dict) -> None:
        self.usage = usage

    def json(self) -> dict:
        return {"content": [{"type": "text", "text": "ok"}], "usage": self.usage}


@pytest.fixture()
def claude(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "cle-test")
    usage = {"input_tokens": 10_000, "output_tokens": 2_000}
    monkeypatch.setattr(m.requests, "post", lambda *a, **k: _Reponse(usage))
    return usage


def _utilisateur() -> str:
    m.DB.create_tables()
    with m.DB.session() as db:
        u = User(email="ia-%s@exemple.fr" % uuid.uuid4().hex[:8], password_hash="x", is_admin=False)
        db.add(u)
        db.commit()
        db.refresh(u)
        return str(u.id)


def _releves(uid: str) -> list[UsageEvent]:
    with m.DB.session() as db:
        return list(db.scalars(select(UsageEvent).where(UsageEvent.user_id == uid,
                                                        UsageEvent.metric == "ia_cout_microdollars")))


# ── Le prix ─────────────────────────────────────────────────────────────────────────────────

def test_le_prix_est_celui_du_TARIF_PUBLIC() -> None:
    assert m._cout_ia_usd("claude-sonnet-4-6", entree=1_000_000, sortie=0) == pytest.approx(3.0)
    assert m._cout_ia_usd("claude-sonnet-4-6", entree=0, sortie=1_000_000) == pytest.approx(15.0)
    assert m._cout_ia_usd("claude-opus-4-8", entree=1_000_000, sortie=1_000_000) == pytest.approx(30.0)
    assert m._cout_ia_usd("claude-haiku-4-5-20251001", entree=1_000_000, sortie=0) == pytest.approx(1.0)


def test_le_prefixe_le_plus_LONG_gagne(monkeypatch) -> None:
    """« claude-opus-5 » est aussi un préfixe de « claude-opus-5-5 », qui coûte moins cher. La table
    est ici dans l'ordre DÉFAVORABLE : le résultat ne doit pas dépendre de l'ordre d'écriture."""
    monkeypatch.setattr(m, "_PRIX_IA_USD_MTOK", {"claude-opus-5": (5.0, 25.0), "claude-opus-5-5": (4.0, 20.0)})
    assert m._cout_ia_usd("claude-opus-5-5-20261001", entree=0, sortie=1_000_000) == pytest.approx(20.0)
    assert m._cout_ia_usd("claude-opus-5", entree=0, sortie=1_000_000) == pytest.approx(25.0)


def test_le_cache_est_facture_a_son_propre_prix() -> None:
    assert m._cout_ia_usd("claude-sonnet-4-6", entree=0, sortie=0, cache_lu=1_000_000) == pytest.approx(0.3)
    assert m._cout_ia_usd("claude-sonnet-4-6", entree=0, sortie=0, cache_ecrit=1_000_000) == pytest.approx(3.75)


def test_un_modele_INCONNU_n_a_pas_de_prix() -> None:
    assert m._cout_ia_usd("gpt-5.5", entree=1000, sortie=1000) is None


# ── Le relevé ───────────────────────────────────────────────────────────────────────────────

def test_un_appel_note_ses_tokens_son_cout_et_le_payeur(claude) -> None:
    uid = _utilisateur()
    jeton = m._poser_contexte_ia(payeur=uid, fonction="essai")
    try:
        assert m._anthropic_messages_text(system="s", user_msg="u", model="claude-sonnet-4-6", max_tokens=100) == "ok"
    finally:
        m._IA_CONTEXTE.reset(jeton)
    (r,) = _releves(uid)
    assert r.amount == 60_000, "10 000 × 3 $ + 2 000 × 15 $ par million = 0,06 $"
    assert r.meta["entree"] == 10_000 and r.meta["sortie"] == 2_000
    assert r.meta["fonction"] == "essai" and r.meta["modele"] == "claude-sonnet-4-6" and r.meta["plan"] == "free"


def test_un_modele_sans_prix_est_NOTE_quand_meme(claude, monkeypatch) -> None:
    uid = _utilisateur()
    jeton = m._poser_contexte_ia(payeur=uid, fonction="essai")
    try:
        m._anthropic_messages_text(system="s", user_msg="u", model="claude-futur-9", max_tokens=100)
    finally:
        m._IA_CONTEXTE.reset(jeton)
    (r,) = _releves(uid)
    assert r.meta["prix_inconnu"] is True and r.meta["entree"] == 10_000


def test_sans_payeur_rien_n_est_ecrit_et_rien_ne_casse(claude) -> None:
    avant = m._IA_CONTEXTE.get()
    assert m._anthropic_messages_text(system="s", user_msg="u", model="claude-sonnet-4-6", max_tokens=100) == "ok"
    assert m._IA_CONTEXTE.get() is avant


def test_un_releve_en_ECHEC_ne_fait_pas_echouer_l_appel(claude, monkeypatch) -> None:
    uid = _utilisateur()

    def _panne(*_a, **_k):
        raise RuntimeError("base indisponible")

    monkeypatch.setattr(billing, "usage_add", _panne)
    jeton = m._poser_contexte_ia(payeur=uid, fonction="essai")
    try:
        assert m._anthropic_messages_text(system="s", user_msg="u", model="claude-sonnet-4-6", max_tokens=100) == "ok"
    finally:
        m._IA_CONTEXTE.reset(jeton)


def test_les_CINQ_points_d_appel_notent_leur_consommation() -> None:
    """Un nouveau client d'IA qui oublierait le relevé serait une dépense invisible."""
    import inspect
    for f in (m._assistant_openai_chat, m._assistant_gemini_chat, m._assistant_claude_chat,
              m._anthropic_messages_text, m._openai_chat_text):
        assert "_noter_consommation_ia(" in inspect.getsource(f), f.__name__
    # Et aucun AUTRE endroit du code n'appelle une IA : chaque point de sortie vers un fournisseur
    # est dans l'une des cinq fonctions ci-dessus.
    reste = Path(m.__file__).read_text(encoding="utf-8")
    for f in (m._assistant_openai_chat, m._assistant_gemini_chat, m._assistant_claude_chat,
              m._anthropic_messages_text, m._openai_chat_text):
        reste = reste.replace(inspect.getsource(f), "")
    for sortie in ("/chat/completions", ":generateContent", "/messages\"", "v1/messages"):
        assert sortie not in reste, "appel d'IA hors des cinq fonctions relevées : %s" % sortie


# ── À travers une vraie requête : le payeur est le PROPRIÉTAIRE du projet ───────────────────

def _route_d_essai() -> None:
    if any(getattr(r, "path", "") == "/api/projects/{slug}/essai-ia" for r in m.app.routes):
        return

    @m.app.get("/api/projects/{slug}/essai-ia")
    def essai_ia(slug: str) -> dict:
        m._anthropic_messages_text(system="s", user_msg="u", model="claude-sonnet-4-6", max_tokens=100)
        return {"ok": True}

    @m.app.get("/api/essai-ia-hors-projet")
    def essai_ia_hors_projet() -> dict:
        m._anthropic_messages_text(system="s", user_msg="u", model="claude-sonnet-4-6", max_tokens=100)
        return {"ok": True}


def _client(uid: str) -> TestClient:
    c = TestClient(m.app)
    c.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    return c


def test_une_requete_sur_un_projet_debite_son_PROPRIETAIRE(claude, monkeypatch) -> None:
    _route_d_essai()
    proprietaire, membre = _utilisateur(), _utilisateur()
    slug = "agence-" + uuid.uuid4().hex[:6]
    vus = []

    def _payeur(uid, s):
        vus.append((uid, s))
        return proprietaire if s == slug else uid

    monkeypatch.setattr(m, "_compte_payeur", _payeur)
    assert _client(membre).get("/api/projects/%s/essai-ia" % slug).status_code == 200
    assert (membre, slug) in vus
    (r,) = _releves(proprietaire)
    assert r.meta["fonction"] == "essai_ia", "la fonction se lit dans la route"
    assert not _releves(membre)


# ── Le tableau du propriétaire (/settings/operations) ───────────────────────────────────────

MOIS = "2031-01"  # un mois où aucun autre test n'écrit


def _releve(uid: str, *, plan: str, fonction: str, microusd: int, entree: int = 1000, sortie: int = 100,
            sans_prix: bool = False, mois: str = MOIS) -> None:
    meta = {"plan": plan, "fonction": fonction, "entree": entree, "sortie": sortie, "modele": "claude-sonnet-4-6"}
    if sans_prix:
        meta["prix_inconnu"] = True
    with m.DB.session() as db:
        db.add(UsageEvent(user_id=uid, period=mois, metric="ia_cout_microdollars", amount=microusd, meta=meta))
        db.commit()


def test_le_prix_HT_se_lit_dans_le_catalogue() -> None:
    assert m._prix_ht_du_plan("pro") == pytest.approx(82.5)
    assert m._prix_ht_du_plan("business") == pytest.approx(165.83)
    assert m._prix_ht_du_plan("free") == 0.0


def test_le_tableau_additionne_par_PLAN_et_par_FONCTION() -> None:
    mois = "2031-02"
    a, b, c = _utilisateur(), _utilisateur(), _utilisateur()
    # La moins chère d'abord : le tri ne doit rien à l'ordre d'écriture.
    _releve(c, plan="solo", fonction="assistant", microusd=1, sans_prix=True, mois=mois)
    _releve(a, plan="pro", fonction="assistant", microusd=1_000_000, mois=mois)
    _releve(a, plan="pro", fonction="api_github_fix", microusd=2_000_000, mois=mois)
    _releve(b, plan="pro", fonction="api_github_fix", microusd=3_000_000, mois=mois)
    t = m._couts_ia_du_mois(mois)
    pro = next(p for p in t["plans"] if p["cle"] == "pro")
    assert pro["comptes"] == 2 and pro["appels"] == 3 and pro["usd"] == pytest.approx(6.0)
    assert pro["eur_par_compte"] == pytest.approx(round(6.0 * m._TAUX_USD_EUR / 2, 2))
    assert pro["marge_ia"] == pytest.approx(82.5 - pro["eur_par_compte"], abs=0.01)
    fix = next(f for f in t["fonctions"] if f["cle"] == "api_github_fix")
    assert fix["appels"] == 2 and fix["usd"] == pytest.approx(5.0)
    assert t["fonctions"][0]["cle"] == "api_github_fix", "la plus chère d'abord"
    assert t["total"]["sans_prix"] == 1 and t["total"]["usd"] == pytest.approx(6.0), "sans prix : poids négligeable"
    assert [p["cle"] for p in t["plans"]] == ["solo", "pro"], "dans l'ordre des plans"


def test_la_page_montre_le_tableau_au_PROPRIETAIRE_seulement(monkeypatch) -> None:
    uid = _utilisateur()
    _releve(uid, plan="business", fonction="api_github_bulk_fix", microusd=4_200_000)
    autre = _utilisateur()
    monkeypatch.setattr(m, "_user_can_access_system_settings", lambda u: str(getattr(u, "id", "")) == uid)
    page = _client(uid).get("/settings/operations?mois=%s" % MOIS).text
    assert "Coût réel des IA — %s" % MOIS in page and "api_github_bulk_fix" in page
    assert "%.2f €" % (4.2 * m._TAUX_USD_EUR) in page
    assert _client(autre).get("/settings/operations").status_code == 403


def test_un_mois_VIDE_le_dit(monkeypatch) -> None:
    uid = _utilisateur()
    monkeypatch.setattr(m, "_user_can_access_system_settings", lambda u: True)
    page = _client(uid).get("/settings/operations?mois=2030-06").text
    assert "Aucun appel relevé sur ce mois" in page


def test_une_requete_hors_projet_debite_la_personne_CONNECTEE(claude) -> None:
    _route_d_essai()
    uid = _utilisateur()
    assert _client(uid).get("/api/essai-ia-hors-projet").status_code == 200
    (r,) = _releves(uid)
    assert r.meta["fonction"] == "essai_ia_hors_projet"
