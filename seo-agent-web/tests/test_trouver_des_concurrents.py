# -*- coding: utf-8 -*-
"""« Trouver avec l'IA » : des concurrents MESURÉS, pas devinés.

Demande du propriétaire, 28/09/2026. Un modèle interrogé sur « les concurrents de ce site »
invente volontiers des domaines plausibles. Un concurrent est un site qui se classe sur les
MÊMES recherches : l'IA formule ces recherches depuis les pages du site, Google (SerpAPI) dit qui
s'y classe, et le nombre de recherches où un domaine apparaît est la preuve.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_une_page_neuve_part_en_brouillon as _brouillon  # noqa: E402
from test_un_article_sans_depot import CRAWL  # noqa: E402

app_module = _brouillon.app_module
customer = _brouillon.customer
plan = _brouillon.plan
m = app_module


def _r(*domaines):
    return [{"url": "https://%s/page" % d, "position": i + 1} for i, d in enumerate(domaines)]


# ── le classement ─────────────────────────────────────────────────────────────────────────────

def test_le_domaine_present_sur_le_PLUS_de_recherches_passe_devant() -> None:
    resultats = {"q1": _r("a.fr", "b.fr"), "q2": _r("b.fr", "c.fr"), "q3": _r("c.fr", "b.fr")}
    classes = m._classer_les_concurrents(resultats, exclus=set())
    assert [c["domaine"] for c in classes] == ["b.fr", "c.fr", "a.fr"]
    assert classes[0]["requetes"] == ["q1", "q2", "q3"] and classes[0]["position"] == 1


def test_a_egalite_de_recherches_la_meilleure_POSITION_departage() -> None:
    classes = m._classer_les_concurrents({"q": _r("x.fr", "y.fr")}, exclus=set())
    assert [c["domaine"] for c in classes] == ["x.fr", "y.fr"]


def test_le_site_lui_meme_les_suivis_et_les_plateformes_sont_ecartes() -> None:
    resultats = {"q": _r("site.fr", "deja-suivi.fr", "fr.wikipedia.org", "www.amazon.fr",
                         "smile.amazon.com", "youtube.com", "vrai-rival.fr")}
    classes = m._classer_les_concurrents(resultats, exclus={"site.fr", "deja-suivi.fr"})
    assert [c["domaine"] for c in classes] == ["vrai-rival.fr"]


def test_une_plateforme_ne_se_reconnait_pas_a_une_SOUS_CHAINE() -> None:
    """`notamazon.fr` ou `fox.com` ne sont pas Amazon ni X."""
    assert not m._est_une_plateforme("notamazon.fr")
    assert not m._est_une_plateforme("fox.com")
    assert m._est_une_plateforme("fr.wikipedia.org") and m._est_une_plateforme("amazon.de")


def test_le_classement_est_BORNE() -> None:
    resultats = {"q": _r(*["r%d.fr" % i for i in range(10)])}
    assert len(m._classer_les_concurrents(resultats, exclus=set())) == 6


# ── les recherches ────────────────────────────────────────────────────────────────────────────

PAGES = m._pages_du_crawl(CRAWL)


def test_les_recherches_de_l_IA_sont_bornees_et_nettoyees(monkeypatch) -> None:
    monkeypatch.setattr(m, "_correction_ai_json", lambda **kw: {"requetes": [
        "dca crypto", "dca crypto", "ab", "x" * 90, "debuter en bourse", "levier trading", "etf",
        "cinquieme"]})
    assert m._requetes_pour_concurrents(PAGES, langue="fr") == [
        "dca crypto", "debuter en bourse", "levier trading", "etf"]


def test_sans_reponse_de_l_IA_les_TITRES_servent_de_recherches(monkeypatch) -> None:
    monkeypatch.setattr(m, "_correction_ai_json", lambda **kw: {})
    requetes = m._requetes_pour_concurrents(PAGES, langue="fr")
    assert requetes and "DCA crypto" in requetes and all(":" not in r for r in requetes), requetes


# ── la route ──────────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def recherche(monkeypatch):
    etat = {"appels": [], "echec": set()}
    monkeypatch.setenv("SERPAPI_API_KEY", "test")
    monkeypatch.setattr(m, "_own_pages_for_project", lambda runs_dir, slug: (CRAWL, "ts"))
    monkeypatch.setattr(m, "_correction_ai_json", lambda **kw: {"requetes": ["requete un", "requete deux"]})

    def _google(requete, langue):
        etat["appels"].append((requete, langue))
        if requete in etat["echec"]:
            raise RuntimeError("quota")
        return _r("site.fr", "rival.fr", "wikipedia.org") if requete == "requete un" else _r("rival.fr", "autre.fr")

    monkeypatch.setattr(m, "_chercher_google", _google)
    return etat


def _post(client, slug):
    client.get(f"/projects/{slug}")
    token = client.cookies.get(m._CSRF_COOKIE_NAME, "")
    return client.post(f"/api/projects/{slug}/competitors/suggest", headers={m._CSRF_HEADER_NAME: token})


def test_les_candidats_sont_MESURES_et_rien_n_est_ajoute(customer, plan, recherche) -> None:
    client, slug, pid, _uid = customer
    r = _post(client, slug)
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["requetes"] == ["requete un", "requete deux"]
    assert [c["domaine"] for c in d["candidats"]] == ["rival.fr", "autre.fr"]
    assert d["candidats"][0]["requetes"] == ["requete un", "requete deux"]
    with m.DB.session() as db:
        assert m._competitor_rows(db, pid) == [], "une suggestion ne doit rien ajouter"


def test_la_recherche_se_fait_dans_la_langue_PRINCIPALE_mesuree(customer, plan, recherche) -> None:
    client, slug, _pid, _uid = customer
    _post(client, slug)
    assert {langue for _q, langue in recherche["appels"]} == {"fr"}


def test_une_recherche_qui_echoue_n_arrete_pas_les_autres(customer, plan, recherche) -> None:
    client, slug, _pid, _uid = customer
    recherche["echec"].add("requete un")
    d = _post(client, slug).json()
    assert d["ok"] and d["requetes"] == ["requete deux"], d


def test_les_recherches_partent_EN_MEME_TEMPS(customer, plan, recherche, monkeypatch) -> None:
    """Mesure du 28/09/2026 : une recherche sur quatre a expire. En serie, le client attendrait
    la somme des delais ; une barriere a deux places ne se franchit qu'en parallele."""
    import threading
    client, slug, _pid, _uid = customer
    barriere = threading.Barrier(2, timeout=5)
    google = m._chercher_google

    def _ensemble(requete, langue):
        barriere.wait()
        return google(requete, langue)

    monkeypatch.setattr(m, "_chercher_google", _ensemble)
    d = _post(client, slug).json()
    assert d["ok"] and d["requetes"] == ["requete un", "requete deux"], d


def test_si_TOUTES_echouent_on_le_dit(customer, plan, recherche) -> None:
    client, slug, _pid, _uid = customer
    recherche["echec"].update({"requete un", "requete deux"})
    assert _post(client, slug).status_code == 502


def test_sans_CLE_de_recherche_rien_n_est_devine(customer, plan, recherche, monkeypatch) -> None:
    client, slug, _pid, _uid = customer
    monkeypatch.delenv("SERPAPI_API_KEY", raising=False)
    monkeypatch.delenv("SERPAPI_KEY", raising=False)
    r = _post(client, slug)
    assert r.status_code == 503 and "devinés" in r.json()["error"]
    assert recherche["appels"] == []


def test_sans_CRAWL_on_ne_sait_pas_quoi_chercher(customer, plan, recherche, monkeypatch) -> None:
    client, slug, _pid, _uid = customer
    monkeypatch.setattr(m, "_own_pages_for_project", lambda runs_dir, slug: ([], ""))
    assert _post(client, slug).status_code == 400 and recherche["appels"] == []


def test_un_plan_sous_PRO_n_atteint_pas_google(customer, plan, recherche) -> None:
    client, slug, _pid, _uid = customer
    plan["plan"] = "solo"
    assert _post(client, slug).status_code == 403 and recherche["appels"] == []


# ── la liste survit a l'ajout ─────────────────────────────────────────────────────────────────


def test_la_page_dit_au_script_QUI_est_deja_suivi(customer, plan, recherche, monkeypatch) -> None:
    """« Ajouter » recharge la page ; le script reaffiche la liste gardee pour l'onglet et y marque
    les sites deja suivis. Il ne le sait que si la page le lui dit (releve le 28/09/2026 : la
    liste disparaissait au premier ajout)."""
    client, slug, _pid, _uid = customer
    monkeypatch.setattr(m, "_validate_public_crawl_target", lambda url: "")
    client.get(f"/projects/{slug}")
    token = client.cookies.get(m._CSRF_COOKIE_NAME, "")
    r = client.post(f"/projects/{slug}/competitors/add", data={"url": "https://www.Rival.fr/x", "_csrf": token},
                    follow_redirects=False)
    assert r.status_code == 303 and "err=" not in r.headers["location"], r.headers["location"]
    html = client.get(f"/projects/{slug}/competitors").text
    assert 'var suivis = ["rival.fr"];' in html
    assert "var plein = false;" in html
    assert "sessionStorage.setItem(cle" in html and "afficher(precedent)" in html
