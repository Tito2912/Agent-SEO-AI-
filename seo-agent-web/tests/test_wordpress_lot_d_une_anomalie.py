"""Toutes les pages d'une anomalie se corrigent dans WordPress en un lot : une validation, une
seule analyse de contrôle.

Avec 40 descriptions trop courtes, il fallait 40 aperçus, 40 validations et un contrôle par page.
Le lot propose les textes page par page (une requête courte chacune : quarante écritures d'IA
dans une seule dépasseraient n'importe quel proxy), les fait relire sur un seul écran, écrit
chaque page sélectionnée avec les mêmes garde-fous qu'une correction seule, et lance UN crawl.
"""
from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlsplit

import pytest
from bs4 import BeautifulSoup
from sqlalchemy import func, select

from backend import app as m, wordpress_cms as wp
from backend.models import AuditLog, CmsChange, IssueTask, JobRecord, UsageEvent
from test_first_verified_correction import BASELINE, OTHER, URL, journey as journey

TROISIEME = "https://atelier.example/troisieme/"
CLE = "meta_description_too_short"
PAGES = (URL, OTHER, TROISIEME)


def _texte(url):
    return ("Atelier de réparation de vélos à Lyon : révision complète, entretien régulier et pièces "
            f"d’origine, page {url.rstrip('/').rsplit('/', 1)[1]}.")


@pytest.fixture()
def lot(journey, monkeypatch):
    monkeypatch.setenv("SEO_AGENT_ENCRYPTION_KEY", "test-cms-key")
    journey["report"](BASELINE, issues={CLE: {"count": 3, "examples": list(PAGES)}}, urls=PAGES)
    pages = {url: {"post_id": 5 + i, "post_title": f"Page {i}", "permalink": url, "post_status": "publish",
                   "post_type": "page", "seo_title": None, "seo_title_rendered": "Atelier",
                   "meta_description": "Courte", "meta_description_rendered": "Courte"}
             for i, url in enumerate(PAGES)}
    ecrits = []

    def exchange(base, username, password, method, path, payload=None):
        if path.startswith("wp/v2/users/me"):
            return {"id": 7, "name": "Atelier", "capabilities": {"wpseo_edit_advanced_metadata": True}}
        if "/run" not in path:
            return {"name": path.rsplit("abilities/", 1)[1], "input_schema": {"properties": {k: {} for k in wp.FIELDS}}}
        if method == "GET":
            return [copy.deepcopy(pages[parse_qs(urlsplit(path).query)["input[permalink]"][0]])]
        page = pages[payload["input"]["permalink"]]
        assert payload["input"]["post_id"] == page["post_id"]
        page.update({k: v for k, v in payload["input"].items() if k in wp.FIELDS})
        ecrits.append(payload["input"]["permalink"])
        return copy.deepcopy(page)

    monkeypatch.setattr(wp, "exchange", exchange)
    monkeypatch.setattr(m, "_length_value_for_page", lambda **kw: _texte(kw["url"]))
    journey.update(pages=pages, ecrits=ecrits, cms=f'/projects/{journey["slug"]}/cms',
                   lot=f'/projects/{journey["slug"]}/cms/wordpress/issues/{CLE}')
    _envoyer(journey, journey["cms"] + "/wordpress/connect", site_url="https://atelier.example",
             username="editor", password="test-application-password")
    return journey


def _envoyer(data, chemin, *, json=False, **champs):
    client = data["client"]
    client.get(data["cms"])
    return client.post(chemin, data={"_csrf": client.cookies.get(m._CSRF_COOKIE_NAME), **champs},
                       headers={"Accept": "application/json"} if json else {}, follow_redirects=False)


def _proposer(data, url):
    reponse = _envoyer(data, data["lot"] + "/propose", json=True, url=url)
    return reponse.json()


def _changes(data):
    with m.DB.session() as db:
        return {c.data["url"]: c for c in db.scalars(select(CmsChange).where(CmsChange.project_id == data["project_id"]))}


def _unites(data):
    with m.DB.session() as db:
        return int(db.scalar(select(func.coalesce(func.sum(UsageEvent.amount), 0)).where(
            UsageEvent.user_id == data["owner"], UsageEvent.metric == "ai_corrections_month")))


def _valider(data, choisis, *, textes=None, montre=None):
    tous = _changes(data)
    ids = [tous[url].id for url in tous if tous[url].state == "preview"]
    valeurs = [(textes or {}).get(tous[url].data["url"], tous[url].data["after"]["meta_description"])
               for url in tous if tous[url].state == "preview"]
    return _envoyer(data, data["lot"] + "/apply", pick=[tous[url].id for url in choisis], ids=ids, values=valeurs,
                    shown_at=(montre or datetime.now(timezone.utc)).isoformat())


def test_l_anomalie_ouvre_le_lot_quand_wordpress_est_connecte(lot):
    page = lot["client"].get(f'/projects/{lot["slug"]}/issues/{CLE}')
    lien = BeautifulSoup(page.text, "html.parser").find("a", string="Corriger ces pages dans WordPress")
    assert lien and lien["href"] == lot["lot"]
    ecran = lot["client"].get(lot["lot"])
    assert ecran.status_code == 200 and all(url in ecran.text for url in PAGES)
    assert ecran.context["remaining"] == 2  # forfait Gratuit : 2 corrections


def test_sans_connexion_ni_pour_un_doublon_il_n_y_a_pas_de_lot(lot):
    _envoyer(lot, lot["cms"] + "/wordpress/disconnect")
    page = lot["client"].get(f'/projects/{lot["slug"]}/issues/{CLE}')
    assert "Corriger ces pages dans WordPress" not in page.text
    assert lot["client"].get(f'/projects/{lot["slug"]}/cms/wordpress/issues/duplicate_titles').status_code == 404


def test_chaque_texte_est_propose_seul_et_le_plafond_arrete_le_lot(lot):
    assert _proposer(lot, URL)["ok"] and _proposer(lot, OTHER)["ok"]
    troisieme = _proposer(lot, TROISIEME)
    assert not troisieme["ok"] and troisieme["stop"]  # plus de correction : le navigateur s'arrête
    assert _unites(lot) == 2 and set(_changes(lot)) == {URL, OTHER} and not lot["ecrits"]
    revue = BeautifulSoup(lot["client"].get(lot["lot"]).text, "html.parser")
    assert len(revue.select('#batch-review input[name="pick"]')) == 2
    assert [li["data-batch-url"] for li in revue.select("[data-batch-url]")] == [TROISIEME]


def test_une_page_hors_de_l_anomalie_n_est_pas_redigee(lot):
    reponse = _proposer(lot, "https://atelier.example/ailleurs/")
    assert not reponse["ok"] and not _changes(lot) and _unites(lot) == 0
    with m.DB.session() as db:
        assert not db.scalar(select(IssueTask).where(IssueTask.url == "https://atelier.example/ailleurs/"))


def test_sans_javascript_le_formulaire_revient_au_lot(lot):
    reponse = _envoyer(lot, lot["lot"] + "/propose", url=URL)
    assert reponse.status_code == 303 and urlsplit(reponse.headers["location"]).path == lot["lot"]
    assert set(_changes(lot)) == {URL}


def test_une_validation_enregistre_la_selection_avec_le_texte_corrige(lot):
    _proposer(lot, URL), _proposer(lot, OTHER)
    corrige = "Réparation et entretien de vélos à Lyon, par un atelier qui révise, règle et remplace les pièces usées."
    reponse = _valider(lot, [URL], textes={URL: corrige})
    assert "err=" not in reponse.headers["location"]
    changes = _changes(lot)
    assert changes[URL].state == "applied" and changes[URL].data["edited"]
    assert lot["pages"][URL]["meta_description"] == corrige and lot["ecrits"] == [URL]
    assert changes[OTHER].state == "preview"


def test_un_texte_corrige_avec_une_balise_est_refuse(lot):
    _proposer(lot, URL)
    reponse = _valider(lot, [URL], textes={URL: "Réparation <b>vélos</b>"})
    assert "err=" in reponse.headers["location"] and not lot["ecrits"]
    assert _changes(lot)[URL].data["after"]["meta_description"] == _texte(URL)


def test_une_page_modifiee_entre_temps_est_arretee_seule(lot):
    _proposer(lot, URL), _proposer(lot, OTHER)
    lot["pages"][OTHER]["meta_description"] = "Modifiée dans WordPress entre-temps"
    reponse = _valider(lot, [OTHER, URL])  # l'échec d'abord : il ne doit pas emporter la suite
    assert OTHER in parse_qs(urlsplit(reponse.headers["location"]).query)["err"][0]
    assert lot["ecrits"] == [URL] and _changes(lot)[OTHER].state == "preview"
    assert lot["pages"][OTHER]["meta_description"] == "Modifiée dans WordPress entre-temps"


def test_un_apercu_ancien_vaut_si_l_ecran_vient_d_etre_affiche(lot):
    _proposer(lot, URL)
    with m.DB.session() as db:
        ligne = db.get(CmsChange, _changes(lot)[URL].id)
        ligne.created_at = datetime.now(timezone.utc) - timedelta(minutes=45)
        db.commit()
    perime = _valider(lot, [URL], montre=datetime.now(timezone.utc) - timedelta(minutes=25))
    assert "expiré" in parse_qs(urlsplit(perime.headers["location"]).query)["err"][0] and not lot["ecrits"]
    assert "err=" not in _valider(lot, [URL]).headers["location"] and lot["ecrits"] == [URL]


def test_reappliquer_une_page_seule_n_ecrit_ni_ne_journalise_rien(lot):
    # L'écriture est désormais partagée avec le lot : la correction seule garde son comportement.
    change = _proposer(lot, URL)["change_id"]
    for _ in range(2):
        _envoyer(lot, lot["cms"] + f"/wordpress/changes/{change}/apply")
    with m.DB.session() as db:
        journal = db.scalar(select(func.count()).select_from(AuditLog).where(
            AuditLog.action == "cms.wordpress.apply", AuditLog.target_id == lot["slug"]))
    assert lot["ecrits"] == [URL] and journal == 1


def test_un_seul_crawl_controle_tout_le_lot_et_rend_son_verdict_page_par_page(lot):
    _proposer(lot, URL), _proposer(lot, OTHER)
    _valider(lot, [URL, OTHER])
    _envoyer(lot, lot["lot"] + "/verify")
    _envoyer(lot, lot["lot"] + "/verify")  # déjà en cours : pas de second crawl
    with m.DB.session() as db:
        crawls = list(db.scalars(select(JobRecord).where(JobRecord.slug == lot["slug"], JobRecord.kind == "crawl")))
    assert len(crawls) == 1
    changes = _changes(lot)
    assert changes[URL].data["verification_job_id"] == changes[OTHER].data["verification_job_id"] == crawls[0].id
    assert lot["client"].get(lot["lot"]).context["poll"]
    ts = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y%m%d-%H%M%S")
    rapport = lot["report"](ts, issues={CLE: {"count": 1, "examples": [TROISIEME]}}, urls=PAGES)
    m._verify_corrections_after_crawl(lot["slug"], rapport, lot["runs_dir"], user_id=lot["owner"], crawl_ts=ts)
    ecran = BeautifulSoup(lot["client"].get(lot["lot"]).text, "html.parser")
    assert [s["data-batch-state"] for s in ecran.select("[data-batch-state]")] == ["verified", "verified"]
