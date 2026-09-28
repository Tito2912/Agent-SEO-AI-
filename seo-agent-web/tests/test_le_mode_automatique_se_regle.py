# -*- coding: utf-8 -*-
"""Le mode automatique se RÈGLE : cadence, jour, heure, fuseau, taille, ton, langues, publication.

Décisions du propriétaire, 28/09/2026 :

* d'une page par mois à deux par semaine — au-delà, on approche du « contenu produit en masse »
  que Google sanctionne ;
* un jour et une heure dans le fuseau du client, Paris par défaut ;
* deux publications : AVEC VÉRIFICATION (la pull request attend ; un e-mail mène à une page de
  Noyaru qui porte le bouton « Valider ») et AUTOMATIQUE (fusionnée seule une fois la CI du
  dépôt verte ; sans CI ou en échec, elle attend et l'e-mail part quand même).

Ce que ces tests défendent : un créneau ne part ni avant son heure, ni deux fois, ni en retard
sur un réglage fait après lui ; le choix arrive à la rédaction ; la validation fusionne sans
qu'un identifiant seul ouvre la page d'un autre compte.
"""
from __future__ import annotations

import sys
import threading
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_le_mode_automatique_est_etroit as _m  # noqa: E402

m = _m.app_module
projet, plan, github, modele, client_connecte = _m.projet, _m.plan, _m.github, _m.modele, _m.client_connecte
PARIS = ZoneInfo("Europe/Paris")


def _ts(annee, mois, jour, heure=0, minute=0, tz=PARIS) -> float:
    return datetime(annee, mois, jour, heure, minute, tzinfo=tz).timestamp()


def _reglage(**champs):
    base = {"enabled": True, "section": "/blog", "last_run": 0, "depuis": 0}
    return m._reglages_contenu_auto({"content_auto": {**base, **champs}})


def _creneau(**champs):
    c = m._prochain_creneau(_reglage(**champs))
    return c.astimezone(PARIS).strftime("%a %Y-%m-%d %H:%M") if c else None


# ── les creneaux ──────────────────────────────────────────────────────────────────────────────
# Le lundi 5 octobre 2026 sert de repere.

def test_hebdomadaire_le_meme_jour_la_semaine_suivante() -> None:
    assert _creneau(frequence="hebdomadaire", jour=0, heure=9,
                    last_run=_ts(2026, 10, 5, 9, 1)) == "Mon 2026-10-12 09:00"


def test_deux_par_semaine_le_jour_choisi_et_trois_jours_apres() -> None:
    apres_lundi = _creneau(frequence="bihebdomadaire", jour=0, heure=9, last_run=_ts(2026, 10, 5, 9, 1))
    apres_jeudi = _creneau(frequence="bihebdomadaire", jour=0, heure=9, last_run=_ts(2026, 10, 8, 9, 1))
    assert (apres_lundi, apres_jeudi) == ("Thu 2026-10-08 09:00", "Mon 2026-10-12 09:00")


def test_toutes_les_deux_semaines() -> None:
    assert _creneau(frequence="bimensuelle", jour=2, heure=14,
                    last_run=_ts(2026, 10, 7, 14, 1)) == "Wed 2026-10-21 14:00"


def test_une_par_mois_le_jour_choisi_de_la_PREMIERE_semaine() -> None:
    assert _creneau(frequence="mensuelle", jour=3, heure=8,
                    last_run=_ts(2026, 10, 1, 8, 1)) == "Thu 2026-11-05 08:00"


def test_un_reglage_fait_APRES_le_creneau_du_jour_attend_le_suivant() -> None:
    """Enregistrer lundi a 10:00 un creneau « lundi 09:00 » ne doit pas ecrire tout de suite."""
    assert _creneau(frequence="hebdomadaire", jour=0, heure=9, last_run=0,
                    depuis=_ts(2026, 10, 5, 10)) == "Mon 2026-10-12 09:00"
    assert _creneau(frequence="hebdomadaire", jour=0, heure=9, last_run=0,
                    depuis=_ts(2026, 10, 5, 8)) == "Mon 2026-10-05 09:00"


def test_l_heure_est_celle_du_FUSEAU_du_client() -> None:
    c = m._prochain_creneau(_reglage(frequence="hebdomadaire", jour=0, heure=14,
                                     fuseau="America/Toronto", depuis=_ts(2026, 10, 5, 0)))
    assert c.astimezone(ZoneInfo("America/Toronto")).strftime("%H:%M") == "14:00"
    assert c.astimezone(PARIS).strftime("%H:%M") == "20:00"


def test_le_changement_d_heure_ne_decale_pas_le_creneau() -> None:
    """Heure d'hiver le 25 octobre 2026 : le lundi suivant reste a 09:00, heure de Paris."""
    assert _creneau(frequence="hebdomadaire", jour=0, heure=9,
                    last_run=_ts(2026, 10, 19, 9, 1)) == "Mon 2026-10-26 09:00"


def test_un_reglage_d_AVANT_cette_fonction_ecrit_au_prochain_passage() -> None:
    """Ni `depuis` ni derniere page : le comportement d'avant, une page tout de suite."""
    c = m._prochain_creneau(m._reglages_contenu_auto({"content_auto": {"enabled": True, "section": "/blog"}}))
    assert c is not None and c.timestamp() < 86400 * 400


@pytest.mark.parametrize("champs", [
    {"frequence": "quotidienne"}, {"jour": 9}, {"heure": 31}, {"fuseau": "Mars/Olympus"},
    {"publication": "fusion"}, {"taille": "geant"},
])
def test_un_reglage_stocke_ILLISIBLE_revient_au_defaut(champs) -> None:
    auto = _reglage(**champs)
    defauts = {"frequence": "hebdomadaire", "jour": 0, "heure": 9, "fuseau": "Europe/Paris",
               "publication": "verification", "taille": ""}
    cle = next(iter(champs))
    assert auto[cle] == defauts[cle]


# ── la route des reglages ─────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("champs, mot", [
    ({"frequence": "quotidienne"}, "fréquence"), ({"jour": 7}, "jour"), ({"heure": 24}, "heure"),
    ({"fuseau": "Mars/Olympus"}, "fuseau"), ({"publication": "fusion"}, "publication"),
    ({"ton": "sarcastique"}, "ton"),
])
def test_un_reglage_INVALIDE_est_refuse_et_rien_n_est_ecrit(client_connecte, plan, champs, mot) -> None:
    client, slug, pid = client_connecte
    avant = _m._auto(pid)
    r = _m._regler(client, slug, enabled=True, section="/blog", **champs)
    assert r.status_code == 400 and mot in r.json()["error"], r.text
    assert _m._auto(pid) == avant


def test_tous_les_reglages_se_gardent_et_se_relisent(client_connecte, plan) -> None:
    client, slug, pid = client_connecte
    r = _m._regler(client, slug, enabled=True, section="/blog", frequence="bihebdomadaire", jour=3,
                   heure=14, fuseau="America/Toronto", taille="long", ton="expert",
                   langues=["de", "EN"], publication="auto")
    assert r.status_code == 200 and "à 14:00 (Montréal / Toronto)" in r.json()["prochain"], r.text
    auto = _m._auto(pid)
    assert {k: auto[k] for k in ("frequence", "jour", "heure", "fuseau", "taille", "ton", "langues",
                                 "publication")} == {
        "frequence": "bihebdomadaire", "jour": 3, "heure": 14, "fuseau": "America/Toronto",
        "taille": "long", "ton": "expert", "langues": ["de", "en"], "publication": "auto"}
    page = client.get(f"/projects/{slug}/content").text
    for attendu in ('value="bihebdomadaire" selected', 'value="3" selected',
                    'value="America/Toronto" selected', 'value="auto" checked', "Prochaine page :"):
        assert attendu in page, attendu


# ── le balayage ───────────────────────────────────────────────────────────────────────────────

def test_le_balayage_attend_son_creneau(projet, plan, github, modele) -> None:
    slug, pid, _uid = projet
    _m._eteindre(pid, depuis=m.time.time(), jour=0, heure=9)   # regle maintenant : rien avant lundi prochain
    assert m._balayer_contenu_auto()["proposees"] == 0 and github["post"] == []


def test_VERIFICATION_la_PR_attend_et_un_e_mail_mene_a_la_page_de_validation(
        projet, plan, github, modele, monkeypatch) -> None:
    slug, pid, _uid = projet
    envois = []
    monkeypatch.setattr(m, "_send_email", lambda **kw: envois.append(kw))
    _m._eteindre(pid, taille="court", ton="expert", publication="verification")
    assert m._balayer_contenu_auto()["proposees"] == 1
    assert "environ 800 mots" in modele[0] and "expert :" in modele[0], "le style arrive a la redaction"
    with m.DB.session() as db:
        tache = db.scalars(m.select(m.IssueTask).where(
            m.IssueTask.project_id == pid, m.IssueTask.issue_key == m._CONTENT_PAGE_KEY)).one()
        note = m.json.loads(tache.note)
    assert note["verification"]["fusion_auto"] is False and note["publication"] == "verification"
    assert len(envois) == 1 and "/projects/%s/content/valider/%s" % (slug, tache.id) in envois[0]["body"]
    corps_pr = [b for p, b in github["post"] if p.endswith("/pulls")][0]["body"]
    assert "après validation" in corps_pr


def test_AUTOMATIQUE_la_PR_est_marquee_pour_fusion_et_rien_n_est_envoye(
        projet, plan, github, modele, monkeypatch) -> None:
    slug, pid, _uid = projet
    envois = []
    monkeypatch.setattr(m, "_send_email", lambda **kw: envois.append(kw))
    _m._eteindre(pid, publication="auto")
    assert m._balayer_contenu_auto()["proposees"] == 1
    with m.DB.session() as db:
        note = m.json.loads(db.scalars(m.select(m.IssueTask).where(
            m.IssueTask.project_id == pid, m.IssueTask.issue_key == m._CONTENT_PAGE_KEY)).one().note)
    assert note["verification"]["fusion_auto"] is True and envois == []
    corps_pr = [b for p, b in github["post"] if p.endswith("/pulls")][0]["body"]
    assert "publication automatique" in corps_pr


def test_AUTOMATIQUE_sans_CI_previent_UNE_fois(projet, plan, github, modele, monkeypatch) -> None:
    """Un depot muet suspend la fusion : le client est prevenu, pas a chaque passage."""
    slug, pid, _uid = projet
    envois = []
    monkeypatch.setattr(m, "_send_email", lambda **kw: envois.append(kw))
    monkeypatch.setattr(m, "_verifications_du_commit", lambda **kw: [])
    monkeypatch.setattr(m, "_decider_de_la_pr", lambda verifs, age_s: ("inconnu", "Aucune vérification"))
    monkeypatch.setattr(m, "_pr_marquer_prete", lambda **kw: None)
    _m._eteindre(pid, publication="auto")
    m._balayer_contenu_auto()
    with m.DB.session() as db:
        tache = db.scalars(m.select(m.IssueTask).where(
            m.IssueTask.project_id == pid, m.IssueTask.issue_key == m._CONTENT_PAGE_KEY)).one()
    assert m._reprendre_une_verification(tache) == "non_verifiable"
    with m.DB.session() as db:
        tache = db.get(m.IssueTask, tache.id)
    note = m.json.loads(tache.note)
    note["verification"]["etat"] = "en_attente"          # un second passage sur la meme page
    m._ecrire_verification(str(tache.id), note)
    with m.DB.session() as db:
        m._reprendre_une_verification(db.get(m.IssueTask, tache.id))
    assert len(envois) == 1 and "suspendue" in envois[0]["body"], envois


def test_deux_passages_SIMULTANES_n_ecrivent_qu_une_page(projet, plan, github, modele) -> None:
    m._CONTENU_AUTO_VERROU.acquire()
    try:
        assert m._balayer_contenu_auto() == {"deja_en_cours": 1}
    finally:
        m._CONTENU_AUTO_VERROU.release()


def test_la_boucle_du_service_ATTEND_avant_son_premier_passage(monkeypatch) -> None:
    """Un redemarrage n'ecrit pas de page en demarrant ; les tests qui ouvrent l'application
    ne declenchent rien."""
    passages = []
    monkeypatch.setattr(m, "_balayer_contenu_auto", lambda **kw: passages.append(1) or {})
    monkeypatch.setattr(m, "_contenu_auto_intervalle_s", lambda: 60)
    arret = threading.Event()
    monkeypatch.setattr(m, "_WORKER_STOP", arret)
    t = threading.Thread(target=m._boucle_contenu_auto, daemon=True)
    t.start()
    t.join(0.3)
    arret.set()
    t.join(2)
    assert passages == []


# ── la validation ─────────────────────────────────────────────────────────────────────────────

def _une_page_en_attente(projet, github, modele, monkeypatch):
    slug, pid, _uid = projet
    monkeypatch.setattr(m, "_send_email", lambda **kw: None)
    m._balayer_contenu_auto()
    with m.DB.session() as db:
        return str(db.scalars(m.select(m.IssueTask.id).where(
            m.IssueTask.project_id == pid, m.IssueTask.issue_key == m._CONTENT_PAGE_KEY)).one())


def _valider(client, slug, task_id):
    page = client.get(f"/projects/{slug}/content/valider/{task_id}")
    token = client.cookies.get(m._CSRF_COOKIE_NAME, "")
    return page, client.post(f"/projects/{slug}/content/valider/{task_id}",
                             data={"_csrf": token}, follow_redirects=False)


@pytest.fixture()
def depot(monkeypatch):
    """Le verdict des verifications du depot, et ce que GitHub recoit."""
    etat = {"decision": ("promouvoir", "Vérifications vertes"), "gestes": [], "refus": ""}
    monkeypatch.setattr(m, "_verifications_du_commit", lambda **kw: [])
    monkeypatch.setattr(m, "_decider_de_la_pr", lambda verifs, age_s: etat["decision"])
    monkeypatch.setattr(m, "_pr_marquer_prete", lambda **kw: etat["gestes"].append("prete"))

    def _put(path, **kw):
        if etat["refus"]:
            raise RuntimeError(etat["refus"])
        etat["gestes"].append(path)
        return {"merged": True}

    monkeypatch.setattr(m, "_github_api_put", _put)
    return etat


def _statut(task_id):
    with m.DB.session() as db:
        return db.get(m.IssueTask, task_id).status


def test_VALIDER_avec_des_verifications_VERTES_fusionne(client_connecte, plan, github, modele, monkeypatch, projet, depot) -> None:
    client, slug, pid = client_connecte
    task_id = _une_page_en_attente(projet, github, modele, monkeypatch)
    page, r = _valider(client, slug, task_id)
    assert page.status_code == 200 and "Valider et fusionner" in page.text
    assert "msg=" in r.headers["location"], r.headers["location"]
    gestes = [g for g in depot["gestes"] if "/contents/" not in g]      # l'ecriture de la page mise a part
    assert gestes == ["prete", "/repos/client/site.fr/pulls/91/merge"], gestes
    assert _statut(task_id) == "done"
    assert "fusionnée" in client.get(f"/projects/{slug}/content/valider/{task_id}").text


def test_VALIDER_sur_un_depot_SANS_verification_suffit(client_connecte, plan, github, modele, monkeypatch, projet, depot) -> None:
    client, slug, pid = client_connecte
    task_id = _une_page_en_attente(projet, github, modele, monkeypatch)
    depot["decision"] = ("inconnu", "Aucune vérification sur ce dépôt")
    _valider(client, slug, task_id)
    assert _statut(task_id) == "done" and depot["gestes"][-1].endswith("/merge")


def test_VALIDER_pendant_la_CI_attend_son_verdict_puis_fusionne(client_connecte, plan, github, modele, monkeypatch, projet, depot) -> None:
    """Relire le texte n'est pas verifier le build : la fusion attend la CI."""
    client, slug, pid = client_connecte
    task_id = _une_page_en_attente(projet, github, modele, monkeypatch)
    depot["decision"] = ("attendre", "Vérifications en cours")
    _page, r = _valider(client, slug, task_id)
    from urllib.parse import unquote_plus
    assert "dès que les vérifications" in unquote_plus(r.headers["location"])
    assert _statut(task_id) != "done" and not any(g.endswith("/merge") for g in depot["gestes"])
    depot["decision"] = ("promouvoir", "Vérifications vertes")
    m._balayer_verifications_pr()
    assert _statut(task_id) == "done", "le balayage suivant fusionne la page validee"


def test_des_verifications_ROUGES_bloquent_meme_une_page_validee(client_connecte, plan, github, modele, monkeypatch, projet, depot) -> None:
    client, slug, pid = client_connecte
    task_id = _une_page_en_attente(projet, github, modele, monkeypatch)
    depot["decision"] = ("refuser", "build rouge")
    monkeypatch.setattr(m, "_commenter_la_pr", lambda *a, **kw: None)
    m._balayer_verifications_pr()
    _page, r = _valider(client, slug, task_id)
    from urllib.parse import unquote_plus
    assert "échouent" in unquote_plus(r.headers["location"]) and _statut(task_id) != "done"
    assert not any(g.endswith("/merge") for g in depot["gestes"])


def test_une_CI_RELANCEE_et_passee_au_vert_se_valide(client_connecte, plan, github, modele, monkeypatch, projet, depot) -> None:
    """Le verdict est RELU a la validation, pas repris de l'ancien passage."""
    client, slug, pid = client_connecte
    task_id = _une_page_en_attente(projet, github, modele, monkeypatch)
    depot["decision"] = ("refuser", "build rouge")
    monkeypatch.setattr(m, "_commenter_la_pr", lambda *a, **kw: None)
    m._balayer_verifications_pr()
    depot["decision"] = ("promouvoir", "Vérifications vertes")      # le client a corrige et relance
    _valider(client, slug, task_id)
    assert _statut(task_id) == "done"


def test_un_REFUS_de_GitHub_est_dit_et_rien_n_est_marque(client_connecte, plan, github, modele, monkeypatch, projet, depot) -> None:
    client, slug, pid = client_connecte
    task_id = _une_page_en_attente(projet, github, modele, monkeypatch)
    depot["refus"] = "405 Required status check is failing"
    _page, r = _valider(client, slug, task_id)
    from urllib.parse import unquote_plus
    assert "err=" in r.headers["location"] and "status check" in unquote_plus(r.headers["location"])
    assert _statut(task_id) != "done"


def test_la_page_d_UN_AUTRE_projet_ne_s_ouvre_pas(client_connecte, plan, github, modele, monkeypatch, projet) -> None:
    client, slug, pid = client_connecte
    task_id = _une_page_en_attente(projet, github, modele, monkeypatch)
    with m.DB.session() as db:
        autre = m.Project(owner_user_id=db.get(m.Project, pid).owner_user_id,
                          slug="autre-" + m.uuid.uuid4().hex[:6], site_name="autre.fr",
                          base_url="https://autre.fr/")
        db.add(autre)
        db.commit()
        autre_slug = autre.slug
    assert client.get(f"/projects/{autre_slug}/content/valider/{task_id}").status_code == 404


def test_le_journal_offre_VALIDER_pour_une_page_en_attente(client_connecte, plan, github, modele, monkeypatch, projet) -> None:
    client, slug, pid = client_connecte
    task_id = _une_page_en_attente(projet, github, modele, monkeypatch)
    assert f"/projects/{slug}/content/valider/{task_id}" in client.get(f"/projects/{slug}/content").text
