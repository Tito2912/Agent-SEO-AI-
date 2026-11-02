"""Le parcours CMS ne connait plus WordPress (02/11/2026) : un connecteur de plus suffit.

Demande du proprietaire : Shopify, PrestaShop, Wix, Webflow et Ghost « de la meme maniere »
que WordPress. Un connecteur fictif, « Essai », est enregistre le temps du test : connexion,
apercu, ecriture relue, suivi, lot d'une anomalie et retablissement passent par lui seul — un
appel a WordPress fait echouer le test."""
from __future__ import annotations

import copy
import os
import re
from pathlib import Path

import pytest
from bs4 import BeautifulSoup
from fastapi.testclient import TestClient
from jinja2 import ChoiceLoader, DictLoader
from sqlalchemy import select

from backend import app as m, auth, cms_guidance, cms_routes, customer_account_profile as profiles, first_correction
from backend import wordpress_cms as wp
from backend.cms_commun import CmsError, Connecteur, values_of
from backend.models import AuditLog, CmsChange, IssueTask, Project, User
from test_cms_integrations import post, row
from test_first_verified_correction import OTHER, URL, _choose, _verify, journey as journey

TEMPLATES = Path(__file__).resolve().parents[1] / "templates"
TITRE = "Service de réparation de vélos à Lyon — Atelier"


class ClientEssai:
    def __init__(self, etat, identifiants):
        self.etat, self.identifiants = etat, identifiants

    def check(self):
        if self.identifiants["jeton"] != "bon-jeton":
            raise CmsError("Jeton Essai refusé.", code="acces_refuse")
        return "Boutique Essai"

    def contains(self, url):
        return url.startswith("https://atelier.example/")

    def read(self, url):
        if url != URL:
            raise CmsError("Page inconnue d’Essai.")
        return copy.deepcopy(self.etat["page"])

    def update(self, page, changes):
        self.etat["ecritures"].append(dict(changes))
        self.etat["page"].update(changes)
        return copy.deepcopy(self.etat["page"])


def _identifiants(form, project):
    if not form["jeton"].strip():
        raise CmsError("Renseignez le jeton Essai.", code="saisie")
    return {"jeton": form["jeton"].strip()}


@pytest.fixture()
def essai(journey, monkeypatch):
    task = _choose(journey)
    monkeypatch.setenv("SEO_AGENT_ENCRYPTION_KEY", "test-cms-key")
    etat = {"page": {"post_id": "page-42", "post_title": "Notre service", "permalink": URL,
                     "seo_title": None, "seo_title_rendered": "Service — Atelier",
                     "meta_description": "Description initiale", "meta_description_rendered": "Description initiale"},
            "ecritures": []}
    cx = Connecteur(nom="essai", libelle="Essai", libelle_complet="Essai (boutique)", provider="essai",
                    client=lambda c: ClientEssai(etat, c), identifiants=_identifiants,
                    recharger=lambda value, project: value, plateforme_ok=lambda site: site["platform"] == "essai",
                    message_plateforme="Choisissez Essai pour utiliser cette connexion.",
                    values=lambda page: values_of(page, "Essai"), raisons={"acces_refuse": "Jeton refusé"},
                    champs=("jeton",))
    monkeypatch.setitem(cms_routes.CONNECTEURS, "essai", cx)
    monkeypatch.setitem(cms_guidance.PLATFORMS, "essai", "Essai")
    formulaire = ('<form method="post" action="/projects/{{ slug }}/cms/essai/connect">'
                  '<input type="hidden" name="_csrf" value="{{ request.state.csrf_token or \'\' }}" />'
                  '<input name="jeton" /></form>')
    env = m.templates.env
    monkeypatch.setattr(env, "loader", ChoiceLoader([env.loader, DictLoader({"cms_connexion_essai.html": formulaire})]))

    def wordpress(*args, **kwargs):
        raise AssertionError("WordPress ne doit pas être appelé")

    monkeypatch.setattr(wp, "exchange", wordpress)
    journey.update(task=task, etat=etat, base=f'/projects/{journey["slug"]}/cms', cx=cx)
    return journey


def _connecter(data, jeton="bon-jeton"):
    return post(data, "/essai/connect", jeton=jeton)


def _connexion(data, nom):
    return m._user_connection_row(user_id=data["owner"], key=f"CMS_{nom}:{data['project_id']}")


def _proprietaire(data):
    client = TestClient(m.app)
    client.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(
        user_id=data["owner"], secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    return client


def test_un_connecteur_de_plus_fait_tout_le_parcours_sans_wordpress(essai, monkeypatch):
    # Aucune plateforme declaree : chaque connecteur montre son propre formulaire.
    avant = essai["client"].get(essai["base"]).text
    for nom in ("essai", "wordpress"):
        assert f'action="/projects/{essai["slug"]}/cms/{nom}/connect"' in avant
    assert 'name="jeton"' in avant
    refus = _connecter(essai, "mauvais")
    assert "err=" in refus.headers["location"] and not _connexion(essai, "ESSAI")
    accord = _connecter(essai)
    assert accord.status_code == 303 and "err=" not in accord.headers["location"]
    assert _connexion(essai, "ESSAI") and not _connexion(essai, "WORDPRESS")
    with m.DB.session() as db:
        assert cms_guidance.settings(db.get(Project, essai["project_id"]).settings)["platform"] == "essai"
        journal = list(db.scalars(select(AuditLog).where(AuditLog.action == "cms.essai.connect",
                                                         AuditLog.target_id == essai["slug"])))
    assert sorted(entree.status for entree in journal) == ["declined", "ok"]
    bilan = cms_routes.connection_report(cx=essai["cx"])
    assert bilan["cms"] == "Essai" and [r["label"] for r in bilan["reasons"]] == ["Jeton refusé"]
    with m.DB.session() as db:
        monkeypatch.setenv("SYSTEM_SETTINGS_OWNER_EMAIL", db.get(User, essai["owner"]).email)
    operations = BeautifulSoup(_proprietaire(essai).get("/settings/operations").text, "html.parser")
    assert "Pilote Essai" in operations.select_one("#pilote-essai h2").get_text()
    assert "Jeton refusé" in operations.select_one("#pilote-essai").get_text()

    reglages = essai["client"].get(essai["base"]).text
    assert "Essai (boutique)" in reglages and "Déconnecter Essai" in reglages and "WordPress + Yoast SEO" not in reglages

    task_id = essai["task"].id
    edition = essai["client"].get(essai["base"] + f"/essai/edit?task_id={task_id}")
    assert edition.status_code == 200 and "Préparer une correction Essai" in edition.text
    apercu = post(essai, "/essai/preview", task_id=task_id, value=TITRE)
    assert "/cms/essai/changes/" in apercu.headers["location"]
    change = apercu.headers["location"].rsplit("/", 1)[1]
    assert row(change).data["cms"] == "essai" and row(change).data["post_id"] == "page-42"
    # L'operation appartient a Essai : l'adresse d'un autre connecteur ne l'ouvre pas.
    assert essai["client"].get(essai["base"] + "/wordpress/changes/" + change).status_code == 404
    assert post(essai, f"/wordpress/changes/{change}/apply").status_code == 404
    assert not essai["etat"]["ecritures"]

    post(essai, f"/essai/changes/{change}/apply")
    assert essai["etat"]["ecritures"] == [{"seo_title": TITRE}] and row(change).state == "applied"
    suivi = essai["client"].get(essai["base"] + "/essai/changes/" + change).text
    assert "Essai a confirmé l’enregistrement." in suivi and "· Essai (boutique)" in suivi
    with m.DB.session() as db:
        note = first_correction.note_data(db.get(IssueTask, task_id).note)
    assert note["cms_change_id"] == change and note["cms_nom"] == "essai"
    for ecran in ("/first-correction", "/corrections"):
        page = essai["client"].get(f'/projects/{essai["slug"]}{ecran}').text
        assert f"/cms/essai/changes/{change}" in page and "/cms/wordpress/" not in page
    # Un controle qui ne conclut pas : la carte de l'accueil ramene au suivi, chez Essai.
    _verify(essai, urls=(OTHER,))
    carte = BeautifulSoup(essai["client"].get("/").text, "html.parser").select_one(".journey-card .btn.primary")
    assert carte["href"] == essai["base"] + f"/essai/changes/{change}#verification"
    with m.DB.session() as db:
        proprietaire = db.get(User, essai["owner"])
    sites = profiles.snapshot(m.DB, proprietaire.id, google_key=m._gsc_oauth_connection_key,
                              bing_key=m._bing_oauth_connection_key)["sites"]
    assert [site["cms"]["saved"] for site in sites] == [True]

    post(essai, f"/essai/changes/{change}/restore")
    assert essai["etat"]["ecritures"][-1] == {"seo_title": None} and row(change).state == "restored"


def test_la_fiche_anomalie_ouvre_le_lot_du_cms_connecte(essai):
    _connecter(essai)
    fiche = BeautifulSoup(essai["client"].get(f'/projects/{essai["slug"]}/issues/missing_title').text, "html.parser")
    lien = fiche.find("a", string="Corriger ces pages dans Essai")
    assert lien["href"] == essai["base"] + "/essai/issues/missing_title"
    lot = essai["client"].get(lien["href"])
    assert lot.status_code == 200 and URL in lot.text and "/cms/wordpress/" not in lot.text


def test_le_lot_n_ecrit_jamais_l_apercu_d_un_autre_cms(essai):
    _connecter(essai)
    apercu = post(essai, "/essai/preview", task_id=essai["task"].id, value=TITRE)
    change = apercu.headers["location"].rsplit("/", 1)[1]
    with m.DB.session() as db:
        ligne = db.get(CmsChange, change)
        ligne.data = {**ligne.data, "cms": "wordpress"}
        db.commit()
    reponse = post(essai, "/essai/issues/missing_title/apply", pick=change)
    assert "err=" in reponse.headers["location"] and not essai["etat"]["ecritures"]
    assert row(change).state == "preview"


def test_une_connexion_ne_sert_pas_hors_de_sa_plateforme(essai, monkeypatch):
    """La plateforme a change sans passer par l'ecran (connexion restee en base) : rien n'est lu."""
    _connecter(essai)
    with m.DB.session() as db:
        projet = db.get(Project, essai["project_id"])
        projet.settings = {**projet.settings, "cms": {"platform": "shopify", "seo_plugin": ""}}
        db.commit()
    lu = []
    lire = ClientEssai.read
    monkeypatch.setattr(ClientEssai, "read", lambda self, url: lu.append(url) or lire(self, url))
    reponse = essai["client"].get(essai["base"] + f"/essai/edit?task_id={essai['task'].id}", follow_redirects=False)
    assert reponse.status_code == 303 and "err=" in reponse.headers["location"] and not lu


def test_quitter_la_plateforme_retire_sa_connexion(essai):
    _connecter(essai)
    post(essai, "/platform", platform="shopify", seo_plugin="")
    assert not _connexion(essai, "ESSAI")


def test_supprimer_le_projet_retire_sa_connexion(essai):
    _connecter(essai)
    client = _proprietaire(essai)
    client.get(essai["base"])
    reponse = client.post("/projects/delete", data={"slugs": [essai["slug"]], "_csrf": client.cookies.get(m._CSRF_COOKIE_NAME)},
                          follow_redirects=False)
    assert reponse.status_code == 303 and not _connexion(essai, "ESSAI")


def test_une_plateforme_sans_connecteur_n_a_pas_d_adresse(essai):
    assert essai["client"].get(essai["base"] + f"/shopify/edit?task_id={essai['task'].id}").status_code == 404
    assert post(essai, "/inconnu/connect", jeton="bon-jeton").status_code == 404


def test_la_confidentialite_nomme_l_acces_de_chaque_cms():
    """Un connecteur stocke un acces chiffre : la politique (FR et EN) le declare (accord du 02/11/2026)."""
    for gabarit, ligne in (("privacy_public.html", '<th scope="row">Connexions</th>'),
                           ("privacy_public_en.html", '<th scope="row">Connections</th>')):
        connexions = next(texte for texte in (TEMPLATES / gabarit).read_text(encoding="utf-8").splitlines() if ligne in texte)
        assert [cx.libelle for cx in cms_routes.CONNECTEURS.values() if cx.libelle not in connexions] == [], gabarit


def test_chaque_connecteur_a_son_formulaire_de_connexion():
    """Le gabarit cms_connexion_<nom>.html envoie a la bonne adresse les champs que le connecteur lit."""
    for nom, cx in cms_routes.CONNECTEURS.items():
        assert cx.nom == nom and cx.champs
        gabarit = (TEMPLATES / f"cms_connexion_{nom}.html").read_text(encoding="utf-8")
        assert f'action="/projects/{{{{ slug }}}}/cms/{nom}/connect"' in gabarit
        assert set(re.findall(r'<(?:input|select|textarea)[^>]*\bname="([a-z_]+)"', gabarit)) - {"_csrf"} == set(cx.champs)
