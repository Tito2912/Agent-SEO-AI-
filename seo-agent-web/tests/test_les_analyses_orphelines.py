"""Les analyses orphelines : ce que les suppressions d'avant le 05/11/2026 ont laissé (backend/analyses_orphelines.py).

Ce que ces tests défendent :
  * seul un dossier rangé comme l'application les range est candidat : un compte absent de la base,
    ou un projet qu'aucun projet du compte — ni d'une équipe dont il est membre — ne porte ;
    hérités, techniques (« _ »), identifiants en majuscules : jamais ;
  * l'écran est au seul propriétaire de la plateforme et ne coche rien ;
  * l'effacement refait l'inventaire : un chemin forgé ou redevenu vivant est gardé et dit ;
  * sans « SUPPRIMER » rien ne part, et une base sans compte refuse l'inventaire.
"""
import uuid
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from bs4 import BeautifulSoup

from backend import analyses_orphelines as orphelins
from backend import app as m
from backend.db import Database
from backend.models import AccountMember
from test_customer_activation import client, lab as lab, project, user

PAGE = "/settings/operations/analyses-orphelines"


class FauxS3:
    """Un bucket en mémoire : des clés de fichiers, listées par dossier comme le fait S3 avec Delimiter."""

    def __init__(self, racine: Path, cles):
        self.racine = racine.resolve()
        self.cles = set(cles)

    def _prefixe(self, dossier) -> str:
        rel = Path(dossier).resolve().relative_to(self.racine).as_posix()
        return "" if rel == "." else rel + "/"

    def lister(self, _runs_dir, dossier):
        p = self._prefixe(dossier)
        return sorted({k[len(p):].split("/")[0] for k in self.cles if k.startswith(p) and "/" in k[len(p):]})

    def effacer(self, dossier, *, recursive=False):
        p = self._prefixe(dossier)
        self.cles = {k for k in self.cles if not k.startswith(p)}


def _fichier(racine: Path, chemin: str) -> Path:
    f = racine / chemin
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text("{}")
    return f


@pytest.fixture()
def arbre(lab, tmp_path, monkeypatch):
    racine = tmp_path / "racine-des-analyses"
    monkeypatch.setattr(m, "DEFAULT_RUNS_DIR", racine)
    proprietaire, membre, tiers = lab["admin"], user(lab["db"]), user(lab["db"])
    project(lab["db"], proprietaire, slug="vivant")
    project(lab["db"], tiers, slug="a-lui")
    with lab["db"].session() as db:
        db.add(AccountMember(owner_user_id=proprietaire.id, member_user_id=membre.id))
        db.commit()
    p, t, disparu = str(proprietaire.id), str(tiers.id), str(uuid.uuid4())
    disque = {nom: _fichier(racine, chemin) for nom, chemin in {
        "vivant": f"{p}/vivant/20261105-120000/audit/report.json",
        "orphelin": f"{p}/supprime-disque/20261001-120000/audit/report.json",
        "technique": f"{p}/_global/20261105-120000/summary.json",
        "equipe": f"{str(membre.id)}/vivant/20261020-120000/audit/report.json",
        "herite": "ancien-site/20260301-120000/audit/report.json",
        # Un identifiant d'AUCUN compte, mais en majuscules : l'application ne range jamais ainsi.
        "majuscules": f"{str(uuid.uuid4()).upper()}/x/20261001-120000/report.json",
    }.items()}
    s3 = FauxS3(racine, [
        f"{p}/vivant/20261105-120000/audit/report.json",
        f"{p}/supprime-disque/20261001-120000/audit/report.json",
        f"{t}/a-lui/20261002-120000/audit/report.json",
        f"{t}/supprime-s3/20260915-120000/audit/report.json",
        f"{disparu}/site/20260910-120000/audit/report.json",
    ])
    monkeypatch.setattr(m.object_store, "list_runs_subdirs", s3.lister)
    monkeypatch.setattr(m, "_delete_runs_path_from_object_store", s3.effacer)
    return {"racine": racine, "disque": disque, "s3": s3, "proprietaire": proprietaire, "tiers": tiers,
            "attendus": {f"{p}/supprime-disque": "disque + S3", f"{t}/supprime-s3": "S3", disparu: "S3"},
            "p": p, "t": t, "disparu": disparu}


def _inventaire(lab, arbre):
    with lab["db"].session() as db:
        return orphelins.inventaire(db, arbre["racine"], lambda d: m.object_store.list_runs_subdirs(m.DEFAULT_RUNS_DIR, d))


def test_l_inventaire_ne_propose_que_les_orphelins(lab, arbre):
    resultat = _inventaire(lab, arbre)
    assert {o["chemin"]: o["ou"] for o in resultat["orphelins"]} == arbre["attendus"]
    raisons = {o["chemin"]: o["raison"] for o in resultat["orphelins"]}
    assert raisons[arbre["disparu"]] == "compte supprimé" and raisons[f"{arbre['t']}/supprime-s3"] == "projet supprimé"
    assert resultat["ignores"] == 2  # le dossier hérité et l'identifiant en majuscules


def test_la_page_est_au_seul_proprietaire_et_ne_coche_rien(lab, arbre):
    assert client(arbre["tiers"]).get(PAGE).status_code == 403
    page = client(arbre["proprietaire"]).get(PAGE)
    assert page.status_code == 200 and page.headers["cache-control"] == "no-store"
    cases = BeautifulSoup(page.text, "html.parser").select("input[name=chemin]")
    assert {c["value"] for c in cases} == set(arbre["attendus"]) and not any(c.has_attr("checked") for c in cases)


def _effacer(proprietaire, chemins, confirmation="SUPPRIMER"):
    c = client(proprietaire)
    c.get(PAGE)
    return c.post(PAGE + "/supprimer", data={"_csrf": c.cookies.get(m._CSRF_COOKIE_NAME), "chemin": chemins,
                                              "confirmation": confirmation}, follow_redirects=False)


def test_l_effacement_refait_l_inventaire_et_garde_le_vivant_et_le_forge(lab, arbre):
    p, t, disparu = arbre["p"], arbre["t"], arbre["disparu"]
    project(lab["db"], arbre["tiers"], slug="supprime-s3")  # recréé entre l'écran et le clic
    forges = [f"{p}/vivant", p, t, "..", "ancien-site", f"{p}/_global", f"{p}/../{t}", ""]

    reponse = _effacer(arbre["proprietaire"], [*arbre["attendus"], *forges])

    message = parse_qs(urlsplit(reponse.headers["location"]).query)["msg"][0]
    assert reponse.status_code == 303 and message.startswith("2 dossiers effacés") and "9 gardés" in message
    assert not arbre["disque"]["orphelin"].exists()
    assert all(arbre["disque"][n].exists() for n in ("vivant", "technique", "equipe", "herite", "majuscules"))
    restes = arbre["s3"].cles
    assert not any(k.startswith((f"{p}/supprime-disque/", f"{disparu}/")) for k in restes)
    assert {f"{p}/vivant/20261105-120000/audit/report.json", f"{t}/a-lui/20261002-120000/audit/report.json",
            f"{t}/supprime-s3/20260915-120000/audit/report.json"} <= restes


def test_sans_SUPPRIMER_rien_n_est_efface(lab, arbre):
    avant = set(arbre["s3"].cles)
    reponse = _effacer(arbre["proprietaire"], list(arbre["attendus"]), confirmation="oui")
    assert "err=" in reponse.headers["location"]
    assert arbre["s3"].cles == avant and arbre["disque"]["orphelin"].exists()


def test_une_base_sans_compte_refuse_l_inventaire(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + str(tmp_path / "vide.db"))
    vide = Database(data_dir=tmp_path / "vide")
    vide.create_tables()
    (tmp_path / "runs" / str(uuid.uuid4()) / "x").mkdir(parents=True)
    with vide.session() as db, pytest.raises(RuntimeError):
        orphelins.inventaire(db, tmp_path / "runs", lambda d: [])
