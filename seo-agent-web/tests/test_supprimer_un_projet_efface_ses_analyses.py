"""Supprimer un projet emporte ses analyses, sur le disque ET dans S3 (05/11/2026).

La doc « Ajouter un projet » le promet (« la suppression est définitive : elle emporte l'historique
des crawls, les rapports… ») et le DPA aussi (art. 12). Jusqu'ici seules les lignes en base
partaient : l'export « Mes données » du propriétaire a fait ressortir les analyses d'un projet
supprimé depuis longtemps.

Ce que ces tests défendent :
  * le dossier du projet supprimé part, et RIEN d'autre : ni un projet au nom voisin, ni le projet
    du même nom chez un autre compte ;
  * S3 est vidé même quand le disque du web n'a aucune copie (le worker n'a pas de disque) — pour
    le projet comme pour le compte entier ;
  * un nom qui sortirait du dossier du projet n'efface rien : un préfixe S3 trop court viderait les
    analyses d'autres comptes, voire tout le bucket.
"""
from pathlib import Path

import pytest

from backend import app as m
from test_customer_activation import client, lab as lab, project, user


@pytest.fixture()
def s3(monkeypatch):
    appels = []
    monkeypatch.setattr(m, "_delete_runs_path_from_object_store",
                        lambda path, *, recursive=False: appels.append((Path(path).resolve(), recursive)))
    return appels


def _rapport(proprietaire, slug):
    fichier = m._runs_dir_for_user(str(proprietaire.id)) / slug / "20261105-120000" / "audit" / "report.json"
    fichier.parent.mkdir(parents=True)
    fichier.write_text("{}")
    return fichier


def _dossier(proprietaire, slug):
    return (m._runs_dir_for_user(str(proprietaire.id)) / slug).resolve()


def _supprimer(proprietaire, *slugs):
    c = client(proprietaire)
    c.get("/")
    return c.post("/projects/delete", data={"_csrf": c.cookies.get(m._CSRF_COOKIE_NAME), "slugs": list(slugs)},
                  follow_redirects=False)


def test_le_projet_supprime_emporte_ses_analyses_et_rien_d_autre(lab, s3):
    titulaire, voisin = user(lab["db"]), user(lab["db"])
    for slug in ("site", "site-bis"):
        project(lab["db"], titulaire, slug=slug)
    project(lab["db"], voisin, slug="site")
    efface = _rapport(titulaire, "site")
    gardes = [_rapport(titulaire, "site-bis"), _rapport(voisin, "site")]

    assert _supprimer(titulaire, "site").status_code == 303

    assert not _dossier(titulaire, "site").exists() and not efface.exists()
    assert all(f.exists() for f in gardes)
    assert s3 == [(_dossier(titulaire, "site"), True)]


def test_s3_est_vide_meme_sans_copie_sur_le_disque_du_web(lab, s3):
    titulaire = user(lab["db"])
    project(lab["db"], titulaire, slug="jamais-ouvert")
    assert not _dossier(titulaire, "jamais-ouvert").exists()

    _supprimer(titulaire, "jamais-ouvert")

    assert s3 == [(_dossier(titulaire, "jamais-ouvert"), True)]


@pytest.mark.parametrize("forme", ["..", ".", "a/b", "../{autre}"])
def test_un_nom_qui_sort_du_dossier_du_projet_n_efface_rien(lab, s3, forme):
    titulaire, autre = user(lab["db"]), user(lab["db"])
    slug = forme.format(autre=autre.id)
    project(lab["db"], titulaire, slug=slug)
    gardes = [_rapport(titulaire, "un-autre-projet"), _rapport(autre, "son-projet")]

    assert _supprimer(titulaire, slug).status_code == 303

    assert all(f.exists() for f in gardes)
    assert s3 == []


def test_le_compte_supprime_vide_s3_meme_sans_copie_sur_le_disque_du_web(lab, s3, tmp_path, monkeypatch):
    racine = tmp_path / "racine-des-analyses"
    monkeypatch.setattr(m, "DEFAULT_RUNS_DIR", racine)
    titulaire = user(lab["db"])
    assert not (racine / str(titulaire.id)).exists()
    c = client(titulaire)
    c.get("/settings/accounts")

    c.post("/settings/account/delete", data={"confirm": "DELETE", "_csrf": c.cookies.get(m._CSRF_COOKIE_NAME)},
           follow_redirects=False)

    assert ((racine / str(titulaire.id)).resolve(), True) in s3
