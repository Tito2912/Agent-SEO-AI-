# -*- coding: utf-8 -*-
"""Une PR fermee sans fusion rend sa tache « a traiter ».

13/10/2026, prosperfactory : la PR #12 (liens de paiement reecrits a tort) a ete fermee sans
fusion. Sa tache « Page -> lien vers redirection » est restee « En cours » — le balayage ne suit
une PR que jusqu'au verdict de sa CI, et rien d'autre ne regardait si elle avait ete abandonnee.
La page Corrections, qui interroge deja GitHub, la remet a traiter. Jamais sur un doute.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-pr-fermee-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as m  # noqa: E402
from backend.app import app  # noqa: E402


def _projet_avec_tache(pr_number: int, *, status: str = "in_progress") -> tuple[str, str, str]:
    with TestClient(app):  # le demarrage cree les tables
        pass
    suffix = os.urandom(4).hex()
    slug = f"pr-fermee-{suffix}"
    with m.DB.session() as db:
        user = m.User(email=f"{slug}@example.com", password_hash=m.auth.hash_password("x" * 12), is_admin=False)
        db.add(user)
        db.flush()
        proj = m.Project(owner_user_id=str(user.id), slug=slug, base_url=f"https://{slug}.example.com",
                         site_name="PR fermee", settings={"github_repo": "o/r"})
        db.add(proj)
        db.flush()
        tache = m.IssueTask(project_id=str(proj.id), user_id=str(user.id), created_by=str(user.id),
                            issue_key="page_has_links_to_redirect_indexable",
                            issue_label="Page → lien vers redirection (indexable)",
                            crawl_ts="20261013-173710", url=f"https://{slug}.example.com/", status=status,
                            severity="warning",
                            note=json.dumps({"pr_url": f"https://github.com/o/r/pull/{pr_number}",
                                             "pr_number": pr_number, "bulk": True}))
        db.add(tache)
        db.commit()
        return str(user.id), slug, str(tache.id)


def _ouvrir(monkeypatch, user_id: str, slug: str, etat: dict | Exception) -> str:
    appels = []

    def faux_get(path, **kw):
        appels.append(path)
        if isinstance(etat, Exception):
            raise etat
        return etat
    monkeypatch.setattr(m, "_github_api_get", faux_get)
    monkeypatch.setattr(m, "_effective_user_connection_value", lambda **kw: ("jeton", "user"))
    token = m.auth.make_session_token(user_id=user_id, secret=os.environ["SEO_AGENT_SECRET_KEY"])
    with TestClient(app) as client:
        client.cookies.set(m.auth.SESSION_COOKIE_NAME, token)
        r = client.get(f"/projects/{slug}/corrections")
    assert r.status_code == 200
    _ouvrir.appels = appels
    return r.text


def _tache(task_id: str):
    with m.DB.session() as db:
        t = db.get(m.IssueTask, task_id)
        return t.status, json.loads(t.note or "{}")


def test_une_pr_fermee_sans_fusion_rend_la_tache_a_traiter(monkeypatch) -> None:
    user_id, slug, tid = _projet_avec_tache(12)

    page = _ouvrir(monkeypatch, user_id, slug, {"state": "closed", "merged": False, "merged_at": None})

    status, note = _tache(tid)
    assert status == "todo"
    assert note == {"pr_fermee": {"pr_number": 12, "pr_url": "https://github.com/o/r/pull/12",
                                  "constatee_le": note["pr_fermee"]["constatee_le"]}}
    assert "PR ouvertes" not in page          # la PR fermee ne compte plus comme ouverte
    # Des CE chargement, pas au suivant : la page montre la tache a traiter.
    assert '<span class="num">1</span><span class="lbl">À traiter</span>' in page
    assert '<span class="num">0</span><span class="lbl">En cours</span>' in page
    # Au chargement suivant, la note est la trace interne : elle ne s'affiche pas comme une note.
    suivante = _ouvrir(monkeypatch, user_id, slug, {"state": "closed", "merged": False})
    assert "pr_fermee" not in suivante and _ouvrir.appels == []


@pytest.mark.parametrize("etat", [
    {"state": "open", "merged": False},                                    # toujours ouverte
    {"state": "closed", "merged": True, "merged_at": "2026-10-13T21:00:00Z"},  # fusionnee : la verification decide
    RuntimeError("GitHub indisponible"),                                   # un doute ne libere rien
    {"message": "Not Found"},
])
def test_rien_ne_bouge_hors_d_une_fermeture_confirmee(monkeypatch, etat) -> None:
    user_id, slug, tid = _projet_avec_tache(13)

    _ouvrir(monkeypatch, user_id, slug, etat)

    status, note = _tache(tid)
    assert status == "in_progress" and note["pr_number"] == 13


def _projet_a_plusieurs_taches(prs: list[int]) -> tuple[str, str]:
    """Un projet dont chaque tache « en cours » porte la PR donnee (plusieurs familles, une PR)."""
    user_id, slug, _ = _projet_avec_tache(prs[0])
    with m.DB.session() as db:
        proj = db.scalars(m.select(m.Project).where(m.Project.slug == slug)).one()
        for i, n in enumerate(prs[1:]):
            db.add(m.IssueTask(project_id=str(proj.id), user_id=user_id, created_by=user_id,
                               issue_key=f"title_too_long_indexable_{i}", issue_label="Titre",
                               crawl_ts="20261013-173710", url=f"https://{slug}.example.com/{i}",
                               status="in_progress", severity="warning",
                               note=json.dumps({"pr_url": f"https://github.com/o/r/pull/{n}", "pr_number": n})))
        db.commit()
    return user_id, slug


COMPTE = '<span class="num">%d</span><span class="lbl">PR ouvertes</span>'


def test_le_compteur_ne_compte_que_les_pr_que_github_confirme_ouvertes(monkeypatch) -> None:
    # Trois familles corrigees dans UNE PR ouverte : une PR ouverte, pas trois.
    user_id, slug = _projet_a_plusieurs_taches([14, 14, 14])

    page = _ouvrir(monkeypatch, user_id, slug, {"state": "open", "merged": False})

    assert COMPTE % 1 in page
    assert len(_ouvrir.appels) == 1          # un appel par PR distincte, partage par la page
    # Le lien reste affiche apres le rechargement (make-avis, 14/10/2026 : il « a disparu »).
    bloc = page.split("data-prs-ouvertes", 1)[1].split("</p>", 1)[0]
    assert 'href="https://github.com/o/r/pull/14"' in bloc and "#14" in bloc
    assert bloc.count("#14") == 1


@pytest.mark.parametrize("etat", [{"state": "closed", "merged": True, "merged_at": "2026-10-13T21:00:00Z"},
                                  RuntimeError("GitHub indisponible")])
def test_une_pr_fusionnee_ou_inconnue_n_est_pas_comptee_ouverte(monkeypatch, etat) -> None:
    user_id, slug = _projet_a_plusieurs_taches([13, 14])

    page = _ouvrir(monkeypatch, user_id, slug, etat)

    assert "PR ouvertes" not in page
    assert "data-prs-ouvertes" not in page


def test_une_tache_terminee_n_est_pas_rouverte(monkeypatch) -> None:
    user_id, slug, tid = _projet_avec_tache(12, status="done")

    _ouvrir(monkeypatch, user_id, slug, {"state": "closed", "merged": False})

    assert _tache(tid)[0] == "done"
    assert _ouvrir.appels == []   # et GitHub n'est meme pas interroge pour elle
