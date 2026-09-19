# -*- coding: utf-8 -*-
"""Une ligne quand un travail est PRIS, une quand il est RENDU.

LE TROU QUE CE FICHIER FERME. Le 19/09/2026, un crawl restait « en attente » pendant que
Render affichait « Deploy live » depuis une demi-heure. Les logs du worker ne disaient rien —
littéralement rien. La cause était banale : deux gros crawls occupaient les deux fils et la
file attendait son tour. Mais **un worker occupé et un worker oisif produisent exactement le
même journal**, parce que la sortie d'un crawl part dans `job.stdout` (en base) et non sur la
sortie standard du conteneur.

Le commit précédent avait rendu le DÉMARRAGE lisible (« N fils démarrés »). Il ne disait rien
de ce que ces fils font ensuite. Compter les « prend » sans « rend » donne le nombre de
travaux en vol à tout instant — la seule chose qu'on voulait savoir et qu'on ne pouvait pas
déduire.

CE QUI SE LIT DANS CHAQUE LIGNE, et pourquoi chaque morceau y est :

    [WORKER] a1b2-1 prend 7f3c9d21 (crawl oryvalo-com)
              │        │     │       └── lequel : « un client monopolise » et « la plateforme
              │        │     │           est saturée » appellent des décisions opposées
              │        │     └── l'identifiant, pour recouper avec la fiche du travail
              │        └── pris, pas rendu : la différence donne les travaux en vol
              └── quel fil, quand il y en a plusieurs

LE RENDU EST DANS UN `finally`. Un travail qui échoue est rendu lui aussi ; sinon le compte
des travaux en vol dérive à chaque erreur et le journal ment de plus en plus avec le temps.
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
import time
import uuid
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-file-lisible-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402
from backend import auth  # noqa: E402
from backend.models import Project, User  # noqa: E402


def _projet_et_travail(statut: str = "queued") -> tuple[str, str]:
    m.DB.create_tables()
    with m.DB.session() as db:
        u = User(email="w-%s@exemple.fr" % uuid.uuid4().hex[:8],
                 password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(u)
        db.commit()
        db.refresh(u)
        uid = str(u.id)
        slug = "site-%s" % uuid.uuid4().hex[:8]
        db.add(Project(owner_user_id=uid, slug=slug, site_name=slug,
                       base_url="https://%s.fr/" % slug))
        db.commit()
    job = m.Job(id=str(uuid.uuid4()), status=statut, created_at=time.time())
    job.result = {"type": "crawl", "slug": slug, "user_id": uid}
    m._save_job(job)
    return job.id, slug


def _un_tour_de_boucle(monkeypatch, caplog, *, execute, jid: str | None) -> str:
    """Fait tourner UN passage de `_job_worker_loop` et rend ce qui a été journalisé.

    On mesure la BOUCLE, pas le résumé : c'est elle qui décide d'écrire ou non, et une
    fonction correcte que personne n'appelle ne journalise rien.

    LA RÉCLAMATION EST FIXÉE, et il a fallu la suite complète pour le voir. Ces tests
    passaient isolément et échouaient avec les autres : `_claim_next_job_id` prend le travail
    le PLUS ANCIEN de la file, et les autres fichiers de test en laissent derrière eux dans la
    base partagée. La boucle réclamait donc le travail d'un voisin. Ce qu'on mesure ici est ce
    qui s'ÉCRIT, pas qui est choisi — le choix a ses propres tests ailleurs.
    """
    tours = {"n": 0}

    def _stop_apres_un_tour() -> bool:
        tours["n"] += 1
        return tours["n"] > 1

    monkeypatch.setattr(m._WORKER_STOP, "is_set", _stop_apres_un_tour)
    monkeypatch.setattr(m._WORKER_STOP, "wait", lambda *_a, **_k: None)
    monkeypatch.setattr(m, "_claim_next_job_id", lambda *, worker_id: jid)
    monkeypatch.setattr(m, "_execute_queued_job", execute)
    with caplog.at_level(logging.INFO, logger=m.logger.name):
        m._job_worker_loop("fil-1")
    return caplog.text


# --- ce qui se lit ----------------------------------------------------------------------------

def test_prendre_un_travail_s_ECRIT(monkeypatch, caplog) -> None:
    jid, slug = _projet_et_travail()
    txt = _un_tour_de_boucle(monkeypatch, caplog, execute=lambda _j: None, jid=jid)
    assert "prend %s" % jid[:8] in txt, txt
    assert slug in txt, "la ligne ne dit pas de quel site il s'agit :\n%s" % txt


def _rendu(jid: str) -> str:
    """« fil-1 rend 7f3c9d21 », et le préfixe n'est pas décoratif.

    MES DEUX PREMIERS TESTS DU RENDU ÉTAIENT CREUX : « prend » CONTIENT « rend », donc
    `"rend %s" % jid[:8]` était déjà satisfait par la ligne de PRISE. Ils passaient sans
    jamais mesurer le rendu, et une mutation qui sortait le journal du `finally` leur a
    survécu. Chercher une sous-chaîne dans un journal exige de vérifier qu'elle ne peut pas
    apparaître ailleurs.
    """
    return "fil-1 rend %s" % jid[:8]


def test_rendre_un_travail_s_ECRIT_avec_sa_duree(monkeypatch, caplog) -> None:
    jid, _slug = _projet_et_travail()
    txt = _un_tour_de_boucle(monkeypatch, caplog, execute=lambda _j: None, jid=jid)
    assert _rendu(jid) in txt, txt
    assert "apres" in txt, "la durée manque :\n%s" % txt


def test_un_travail_qui_ECHOUE_est_rendu_lui_aussi(monkeypatch, caplog) -> None:
    """Le `finally` : sans lui, le compte des travaux en vol dérive à chaque erreur et le
    journal ment de plus en plus avec le temps."""
    jid, _slug = _projet_et_travail()

    def _boum(_j):
        raise RuntimeError("le crawl a explosé")

    txt = _un_tour_de_boucle(monkeypatch, caplog, execute=_boum, jid=jid)
    assert "fil-1 prend %s" % jid[:8] in txt
    assert _rendu(jid) in txt, "un travail en échec n'est jamais rendu :\n%s" % txt


def test_PRIS_et_RENDU_s_equilibrent(monkeypatch, caplog) -> None:
    """La propriété qui rend le journal exploitable : la différence entre les deux compteurs
    donne le nombre de travaux en vol. Elle ne tient que si chaque prise a son rendu."""
    _jid, _slug = _projet_et_travail()
    txt = _un_tour_de_boucle(monkeypatch, caplog, execute=lambda _j: None, jid=_jid)
    assert txt.count("[WORKER] fil-1 prend") == txt.count("[WORKER] fil-1 rend") == 1, txt


def test_une_file_VIDE_n_ecrit_rien(monkeypatch, caplog) -> None:
    """Un worker oisif doit rester silencieux : une ligne par seconde et par fil noierait le
    signal qu'on vient d'ajouter."""
    txt = _un_tour_de_boucle(monkeypatch, caplog, execute=lambda _j: None, jid=None)
    assert "prend" not in txt and "rend" not in txt, txt


# --- le resume --------------------------------------------------------------------------------

def test_le_resume_nomme_le_TYPE_et_le_SITE() -> None:
    jid, slug = _projet_et_travail()
    assert m._resume_de_job(jid) == "crawl %s" % slug


def test_un_travail_SANS_site_se_resume_quand_meme() -> None:
    """Les travaux d'administration (autopilot) n'ont pas de slug. Une ligne tronquée vaut
    mieux qu'une exception dans la boucle qui consomme la file."""
    m.DB.create_tables()
    with m.DB.session() as db:
        u = User(email="a-%s@exemple.fr" % uuid.uuid4().hex[:8],
                 password_hash=auth.hash_password("x" * 12), is_admin=True)
        db.add(u)
        db.commit()
        db.refresh(u)
        uid = str(u.id)
    job = m.Job(id=str(uuid.uuid4()), status="queued", created_at=time.time())
    job.result = {"type": "autopilot", "user_id": uid}
    m._save_job(job)
    assert m._resume_de_job(job.id) == "autopilot"


def test_un_travail_INTROUVABLE_ne_fait_pas_tomber_la_boucle() -> None:
    """Journaliser ne doit jamais casser ce qu'on journalise."""
    assert m._resume_de_job("inexistant") == "?"
