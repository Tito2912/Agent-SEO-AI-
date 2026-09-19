# -*- coding: utf-8 -*-
"""Un worker qui ne consomme rien doit le DIRE.

CE QUI S'EST PASSE LE 19/09/2026. Un crawl reste « en attente ». Sur Render le service
worker affiche « Deploy live » depuis une demi-heure. La file se remplit. Et nulle part —
ni journal, ni Sentry, ni interface — une ligne n'indique que personne ne consomme. Il a
fallu lire le code de bout en bout pour ne meme pas trancher, faute d'une trace.

La panne ne ressemble a rien. C'est exactement ce qui la rend chere : il n'y a pas d'erreur
a chercher, il y a une ABSENCE a remarquer. Deux lignes de journal separent les deux etats
qu'on ne pouvait pas distinguer :

    « N fils demarres »  -> ils tournent ; si la file stagne, le probleme est ailleurs
                            (un fil bloque dans un crawl, une base differente).
    « AUCUN fil demarre » -> le worker est desactive par son environnement, et la valeur
                            fautive est dans la ligne.

Le silence, lui, ne veut plus rien dire : il signifie que le processus n'a jamais atteint
`_start_job_worker` — donc qu'il est bloque AVANT, dans `alembic upgrade head` de
l'entrypoint. Trois etats distinguables au lieu d'un seul, pour deux appels a `logger`.

CE QUE CES TESTS NE FONT PAS : demarrer de vrais fils. Ils remplacent `threading.Thread`,
parce qu'une suite de tests qui lance des consommateurs de file les laisse tourner.
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-worker-muet-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402


class _FilFactice:
    """Un fil qui n'en est pas un : on mesure les demarrages, pas le travail."""

    demarres: list[str] = []

    def __init__(self, target=None, args=(), daemon=False, **_kw) -> None:
        self._args = args

    def start(self) -> None:
        _FilFactice.demarres.append(str(self._args[0]) if self._args else "")


def _demarrer(monkeypatch, caplog, *, desactive: str | None, concurrence: str = "2"):
    """Repart d'un etat neuf : `_WORKER_STARTED` est un drapeau de module."""
    _FilFactice.demarres = []
    monkeypatch.setattr(m, "_WORKER_STARTED", False)
    monkeypatch.setattr(m, "_WORKER_THREADS", [])
    monkeypatch.setattr(m.threading, "Thread", _FilFactice)
    if desactive is None:
        monkeypatch.delenv("SEO_AGENT_DISABLE_WORKER", raising=False)
    else:
        monkeypatch.setenv("SEO_AGENT_DISABLE_WORKER", desactive)
    monkeypatch.setenv("SEO_AGENT_WORKER_CONCURRENCY", concurrence)
    with caplog.at_level(logging.INFO, logger=m.logger.name):
        m._start_job_worker()
    return caplog.text


def test_un_worker_desactive_le_DIT_et_nomme_la_valeur_fautive(monkeypatch, caplog) -> None:
    """Sans cette ligne, un worker desactive est indiscernable d'un worker qui travaille.

    La valeur figure dans le message : savoir QUE le worker est eteint ne suffit pas, il faut
    savoir laquelle des variables d'environnement l'a eteint pour la corriger.
    """
    texte = _demarrer(monkeypatch, caplog, desactive="true")
    assert "AUCUN fil demarre" in texte, texte
    assert "'true'" in texte, "le journal doit citer la valeur qui désactive : %s" % texte
    assert not _FilFactice.demarres


def test_un_worker_actif_annonce_COMBIEN_de_fils(monkeypatch, caplog) -> None:
    """Le nombre compte autant que le fait : un worker a un seul fil se bloque derriere un
    gros crawl, et la file stagne alors sans que rien soit en panne."""
    texte = _demarrer(monkeypatch, caplog, desactive="false", concurrence="2")
    assert "2 fil(s) demarre(s)" in texte, texte
    assert len(_FilFactice.demarres) == 2


def test_la_ligne_sort_APRES_le_demarrage_reel_des_fils(monkeypatch, caplog) -> None:
    """Annoncer avant de demarrer serait pire que se taire : le journal affirmerait que des
    fils tournent alors qu'un `start()` en echec aurait laisse la file sans consommateur."""
    ordre: list[str] = []

    class _FilQuiNote(_FilFactice):
        def start(self) -> None:
            ordre.append("fil")
            super().start()

    class _JournalQuiNote(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            if "fil(s) demarre(s)" in record.getMessage():
                ordre.append("journal")

    monkeypatch.setattr(m, "_WORKER_STARTED", False)
    monkeypatch.setattr(m, "_WORKER_THREADS", [])
    monkeypatch.setattr(m.threading, "Thread", _FilQuiNote)
    monkeypatch.setenv("SEO_AGENT_DISABLE_WORKER", "false")
    monkeypatch.setenv("SEO_AGENT_WORKER_CONCURRENCY", "2")
    h = _JournalQuiNote()
    niveau = m.logger.level
    m.logger.addHandler(h)
    m.logger.setLevel(logging.INFO)
    try:
        m._start_job_worker()
    finally:
        m.logger.removeHandler(h)
        m.logger.setLevel(niveau)

    assert ordre == ["fil", "fil", "journal"], ordre


def test_un_second_appel_ne_redemarre_rien(monkeypatch, caplog) -> None:
    """Le web et le worker importent le meme module ; le drapeau doit rester un verrou."""
    _demarrer(monkeypatch, caplog, desactive="false", concurrence="2")
    _FilFactice.demarres = []
    m._start_job_worker()
    assert not _FilFactice.demarres
