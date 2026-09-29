# -*- coding: utf-8 -*-
"""Les CGU affirment des règles de fonctionnement : chacune est celle du code.

Réécrites le 29/09/2026 (demande du propriétaire). L'ancienne version était un « modèle de base à
compléter » publié tel quel, datée du jour de chaque visite. Un texte contractuel qui décrit un
fonctionnement que le produit n'a pas est pire qu'un texte vague : ces tests relient chaque
affirmation vérifiable à la ligne de code qui la tient.
"""
from __future__ import annotations

import os
import sys
import tempfile
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
os.environ.setdefault("SEO_AGENT_DATA_DIR", tempfile.mkdtemp(prefix="seo-agent-cgu-"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", tempfile.mkdtemp(prefix="seo-agent-cgu-runs-"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as m  # noqa: E402
from backend import billing  # noqa: E402


def _cgu() -> str:
    return TestClient(m.app).get("/terms").text


def test_les_quotas_se_renouvellent_le_1ER_DU_MOIS_UTC() -> None:
    """Ce que les CGU promettent, c'est `_period_key` : le mois calendaire, en UTC — pas la date
    anniversaire de l'abonnement."""
    assert "se renouvellent le 1er de chaque mois" in _cgu() and "UTC" in _cgu()
    fin = datetime(2026, 10, 31, 23, 59, tzinfo=UTC)
    debut = datetime(2026, 11, 1, 0, 0, tzinfo=UTC)
    assert billing._period_key(fin) != billing._period_key(debut)
    assert billing._period_key(datetime(2026, 11, 1, 0, 0, tzinfo=UTC)) == billing._period_key(
        datetime(2026, 11, 30, 23, 59, tzinfo=UTC))


def test_le_mode_de_fusion_automatique_porte_le_NOM_que_l_application_affiche() -> None:
    """Les CGU nomment l'option ; le client doit la retrouver sous ce nom dans ses reglages."""
    assert "« Full Access »" in _cgu()
    assert "Full Access" in (WEB_ROOT / "templates" / "project_automation.html").read_text(encoding="utf-8")
    assert "Automatique" in (WEB_ROOT / "templates" / "content.html").read_text(encoding="utf-8")


def test_l_analyse_des_concurrents_respecte_TOUJOURS_robots_txt(monkeypatch) -> None:
    """« respecte toujours leurs regles d'exploration » : la commande du crawl concurrent ne porte
    jamais `--ignore-robots`, quel que soit le reglage du projet."""
    assert "respecte toujours leurs règles d'exploration" in _cgu()
    m.DB.create_tables()
    vu = {}

    def _faux(job, cmd, cwd, job_kind, timeout_s=None, env_extra=None):
        vu["cmd"] = cmd
        return 0

    monkeypatch.setattr(m, "_run_subprocess_streaming", _faux)
    monkeypatch.setattr(m, "_validate_public_crawl_target", lambda url: "")
    tag = uuid.uuid4().hex[:8]
    with m.DB.session() as db:
        user = m.User(email=f"cgu-{tag}@exemple.fr", password_hash="x", is_admin=False)
        db.add(user)
        db.commit()
        db.refresh(user)
        proj = m.Project(owner_user_id=str(user.id), slug=f"cgu-{tag}", site_name="site.fr",
                         base_url="https://site.fr/", settings={"crawl": {"ignore_robots": True}})
        db.add(proj)
        db.commit()
        db.refresh(proj)
        rival = m.CompetitorSite(project_id=str(proj.id), user_id=str(user.id), domain="rival.fr",
                                 base_url="https://rival.fr", status="new")
        db.add(rival)
        db.commit()
        db.refresh(rival)
        uid, cid = str(user.id), str(rival.id)
    job = m.Job(id=str(uuid.uuid4()), status="queued", created_at=time.time())
    job.result = {"type": "competitor", "competitor_id": cid, "user_id": uid}
    m._save_job(job)
    m._run_competitor_crawl_job(job.id, uid, cid)
    assert "--ignore-robots" not in vu["cmd"], vu["cmd"]


def test_la_date_est_celle_du_DOCUMENT_et_le_modele_a_disparu() -> None:
    page = _cgu()
    assert "Mise à jour le 29 septembre 2026" in page
    assert "modèle de base à compléter" not in page
    assert datetime.now(UTC).strftime("%Y-%m-%d") not in page


def test_l_editeur_est_DECLARE(monkeypatch) -> None:
    monkeypatch.delenv("LEGAL_ENTITY", raising=False)
    assert "est édité par" not in _cgu()
    monkeypatch.setenv("LEGAL_ENTITY", "Exemple SAS, 1 rue de Test, 75000 Paris")
    assert "est édité par Exemple SAS, 1 rue de Test" in _cgu()


def test_les_cgu_renvoient_aux_TARIFS_et_a_la_CONFIDENTIALITE() -> None:
    page = _cgu()
    assert 'href="/pricing"' in page and 'href="/privacy"' in page
