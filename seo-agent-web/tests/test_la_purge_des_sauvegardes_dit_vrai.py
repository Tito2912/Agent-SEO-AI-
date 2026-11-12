# -*- coding: utf-8 -*-
"""La purge des sauvegardes ne compte comme effacé que ce que S3 a effacé (12/11/2026).

La confidentialité et le DPA promettent que les sauvegardes disparaissent 30 jours après leur
création. `delete_objects` répond 200 même quand S3 refuse d'effacer : les refus sont dans
`Errors`, objet par objet. La boucle comptait tout le lot comme effacé, et un échec de la purge
n'était qu'une ligne de journal : la tâche restait « réussie » sur Render.
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

from backend import backup  # noqa: E402

MAINTENANT = dt.datetime.now(dt.timezone.utc)
VIEUX = MAINTENANT - dt.timedelta(days=40)


class FauxS3:
    """Un bucket en mémoire qui répond comme S3 : refus dans `Errors`, pas d'exception."""

    def __init__(self, objets: dict[str, dt.datetime], refuses: set[str] = frozenset(),
                 liste_refusee: bool = False) -> None:
        self.objets = dict(objets)
        self.refuses = set(refuses)
        self.liste_refusee = liste_refusee

    def list_objects_v2(self, **kwargs):
        if self.liste_refusee:
            raise PermissionError("AccessDenied")
        prefixe = kwargs["Prefix"]
        return {"Contents": [{"Key": k, "LastModified": d} for k, d in self.objets.items()
                             if k.startswith(prefixe)],
                "IsTruncated": False}

    def delete_objects(self, *, Bucket, Delete):
        erreurs = []
        for objet in Delete["Objects"]:
            if objet["Key"] in self.refuses:
                erreurs.append({"Key": objet["Key"], "Code": "AccessDenied", "Message": "Access Denied"})
            else:
                self.objets.pop(objet["Key"], None)
        return {"Errors": erreurs} if erreurs else {}

    def upload_file(self, path, bucket, key, ExtraArgs=None):
        self.objets[key] = MAINTENANT


def _bucket(**options) -> FauxS3:
    return FauxS3({"backups/essai/db-vieux-1.dump": VIEUX,
                   "backups/essai/db-vieux-2.dump": VIEUX,
                   "backups/essai/db-recent.dump": MAINTENANT}, **options)


def _purger(s3: FauxS3):
    return backup._cleanup_old_backups(client=s3, bucket="b", prefix="backups/essai/", retention_days=30)


def test_une_purge_complete_efface_les_vieilles_et_garde_la_recente() -> None:
    s3 = _bucket()
    assert _purger(s3) == (2, [])
    assert sorted(s3.objets) == ["backups/essai/db-recent.dump"]


def test_un_objet_que_S3_REFUSE_n_est_pas_compte_comme_efface() -> None:
    s3 = _bucket(refuses={"backups/essai/db-vieux-2.dump"})
    effaces, refus = _purger(s3)
    assert effaces == 1
    assert refus == ["backups/essai/db-vieux-2.dump (AccessDenied)"]
    assert "backups/essai/db-vieux-2.dump" in s3.objets


def _lancer(monkeypatch, s3: FauxS3) -> int:
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h:5432/db")
    monkeypatch.setenv("S3_BUCKET_NAME", "b")
    monkeypatch.setenv("BACKUP_SKIP_DATA_DIR", "true")
    monkeypatch.setenv("BACKUP_RETENTION_DAYS", "30")
    monkeypatch.setenv("BACKUP_ENV", "essai")
    monkeypatch.setenv("BACKUP_S3_PREFIX", "backups")
    monkeypatch.delenv("BACKUP_INCLUDE_RUNS_DIR", raising=False)

    def faux_pg_dump(*, database_url: str, out_path: Path) -> dict:
        out_path.write_bytes(b"dump")
        return {}

    monkeypatch.setattr(backup, "_run_pg_dump", faux_pg_dump)
    monkeypatch.setattr(backup, "_s3_client", lambda: s3)
    return backup.main()


def test_la_tache_REUSSIT_quand_la_purge_est_complete(monkeypatch) -> None:
    s3 = _bucket()
    assert _lancer(monkeypatch, s3) == 0
    assert not any("vieux" in k for k in s3.objets)
    assert any(k.startswith("backups/essai/db-") and "recent" not in k for k in s3.objets), \
        "la sauvegarde du jour est bien envoyée"


def test_la_tache_ECHOUE_quand_S3_refuse_d_effacer(monkeypatch, capsys) -> None:
    s3 = _bucket(refuses={"backups/essai/db-vieux-1.dump"})
    assert _lancer(monkeypatch, s3) == 3
    sortie = capsys.readouterr()
    assert "retention refused=1" in sortie.out
    assert "la purge a echoue" in sortie.err


def test_la_tache_ECHOUE_quand_la_liste_est_refusee(monkeypatch, capsys) -> None:
    assert _lancer(monkeypatch, _bucket(liste_refusee=True)) == 3
    assert "retention cleanup error" in capsys.readouterr().out
