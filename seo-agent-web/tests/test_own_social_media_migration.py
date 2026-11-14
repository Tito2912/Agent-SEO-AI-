"""One additive head, empty authority and preserved owner/DPA across rollback."""

import os
from pathlib import Path
import subprocess
import sys

import pytest
from sqlalchemy import inspect, select, func
from backend.db import Database
from backend.models import User, DpaAcceptance, OwnSocialMedia


@pytest.mark.parametrize("precreated", [False, True], ids=["migrations", "create-all"])
def test_media_migration_preserves_owner_and_existing_acceptance(
    tmp_path, monkeypatch, precreated
):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + str(tmp_path / "migration.db"))
    database = Database(data_dir=tmp_path / "data")
    web = Path(__file__).resolve().parents[1]

    def migrate(direction, revision):
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "alembic",
                "-c",
                str(web / "alembic.ini"),
                direction,
                revision,
            ],
            cwd=web,
            env=os.environ.copy(),
            capture_output=True,
            text=True,
            timeout=45,
        )
        assert result.returncode == 0, result.stderr

    if precreated:
        database.create_tables()
    migrate("upgrade", "20261112_0030")
    with database.session() as db:
        owner = User(
            email="media-migration@example.invalid",
            password_hash="fictional",
            is_admin=True,
        )
        db.add(owner)
        db.flush()
        uid = owner.id
        acceptance = DpaAcceptance(
            user_id=uid,
            email=owner.email,
            organisation="Fictional",
            signataire="Owner",
            fonction="Owner",
            version="fixture",
            langue="fr",
            texte_sha256="a" * 64,
            adresse="Fictional address",
        )
        db.add(acceptance)
        db.commit()
        aid = acceptance.id
    migrate("upgrade", "head")
    migrate("upgrade", "head")
    database.engine.dispose()  # Fresh SQLite connection after external DDL.
    table = inspect(database.engine)
    assert (
        table.get_foreign_keys("own_social_media")[0]["options"]["ondelete"]
        == "CASCADE"
    )
    assert any(
        x["column_names"] == ["owner_id", "authorization_key"]
        for x in table.get_unique_constraints("own_social_media")
    )
    assert any(
        x["column_names"] == ["owner_id"] for x in table.get_indexes("own_social_media")
    )
    with database.session() as db:
        assert db.scalar(select(func.count()).select_from(OwnSocialMedia)) == 0
    migrate("downgrade", "20261112_0030")
    assert not inspect(database.engine).has_table("own_social_media")
    with database.session() as db:
        assert db.get(User, uid).is_admin
        assert db.get(DpaAcceptance, aid).adresse == "Fictional address"
    migrate("upgrade", "head")
    database.engine.dispose()
