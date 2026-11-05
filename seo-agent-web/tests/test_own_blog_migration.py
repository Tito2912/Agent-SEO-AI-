"""Upgrade grants no mandate; rollback preserves existing approvals and accounts."""

import os
from pathlib import Path
import subprocess
import sys

import pytest
from sqlalchemy import inspect, select, func
from backend.db import Database
from backend.models import OwnBlogPublication, User, TelegramApprovalChannel


@pytest.mark.parametrize("precreated", [False, True])
def test_blog_storage_upgrade_idempotency_and_independent_rollback(
    tmp_path, monkeypatch, precreated
):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + str(tmp_path / "blog.db"))
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
    migrate("upgrade", "20261105_0027")
    with database.session() as db:
        user = User(
            email="blog-migration@example.invalid",
            password_hash="fixture",
            is_admin=True,
        )
        db.add(user)
        db.flush()
        uid = user.id
        db.add(
            TelegramApprovalChannel(
                id="owner",
                owner_id=uid,
                enabled=False,
                binding="a" * 32,
                config_hash="b" * 64,
                bot_id="202",
                chat_id="101",
                lang="fr",
                offset="0",
            )
        )
        db.commit()
    migrate("upgrade", "20261105_0028")
    migrate("upgrade", "20261105_0028")
    with database.session() as db:
        assert db.scalar(select(func.count()).select_from(OwnBlogPublication)) == 0
        assert db.get(TelegramApprovalChannel, "owner").enabled is False
    columns = {
        c["name"] for c in inspect(database.engine).get_columns("own_blog_publications")
    }
    assert columns == set(OwnBlogPublication.__table__.columns.keys())
    migrate("downgrade", "20261105_0027")
    assert not inspect(database.engine).has_table("own_blog_publications")
    with database.session() as db:
        assert db.get(User, uid).email == "blog-migration@example.invalid"
        assert db.get(TelegramApprovalChannel, "owner").binding == "a" * 32
    migrate("upgrade", "20261105_0028")
    database.engine.dispose()
