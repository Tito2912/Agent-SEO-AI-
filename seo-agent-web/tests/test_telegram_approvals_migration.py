"""Additive owner decision storage; migrations grant no authority."""

import os
from pathlib import Path
import subprocess
import sys

import pytest
from sqlalchemy import inspect, select, func
from backend.db import Database
from backend.models import (
    TelegramApprovalChannel,
    TelegramApprovalRequest,
    User,
    DpaAcceptance,
)


@pytest.mark.parametrize("precreated", [False, True])
def test_upgrade_has_no_binding_or_request_and_downgrade_preserves_owner(
    tmp_path, monkeypatch, precreated
):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + str(tmp_path / "approvals.db"))
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
    migrate("upgrade", "20261105_0026")
    with database.session() as db:
        owner = User(
            email="telegram-migration@example.invalid",
            password_hash="fixture",
            is_admin=True,
        )
        db.add(owner)
        db.commit()
        uid = owner.id
        acceptance = DpaAcceptance(
            user_id=uid,
            email=owner.email,
            organisation="Fixture",
            signataire="Fixture",
            fonction="Owner",
            version="fixture",
            langue="fr",
            texte_sha256="a" * 64,
        )
        db.add(acceptance)
        db.commit()
        acceptance_id = acceptance.id
    migrate("upgrade", "20261105_0027")
    migrate("upgrade", "20261105_0027")
    with database.session() as db:
        assert db.scalar(select(func.count()).select_from(TelegramApprovalChannel)) == 0
        assert db.scalar(select(func.count()).select_from(TelegramApprovalRequest)) == 0
    migrate("downgrade", "20261105_0026")
    assert not inspect(database.engine).has_table("telegram_approval_channels")
    assert not inspect(database.engine).has_table("telegram_approval_requests")
    with database.session() as db:
        assert db.get(User, uid).is_admin
        assert db.get(DpaAcceptance, acceptance_id).texte_sha256 == "a" * 64
    migrate("upgrade", "20261105_0027")
    database.engine.dispose()
