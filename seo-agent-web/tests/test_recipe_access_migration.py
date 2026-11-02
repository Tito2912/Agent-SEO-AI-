"""The real startup migration creates the permission without changing customer data."""
import os
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import inspect

from backend.db import Database
from backend.models import User


@pytest.mark.parametrize('precreated', [False, True])
def test_upgrade_existing_or_create_all_schema_preserves_accounts_and_downgrades_cleanly(tmp_path, monkeypatch, precreated):
    monkeypatch.setenv('DATABASE_URL', 'sqlite:///'+str(tmp_path/'migration.db'))
    database = Database(data_dir=tmp_path/'data')
    web = Path(__file__).resolve().parents[1]

    def migrate(direction, revision):
        result = subprocess.run([sys.executable, '-m', 'alembic', '-c', str(web/'alembic.ini'), direction, revision],
                                cwd=web, env=os.environ.copy(), capture_output=True, text=True, timeout=45)
        assert result.returncode == 0, result.stderr

    if precreated:
        database.create_tables()
    migrate('upgrade', '20261021_0017')
    with database.session() as db:
        customer = User(email='migration-recipe@example.invalid', password_hash='fixture', is_admin=False)
        db.add(customer);db.commit();uid = customer.id
    migrate('upgrade', 'head')
    schema = inspect(database.engine)
    assert 'project_recipe_access' in schema.get_table_names()
    assert schema.get_pk_constraint('project_recipe_access')['constrained_columns'] == ['user_id']
    assert {tuple(c['column_names']) for c in schema.get_unique_constraints('project_recipe_access')} == {('grant_id',)}
    with database.session() as db:
        assert db.get(User, uid).email == 'migration-recipe@example.invalid' and not db.get(User, uid).is_admin
    migrate('downgrade', '20261021_0017')
    assert 'project_recipe_access' not in inspect(database.engine).get_table_names()
    with database.session() as db:assert db.get(User, uid) is not None
    migrate('upgrade', 'head')
    database.engine.dispose()
