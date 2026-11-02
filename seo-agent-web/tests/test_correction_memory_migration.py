"""The additive journal migration preserves existing customer data."""
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import inspect

from backend import correction_memory
from backend.db import Database
from backend.models import IssueTask, Project, User


@pytest.mark.parametrize('precreated', [False, True])
def test_upgrade_replay_and_downgrade_preserve_project_and_task(tmp_path, monkeypatch, precreated):
    monkeypatch.setenv('DATABASE_URL', 'sqlite:///'+str(tmp_path/'migration.db'))
    database = Database(data_dir=tmp_path/'data')
    web = Path(__file__).resolve().parents[1]

    def migrate(direction, revision):
        result = subprocess.run([sys.executable, '-m', 'alembic', '-c', str(web/'alembic.ini'), direction, revision],
                                cwd=web, env=os.environ.copy(), capture_output=True, text=True, timeout=45)
        assert result.returncode == 0, result.stderr

    if precreated:
        database.create_tables()
    migrate('upgrade', '20261102_0018')
    with database.session() as db:
        owner = User(email='migration-memory@example.invalid', password_hash='test', is_admin=False)
        db.add(owner);db.flush()
        p = Project(owner_user_id=owner.id, slug='same', base_url='https://example.invalid/', site_name='Fixture', settings={})
        db.add(p);db.flush()
        baseline = datetime.now(timezone.utc)-timedelta(days=2)
        t = IssueTask(project_id=p.id, user_id=owner.id, issue_key='missing_title', crawl_ts=baseline.isoformat())
        db.add(t);db.commit();pid, tid = p.id, t.id
    migrate('upgrade', '20261102_0019')
    migrate('upgrade', '20261102_0019')
    schema = inspect(database.engine)
    assert 'correction_observations' in schema.get_table_names()
    assert {tuple(c['column_names']) for c in schema.get_unique_constraints('correction_observations')} == {('observation_key',)}
    assert {c['referred_table'] for c in schema.get_foreign_keys('correction_observations')} == {'projects', 'issue_tasks'}
    assert {i['name'] for i in schema.get_indexes('correction_observations')} == {'ix_correction_observations_project_control'}
    with database.session() as db:
        assert correction_memory.record_control(db, db.get(IssueTask, tid),
                    crawl_ts=(baseline+timedelta(days=1)).isoformat(), result='still_present', baseline_confirmed=True)
        db.commit()
    migrate('downgrade', '20261102_0018')
    assert 'correction_observations' not in inspect(database.engine).get_table_names()
    with database.session() as db:
        assert db.get(Project, pid).slug == 'same' and db.get(IssueTask, tid).issue_key == 'missing_title'
    migrate('upgrade', '20261102_0019')
    database.engine.dispose()
