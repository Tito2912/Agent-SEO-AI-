"""Mandate storage is additive and can be rolled back without losing controls."""
import os
from pathlib import Path
import subprocess
import sys

import pytest
from sqlalchemy import inspect

from backend.db import Database
from backend.models import AutocorrectionRun, CorrectionObservation, IssueTask, Project, User
from backend import correction_memory as memory
from test_correction_memory import BASE, CONTROL, URL, stamp


@pytest.mark.parametrize('precreated', [False, True])
def test_publication_upgrade_downgrade_retains_preview_and_controls(tmp_path, monkeypatch, precreated):
    monkeypatch.setenv('DATABASE_URL', 'sqlite:///'+str(tmp_path/'executor.db'))
    database = Database(data_dir=tmp_path/'data');web = Path(__file__).resolve().parents[1]
    def migrate(direction, revision):
        result = subprocess.run([sys.executable,'-m','alembic','-c',str(web/'alembic.ini'),direction,revision],
                                cwd=web, env=os.environ.copy(), capture_output=True, text=True, timeout=45)
        assert result.returncode == 0, result.stderr
    if precreated:
        database.create_tables()
    migrate('upgrade', '20261103_0021')
    with database.session() as db:
        owner = User(email='mandate-migration@example.invalid', password_hash='fixture', is_admin=False)
        db.add(owner);db.flush()
        p = Project(owner_user_id=owner.id, slug='preserved', site_name='Fixture', base_url='https://atelier.example/', settings={})
        db.add(p);db.flush()
        t = IssueTask(project_id=p.id, user_id=owner.id, issue_key='missing_title', url=URL, crawl_ts=stamp(BASE), status='in_progress')
        db.add(t);db.flush()
        memory.record_control(db,t,crawl_ts=stamp(CONTROL),result='still_present',baseline_confirmed=True,scope_urls={URL})
        db.commit();ids=(owner.id,p.id,t.id);oid=db.query(CorrectionObservation).one().id
    migrate('upgrade', '20261103_0023');migrate('upgrade', '20261103_0023')
    schema = inspect(database.engine)
    assert schema.has_table('correction_publications')
    assert {tuple(v['column_names']) for v in schema.get_unique_constraints('correction_publications')} == {('run_id',)}
    assert {tuple(v['column_names']) for v in schema.get_unique_constraints('autocorrection_runs')} == {('task_id','application_key')}
    with database.session() as db:
        db.add(AutocorrectionRun(project_id=ids[1],task_id=ids[2],owner_id=ids[0],application_key='a'*64,
            decision_key='b'*64,config_key='c'*64,policy_version=1,consent_version=1,lang='fr',
            state='queued',reason='authorized',expires_at=CONTROL,limits={'credits':1},reserved_credits=0,
            provider_calls=0,input_chars=0,output_tokens=0))
        db.commit()
    migrate('downgrade', '20261103_0022')
    assert not inspect(database.engine).has_table('correction_publications')
    assert inspect(database.engine).has_table('autocorrection_runs')
    with database.session() as db:
        assert db.get(User,ids[0]).email == 'mandate-migration@example.invalid'
        assert db.get(Project,ids[1]).slug == 'preserved'
        assert db.get(IssueTask,ids[2]).issue_key == 'missing_title'
        assert db.get(CorrectionObservation,oid).result == 'still_present'
    migrate('upgrade', '20261103_0023');database.engine.dispose()
