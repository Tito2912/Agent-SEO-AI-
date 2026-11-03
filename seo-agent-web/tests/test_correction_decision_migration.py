"""Decision storage leaves the existing controls and knowledge untouched."""
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import inspect

from backend import correction_memory as memory
from backend.db import Database
from backend.models import CorrectionObservation, IssueTask, Project, User


@pytest.mark.parametrize('precreated',[False,True])
def test_decision_upgrade_downgrade_preserves_existing_data(tmp_path,monkeypatch,precreated):
    monkeypatch.setenv('DATABASE_URL','sqlite:///'+str(tmp_path/'decisions.db'))
    database=Database(data_dir=tmp_path/'data');web=Path(__file__).resolve().parents[1]
    def migrate(direction,revision):
        result=subprocess.run([sys.executable,'-m','alembic','-c',str(web/'alembic.ini'),direction,revision],
                   cwd=web,env=os.environ.copy(),capture_output=True,text=True,timeout=45)
        assert result.returncode==0,result.stderr
    if precreated:database.create_tables()
    migrate('upgrade','20261102_0020')
    with database.session() as db:
        owner=User(email='decision-migration@example.invalid',password_hash='fixture',is_admin=False)
        db.add(owner);db.flush()
        p=Project(owner_user_id=owner.id,slug='preserved',site_name='Fixture',base_url='https://example.invalid/',settings={})
        db.add(p);db.flush()
        before=datetime.now(timezone.utc)-timedelta(days=3)
        t=IssueTask(project_id=p.id,user_id=owner.id,issue_key='missing_title',url='https://example.invalid/',crawl_ts=before.isoformat(),status='in_progress')
        db.add(t);db.flush()
        memory.record_control(db,t,crawl_ts=(before+timedelta(days=1)).isoformat(),result='still_present',baseline_confirmed=True)
        db.commit();tid,pid,uid=t.id,p.id,owner.id;oid=db.query(CorrectionObservation).one().id
    migrate('upgrade','20261103_0021');migrate('upgrade','20261103_0021')
    assert 'correction_decisions' in inspect(database.engine).get_table_names()
    with database.session() as db:
        t=db.get(IssueTask,tid)
        memory.record_control(db,t,crawl_ts=(before+timedelta(days=2)).isoformat(),result='still_present',
                              baseline_confirmed=True,scope_urls={'https://example.invalid/'})
        db.commit()
    migrate('downgrade','20261102_0020')
    assert 'correction_decisions' not in inspect(database.engine).get_table_names()
    assert 'seo_knowledge_sources' in inspect(database.engine).get_table_names()
    with database.session() as db:
        assert db.get(User,uid).email=='decision-migration@example.invalid'
        assert db.get(Project,pid).slug=='preserved'
        assert db.get(CorrectionObservation,oid).result=='still_present'
    migrate('upgrade','20261103_0021');database.engine.dispose()
