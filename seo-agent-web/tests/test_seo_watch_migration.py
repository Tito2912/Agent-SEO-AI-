"""Knowledge schema is additive and does not change customer correction history."""
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import inspect

from backend import correction_memory, seo_watch
from backend.db import Database
from backend.models import CorrectionObservation, IssueTask, Project, User


@pytest.mark.parametrize('precreated',[False,True])
def test_upgrade_and_downgrade_keep_accounts_projects_and_observations(tmp_path,monkeypatch,precreated):
    monkeypatch.setenv('DATABASE_URL','sqlite:///'+str(tmp_path/'watch.db'))
    database=Database(data_dir=tmp_path/'data');web=Path(__file__).resolve().parents[1]
    def migrate(direction,revision):
        out=subprocess.run([sys.executable,'-m','alembic','-c',str(web/'alembic.ini'),direction,revision],
                           cwd=web,env=os.environ.copy(),capture_output=True,text=True,timeout=45)
        assert out.returncode==0,out.stderr
    if precreated:database.create_tables()
    migrate('upgrade','20261102_0019')
    with database.session() as db:
        owner=User(email='watch-migration@example.invalid',password_hash='fixture',is_admin=False)
        db.add(owner);db.flush()
        p=Project(owner_user_id=owner.id,slug='unchanged',base_url='https://example.invalid/',site_name='Fixture',settings={})
        db.add(p);db.flush()
        before=datetime.now(timezone.utc)-timedelta(days=2)
        t=IssueTask(project_id=p.id,user_id=owner.id,issue_key='missing_title',crawl_ts=before.isoformat())
        db.add(t);db.flush()
        correction_memory.record_control(db,t,crawl_ts=(before+timedelta(days=1)).isoformat(),result='still_present',baseline_confirmed=True)
        db.commit();pid,uid=p.id,owner.id
        oid=db.query(CorrectionObservation).first().id
    migrate('upgrade','20261102_0020');migrate('upgrade','20261102_0020')
    schema=inspect(database.engine)
    assert {'seo_knowledge_sources','seo_knowledge_revisions'}<=set(schema.get_table_names())
    assert set(schema.get_pk_constraint('seo_knowledge_revisions')['constrained_columns'])=={'source_key','fingerprint'}
    assert seo_watch.refresh_source(database,seo_watch.SOURCES[0].key,
               fetcher=lambda s:{'fingerprint':'a'*64,'text_chars':1000,'excerpt':'Public source.'})=='checked'
    migrate('downgrade','20261102_0019')
    assert 'seo_knowledge_sources' not in inspect(database.engine).get_table_names()
    with database.session() as db:
        assert db.get(User,uid).email=='watch-migration@example.invalid'
        assert db.get(Project,pid).slug=='unchanged'
        assert db.get(CorrectionObservation,oid).result=='still_present'
    migrate('upgrade','20261102_0020');database.engine.dispose()
