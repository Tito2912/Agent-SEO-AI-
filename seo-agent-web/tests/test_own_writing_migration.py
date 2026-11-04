"""Additive drafts storage, no authorization on migration, independent rollback."""
import os
from pathlib import Path
import subprocess
import sys
import uuid
from datetime import datetime,timedelta,timezone

import pytest
from sqlalchemy import func,inspect,select
from backend.db import Database
from backend.models import OwnSeoDraft,OwnSeoWorkspace,OwnSeoBrief,Project,User


@pytest.mark.parametrize('precreated',[False,True])
def test_drafts_upgrade_and_rollback_leave_own_project_and_brief_intact(tmp_path,monkeypatch,precreated):
    monkeypatch.setenv('DATABASE_URL','sqlite:///'+str(tmp_path/'writing.db'))
    database=Database(data_dir=tmp_path/'data');web=Path(__file__).resolve().parents[1]
    def migrate(direction,revision):
        result=subprocess.run([sys.executable,'-m','alembic','-c',str(web/'alembic.ini'),direction,revision],
            cwd=web,env=os.environ.copy(),capture_output=True,text=True,timeout=45)
        assert result.returncode==0,result.stderr
    if precreated:database.create_tables()
    migrate('upgrade','20261104_0024')
    with database.session() as db:
        owner=User(email='writing-migration@example.invalid',password_hash='fixture',is_admin=True);db.add(owner);db.flush()
        project=Project(owner_user_id=owner.id,slug='noyaru',site_name='Noyaru',base_url='https://noyaru.com/',settings={});db.add(project);db.flush()
        db.add(OwnSeoWorkspace(id='noyaru',owner_id=owner.id,project_id=project.id,token='a'*32,paused=False))
        brief=OwnSeoBrief(owner_id=owner.id,project_id=project.id,workspace_token='a'*32,candidate_key='b'*64,lang='fr',kind='research',title='Brief',evidence={},status='reviewed')
        db.add(brief);db.commit();ids=owner.id,project.id,brief.id
    migrate('upgrade','20261104_0025');migrate('upgrade','20261104_0025')
    with database.session() as db:
        assert db.scalar(select(func.count()).select_from(OwnSeoDraft))==0
        db.add(OwnSeoDraft(owner_id=ids[0],project_id=ids[1],brief_id=ids[2],workspace_token='a'*32,authorization_key=uuid.uuid4().hex,
            input_hash='c'*64,bundle={},content={},status='queued',provider_calls=0,authorized_until=datetime.now(timezone.utc)+timedelta(hours=24)));db.commit()
    migrate('downgrade','20261104_0024')
    assert not inspect(database.engine).has_table('own_seo_drafts')
    with database.session() as db:
        assert db.get(Project,ids[1]).base_url=='https://noyaru.com/'
        assert db.get(OwnSeoBrief,ids[2]).status=='reviewed'
        assert db.get(OwnSeoWorkspace,'noyaru').token=='a'*32
    migrate('upgrade','20261104_0025');database.engine.dispose()
