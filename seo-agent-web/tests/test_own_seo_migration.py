"""The own-site workspace is additive and never adopts a customer project on upgrade."""
import os
from pathlib import Path
import subprocess
import sys

import pytest
from sqlalchemy import inspect
from backend.db import Database
from backend.models import OwnSeoWorkspace,OwnSeoBrief,Project,User


@pytest.mark.parametrize('precreated',[False,True])
def test_migration_preserves_projects_and_starts_unbound(tmp_path,monkeypatch,precreated):
    monkeypatch.setenv('DATABASE_URL','sqlite:///'+str(tmp_path/'own.db'))
    database=Database(data_dir=tmp_path/'data');web=Path(__file__).resolve().parents[1]
    def migrate(direction,revision):
        result=subprocess.run([sys.executable,'-m','alembic','-c',str(web/'alembic.ini'),direction,revision],
            cwd=web,env=os.environ.copy(),capture_output=True,text=True,timeout=45)
        assert result.returncode==0,result.stderr
    if precreated:database.create_tables()
    migrate('upgrade','20261103_0023')
    with database.session() as db:
        owner=User(email='own-migration@example.invalid',password_hash='fixture',is_admin=True)
        db.add(owner);db.flush();project=Project(owner_user_id=owner.id,slug='noyaru',site_name='Noyaru',base_url='https://noyaru.com/',settings={})
        db.add(project);db.commit();ids=owner.id,project.id
    migrate('upgrade','20261104_0024');migrate('upgrade','20261104_0024')
    assert inspect(database.engine).has_table('own_seo_workspace')
    with database.session() as db:
        assert not db.get(OwnSeoWorkspace,'noyaru')
        db.add(OwnSeoWorkspace(id='noyaru',owner_id=ids[0],project_id=ids[1],token='a'*32,paused=False))
        db.add(OwnSeoBrief(owner_id=ids[0],project_id=ids[1],workspace_token='a'*32,candidate_key='b'*64,
            lang='fr',kind='research',title='Brief',evidence={'source':'test'},status='prepared'))
        db.commit()
    migrate('downgrade','20261103_0023')
    assert not inspect(database.engine).has_table('own_seo_briefs')
    with database.session() as db:
        assert db.get(Project,ids[1]).base_url=='https://noyaru.com/'
        assert db.get(User,ids[0]).email=='own-migration@example.invalid'
    migrate('upgrade','20261104_0024');database.engine.dispose()
