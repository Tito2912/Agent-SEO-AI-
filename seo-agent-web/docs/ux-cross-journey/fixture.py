# Synthetic local-only users, reports and provider configuration.
import os,sys,tempfile,socket,threading,time,logging,json
from pathlib import Path
from datetime import datetime,timedelta,timezone
ROOT=Path(__file__).resolve().parents[2]
DATA=Path(tempfile.mkdtemp(prefix='noyaru-cross-'))
sys.path.insert(0,str(ROOT));os.environ.update(SEO_AGENT_DATA_DIR=str(DATA/'data'),DATABASE_URL='',SEO_AGENT_DISABLE_WORKER='true',SEO_AGENT_SECRET_KEY='cross-local-only',PLAN_CONFIG_JSON='',SIGNUP_DISABLED='',SIGNUP_INVITE_CODE='')
from backend import app as m,auth
from backend.models import User,Project,AccountMember,BillingCustomer,BillingSubscription
from sqlalchemy import select
import uvicorn
logging.disable(logging.CRITICAL);m.DB.create_tables();m._email_verification_enabled=lambda:False
m.billing.stripe_enabled=lambda:False;m._assistant_effective_provider=lambda:'none'
with m.DB.session() as db:
    users={}
    for kind in ('owner','member','admin','new','stranger'):
        u=User(email=kind+'@cross.example.invalid',password_hash='local-only',country='FR',is_admin=kind=='admin');db.add(u);db.flush();users[kind]=str(u.id)
    db.add(BillingCustomer(user_id=users['owner'],stripe_customer_id='cus_cross'))
    db.add(BillingSubscription(user_id=users['owner'],stripe_customer_id='cus_cross',stripe_subscription_id='sub_cross',stripe_price_id='price_cross',plan_key='pro',status='active'))
    for slug,name in [('atelier','Atelier de réparation & services'),('sans-analyse','Site à analyser')]:
        db.add(Project(owner_user_id=users['owner'],slug=slug,site_name=name,base_url='https://atelier.example/',settings={}))
    db.add(AccountMember(owner_user_id=users['owner'],member_user_id=users['member']));db.commit()
os.environ['SYSTEM_SETTINGS_OWNER_EMAIL']='admin@cross.example.invalid'
runs=DATA/'runs';m._runs_dir_for_user=lambda uid:runs/uid;m.DEFAULT_RUNS_DIR=runs
baseline=(datetime.now(timezone.utc)-timedelta(days=1)).strftime('%Y%m%d-%H%M%S')
URL='https://atelier.example/service/'
def report(uid,ts,issues=None):
    payload={'meta':{'base_url':'https://atelier.example/','site_name':'Atelier de réparation & services'},'pages':[{'url':URL,'status_code':200,'content_type':'text/html','indexable':True,'title':'Service local'}],'issues':issues if issues is not None else {'missing_title':{'count':1,'examples':[URL]},'missing_meta_description':{'count':1,'examples':[URL]}}}
    audit=runs/uid/'atelier'/ts/'audit';audit.mkdir(parents=True,exist_ok=True);(audit/'report.json').write_text(json.dumps(payload));(audit.parent/'run.json').write_text(json.dumps({'site_name':'Atelier de réparation & services','base_url':'https://atelier.example/'}))
    (audit/'fix-suggestions.json').write_text(json.dumps(m.fix_suggestions.build_fix_suggestions_payload(report=payload,slug='atelier',timestamp=ts,site_name='Atelier de réparation & services',base_url='https://atelier.example/')))
    return payload
report(users['owner'],baseline)
sock=socket.socket();sock.bind(('127.0.0.1',0));base='http://127.0.0.1:'+str(sock.getsockname()[1]);server=uvicorn.Server(uvicorn.Config(m.app,log_level='critical',access_log=False));threading.Thread(target=lambda:server.run(sockets=[sock]),daemon=True).start()
for _ in range(200):
    if server.started:break
    time.sleep(.05)
def cookie(kind):return {'name':auth.SESSION_COOKIE_NAME,'value':auth.make_session_token(user_id=users[kind],secret=os.environ['SEO_AGENT_SECRET_KEY']),'url':base}
