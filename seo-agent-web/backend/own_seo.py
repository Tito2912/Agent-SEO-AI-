"""Deterministic own-site planning. No remote provider, crawl or publication tools."""
from datetime import datetime,timedelta,timezone
import hashlib
import html
import json
import math
import uuid
from urllib.parse import urlsplit

from sqlalchemy import select,text

try:
    from . import content_library,first_correction,seo_watch,search_scope
    from .models import OwnSeoWorkspace,OwnSeoBrief,Project,User,AuditLog
except ImportError:
    import content_library
    import first_correction
    import seo_watch
    import search_scope
    from models import OwnSeoWorkspace,OwnSeoBrief,Project,User,AuditLog

BASE='https://noyaru.com/'
KEY='noyaru'
MAX_CANDIDATES=20
FRESH_DAYS=7


def module():
    if __package__:
        from . import app
    else:
        import app
    return app


def clock():
    return datetime.now(timezone.utc)


def digest(value):
    return hashlib.sha256(json.dumps(value,ensure_ascii=True,sort_keys=True,default=str).encode()).hexdigest()


def is_target(value):
    return search_scope.same_property(BASE,value)


def locked(db):
    if db.get_bind().dialect.name=='postgresql':
        key=int.from_bytes(hashlib.sha256(b'noyaru-own-seo-workspace').digest()[:8],'big',signed=True)
        db.execute(text('SELECT pg_advisory_xact_lock(:key)'),{'key':key})
    elif db.get_bind().dialect.name=='sqlite':
        connection=db.connection()
        if not connection.connection.driver_connection.in_transaction:
            connection.exec_driver_sql('BEGIN IMMEDIATE')


def actor(db,owner_id):
    value=db.get(User,owner_id)
    if not module()._user_can_access_system_settings(value):
        raise ValueError('owner_required')
    return value


def scope(db,owner_id):
    actor(db,owner_id)
    workspace=db.get(OwnSeoWorkspace,KEY)
    project=db.get(Project,workspace.project_id) if workspace and workspace.project_id else None
    if (not workspace or workspace.owner_id!=owner_id or not project
            or project.owner_user_id!=owner_id or not is_target(project.base_url)):
        return workspace,None
    return workspace,project


def audit(db,owner_id,action,*,project=None,brief=None,token=''):
    db.add(AuditLog(actor_user_id=owner_id,action='own_seo.'+action,target_type='own_seo',target_id=KEY,
        meta={'project_id':project.id if project else None,'brief_id':brief.id if brief else None,'binding':token}))


def bind(owner_id,project_id,expected_token,*,confirm):
    m=module()
    if not confirm:raise ValueError('confirm_required')
    with m.DB.session() as db:
        locked(db);actor(db,owner_id)
        workspace=db.get(OwnSeoWorkspace,KEY)
        if (workspace.token if workspace else '')!=expected_token:raise ValueError('changed')
        project=db.get(Project,project_id)
        if not project or project.owner_user_id!=owner_id or not is_target(project.base_url):raise ValueError('target_required')
        if workspace and workspace.owner_id==owner_id and workspace.project_id==project_id:
            return workspace.token
        if not workspace:
            workspace=OwnSeoWorkspace(id=KEY,token=uuid.uuid4().hex,paused=False);db.add(workspace)
        workspace.owner_id=owner_id;workspace.project_id=project.id;workspace.token=uuid.uuid4().hex;workspace.paused=False
        audit(db,owner_id,'bind',project=project,token=workspace.token);db.commit()
        return workspace.token


def pause(owner_id,token,paused):
    with module().DB.session() as db:
        locked(db);workspace,project=scope(db,owner_id)
        if not project or workspace.token!=token:raise ValueError('changed')
        workspace.paused=paused;audit(db,owner_id,'pause',project=project,token=token);db.commit()


def public_paths():
    paths={'/','/en','/pricing','/en/pricing','/support','/en/support','/ressources-seo','/en/ressources-seo'}
    for lang in ('fr','en'):
        for page in (content_library.docs_pages()+content_library.blog_pages() if lang=='fr' else content_library.translated_pages('docs',lang)+content_library.translated_pages('blog',lang)):
            paths.add(('/en' if lang=='en' else '')+('/docs/' if page['collection']=='docs' else '/ressources-seo/')+page['slug'])
    return paths


def public_url(value,paths):
    try:
        url=urlsplit(str(value or ''))
        if (url.scheme!='https' or url.hostname!='noyaru.com' or url.port not in (None,443)
                or url.username or url.password or url.query or url.fragment
                or (url.path.rstrip('/') or '/') not in paths):return ''
        return 'https://noyaru.com'+(url.path.rstrip('/') or '/')
    except ValueError:return ''


def number(value):
    if type(value) not in (int,float) or not math.isfinite(value) or value<0:return None
    return float(value)


def clean(value,limit):
    if not isinstance(value,str):return ''
    value=value.strip()
    return value if 1<=len(value)<=limit and not any(ord(c)<32 for c in value) else ''


def view(db,owner_id,*,lang='fr'):
    lang='en' if lang=='en' else 'fr'
    m=module();workspace,project=scope(db,owner_id)
    result={'workspace':workspace,'project':project,'candidates':[],'technical':[],'sources':[],
        'lang':lang,'report_status':'missing','crawl':'','limited':False,'knowledge':seo_watch.catalog(db,lang=lang,now=clock())['sources']}
    if not project:return result
    paths=public_paths();runs=m._runs_dir_for_user(owner_id)
    crawls=m.dash.list_project_crawls(runs,project.slug)
    report=None;ts=''
    for value in reversed(crawls[-20:]):
        if report:=m.dash.load_report_json(runs,project.slug,value):ts=value;break
    result['limited']=len(crawls)>20
    if report:
        meta=report.get('meta') if isinstance(report.get('meta'),dict) else {}
        at=first_correction.timestamp(ts)
        result['crawl']=ts
        if not is_target(meta.get('base_url')):result['report_status']='mismatch'
        elif not at or at>clock()+timedelta(minutes=5) or clock()-at>timedelta(days=FRESH_DAYS):result['report_status']='old'
        elif meta.get('stopped_on_time_budget') or (meta.get('blocked_by_host') if not isinstance(meta.get('blocked_by_host'),dict) else meta['blocked_by_host'].get('count')):result['report_status']='partial'
        else:
            result['report_status']='current'
            issues=report.get('issues') if isinstance(report.get('issues'),dict) else {}
            for key,node in issues.items():
                if key not in m.dash.ISSUE_CATALOG or key.startswith(('bing_','gsc_')) or key in m.dash.DELTA_ISSUE_KEYS:continue
                if not isinstance(node,dict) or not number(node.get('count')):continue
                label=m.dash.issue_meta(key)
                result['technical'].append({'key':key,'label':label.label,'severity':label.severity,'count':int(node['count'])})
            result['technical'].sort(key=lambda row:(m.dash.SEVERITY_ORDER.get(row['severity'],9),-row['count'],row['key']))
            result['technical']=result['technical'][:5]
            for source in ('gsc_api','bing'):
                data=meta.get(source) if isinstance(meta.get(source),dict) else {}
                prop=data.get('property') if source=='gsc_api' else data.get('site_url')
                matched=prop=='sc-domain:noyaru.com' if source=='gsc_api' and str(prop).startswith('sc-domain:') else search_scope.same_property(BASE,prop)
                end=first_correction.timestamp(data.get('end_date'))
                start=first_correction.timestamp(data.get('start_date'))
                valid=bool(data.get('enabled') and data.get('ok') and matched and start and end and start<=end
                    and end-start<=timedelta(days=365) and end<=clock() and clock()-end<=timedelta(days=45))
                result['sources'].append({'source':source,'valid':valid,'start':data.get('start_date') if valid else None,'end':data.get('end_date') if valid else None})
                if not valid:continue
                for dimension in ('pages','queries'):
                    group=data.get(dimension) if isinstance(data.get(dimension),dict) else {}
                    rows=group.get('top') if isinstance(group.get('top'),list) else []
                    result['limited']|=bool(number(group.get('rows')) and number(group['rows'])>len(rows))
                    for row in rows[:10]:
                        if not isinstance(row,dict):continue
                        impressions=number(row.get('impressions'));clicks=number(row.get('clicks'));position=number(row.get('position'))
                        if (impressions is None or impressions<20 or not impressions.is_integer()
                                or clicks is None or not clicks.is_integer() or clicks>impressions
                                or position is None or not 1<=position<=20):continue
                        page=public_url(row.get('page'),paths) if dimension=='pages' else ''
                        query=clean(row.get('query'),180) if dimension=='queries' else ''
                        if (dimension=='pages' and not page) or (dimension=='queries' and not query):continue
                        kind='page' if page else 'research'
                        title=page if page else query
                        evidence={'version':1,'source':source,'property':prop,'crawl':ts,'start':data['start_date'],'end':data['end_date'],
                            'page':page,'query':query,'impressions':int(impressions),'clicks':int(clicks),'position':round(position,2),
                            'catalog':digest(sorted(paths))}
                        result['candidates'].append({'kind':kind,'title':title,'evidence':evidence})
    current={row['key']:row for row in result['knowledge'] if row['status']=='current'}
    seed={'fr':'SEO et GEO : quels résultats mesurer pour une petite entreprise ?',
          'en':'SEO and GEO: which results should a small business measure?'}
    seed_slug='seo-geo-mesurer-sa-visibilite'
    if all(key in current for key in ('google_generative_search','bing_ai_performance')) and not content_library.get_article(seed_slug):
        refs=[{key:value for key,value in current[name].items() if key in {'key','title','url','fingerprint','guidance','checked_at','reviewed_at'}}
              for name in ('google_generative_search','bing_ai_performance')]
        result['candidates'].append({'kind':'guide','title':seed.get(lang,seed['fr']),
            'evidence':{'version':1,'source':'editorial','slug':seed_slug,'references':refs,'catalog':digest(sorted(paths))}})
    result['candidates'].sort(key=lambda row:(0 if row['kind']=='page' else 1 if row['kind']=='research' else 2,
        -row['evidence'].get('impressions',0),row['title']))
    # One question/page produces one brief suggestion; retain the strongest sampled
    # observation as its named source, never add provider volumes together.
    unique={}
    for item in result['candidates']:
        unique.setdefault((item['kind'],' '.join(item['title'].casefold().split())),item)
    result['candidates']=list(unique.values())
    result['limited']|=len(result['candidates'])>MAX_CANDIDATES
    result['candidates']=result['candidates'][:MAX_CANDIDATES]
    for item in result['candidates']:
        stable={**item['evidence']}
        if 'references' in stable:
            stable['references']=[{k:v for k,v in ref.items() if k not in {'checked_at','reviewed_at'}} for ref in stable['references']]
        item['key']=digest([workspace.token,lang,item['kind'],item['title'],stable])
    return result


def prepare(owner_id,token,candidate_key,lang):
    lang='en' if lang=='en' else 'fr'
    m=module()
    with m.DB.session() as db:
        locked(db);state=view(db,owner_id,lang=lang)
        workspace,project=state['workspace'],state['project']
        if not project or workspace.token!=token:raise ValueError('changed')
        if workspace.paused:raise ValueError('paused')
        item=next((item for item in state['candidates'] if item['key']==candidate_key),None)
        if not item:raise ValueError('evidence_changed')
        existing=db.scalar(select(OwnSeoBrief).where(OwnSeoBrief.workspace_token==token,OwnSeoBrief.candidate_key==candidate_key))
        if existing:return existing.id
        brief=OwnSeoBrief(owner_id=owner_id,project_id=project.id,workspace_token=token,candidate_key=candidate_key,
            kind=item['kind'],title=item['title'],evidence=item['evidence'],lang=lang,status='prepared')
        db.add(brief);db.flush();audit(db,owner_id,'prepare',project=project,brief=brief,token=token);db.commit()
        return brief.id


def brief_for(db,owner_id,brief_id):
    workspace,project=scope(db,owner_id)
    brief=db.get(OwnSeoBrief,brief_id)
    if not project or not brief or brief.owner_id!=owner_id or brief.project_id!=project.id or brief.workspace_token!=workspace.token:
        raise ValueError('brief_missing')
    return workspace,project,brief


def review_in_session(db,owner_id,brief_id,token,*,confirm,status):
    workspace,project,brief=brief_for(db,owner_id,brief_id)
    if workspace.token!=token:raise ValueError('changed')
    if status not in {'reviewed','discarded'}:raise ValueError('changed')
    if status=='reviewed':
        if not confirm:raise ValueError('confirm_required')
        if workspace.paused:raise ValueError('paused')
        state=view(db,owner_id,lang=brief.lang)
        if brief.candidate_key not in {row['key'] for row in state['candidates']}:raise ValueError('evidence_changed')
    brief.status=status;brief.reviewed_at=clock() if status=='reviewed' else None
    audit(db,owner_id,'review',project=project,brief=brief,token=token)


def review(owner_id,brief_id,token,*,confirm,status):
    with module().DB.session() as db:
        locked(db);review_in_session(db,owner_id,brief_id,token,confirm=confirm,status=status);db.commit()


def history(db,owner_id,state):
    if not state['project']:return []
    rows=list(db.scalars(select(OwnSeoBrief).where(OwnSeoBrief.owner_id==owner_id,OwnSeoBrief.project_id==state['project'].id,
        OwnSeoBrief.workspace_token==state['workspace'].token).order_by(OwnSeoBrief.created_at.desc()).limit(21)))
    state['limited']|=len(rows)>20
    keys={state['lang']:{item['key'] for item in state['candidates']}}
    for lang in {row.lang for row in rows[:20]}-keys.keys():
        keys[lang]={item['key'] for item in view(db,owner_id,lang=lang)['candidates']}
    return [{'row':row,'current':row.candidate_key in keys[row.lang]} for row in rows[:20]]


def export(brief):
    en=brief.lang=='en';evidence=brief.evidence
    title=html.escape(brief.title)
    for character in '\\`*_[]':title=title.replace(character,'\\'+character)
    text=[('# Editorial brief' if en else '# Brief éditorial')+' — '+title,
        ('Prepared on: ' if en else 'Préparé le : ')+brief.created_at.isoformat(),
        ('Status: ' if en else 'État : ')+brief.status,
        ('This is a brief to research and write, not a publishable article.' if en else 'Ce document est un brief à documenter et à rédiger, pas un article publiable.'),
        '\n## '+('Evidence' if en else 'Preuves'),'```json\n'+json.dumps(evidence,ensure_ascii=True,indent=2)+'\n```',
        '\n## '+('Reader and intent' if en else 'Lecteur et intention'),
        ('Independent professionals and small businesses learning to improve their visibility.' if en else 'Indépendants et petites entreprises qui apprennent à améliorer leur visibilité.'),
        '\n## '+('Suggested outline' if en else 'Plan proposé')]
    outline=(['Clarify the reader’s question','Show a concrete Noyaru example using public product evidence',
        'Explain the practical steps and their limits','Distinguish crawling, indexing, ranking and AI citations',
        'Cite current official sources and identify remaining uncertainties'] if en else
        ['Préciser la question du lecteur','Montrer un exemple concret de Noyaru avec des preuves produit publiques',
        'Expliquer les étapes pratiques et leurs limites','Distinguer exploration, indexation, classement et citations IA',
        'Citer les sources officielles actuelles et signaler les incertitudes'])
    text+=['- '+line for line in outline]
    text+=['\n## '+('Before writing' if en else 'Avant rédaction'),
        ('Check existing resources to improve an existing page rather than duplicate it. Search demand does not prove a content gap. Do not invent volume, results, customer stories or product features. Content effects need later observation and do not prove causality.' if en else
        'Comparer les ressources existantes pour améliorer une page plutôt que la dupliquer. Une recherche observée ne prouve pas un manque de contenu. Ne pas inventer volume, résultat, cas client ou fonction produit. Les effets éditoriaux demandent une observation ultérieure et ne prouvent pas une causalité.'),
        ('Evidence snapshot at preparation. Check current source status and dates before writing.' if en else 'Photographie des preuves à la préparation. Vérifier les dates et l’état actuel des sources avant rédaction.'),
        '\nhttps://noyaru.com/ressources-seo']
    return '\n\n'.join(text)+'\n'
