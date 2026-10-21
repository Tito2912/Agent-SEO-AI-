from pathlib import Path
exec(Path(__file__).with_name('fixture.py').read_text())
import asyncio
from urllib.parse import urlsplit,urlunsplit,parse_qsl,urlencode
from playwright.async_api import async_playwright,expect
from backend.models import IssueTask
OUT=Path('/tmp/noyaru-cross-browser');OUT.mkdir(exist_ok=True)
async def main():
    checks=[];errors=[];network={'external':0,'posts':[]};failures=[]
    async with async_playwright() as p:
        b=await p.chromium.launch(executable_path='/home/aron/.cache/ms-playwright/chromium-1223/chrome-linux64/chrome',args=['--no-sandbox'])
        async def guard(route):
            req=route.request;path=urlsplit(req.url).path
            if not req.url.startswith(base+'/'):network['external']+=1;await route.abort()
            elif req.method not in ('GET','HEAD') and path!='/projects/atelier/first-correction':raise AssertionError('Unexpected mutation '+path)
            else:
                if req.method=='POST':network['posts'].append(path)
                await route.continue_()
        async def context(lang,width,kind=None,notice=False,js=True):
            c=await b.new_context(viewport={'width':width,'height':900},locale='fr-FR' if lang=='fr' else 'en-GB',reduced_motion='reduce',java_script_enabled=js)
            await c.route('**/*',guard)
            if kind:await c.add_cookies([cookie(kind)])
            if not notice:await c.add_init_script("try { localStorage.setItem('cookie_consent','1') } catch {}")
            page=await c.new_page();page.on('pageerror',lambda e:errors.append(str(e)));return c,page
        async def visit(page,path,lang,width,screen,expected=200):
            u=urlsplit(path);query=dict(parse_qsl(u.query));query['lang']=lang
            path=urlunsplit(('', '',u.path,urlencode(query),u.fragment));r=await page.goto(base+path)
            item={'screen':screen,'path':u.path,'lang':lang,'width':width,'status':r.status,'final_path':urlsplit(page.url).path,'overflow':await page.evaluate('document.documentElement.scrollWidth>innerWidth'),'html_lang':await page.locator('html').get_attribute('lang')}
            if r.status!=expected or item['overflow'] or item['html_lang']!=lang:failures.append(item)
            checks.append(item);(OUT/'progress.json').write_text(json.dumps(checks,ensure_ascii=False))
            return r
        public=['/support','/status','/docs','/docs/demarrer','/ressources-seo','/terms','/privacy','/mentions-legales','/pricing']
        # Follow every actual sidebar link, rather than inventing routes from menu names.
        c,page=await context('fr',1440,'owner');await page.goto(base+'/projects/atelier?lang=fr')
        links=await page.locator('.sidebar a[href]').evaluate_all("nodes=>Array.from(new Set(nodes.map(a=>a.getAttribute('href')).filter(p=>p.startsWith('/')&&!p.startsWith('/auth/'))))")
        links=[x for x in links if not x.startswith('/settings/activation') and x not in ('/automation','/settings/system','/settings/operations')]
        await c.close()
        for width in (1440,768,390,360):
            for lang in ('fr','en'):
                for kind in (None,'owner'):
                    c,page=await context(lang,width,kind)
                    for path in public:
                        localized='/en'+path if lang=='en' else path
                        await visit(page,localized,lang,width,'public-'+('connected' if kind else 'anonymous'))
                    if kind is None:await visit(page,'/en/' if lang=='en' else '/',lang,width,'public-home')
                    if kind and width==360 and lang=='en':
                        await visit(page,'/en/support',lang,width,'public-keyboard')
                        await page.keyboard.press('Control+Home');await page.locator('.skip-link').focus();await page.keyboard.press('Enter');await expect(page.locator('#main-content')).to_be_focused()
                        await page.evaluate('scrollTo(0,0)');await page.screenshot(path=str(OUT/'support-connected-en-360.png'))
                    await c.close()
                c,page=await context(lang,width,'owner')
                for path in links:await visit(page,path,lang,width,'client-menu')
                for path in ('/projects/sans-analyse','/projects/atelier/first-correction?choose=1','/projects/atelier/issues/missing_title'):
                    await visit(page,path,lang,width,'client-transition')
                if width<980:
                    await page.locator('.nav-mobile-bouton').click();await expect(page.locator('.sidebar')).to_be_visible();await page.keyboard.press('Escape');await expect(page.locator('#nav-mobile')).not_to_be_checked()
                    checks.append({'screen':'mobile-menu-keyboard','lang':lang,'width':width,'opened_and_closed':True})
                await visit(page,'/billing?msg=Paiement%20confirm%C3%A9.',lang,width,'confirmation')
                button=page.locator('[data-dismiss-feedback]');await expect(button).to_have_text('Close' if lang=='en' else 'Fermer');assert 'confirmation' in await button.get_attribute('aria-label');assert (await button.bounding_box())['height']>=44
                if width==360:await page.wait_for_timeout(5500);await expect(button).to_be_visible()
                await button.focus();await page.keyboard.press('Enter');await expect(button).to_have_count(0);assert await page.locator('.billing-scope').evaluate('e=>e===document.activeElement')
                checks.append({'screen':'confirmation-dismiss-keyboard','lang':lang,'width':width,'localized':True,'focus_preserved':True});await c.close()
                c,page=await context(lang,width,'new');await visit(page,'/',lang,width,'new-account-no-site');await expect(page.locator('#first-audit-url')).to_be_visible();await c.close()
        # Every versioned public article is displayed once, in its real route language.
        c,page=await context('fr',390)
        for directory,prefix in [('docs','/docs'),('blog','/ressources-seo')]:
            for source in sorted((ROOT/'content'/directory).rglob('*.md')):
                lang='en' if source.parent.name=='en' else 'fr';path=('/en' if lang=='en' else '')+prefix+'/'+source.stem
                await visit(page,path,lang,390,'public-article')
        await c.close()
        for lang in ('fr','en'):
            for width in (1440,360):
                c,page=await context(lang,width,'member')
                for path in ('/projects/atelier','/projects/atelier/corrections','/projects/atelier/settings/crawl','/billing?project=atelier&return_to=%2Fprojects%2Fatelier%2Fissues'):
                    await visit(page,path,lang,width,'shared-project')
                await c.clear_cookies();await visit(page,'/projects/atelier/issues',lang,width,'expired-session');assert urlsplit(page.url).path=='/auth/login';await c.close()
                c,page=await context(lang,width,'admin')
                for path in ('/automation','/settings/system','/settings/operations','/settings/activation','/settings/activation/accounts','/settings/activation/accounts/'+users['owner']):
                    await visit(page,path,lang,width,'administration')
                await c.close()
                c,page=await context(lang,width,notice=True,js=False)
                for path in ('/support','/docs','/pricing','/terms'):
                    await visit(page,('/en'+path if lang=='en' else path),lang,width,'public-without-javascript')
                    assert await page.locator('#cookie-banner').evaluate('e=>getComputedStyle(e).position')=='static';await expect(page.locator('#cookie-accept')).to_be_hidden()
                if lang=='fr' and width==360:await page.locator('#cookie-banner').screenshot(path=str(OUT/'notice-nojs-fr-360.png'))
                await c.close()
            # Cookie information closes and is remembered; storage refusal still allows closing.
            for blocked in (False,True):
                c,page=await context(lang,360,notice=True)
                if blocked:await c.add_init_script("Object.defineProperty(window,'localStorage',{get(){throw new DOMException('Blocked','SecurityError')}})")
                await visit(page,'/en/support' if lang=='en' else '/support',lang,360,'cookie-storage-blocked' if blocked else 'cookie-new-browser')
                close=page.locator('#cookie-accept');await expect(close).to_be_visible();assert (await close.bounding_box())['height']>=44;await close.focus();await page.keyboard.press('Enter');await expect(page.locator('#cookie-banner')).to_have_count(0);await expect(page.locator('#main-content')).to_be_focused()
                if not blocked:await page.reload();await expect(page.locator('#cookie-banner')).to_have_count(0)
                checks.append({'screen':'cookie-dismiss-keyboard','lang':lang,'storage_blocked':blocked,'focus_preserved':True});await c.close()
        # Complete one manual journey against real local handlers; analysis itself is simulated.
        c,page=await context('fr',390,'member');await visit(page,'/projects/atelier/first-correction?choose=1','fr',390,'correction-select')
        form=page.locator('form').filter(has=page.locator('input[name=issue_key][value=missing_title]'));await form.locator('button[type=submit]').click();await page.wait_for_url('**/first-correction?**')
        with m.DB.session() as db:
            task=db.scalar(select(IssueTask).where(IssueTask.issue_key=='missing_title'));assert task.status=='todo';tid=str(task.id)
        ready=page.locator('form').filter(has=page.locator('input[name=action][value=ready]'));await ready.locator('button').click();await expect(page.locator('#verification form[action$="/crawl"]')).to_be_visible()
        with m.DB.session() as db:assert db.get(IssueTask,tid).status=='in_progress'
        checks.append({'screen':'manual-change-awaits-analysis','lang':'fr','status_is_not_verified':True})
        ts=(datetime.now(timezone.utc)+timedelta(minutes=2)).strftime('%Y%m%d-%H%M%S');payload=report(users['owner'],ts,{'missing_meta_description':{'count':1,'examples':[URL]}})
        m._verify_corrections_after_crawl('atelier',payload,runs/users['owner'],user_id=users['owner'],crawl_ts=ts)
        await page.reload();await expect(page.locator('.first-correction-result')).to_be_visible();await page.locator('.first-correction-result a[href$="?choose=1"]').click();await expect(page.locator('.first-correction-choice')).to_have_count(1)
        assert await page.locator('input[name=issue_key][value=missing_title]').count()==0
        checks.append({'screen':'verified-result-to-next-correction','lang':'fr','analysis_simulated':True,'previous_result_preserved':True});await c.close()
        assert not errors,errors
        data={'base_commit':'d7f4f24','checks':checks,'failures':failures,'javascript_errors':errors,'network':network,'http_routes_loaded':len([r for r in m.app.routes if getattr(r,'methods',None)]),'client_menu_paths':links,'notes':['Comptes, base et rapports synthétiques. Aucun OAuth, paiement, email ou écriture de site réel.','Les seuls POST locaux choisissent une correction et déclarent une modification manuelle fictive. La nouvelle analyse est injectée par la fonction de vérification réelle avec un rapport simulé.','Aucun POST de crawl, modèle ou fournisseur. Le passage à la correction suivante ne choisit pas automatiquement la page.']}
        (OUT/'browser.json').write_text(json.dumps(data,ensure_ascii=False,indent=2));print(json.dumps({'checks':len(checks),'failures':failures,'errors':errors,'network':network,'routes':data['http_routes_loaded']}));await b.close()
asyncio.run(main());server.should_exit=True
