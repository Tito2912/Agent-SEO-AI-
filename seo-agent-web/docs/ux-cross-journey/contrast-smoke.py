from pathlib import Path
exec(Path(__file__).with_name('fixture.py').read_text())
import asyncio
from playwright.async_api import async_playwright
OUT=Path('/tmp/noyaru-cross-browser');OUT.mkdir(exist_ok=True)
JS=r'''els=>els.filter(e=>e.checkVisibility()&&e.textContent.trim()).map(e=>{
 const rgba=s=>{const m=s.match(/[\d.]+/g);return m?m.map(Number):[0,0,0,0]};
 const blend=(f,b)=>f.slice(0,3).map((x,i)=>x*(f[3]??1)+b[i]*(1-(f[3]??1)));
 const chain=[];for(let n=e;n;n=n.parentElement)chain.push(n);
 let bg=[255,255,255],gradient=false;for(const n of chain.reverse()){const s=getComputedStyle(n);if(s.backgroundImage!=='none')gradient=true;bg=blend(rgba(s.backgroundColor),bg)}
 const lum=rgb=>rgb.map(x=>{x/=255;return x<=.04045?x/12.92:((x+.055)/1.055)**2.4}).reduce((s,x,i)=>s+x*[.2126,.7152,.0722][i],0);
 const fg=blend(rgba(getComputedStyle(e).color),bg),a=lum(fg),b=lum(bg);return {text:e.textContent.trim().slice(0,90),ratio:(Math.max(a,b)+.05)/(Math.min(a,b)+.05),gradient,height:e.getBoundingClientRect().height}
})'''
async def main():
 rows=[];controls=[]
 async with async_playwright() as p:
  b=await p.chromium.launch(executable_path='/home/aron/.cache/ms-playwright/chromium-1223/chrome-linux64/chrome',args=['--no-sandbox'])
  for width in (1440,768,390,360):
   for lang in ('fr','en'):
    for kind in (None,'owner'):
     c=await b.new_context(viewport={'width':width,'height':1100},locale=lang)
     async def guard(route):
      assert route.request.method in ('GET','HEAD')
      if route.request.url.startswith(base+'/'):await route.continue_()
      else:await route.abort()
     await c.route('**/*',guard)
     if kind:await c.add_cookies([cookie(kind)])
     page=await c.new_page();await page.goto(base+('/en' if lang=='en' else '')+'/support')
     samples=await page.locator('.pub-nav-actions .btn,.pub-nav-links a,#cookie-banner span,#cookie-banner a,#cookie-banner button').evaluate_all(JS)
     for x in samples:x.update(lang=lang,width=width,screen='public-'+('connected' if kind else 'anonymous'));rows.append(x)
     controls += [{'screen':x['screen'],'text':x['text'],'width':width,'height':x['height']} for x in samples if x['text'] not in ('Ce site utilise uniquement des cookies nécessaires au fonctionnement du service. Aucun cookie ',) and x['height']<44 and not x['text'].startswith(('This site','Ce site'))]
     if width==360 and kind=='owner' and lang=='fr':await page.locator('#cookie-accept').click();await page.screenshot(path=str(OUT/'support-connected-fr-360.png'))
     if kind:
      await page.goto(base+'/billing?project=atelier&msg=Paiement%20confirm%C3%A9.&lang='+lang)
      samples=await page.locator('.feedback-dismiss,.billing-scope .btn').evaluate_all(JS)
      for x in samples:x.update(lang=lang,width=width,screen='billing-return-and-confirmation');rows.append(x);assert x['height']>=44
      if width==360 and lang=='en':await page.screenshot(path=str(OUT/'billing-long-site-en-360.png'))
     await c.close()
  await b.close()
 bad=[x for x in rows if x['gradient'] or x['ratio']<4.5];assert not bad,bad;assert not controls,controls
 data={'samples':rows,'minimum_text_ratio':min(x['ratio'] for x in rows),'failures':bad,'notes':['Échantillons des textes de navigation commune, information cookies, bouton de retour long et fermeture de confirmation. Pas un audit WCAG complet.','Couleurs calculées dans Chromium avec composition des fonds RGBA. Aucun dégradé dans les échantillons. Cible texte 4,5:1 ; boutons/liens modifiés au moins 44 px.']}
 (OUT/'contrast.json').write_text(json.dumps(data,ensure_ascii=False,indent=2));print(json.dumps({'samples':len(rows),'minimum':data['minimum_text_ratio'],'failures':len(bad)}))
asyncio.run(main());server.should_exit=True
