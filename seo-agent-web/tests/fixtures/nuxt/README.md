# Fixture Nuxt — la boucle complète

Sixième et dernière stack Node passée par la boucle. Nuxt est le seul des sept où le `<head>`
n'est ni du HTML ni du front matter mais un **appel de fonction** : `useHead({ link: [{ rel:
'canonical', href: canonical }] })`. La valeur reste une affectation au niveau du `<script setup>`,
ce qui est exactement ce que le correctif né du fixture Astro couvre.

## Le défaut injecté

`pages/blog.vue` déclare `const canonical = '…/blog/'` alors que le site sert `/blog` et redirige
`/blog/` vers lui en 301. `pages/a-propos.vue` est la page témoin ; `app.vue` est le composant
racine partagé.

`nuxt.config.ts` pose `app.head.htmlAttrs.lang` : Nuxt n'émet pas de `lang` sans ça et les trois
pages étaient signalées `html_lang_attribute_missing` — un vrai défaut, mais pas celui qu'on
mesure.

## Dérouler la boucle

```bash
cd seo-agent-web/tests/fixtures/nuxt
npm install --legacy-peer-deps    # npm 10.8.2 plante sinon (bug arborist 'edgesOut'),
                                  # ce n'est pas un probleme Nuxt
npm run build                     # nuxt generate -> .output/public
python ../../static_site_server.py .output/public 8746 &

SEO_AUDIT_ALLOW_PRIVATE_HOSTS=1 python ../../../../skills/public/seo-autopilot/scripts/seo_audit.py \
    http://127.0.0.1:8746/ --sitemap http://127.0.0.1:8746/sitemap.xml --output-dir /tmp/nx-before
```

## Résultat mesuré (2026-08-29)

`canonical_points_to_redirect`, `redirect_3xx` et `sitemap_non_canonical_page` : **1 → 0** chacun.
Stack `nuxt`, idiome `useHead()`, cible `pages/blog.vue` seule — **jamais `app.vue`** — une ligne.
Pages 4 → 3. Aucun trou de réécriture.
