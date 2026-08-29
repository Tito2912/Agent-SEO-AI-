# Fixture Gatsby — la boucle complète

Quatrième stack passée par la boucle, et **la plus importante des quatre** : c'est celle qui
avait un bug actif. Avant le 2026-08-29, un dépôt Gatsby était détecté comme `next-pages` — le
ciblage était *correct*, `src/pages/*` étant bien la convention de routage de Gatsby — mais le
modèle recevait la consigne d'écrire `next/head`, un module qui n'existe pas ici. **La PR passait
la revue humaine et cassait le build.** Le danger n'était pas « ne sait pas faire », c'était
« se trompe avec assurance ».

## Le défaut injecté

`src/pages/blog.js` déclare `const canonical = '…/blog/'` alors que le site sert `/blog` et
redirige `/blog/` vers lui en 301. Le `<link rel="canonical">` est écrit dans l'export `Head`
(Gatsby Head API) avec l'expression `href={canonical}` ; la valeur littérale est au niveau module.

`src/pages/a-propos.js` est la page témoin. `gatsby-ssr.js` pose `lang="fr"` : Gatsby ne l'émet
pas par défaut et les trois pages étaient signalées `html_lang_attribute_missing` — un vrai défaut
mais pas celui qu'on mesure, et le fixture doit être sain hors du défaut injecté.

## Dérouler la boucle

```bash
cd seo-agent-web/tests/fixtures/gatsby
npm install && npm run build      # le plus lent des quatre fixtures
python ../../static_site_server.py public 8744 &

SEO_AUDIT_ALLOW_PRIVATE_HOSTS=1 python ../../../../skills/public/seo-autopilot/scripts/seo_audit.py \
    http://127.0.0.1:8744/ --sitemap http://127.0.0.1:8744/sitemap.xml --output-dir /tmp/gatsby-before
```

Puis ciblage + réécriture déterministe sur la source, `npm run build`, et recrawl.

## Résultat mesuré (2026-08-29)

| | avant | après |
|---|---|---|
| `canonical_points_to_redirect` | 1 | 0 |
| `redirect_3xx` | 1 | 0 |
| `sitemap_non_canonical_page` | 1 | 0 |

Stack détectée **`gatsby`** (et non plus `next-pages`), idiome **API Head de Gatsby** (et non plus
`next/head`), cible `src/pages/blog.js` seule, **une ligne** réécrite. Pages crawlées 4 → 3.

Aucun trou de réécriture ici : le correctif né du fixture Astro couvrait déjà la forme
`const canonical = …` en JSX. Ce qui restait à prouver sur Gatsby, ce n'était pas la réécriture —
c'était que la **bonne consigne** soit donnée, et que le résultat **compile**.
