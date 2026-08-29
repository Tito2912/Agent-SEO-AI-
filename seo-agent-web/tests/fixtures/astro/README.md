# Fixture Astro — la boucle complète

Ce dépôt minimal existe pour prouver ce que les arbres de fixtures ne peuvent pas : **qu'un
correctif écrit par l'agent compile encore, et que reconstruire fait vraiment disparaître
l'anomalie.** Le ciblage peut être juste pendant que le résultat est faux — c'est exactement ce
qui est arrivé sur Gatsby, où le bon fichier recevait du code d'un autre framework.

## Le défaut injecté

`src/pages/blog.astro` déclare `const canonical = '…/blog/'` alors que le site sert `/blog` et
redirige `/blog/` vers lui en 301. C'est le défaut trouvé sur un vrai site client
(voiceoverstudioai.com, PR#1) : **le correctif juste consiste à retirer un seul caractère.**

Tout le reste du site est délibérément sain — descriptions au-dessus du seuil, Open Graph et
Twitter complets, sitemap et robots présents — pour que « zéro anomalie à la fin » soit une
preuve et non une coïncidence. `src/pages/a-propos.astro` est la page témoin : un correctif qui
la touche est trop large. `src/layouts/Base.astro` est le gabarit partagé : y écrire le canonical
d'une page le poserait sur toutes.

## Dérouler la boucle

```bash
cd seo-agent-web/tests/fixtures/astro
npm install && npm run build

# servir la sortie comme le ferait un hébergeur statique (URLs propres + 301 sur /x/)
python ../../static_site_server.py dist 8741 &

SEO_AUDIT_ALLOW_PRIVATE_HOSTS=1 python ../../../../skills/public/seo-autopilot/scripts/seo_audit.py \
    http://127.0.0.1:8741/ --sitemap http://127.0.0.1:8741/sitemap.xml --output-dir /tmp/astro-before
```

`SEO_AUDIT_ALLOW_PRIVATE_HOSTS=1` n'est pas optionnel et son absence est silencieuse : le garde
SSRF laisse passer les pages (Playwright) mais bloque robots.txt et le sitemap, donc le crawl a
l'air de réussir en sautant les pages qui ne sont listées que dans le sitemap.

Ensuite : appliquer le ciblage et la réécriture déterministe sur la SOURCE, `npm run build` à
nouveau, et recrawler.

## Résultat mesuré (2026-08-29)

| | avant | après |
|---|---|---|
| `canonical_points_to_redirect` | 1 | 0 |
| `redirect_3xx` | 1 | 0 |
| `sitemap_non_canonical_page` | 1 | 0 |

Le ciblage a retenu `src/pages/blog.astro` **seul** — ni le layout partagé, ni la page témoin — et
la réécriture a changé **une ligne**. Le nombre de pages crawlées passe de 4 à 3 : `/blog/`
n'existe plus comme URL distincte, ce qui est le bon résultat et non une page perdue.

## Ce que la boucle a trouvé

Au premier passage, la réécriture déterministe a fait **0 remplacement** alors que le ciblage
était parfait. En Astro le `<link rel="canonical">` vit dans le layout sous forme
`href={canonical}` et la valeur est une **affectation** dans la page. `_JS_CANONICAL_RE` ne
connaissait que la forme propriété `canonical:`, donc la famille basculait en silence sur le
repli IA — perdant son badge « correctif mécanique » sur la façon la plus idiomatique d'écrire un
canonical en Astro. Corrigé : le motif accepte maintenant `canonical:`, `canonical =` et
`canonical="…"`, et rejette `data-canonical=` et `mycanonical:` (que l'ancien acceptait à tort).
