# Fixture SvelteKit — la boucle complète

Troisième stack passée par la boucle, et la première des deux que le détecteur ne reconnaissait
pas du tout avant le 2026-08-29 : SvelteKit était lu comme du HTML statique, et l'agent proposait
`src/app.html` — le gabarit de **tout** le site — pour corriger le titre d'une seule page.

## Le défaut injecté

`src/routes/blog/+page.svelte` déclare `const canonical = '…/blog/'` alors que le site sert
`/blog` et redirige `/blog/` vers lui en 301. Le `<link rel="canonical">` est dans le
`<svelte:head>` de la page mais avec une **expression** `href={canonical}` : la valeur littérale
est dans le `<script>`, comme en Astro.

`src/routes/a-propos/+page.svelte` est la page témoin. `src/routes/+layout.svelte` est le layout
partagé. `src/app.html` est le shell du site — `_SHELL_FILES` l'exclut explicitement de la carte
des routes.

Le site est entièrement prérendu (`adapter-static`, `prerender = true`) : le crawler doit voir un
site construit, exactement ce qu'un client déploie.

## Dérouler la boucle

```bash
cd seo-agent-web/tests/fixtures/sveltekit
npm install && npm run build
python ../../static_site_server.py build 8743 &

SEO_AUDIT_ALLOW_PRIVATE_HOSTS=1 python ../../../../skills/public/seo-autopilot/scripts/seo_audit.py \
    http://127.0.0.1:8743/ --sitemap http://127.0.0.1:8743/sitemap.xml --output-dir /tmp/sk-before
```

Puis ciblage + réécriture déterministe sur la source, `npm run build`, et recrawl.

## Résultat mesuré (2026-08-29)

| | avant | après |
|---|---|---|
| `canonical_points_to_redirect` | 1 | 0 |
| `redirect_3xx` | 1 | 0 |
| `sitemap_non_canonical_page` | 1 | 0 |

Stack détectée `sveltekit`, 3 routes mappées depuis les `+page.svelte` — **ni `+layout.svelte`,
ni `src/app.html`** — cible `src/routes/blog/+page.svelte` seule, **une ligne** réécrite.

**Aucun trou trouvé**, comme pour Hugo : le correctif du réécriveur né d'Astro couvrait déjà la
forme `const canonical = …` dans un `<script>` Svelte. Deuxième stack consécutive à venir
gratuitement.
