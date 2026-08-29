# Fixture Next.js Pages Router — la boucle complète

Cinquième stack passée par la boucle. Next **App** Router était déjà validé en production sur
trois sites clients ; **Pages Router est un routeur différent avec un idiome de `<head>`
différent** (`<Head>` de `next/head`, jamais un export `metadata`), et n'avait jamais été éprouvé.

## Le défaut injecté

`pages/blog.js` déclare `const canonical = '…/blog/'` alors que le site sert `/blog` et redirige
`/blog/` vers lui en 301. Le `<link rel="canonical">` est dans le `<Head>` avec l'expression
`href={canonical}` ; la valeur littérale est au niveau module.

`pages/a-propos.js` est la page témoin. `pages/_document.js` est le gabarit partagé : il pose
`lang="fr"` (Next Pages n'émet rien sans lui, et les trois pages étaient signalées
`html_lang_attribute_missing`) et ne doit jamais recevoir une valeur propre à une page.

## Dérouler la boucle

```bash
cd seo-agent-web/tests/fixtures/next-pages
npm install && npm run build      # output: 'export' -> out/
python ../../static_site_server.py out 8745 &

SEO_AUDIT_ALLOW_PRIVATE_HOSTS=1 python ../../../../skills/public/seo-autopilot/scripts/seo_audit.py \
    http://127.0.0.1:8745/ --sitemap http://127.0.0.1:8745/sitemap.xml --output-dir /tmp/np-before
```

## Résultat mesuré (2026-08-29)

`canonical_points_to_redirect`, `redirect_3xx` et `sitemap_non_canonical_page` : **1 → 0** chacun.
Stack `next-pages`, idiome `next/head` (et non l'export `metadata` de l'App Router), cible
`pages/blog.js` seule, une ligne. Pages 4 → 3. Aucun trou de réécriture.
