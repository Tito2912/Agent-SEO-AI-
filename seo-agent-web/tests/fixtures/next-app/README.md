# Fixture next-app — la boucle complète

Le stack que le produit connaît le mieux : c'est celui de voiceoverstudioai.com, d'avis-invest et
d'oryvalo, et celui où la boucle a été prouvée jusqu'à une pull request mergée (PR#1 canonical,
PR#2/#5 longueur des titres, PR#3 réécriture de snippet). Il n'avait pourtant **pas de fixture** :
tout ce qu'on savait de lui venait de dépôts clients, qu'on ne peut ni casser ni rejouer.

Écrit le 2026-08-30 pour compléter les neuf, et pour que la même arborescence puisse être
déployée, cassée et corrigée autant de fois qu'on veut sans toucher au site de personne.

## Ce qu'il apporte que les autres n'ont pas

* Les valeurs de tête sont un **export `metadata`** — ni balisage, ni front matter, ni binding :
  une propriété d'objet dans un module TypeScript.
* Le **sitemap est généré** (`app/sitemap.ts`) : aucun `<loc>` littéral à réécrire. C'est la
  raison d'être du repli IA des familles sitemap, et le seul fixture qui pose la question.
* Le **layout partagé** (`app/layout.tsx`) ne porte aucune valeur de page. Un correctif per-page
  qui atterrirait dedans donnerait le même titre à tout le site — c'est la panne que
  `_PER_PAGE_ONLY_KEYS` et `repo_index.is_shared_path` existent pour empêcher, et ce fichier
  permet de le vérifier contre un vrai build plutôt que contre une liste de chemins.

## Le défaut injecté

`app/blog/page.tsx` déclare `alternates: { canonical: "…/blog/" }` avec un slash final, alors que
`trailingSlash: false` fait servir `/blog` et rediriger `/blog/` vers lui. Un caractère à retirer,
comme la PR#1 chez le premier client.

Le reste est délibérément sain — descriptions au-delà de 100 caractères, OG complet, `lang`
renseigné, sitemap cohérent — pour que « zéro à la fin » soit une preuve et non une coïncidence.
`app/a-propos/page.tsx` est la page témoin qui doit rester **rigoureusement intacte**.

## Boucle locale

```
npm install && npm run build          # export statique -> out/
python tests/static_site_server.py 8749 --root tests/fixtures/next-app/out
SEO_AUDIT_ALLOW_PRIVATE_HOSTS=1 python ../skills/public/seo-autopilot/scripts/seo_audit.py \
  http://127.0.0.1:8749/ --sitemap http://127.0.0.1:8749/sitemap.xml --output-dir /tmp/fx-nextapp
```

Après la réécriture d'un caractère et un rebuild : `canonical_points_to_redirect` à zéro, et les
pages passent de 4 à 3 — `/blog/` cesse d'exister comme URL distincte, ce qui est le bon
résultat.

## Boucle réelle

`ops/stack_loop.py` publie cette arborescence sur GitHub + Netlify et pilote le produit :
projet, crawl, PR d'anomalie, PR mots-clés, merge, re-crawl. `prepare --site <url>` réécrit
l'hôte des canonicals (ils visent un port local) **en gardant le slash**, sans quoi le crawl
signalerait un canonical vers un autre domaine — une autre anomalie que celle qu'on veut prouver.
