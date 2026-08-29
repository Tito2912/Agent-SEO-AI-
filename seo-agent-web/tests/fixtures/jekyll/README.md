# Fixture Jekyll — la boucle complète

Neuvième et dernière stack. Jekyll apporte deux choses qu'aucune des huit autres n'avait : le
front matter en **YAML** (troisième sérialisation après le JavaScript d'Astro et le TOML de Hugo)
et **Liquid** dans le gabarit (`href="{{ page.canonical }}"`, quatrième langage de template).

C'est aussi la seule stack qui a demandé une installation système : Ruby 3.3 + `gem install
jekyll`, là où Hugo tenait dans un binaire posé à côté.

## Le défaut injecté

`blog.html` déclare `canonical: "…/blog/"` alors que le site sert `/blog` et redirige `/blog/`
vers lui en 301. `a-propos.html` est la page témoin, `_layouts/default.html` le gabarit partagé.

## Dérouler la boucle

```bash
# Ruby n'est pas fourni : winget install RubyInstallerTeam.RubyWithDevKit.3.3
# puis gem install jekyll   (Ruby 3.3 et non 4.x : les versions recentes ont retire
#                            des gems par defaut dont Jekyll depend)
cd seo-agent-web/tests/fixtures/jekyll
jekyll build --destination _site
python ../../static_site_server.py _site 8747 &

SEO_AUDIT_ALLOW_PRIVATE_HOSTS=1 python ../../../../skills/public/seo-autopilot/scripts/seo_audit.py \
    http://127.0.0.1:8747/ --sitemap http://127.0.0.1:8747/sitemap.xml --output-dir /tmp/jk-before
```

**Piège YAML :** un ` : ` non quoté dans une valeur de front matter termine le scalaire et Jekyll
émet une `YAML Exception` — que le build ignore, en produisant silencieusement une page sans
métadonnées. Toutes les descriptions sont donc quotées.

## Résultat mesuré (2026-08-29)

`canonical_points_to_redirect`, `redirect_3xx` et `sitemap_non_canonical_page` : **1 → 0** chacun.
Stack `jekyll`, cible `blog.html` seule, **une ligne** réécrite. Pages 4 → 3. Aucun trou de
réécriture : la forme propriété `canonical:` était déjà couverte avant le correctif d'Astro.

## La limite trouvée, et elle est réelle

**Le front matter n'est pas lu, donc les `permalink:` par page sont invisibles.** Les routes de ce
fixture viennent du repli HTML statique, qui mappe par **nom de fichier**. Ça tombe juste ici
parce que `blog.html` sert bien `/blog`. Mais si une page déclarait `permalink: /articles/actus`,
la carte annoncerait `/blog → blog.html` — une route qui n'existe pas sur le site déployé — et
ignorerait la vraie URL.

Conséquence : sur un site Jekyll à permalinks personnalisés, le ciblage retombe sur le mappage
par IA, l'étape derrière chaque correctif appliqué au mauvais fichier. La route fantôme, elle,
est inoffensive : elle désigne une URL que le crawl ne rapportera jamais.

Ce n'est **pas un bug mais une limite de conception assumée** : `repo_index` se construit depuis
l'arbre de fichiers seul, sans jamais ouvrir un fichier, ce qui lui vaut de ne coûter aucun appel
d'API supplémentaire. Lever la limite voudrait dire changer ce contrat. `test_jekyll_permalinks.py`
épingle le comportement actuel pour que ce soit une frontière connue et non une surprise.
