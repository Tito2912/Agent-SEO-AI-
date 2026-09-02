# Fixture static-html — la boucle complète

Le neuvième stack, et le seul où **la page EST le fichier** : aucun générateur, aucun gabarit,
aucune sérialisation de front matter. C'est aussi le stack le plus répandu chez les petits sites,
et l'un des deux validés en production (elevenlabs-avis.com).

Il a été écrit le 2026-08-30, après une mesure qui a mal tourné : interrogé sur la réécriture de
snippet, `_find_head_text_value` **ne trouvait rien** sur une page HTML pure. Il connaissait
toutes les façons dont un *framework* déclare un titre — prop de composant, TOML, YAML quoté et
nu, binding JS — et aucune de celles de HTML : `<title>…</title>` et
`<meta name="description" content="…">`. Le client sur site statique recevait « ces valeurs sont
assemblées » au lieu d'une pull request.

## Le défaut injecté

`blog.html` déclare `<link rel="canonical" href="…/blog/" />` avec un slash final, alors que
l'hôte sert `/blog` et redirige `/blog/` vers lui — le défaut de la PR#1 de voiceoverstudioai,
celui dont la correction consiste à retirer un caractère.

Tout le reste est délibérément sain : descriptions au-delà de 100 caractères, OG et Twitter
complets, `lang` renseigné, sitemap et robots.txt présents. « Zéro à la fin » est donc une
preuve, pas une coïncidence. `a-propos.html` est la page témoin qui doit rester **rigoureusement
intacte**, et elle porte volontairement un `title="revenir a l'accueil"` sur un lien : c'est le
piège exact qui ferait réécrire une info-bulle à la place du titre de la page.

## Boucle locale

```
python tests/static_site_server.py 8748 --root tests/fixtures/static-html
SEO_AUDIT_ALLOW_PRIVATE_HOSTS=1 python ../skills/public/seo-autopilot/scripts/seo_audit.py \
  http://127.0.0.1:8748/ --sitemap http://127.0.0.1:8748/sitemap.xml --output-dir /tmp/fx-static
```

Attendu avant correction : `canonical_points_to_redirect` 1, plus les conséquences que la même
URL entraîne (`redirect_3xx`, `sitemap_non_canonical_page` selon le crawl). Après la réécriture
d'un caractère : zéro, et le nombre de pages passe de 4 à 3, parce que `/blog/` cesse d'exister
comme URL distincte — le bon résultat, pas une page perdue.

Aucune construction : `publish = "."`. C'est ce qui fait de ce stack le moins cher des neuf à
déployer, et le plus rapide à re-crawler après un merge.

## Boucle réelle

`ops/stack_loop.py` publie cette arborescence sur un dépôt GitHub et un site Netlify, puis pilote
le produit lui-même : projet, crawl, PR d'anomalie, PR mots-clés, merge, re-crawl. Les canonicals
absolus visent ici un port local ; `prepare --site <url>` réécrit l'hôte et **garde le slash**.
