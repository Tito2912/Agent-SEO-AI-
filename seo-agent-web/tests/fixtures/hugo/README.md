# Fixture Hugo — la boucle complète

Même rôle que le fixture Astro : prouver qu'un correctif écrit par l'agent **compile encore** et
que **reconstruire fait disparaître l'anomalie**. Hugo a été choisi en deuxième parce qu'il
sépare ce qu'Astro réunit : le `<head>` vit dans un **template** `layouts/`, la valeur par page
dans le **front matter** de `content/`. Deux fichiers, deux langages.

## Le défaut injecté

`content/blog.md` déclare `canonical = "…/blog/"` alors que le site sert `/blog` et redirige
`/blog/` vers lui en 301. **Front matter TOML délibérément** : la valeur est une *affectation*
(`canonical = "…"`), la forme que le réécriveur ne savait pas lire avant le 2026-08-29 — le
correctif est né du fixture Astro et se vérifie ici sur une deuxième stack et un deuxième
langage.

`content/a-propos.md` est la page témoin. `layouts/_default/baseof.html` est le template partagé :
il rend `href="{{ .Params.canonical }}"`, sans URL littérale — un correctif par page ne doit
jamais y atterrir.

Hugo génère son propre `sitemap.xml` avec des slashs finaux, ce qui contredit l'hébergeur sans
slash simulé ici et ajouterait deux anomalies sans rapport ; `disableKinds` le désactive et le
site fournit le sien, comme le fait un vrai site Hugo déployé ainsi.

## Dérouler la boucle

Hugo n'est pas installé sur la machine de dev par défaut. Le binaire se récupère sans installer
quoi que ce soit :

```bash
curl -sL -o hugo.zip https://github.com/gohugoio/hugo/releases/download/v0.165.0/hugo_0.165.0_windows-amd64.zip
# puis extraire hugo.exe

cd seo-agent-web/tests/fixtures/hugo
hugo --destination public --cleanDestinationDir
python ../../static_site_server.py public 8742 &

SEO_AUDIT_ALLOW_PRIVATE_HOSTS=1 python ../../../../skills/public/seo-autopilot/scripts/seo_audit.py \
    http://127.0.0.1:8742/ --sitemap http://127.0.0.1:8742/sitemap.xml --output-dir /tmp/hugo-before
```

Puis ciblage + réécriture déterministe sur la source, `hugo` à nouveau, et recrawl.

## Résultat mesuré (2026-08-29)

| | avant | après |
|---|---|---|
| `canonical_points_to_redirect` | 1 | 0 |
| `redirect_3xx` | 1 | 0 |
| `sitemap_non_canonical_page` | 1 | 0 |

Stack détectée `hugo`, routes `content/*.md` (jamais les `layouts/`), cible `content/blog.md`
seule, **une ligne** réécrite. Pages crawlées 4 → 3 : `/blog/` cesse d'exister comme URL
distincte.

Contrairement à Astro, **aucun trou n'a été trouvé ici** — le correctif du réécriveur né d'Astro
couvrait déjà le TOML de Hugo. C'est le résultat qu'on veut voir se répéter : la première stack
paie le prix, les suivantes le récoltent.
