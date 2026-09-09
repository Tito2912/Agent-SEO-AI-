# Parcours d'obstacles du correcteur

Un banc de non-régression : **31 pages, une anomalie chacune**, dans le dépôt fixture
`pployeraffiliation-a11y/noyaru-stack-static-html` (branche `main`).

Il existe parce que 34 familles revendiquées par le correcteur ne s'étaient produites sur aucun
site réel : elles ne reposaient que sur des tests unitaires — exactement le statut qu'avaient les
familles dans lesquelles huit défauts ont été trouvés le 08/09/2026. Attendre qu'un client les
porte n'était pas un plan.

## Règle absolue

**Une branche de correction ne doit JAMAIS être fusionnée dans `main`.** Le parcours doit garder
ses défauts pour resservir. Les PR ouvertes pour obtenir une prévisualisation Netlify se ferment
sans fusion.

## Rejouer le banc

```
# 1) crawl de reference (le parcours, defauts intacts)
python skills/public/seo-autopilot/scripts/seo_audit.py \
  https://noyaru-stack-static-html.netlify.app/ --max-pages 60 --workers 3 \
  --output-dir <workdir>/before

# 2) le correcteur, toutes familles, sur UNE branche
GAUNTLET_WORKDIR=<workdir> FIXTURE_TOKEN=<PAT> python seo-agent-web/ops/gauntlet/run.py

# 3) ouvrir une PR pour obtenir la preview, crawler la preview, comparer, PUIS FERMER la PR
```

## Deux pieges de mesure, verifies

- **Netlify injecte `X-Robots-Tag: noindex` sur toute prévisualisation.** Comparer un crawl de
  production a un crawl de preview fausse toutes les familles d'indexabilite (`noindex_page`,
  les variantes `_not_indexable`). Comparer les familles de base, ou preview contre preview.
- Le crawl de reference et celui de verification doivent utiliser le **meme `--max-pages`** :
  un plafond different change les comptes et ressemble a une regression.

## Reconstruire les pages

`build_pages.py` (les 31 pages) puis `build_scaffold.py` (index, ressources, redirection,
sitemap). Chaque page porte en tete un commentaire nommant la famille visee, pour qu'un lecteur
du depot distingue une fixture d'une erreur.
