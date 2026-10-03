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

## Cycle Controle

`live_cycle.py` utilise le meme correcteur que les routes produit et se limite aux neuf depots
fixtures. Il ouvre deux PR brouillon, attend le succes du build Netlify pour leur commit exact,
crawle la preview temoin et la preview corrigee, puis ferme les deux PR **sans fusion**.
La fermeture est aussi tentee en cas d'echec. `cycle.json` conserve le resultat et le nettoyage.

Depuis `seo-agent-web`, avec l'environnement Python du projet :

```sh
python ops/gauntlet/live_cycle.py --workdir <dossier-temporaire> --stacks astro next-app
```

Par defaut, seules les corrections mecaniques sont appliquees. `--ai-families` permet de nommer
explicitement un petit lot a exercer avec Claude ; `--ai-max-files` vaut 2 fichiers par famille.
Ce plafond est un budget de banc, pas une certification de toutes les occurrences du site.
Les appels IA sont traces dans `claude.log`, jamais avec leurs cles.

Une famille absente de la preview temoin reste `unverified`, meme si son compteur vaut zero
apres correction. Les settings, les pages HTML effectivement servies, les cibles revisitees et
l'ajout de `noindex` sont controles. Les hausses d'autres familles et les nouvelles URL touchees
restent visibles, meme quand une correction deplace une anomalie sans changer son compteur.
Trois familles sensibles a l'indexabilite disposent aussi de controles directs du HTML observe :
canonical vers une 4xx, liens internes HTTP et langue du HTML servi. Ces preuves sont rapportees
separement des compteurs du crawler et seulement sur les pages effectivement observees.
Le banc impose une base SQLite temporaire et n'utilise jamais la base client.

## Bilans Conserves

- `validation-2026-10-01.json` : HTML statique, verification et interface.
- `validation-multistack-2026-10-01.json` : Astro et Next App, appels Claude limites ;
  la premiere regression sitemap reste conservee avec sa contre-verification.
- `validation-preclients-2026-10-02.json` : six autres stacks, builds et recrawls reels
  sans nouveaux appels Claude ; tests des quotas, confirmations et demandes concurrentes.
- `validation-postgres-et-ia-2026-10-02.json` : vraie base PostgreSQL temporaire, panne de
  connexion, pool et dernier credit partage par six processus ; Claude sur Hugo/Nuxt et
  disparition du reliquat Open Graph sur Hugo/Gatsby/Nuxt apres builds et recrawls reels.
- `validation-reprise-2026-10-03.json` : journal durable des corrections, apercus payes
  recuperables, debit atomique et reprise d'une PR deja acceptee ; migration SQLite/PostgreSQL.

Ces bilans ne certifient pas toutes les anomalies ni tous les clients. Les corrections locales
ne sont pas deployees. Le dernier bilan conserve 2 741 tests reussis sur SQLite et 97 tests
cibles reussis avec PostgreSQL isole. Les 11 cas PostgreSQL ignores dans le lancement SQLite
ont tous ete exerces separement sur cette vraie base. Le serveur temporaire a ensuite ete arrete.

Le journal permet de retrouver une PR acceptee avant une perte de reponse ou un echec du debit,
sans reecrire les fichiers ni facturer deux fois. Une branche partielle ou une PR impossible a
confirmer reste bloquee : aucune relance automatique destructive n'est activee. L'outillage de
reconciliation operateur, les pannes GitHub reelles et la conservation des recus restent a cadrer.
La capacite du deploiement reel, les familles masquees par le noindex des previews et les lots IA
incomplets restent a valider.

Le bilan PostgreSQL/IA du 2 octobre corrige aussi l'explication historique du reliquat Open Graph : le canonical
avait deja ete passe en HTTPS, mais son slash final differait encore d'og:url. Les anciens
rapports restent conserves ; la nouvelle preuve et sa contre-verification sont dans ce bilan.

## Reconstruire les pages

`build_pages.py` (les 31 pages) puis `build_scaffold.py` (index, ressources, redirection,
sitemap). Chaque page porte en tete un commentaire nommant la famille visee, pour qu'un lecteur
du depot distingue une fixture d'une erreur.
