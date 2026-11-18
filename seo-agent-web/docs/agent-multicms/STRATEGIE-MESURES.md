# Stratégie et résultats — première livraison C078

État du lot : modules purs et tests locaux. Ni appel réseau, ni écriture CMS,
ni connexion de client, ni recette de production. Le branchement aux routes,
permissions, quotas et modèles appartient au lot d'intégration Codex.

## Trois actions expliquées

`backend.agent_strategy.recommend_actions` est une fonction sans effet externe :

```python
actions = recommend_actions(
    base_url="https://atelier.example", slug="atelier",
    report=report, query_rows=query_rows, competitor_topics=findings,
    existing_pages=[], business_context={
        "activity": "Réparation de vélos",
        "services": ["réparation vélo", "entretien des freins"],
        "important_pages": ["https://atelier.example/reparation"],
    },
    decisions=[{"action_key": "clé enregistrée côté serveur", "state": "dismissed"}],
    limit=3,
)
```

Les noms `priority_urls` et `topics` sont également acceptés. Le contexte peut
préciser `expected_indexable_urls` et `excluded_topics`. Les mots de l'activité
déclarée sont utilisés pour filtrer les sujets concurrents ; ils n'établissent ni
un volume de recherche ni une rentabilité. Un contexte insuffisant réduit le
nombre de suggestions ; la fonction ne complète jamais artificiellement à trois.

Les données d'entrée reprennent les formes existantes :

- Rapport : `pages: [{url,title,h1,status_code,indexable,...}]` et
  `issues: {issue_key: {count,examples:[url,...]}}`. Les exemples sont des
  observations, pas la liste exhaustive des pages concernées.
- Search Console : `keyword` ou `query`, `page`, `clicks`, `impressions`,
  `position`, ou la forme API `keys:[query,page]`. Les lignes non mesurées,
  non numériques ou étrangères au projet sont exclues ; un CTR fourni est
  recalculé depuis les compteurs. Des impressions au niveau requête/page ne
  sont jamais présentées comme le total exhaustif d'une page.
- Concurrents : résultat de `competitors.compare`, avec `covered`, `terms`,
  `competitor_title`, `competitor_url`, `own_url` et `match_score`.
  Dans l'application, `CompetitorSite.pages` contient les pages déjà analysées.
  La comparaison se fait localement avec les pages du dernier rapport, en
  respectant l'accès au module concurrents, sans nouveau crawl sur un GET.

Chaque sortie comporte `action_key`, `kind`, `title`, `page_url`, `reason`,
`evidence`, `effort`, `uncertainty`, `next_href`. Les types sont :

1. `technical_indexability` : blocage ou défaut d'accès observé sur une page
   importante (page déclarée, accueil ou signal Search Console). La proposition
   demande d'examiner le rôle de la page : elle n'autorise pas à retirer un noindex.
2. `improve_existing` : signal faible CTR ou position moyenne 11–20 déjà détecté
   par `keywords.find_opportunities`. La cause reste à déterminer ; un faible
   CTR ne prouve pas un défaut du titre et n'entraîne pas une réécriture automatique.
3. `technical_metadata` : titre/description manquants constatés dans le crawl.
4. `new_content` : sujet explicitement non couvert chez un concurrent, avec au
   moins deux termes en commun avec l'activité déclarée. Les pages connues sont
   revérifiées et les sujets voisins dédupliqués. La demande et l'absence de page
   équivalente restent à confirmer. `page_url` est vide : aucune URL nouvelle
   n'est inventée ; l'action mène à Contenu, sans lancer de génération.

Une page n'occupe qu'une place dans la liste. La priorité technique précède les
opportunités de contenu ; l'ordre est une règle produit expliquée, pas un modèle
de gain. Les données incomplètes ne donnent pas lieu à des volumes estimés.

`decisions` peut être une liste `{action_key,state}` / `{action_key,status}` ou un
dictionnaire `action_key -> état`. `dismissed`, `refused`, `ignored`, `accepted`,
`in_progress`, `done` et `completed` retirent l'action de la sélection. Un état
accepté signifie seulement que l'action est prise en charge. **La fonction ne
confirme jamais l'exécution ou la réussite d'un travail.** L'intégration réserve
les états de réalisation aux vérifications du serveur. Les clés sont stables
pour le couple type/page (type/vocabulaire pour un sujet) ; le cycle de révision
des refus et la réouverture après régression doivent être gérés explicitement
par l'orchestrateur, sans transformer un changement de crawl en nouvel accord.

Les URL d'action restent dans l'origine et le chemin de base du projet.
`next_href` pointe uniquement vers les routes existantes Anomalies,
Mots-clés/opportunités ou Contenu. Les liens de concurrents dans les preuves
restent des observations externes assainies ; ils ne deviennent jamais une
destination de publication.

## Mesures par page et périodes comparables

`backend.agent_results.summarise_period(period, page_url=...)` normalise une
période. `compare_periods(before, after, page_url=..., change_at=...)` décide si
les deux observations peuvent être comparées.

Le serveur fournit chaque période avec ces métadonnées explicites :

```json
{
  "ok": true,
  "complete": true,
  "property": "sc-domain:atelier.example",
  "page_url": "https://atelier.example/reparation",
  "dimension": "page",
  "filters": {"country": "fra", "search_type": "web"},
  "start_date": "2026-10-01",
  "end_date": "2026-10-07",
  "rows": [{"page":"https://atelier.example/reparation",
            "clicks":5,"impressions":100,"position":8}]
}
```

`date_start` / `date_end` sont des alias acceptés. Les lignes peuvent utiliser
`page`, `url`, `keyword` (forme normalisée actuelle d'une dimension page) ou
`keys:[page]`. Des lignes journalières disjointes avec `date` sont possibles.
L'intégration doit assurer le périmètre de récupération et la complétude ; un
simple `ok=true` ou une ligne absente d'un résultat limité ne suffit pas à
conclure à zéro. Une liste `query_page`, potentiellement anonymisée ou bornée,
ne constitue pas un total page et n'est pas acceptée ici.

Les états sont `complete`, `partial`, `unavailable` ou `invalid`. Une réponse
partielle peut montrer ses compteurs observés, mais elle ne permet aucune
comparaison validée. Une réponse partielle vide donne `metrics:null`, jamais zéro.
Seule une réponse explicitement complète et vide peut établir zéro clic et zéro
impression ; son CTR et sa position restent inconnus (`null`).

Les comparaisons exigent la même URL, propriété, dimension, filtres et durée,
avec des périodes ordonnées sans chevauchement. Si la date de modification est
fournie, elle doit inclure un fuseau ; le jour de modification est exclu des
deux périodes selon le calendrier Pacific de Search Analytics. Des durées qui
ne sont pas des semaines complètes portent une note sur les jours de semaine.

CTR = somme des clics / somme des impressions. Une position moyenne se pondère
par les impressions de lignes disjointes au même périmètre ; une position
manquante sur une ligne mesurée laisse la position agrégée inconnue. Les
doublons, mélanges d'agrégats et journées, filtres incompatibles et valeurs
invalides ne produisent aucun résultat comparatif.

La sortie inclut `comparable`, `reason`, les deux périodes normalisées,
`changes` par métrique (`absolute`, `relative`) et des notes. Une référence
nulle ne donne pas de pourcentage infini. Le delta de CTR reste une fraction
(0,01 = un point de pourcentage), pas une valeur déjà multipliée par 100.
`causality` vaut toujours `not_established` et `commercial_results` reste `null`.
Il faut une source autorisée distincte pour les contacts/commandes ; aucun revenu
ni client n'est déduit de clics, d'impressions ou d'une amélioration technique.

## Validation locale

Tests dédiés : `test_agent_strategy.py`, `test_agent_results.py`. Ils couvrent
les décisions client, la déduplication, les données manquantes, la pertinence
métier, le cloisonnement des URL, les dates/filtres, les réponses partielles,
les compteurs pondérés et l'absence d'attribution commerciale/causale.

Les tests existants `test_keyword_opportunities.py` et `test_competitor_topics.py`
sont exécutés avec les tests du lot pour vérifier la compatibilité des règles.
Ce résultat local ne remplace ni CI ni recette du parcours intégré.
