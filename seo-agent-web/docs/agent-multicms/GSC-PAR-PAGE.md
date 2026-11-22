# Mesure Search Console d'une page — C078

`backend.agent_gsc.fetch_page_period` réalise une lecture explicite avec la
connexion GSC déjà autorisée. Aucun changement OAuth ni périmètre supplémentaire.
Il réutilise `fetch_module._get_access_token(credentials_path, scopes=None)` ;
le chemin est fourni par l'intégration existante. Les tests emploient uniquement
un fournisseur de jeton fictif et un transport injecté.

```python
period = fetch_page_period(
    fetch_module=gsc_fetch,
    credentials_path=credentials_path,
    property_url="sc-domain:atelier.example",
    page_url="https://atelier.example/reparation",
    start_date="2026-10-01",
    end_date="2026-10-07",
)
```

Le résultat suit le contrat de `agent_results.compare_periods` : `ok`, `complete`,
`reason`, `page_url`, `property`, `start_date`, `end_date`, `dimension:"page"`,
`filters:{search_type}`, `rows`. `data_state:"final"` et
`calendar_timezone:"America/Los_Angeles"` précisent le contexte.

La requête emploie une dimension page, un filtre page `equals`, `type`,
`dataState:"final"` et `aggregationType:"auto"`. Les dates API sont en Pacific.
Les regroupements de requêtes ne fournissent pas les totaux par page ; ils ne
sont ni additionnés ni paginés ici. Le plafond de deux lignes sert à détecter
une anomalie de réponse : ce filtre ne doit produire qu'une ligne agrégée.
[Référence Google vérifiée le 17 novembre 2026](https://developers.google.com/webmaster-tools/v1/searchanalytics/query).

Politiques locales prudentes : la fin doit être antérieure ou égale à la date
Pacific du jour moins trois jours. Les fenêtres encore fraîches ou futures sont
refusées avant toute demande de jeton. Les fenêtres dépassant la rétention de
seize mois sont refusées avec une marge d'un jour ; une absence hors rétention
ne prouve pas zéro clic. Cette marge est un choix de Noyaru, pas une garantie de
disponibilité à trois jours. La limite historique est décrite dans
[l'aide officielle Google](https://support.google.com/analytics/answer/10737381?hl=en).

L'endpoint Google est fixe ; propriété encodée dans le chemin, redirections
refusées, délais connexion/lecture de 5/15 secondes, lecture en flux plafonnée
à 64 Kio décompressés. La fermeture de la réponse est systématique. Aucun
contenu de réponse, texte d'exception ou jeton n'est exposé dans le résultat.
Il n'y a ni nouvelle tentative automatique ni découverte d'une autre propriété.

`complete:true` signifie une réponse API finalisée, bornée et cohérente pour
**cette** page/propriété/période/type. La propriété doit contenir l'URL demandée.
Une réponse doit annoncer une agrégation `byPage` ou `auto` et zéro ou une
ligne de la page exacte, avec compteurs finis et CTR cohérent. Une réponse vide
valide peut établir zéro ligne rapportée ; une réponse HTTP échouée, vide
inexploitable, tronquée, mal formée ou au mauvais périmètre reste indisponible.
Les limites internes de Search Console subsistent : cette complétude du contrat
n'est pas un inventaire indépendant de tout le trafic du site.

Raisons stables principales : `invalid_scope`, `invalid_dates`,
`invalid_search_type`, `period_not_final`, `period_out_of_retention`,
`authorization_unavailable`, `authorization_refused`, `property_access_refused`,
`rate_limited`, `redirect_refused`, `http_error`, `network_error`, `invalid_json`,
`response_too_large`, `invalid_response`, `aggregation_mismatch`,
`unexpected_rows`, `page_mismatch`, `invalid_metrics`.

L'intégration vérifie les droits projet, choisit la propriété autorisée et
enregistre les deux fenêtres avec leurs métadonnées. Un appel constitue une
observation ; seule la comparaison valide établit des différences de mesures,
sans attribuer leur cause à une correction ou déduire des contacts/revenus.

Tests locaux : `tests/test_agent_gsc.py` couvre le contrat réel du POST, l'absence
de nouveau scope, les échecs HTTP/réseau/autorisation, les bornes de réponse,
la fraîcheur/rétention, les métriques, l'identité de page et le passage dans
`agent_results`. Aucun appel à Google ou compte réel n'est exécuté par les tests.
