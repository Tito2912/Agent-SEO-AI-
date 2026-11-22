# Transport éditorial CMS — C078

Lot Codex transport, réalisé dans le checkout isolé `/tmp/noyaru-agent-transport-20261117`, branche `feature/cms-content-transport`. AGENTS et journal commun C078 lus avant édition. Aucun compte, secret, contenu client ou service externe modifié ; toutes les requêtes des tests sont simulées.

## Interface

```python
transport = ContentTransport(platform, credentials, project_url=project.base_url)
state = transport.check()  # lecture seule
if state["verified"]:
    adapter = get_adapter(platform, transport=transport, config=mapping)
```

`transport(method, path, payload=None, headers=None)` retourne une réponse structurellement compatible avec `cms_content_adapters.base.TransportResponse` : `status`, `headers`, `body` (bytes), `.json()`, `.text()`. Le type n’est pas une sous-classe de celui de l’adaptateur ; aucune dépendance circulaire. Les erreurs HTTP/transport sont des `cms_commun.CmsError` (`code`, `uncertain`) à traduire par l’orchestrateur si nécessaire. Les erreurs applicatives GraphQL/partielles restent interprétées par l’adaptateur.

Identifiants attendus, conservés uniquement dans l’instance ; l’appelant doit utiliser le stockage chiffré existant et ne jamais journaliser les valeurs :

| Plateforme | Clés |
|---|---|
| WordPress | `base_url`, `username`, `password` |
| Ghost | `base_url`, `key` |
| Shopify | `shop` (hôte `*.myshopify.com`), `token` |
| Webflow | `site_id`, `token` |
| Wix | `site_id`, `token` (clé API serveur) |
| PrestaShop | `base_url`, `key` |

Les noms français des anciens connecteurs ne sont pas automatiquement convertis. Le nouveau transport éditorial ne modifie pas leurs contrats.

## Vérification et destinations

`check()` remet d’abord la vérification en échec, effectue uniquement des lectures, puis renvoie `{verified, label, reason, destinations}`. Les écritures restent bloquées tant que cette instance n’a pas prouvé l’association au projet ; aucun drapeau `verified` fourni par un formulaire ou un stockage n’est accepté. Une vérification ne prouve pas les droits de publication : le CMS les contrôle lors de l’écriture explicite.

- WordPress : hôte identique au projet et lecture authentifiée des posts avec `context=edit`. Destination `posts` / Articles.
- Ghost : hôte identique ou administration `.ghost.io`, URL de `/site/` associée au projet, puis lecture authentifiée des posts. Destination `posts` / Articles. JWT HS256 de courte durée via la fonction existante, version Admin v5.0 par défaut. Aucun endpoint newsletter autorisé.
- Shopify : endpoint Admin GraphQL **2026-10** fixé, `shop.primaryDomain.url` associé au projet, puis blogs existants. Destinations `[{id: "gid://shopify/Blog/...", title, kind: "blog"}]`, aucune sélection automatique. Au-delà de 100 blogs, refus explicite jusqu’à ajout d’un sélecteur paginé, pas de liste tronquée silencieusement.
- Webflow : `/v2/sites/{site_id}` avec ID et domaine correspondant (customDomains ou shortName.webflow.io), puis liste des collections de ce site. Destinations `[{id, title, kind: "collection", fields: [], schema_loaded: False}]`. **Après choix explicite**, `describe_destination(id)` charge le schéma et renvoie les champs `[{slug,title,type,required,editable}]`, `schema_loaded: True`. Les lectures/écritures sur les collections d’un autre site sont refusées, même avec un jeton couvrant plusieurs sites. Aucune modification de schéma n’est autorisée. Les scopes `sites:read` et `cms:read` sont requis pour la découverte ; `cms:write` reste nécessaire pour publier.
- Wix : la clé et le site ID sont associés au domaine publié via les balises SEO par défaut ; une canonical modifiée à la main ne suffit pas. Cela exige une permission de lecture SEO supplémentaire à Manage Blog. Sans cette preuve (notamment site non publié), `verified=False`, pas de publication. Destination `blog` / Blog. La découverte de membres/auteurs n’est pas implémentée dans ce transport.
- PrestaShop : même hôte que le projet et lecture du Webservice des pages CMS. Destination `cms-pages` / Pages de contenu, **pas un blog**. La découverte des langues/boutiques/catégories n’est pas implémentée ; les choix `id_lang` et `id_shop` relèvent encore du mapping explicitement configuré.

`destinations` ne contient jamais de token, clé ou mot de passe. Pour les destinations hors Webflow, `describe_destination(id)` retourne la destination déjà listée.

## Bornes

HTTPS/443 uniquement, résolution DNS publique et épinglée pour chaque requête via `adresse_publique`/`HTTPSEpingle`, délai de la connexion commune (12 s), réponse bornée à 4 Mio, requête à 2 Mio. Aucune redirection ni réessai. Liste de chemins/méthodes/paramètres par fournisseur ; URL absolue, traversée, contrôles, autorité et surcharges de méthode refusés avant réseau. Les en-têtes d’authentification sont construits par le transport, jamais acceptés de l’adaptateur. Les réponses n’exposent que Content-Type, ETag, Last-Modified au niveau des en-têtes.

Shopify : une seule opération GraphQL, racines éditoriales autorisées ; `articleCreate` et `articleUpdate` sont des écritures, les requêtes de contrôle restent des lectures malgré POST. Pas de mutations produits, clients, fragments ou opérations concaténées. Le corps est transmis sous forme de variables par les adaptateurs.

Une écriture interrompue, redirigée, sur réponse serveur indéterminée ou illisible devient `uncertain=True`. Les refus certains (auth, 400/409/413/415/422/429) ne déclenchent aucun réessai. Le journal central doit réconcilier un résultat incertain avant de relancer une création.

Ce lot n’implémente pas le téléversement de médias, les connexions OAuth distribuées ni la pagination générale des destinations. Il ne prouve aucune recette réelle ou disponibilité en production.

## Références officielles consultées le 17/11/2026

- [WordPress REST authentification](https://developer.wordpress.org/rest-api/using-the-rest-api/authentication/)
- [Ghost Admin API](https://docs.ghost.org/admin-api/)
- [Shopify Shop](https://shopify.dev/docs/api/admin-graphql/latest/objects/shop), [référence Admin GraphQL](https://shopify.dev/docs/api/admin-graphql/latest)
- [Webflow Get Site](https://developers.webflow.com/data/reference/sites/get), [List Collections](https://developers.webflow.com/data/reference/cms/collections/list), [Get Collection Details](https://developers.webflow.com/data/reference/cms/collections/get)
- Vérification Wix alignée sur le [connecteur Noyaru existant](../../backend/wix_cms.py) pour les balises résolues et héritées ; aucune nouvelle méthode d’association non documentée inventée
- [PrestaShop pages CMS](https://devdocs.prestashop-project.org/9/webservice/resources/content_management_system/)


## Complément intégré le 18 novembre : médias WordPress et Ghost

`ContentTransport.supports_image_upload` est vrai pour WordPress et Ghost seulement.
`upload_image(data=..., filename=..., content_type=...)` vérifie le raster réel
(PNG/JPEG/WebP, 10 Mio, 16 millions de pixels, côté maximum 8192, une seule frame),
puis utilise le transport HTTPS authentifié et épinglé déjà vérifié. WordPress reçoit
le fichier binaire ; Ghost reçoit un multipart. Les autres plateformes refusent cet
appel. L’URL retournée doit être publique, HTTPS et sans jeton de requête ; les URLs
relatives Ghost sont résolues sur le domaine public du projet. Aucun nouvel envoi
après un timeout ambigu et aucune suppression automatique des médias orphelins.

`cms_media_workflow` stocke une intention avant le transfert, après consentement,
et réutilise le résultat confirmé pour une même image/connexion/article. Les images
privées restent refusées pour les autres CMS ; ce lot ne simule pas un upload réussi.
Le paquet d’adaptateurs éditoriaux conserve `capabilities.upload_images=False` :
le transfert est fourni par cette couche séparée, pas par leurs quatre méthodes.
Les tests utilisent des fichiers fictifs et de faux échanges HTTP, sans compte réel.
