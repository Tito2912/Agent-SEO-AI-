# Contrat des adaptateurs editoriaux multi-CMS

Lot C078 (17 novembre 2026). Journal commun lu : entree C078 du 17/11 18:52 Paris,
reservations confirmees (nouveaux fichiers sous `backend/cms_content_adapters/`,
tests `tests/test_cms_content_adapters*`, ce document). Aucun module existant n'est
modifie. Ce document decrit l'interface implementee ; aucune recette reelle n'est
affirmee et aucun test ne cible un CMS client.

## Perimetre et exclusions explicites

- Perimetre : brouillons, mises a jour et publications d'articles ou de pages sur
  WordPress, Ghost, Shopify, Webflow, Wix et PrestaShop (pages CMS seulement).
- Separation nette du socle `cms_commun.Connecteur` : ce socle reste limite aux
  titres/meta-descriptions et n'est pas etendu par ce lot.
- `capabilities.upload_images = False` pour les SIX adaptateurs. L'upload de media
  (POST /media WordPress, assets Webflow, Wix Media Manager...) n'est pas implemente
  ici. Les brouillons referencent des URL `https://` deja hebergees ; toute URL
  `javascript:` ou `data:` dans le HTML est refusee en amont par `html_safe`. Cette
  limite est explicite et declaree ; aucun adaptateur ne la contourne.
- Aucun envoi de newsletter. Ghost force `send_email_when_published=False` dans
  chaque creation/mise a jour.
- Aucune injection de lien externe non present dans le brouillon source.
- PrestaShop : PAGES CMS seulement. Il n'y a pas de blog natif universel dans le coeur
  PrestaShop ; cet adaptateur ne cree ni ne met a jour d'article de blog.
- Wix : la conversion HTML -> Ricos couvre un sous-ensemble editorial explicite
  (voir plus bas). Toute balise ou attribut hors sous-ensemble leve une erreur ;
  rien n'est perdu silencieusement.

## Interface exacte

```python
from backend.cms_content_adapters import (
    AdapterError, ArticleDraft, ArticleImage, Capabilities, PLATFORMS,
    RemoteArticle, TransportResponse, get_adapter,
)

@dataclass(frozen=True)
class ArticleImage:
    url: str
    alt: str = ""
    caption: str = ""

@dataclass(frozen=True)
class ArticleDraft:
    title: str
    html: str
    slug: str
    excerpt: str = ""
    language: str = "fr"
    author: str = ""
    images: tuple[ArticleImage, ...] = ()

@dataclass(frozen=True)
class RemoteArticle:
    id: str                     # identifiant distant (str, meme quand numerique)
    status: str                 # 'draft' | 'published' | 'scheduled' | 'unknown'
    url: str                    # peut etre vide (Shopify, Webflow, PrestaShop)
    revision: str               # jeton opaque (voir la section Statut / Revision)

@dataclass(frozen=True)
class Capabilities:
    create_draft: bool
    read: bool
    publish: bool
    update_draft: bool
    upload_images: bool          # False pour toute plateforme dans ce lot
    requires: tuple[str, ...]    # cles de configuration exigees
    notes: str
```

Les quatre operations attendues par l'appelant :

```python
adapter = get_adapter(platform, *, transport=transport, config=config)

remote = adapter.create_draft(draft)                         # -> brouillon
remote = adapter.read(remote_id)
remote = adapter.publish(remote_id, expected_revision=...)    # revision obligatoire
remote = adapter.update_draft(remote_id, draft, expected_revision=...)
```

Regles invariantes :

- `create_draft` ne publie JAMAIS. `publish` est l'unique point de publication.
- `publish` et `update_draft` exigent `expected_revision`. L'adaptateur relit la
  version distante puis compare ; un desaccord leve `AdapterError(code="conflict")`
  AVANT toute ecriture. Aucune ecriture n'est envoyee en cas de conflit.
- `update_draft` ne touche jamais au statut distant : ni publication, ni passage en
  brouillon force. Le CMS conserve son statut, seul le contenu change.

## Fabrique et transport injecte

```python
def transport(method: str, path: str,
              payload=None, headers=None) -> TransportResponse: ...

@dataclass(frozen=True)
class TransportResponse:
    status: int
    headers: Mapping[str, str]
    body: bytes
```

- `method` : `GET` / `POST` / `PUT` / `PATCH` selon l'adaptateur et l'operation.
- `path` : CHEMIN RELATIF, commence par `/`. Les adaptateurs n'embarquent ni schema
  ni hote : le transport ajoute `https://<base>`. Les chemins sortants ne contiennent
  jamais `://`. (Verifie par `test_path_is_relative_not_full_url` dans les 5
  plateformes HTTP.)
- `payload` :
  - `None` pour GET ;
  - `dict` pour JSON (le transport serialise et ajoute `Content-Type: application/json`) ;
  - `bytes` pour XML PrestaShop ; l'adaptateur fournit `Content-Type: application/xml`
    dans `headers`.
- `headers` : dict sans secret. Jamais d'`Authorization` passe par l'adaptateur ;
  c'est le transport qui injecte le jeton et, pour Ghost, le JWT HS256 court et
  `Accept-Version` (si pas deja dans l'en-tete fourni).
- Retour : `TransportResponse(status, headers, body)` avec `body: bytes`. L'adaptateur
  interprete lui-meme le code HTTP (`_expect_ok` / `_expect_xml`) et parse le corps
  (`response.json()` ou `response.text()`), avec bornage defensif (`MAX_BODY = 4 MiB`).
- Exceptions : `wrap_transport_errors` convertit toute exception en
  `AdapterError(code="network", uncertain=<write>)`. Si le transport leve une
  exception portant deja `uncertain` et `code` (typiquement `cms_commun.CmsError`
  via le socle existant), on PRESERVE ces attributs : une redirection ou une reponse
  perdue cote transport reste signalee comme ecriture incertaine jusqu'a l'appelant.

Le transport reste responsable de :
- validation du domaine, resolution DNS, epinglage sur IP publique si necessaire ;
- refus des redirections avec identifiants ;
- preparation de `Authorization`, JWT Ghost, headers API-version ;
- NE JAMAIS retenter un POST interrompu.

## Statut et revision par plateforme

| Plateforme | Revision (jeton)                         | Origine de lecture                           | `status` renvoye                  |
|------------|------------------------------------------|----------------------------------------------|-----------------------------------|
| WordPress  | `modified_gmt` (fallback `modified`)     | GET `/wp-json/wp/v2/posts/{id}?context=edit` | `draft` / `published` / `scheduled` |
| Ghost      | SHA-256 du snapshot (dont `updated_at`)                              | GET `/ghost/api/admin/posts/{id}/?formats=html` | `draft` / `published` / `scheduled` |
| Shopify    | `updatedAt`                                | query GraphQL `article(id)`                   | `draft` / `published` / `scheduled` |
| Webflow    | `lastUpdated`                              | GET `/v2/collections/{c}/items/{id}`          | `draft` / `published`             |
| Wix        | `revision`                                 | GET `/blog/v3/draft-posts/{id}`               | `draft` / `published`             |
| PrestaShop | SHA-256 du GET canonique, toutes langues                   | GET `/api/content_management_system/{id}`     | `draft` (`active=0`) / `published` (`active=1`) |

Ghost exige `updated_at` dans toute mise a jour (422 sans) : l'adaptateur le relit
avant chaque ecriture et le renvoie tel quel. Wix exige `revision` dans tout PATCH.
Ghost conserve le vrai `updated_at` pour le PUT, mais sa revision Noyaru est une
empreinte du snapshot (id, titre, slug, HTML, extrait, statut et date). Elle detecte
aussi une edition au meme horodatage. Les appels demandent `formats=html`.
PrestaShop ne fournit pas de date_upd pour les pages CMS : la revision locale est
une empreinte de toutes les valeurs du GET canonique (toutes langues et etat).
POST/PATCH peuvent renvoyer un document partiel ; une relecture suit chaque succes.
Une erreur de relecture apres ecriture reste incertaine, sans nouvel envoi.
Voir PRESTASHOP-CONTRAT-NATIF.md pour les gardes de conservation multilingue.

## Mapping Webflow (field_mapping)

La Data API v2 Webflow n'a pas de schema editorial universel : chaque collection
definit ses propres champs (`name`, `slug`, `body`, `excerpt`...). La configuration
DOIT fournir le mapping explicite :

```python
config = {
    "collection_id": "<id de la collection>",
    "field_mapping": {
        "title": "<slug du champ titre>",    # requis
        "slug":  "<slug du champ slug>",     # requis
        "body":  "<slug du champ corps>",    # requis
        "excerpt": "<slug du champ resume>",   # optionnel
        "author":  "<slug du champ auteur>",   # optionnel (declenche l'exigence
                                                #  de `draft.author` si present)
    },
    "locales": ("en", "fr"),                    # optionnel ; envoye tel quel au
                                                 # nouvel endpoint /items/insert
}
```

Trois cles de mapping sont obligatoires (`title`, `slug`, `body`). Si `author` est
present dans le mapping, l'adaptateur refuse un brouillon sans auteur. L'absence
d'une des trois cles obligatoires leve `AdapterError(code="mapping")` a
l'instanciation (aucun appel reseau).

Publication :
- creation via POST `/v2/collections/{id}/items/insert` (brouillon staged) ;
- publication explicite via POST `/v2/collections/{id}/items/publish` avec
  `{"itemIds": [...]}` ;
- une reponse de publication SANS `publishedItemIds` contenant l'item attendu, ou
  AVEC un champ `errors`, leve `AdapterError(code="partial", uncertain=True)` :
  l'appelant doit verifier cote Webflow. L'adaptateur ne retente pas.

## Auteur (Shopify, Wix)

- **Shopify** : `draft.author` renseigne `article.author.name` dans
  `ArticleCreateInput` / `ArticleUpdateInput`. Un brouillon sans auteur est refuse
  (`AdapterError(code="author")`) AVANT tout appel reseau. Aucune valeur par defaut
  n'est inventee.
- **Wix** : `draft.author` est le `memberId` de l'auteur (chaine). La creation d'un
  draft post via une application tierce exige ce champ ; sans auteur, l'adaptateur
  refuse de meme AVANT tout appel reseau.
- **WordPress** : `draft.author` est OPTIONNEL. S'il est fourni, il doit etre
  l'identifiant numerique du compte (`int`-able). Un nom textuel est refuse
  (`AdapterError(code="author")`), aucune resolution implicite n'est tentee.
- **Ghost** : `draft.author` est optionnel. L'adaptateur le place dans `authors`
  comme `{"email": ...}` si la chaine contient `@`, sinon comme `{"id": ...}`.
- **Webflow** : requis uniquement si `field_mapping` declare un champ `author`.
- **PrestaShop** : les pages CMS n'ont pas de champ auteur natif ; `draft.author`
  n'est pas utilise par cet adaptateur.

## PrestaShop : methodes HTTP

PATCH est officiellement pris en charge par le Webservice PrestaShop depuis
PrestaShop 8 (voir la fiche recapitulative des verbes HTTP du Webservice
PrestaShop 9). Les boutiques PrestaShop 1.7 n'exposent que PUT : cet adaptateur ne
les gere pas, il attend une boutique 8.x+.

| Operation      | Methode  | Chemin                                               |
|----------------|----------|------------------------------------------------------|
| create_draft   | POST     | `/api/content_management_system?...`                 |
| read           | GET      | `/api/content_management_system/{id}?...`            |
| update_draft   | PATCH    | `/api/content_management_system/{id}?...`            |
| publish        | PATCH    | `/api/content_management_system/{id}?...` (id+active)|

`id_shop` (query string) et `id_lang` (element `<language id>`) sont injectes dans
chaque chemin / chaque bloc XML. PATCH d'activation envoie uniquement `<id>` et
`<active>1</active>`, sans ecrire a nouveau les champs texte.

Defenses XML :
- `xml.sax.saxutils.escape` applique a toute valeur utilisateur ;
- corps borne a 2 MiB (`MAX_XML`) a l'ecriture ;
- analyse de la reponse avec `xml.etree.ElementTree.fromstring` ;
- REFUS prealable de toute reponse contenant `<!DOCTYPE` ou `<!ENTITY` dans les
  4 premiers kio : expansion d'entites (billion laughs) et toute surface XXE
  residuelle sont exclues.

## Conversion Ricos (Wix) : sous-ensemble explicite

Supportes :

- Paragraphes `<p>` et titres `<h1>` a `<h6>`.
- Listes `<ul>` / `<ol>` avec `<li>` (une seule profondeur).
- Decorations : `<strong>/<b>` (BOLD), `<em>/<i>` (ITALIC), `<u>` (UNDERLINE).
- Lien `<a href>` : scheme `https`, `mailto:`, `#`, ou chemin absolu `/`.
- Separateur `<hr>` et saut de ligne `<br>` dans un paragraphe.
- Image `<img src alt>` en BLOC AUTONOME (hors paragraphe), URL hebergee.

Refuses (leve `AdapterError(code="validation")`, aucune perte silencieuse) :

- Tableaux (`<table>`), code (`<pre>`, `<code>`), iframe, script, style.
- Images inline dans un paragraphe (Wix attend un bloc autonome).
- Liens `javascript:` ou `data:`.
- Listes imbriquees (profondeur > 1).
- Toute balise non listee ci-dessus.

## Catalogue de codes d'erreur

| code                 | sens                                                               |
|----------------------|--------------------------------------------------------------------|
| `mapping`            | configuration incomplete ou invalide a l'instanciation             |
| `validation`         | contenu invalide (titre/slug absent, HTML hors subset, id invalide) |
| `author`             | auteur obligatoire manquant pour la plateforme                     |
| `revision_required`  | `expected_revision` non fourni a `publish` / `update_draft`        |
| `conflict`           | la version distante a change depuis l'apercu                       |
| `auth`               | 401 / 403                                                          |
| `not_found`          | 404                                                                |
| `limit`              | 429                                                                |
| `refused`            | 4xx autre, ou erreurs GraphQL top-level                            |
| `user_errors`        | GraphQL `userErrors` non vide (HTTP 200)                            |
| `partial`            | publication Webflow partielle / non confirmee (`uncertain=True`)   |
| `server`             | 5xx (`uncertain=True` sur ecriture)                                |
| `network`            | exception transport (`uncertain=True` sur ecriture)                |
| `unreadable`         | reponse sans structure attendue, ou DOCTYPE/ENTITY PrestaShop      |
| `unsupported`        | plateforme inconnue dans la fabrique                                |

## Couverture de tests proposee

Un test par plateforme + un test de contrat partage. Tous les echanges sont
simules par des callables en memoire ; aucun secret n'est utilise ; aucun test
n'est execute contre un CMS reel.

- `tests/test_cms_content_adapters.py` : contrat partage. Types gele (dataclasses
  frozen), fabrique (plateformes inconnues, mappings manquants, instanciation sans
  reseau), validation HTML (refus script/iframe/onclick/javascript/data), conversion
  Ricos (paragraphes, decorations, listes, titres, refus silencieux desactive),
  helper `check_revision`, `wrap_transport_errors` (preservation de `uncertain` depuis
  une CmsError-like du transport), bornage `TransportResponse.json()`.
- `tests/test_cms_content_adapters_wordpress.py` : creation `status=draft`, read,
  publish/update avec revision, conflit sans ecriture, HTTP 401/404/429/502, auteur
  non-entier refuse, HTML dangereux refuse avant reseau, path relatif, preservation
  `uncertain` d'une CmsError-like, traduction `future` -> `scheduled`.
- `tests/test_cms_content_adapters_ghost.py` : creation `status=draft` sans
  `updated_at`, update avec `updated_at` relu, publish `published` + `updated_at`,
  pas de newsletter (`send_email_when_published=False`), conflit sans ecriture,
  auteur par email ou par id, 404 / 502, path relatif, `scheduled`.
- `tests/test_cms_content_adapters_shopify.py` : `isPublished=false` par defaut,
  `blogId` obligatoire, auteur obligatoire, `variables` GraphQL (titre/slug jamais
  dans la requete), `userErrors` en HTTP 200 traite en erreur, erreurs GraphQL
  top-level en `refused`, conflit sans ecriture (pas d'`articleUpdate` envoye),
  requete GraphQL stable entre appels, HTTP 401 / 500, path relatif.
- `tests/test_cms_content_adapters_webflow.py` : `/items/insert` avec `locales`
  pluriel, mapping explicite applique, auteur requis si mappe, publication partielle
  (`errors` ou `publishedItemIds` incomplet) en `partial, uncertain=True` sans
  retentative, conflit sans ecriture (y compris sur publish), HTTP 401 / 429 / 502,
  path relatif, creation reste `draft`.
- `tests/test_cms_content_adapters_wix.py` : `publish:false` a la creation,
  `memberId` obligatoire, Ricos sous-ensemble (titres, listes, liens), REFUS
  explicite de `<table>`, `<script>`, `<iframe>`, `<a href="javascript:">`, conflit
  sans ecriture, revision + `fieldMask` envoyes sur PATCH, HTTP 409 / 502, path
  relatif.
- `tests/test_cms_content_adapters_prestashop.py` : `active=0` a la creation, PATCH
  utilise pour update_draft et pour l'activation (publish), activation XML
  minimale (`<id>` + `<active>` seuls), `id_shop` dans chaque chemin, `id_lang` dans
  chaque bloc `<language>`, DOCTYPE refuse, declaration `<!ENTITY>` sans DOCTYPE
  refusee, XML malforme traite en `unreadable`, HTTP 503 sur ecriture en
  `uncertain=True`, HTML surdimensionne refuse avant reseau, conflit sans ecriture.

Codex execute les tests. Ce document ne remplace pas une recette reelle sur une
instance client ; aucune telle recette n'est ni effectuee ni declaree ici.
