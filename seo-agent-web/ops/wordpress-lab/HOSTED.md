# Recette depuis Noyaru en ligne

Objectif : tester **Noyaru en ligne → HTTPS public → WordPress + Yoast**, avec le compte Noyaru déjà réservé aux tests. Les tests sur boucle locale ne déclarent pas cette recette réussie.

## Déployer une démonstration distincte

Le fichier optionnel [`render.wordpress-demo.yaml`](../../../render.wordpress-demo.yaml) prépare un **nouveau service**, sans base, disque, groupe de variables ni secret de Noyaru. Après fusion, créer un nouveau Blueprint depuis le dépôt en sélectionnant explicitement ce chemin. Ne jamais synchroniser ce fichier sur le Blueprint de production. Désactiver Auto Sync pour ce nouveau Blueprint. Vérifier le récapitulatif Render avant création ; ne pas sélectionner une offre payante pour contourner une limite sans accord.

L'offre Free comporte des quotas, une mise en veille après inactivité et un stockage éphémère. Le runtime Playground doit encore être validé dans les limites mémoire de cette instance : son démarrage local ne garantit pas son démarrage chez Render. En cas de mémoire insuffisante, arrêter la recette et examiner l'hébergement et son coût. Sources : [services web](https://render.com/docs/web-services), [limites Free](https://render.com/docs/free), [Blueprints](https://render.com/docs/blueprint-spec).

Le service démarre un vrai WordPress / Yoast temporaire. Le frontal public écoute le port Render ; WordPress reste sur boucle locale. Le connecteur Noyaru conserve ses contrôles DNS/TLS. Versions : Node 24.22.0, Playground CLI 3.1.58, PHP 8.3, WordPress branche 6.9, Yoast 28.7.

Le site contient une page fictive. Le mot de passe administrateur par défaut est remplacé ; la passerelle bloque l'administration, la connexion interactive, XML-RPC et les autres écritures. Deux mots de passe d'application jetables servent au test éditeur et au refus des droits insuffisants. Les cookies ne sont pas transmis à WordPress. Les courriels, cron et accès réseau PHP sont désactivés. La page reste indexable pour tester les anomalies correspondantes : **n'ajouter aucune donnée personnelle ni contenu client**.

Après deux heures, la page, l'API et le manifeste renvoient HTTP 410. La santé reste HTTP 200 pour éviter un redémarrage automatique lié à cette expiration. Cela ne supprime ni ne suspend le service Render. Tout redémarrage ou retour de veille peut recréer le site avec de nouveaux identifiants : reprendre avec un nouveau manifeste, une nouvelle connexion et une nouvelle analyse. Supprimer explicitement le service après la recette.

## Récupérer les identifiants

L'origine publique vient de `RENDER_EXTERNAL_URL`. Render génère `NOYARU_WP_DEMO_KEY` dans les variables **de ce seul service**. Le manifeste avec les mots de passe d'application n'est accessible qu'avec l'en-tête `Authorization: Bearer …` sur `/_noyaru-demo/credentials`. Aucun secret n'est écrit dans les logs.

Dans un terminal privé avec Python et `requests`, remplacer uniquement le domaine dans ce script. La clé est demandée sans affichage ; le fichier est créé sans écraser un fichier existant. Conserver la vérification TLS et le refus des redirections. Ne jamais placer ces identifiants dans une URL, une PR, une capture, un message ou un rapport.

```python
import getpass
import json
import os
import requests

origin = "https://VOTRE-SERVICE.onrender.com"
key = getpass.getpass("Clé du service de démonstration : ")
response = requests.get(
    origin + "/_noyaru-demo/credentials",
    headers={"Authorization": "Bearer " + key},
    allow_redirects=False, timeout=90,
)
if response.status_code != 200:
    raise SystemExit("Manifeste indisponible : vérifier le service, la clé ou l'expiration.")
manifest = response.json()
if manifest["site_url"] != origin:
    raise SystemExit("Origine inattendue : arrêter la recette.")
fd = os.open("wordpress-demo.credentials.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, "w") as handle:
    json.dump(manifest, handle)
print("Fichier privé créé ; supprimer après la recette.")
```

Sous Windows, vérifier également les droits du dossier privé. Le manifeste contient `boot_id`, `expires_at`, `site_url`, `url`, les versions et `accounts.editor` / `accounts.subscriber`. Après une recréation, supprimer l'ancien fichier et récupérer un nouveau manifeste.

## Scénario à exécuter

Utiliser le **compte Noyaru de test existant** et ses quotas disponibles. Ne pas contourner les quotas ni activer de facturation pour cette recette. Ne modifier aucun projet client.

1. Ouvrir la page publique `url`. Vérifier le titre HTML « Démo » et l'absence de méta-description. Noter le `boot_id` et les versions, sans identifiants.
2. Créer un **nouveau projet Noyaru réservé à cette démonstration**, avec `site_url`. Lancer une véritable analyse publique et attendre son résultat. Vérifier les anomalies de titre trop court et de méta-description manquante sur cette page. Ne pas importer de rapport synthétique.
3. Choisir WordPress + Yoast dans les intégrations du projet. Essayer `subscriber` : Noyaru doit refuser ses droits. Connecter ensuite `editor` avec son mot de passe d'application.
4. Préparer l'aperçu du titre : « Réparation de vélos à Lyon — Atelier Noyaru Démo ». **Avant validation**, le HTML public doit encore afficher « Démo ». Valider, contrôler le reçu Noyaru et le nouveau titre dans le HTML.
5. Appliquer de même la méta-description : « Découvrez notre atelier fictif de réparation de vélos à Lyon, créé uniquement pour tester les corrections SEO de Noyaru. » Vérifier l'avant/après dans le HTML public et le H1 inchangé.
6. Lancer une nouvelle analyse publique et utiliser le parcours de vérification Noyaru. Un succès Yoast seul ne confirme pas la résolution SEO. Consigner le résultat réellement obtenu, y compris une erreur ou une limite de quota.
7. Depuis l'historique CMS du projet, rétablir chaque valeur d'origine. Vérifier « Démo », l'absence de méta-description et le H1 inchangé. En cas de réponse réseau perdue, utiliser d'abord le contrôle en lecture du reçu : ne pas répéter aveuglément une application.
8. Déconnecter WordPress. Vérifier l'état déconnecté et le refus de préparer une correction avec l'ancienne connexion. Supprimer uniquement le projet créé pour cet essai, le service Render et le fichier privé d'identifiants. La suppression du WordPress éphémère supprime ses comptes et mots de passe d'application.

Si le `boot_id` change, si le site dort ou si le HTTP 410 apparaît, arrêter et recommencer avec une incarnation neuve. Une restauration sur une autre incarnation n'est pas une preuve valable.

Rapport à conserver : date, commit Noyaru déployé, versions WordPress/Yoast, `boot_id`, étapes réussies/échouées, identifiants d'analyse et de reçus sans secrets, restauration et nettoyage. **État initial de cette livraison : recette HTTPS publique non exécutée.**

## Vérifier le paquet localement

```sh
npm run test:gateway
npm run test:hosted
```

Le premier contrôle les secrets, en-têtes, routes, taille des requêtes et expiration. Le second démarre réellement WordPress / Yoast, passe par la passerelle sur boucle locale, contrôle schémas et droits, écrit les deux métadonnées et les restaure. Son rapport ne contient aucun identifiant. Le laboratoire `npm test` conserve ses formulaires Noyaru et sa base isolée. Ces tests ne remplacent pas le scénario depuis `noyaru.com`.
