# Recette depuis Noyaru en ligne

Objectif : tester **Noyaru en ligne → HTTPS public → WordPress + Yoast**, avec le compte Noyaru déjà réservé aux tests. Les tests sur boucle locale ne déclarent pas cette recette réussie.

## Déployer une démonstration distincte

Le fichier optionnel [`render.wordpress-demo.yaml`](../../../render.wordpress-demo.yaml) prépare un **nouveau service**, sans base, disque, groupe de variables ni secret de Noyaru. Après fusion, créer un nouveau Blueprint depuis le dépôt en sélectionnant explicitement ce chemin. Ne jamais synchroniser ce fichier sur le Blueprint de production. Désactiver Auto Sync pour ce nouveau Blueprint. Vérifier le récapitulatif Render avant création ; ne pas sélectionner une offre payante pour contourner une limite sans accord.

Si le service a déjà été créé avec le runtime Node, **ne pas créer un second Blueprint** : après fusion, ouvrir le Blueprint de démonstration existant et lancer **Manual Sync**. Vérifier que les changements concernent uniquement `noyaru-wordpress-demo`, passent son runtime en Docker et conservent `plan: free`. La clé existante reste valable. La synchronisation applique le changement de runtime et déclenche le déploiement ; un simple redéploiement de l'ancien service Node n'applique pas ce changement. Conserver Auto Sync sur No. [Changement de runtime Render](https://render.com/docs/native-runtimes#changing-runtimes-via-blueprint).

L'offre Free comporte des quotas, une mise en veille après inactivité et un stockage éphémère. Elle dispose de 512 Mo et 0,1 CPU. Le runtime Playground a atteint 729 Mio dans une mesure locale ; une limite de tas V8 à 128 Mo n'a pas permis son démarrage. Ce constat local ne prouve pas la cause d'une panne Render, mais justifie son remplacement pour cet hébergement. Le laboratoire Playground reste disponible pour les tests locaux. Sources : [ressources des instances](https://render.com/docs/compute-plans), [limites Free](https://render.com/docs/free).

Le service démarre un vrai WordPress / Yoast avec PHP natif et le plugin SQLite Database Integration. Les archives officielles sont téléchargées et vérifiées par SHA-256 à la construction de l'image. Le démarrage ne télécharge aucun composant. Le frontal Node écoute immédiatement le port Render et répond HTTP 503 jusqu'à la fin de l'installation et au contrôle réel du HTML WordPress. Les logs indiquent les étapes sans identifiants. PHP écoute uniquement sur boucle locale. Le connecteur Noyaru conserve ses contrôles DNS/TLS. Versions : Node 24.22.0, PHP branche 8.3, WordPress 6.9.10, Yoast 28.7, SQLite Integration 2.2.23. Cette base SQLite est réservée à la démonstration, hors de la racine publique.

Le site contient une page fictive. Le mot de passe administrateur est aléatoire et remplacé ; la passerelle bloque l'administration, la connexion interactive, XML-RPC et les autres écritures. Deux mots de passe d'application jetables servent au test éditeur et au refus des droits insuffisants. Les cookies ne sont pas transmis à WordPress. Les courriels et cron sont désactivés ; les appels HTTP externes de WordPress sont bloqués. Le processus PHP ne reçoit pas la clé Bearer de l'opérateur. La page reste indexable pour tester les anomalies correspondantes : **n'ajouter aucune donnée personnelle ni contenu client**.

Après deux heures, la page, l'API et le manifeste renvoient HTTP 410. La santé reste HTTP 200 pour éviter un redémarrage automatique lié à cette expiration. Cela ne supprime ni ne suspend le service Render. Tout redémarrage ou retour de veille peut recréer le site avec de nouveaux identifiants : reprendre avec un nouveau manifeste, une nouvelle connexion et une nouvelle analyse. Supprimer explicitement le service après la recette.

## Récupérer les identifiants

L'origine publique vient de `RENDER_EXTERNAL_URL`. Render génère `NOYARU_WP_DEMO_KEY` dans les variables **de ce seul service**. Conserver cette valeur exactement, y compris les caractères `+`, `/` et le `=` final de son format base64. Les anciennes clés base64url restent acceptées. Le manifeste avec les mots de passe d'application n'est accessible qu'avec l'en-tête `Authorization: Bearer …` sur `/_noyaru-demo/credentials`. Aucun secret n'est écrit dans les logs.

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

Le premier contrôle les secrets, en-têtes, routes, taille des requêtes et expiration. Le second utilise le laboratoire Playground. Le workflow « WordPress pilot » ajoute un job qui construit l'image PHP native et exécute le même scénario sur un conteneur limité à **480 Mio (moins de 512 Mo), 0,1 CPU, sans swap**, en lecture seule avec un `/tmp` privé temporaire. Il vérifie le pic mémoire mesuré dans le cgroup, les droits, les deux écritures et leur restauration, le H1 inchangé, puis un redémarrage avec de nouveaux identifiants et le refus des anciens. Les appels d'API sont limités à 12 secondes, comme le connecteur. Ces résultats dans GitHub ne remplacent pas la recette depuis `noyaru.com` ni la validation de la plateforme Render.

Pour lancer ce contrôle avec Docker installé :

```sh
docker build -t noyaru-wordpress-free -f native/Dockerfile .
NOYARU_DEMO_CONTAINER_IMAGE=noyaru-wordpress-free node hosted-check.mjs
```

Le programme supprime uniquement son conteneur et son fichier temporaire d'environnement après l'essai. Le laboratoire `npm test` conserve ses formulaires Noyaru et sa base isolée.
