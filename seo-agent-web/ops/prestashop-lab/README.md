# Laboratoire PrestaShop

Une vraie boutique PrestaShop sur la machine, sans Docker ni installation système, pour éprouver
`backend/prestashop_cms.py` avant chaque mise en ligne. Premier essai le 02/11/2026 : PrestaShop
8.2.8 bilingue (fr, en), 25 constats sur 25.

## Ce que le laboratoire a appris (et que le connecteur applique)

- Un champ vide est **absent** de la réponse JSON du Webservice, pas vide.
- Un `PATCH` qui n'envoie **qu'une langue** d'un champ écrit, **efface les autres langues** et
  répond 500. Le connecteur relit toutes les langues et les renvoie toutes.
- Un `PATCH` de page CMS **réenregistre son contenu** (texte modifié) et peut répondre 500 : le
  connecteur ne modifie pas les pages CMS.
- Sur un produit ou une catégorie, seuls le champ envoyé et `date_upd` changent en base
  (vérifié table par table : stock, déclinaisons, prix, catégories, étiquettes, caractéristiques).
- PrestaShop 1.7 n'a pas de `PATCH` : le connecteur exige la version 8.

## Préparer la boutique

Dans un dossier vide hors du dépôt, désigné par `PRESTASHOP_LAB` :

1. Télécharger et vérifier (empreintes SHA-256 publiées) PHP 8.1 NTS x64
   (windows.php.net, PrestaShop 8 n'accepte pas PHP 8.2+), MariaDB 11.4 winx64 (archive.mariadb.org)
   et `prestashop_8.2.8.zip` (release GitHub, empreinte dans les métadonnées de l'asset).
2. Décompresser : `php81/`, `mariadb-11.4.13-winx64/`, et le `prestashop.zip` intérieur dans `shop/`.
3. `php81/php.ini` : extensions curl, fileinfo, gd, intl, mbstring, mysqli, openssl, pdo_mysql, zip,
   sodium, exif ; `memory_limit = 1024M`.
4. `OPENSSL_CONF` doit désigner `php81/extras/ssl/openssl.cnf`, sinon l'installation échoue
   (`openssl_pkey_get_details(false)`).
5. MariaDB : `mariadb-install-db.exe --datadir=data`, puis
   `mariadbd.exe --datadir=data --port=3307 --bind-address=127.0.0.1 --console`.
6. Installer (sous Git Bash, `MSYS_NO_PATHCONV=1` : sinon `--base_uri=/` devient un chemin Windows) :
   `php install/index_cli.php --language=fr --domain=localhost:8080 --base_uri=/ --db_server=127.0.0.1:3307
   --db_user=root --db_password= --db_name=prestashop --db_create=1 --name="Atelier Presta" --country=fr
   --email=… --password=… --rewrite=1 --fixtures=1`, puis supprimer `shop/install`.
7. `python cle_webservice.py` (webservice activé, clé « Noyaru » aux droits minimaux, jamais affichée),
   `php langue_en.php` (anglais par le pack officiel), puis
   `php -S 127.0.0.1:8080 -t shop router.php`.

## Rejouer l'essai

    PRESTASHOP_LAB=<dossier> python ops/prestashop-lab/essai_local.py

Le script modifie la boutique de laboratoire : jamais contre une vraie boutique.

## Recette des pages CMS natives (C082)

Le scénario ci-dessus concernait les anciennes corrections SEO. La nouvelle
recette `native_publication.py` concerne des pages CMS fictives du lot #95 :
création inactive, révision par empreinte, modification de la seconde langue sans
perdre la première, activation sans changement du texte, HTML public local et
conflit de version. Elle observe séparément la création dans une langue non défaut.

Préparer une **nouvelle boutique jetable 8.2.8**, française par défaut, hors dépôt,
sur un domaine `127.0.0.1:PORT` ou `localhost:PORT`. Les mêmes PHP 8.1 et MariaDB
portables que ci-dessus conviennent ; une extraction Linux en `/tmp` évite toute
installation système. L'installateur CLI doit recevoir explicitement son serveur
et son port de base de données. Aucun site marchand existant ne doit servir ici.

```sh
PRESTASHOP_LAB=/chemin/labo php ops/prestashop-lab/prepare_native.php
# Retirer/renommer le dossier shop/install après l'installation, comme le demande PrestaShop.
# La recette attend shop/install-disabled pour identifier cette préparation.
PRESTASHOP_LAB=/chemin/labo php -S 127.0.0.1:8091 -t /chemin/labo/shop ops/prestashop-lab/router.php
python ops/prestashop-lab/native_publication.py --lab-root /chemin/labo --server http://127.0.0.1:8091 --report /tmp/prestashop-native-report.json
```

`prepare_native.php` vérifie le domaine local, désactive le courrier, ajoute une
langue anglaise pour éprouver l'API et crée une clé aléatoire limitée à
`content_management_system` GET/POST/PATCH. Cette clé reste dans `native-key.txt`
(mode 0600) **hors racine web** ; elle n'est jamais affichée. Une préparation
existante n'est pas écrasée. `fixture.json` ne contient que les métadonnées publiques.
Ne pas copier ces fichiers dans Git. À la fin, arrêter PHP/MariaDB et supprimer les
données de la boutique, la clé et la configuration générée. Aucun hébergement ni
compte externe n'est nécessaire. Le scénario n'utilise pas le transport de
production pour contourner ses refus des adresses privées : son dernier saut
DNS/TLS est remplacé uniquement dans ce processus local de recette.
