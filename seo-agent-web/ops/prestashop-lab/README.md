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
