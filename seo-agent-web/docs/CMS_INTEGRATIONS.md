# Intégrations CMS

Le choix de plateforme est enregistré dans `Project.settings.cms` sans remplacer les autres réglages. Les instructions du premier correctif et du détail des anomalies utilisent cette préférence. WordPress (Yoast et Rank Math), Shopify, Webflow et Wix disposent de chemins guidés. Les sites sur mesure conservent le parcours manuel et GitHub existant.

## Pilote WordPress + Yoast

Le connecteur utilise l’API officielle Yoast Abilities de WordPress : WordPress 6.9 minimum et Yoast SEO 28.7 minimum pour les titres et descriptions. Les données SEO Yoast doivent être optimisées. La connexion vérifie réellement l’identité, la capacité `wpseo_edit_advanced_metadata` et les schémas des deux fonctions nécessaires. Les permissions propres à chaque publication restent contrôlées par WordPress.

L’authentification emploie un identifiant et un mot de passe d’application WordPress dédié, transmis sur HTTPS, chiffré avec le mécanisme de secrets existant et isolé par propriétaire et identifiant de projet. Le domaine doit correspondre à celui du projet ; les sous-dossiers sont acceptés. Chaque appel résout toutes les adresses DNS et refuse les adresses non publiques. La connexion utilise une IP validée avec certificat TLS et SNI du domaine. Les redirections ne sont jamais suivies avec les identifiants.

Périmètre : titre SEO ou méta-description d’une page ou d’un article publié, identifié dans le rapport d’origine d’une tâche. Les produits, archives, taxonomies, brouillons, contenus, réglages d’indexation et autres métadonnées sont exclus du pilote. Le serveur choisit le champ selon l’anomalie, puis vérifie le permalien et l’identifiant renvoyés par WordPress.

Le parcours lit la valeur actuelle, prépare un aperçu valable vingt minutes, puis attend un POST de validation protégé par CSRF. L’aperçu et les GET n’écrivent jamais sur le CMS. L’avant/après et l’état de l’opération sont conservés dans la nouvelle table `cms_changes` (migration Alembic `20261018_0014`, également créée par `create_tables` sur les bases locales). Les tâches compatibles disposent aussi d’un accès au connecteur dans la page Corrections. L’historique du projet permet de retrouver les confirmations et rétablissements.

L’application relit les deux métadonnées avant l’envoi et refuse une valeur qui a changé, une nouvelle connexion ou une autre identité de page. Le verrou du compte sérialise les écritures Noyaru entre processus. **L’API Yoast ne fournit pas de comparaison atomique avec la valeur précédente** : une édition simultanée dans WordPress entre cette relecture et l’écriture reste possible. L’aperçu n’est pas une transaction distribuée avec l’éditeur WordPress.

L’état « en cours » est enregistré avant l’appel réseau. Une réponse perdue ou une panne locale bloque toute répétition de l’écriture pour cette opération ; le bouton de vérification relit seulement les valeurs actuelles. La restauration remet la valeur brute précédente, y compris `null` pour retrouver un modèle Yoast, seulement si les métadonnées correspondent encore à la correction. Un succès d’écriture laisse la tâche « en cours » : seul le vérificateur existant, après une nouvelle analyse publique, peut confirmer la résolution SEO. Le contrôle conserve ses quotas habituels ; le connecteur ne consomme aucun crédit IA.

La déconnexion supprime le secret pour ce projet ; la révocation distante du mot de passe d’application se fait dans WordPress. La suppression du projet efface aussi ses identifiants CMS et son journal. La suppression du compte bénéficie des cascades existantes.

## Validation et disponibilité

Tests locaux : contrat des appels, droits et isolation des comptes, chiffrage, validation du domaine/DNS/TLS, CSRF, absence d’écriture avant validation, concurrence, valeurs périmées, pertes de réponse, panne du journal, vérification et rétablissement. Les essais de navigation utilisent une API WordPress simulée, jamais un site client. Le laboratoire optionnel `ops/wordpress-lab` complète ces tests avec WordPress et Yoast réels : connexion, aperçu sans écriture, application et rétablissement des deux champs, HTML réel, contrôle Noyaru, droits, modifications externes et déconnexion. Le workflow « WordPress pilot » peut être relancé depuis GitHub Actions sans site WordPress personnel.

Le connecteur est présenté comme un pilote. Un essai entre Noyaru en ligne et un WordPress de démonstration hébergé reste nécessaire pour valider les particularités d’un hébergeur, d’un pare-feu ou d’une extension. Le laboratoire local ne valide pas cette liaison HTTPS publique. Le [paquet hébergé optionnel et son scénario](../ops/wordpress-lab/HOSTED.md) préparent cet essai dans un service distinct, avec des données fictives et des identifiants éphémères. Aucun essai public ni déploiement de ce service n'est déclaré effectué par les tests locaux. Shopify, Webflow, Wix et Rank Math bénéficient d’instructions manuelles, pas de connecteur d’écriture dans cette version.

## Références officielles

- [API des métadonnées Yoast](https://developer.yoast.com/features/yoast-seo-abilities/posts-seo-data/)
- [Prérequis des fonctions Yoast](https://developer.yoast.com/features/yoast-seo-abilities/overview/)
- [Yoast 28.7 : titres et descriptions](https://developer.yoast.com/changelog/yoast-seo/28.7/)
- [Authentification WordPress](https://developer.wordpress.org/rest-api/using-the-rest-api/authentication/)
- [Édition de l’extrait Yoast](https://yoast.com/snippet-preview/)
- [Métadonnées Rank Math](https://rankmath.com/kb/seo-meta-tags/)
- [Référencement Shopify](https://help.shopify.com/fr/manual/promoting-marketing/seo/adding-keywords)
- [Titres et descriptions Webflow](https://help.webflow.com/hc/en-us/articles/33961237278611-Add-SEO-title-and-meta-description)
- [Réglages SEO Wix](https://support.wix.com/en/article/customizing-your-pages-seo-settings-in-the-seo-panel)
