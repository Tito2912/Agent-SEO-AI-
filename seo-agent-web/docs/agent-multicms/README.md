# Agent Noyaru — première livraison multi-CMS

État du 18 novembre 2026 : code en branche isolée, contrôles locaux et recettes simulées.
Aucun déploiement ni contenu client publié. Une recette WordPress locale réelle,
jetable, est réussie ; les autres environnements restent à éprouver. Ce document
distingue le code, les tests et la recette réelle.

## Ce que contient ce lot

- Un plan de trois actions au plus, justifiées par les données du crawl, les requêtes Search Console et les sujets concurrents accessibles selon le forfait. Les pages importantes et l’activité guident le choix ; aucun volume de recherche n’est inventé.
- Les propositions écartées sont mémorisées. Ce choix ne devient jamais une preuve de correction réalisée.
- Un parcours éditorial séparé des anciens connecteurs de titres/méta : article déjà rédigé → aperçu isolé → consentement → brouillon CMS → demande explicite de publication sur les plateformes autorisées.
- Des intentions persistantes avant les écritures distantes ; un double clic ou un timeout ne déclenche pas une nouvelle création. Une relecture distante permet de confirmer un identifiant connu ; une version modifiée ailleurs ne remplace pas l’aperçu approuvé.
- Le transfert natif, consenti et journalisé des images privées sur WordPress et Ghost. Les autres adaptateurs exigent pour l’instant des images déjà hébergées publiquement, et refusent les références privées non transférées.
- Des mesures par page sur deux fenêtres de 7 ou 28 jours, de même périmètre, autour d’une date de modification. Le jour du changement est exclu ; les données finales Google doivent être disponibles. Les clics/impressions/CTR/position ne deviennent ni revenus ni attribution causale.

## Couverture exacte

| Plateforme | Contenu natif préparé | Images privées Noyaru | Publication dans ce lot | Recette réelle |
|---|---|---|---|---|
| WordPress autohébergé | Articles REST natifs, indépendamment de Yoast | Transfert natif | Demande explicite après brouillon | Texte : réussie sur WordPress 6.9.11 local jetable ; hébergeur HTTPS et médias à faire |
| Ghost | Articles Admin API ; aucun envoi newsletter | Transfert natif | Demande explicite après brouillon | À faire |
| Webflow | Éléments d’une collection choisie ; mapping explicite du champ de contenu | Non ; images déjà hébergées | Uniquement les éléments choisis, après case d’accord ; publication du site entier interdite | À faire |
| Wix Blog | Nouveaux articles Draft Posts v3 ; sous-ensemble Ricos explicite | Non ; images déjà hébergées | Demande explicite ; Manage Blog + auteur requis | À faire ; ne lève pas l’ancienne limitation SEO sur articles existants |
| PrestaShop 8/9 | Pages CMS natives, une langue/boutique par appel | Non ; images déjà hébergées | Activation explicite de la page ; pas de blog universel | À faire |

Le connecteur vérifie l’association au domaine du projet avant toute écriture. Les anciennes clés de métadonnées ne reçoivent aucun droit supplémentaire. Les nouvelles connexions sont chiffrées, propres au projet, et supprimées avec lui. Chaque écriture CMS exige une case d’accord et conserve avant l’envoi l’acteur, l’horodatage, la destination et l’empreinte du contenu. Les transferts médias conservent leur propre accord.

Le module technique Shopify reste en réserve et n’est exposé par aucun écran ou endpoint client ; il ne fait pas partie de cette livraison. Son application et sa connexion commerciale seront traitées ultérieurement.

## Accès dans l’application de travail

`/projects/<slug>/agent` : plan, préférences et historique.

`/projects/<slug>/content/publication` : plateforme, connexion, choix de l’article et aperçu.

`/projects/<slug>/agent/results` : comparaison des métriques par page.

Les aperçus Chromium dans `previews/` emploient uniquement un compte et un site fictifs. Ils démontrent le rendu et le parcours local, pas une connexion CMS réelle. Le contenu de l’iframe d’aperçu a été chargé en sandbox.

## Avant activation publique

1. Compléter la recette WordPress locale (voir `WORDPRESS-LOCAL.json`) par un hébergeur HTTPS externe et les médias ; rejouer sur les autres environnements CMS de test les droits de lecture, création, relecture, modification concurrente, publication, échec ambigu et média. Ne pas utiliser de site client pour un essai de publication sans accord.
2. Faire relire la matrice et les formulations par le Claude historique, comme demandé sous C078. C079 autorise Webflow page par page et garde Shopify hors interface client ; les CGU/confidentialité restent à mettre à jour avec le lot livré ; aucun fichier juridique public modifié dans ce lot.
3. Ne pas annoncer de compatibilité générale à partir des seuls tests de transport et d’adaptateur. Une API documentée n’est pas une recette réelle.

## Plan complet accepté : lots suivants

| Lot | Suite concrète | Critère de livraison |
|---|---|---|
| Publication multi-CMS | Recettes réelles, médias des autres CMS, onboarding OAuth pertinent, prérequis propres aux boutiques/collections | Un utilisateur peut connecter son site, relire et publier selon les capacités annoncées, sans URL d’image privée cassée |
| Stratégie | Intentions de recherche et résultats Google sourcés, regroupement des sujets, arbitrage amélioration/création, calendrier | Chaque action explique son intérêt métier, les données employées et ses limites ; pas de page concurrente copiée par défaut |
| Qualité éditoriale | Informations métier confirmées, sources et affirmations à vérifier, édition du brouillon, chevauchement des pages | Les promesses, tarifs, réalisations et chiffres sont vérifiables ; le client peut corriger avant publication |
| Mesures | Connexion aux conversions du client, suivi des pages déployées, comparaisons dans la durée | Séparer évolution observée, preuve technique et résultat commercial ; aucun revenu déduit des clics |
| Orchestration | Exécution autorisée par niveau, budget, historique des décisions et adaptation du prochain plan | Une proposition refusée n’est pas relancée ; une écriture interrompue n’est pas répétée aveuglément ; les blocages sont expliqués |
| Visibilité IA | Questions représentatives, réponses/sources conservées, modèles/langues/dates et évolution | Mesure reproductible avec limites documentées ; aucune recommandation par les IA garantie |

Le réseau de backlinks et la multiplication des plateformes ne sont pas des résultats acquis. Les opportunités existantes restent un module distinct.

## Contrats et validation

Voir `CONTRAT-ADAPTATEURS.md`, `TRANSPORT.md`, `STRATEGIE-MESURES.md`, `GSC-PAR-PAGE.md` et le rapport `VALIDATION.md`.
