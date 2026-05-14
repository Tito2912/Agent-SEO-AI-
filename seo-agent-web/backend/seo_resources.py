from __future__ import annotations

from typing import Any


SEO_RESOURCES: list[dict[str, Any]] = [
    {
        "slug": "audit-seo-technique-checklist-priorites",
        "kind": "Guide",
        "title": "Audit SEO technique : la checklist pour prioriser les corrections",
        "meta_title": "Audit SEO technique : checklist et priorités",
        "description": "Une méthode simple pour transformer un crawl SEO en plan d'action clair : indexabilité, balises, liens internes, performance et suivi.",
        "updated_at": "2026-05-15",
        "reading_time": "10 min",
        "audience": "Freelances, PME et responsables marketing",
        "keywords": ["audit SEO", "crawl SEO", "priorisation SEO", "SEO technique"],
        "summary": [
            "Un audit SEO utile ne se limite pas à lister des erreurs. Il doit aider à décider quoi corriger maintenant, quoi surveiller et quoi ignorer.",
            "Cette checklist classe les problèmes par impact probable sur l'indexation, l'exploration et la conversion.",
            "L'objectif est de transformer un rapport technique en plan d'action lisible par une équipe marketing, un dirigeant ou un client."
        ],
        "scenarios": [
            {
                "title": "Site vitrine B2B",
                "body": "Priorité aux pages services, aux formulaires, aux pages locales et aux contenus qui génèrent déjà des impressions dans Google Search Console."
            },
            {
                "title": "E-commerce",
                "body": "Priorité aux catégories, aux produits stratégiques, aux facettes indexables, aux redirections de produits supprimés et aux sitemaps."
            },
            {
                "title": "Blog ou média",
                "body": "Priorité aux articles qui ont du potentiel, aux contenus orphelins, aux titles dupliqués et au maillage entre contenus proches."
            }
        ],
        "sections": [
            {
                "heading": "Commencer par l'indexabilité",
                "body": [
                    "Avant de retravailler les contenus, vérifiez que les pages importantes peuvent être explorées et indexées. Les erreurs de robots.txt, de meta noindex, de canonical ou de redirection peuvent annuler tout le reste.",
                    "Les pages stratégiques doivent répondre en 200, être présentes dans le sitemap, avoir une canonical cohérente et recevoir des liens internes."
                ],
                "bullets": [
                    "Pages en noindex alors qu'elles doivent générer du trafic.",
                    "Canonicals qui pointent vers une autre URL sans raison claire.",
                    "Pages indexables absentes du sitemap XML.",
                    "Redirections internes vers des URLs non finales."
                ],
            },
            {
                "heading": "Corriger les signaux visibles dans les SERP",
                "body": [
                    "Les balises title et meta descriptions n'ont pas toutes le même poids, mais elles influencent la compréhension de la page et le taux de clic.",
                    "Traitez d'abord les pages qui ont déjà des impressions dans Google Search Console, puis les pages business comme services, catégories, comparatifs et pages locales."
                ],
                "bullets": [
                    "Title manquant, dupliqué ou trop vague.",
                    "Meta description absente sur les pages à fort potentiel.",
                    "H1 absent ou incohérent avec l'intention de recherche.",
                    "Contenu trop proche entre plusieurs pages."
                ],
            },
            {
                "heading": "Transformer l'audit en routine mensuelle",
                "body": [
                    "Un audit ponctuel donne une photo. Une routine mensuelle montre les tendances : nouvelles erreurs, pages corrigées, baisse du nombre de liens cassés et progression des clics.",
                    "Le bon rythme dépend de la taille du site, mais un suivi régulier évite que les problèmes techniques s'accumulent."
                ],
                "bullets": [
                    "Relancer un crawl après chaque mise en production importante.",
                    "Comparer les problèmes critiques d'un mois sur l'autre.",
                    "Documenter les corrections réalisées et leurs effets.",
                    "Prioriser les anomalies qui touchent des pages génératrices de leads ou de ventes."
                ],
            },
            {
                "heading": "Exemple de priorisation après un crawl",
                "body": [
                    "Imaginez un crawl de 800 URLs qui remonte 230 alertes. Le mauvais réflexe consiste à corriger ligne par ligne. Le bon réflexe consiste à regrouper les problèmes par impact.",
                    "Si 18 pages services sont en canonical vers la home, elles passent avant 120 meta descriptions manquantes sur des archives peu visibles. Si 40 liens internes pointent vers une redirection, la correction peut améliorer l'exploration de tout le site."
                ],
                "bullets": [
                    "Critique : pages business non indexables, erreurs 5xx, canonical incohérente, pages importantes en 404.",
                    "Important : titles dupliqués sur pages visibles, H1 absents, profondeur de clic trop élevée, liens internes cassés.",
                    "À surveiller : descriptions manquantes sur pages secondaires, images sans alt, anciennes URLs sans trafic.",
                    "À ignorer temporairement : alertes mineures sur pages non stratégiques ou volontairement exclues."
                ],
            },
            {
                "heading": "Ce qu'un rapport d'audit doit contenir",
                "body": [
                    "Un rapport utile ne doit pas seulement afficher des scores. Il doit expliquer ce qui bloque, quelles URLs sont concernées, pourquoi cela compte et quelle action lancer.",
                    "Pour une équipe non technique, chaque recommandation doit pouvoir devenir une tâche : corriger une canonical, réécrire un title, ajouter une page au sitemap, mettre à jour un lien interne ou relancer un crawl de validation."
                ],
                "bullets": [
                    "Un résumé exécutif en quelques lignes.",
                    "La liste des problèmes critiques avec URLs concernées.",
                    "Une estimation de l'impact et de l'effort.",
                    "Les corrections effectuées depuis le dernier audit.",
                    "Les prochaines actions à planifier avant le prochain crawl."
                ],
            },
        ],
        "checklist": [
            "Exporter la liste des pages indexables.",
            "Identifier les statuts HTTP non 200 sur les pages importantes.",
            "Vérifier sitemap, canonical, title, H1 et maillage interne.",
            "Classer chaque correction par impact business et effort.",
            "Planifier un nouveau crawl après correction."
        ],
        "faq": [
            {
                "question": "Quelle est la différence entre un crawl SEO et un audit SEO ?",
                "answer": "Le crawl collecte les données techniques du site : statuts HTTP, balises, liens, canonicals, profondeur, indexabilité. L'audit interprète ces données pour décider quoi corriger en priorité."
            },
            {
                "question": "Combien de temps faut-il pour faire un audit SEO technique ?",
                "answer": "Un premier audit peut prendre quelques heures sur un petit site et plusieurs jours sur un gros catalogue. Le temps dépend surtout du nombre de pages, de templates et de problèmes à qualifier."
            },
            {
                "question": "Faut-il corriger toutes les alertes d'un audit SEO ?",
                "answer": "Non. Certaines alertes ont peu d'impact ou concernent des pages secondaires. Il faut d'abord traiter les problèmes qui empêchent l'indexation, dégradent l'exploration ou touchent des pages business."
            },
            {
                "question": "Quand relancer un crawl après correction ?",
                "answer": "Relancez un crawl dès qu'un lot de corrections est publié. Cela permet de vérifier que les problèmes sont réellement résolus et qu'aucune régression n'a été introduite."
            }
        ],
        "cta": "Avec SEO Audit, vous pouvez lancer ce contrôle, prioriser les anomalies et suivre les corrections depuis un seul tableau de bord.",
    },
    {
        "slug": "connecter-google-search-console-audit-mensuel",
        "kind": "Tutoriel",
        "title": "Comment utiliser Google Search Console dans un audit SEO mensuel",
        "meta_title": "Google Search Console : audit SEO mensuel",
        "description": "Un tutoriel pour croiser les données Google Search Console avec un crawl SEO et décider quelles pages optimiser en priorité.",
        "updated_at": "2026-05-15",
        "reading_time": "9 min",
        "audience": "Sites vitrines, blogs et e-commerce",
        "keywords": ["Google Search Console", "audit mensuel", "performance SEO", "requêtes SEO"],
        "summary": [
            "Google Search Console montre comment Google voit votre site dans la recherche. Le crawl montre ce qui peut bloquer ou limiter cette visibilité.",
            "En combinant les deux, vous priorisez les pages qui ont déjà de la demande et qui peuvent progresser rapidement.",
            "L'audit mensuel sert à repérer les opportunités, mais aussi à comprendre pourquoi une page visible ne transforme pas encore son potentiel en clics."
        ],
        "scenarios": [
            {
                "title": "Page avec beaucoup d'impressions mais peu de clics",
                "body": "Le sujet intéresse Google et les internautes, mais le title, la description ou l'angle de la page ne déclenchent pas assez de clics."
            },
            {
                "title": "Page en position 8 à 20",
                "body": "La page est proche d'un seuil intéressant. Un meilleur contenu, plus de liens internes et un title plus précis peuvent aider."
            },
            {
                "title": "Page qui perd des clics",
                "body": "Il faut vérifier la concurrence, la fraîcheur du contenu, les changements techniques et les requêtes qui déclinent."
            }
        ],
        "sections": [
            {
                "heading": "Repérer les pages avec potentiel",
                "body": [
                    "Cherchez les pages qui ont beaucoup d'impressions mais un taux de clic faible, ou des positions moyennes proches de la première page.",
                    "Ces URLs sont souvent de bonnes candidates pour améliorer le title, la meta description, l'introduction, les FAQ ou les liens internes."
                ],
                "bullets": [
                    "Impressions élevées et CTR inférieur aux autres pages.",
                    "Position moyenne entre 8 et 20.",
                    "Requêtes pertinentes mais page insuffisamment alignée.",
                    "Pages qui perdent progressivement des clics."
                ],
            },
            {
                "heading": "Croiser performance et technique",
                "body": [
                    "Une page peut avoir du potentiel mais être freinée par des erreurs techniques. Vérifiez ses statuts, ses balises, sa canonical, sa profondeur de clic et ses liens internes.",
                    "Les gains les plus rapides viennent souvent de pages déjà visibles mais mal présentées ou mal reliées."
                ],
                "bullets": [
                    "Page stratégique trop profonde dans l'arborescence.",
                    "Title non aligné avec la requête principale.",
                    "Contenu concurrentiel mais liens internes insuffisants.",
                    "Problèmes de redirection ou de canonical."
                ],
            },
            {
                "heading": "Créer un reporting utile",
                "body": [
                    "Le reporting mensuel doit rester lisible : quelques indicateurs, les actions réalisées, les pages à surveiller et les prochaines priorités.",
                    "Évitez les tableaux trop longs. Un bon rapport doit aider une équipe à décider."
                ],
                "bullets": [
                    "Clics, impressions, CTR et position moyenne.",
                    "Top pages en progression et en baisse.",
                    "Anomalies techniques nouvelles ou corrigées.",
                    "Actions prévues pour le mois suivant."
                ],
            },
            {
                "heading": "Exemple de lecture d'une page à optimiser",
                "body": [
                    "Prenons une page qui reçoit 12 000 impressions mensuelles, 180 clics, un CTR de 1,5 % et une position moyenne de 9,8. Elle est déjà comprise par Google, mais elle n'est pas encore assez convaincante dans les résultats.",
                    "Le premier diagnostic consiste à comparer les requêtes principales, le title, la promesse de la page et les concurrents visibles. Si le title est vague ou trop centré sur la marque, une réécriture peut être prioritaire."
                ],
                "bullets": [
                    "Vérifier si la requête principale apparaît dans le title et le H1.",
                    "Comparer la promesse de la page avec les résultats concurrents.",
                    "Ajouter une section qui répond mieux à l'intention dominante.",
                    "Renforcer les liens internes depuis les pages déjà fortes.",
                    "Suivre le CTR et la position sur le mois suivant."
                ],
            },
            {
                "heading": "Construire une routine mensuelle simple",
                "body": [
                    "Un audit mensuel ne doit pas devenir un export interminable. Il doit répondre à trois questions : quelles pages montent, quelles pages baissent et quelles actions peuvent améliorer la visibilité.",
                    "La meilleure routine consiste à sélectionner 5 à 10 URLs prioritaires, puis à documenter les corrections appliquées. Le mois suivant, vous comparez les mêmes URLs avant d'élargir le périmètre."
                ],
                "bullets": [
                    "Semaine 1 : repérer les pages à potentiel et les baisses.",
                    "Semaine 2 : corriger titles, descriptions, contenu et maillage interne.",
                    "Semaine 3 : relancer un crawl pour valider les points techniques.",
                    "Semaine 4 : suivre les premiers signaux et préparer le prochain lot."
                ],
            },
        ],
        "checklist": [
            "Connecter la propriété Search Console.",
            "Identifier les pages avec impressions et CTR faible.",
            "Comparer ces pages avec les anomalies du crawl.",
            "Optimiser les titres, descriptions et liens internes.",
            "Mesurer l'évolution au prochain rapport."
        ],
        "faq": [
            {
                "question": "Google Search Console suffit-il pour faire un audit SEO ?",
                "answer": "Non. Search Console montre les performances dans Google, mais ne remplace pas un crawl. Il faut croiser les deux pour comprendre si une page manque de visibilité à cause du contenu, du maillage ou d'un blocage technique."
            },
            {
                "question": "Quels indicateurs suivre chaque mois ?",
                "answer": "Les plus utiles sont les clics, impressions, CTR, position moyenne, pages en hausse, pages en baisse et anomalies techniques nouvelles sur les URLs importantes."
            },
            {
                "question": "Faut-il optimiser toutes les pages avec impressions ?",
                "answer": "Non. Priorisez les pages alignées avec vos offres, celles proches de la première page et celles dont le CTR est faible malgré des requêtes pertinentes."
            },
            {
                "question": "Quand voit-on l'effet d'une optimisation Search Console ?",
                "answer": "Il faut souvent attendre plusieurs semaines. Les changements de title peuvent produire des signaux plus vite, tandis que les optimisations de contenu et de maillage demandent plus de recul."
            }
        ],
        "cta": "SEO Audit centralise les données de crawl et de performance pour éviter de jongler entre plusieurs exports.",
    },
    {
        "slug": "corriger-title-meta-description-grande-echelle",
        "kind": "Tutoriel",
        "title": "Corriger les title et meta descriptions à grande échelle",
        "meta_title": "Title et meta descriptions : corrections à grande échelle",
        "description": "Une méthode pour détecter, prioriser et réécrire les balises title et meta descriptions sans perdre de cohérence éditoriale.",
        "updated_at": "2026-05-15",
        "reading_time": "10 min",
        "audience": "Sites avec beaucoup de pages",
        "keywords": ["balise title", "meta description", "SERP", "optimisation SEO"],
        "summary": [
            "Les balises title et meta descriptions sont faciles à négliger lorsque le site grossit.",
            "Une méthode par lots permet de corriger rapidement les pages qui ont le plus d'impact.",
            "Le but n'est pas d'écrire une balise parfaite pour chaque URL, mais de mettre en place une logique éditoriale cohérente et mesurable."
        ],
        "scenarios": [
            {
                "title": "Site de services",
                "body": "Chaque title doit faire comprendre l'offre, la cible et parfois la zone géographique sans devenir artificiel."
            },
            {
                "title": "Catalogue e-commerce",
                "body": "Les modèles de title doivent rester lisibles à grande échelle et éviter les duplications entre catégories, marques et filtres."
            },
            {
                "title": "Blog éditorial",
                "body": "Les titles doivent refléter l'angle réel de l'article et éviter la cannibalisation entre contenus proches."
            }
        ],
        "sections": [
            {
                "heading": "Segmenter avant de réécrire",
                "body": [
                    "Ne traitez pas toutes les pages de la même façon. Séparez les pages commerciales, les articles, les catégories, les pages locales et les pages techniques.",
                    "Chaque groupe doit avoir une logique de rédaction claire."
                ],
                "bullets": [
                    "Pages services : intention, bénéfice, zone ou cible.",
                    "Articles : promesse claire et angle de recherche.",
                    "Catégories : produit, usage et différenciation.",
                    "Pages locales : activité, ville et preuve de confiance."
                ],
            },
            {
                "heading": "Prioriser les doublons et les pages visibles",
                "body": [
                    "Les doublons de title envoient un signal flou. Corrigez-les d'abord sur les pages qui sont indexables et qui reçoivent déjà des impressions.",
                    "Une page invisible dans Search Console peut attendre, sauf si elle est critique pour votre parcours client."
                ],
                "bullets": [
                    "Titles dupliqués sur plusieurs pages indexables.",
                    "Titles trop courts ou remplis du nom de marque uniquement.",
                    "Meta descriptions absentes sur pages stratégiques.",
                    "Balises incohérentes avec le contenu réel."
                ],
            },
            {
                "heading": "Garder une règle éditoriale simple",
                "body": [
                    "La cohérence compte autant que la longueur. Rédigez des titres lisibles, précis et utiles, puis contrôlez les pages après publication.",
                    "Les modèles automatiques sont utiles, mais ils doivent rester adaptés à l'intention de recherche."
                ],
                "bullets": [
                    "Placer le sujet principal tôt dans le title.",
                    "Éviter les titres artificiellement bourrés de mots-clés.",
                    "Écrire une meta description orientée bénéfice ou preuve.",
                    "Relire les pages générées automatiquement."
                ],
            },
            {
                "heading": "Exemples de modèles de title",
                "body": [
                    "Les modèles sont utiles lorsqu'un site contient beaucoup de pages, mais ils doivent être contrôlés. Un bon modèle donne une structure sans produire des titres mécaniques ou répétitifs.",
                    "La marque peut être présente, mais elle ne doit pas prendre toute la place si le site n'est pas déjà très connu sur la requête."
                ],
                "bullets": [
                    "Service local : Audit SEO technique à Lyon - Accompagnement PME.",
                    "Catégorie e-commerce : Chaussures de trail homme - modèles légers et imperméables.",
                    "Article tutoriel : Comment corriger les titles dupliqués sans perdre de trafic.",
                    "Page SaaS : Outil d'audit SEO pour suivre vos corrections techniques."
                ],
            },
            {
                "heading": "Contrôler après publication",
                "body": [
                    "Une réécriture de title ou de meta description doit être suivie. Si une page avait déjà des impressions, surveillez l'évolution du CTR, de la position moyenne et des requêtes associées.",
                    "Il faut aussi relancer un crawl pour vérifier que les balises publiées correspondent bien aux règles prévues. Les CMS, plugins SEO ou templates peuvent parfois réécrire les balises autrement que prévu."
                ],
                "bullets": [
                    "Vérifier que le title rendu dans le HTML est le bon.",
                    "Contrôler les doublons restants après publication.",
                    "Comparer CTR et impressions sur les requêtes principales.",
                    "Repérer les pages dont le title s'écarte du modèle.",
                    "Documenter les règles pour éviter de recréer les mêmes erreurs."
                ],
            },
        ],
        "checklist": [
            "Exporter les titles et meta descriptions.",
            "Filtrer les pages indexables.",
            "Regrouper par type de page.",
            "Corriger les doublons sur pages prioritaires.",
            "Relancer un crawl pour valider les changements."
        ],
        "faq": [
            {
                "question": "Quelle longueur idéale pour une balise title ?",
                "answer": "Il n'existe pas de longueur parfaite. Il faut surtout un title clair, lisible et aligné avec l'intention. En pratique, évitez les titres trop longs qui masquent l'information principale dans les résultats."
            },
            {
                "question": "La meta description influence-t-elle directement le classement ?",
                "answer": "Elle n'est pas un facteur de classement direct comme le contenu principal, mais elle influence la compréhension et peut améliorer le taux de clic si elle répond mieux à l'intention de recherche."
            },
            {
                "question": "Faut-il mettre le nom de marque dans tous les titles ?",
                "answer": "Pas toujours. Sur une marque connue, cela peut renforcer la confiance. Sur une page très concurrentielle, il vaut mieux d'abord clarifier le sujet, l'offre ou le bénéfice."
            },
            {
                "question": "Comment gérer les titles sur un gros site ?",
                "answer": "Travaillez par type de page. Définissez des modèles pour les pages similaires, puis relisez manuellement les pages stratégiques qui génèrent du trafic ou des conversions."
            }
        ],
        "cta": "SEO Audit détecte les titles manquants, trop proches ou dupliqués et vous aide à concentrer l'effort sur les pages importantes.",
    },
    {
        "slug": "frequence-crawl-seo-site-vitrine-ecommerce-blog",
        "kind": "Guide",
        "title": "À quelle fréquence crawler un site vitrine, un e-commerce ou un blog ?",
        "meta_title": "Fréquence de crawl SEO : site vitrine, e-commerce, blog",
        "description": "Un guide pour choisir le bon rythme de crawl SEO selon la taille du site, la fréquence de publication et le risque technique.",
        "updated_at": "2026-05-14",
        "reading_time": "5 min",
        "audience": "Équipes marketing et dirigeants",
        "keywords": ["crawl SEO", "fréquence audit SEO", "site vitrine", "e-commerce"],
        "summary": [
            "Crawler trop rarement laisse les problèmes s'installer. Crawler trop souvent peut créer du bruit.",
            "Le bon rythme dépend de la fréquence des changements et de l'impact business du site."
        ],
        "sections": [
            {
                "heading": "Site vitrine : mensuel ou après chaque refonte",
                "body": [
                    "Un site vitrine change moins souvent, mais une erreur sur une page service peut coûter cher. Un crawl mensuel suffit souvent.",
                    "Ajoutez un crawl après une mise en production, une refonte, un changement de CMS ou une migration d'URL."
                ],
                "bullets": [
                    "Contrôle mensuel des pages services.",
                    "Vérification après modification de menus ou URLs.",
                    "Surveillance des formulaires et pages de conversion.",
                    "Comparaison des erreurs d'un mois sur l'autre."
                ],
            },
            {
                "heading": "E-commerce : hebdomadaire pour les zones critiques",
                "body": [
                    "Les catalogues évoluent vite : produits expirés, filtres, catégories, pagination, facettes et redirections.",
                    "Un crawl hebdomadaire ou ciblé sur les catégories importantes permet de repérer rapidement les pertes d'indexabilité."
                ],
                "bullets": [
                    "Statuts 404 sur produits supprimés.",
                    "Catégories sans contenu ou sans produits.",
                    "Facettes indexables sans valeur SEO.",
                    "Sitemaps produits incohérents."
                ],
            },
            {
                "heading": "Blog ou média : suivre les publications",
                "body": [
                    "Un blog qui publie souvent doit surveiller les liens internes, les articles orphelins et les contenus qui se cannibalisent.",
                    "Un crawl après chaque lot de publication aide à vérifier que les nouveaux contenus sont reliés correctement."
                ],
                "bullets": [
                    "Articles sans liens internes entrants.",
                    "Titres trop proches entre articles.",
                    "Anciennes URLs qui génèrent encore des erreurs.",
                    "Pages d'auteurs ou tags indexées sans stratégie."
                ],
            },
        ],
        "checklist": [
            "Définir les pages critiques du site.",
            "Choisir un rythme par type de site.",
            "Relancer un crawl après chaque changement majeur.",
            "Suivre les nouvelles erreurs plutôt que tout l'historique.",
            "Adapter la fréquence selon les incidents observés."
        ],
        "cta": "SEO Audit permet de relancer des contrôles réguliers et de comparer les résultats pour suivre la qualité technique dans le temps.",
    },
    {
        "slug": "netlinking-opportunites-backlinks-sans-spam",
        "kind": "Guide",
        "title": "Netlinking : trouver des opportunités de backlinks sans spam",
        "meta_title": "Netlinking : opportunités de backlinks sans spam",
        "description": "Des pistes concrètes pour trouver des backlinks utiles, cohérents et durables sans tomber dans les pratiques risquées.",
        "updated_at": "2026-05-14",
        "reading_time": "6 min",
        "audience": "Indépendants, SaaS et PME",
        "keywords": ["netlinking", "backlinks", "SEO off-site", "autorité"],
        "summary": [
            "Le netlinking n'est pas seulement une course au volume. Les liens utiles viennent de contextes cohérents, visibles et durables.",
            "Une stratégie saine part de vos contenus, de vos partenaires et de vos preuves d'expertise."
        ],
        "sections": [
            {
                "heading": "Commencer par les liens les plus naturels",
                "body": [
                    "Listez les partenaires, annuaires professionnels sérieux, clients publics, intégrations, outils utilisés et communautés métier.",
                    "Ces opportunités sont souvent plus crédibles que des liens achetés sur des sites sans rapport."
                ],
                "bullets": [
                    "Pages partenaires et intégrations.",
                    "Articles invités vraiment spécialisés.",
                    "Études de cas avec clients ou prestataires.",
                    "Annuaires métiers modérés et pertinents."
                ],
            },
            {
                "heading": "Créer des actifs qui méritent un lien",
                "body": [
                    "Un contenu utile facilite la prospection : checklist, benchmark, tutoriel, modèle, outil gratuit ou analyse de marché.",
                    "L'objectif est de donner une raison claire de citer votre page."
                ],
                "bullets": [
                    "Guide détaillé sur un problème précis.",
                    "Template ou checklist téléchargeable.",
                    "Données originales ou retour d'expérience.",
                    "Comparatif transparent et maintenu."
                ],
            },
            {
                "heading": "Surveiller la qualité plutôt que le volume",
                "body": [
                    "Un bon backlink doit avoir un contexte thématique, une page indexable, un lien visible et une ancre naturelle.",
                    "Évitez les réseaux de sites sans trafic, les ancres sur-optimisées et les pages remplies de liens sortants."
                ],
                "bullets": [
                    "Lien placé dans un contenu utile.",
                    "Site cohérent avec votre marché.",
                    "Ancre descriptive mais non forcée.",
                    "Page accessible et indexable."
                ],
            },
        ],
        "checklist": [
            "Lister les partenaires et contextes légitimes.",
            "Créer un contenu qui justifie une citation.",
            "Prioriser les sites proches de votre audience.",
            "Suivre les backlinks gagnés et perdus.",
            "Éviter les ancres artificielles et les sites sans cohérence."
        ],
        "cta": "SEO Audit aide à organiser les opportunités de backlinks et à garder une vision claire de votre profil de liens.",
    },
    {
        "slug": "core-web-vitals-lire-signaux-seo",
        "kind": "Tutoriel",
        "title": "Core Web Vitals : lire les signaux sans se perdre dans les scores",
        "meta_title": "Core Web Vitals : comprendre les signaux SEO",
        "description": "Un tutoriel pour interpréter LCP, INP et CLS, puis décider quelles optimisations de performance prioriser.",
        "updated_at": "2026-05-14",
        "reading_time": "5 min",
        "audience": "Équipes web et marketing",
        "keywords": ["Core Web Vitals", "LCP", "INP", "CLS", "performance SEO"],
        "summary": [
            "Les Core Web Vitals mesurent l'expérience réelle des utilisateurs, mais ils doivent être interprétés avec contexte.",
            "Le but n'est pas d'obtenir un score parfait partout, mais de réduire les blocages qui touchent les pages importantes."
        ],
        "sections": [
            {
                "heading": "Comprendre les trois signaux",
                "body": [
                    "LCP mesure le chargement de l'élément principal, INP la réactivité aux interactions et CLS la stabilité visuelle.",
                    "Chaque signal révèle un problème différent. Les traiter ensemble évite les optimisations inutiles."
                ],
                "bullets": [
                    "LCP : image hero, rendu serveur, polices, ressources bloquantes.",
                    "INP : JavaScript lourd, écouteurs d'événements, composants trop coûteux.",
                    "CLS : images sans dimensions, bannières injectées, polices tardives.",
                    "Pages mobiles : souvent les plus sensibles."
                ],
            },
            {
                "heading": "Prioriser les pages avec enjeu SEO",
                "body": [
                    "Toutes les pages lentes ne méritent pas le même effort. Commencez par les pages qui reçoivent des impressions, génèrent des conversions ou servent de pages d'entrée.",
                    "Une page profonde sans trafic peut attendre si une page service critique est lente."
                ],
                "bullets": [
                    "Pages avec trafic organique.",
                    "Pages services et catégories importantes.",
                    "Pages avec taux de conversion élevé.",
                    "Templates réutilisés sur beaucoup d'URLs."
                ],
            },
            {
                "heading": "Mesurer après chaque correction",
                "body": [
                    "Les données terrain évoluent lentement. Combinez mesures laboratoire et suivi réel pour éviter de conclure trop vite.",
                    "Documentez les modifications : images compressées, scripts différés, cache, rendu serveur ou simplification d'interface."
                ],
                "bullets": [
                    "Tester avant/après sur le même template.",
                    "Suivre les pages les plus visibles.",
                    "Éviter d'ajouter de nouveaux scripts sans contrôle.",
                    "Relancer un audit après chaque lot de corrections."
                ],
            },
        ],
        "checklist": [
            "Identifier les templates lents.",
            "Comparer mobile et desktop.",
            "Relier performance et pages SEO prioritaires.",
            "Corriger d'abord les ressources bloquantes évidentes.",
            "Suivre l'évolution au fil des déploiements."
        ],
        "cta": "SEO Audit vous aide à intégrer la performance dans une routine d'audit plus large, sans isoler les Core Web Vitals du reste du SEO.",
    },
]


def all_resources() -> list[dict[str, Any]]:
    return list(SEO_RESOURCES)


def featured_resources(limit: int = 3) -> list[dict[str, Any]]:
    return SEO_RESOURCES[: max(0, int(limit))]


def get_resource(slug: str) -> dict[str, Any] | None:
    clean = str(slug or "").strip().lower()
    for resource in SEO_RESOURCES:
        if resource.get("slug") == clean:
            return resource
    return None
