# Lot 23 — Retrouver la page choisie dans les instructions

Base : `3f35536`, après le lot 21. Worktree et branche séparés. Le lot 22 (#56) est indépendant, avec sa CI réussie et sa revue automatique encore en cours lors du démarrage de ce lot. Aucune branche de Claude, page juridique ou moteur de langues modifié.

## Bilan avant modification

**P** : navigateur de production authentifié, constat en lecture seule du parcours témoin pendant la recette du lot 22. **C** : examen du code. **L** : reproduction locale dans Chromium avec comptes et rapports fictifs. Les vérifications locales après changement sont décrites séparément.

| Écran / état | Preuve | Problème et conséquence | Proposition / priorité |
| --- | --- | --- | --- |
| Correction guidée → instructions détaillées | P, C | Le lien conserve l’anomalie et l’analyse, mais perd l’URL choisie. Le client retrouve plusieurs pages et doit rechercher la sienne avant de préparer une correction. | Transmettre l’URL et ouvrir directement son bloc d’actions. P1. |
| Instructions → liste des exemples | C | Le plan affiche six exemples. Ajouter une recherche `q` ne cible pas ces actions : elle filtre seulement les tableaux détaillés. Une page hors des six exemples reste difficile à retrouver. | Valider la page contre les preuves de l’anomalie, puis la placer en tête des exemples, avec une indication visible et un retour au parcours guidé. P1. |
| Ancien lien → contexte indisponible | C | Le lecteur peut se rabattre sur une autre analyse si celle demandée n’existe plus. Une URL reçue dans un lien ne prouve pas qu’elle appartient à cette anomalie. | Expliquer le contexte introuvable et proposer les pages disponibles ou le retour à la correction, sans inventer de cible. P1. |
| Page choisie → proposition déjà enregistrée | C | Une nouvelle préparation est proposée à côté du reçu existant. | Mettre la relecture du reçu existant en action principale dans le bloc choisi. P2. |
| Bloc choisi → mobile | L | Le nouveau bouton comprime une URL longue à 360 px ; une ancre trop haute passe sous l’en-tête fixe. | Réserver une ligne à l’URL et conserver la sélection visible sous l’en-tête. P1. |

## Parcours et règles communes

Correction guidée → instructions de la même anomalie, de la même analyse et de la même page → aperçu de cette URL, ou relecture de sa proposition existante → retour à la correction guidée.

Le choix est indiqué par le badge existant et une bordure d’accent. L’URL dispose de sa propre ligne ; la date de l’analyse reste visible. Les boutons mesurent au moins 44 px ; le focus clavier et l’ancre restent sous l’en-tête. Un lien ouvre des instructions, jamais une génération ou une confirmation automatique. Sans JavaScript, l’URL, les instructions et le retour restent accessibles ; une explication indique la dépendance de la préparation GitHub.

## Réalisation

- Le lien guidé transmet `page_url`, le timestamp et l’ancre `#page-selection`. Les paramètres internes de l’URL, Unicode et caractères encodés restent intacts.
- La vue valide une URL HTTP(S) par correspondance exacte avec les pages impactées de l’anomalie dans le rapport affiché. Un timestamp demandé différent du rapport de repli empêche le ciblage. Un contexte inconnu n’est pas réinjecté comme lien externe.
- Une page valide est promue dans les six exemples, sans modifier les preuves, compteurs ou suivis. Son bloc conserve les reçus et les attributs d’action existants. Une proposition enregistrée place sa relecture en action principale. La préparation reste secondaire : un reçu ne suffit pas à connaître l’état actuel de la PR sur GitHub, et une PR fermée peut autoriser une nouvelle proposition. Les contrôles existants du serveur restent déterminants.
- Les filtres et liens de pagination conservent le contexte validé. Les liens ordinaires continuent d’afficher leurs exemples habituels. Le message d’indisponibilité et l’aide sans JavaScript ajoutent deux traductions au catalogue existant.

## Vérifications

| Contrôle | Résultat / limite |
| --- | --- |
| Production avant changement | Lien réel guidé sans URL, analyse conservée, correction toujours à faire après le refus de préparation du lot 22. Ce constat ne prouve pas le déploiement du présent lot. Aucun envoi supplémentaire à l’IA. |
| Tests serveur | **91 réussis**, dont **11 nouveaux cas** : lien complet avec paramètres/Unicode, page hors des six exemples, compteurs et preuves inchangés, contextes inconnus ou dangereux, analyse de repli, refus d’accès intercompte, reçu conservé, pagination et liens ordinaires. La lecture ne modifie pas le suivi et ne déclenche ni IA, débit ni écriture GitHub. Un avertissement de dépréciation existant. |
| [Chromium local](ux-issue-context/browser.json) | **20 observations** : parcours guidé et contexte inconnu × FR/EN × 1 440/768/390/360 px, puis deux parcours sans JavaScript et deux états avec PR existante. URL et analyse transmises au dialogue, accord non coché, aucune préparation envoyée. Retour du focus et navigation inverse vérifiés. Aucun débordement final, erreur JavaScript, appel extérieur ou écriture. |
| [Contrastes](ux-issue-context/contrast.json) | **24 échantillons**, minimum **6,82:1**, sur le badge et les actions du bloc. Focus visible, ancre sous l’en-tête et cibles de 44 px vérifiés. Ce n’est pas un audit WCAG intégral. [FR mobile](ux-issue-context/page-context-fr-360.png) · [EN mobile](ux-issue-context/page-context-en-360.png). |
| Contrôles statiques | Compilation Python, Ruff F/E9, Bandit medium/high et espaces vérifiés. Avertissements de syntaxe/de `nosec` existants. La CI complète et la revue du dernier commit sont consignées dans la PR. |

Aucune nouvelle route HTTP ni template. Un paramètre de lecture et une feuille CSS dédiée sont ajoutés. Les tests et images utilisent uniquement un site fictif. Aucun fichier client, identifiant de compte, cookie, source privée ou JSON opérateur brut n’est publié. Aucune nouvelle analyse réelle, génération IA, PR du site témoin, application ou publication n’est attestée par ce lot.
