# Lot 26 — Retrouver son site après une lecture de l'aide

Base initiale : `f1a53c4` ; base finale : `b334f54`, avec les captures FR/EN de Claude. 2 novembre 2026. Branche/worktree isolés.
Les articles, traductions, captures et fichiers de langue restent réservés à Claude.

## Audit avant changement

| Écran / parcours | Preuve | Problème et conséquence | Amélioration | Priorité |
| --- | --- | --- | --- | --- |
| Anomalies → Aide → application | Navigateur de production authentifié et code | Le lien ne transporte ni projet ni analyse ; le CTA de l'article renvoie à `/`. Le client doit retrouver son site et son écran. | Conserver un projet explicitement connu et autorisé, puis ouvrir l'écran concerné. | P2 |
| Menu Documentation → index → article lié | Code | Aucun contexte transporté entre les pages ; le choix du guide interrompt le parcours du site. | Propager un contexte borné entre les seules pages d'aide et guides. | P2 |
| Guide SEO → mise en pratique | Code | Le CTA renvoie toujours au tableau de bord général, même lorsqu'un site autorisé est fourni. | Accès GET à la fonctionnalité concernée, avec le site clairement indiqué. | P2 |
| Lien partagé, anonyme, ancien membre ou projet inconnu | Code, exigences de droits | Un paramètre d'URL seul ne prouve pas l'accès ; il ne doit pas sélectionner une cible de modification. | Revalider le projet à chaque requête ; conserver l'aide générique lorsque le contexte n'est pas autorisé. | P1 |

## Parcours cible et règles

Écran d'un site → aide dans la bonne langue → autres articles → retour à la
fonctionnalité du même site. Un bandeau affiche le site et permet de revenir à
la liste des projets. Aucun site n'est déduit du référent, d'un cookie, du premier
projet du compte ou d'une URL externe.

Les liens restent des consultations GET. Les paramètres privés ne sont pas
recopiés ; le site est vérifié dans le périmètre courant du compte, y compris
les sites partagés. Les métadonnées SEO restent génériques et canoniques. Les
réponses personnalisées ne doivent pas être mises en cache. Sans JavaScript,
la navigation doit rester utilisable.

## Réalisation et validation

Le contexte est résolu une seule fois par requête, à partir du site de l'écran
ou du paramètre explicite `project`. Les droits du propriétaire ou du membre
sont relus ; aucune sélection automatique ou persistante n'est ajoutée. Le
paramètre `crawl` n'est transmis que s'il désigne un rapport encore présent chez
le propriétaire de ce même site. Une analyse absente ou mal formée est écartée.

Les liens du menu, de l'aide de l'écran, des sommaires, articles liés et corps
d'article conservent ce contexte. Le sélecteur de langue réutilise son extension
de liens existante : aucun fichier de langues n'est modifié. Les sources Markdown
et le cache partagé des articles restent intacts ; seuls les liens de la copie
rendue sont adaptés. Les URLs externes, ancres et exemples de code sont conservés.

Les CTA ciblent une liste fixe d'écrans GET adaptés au guide. Ils ne visent ni
un endpoint d'analyse ou de correction, ni une connexion OAuth, ni un export qui
se téléchargerait automatiquement. Les articles de compte gardent leurs liens
Mon compte/Équipe et l'ajout de site garde la liste des projets. Le contexte peut
être quitté explicitement par « Projets » dans le bandeau. Les fonctionnalités
restent soumises aux droits et limites de leurs écrans habituels.

Les réponses personnalisées, y compris le repli français d'un article non
traduit, utilisent `private, no-store` et une politique de référent `same-origin`.
Canonical, alternates et données structurées ne contiennent pas de projet.

| Contrôle | Résultat et portée |
| --- | --- |
| Production avant changement | Anomalies → aide sans projet ni analyse, CTA `/`, constat authentifié en lecture seule. Aucun formulaire soumis. |
| Tests ciblés | 158 réussis, 1 ignoré : droits propriétaire/membre, révocation, contexte inconnu ou ambigu, rapport absent/mal formé, réglages du compte distincts, neuf guides, langues, cache, inventaire et nouveaux tests de captures de Claude. |
| Chromium local | 232 observations : propriétaire/membre, FR/EN, 1 440/1 030/768/390/360 px, plus 360 px sans JavaScript ; anonymes et contexte refusé à 1 440/360 px. Parcours aller-retour, corps d'article, sommaire, pages liées, changement de langue, menu mobile et sortie explicite du contexte. Quatre cas supplémentaires avec nom de site long à 320/360 px, FR/EN. Aucun débordement, erreur JS, requête externe ou écriture HTTP. |
| Accessibilité locale | Bandeau nommé, texte du site échappé, cibles de 44 px et focus visible ; captures locales de travail examinées à 1 440/360 px. Aucune capture de documentation ou traduction de Claude modifiée. |
| Contrôles statiques | Compilation Python et des 90 templates Jinja, Ruff F/E9 et Bandit medium/high sur le nouveau module réussis. CI complète et revue automatique à consigner dans la PR avant fusion. |

La recette du navigateur utilise des comptes, projets et rapports fictifs sur
les vraies routes applicatives. Elle ne vaut pas contrôle humain au lecteur
d'écran. Le constat de production décrit l'état avant changement ; le résultat
après déploiement est consigné séparément dans la PR.

Coordination : le commit de Claude `b334f54` (captures FR/EN, adaptation du moteur
de contenu, textes associés) est intégré avant la validation finale. Aucun de
ses fichiers de contenu, de langues ou d'images n'est modifié par ce lot.
