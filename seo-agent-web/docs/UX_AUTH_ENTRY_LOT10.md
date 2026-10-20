# Lot 10 — Connexion et inscription

## Audit avant modification

Base : `origin/main` à `79b50f2`, après fusion de la #34. Revue isolée dans `noyaru-auth-entry` ; aucun fichier de travail de Claude utilisé ou modifié. Aucun `AGENTS.md` ni `CLAUDE.md` suivi dans le dépôt. Les consignes CI et les audits précédents restent applicables.

**Navigateur local (L)** : Chromium, base SQLite isolée, sans session, Google volontairement non configuré, inscription avec code d’invitation ; connexion et inscription en français et anglais, à 1440 et 360 px, soit 8 cas. Aucun POST ni appel fournisseur. **Code (C)** : GET et POST d’authentification, Google, pays, attribution facultative, invitation d’équipe et tests existants. Pas de nouvelle visite de la production ; les comptes et fournisseurs réels nécessitent une recette autorisée.

| Écran / état | Preuve | Problème | Conséquence client | Proposition | Priorité |
| --- | --- | --- | --- | --- | --- |
| Connexion et inscription sans Google configuré | L, C | Bouton Google toujours visible ; le POST ne peut que renvoyer une erreur de configuration. | Choix sans issue dès l’entrée. | Rendre ce choix uniquement lorsque les deux paramètres serveur sont présents ; conserver le contrôle POST. Une configuration ne prouve pas le fonctionnement du fournisseur. | P2 |
| Inscription avec code d’invitation | L, C | Code placé après le bouton Google, alors que le démarrage Google le vérifie. | Retour en erreur sans avoir vu le prérequis. | Placer pays et code avant les deux méthodes. | P2 |
| Inscription depuis une invitation d’équipe avec code global | C | Le GET masque le code car l’email invité suffit pour l’inscription par email ; Google exige encore le code au démarrage. | Une méthode proposée ne peut pas aboutir avec les champs affichés. | Proposer la méthode email dans cet état ; ne pas modifier les autorisations ni prétendre que le paramètre `next` prouve une invitation. | P2 |
| Validation au clavier des deux formulaires | C, à exécuter après modification | Google est le premier bouton de soumission du formulaire ; la validation implicite peut donc déclencher Google depuis les champs email/mot de passe. | Départ vers une autre méthode au lieu de la connexion attendue. | Placer la soumission email en premier dans le DOM ; vérifier Entrée sur les deux formulaires. | P1 |
| Inscription issue d’une campagne | C | Long accord facultatif placé avant les méthodes. | Une information secondaire prend la place des actions d’entrée. | Regrouper sous un volet natif clairement facultatif, avant les deux soumissions ; conserver texte, liens, case décochée, identifiant et consentement explicite. | P2 |
| Lisibilité et accès tactile | L, C | Textes secondaires de 12 px et actions compactes ; hiérarchie héritée. | Lecture et manipulation plus difficiles sur mobile. | CSS limité à ces deux écrans : corps lisible, champs et liens de 44 px, contraste renforcé, focus visible, retours à la ligne. | P2 |
| Réussite, erreur et inscriptions fermées | C | Messages sans rôle explicite ; formulaire encore présent avec boutons désactivés. | Retour d’état et sortie moins évidents. | Messages persistants avec rôles ; fermeture explicite avec connexion et aide utilisables. | P2 |

## Parcours et règles retenus

Connexion : saisir ses identifiants → se connecter ; Google reste une alternative seulement si configuré. Mot de passe oublié, renvoi de vérification, création de compte et aide restent accessibles. Inscription : déclarer le pays, fournir le code si demandé → créer le compte par email ou choisir Google → suivre la vérification et l’accueil déjà existants. La touche Entrée depuis un champ email/mot de passe doit choisir la méthode email.

Le retour `next`, la normalisation serveur, la protection CSRF, les limites de tentative, la vérification email, les restrictions d’inscription et les preuves de consentement restent les autorités existantes. Aucun pays deviné ; aucun état de fournisseur déclaré opérationnel ; aucune nouvelle étape obligatoire. Tous les textes réutilisent les traductions existantes. Les pages juridiques, articles FR/EN, catalogues, moteur de langues et layouts partagés sont réservés au travail de Claude et restent hors lot.

Règles visuelles : identité sombre et verte conservée, une action email principale, choix alternatif séparé par « ou », lien de changement d’écran distinct du formulaire, aide visible, largeur de lecture limitée, espacement commun et contrôles confortables. Pas de JavaScript nécessaire aux méthodes, à l’accord facultatif ou aux sorties.

## Livraison et validation

Le choix Google et le séparateur sont rendus uniquement quand les deux paramètres serveur existent. Pour l’inscription par invitation d’équipe avec code global, seule la méthode email est proposée : aucun assouplissement du démarrage Google ni des droits d’inscription. Le POST refuse toujours un pays invalide avant d’accéder à OAuth. Le test existant de cet ordre distingue désormais la lecture de configuration au GET de la soumission protégée au POST.

La soumission email est le premier bouton du formulaire. Pays et code précèdent les deux méthodes. Les changements d’écran conservent l’email normalisé et `next` ; les erreurs ne réaffichent jamais le mot de passe. Les messages ont des rôles `status` ou `alert` et restent visibles. La fermeture des inscriptions présente son état, la connexion et l’aide. L’accord de campagne reste décoché et explicitement facultatif, avec les mêmes textes, liens et données ; son volet natif fonctionne sans JavaScript. Le texte d’acceptation des CGU et de la confidentialité reste identique.

L’inclusion `_auth_entry_intro.html` et `auth-entry.css` sont réservées à la connexion et l’inscription. Contrôles de 44–46 px, textes secondaires de 14 px, champs de 16 px, labels associés, indication de longueur reliée au mot de passe, focus visible et contraste renforcé. Aucune nouvelle route ni nouveau script.

Preuves dans [ux-auth-entry](ux-auth-entry/) :

- [Référence initiale](ux-auth-entry/baseline.json) : 8 cas, Google non configuré pourtant visible, code après son bouton, texte secondaire de 12 px.
- [Recette après changement](ux-auth-entry/browser.json) : **96 cas** ; français/anglais, 1440, 768, 390 et 360 px. Google configuré ou absent, inscription ordinaire, campagne repliée/ouverte, invitation d’équipe, inscriptions fermées, erreurs réelles locales de connexion et d’inscription au clavier, présentation d’un message de réussite et navigation vers l’aide dans la langue courante. Huit cas supplémentaires sans JavaScript sont inclus dans les 96. Aucun débordement horizontal ni erreur JavaScript.
- **16 POST locaux rejetés**, huit de connexion et huit d’inscription : Entrée depuis le mot de passe déclenche bien l’email, conserve destination/email ou pays/email selon l’écran et efface le mot de passe. Aucun compte créé, aucun email envoyé, aucune requête fournisseur ou modification de site client. Le message de réussite injecté par paramètre vérifie son rendu seulement.
- [Contrastes calculés](ux-auth-entry/contrast.json) : **1 208 échantillons**, minimum **6,54:1** sur les textes échantillonnés. Vérification des hauteurs des contrôles visibles, des labels, du focus et des volets au clavier. Ces mesures ne sont pas une certification d’accessibilité.
- Captures : avant [connexion](ux-auth-entry/before-login-fr-360.png) / [inscription](ux-auth-entry/before-signup-fr-360.png) ; après [connexion ordinateur](ux-auth-entry/login-fr-1440.png), [inscription mobile](ux-auth-entry/signup-fr-360.png), [erreur en anglais](ux-auth-entry/login-error-en-360.png), [accord facultatif](ux-auth-entry/signup-campaign-fr-360.png), [inscriptions fermées](ux-auth-entry/signup-closed-en-390.png).

**153 tests ciblés réussis**, dont 12 nouveaux cas ; compilation Python, Ruff (`F,E9`), Bandit medium/high et `git diff --check` réussis. Commande locale ciblée : `python -m pytest -q tests/test_auth_entry.py tests/test_customer_country_verification.py tests/test_signup_attribution.py tests/test_i18n.py tests/test_verify_link.py tests/test_signup_email_failure.py tests/test_account_team_journey.py tests/test_auth_emails.py`. Les nouveaux tests couvrent les configurations partielles de Google, l’erreur de connexion, les liens de reprise, le refus d’un faux retour d’invitation et la fermeture des inscriptions. La CI complète est suivie sur la PR ; son résultat doit être rattaché à son dernier commit.

## Limites et points restant hors lot

La configuration de Google n’atteste ni sa disponibilité ni un retour OAuth réussi. Connexion Google réelle, délivrabilité des emails, lecteur d’écran humain, gestionnaire de mots de passe, navigateurs mobiles natifs et parcours authentifiés en production restent à vérifier en recette. Les écrans de récupération de mot de passe et de vérification email conservent leur rendu existant ; leurs liens et tests de sécurité sont inclus dans la régression. Les pages juridiques FR/EN restent au travail de Claude, sans modification de cette branche. Aucun travail non publié de Claude n’est visible ni validé.
