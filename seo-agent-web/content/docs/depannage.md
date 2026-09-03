---
title: "Dépannage : les problèmes fréquents"
meta_title: "Dépannage — documentation {{app_name}}"
description: "Crawl vide ou incomplet, Search Console qui ne remonte rien, boutons de correction absents, anomalie qui persiste : les causes et les vérifications."
kind: "Dépannage"
section: "Compte et facturation"
order: 51
updated_at: "2026-09-03"
audience: "Tous les plans"
keywords: ["dépannage", "erreurs", "crawl vide", "problème"]
app_href: "/"
related: ["lancer-un-crawl", "connecter-search-console", "connecter-github", "verifier-une-correction"]
faq:
  - question: "À qui écrire si rien de tout cela ne règle mon problème ?"
    answer: "À {{support_email}}, en précisant l'e-mail du compte, le nom du projet, l'heure approximative du problème et une capture d'écran. Ces quatre éléments suffisent presque toujours à retrouver la trace côté serveur."
  - question: "Où voir si le service lui-même a un incident ?"
    answer: "La page Statut affiche l'état des services. Si un composant y est en erreur, inutile de chercher plus loin de votre côté."
---

Les cas qui reviennent, avec la vérification à faire en premier.

## Le crawl ne trouve que la page d'accueil

**Cause la plus fréquente : le maillage interne n'est pas lisible par le crawler.**

- Les liens sont-ils de vraies balises `<a href="…">` ? Un menu construit en JavaScript sans
  liens réels n'est suivi par personne.
- Le `robots.txt` bloque-t-il l'exploration ? Ouvrez `votresite.com/robots.txt`.
- La profondeur maximale est-elle trop basse ? Une profondeur de 1 ne voit que ce qui est lié
  depuis la page de départ.
- L'URL de départ est-elle bien la version canonique — bon protocole, bon `www` ? Sinon le
  premier saut est une redirection.

## Le crawl s'est arrêté avant la fin

Trois causes, et la page du job dit laquelle :

1. **Limite de pages atteinte** — celle du projet, celle du plan, ou le quota mensuel.
2. **Délai maximal du job dépassé** — un site lent, ou beaucoup de pages.
3. **L'hôte a refusé les requêtes** — 403 ou 429. Baissez le nombre de workers et augmentez le
   délai minimum entre requêtes.

Dans le troisième cas, ces refus ne sont **pas** comptés comme des erreurs de votre site : ils
apparaissent comme un incident de crawl.

→ [Lancer un crawl](/docs/lancer-un-crawl)

## Search Console est connectée mais aucune donnée ne remonte

Dans neuf cas sur dix, **la propriété ne correspond pas à l'URL du projet**.

Vérifiez caractère par caractère : `https` contre `http`, avec `www` contre sans, barre finale.
Si vous avez une propriété de type Domaine (`sc-domain:…`), prenez-la : elle couvre tous les
cas.

Autres pistes :

- Le compte Google autorisé a-t-il bien accès à cette propriété ?
- La période demandée est-elle assez large ? Sur un site récent, 7 jours peuvent ne rien
  contenir.
- Le seuil d'impressions minimum n'exclut-il pas tout le trafic d'un petit site ?

Rappel : Google publie avec deux à trois jours de retard.

→ [Connecter Search Console](/docs/connecter-search-console)

## Les boutons de correction n'apparaissent pas

Dans l'ordre de vérification :

1. **Un dépôt est-il rattaché à ce projet ?** La bannière de la page Corrections le dit.
2. **GitHub est-il toujours connecté ?** Un jeton révoqué côté GitHub coupe la connexion sans
   prévenir.
3. **L'anomalie est-elle corrigeable dans le code ?** Une erreur 5xx ou une décision
   éditoriale n'a pas de bouton, et c'est volontaire.
4. **Votre plan inclut-il les corrections IA ?** Le plan Free ne les inclut pas.

→ [Connecter GitHub](/docs/connecter-github)

## « Correction créée » mais rien dans mon dépôt

Regardez le lien de la pull request sur la tâche, page Corrections. Elle est presque toujours
là, sur une branche dédiée — et pas sur votre branche principale, ce qui est le comportement
attendu.

Si la PR existe mais n'apparaît pas dans votre liste habituelle, vérifiez que vous regardez le
bon dépôt et la bonne branche cible.

## L'anomalie est toujours là après la fusion

Trois causes, dans cet ordre :

1. Le déploiement n'a pas eu lieu, ou a échoué.
2. Un cache sert encore l'ancienne version — CDN, cache de page, service worker.
3. Le crawl de vérification est antérieur au déploiement.

Test rapide : ouvrez l'URL en navigation privée et regardez le code source. Si le correctif y
est, c'est un cache. S'il n'y est pas, c'est un déploiement.

→ [Vérifier une correction](/docs/verifier-une-correction)

## Un score a chuté sans que je change rien

Presque toujours explicable :

- **De nouvelles pages ont été publiées** et portent des anomalies de gabarit.
- **Le crawl précédent était partiel** — annulé, ou arrêté sur une limite. Comparer un crawl
  complet à un crawl partiel n'a pas de sens.
- **Une ressource externe liée est tombée**, ce qui fait apparaître des liens cassés.

Ne comparez jamais deux scores : comparez les deux crawls. La vue de comparaison nomme
précisément ce qui est apparu.

→ [Lire le rapport](/docs/lire-le-rapport)

## Une anomalie que je crois fausse

Ouvrez la fiche de l'anomalie et regardez la colonne de preuve : elle affiche ce que le
crawler a **exactement** reçu.

Deux cas fréquents où le désaccord est apparent :

- **Vous regardez la page rendue, le crawler a lu le HTML servi.** Si une balise est injectée
  par JavaScript après coup, elle peut être absente de la réponse initiale.
- **Vous êtes connecté, le crawler ne l'est pas.** Une page qui change selon la session n'est
  pas la même des deux côtés.

Si la preuve ne correspond toujours pas à ce que vous constatez, écrivez à {{support_email}}
avec l'URL et l'anomalie concernée : c'est exactement le genre de cas qui améliore le
crawler.

## Écrire au support

{{support_email}}, avec quatre éléments : l'e-mail du compte, le nom du projet, l'heure
approximative du problème, une capture d'écran. Ils suffisent presque toujours à retrouver la
trace côté serveur.
