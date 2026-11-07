# SAV : tarifs publics dans les réponses supervisées

## Constat du pilote

Les captures du pilote des 6 et 7 novembre 2026 confirment la réception d'une demande
dans Gmail avec le libellé SAV, son import dans Noyaru et la réception de la carte
Telegram. Une question publique sur le forfait le moins cher a ensuite été orientée
vers une reprise manuelle avec le motif « informations de compte ». Aucun envoi de
réponse n'est établi par cette recette.

| Problème examiné dans le code | Conséquence | Correction | Priorité |
| --- | --- | --- | --- |
| « forfait », « tarif », « prix » et plusieurs équivalents anglais ne sélectionnent pas la documentation des plans | L'IA ne reçoit pas la référence pertinente | Sélection FR/EN et priorité aux plans dans les questions mêlant plusieurs sujets | P1 |
| Les extraits ignorent les tableaux | Les prix restent absents même si la bonne page est sélectionnée | Lignes de tableau associant chaque montant à sa formule, après résolution des valeurs configurées | P1 |
| La consigne ne distingue pas explicitement tarifs publics et faits de compte | Une question commerciale générale peut être escaladée faute de contexte | Distinction explicite, sans autoriser d'accès au compte, remboursement ou action de facturation | P1 |

Les extraits restent limités à 1 800 caractères, séparateurs compris, sur trois sources.
Une ligne est conservée entière ou omise ; les montants ne sont pas séparés des noms
de formules. Les empreintes portent toujours sur le document complet résolu : un
changement de tarif invalide l'ancienne autorisation avant l'appel payant.

## Parcours et vérification

Demande nouvelle → sources publiques et montants courants dans le dossier Telegram →
autorisation de rédaction → brouillon ou escalade justifiée → autorisation distincte
du texte et du destinataire → envoi unique et reçu fournisseur.

Les tests reproduisent la question du pilote, les questions FR/EN de prix et les
questions mêlant plusieurs sujets. Ils contrôlent aussi les tableaux, la borne des
extraits, l'invalidation après changement de prix et le maintien des quatre motifs
de reprise manuelle. Gmail, IA, Telegram et transport email sont simulés.

Cette correction ne relance pas une demande ayant déjà consommé son appel IA et
n'envoie aucun email du pilote. Une nouvelle rédaction réelle exige une nouvelle
validation, puis l'envoi exact conserve sa propre validation. Une réponse générée
reste à relire : les tests n'affirment pas qu'un modèle ne choisira plus jamais
l'escalade.

Restent à valider séparément : réception des clients sans exception anti-spam de
test, récupération volontaire d'un message écarté, lisibilité des longs dossiers
Telegram et durée de la connexion Google en mode test.

Coordination : copie isolée, même arbre que `main` `4fa9368`. Aucun fichier des
intégrations CMS, traduction ou capture de Claude n'est modifié.
