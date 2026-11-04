"""Page-local copy avoids concurrent translation catalogues."""

def strings(lang):
    en = lang == 'en'
    def t(fr,english):
        return english if en else fr
    states = {
        'queued':t('Publication autorisée','Publication authorized'),
        'creating_pr':t('Création du brouillon GitHub','Creating the GitHub draft'),
        'waiting_checks':t('Vérifications GitHub en cours','Waiting for GitHub checks'),
        'marking_ready':t('Préparation de la fusion','Preparing the merge'),
        'merging':t('Confirmation de la fusion','Confirming the merge'),
        'waiting_deployment':t('Fusion confirmée, publication attendue','Merged, waiting for deployment'),
        'deployment_confirmed':t('Publication confirmée, analyse à préparer','Deployment confirmed, preparing analysis'),
        'waiting_control':t('Contrôle SEO en cours','SEO check in progress'),
        'verified':t('Correction vérifiée','Correction verified'),
        'review_required':t('Votre relecture est nécessaire','Your review is needed'),
        'revoked':t('Autorisation arrêtée','Authorization stopped'),
        'expired':t('Autorisation expirée','Authorization expired'),
    }
    reasons = {
        'consent_required':t('Validez les deux confirmations après avoir relu les changements.','Review the changes and accept both confirmations.'),
        'pages_budget':t('Choisissez une limite de 1 à 300 pages.','Choose a limit between 1 and 300 pages.'),
        'branch_protection':t('Le dépôt doit imposer des contrôles réussis, une branche à jour et leur application aux administrateurs. Aucun réglage du dépôt n’est modifié.','The repository must enforce successful checks, an up-to-date branch and enforcement for administrators. Repository settings are preserved.'),
        'checks_failed':t('Une vérification GitHub a échoué. Relisez la PR.','A GitHub check failed. Review the PR.'),
        'checks_incomplete':t('Les résultats GitHub sont incomplets. La fusion est arrêtée.','GitHub results are incomplete. The merge has stopped.'),
        'base_changed':t('La branche principale a changé. Les changements doivent être relus.','The base branch changed. The changes need another review.'),
        'pr_changed':t('Le code ou la cible de la PR a changé depuis votre accord.','The PR code or target changed after your approval.'),
        'source_changed':t('Le fichier source a changé. Relisez un nouvel aperçu.','The source file changed. Review a new preview.'),
        'write_unknown':t('La réponse GitHub est incertaine. Noyaru consulte le reçu sans répéter l’écriture.','The GitHub response is uncertain. Noyaru reads back the receipt without repeating the write.'),
        'deployment_target':t('Le reçu de déploiement ne vise pas l’adresse de ce site.','The deployment receipt does not target this site.'),
        'deployment_failed':t('Le déploiement a échoué ou est inactif. Aucun contrôle SEO supplémentaire n’est lancé.','The deployment failed or is inactive. No further SEO check is started.'),
        'deployment_superseded':t('Une autre version a remplacé le commit fusionné. Votre relecture est nécessaire.','Another version replaced the merged commit. Your review is needed.'),
        'quota':t('Le quota de pages disponible ne permet pas ce contrôle.','The available page quota does not allow this check.'),
        'defect_present':t('Le contrôle observe encore le problème. Noyaru s’arrête pour votre relecture.','The check still observes the issue. Noyaru stops for your review.'),
        'control_inconclusive':t('Ce contrôle ne permet pas de confirmer la correction.','This check cannot confirm the correction.'),
        'control_failed':t('L’analyse a été interrompue. Elle ne sera pas relancée par ce mandat.','The analysis stopped. This mandate will not start it again.'),
        'request_budget':t('Le budget de requêtes GitHub est atteint.','The GitHub request budget has been reached.'),
        'paused':t('Les prochaines actions sont en pause. Une requête déjà envoyée peut se terminer.','Further actions are paused. An already sent request may complete.'),
        'revoked':t('Les prochaines actions sont arrêtées. Une fusion déjà effectuée reste publiée.','Further actions have stopped. A completed merge remains published.'),
        'expired':t('Les 24 heures d’autorisation sont écoulées.','The 24-hour authorization has expired.'),
    }
    for code in ('authorized','creating_pr','waiting_checks','marking_ready','merging','waiting_deployment','deployment_confirmed','waiting_control','verified'):
        reasons.setdefault(code,states.get(code,states['queued']))
    reasons['control_queued'] = t('Une seule analyse est en file d’attente.','One analysis is queued.')
    reasons['analysis_busy'] = t('Une analyse existe déjà pour ce projet. Le contrôle attend sa fin.','An analysis already exists for this project. The check waits for it to finish.')
    return dict(title=t('Publier et vérifier une correction','Publish and verify a correction'),
        intro=t('Confiez à Noyaru l’application d’un aperçu que vous avez relu, puis son contrôle.','Let Noyaru apply a preview you reviewed, then check the result.'),
        scope=t('Un aperçu, un fichier, une PR et un contrôle. Le mandat dure 24 heures. Aucun nouvel appel IA ni crédit IA.','One preview, one file, one PR and one check. The mandate lasts 24 hours. No new AI call or AI credit.'),
        requirements=t('Pilote GitHub : contrôles obligatoires et protection stricte de la branche, y compris pour les administrateurs. Le déploiement doit fournir à GitHub un reçu de production pour ce commit et cette adresse de site.','GitHub pilot: required checks and strict branch protection, including administrators. Deployment must report a production receipt to GitHub for this commit and site address.'),
        limit=t('Le contrôle utilise votre quota habituel de pages, dans la limite ci-dessous et de votre abonnement. Une fusion seule ne confirme ni la publication ni la correction SEO.','The check uses your usual page quota, within the limit below and your plan. A merge alone confirms neither deployment nor the SEO correction.'),
        back=t('Préparer les aperçus','Prepare previews'),reload=t('Actualiser le suivi','Refresh tracking'),
        unavailable=t('Le suivi est momentanément indisponible. Aucun nouveau mandat n’a été créé.','Tracking is temporarily unavailable. No new mandate was created.'),
        owner=t('Seul le propriétaire peut autoriser ou arrêter une publication.','Only the owner can authorize or stop publication.'),
        empty=t('Aucun aperçu prêt à publier','No preview ready to publish'),
        empty_text=t('Préparez un aperçu, puis relisez les changements avant de revenir ici.','Prepare a preview, then review the changes before returning here.'),
        review=t('J’ai relu le fichier et les changements ci-dessus. J’autorise la création de la branche et de la PR, puis la fusion de ce code si les contrôles réussissent.','I reviewed the file and changes above. I authorize creating the branch and PR, then merging this code if checks pass.'),
        consent=t('J’autorise un seul contrôle SEO après confirmation du déploiement, dans le budget de pages indiqué.','I authorize one SEO check after deployment confirmation, within the stated page budget.'),
        pages=t('Nombre maximal de pages pour le contrôle','Maximum pages for the check'),
        environment=t('Nom de l’environnement de production dans GitHub','Production environment name in GitHub'),
        authorize=t('Autoriser la publication et un contrôle','Authorize publication and one check'),
        revoke=t('Arrêter les prochaines actions','Stop further actions'),history=t('Suivi des publications','Publication tracking'),
        expires=t('Valable jusqu’au','Valid until'),budget=t('Budget du contrôle','Check budget'),pages_unit=t('pages maximum','pages maximum'),
        pr=t('Relire la PR sur GitHub','Review the PR on GitHub'),job=t('Voir l’analyse','View the analysis'),
        tracking=t('Retrouver la correction','Open the correction'),details=t('Reçus et limites','Receipts and limits'),
        requests=t('Lectures / écritures GitHub','GitHub reads / writes'),diff=t('Changements proposés','Proposed changes'),
        limited=t('Seuls les 20 éléments les plus récents sont affichés.','Only the 20 most recent items are shown.'),
        stopped=t('Arrêt demandé : une requête déjà envoyée peut se terminer. Aucun retour arrière automatique.','Stop requested: an already sent request may complete. No automatic rollback.'),
        fallback=t('Les preuves ou les droits ont changé ou sont indisponibles. Noyaru s’arrête ; relisez le suivi de la correction.','Evidence or permissions changed or are unavailable. Noyaru stops; review correction tracking.'),
        messages={'authorized':t('Autorisation enregistrée. Le suivi se mettra à jour après traitement.','Authorization saved. Tracking will update after processing.'),'revoked':t('Arrêt enregistré.','Stop recorded.')},
        states=states,reasons=reasons)
