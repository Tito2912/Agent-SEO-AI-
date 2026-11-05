"""Owner support copy, deliberately separate from shared CMS/i18n files."""


def strings(lang):
    def t(fr, en):
        return en if lang == "en" else fr

    return dict(
        title=t("SAV email de Noyaru", "Noyaru email support"),
        intro=t(
            "Recevoir les demandes, préparer une réponse utile et décider depuis Telegram.",
            "Receive requests, prepare a useful reply and decide from Telegram.",
        ),
        reload=t("Actualiser la page", "Reload page"),
        back=t("Système", "System"),
        bot=t("Validations Telegram", "Telegram approvals"),
        privacy_title=t("Confidentialité et périmètre", "Privacy and scope"),
        limits=t(
            "Pilote : 20 messages importés, 2 rédactions IA payantes et 10 tentatives d’envoi au plus par 24 h glissantes. Aucun crédit SEO du client n’est utilisé. Le plafond d’appels IA n’est pas un plafond en euros.",
            "Pilot: up to 20 imported messages, 2 paid AI drafts and 10 send attempts per rolling 24 hours. No customer SEO credit is used. The AI request cap is not a monetary cap.",
        ),
        privacy=t(
            "Les copies réduites des demandes et réponses sont chiffrées et retirées de Noyaru après 30 jours. La réduction masque certains secrets ; elle ne garantit pas l’anonymat. Les originaux Gmail et les messages déjà transmis au bot ou au fournisseur suivent leurs propres règles de conservation.",
            "Reduced copies of requests and replies are encrypted and removed from Noyaru after 30 days. Redaction masks some secrets; it does not guarantee anonymity. Gmail originals and messages already sent to the bot or provider follow their own retention rules.",
        ),
        identity=t(
            "Un email reçu ne prouve pas l’identité du titulaire d’un compte. Le SAV n’accède pas aux comptes ou sites clients et ne peut pas rembourser, changer un abonnement ou supprimer un compte.",
            "An incoming email does not prove account ownership. Support does not access customer accounts or sites and cannot issue refunds, change subscriptions or delete accounts.",
        ),
        setup=t("Connecter la réception SAV", "Connect support inbox"),
        scope=t(
            "Google autorise la lecture de la boîte entière. Noyaru limite ses requêtes au libellé SAV confirmé et aux nouveaux messages ; le libellé n’est pas une restriction OAuth de Google. Utilisez une Gmail dédiée au SAV et un projet Google Cloud distinct de Search Console. Noyaru n’importe pas les pièces jointes.",
            "Google grants read access to the whole mailbox. Noyaru limits requests to the confirmed support label and new messages; the label is not a Google OAuth restriction. Use a dedicated support Gmail and a Google Cloud project separate from Search Console. Noyaru does not import attachments.",
        ),
        instructions=t(
            "Dans le projet Google Cloud dédié : activer Gmail API, autoriser le compte de test si nécessaire, configurer un client OAuth Web et ajouter cette adresse de retour exacte. Google peut demander des vérifications selon le statut du projet et les permissions utilisées. Saisir les identifiants uniquement dans ce formulaire.",
            "In the dedicated Google Cloud project: enable Gmail API, allow the test account if needed, configure a Web OAuth client and register this exact redirect URI. Google may require verification depending on project status and scopes. Enter credentials only in this form.",
        ),
        mailbox=t(
            "Adresse Gmail principale qui reçoit contact@noyaru.com",
            "Primary Gmail address receiving contact@noyaru.com",
        ),
        label=t("Libellé Gmail réservé au SAV", "Gmail label reserved for support"),
        label_help=t(
            "Créer ce libellé dans Gmail et y appliquer uniquement les demandes adressées à contact@noyaru.com. Le connecteur ne crée ni filtre ni libellé dans Gmail.",
            "Create this label in Gmail and apply it only to requests addressed to contact@noyaru.com. The connector creates no Gmail filter or label.",
        ),
        client_id=t("Identifiant du client OAuth SAV", "Support OAuth client ID"),
        secret=t("Secret du client OAuth SAV", "Support OAuth client secret"),
        setup_ack=t(
            "J’utilise un compte SAV et un projet Google dédiés. Je comprends l’accès Google à la boîte et la limite appliquée par Noyaru au libellé choisi. Cette configuration remplace l’ancienne liaison et n’envoie aucun email.",
            "I use a dedicated support account and Google project. I understand Google mailbox access and Noyaru’s restriction to the chosen label. This replaces the previous binding and sends no email.",
        ),
        save=t("Enregistrer cette configuration", "Save configuration"),
        connected=t("Compte Google relié", "Google account connected"),
        connect=t("Se connecter à Gmail", "Connect Gmail"),
        connect_ack=t(
            "Autoriser la connexion Google en lecture seule pour cette boîte SAV. Aucun email ni changement Gmail.",
            "Authorize a read-only Google connection for this support inbox. No email or Gmail changes.",
        ),
        activate=t("Activer ou reprendre le SAV", "Enable or resume support"),
        activate_ack=t(
            "Activer la réception et les demandes de validation Telegram. Au premier démarrage ou après expiration du curseur, la lecture part de maintenant ; après une simple pause, elle reprend au dernier point confirmé. Aucun ancien message n’est importé par recherche globale.",
            "Enable receiving and Telegram approval requests. On first activation or after cursor expiry, reading starts now; after a simple pause, it resumes from the last confirmed point. No global search imports old mail.",
        ),
        pause=t("Mettre le SAV en pause", "Pause support"),
        poll=t("Relever les nouvelles demandes", "Check new requests"),
        active=t("Actif", "Active"),
        paused=t("En pause", "Paused"),
        connection=t("Connexion et réception", "Connection and receiving"),
        sender=t("Envoi depuis contact@noyaru.com", "Sending from contact@noyaru.com"),
        sender_help=t(
            "Le transport existant peut être utilisé si son adresse d’envoi est contact@noyaru.com et s’il fournit un reçu API. Sinon, configurer un accès SAV distinct ci-dessous. Une clé ou un envoi accepté ne prouve pas la livraison. Avec Brevo, cette API ne garantit pas le regroupement dans le fil du destinataire.",
            "The existing transport can be used if its sender is contact@noyaru.com and it provides an API receipt. Otherwise configure separate support access below. A key or accepted send does not prove delivery. With Brevo, this API does not guarantee recipient conversation threading.",
        ),
        sender_ready=t("Transport disponible", "Transport available"),
        sender_missing=t(
            "Transport avec reçu à configurer", "Configure a transport with receipts"
        ),
        provider=t("Fournisseur", "Provider"),
        api_key=t(
            "Clé API SAV (jamais affichée après enregistrement)",
            "Support API key (never displayed after saving)",
        ),
        sender_ack=t(
            "J’ai vérifié l’autorisation d’envoyer depuis contact@noyaru.com chez ce fournisseur. Ne pas modifier les emails transactionnels existants ; chaque réponse demandera encore mon accord Telegram.",
            "I checked permission to send as contact@noyaru.com with this provider. Keep existing transactional email settings; every reply still needs my Telegram approval.",
        ),
        sender_save=t("Enregistrer cet accès SAV", "Save support sender access"),
        history=t("Demandes récentes", "Recent requests"),
        empty=t(
            "Aucune demande importée. Après connexion et activation, seuls les nouveaux messages admissibles du libellé confirmé apparaîtront ici.",
            "No imported requests. After connection and activation, only new eligible messages from the confirmed label appear here.",
        ),
        unknown_subject=t(
            "Message écarté ou à examiner", "Ignored message or needs review"
        ),
        question=t("Demande réduite", "Reduced request"),
        language=t("Langue de la réponse", "Reply language"),
        set_language=t("Choisir cette langue", "Use this language"),
        draft=t("Réponse proposée", "Proposed reply"),
        sources=t("Documentation utilisée", "Documentation used"),
        edit=t(
            "Préparer ou corriger manuellement la réponse",
            "Prepare or revise the reply manually",
        ),
        body=t("Texte de la réponse", "Reply text"),
        edit_ack=t(
            "J’ai relu les faits et choisi les documents qui les étayent. Préparer un nouveau brouillon à valider dans Telegram ; aucun email envoyé.",
            "I reviewed the facts and selected supporting documents. Prepare a fresh draft for Telegram approval; send no email.",
        ),
        edit_save=t(
            "Soumettre ce brouillon à Telegram", "Submit draft for Telegram approval"
        ),
        pending=t(
            "La décision attend votre lecture dans le bot privé. Approuver puis confirmer est nécessaire pour chaque appel IA ou envoi.",
            "The decision awaits your review in the private bot. Approve and confirm for each AI request or email.",
        ),
        check=t("Reprendre le travail déjà autorisé", "Resume already authorized work"),
        cancel=t("Écarter cette demande", "Ignore this request"),
        download=t("Télécharger cette copie SAV", "Download support copy"),
        accepted=t(
            "Le fournisseur a accepté cet envoi. Sa livraison et la résolution de la demande ne sont pas confirmées.",
            "The provider accepted this email. Delivery and resolution are not confirmed.",
        ),
        uncertain=t(
            "Le résultat de l’envoi est inconnu. Noyaru ne renverra pas cet email automatiquement. Vérifier les journaux du fournisseur avant une intervention manuelle.",
            "The send outcome is unknown. Noyaru will not resend automatically. Check provider logs before manual intervention.",
        ),
        human=t(
            "À reprendre par une personne, depuis la boîte SAV ou les procédures habituelles. Cette confirmation ne marque pas la demande comme résolue.",
            "A person must take over through the support inbox or normal procedures. Acknowledging this does not mark the request resolved.",
        ),
        gmail=t("Ouvrir Gmail", "Open Gmail"),
        states={
            k: t(fr, en)
            for k, fr, en in [
                ("received", "Rédaction à autoriser", "Writing approval needed"),
                ("review_required", "Examen humain nécessaire", "Human review needed"),
                ("writing_queued", "Rédaction autorisée", "Writing authorized"),
                ("writing", "Rédaction en cours", "Writing"),
                ("draft", "Envoi à valider", "Send approval needed"),
                (
                    "draft_refused",
                    "Réponse refusée, à revoir",
                    "Reply rejected, needs revision",
                ),
                ("send_queued", "Envoi autorisé", "Send authorized"),
                ("sending", "Envoi en cours", "Sending"),
                ("accepted", "Acceptée par le fournisseur", "Accepted by provider"),
                ("send_uncertain", "Envoi non confirmé", "Send unconfirmed"),
                ("human", "Reprise manuelle demandée", "Human handling requested"),
                ("ignored", "Écartée", "Ignored"),
            ]
        },
        errors={
            k: t(fr, en)
            for k, fr, en in [
                (
                    "unavailable",
                    "Le SAV est momentanément indisponible. Aucun nouvel envoi n’est autorisé.",
                    "Support is temporarily unavailable. No new send is authorized.",
                ),
                (
                    "encryption_required",
                    "Le chiffrement des données doit être configuré avant de relier une boîte.",
                    "Configure data encryption before connecting an inbox.",
                ),
                (
                    "credentials_unavailable",
                    "Cet accès chiffré est indisponible. Revoir la connexion.",
                    "Encrypted access is unavailable. Review the connection.",
                ),
                (
                    "configuration_required",
                    "Vérifier l’adresse Gmail, le libellé et les identifiants OAuth du projet SAV.",
                    "Check the Gmail address, label and support project OAuth credentials.",
                ),
                (
                    "separate_client_required",
                    "Utiliser des identifiants OAuth SAV distincts de la connexion Search Console.",
                    "Use support OAuth credentials separate from Search Console.",
                ),
                (
                    "connection_required",
                    "Terminer la connexion Google de la boîte SAV.",
                    "Complete the support inbox Google connection.",
                ),
                (
                    "oauth_changed",
                    "Cette connexion a expiré ou a déjà été utilisée. Recommencer depuis cet écran.",
                    "This connection expired or was already used. Start again here.",
                ),
                (
                    "scope_changed",
                    "Google n’a pas accordé exactement la permission de lecture SAV attendue. Revoir le projet dédié.",
                    "Google did not grant exactly the expected support read permission. Review the dedicated project.",
                ),
                (
                    "mailbox_changed",
                    "Le compte Google choisi ne correspond pas à la Gmail configurée.",
                    "The selected Google account does not match the configured Gmail.",
                ),
                (
                    "label_changed",
                    "Le libellé SAV est absent ou a changé. Vérifier Gmail avant de reprendre.",
                    "The support label is missing or changed. Check Gmail before resuming.",
                ),
                (
                    "not_found",
                    "Le suivi Gmail a expiré ou un message a disparu. Après expiration, une nouvelle activation repart de maintenant ; examiner la période manquante dans Gmail.",
                    "Gmail tracking expired or a message disappeared. After expiry, activation restarts from now; review the missing period in Gmail.",
                ),
                (
                    "read_limit",
                    "La limite de lecture est atteinte. Le suivi reste conservé pour la prochaine fenêtre.",
                    "The read limit was reached. Tracking is kept for the next window.",
                ),
                (
                    "import_limit",
                    "La limite de 20 imports par 24 h est atteinte. Aucun message en attente n’est déclaré traité.",
                    "The 20 imports per 24h limit was reached. Pending messages are not marked handled.",
                ),
                (
                    "attachments_require_human",
                    "Ce format ou ses pièces jointes nécessitent une lecture humaine ; leurs données n’ont pas été importées.",
                    "This format or its attachments need human review; their data was not imported.",
                ),
                (
                    "plain_text_required",
                    "Une version texte admissible manque. Reprendre le message dans Gmail.",
                    "An eligible plain text version is missing. Review the message in Gmail.",
                ),
                (
                    "message_limit",
                    "Le message dépasse les limites de ce pilote. Reprendre dans Gmail.",
                    "The message exceeds pilot limits. Review it in Gmail.",
                ),
                (
                    "sensitive_request",
                    "La demande est sensible et nécessite une reprise humaine.",
                    "The request is sensitive and needs human handling.",
                ),
                (
                    "sender_unconfirmed",
                    "Les coordonnées du correspondant sont ambiguës. Aucun envoi proposé.",
                    "Sender details are ambiguous. No send is proposed.",
                ),
                (
                    "outside_label",
                    "Message hors du libellé SAV autorisé.",
                    "Message outside the authorized support label.",
                ),
                (
                    "outside_support",
                    "Message non adressé à contact@noyaru.com.",
                    "Message not addressed to contact@noyaru.com.",
                ),
                (
                    "outside_window",
                    "Message en dehors de la période de réception autorisée.",
                    "Message outside the authorized receiving window.",
                ),
                (
                    "loop_blocked",
                    "Message automatique ou émis par le SAV : aucune boucle de réponse.",
                    "Automated or support-generated message: no reply loop.",
                ),
                (
                    "thread_changed",
                    "Une autre demande a été reçue dans ce fil. Examiner les messages récents avant de répondre.",
                    "Another request arrived in this conversation. Review recent messages before replying.",
                ),
                (
                    "message_changed",
                    "Le message ou son périmètre a changé. Aucun nouvel envoi.",
                    "The message or scope changed. No new send.",
                ),
                (
                    "inbox_sync_required",
                    "Relever les nouvelles demandes avant de reprendre cette réponse.",
                    "Check new requests before resuming this reply.",
                ),
                (
                    "provider_required",
                    "Configurer le service IA avant de proposer une rédaction.",
                    "Configure the AI service before requesting a draft.",
                ),
                (
                    "sources_required",
                    "La documentation nécessaire est indisponible. Une personne doit reprendre.",
                    "Required documentation is unavailable. A person must take over.",
                ),
                (
                    "sources_changed",
                    "Les sources ont changé. Relire et préparer une nouvelle version de la réponse.",
                    "Sources changed. Review and prepare a fresh reply version.",
                ),
                (
                    "invalid_output",
                    "La réponse préparée n’a pas passé les contrôles. Reprise humaine, sans nouvel appel IA automatique.",
                    "The prepared reply failed checks. Human handling, no automatic AI retry.",
                ),
                (
                    "expired",
                    "L’autorisation a expiré. Aucun nouvel appel ni envoi.",
                    "Authorization expired. No new request or send.",
                ),
                ("paused", "Le SAV est en pause.", "Support is paused."),
                ("busy", "Un travail est déjà en cours.", "Work is already running."),
                (
                    "budget",
                    "Le plafond de ce pilote est atteint.",
                    "The pilot limit was reached.",
                ),
                (
                    "sender_required",
                    "Configurer un transport avec reçu autorisé à envoyer depuis contact@noyaru.com.",
                    "Configure a receipted transport allowed to send from contact@noyaru.com.",
                ),
                (
                    "sender_changed",
                    "L’accès d’envoi a changé : relire la réponse avant une nouvelle validation.",
                    "Sender access changed: review the reply before fresh approval.",
                ),
                (
                    "transport_uncertain",
                    "La réponse du service n’est pas confirmée. Aucun renvoi automatique.",
                    "The service response is unconfirmed. No automatic resend.",
                ),
                (
                    "provider_uncertain",
                    "Un appel IA a déjà été tenté. Aucun deuxième appel automatique.",
                    "An AI request was already attempted. No automatic second request.",
                ),
                (
                    "send_uncertain",
                    "Une tentative d’envoi existe déjà ; ne pas la répéter sans preuve.",
                    "A send attempt already exists; do not repeat it without evidence.",
                ),
                (
                    "connection_denied",
                    "Le service refuse cet accès. Revoir la connexion.",
                    "The service denied access. Review the connection.",
                ),
                (
                    "provider_refused",
                    "Le service n’a pas confirmé la demande. Revoir son suivi.",
                    "The service did not confirm the request. Review its tracking.",
                ),
                (
                    "human_required",
                    "Une personne doit reprendre cette demande.",
                    "A person must take over this request.",
                ),
                (
                    "changed",
                    "La demande ou ses accès ont changé. Consulter son état actuel.",
                    "The request or access changed. Review the current state.",
                ),
                (
                    "owner_changed",
                    "Cette configuration appartient à un autre propriétaire.",
                    "This configuration belongs to another owner.",
                ),
                (
                    "channel_required",
                    "Configurer la réception SAV avant cette action.",
                    "Configure support receiving first.",
                ),
                (
                    "pause_first",
                    "Mettre le SAV en pause avant de remplacer sa connexion.",
                    "Pause support before replacing its connection.",
                ),
                (
                    "confirm_required",
                    "Cocher la confirmation avant de poursuivre.",
                    "Confirm before continuing.",
                ),
                (
                    "account",
                    "La réponse exige des informations de compte.",
                    "The reply requires account information.",
                ),
                (
                    "billing",
                    "La demande concerne une intervention de facturation.",
                    "The request needs billing intervention.",
                ),
                (
                    "security",
                    "La demande concerne la sécurité.",
                    "The request concerns security.",
                ),
                (
                    "unclear",
                    "Les informations ne suffisent pas pour répondre.",
                    "There is not enough evidence to reply.",
                ),
            ]
        },
    )
