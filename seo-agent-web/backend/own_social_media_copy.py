"""Production UI copy, not prompts or authority."""


def strings(lang):
    def t(fr, en):
        return en if lang == "en" else fr

    return dict(
        title=t("Fabriquer une vidéo Noyaru", "Produce a Noyaru video"),
        intro=t(
            "Des captures de l’application, une voix off et un rendu à regarder avant diffusion.",
            "App captures, voiceover and a video to watch before distribution.",
        ),
        studio=t("Studio réseaux sociaux", "Social content studio"),
        bot=t("Valider dans Telegram", "Decide in Telegram"),
        reload=t("Actualiser", "Reload"),
        limits=t("Régler mon plafond vocal", "Set my audio call limit"),
        limit_label=t(
            "Appels vocaux au plus par 24 h glissantes",
            "Maximum audio calls per rolling 24h",
        ),
        limit_help=t(
            "Vous choisissez ce plafond Noyaru, de 1 à 100 appels. Chaque appel payant exige toujours votre accord dans Telegram. Ce réglage ne lance aucun appel et ne remet aucun compteur à zéro. Un ancien mandat garde son plafond initial ; préparez une nouvelle fabrication pour utiliser une hausse.",
            "You choose this Noyaru limit, from 1 to 100 calls. Each paid call still requires your Telegram approval. Saving starts no call and resets no counter. An older mandate keeps its original limit; prepare a new production to use an increase.",
        ),
        limit_save=t("Enregistrer mon plafond", "Save my limit"),
        limit_saved=t(
            "Plafond enregistré. Les tentatives passées restent comptées ; aucun appel lancé.",
            "Limit saved. Previous attempts remain counted; no call started.",
        ),
        other_limits=t("Autres limites du pilote", "Other pilot limits"),
        preparation_count=t(
            "Demandes gratuites préparées au plus par 24 h",
            "Maximum free preparations per 24h",
        ),
        storage_count=t(
            "Rendus conservés au plus : 10. Retirez un ancien rendu avant d’en fabriquer un autre si cette limite est atteinte.",
            "Up to 10 stored outputs. Remove an older output before producing another when this limit is reached.",
        ),
        availability=t(
            "Disponibilité du prochain essai", "Availability for the next attempt"
        ),
        remaining=t(
            "Appels vocaux disponibles sur 24 h glissantes",
            "Audio calls available per rolling 24h",
        ),
        budget_info=t(
            "Les appels déjà commencés restent comptés, même si aucune vidéo exploitable n’a pu être produite. Cette disponibilité ne réserve pas d’appel.",
            "Started calls remain counted, even if no usable video was produced. This availability does not reserve a call.",
        ),
        retry_at=t(
            "Une nouvelle demande sera possible à partir du",
            "A new request can be made from",
        ),
        retry_check=t("Vérifier la disponibilité", "Check availability"),
        preview=t("1. Découvrir le storyboard", "1. Preview the storyboard"),
        scope=t(
            "Première démonstration : comprendre le score de santé. Captures de l’application sur un projet fictif, sans visage. Les autres sujets restent disponibles dans le studio éditorial.",
            "First demonstration: understand the health score. App captures with a fictional project, no face. Other topics remain available in the editorial studio.",
        ),
        voice=t("Texte de la voix off", "Voiceover script"),
        caption=t("Texte d’accompagnement", "Caption"),
        consent=t(
            "Demander dans Telegram l’autorisation d’un appel vocal OpenAI payant et du montage privé. Aucun coût à cette préparation ; aucune publication sociale.",
            "Request Telegram authorization for one paid OpenAI audio call and a private montage. No cost for this preparation; no social publication.",
        ),
        request=t(
            "Demander la fabrication dans Telegram", "Request production in Telegram"
        ),
        limit=t(
            "Pilote : plafond vocal choisi ci-dessus, 4 096 jetons de sortie au plus par appel. Ce n’est pas un plafond en euros. Aucun crédit SEO client. Modèle gpt-audio-1.5, voix coral ; seul le texte affiché est transmis.",
            "Pilot: audio call limit selected above, at most 4096 output tokens per call. This is not a euro cap. No customer SEO credit. Model gpt-audio-1.5, coral voice; only the displayed script is sent.",
        ),
        pipeline=t("2. Fabrication et relecture", "2. Production and review"),
        review=t(
            "Regardez la vidéo entière : voix, texte, lisibilité et exactitude. La seconde décision Telegram marque ce fichier prêt à diffuser ; elle ne le publie pas.",
            "Watch the entire video: voice, wording, legibility and accuracy. The second Telegram decision marks this file ready for distribution; it does not publish it.",
        ),
        comparison_accepted=t(
            "Vous avez relu et accepté cette version. L’écart reste visible pour garder la trace de votre décision.",
            "You reviewed and accepted this version. The difference remains visible as a record of your decision.",
        ),
        comparison=t("Écart de narration à relire", "Narration difference to review"),
        comparison_done=t("Écart de narration accepté", "Narration difference accepted"),
        compare_help=t(
            "La transcription reçue diffère du script. Écoutez la vidéo entière, puis vérifiez les deux textes. La transcription du fournisseur aide cette relecture ; elle ne prouve pas les mots réellement prononcés. Rien n’est marqué prêt sans votre décision explicite dans Telegram.",
            "The received transcript differs from the script. Listen to the entire video, then check both texts. The provider transcript helps this review; it does not prove what was actually spoken. Nothing is marked ready without your explicit Telegram decision.",
        ),
        expected=t("Script approuvé", "Approved script"),
        received=t("Transcription reçue", "Received transcript"),
        differences=t("Différences dans le texte", "Text differences"),
        diff_key=t(
            "Texte barré : retiré du script. Texte souligné : ajouté dans la transcription reçue.",
            "Struck-through text: removed from the script. Underlined text: added in the received transcript.",
        ),
        download=t("Télécharger la vidéo", "Download video"),
        waiting=t(
            "La progression se met à jour ici. Vous pouvez quitter cette page ; retrouvez la décision dans votre bot privé.",
            "Progress updates here. You can leave this page; find the decision in your private bot.",
        ),
        cancel=t("Arrêter et retirer ce rendu", "Stop and remove this output"),
        cancel_info=t(
            "Un appel vocal déjà commencé peut être facturé. Arrêter empêche la suite et retire le fichier conservé.",
            "An audio call already started may be billed. Stopping prevents further steps and removes the stored file.",
        ),
        unavailable=t(
            "Le suivi est momentanément indisponible.",
            "Tracking is temporarily unavailable.",
        ),
        recent=t("Mes dernières fabrications", "My recent productions"),
        empty=t("Aucune fabrication pour le moment.", "No production yet."),
        fresh=t("Préparer une nouvelle fabrication", "Prepare a new production"),
        open=t("Ouvrir", "Open"),
        ai=t(
            "Voix générée par IA · données fictives",
            "AI-generated voice · fictional data",
        ),
        states={
            k: t(fr, en)
            for k, fr, en in [
                (
                    "proposal",
                    "Autorisation de fabrication attendue dans Telegram",
                    "Waiting for production authorization in Telegram",
                ),
                ("queued", "Fabrication autorisée", "Production authorized"),
                ("running", "Fabrication en cours", "Production in progress"),
                ("review", "Vidéo à regarder et relire", "Watch and review the video"),
                (
                    "approved",
                    "Fichier relu, prêt à diffuser",
                    "Reviewed file, ready for distribution",
                ),
                ("stopped", "Fabrication arrêtée", "Production stopped"),
                (
                    "uncertain",
                    "Résultat vocal incertain : aucun nouvel appel automatique",
                    "Uncertain audio result: no automatic retry",
                ),
                ("cancelled", "Fabrication retirée", "Production removed"),
            ]
        },
        errors={
            k: t(fr, en)
            for k, fr, en in [
                (
                    "limit_invalid",
                    "Choisissez un nombre entier de 1 à 100 appels.",
                    "Choose a whole number from 1 to 100 calls.",
                ),
                (
                    "limit_changed",
                    "Le plafond a été modifié dans un autre onglet. Actualisez avant de l’enregistrer.",
                    "The limit changed in another tab. Reload before saving.",
                ),
                (
                    "preparation_limit",
                    "La limite des demandes gratuites préparées sur 24 h est atteinte. Aucun appel lancé.",
                    "The free preparation limit per 24h has been reached. No call started.",
                ),
                (
                    "changed",
                    "Le dossier a changé. Revenez au studio pour préparer une nouvelle version.",
                    "The pack changed. Return to the studio to prepare a new version.",
                ),
                (
                    "voice_required",
                    "La clé OpenAI doit être configurée dans Système pour la voix off.",
                    "Configure the OpenAI key in System for voiceover.",
                ),
                (
                    "renderer_required",
                    "Le moteur vidéo n’est pas disponible sur cette instance.",
                    "The video renderer is unavailable on this instance.",
                ),
                (
                    "captures_changed",
                    "Les captures ne correspondent plus à cette version de l’interface. Une nouvelle livraison de captures est nécessaire.",
                    "Captures no longer match this interface version. A fresh capture release is required.",
                ),
                (
                    "sources_changed",
                    "Les repères du dossier ont changé. Préparez un nouveau dossier après relecture.",
                    "Pack references changed. Prepare a fresh pack after review.",
                ),
                (
                    "inactive",
                    "Activez les validations Telegram dans Système avant la fabrication.",
                    "Enable Telegram approvals in System before production.",
                ),
                (
                    "not_configured",
                    "Configurez les validations Telegram dans Système.",
                    "Configure Telegram approvals in System.",
                ),
                (
                    "connection_changed",
                    "Les accès ou les paramètres vocaux ont changé. Préparez une nouvelle demande à valider dans Telegram.",
                    "Access or audio settings changed. Prepare a fresh request for Telegram approval.",
                ),
                (
                    "binding_changed",
                    "La connexion Telegram a changé ou est suspendue.",
                    "The Telegram connection changed or is paused.",
                ),
                (
                    "expired",
                    "La demande a expiré. Aucun appel supplémentaire n’est autorisé.",
                    "The request expired. No additional call is authorized.",
                ),
                (
                    "font_required",
                    "Les polices du montage sont indisponibles sur cette instance.",
                    "Montage fonts are unavailable on this instance.",
                ),
                (
                    "storage_limit",
                    "Dix rendus sont conservés. Retirez un ancien rendu avant de continuer.",
                    "Ten outputs are stored. Remove an old output to continue.",
                ),
                (
                    "owner_required",
                    "L’accès propriétaire ne permet plus cette fabrication.",
                    "Owner access no longer allows this production.",
                ),
                (
                    "budget",
                    "Le plafond du pilote est atteint. Aucun appel supplémentaire.",
                    "The pilot cap was reached. No additional call.",
                ),
                (
                    "confirm_required",
                    "Cochez la confirmation pour demander la fabrication.",
                    "Check the confirmation to request production.",
                ),
                (
                    "voice_refused",
                    "Le fournisseur a refusé la narration. Aucun nouvel appel automatique.",
                    "The provider refused narration. No automatic retry.",
                ),
                (
                    "narration_changed",
                    "Le contrôle de narration a arrêté cet essai. L’ancien diagnostic peut désigner une transcription différente ou une réponse coupée ; le fichier n’a pas été conservé.",
                    "The narration check stopped this attempt. The older diagnostic can mean a different transcript or a cut-off response; the file was not retained.",
                ),
                (
                    "narration_review",
                    "La narration diffère : regardez la vidéo et comparez les textes avant de l’accepter ou de la refuser dans Telegram.",
                    "The narration differs: watch the video and compare both texts before accepting or rejecting it in Telegram.",
                ),
                (
                    "narration_truncated",
                    "La génération vocale a atteint sa limite de sortie. Réponse coupée, aucun nouvel appel automatique.",
                    "Audio generation reached its output limit. Response cut off; no automatic retry.",
                ),
                (
                    "narration_incomplete",
                    "La génération vocale ne s’est pas terminée normalement. Aucun nouvel appel automatique.",
                    "Audio generation did not finish normally. No automatic retry.",
                ),
                (
                    "audio_invalid",
                    "La réponse vocale n’est pas exploitable. Aucun nouvel appel automatique.",
                    "The audio response is unusable. No automatic retry.",
                ),
                (
                    "audio_missing",
                    "Le fournisseur a répondu sans fichier vocal. Fabrication arrêtée, sans nouvel appel automatique.",
                    "The provider replied without an audio file. Production stopped, with no automatic retry.",
                ),
                (
                    "audio_encoding",
                    "Le fichier vocal reçu est illisible. Fabrication arrêtée, sans nouvel appel automatique.",
                    "The received audio file is unreadable. Production stopped, with no automatic retry.",
                ),
                (
                    "audio_format",
                    "Le format du son reçu n’est pas compatible avec le montage. Fabrication arrêtée, sans nouvel appel automatique.",
                    "The received audio format is incompatible with the montage. Production stopped, with no automatic retry.",
                ),
                (
                    "audio_incomplete",
                    "Le fichier vocal reçu est incomplet. Fabrication arrêtée, sans nouvel appel automatique.",
                    "The received audio file is incomplete. Production stopped, with no automatic retry.",
                ),
                (
                    "audio_duration",
                    "La voix reçue est trop courte ou trop longue pour cette vidéo (8 à 60 secondes). Fabrication arrêtée, sans nouvel appel automatique.",
                    "The received voice is too short or too long for this video (8 to 60 seconds). Production stopped, with no automatic retry.",
                ),
                (
                    "render_failed",
                    "Le montage local a échoué. Aucun nouvel appel vocal n’est lancé.",
                    "Local montage failed. No new audio call is made.",
                ),
                (
                    "voice_uncertain",
                    "Le résultat de l’appel vocal est inconnu. Ne relancez pas sans vérifier le fournisseur.",
                    "The audio request result is unknown. Check the provider before starting again.",
                ),
                (
                    "mandate_required",
                    "Aucun mandat Telegram applicable ne permet cette fabrication.",
                    "No applicable Telegram mandate authorizes this production.",
                ),
                (
                    "unavailable",
                    "Le suivi est momentanément indisponible.",
                    "Tracking is temporarily unavailable.",
                ),
            ]
        },
    )
