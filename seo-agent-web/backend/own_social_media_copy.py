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
            "Pilote : deux appels vocaux au plus par 24 h glissantes, 2 048 jetons de sortie au plus par appel. Ce n’est pas un plafond en euros. Aucun crédit SEO client. Modèle gpt-audio-1.5, voix coral ; seul le texte affiché est transmis.",
            "Pilot: at most two audio calls per rolling 24h, at most 2048 output tokens per call. This is not a euro cap. No customer SEO credit. Model gpt-audio-1.5, coral voice; only the displayed script is sent.",
        ),
        pipeline=t("2. Fabrication et relecture", "2. Production and review"),
        review=t(
            "Regardez la vidéo entière : voix, texte, lisibilité et exactitude. La seconde décision Telegram marque ce fichier prêt à diffuser ; elle ne le publie pas.",
            "Watch the entire video: voice, wording, legibility and accuracy. The second Telegram decision marks this file ready for distribution; it does not publish it.",
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
                    "L’accès fournisseur a changé. Un nouveau mandat est nécessaire.",
                    "Provider access changed. A new mandate is required.",
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
                    "La transcription fournie ne correspond pas au texte approuvé. Le rendu est arrêté.",
                    "The supplied transcript does not match the approved script. Production stopped.",
                ),
                (
                    "audio_invalid",
                    "La réponse vocale n’est pas exploitable. Aucun nouvel appel automatique.",
                    "The audio response is unusable. No automatic retry.",
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
