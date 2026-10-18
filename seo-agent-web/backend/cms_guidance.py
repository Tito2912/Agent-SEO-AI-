"""Platform-specific editing paths; platform selection never implies an API connection."""
from __future__ import annotations

import re

PLATFORMS = {"": "Je ne sais pas encore", "wordpress": "WordPress", "shopify": "Shopify",
             "webflow": "Webflow", "wix": "Wix", "code": "Site développé sur mesure", "other": "Autre CMS"}
SEO_PLUGINS = {"": "Je ne sais pas", "yoast": "Yoast SEO", "rank_math": "Rank Math", "other": "Autre extension"}
SOURCES = {
    "wordpress": "https://wordpress.org/documentation/article/pages/",
    "yoast": "https://yoast.com/snippet-preview/",
    "rank_math": "https://rankmath.com/kb/seo-meta-tags/",
    "shopify": "https://help.shopify.com/fr/manual/promoting-marketing/seo/adding-keywords",
    "webflow": "https://help.webflow.com/hc/en-us/articles/33961237278611-Add-SEO-title-and-meta-description",
    "wix": "https://support.wix.com/en/article/customizing-your-pages-seo-settings-in-the-seo-panel",
}


def settings(project_settings):
    node = (project_settings or {}).get("cms")
    node = node if isinstance(node, dict) else {}
    platform = node.get("platform") if isinstance(node.get("platform"), str) and node.get("platform") in PLATFORMS else ""
    plugin = node.get("seo_plugin") if platform == "wordpress" and isinstance(node.get("seo_plugin"), str) and node.get("seo_plugin") in SEO_PLUGINS else ""
    return {"platform": platform, "label": PLATFORMS[platform], "seo_plugin": plugin}


def field_for_issue(key):
    key = re.sub(r"_(?:not_)?indexable$", "", key or "")
    if key in {"missing_title", "title_too_long", "title_too_short", "duplicate_titles"}:
        return "seo_title"
    if key in {"missing_meta_description", "meta_description_too_long", "meta_description_too_short", "duplicate_meta_descriptions"}:
        return "meta_description"
    return ""


def for_issue(project_settings, key):
    site = settings(project_settings)
    platform, plugin = site["platform"], site["seo_plugin"]
    field = field_for_issue(key)
    steps = []
    if field:
        label = "titre SEO" if field == "seo_title" else "méta-description"
        if platform == "wordpress":
            steps = ["Ouvrez la page ou l’article concerné dans l’administration WordPress."]
            if plugin == "yoast":
                steps += [f"Dans le panneau Yoast SEO, ouvrez l’aperçu de recherche et modifiez le champ {label}."]
            elif plugin == "rank_math":
                steps += [f"Dans Rank Math, ouvrez Général puis Modifier l’extrait (Edit Snippet) et renseignez le champ {label}."]
            else:
                steps += [f"Ouvrez les réglages de votre extension SEO et recherchez le champ {label}. Indiquez l’extension utilisée pour obtenir un chemin plus précis."]
            steps += ["Enregistrez la page avec Mettre à jour. Si le site utilise un cache, actualisez-le avant le contrôle."]
        elif platform == "shopify":
            steps = ["Ouvrez le produit, la collection, la page ou l’article concerné dans Shopify.",
                     f"Dans l’aperçu de référencement sur les moteurs de recherche, ouvrez la modification du référencement puis renseignez le {label}.",
                     "Enregistrez. Pour la page d’accueil, utilisez les préférences de la boutique en ligne."]
        elif platform == "webflow":
            steps = ["Ouvrez les paramètres de la page dans Webflow. Pour une page de collection, ouvrez le modèle et vérifiez les champs de l’élément concerné.",
                     f"Dans SEO Settings, modifiez le {'Title Tag' if field == 'seo_title' else 'Meta Description'}.",
                     "Enregistrez puis publiez sur le domaine du site analysé. Une modification en brouillon ne sera pas visible par Noyaru."]
        elif platform == "wix":
            steps = ["Dans Wix, ouvrez les réglages SEO de la page concernée.",
                     f"Dans les réglages de base du référencement, modifiez le {'titre pour les moteurs de recherche' if field == 'seo_title' else 'champ de description'}.",
                     "Enregistrez puis publiez la modification sur votre site."]
        elif platform == "code":
            steps = [f"Modifiez le {label} dans les métadonnées de la page ou du modèle concerné.",
                     "Relisez la modification dans votre dépôt, puis publiez-la sur le site analysé."]
    elif platform in {"wordpress", "shopify", "webflow", "wix"}:
        steps = [f"Ouvrez la page concernée dans {site['label']} et suivez les instructions du problème ci-dessous.",
                 "Selon le problème, le réglage peut dépendre du thème, d’une extension ou de votre hébergeur. Transmettez les instructions au prestataire si nécessaire.",
                 "Enregistrez et publiez la modification avant de lancer le contrôle."]
    return {**site, "steps": steps, "source": SOURCES.get(plugin if plugin in {"rank_math", "yoast"} else platform, "") if steps and field else "",
            "field": field, "field_label": "Titre SEO" if field == "seo_title" else "Méta-description"}
