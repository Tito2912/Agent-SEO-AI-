# -*- coding: utf-8 -*-
"""Parmi les familles VIVANTES, lesquelles une EDITION DE FICHIER repare-t-elle vraiment ?

    python seo-agent-web/ops/reparabilite.py

`couverture.py` s'arrete a « cette famille peut se lever ». C'est la question suivante qui
decide d'un chantier, et c'est celle qui a manque deux fois : une famille peut etre vivante,
parfaitement nommee, et n'avoir AUCUN fichier du depot client qui la porte - ou en avoir un et
ne pas devoir etre touchee.

Chaque famille vivante recoit ici un verdict et sa raison, etabli en lisant sa DETECTION dans
le crawler, jamais son nom. Cinq verdicts :

  FICHIER     un fichier du depot la porte ET la valeur a ecrire est deja CONNUE - candidate ;
  DEVINER     un fichier la porte, mais la valeur devrait etre inventee : « REFUSER bat
              REPARER des qu'il faudrait deviner » ;
  DECISION    le proprietaire a choisi ; corriger serait decider a sa place ;
  BAISSE      la seule correction atteignable degrade le site - « une baisse n'est pas une
              correction » ;
  HORS_DEPOT  aucun fichier du depot ne la porte (serveur, TLS, DNS, API tierce, sortie de
              build).

La difference entre FICHIER et DEVINER est la seule qui compte pour ouvrir un chantier, et elle
tient a une question unique : la valeur a ecrire est-elle MESUREE quelque part ? Pour
`links_with_no_anchor_text` oui - la cible a ete crawlee et son titre est connu, comme la cible
tranchait deja le hreflang en trop. Pour `http_404` non : creer la page manquante demanderait
d'en inventer le contenu.

Le controle d'integrite est la raison d'etre du fichier : toute famille vivante non classee
fait echouer ce script ET le test qui l'appelle. Corriger une famille ou en ajouter une oblige
donc a revenir ici, au lieu de laisser la liste vieillir en silence.
"""

from __future__ import annotations

import sys
from pathlib import Path

RACINE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RACINE / "seo-agent-web"))

from ops import couverture  # noqa: E402

# (verdict, raison) - la raison dit ce que la DETECTION mesure, puis ce qu'il faudrait ecrire.
VERDICTS: dict[str, tuple[str, str]] = {
    # --- FICHIER : la valeur a ecrire est deja connue, et rien ne s'y oppose ------------------
    # VIDE le 18/09/2026. Les quatre candidates de la premiere mesure ont toutes ete instruites :
    # une est devenue un correcteur (links_with_no_anchor_text), trois ont ete tranchees contre.
    # Une colonne vide ici ne veut pas dire « tout est corrige » mais « plus rien ne se repare par
    # une edition de fichier sans decider a la place du client ».

    # --- DECISION : instruite le 18/09/2026, puis refusee ------------------------------------
    "redirect_chain": (
        "DECISION",
        "plus d'une redirection avant la page finale ; la destination est bien mesuree, mais "
        "c'est un SYMPTOME : ses trois portes d'entree sont le lien interne "
        "(page_has_links_to_redirect, corrige), l'entree de sitemap (sitemap_3xx_redirect, "
        "corrige) et l'empilement de regles de config (refuse par _REDIRECT_CONFIG_KEYS). Un "
        "correcteur ouvrirait une SECONDE PR sur les lignes que la premiere vient de changer "
        "- voir test_redirect_chain_est_un_symptome",
    ),
    "redirect_chain_too_long": (
        "DECISION",
        "meme mesure au-dela de 4 sauts : memes portes d'entree, meme decision",
    ),
    "no_hsts": (
        "DECISION",
        "en-tete Strict-Transport-Security absent ; il s'ecrit bien dans un fichier du depot sur "
        "un hebergeur statique, mais la DETECTION ne lit que sa presence : un max-age symbolique "
        "ferait taire l'anomalie sans proteger, et un max-age reel engage le navigateur du "
        "visiteur pour des mois sans retour possible. Ici c'est l'AJOUT qui ne se reprend pas "
        "- voir test_hsts_est_une_decision_du_client",
    ),
    # --- DEVINER : il faudrait inventer la valeur --------------------------------------------
    "http_404": ("DEVINER", "une URL crawlee rend 404 ; la reparer, c'est ecrire la page absente"),
    "http_4xx": ("DEVINER", "meme mesure elargie a tout le 4xx, meme page a inventer"),
    "image_broken": ("DEVINER", "une image rend une erreur ; ni le binaire ni le bon chemin ne sont mesures"),
    "javascript_broken": ("DEVINER", "un .js rend une erreur ; le fichier attendu n'existe nulle part"),
    "javascript_broken_links": ("DEVINER", "liens tires du JS qui cassent ; la cible voulue n'est pas mesuree"),
    "broken_internal_javascript_and_css_files": ("DEVINER", "un JS/CSS interne rend une erreur : recreer ou re-router au juge"),
    "page_has_broken_image": ("DEVINER", "la page porte une image cassee - meme inconnue que image_broken"),
    "page_has_broken_javascript": ("DEVINER", "la page porte un JS casse - meme inconnue que javascript_broken"),
    "page_has_links_to_broken_page": ("DEVINER", "un lien mene a une page cassee ; choisir entre reparer la cible et retirer le lien n'est pas mesure"),
    "broken_redirect": ("DEVINER", "une redirection aboutit a une erreur ; la bonne destination n'est pas connue"),
    "redirect_loop": ("DEVINER", "boucle de redirections ; rien ne dit lequel des sauts est celui de trop"),
    "orphaned_sitemap_pages": ("DEVINER", "URL du sitemap sans lien entrant - meme page source a choisir"),
    "canonical_url_has_no_incoming_internal_links": ("DEVINER", "la cible d'un canonical n'a aucun lien entrant : meme page source a choisir"),
    "redirected_page_has_no_incoming_internal_links": ("DEVINER", "meme absence de lien entrant sur une page redirigee"),
    "page_has_only_one_dofollow_incoming_internal_link": ("DEVINER", "un seul lien dofollow entrant ; d'ou viendrait le second n'est pas mesure"),
    "page_has_no_outgoing_links": ("DEVINER", "page sans aucun lien sortant ; vers quoi lier n'est pas mesure"),
    "more_than_three_parameters_in_url": ("DEVINER", "plus de trois parametres dans l'URL ; la forme voulue depend du routage"),
    "robots_invalid_format": ("DEVINER", "le PARSEUR robots a leve une exception : on sait que le fichier est illisible, pas ce qui y manque (seo_audit.py:9353)"),
    "sitemap_invalid_format": ("DEVINER", "XML de sitemap illisible ; reparer un XML casse demande d'en deviner l'intention"),
    "incorrect_pages_found_in_sitemap_xml": (
        "DEVINER",
        "LE NOM MENT : ce ne sont pas des pages mais des FICHIERS sitemap en sitemap_parse_error "
        "ou sitemap_too_large (seo_audit.py:9400) - meme inconnue que sitemap_invalid_format",
    ),
    "sitemap_file_too_large": ("DEVINER", "sitemap au-dela de la limite ; le scinder suppose de savoir qui le GENERE"),
    "similar_ai_generated_content": ("DEVINER", "pages trop ressemblantes ; les reecrire est un travail editorial, et le libelle ne pretend meme pas reconnaitre une IA"),

    # --- DECISION : le proprietaire a choisi -------------------------------------------------
    "indexable_page_blocked_from_all_ai_search_bots": ("DECISION", "tranchee le 18/09/2026, correcteur ecrit puis jete : voir test_OUVRIR_des_robots_d_IA_reste_une_decision_du_client"),
    "indexable_page_blocked_from_some_ai_search_bots": ("DECISION", "meme decision, et c'est le cas ou la tentation de corriger est la plus forte"),
    "blocked_by_robots": ("DECISION", "page interdite par le robots.txt du client : le fichier est bien dans le depot, mais rouvrir est exactement la decision refusee pour les robots d'IA"),
    "noindex_page": ("DECISION", "meta robots / X-Robots-Tag noindex ; desindexer une page est un choix legitime"),
    "nofollow_page": ("DECISION", "meme chose pour nofollow"),
    "noindex_and_nofollow_page": ("DECISION", "les deux a la fois - toujours le choix du proprietaire"),
    "noindex_follow_page": ("DECISION", "noindex sans nofollow : forme deliberee et courante (pagination, filtres)"),
    "noindex_in_html_and_http_header": ("DECISION", "la directive est posee deux fois ; retirer une moitie ne change RIEN au comportement, et l'en-tete n'est pas dans le depot"),
    "nofollow_in_html_and_http_header": ("DECISION", "meme redondance sans effet, meme en-tete hors depot"),
    "page_has_nofollow_outgoing_internal_links": ("DECISION", "rel=nofollow sur des liens INTERNES ; le retirer rouvre un passage que le proprietaire a ferme (panier, connexion) - meme asymetrie que les robots d'IA"),

    # --- BAISSE : la seule correction atteignable degrade ------------------------------------
    "document_uses_plugins": ("BAISSE", "<embed>/<object>/<applet> comptes dans le HTML ; les retirer supprime le contenu qu'ils portent"),
    "meta_refresh_redirect": ("BAISSE", "<meta http-equiv=refresh> compte dans le HTML ; le retirer CASSE la redirection, et poser la 301 qui la remplace n'est pas a portee d'un fichier HTML"),

    # --- HORS_DEPOT : aucun fichier du depot ne la porte -------------------------------------
    "bing_blocked_urls": ("HORS_DEPOT", "remontee de l'API Bing Webmaster"),
    "bing_crawl_issues": ("HORS_DEPOT", "remontee de l'API Bing Webmaster"),
    "bing_pages_push_page_1": ("HORS_DEPOT", "opportunite de classement lue chez Bing, pas un defaut de fichier"),
    "bing_pages_quick_wins": ("HORS_DEPOT", "pages proches du top chez Bing : une opportunite lue par API, pas un defaut de fichier"),
    "bing_sitemaps": ("HORS_DEPOT", "etat des sitemaps tel que Bing les voit"),
    "bing_urlinfo_non_200": ("HORS_DEPOT", "statut renvoye par l'API Bing"),
    "gsc_indexing_errors": ("HORS_DEPOT", "remontee de la Search Console"),
    "gsc_indexing_notices": ("HORS_DEPOT", "remontee de la Search Console"),
    "gsc_indexing_warnings": ("HORS_DEPOT", "remontee de la Search Console"),
    "gsc_pages_push_page_1": ("HORS_DEPOT", "opportunite de classement lue chez Google"),
    "gsc_pages_quick_wins": ("HORS_DEPOT", "meme origine Search Console"),
    "certificate_expiration": ("HORS_DEPOT", "certificat TLS de l'hote"),
    "certificate_name_mismatch": ("HORS_DEPOT", "certificat TLS de l'hote"),
    "insecure_cipher": ("HORS_DEPOT", "suite cryptographique negociee par le serveur"),
    "old_tls_version": ("HORS_DEPOT", "version de TLS servie par l'hote"),
    "dns_resolution_issue": ("HORS_DEPOT", "resolution DNS du domaine"),
    "http_500": ("HORS_DEPOT", "la page repond 500 : c est l applicatif servi qui echoue, aucun markup a reecrire"),
    "http_5xx": ("HORS_DEPOT", "meme panne cote serveur, elargie a tout le 5xx"),
    "sitemap_5xx_page": ("HORS_DEPOT", "page du sitemap en panne serveur"),
    "sitemap_page_timed_out": ("HORS_DEPOT", "page du sitemap qui n'a pas repondu a temps"),
    "timed_out_links": ("HORS_DEPOT", "liens dont la cible n'a pas repondu a temps"),
    "slow_server_response_for_ai_crawlers": ("HORS_DEPOT", "ttfb mesure au-dela du seuil : c'est l'hebergement, et le test rappelle que meme la mesure a failli accuser a tort"),
    "http_to_https_redirect": ("HORS_DEPOT", "redirection de scheme posee par le serveur ou le CDN"),
    "https_to_http_redirect": ("HORS_DEPOT", "meme redirection de scheme, en sens inverse"),
    "http_page_has_internal_links_to_https": ("HORS_DEPOT", "page servie en http qui lie en https : c'est la page http qui ne devrait plus etre servie"),
    "redirect_302": ("HORS_DEPOT", "302 au lieu de 301 ; le code vit dans la couche qui redirige, et rien ne mesure si le provisoire est DELIBERE"),
    "css_not_minified": ("HORS_DEPOT", "sortie de build : minifier la source du depot detruirait la source"),
    "javascript_not_minified": ("HORS_DEPOT", "sortie de build, meme raison"),
    "image_file_size_too_large": ("HORS_DEPOT", "poids d'un binaire servi ; recompresser n'est pas une edition de markup"),
    "pages_to_submit_to_indexnow": ("HORS_DEPOT", "appel d'API a emettre, aucun fichier en cause"),
}

ORDRE = ("FICHIER", "DEVINER", "DECISION", "BAISSE", "HORS_DEPOT")


def verdicts_manquants() -> tuple[list[str], list[str]]:
    """(vivantes non classees, verdicts devenus sans objet) - les deux doivent rester vides."""
    vivantes = set(couverture.mesurer()["groupes"]["VIVANTE"])
    return sorted(vivantes - set(VERDICTS)), sorted(set(VERDICTS) - vivantes)


def main() -> None:
    vivantes = couverture.mesurer()["groupes"]["VIVANTE"]
    orphelines, perimes = verdicts_manquants()

    for nom in ORDRE:
        lot = [k for k in vivantes if VERDICTS.get(k, ("", ""))[0] == nom]
        print("\n=== %s : %d ===" % (nom, len(lot)))
        for k in lot:
            print("   %s\n       %s" % (k, VERDICTS[k][1]))

    if orphelines or perimes:
        print("\n!!! la table a vieilli")
        for k in orphelines:
            print("   VIVANTE non classee : %s" % k)
        for k in perimes:
            print("   verdict sans objet (corrigee ou disparue) : %s" % k)
        raise SystemExit(1)
    print("\n%d vivantes, toutes classees." % len(vivantes))


if __name__ == "__main__":
    main()
