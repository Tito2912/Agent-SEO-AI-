# -*- coding: utf-8 -*-
"""L'anomalie a-t-elle DISPARU ? La seule question que le banc ne posait pas.

Le verdict `ok` de `run.py` signifie « un fichier a ete patche », pas « la famille ne se
declenche plus ». Mesure du 11/09/2026 : `missing_alt_text` etait comptee reussie sur les NEUF
stacks alors que le modele modifiait six pages sans image pendant que l'image fautive restait
intacte. Rien ne l'a signale.

Ce script recrawle la PREVIEW de chaque PR de correction et confronte, famille par famille, le
compte d'avant a celui d'apres.

    python verifier_apres_correction.py <prefixe_des_passages> <dossier_des_references>

Piege connu et assume : Netlify sert toute preview en `noindex`, donc les familles a variante
d'indexabilite se deplacent d'une variante a l'autre. On les REGROUPE avant de comparer, ce qui
rend la comparaison honnete sans avoir a fermer les yeux sur elles.
"""

import json
import os
import subprocess
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    "..", "..", "..", "..", ".."))
AUDIT = None
STACKS = ["static-html", "astro", "hugo", "jekyll", "gatsby", "nuxt",
          "sveltekit", "next-pages", "next-app"]


# Familles que le CRAWLER replie l'une dans l'autre selon l'indexabilite. Les comparer separement
# produit une fausse alerte des que l'indexabilite change entre les deux crawls — et elle change
# toujours, puisque Netlify sert toute preview en `noindex`. Mesure du 11/09/2026 : sur hugo,
# l'index du parcours (sans description) passait de `meta_description_too_short_indexable` a
# `missing_meta_description`, et le controle criait a la regression sur une page qu'aucune
# correction n'avait touchee.
_REPLIS = {
    "missing_meta_description": "description",
    "meta_description_too_short": "description",
    "meta_description_too_long": "description",
}


def base(key: str) -> str:
    nu = key.removesuffix("_not_indexable").removesuffix("_indexable")
    return _REPLIS.get(nu, nu)


# Familles dont la detection exige de RESOUDRE une cible : le canonical ou le hreflang d'une page
# doit designer une page que le crawl a vue. Sur une preview, ces valeurs restent en absolu vers
# l'hote de PRODUCTION — c'est voulu, les rendre relatives desaccorderait `og:url`, qui doit rester
# absolu, et `open_graph_url_not_matching_canonical` se declencherait alors sur TOUTES les pages.
#
# Mesure du 17/09/2026 sur les neuf stacks : 53 occurrences en production, ZERO sur les previews.
# Pas une seule survivante. Le controle lisait ce zero comme une correction.
#
# C'etait le defaut du 15/09 sous une forme plus sournoise. Alors, la preview repondait 404 partout
# et le garde-fou de comparabilite pouvait l'attraper ; ICI LA PREVIEW EST SAINE — elle sert toutes
# ses pages, elle passe la comparabilite — et elle ment quand meme, sur ces familles-la seulement.
#
# RESOLU par `--canonical-host-alias`, que la commande de crawl ci-dessous passe systematiquement :
# chaque page crawlee est aussi indexee sous le nom que le site lui donne. Ces familles sont donc
# JUGEES comme les autres, et cette liste n'est plus une exclusion mais une ALARME : l'une d'elles
# qui tombe pile a zero merite qu'on verifie d'abord que l'alias a pris.
_FAMILLES_A_CIBLE_RESOLUE = {
    "canonical_points_to_4xx",
    "canonical_points_to_5xx",
    "canonical_points_to_redirect",
    "non_canonical_page_specified_as_canonical_one",
    "hreflang_to_non_canonical",
    "hreflang_to_redirect_or_broken_page",
    "canonical_url_has_no_incoming_internal_links",
}


def _jeton() -> str:
    """Le jeton du banc : d'abord l'environnement, le fichier seulement en second.

    Il etait lu UNIQUEMENT dans `~/.noyaru-token`. Le 15/09/2026 ce fichier a ete supprime et le
    jeton revoque — a juste titre, il dormait en clair sur un disque et ouvrait les neuf depots —
    et ce controle s'est mis a tomber sur une pile d'appels illisible au lieu de dire ce qui
    manquait. L'environnement d'abord, c'est aussi ce que `run.py` et `stack_loop.py` font deja
    (`FIXTURE_TOKEN`, `GITHUB_TOKEN`) : un jeton qui ne touche pas le disque ne s'y oublie pas.
    """
    for nom in ("FIXTURE_TOKEN", "GITHUB_TOKEN"):
        valeur = (os.environ.get(nom) or "").strip()
        if valeur:
            return valeur
    chemin = os.path.expanduser("~/.noyaru-token")
    if os.path.exists(chemin):
        with open(chemin, encoding="utf-8") as fh:
            return fh.read().strip()
    raise SystemExit(
        "Aucun jeton GitHub : renseigne FIXTURE_TOKEN (ou GITHUB_TOKEN) dans l'environnement.\n"
        "Le fichier ~/.noyaru-token n'existe plus — il a ete supprime le 15/09/2026 et son jeton "
        "revoque.")


def _pr_de_branche(stack: str, branche: str) -> str:
    import urllib.request
    tok = _jeton()
    url = ("https://api.github.com/repos/pployeraffiliation-a11y/noyaru-stack-%s/pulls"
           "?head=pployeraffiliation-a11y:%s&state=all" % (stack, branche))
    req = urllib.request.Request(url)
    req.add_header("Authorization", "Bearer " + tok)
    req.add_header("Accept", "application/vnd.github+json")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            rows = json.load(resp)
        return str(rows[0]["number"]) if rows else ""
    except Exception:
        return ""


def comptes(path: str) -> dict[str, int]:
    """Le compte par famille, SANS compter deux fois la meme page.

    Le crawler emet, pour une meme page, la famille generique ET sa variante d'indexabilite —
    `meta_description_too_short` vaut 1 et `meta_description_too_short_indexable` vaut 1 pour
    l'unique page concernee. Les additionner doublait le compte.

    Mesure du 16/09/2026, nuxt : ce controle annoncait une regression de 5 a 9 sur la famille
    des descriptions. En pages distinctes, elle allait de 3 a 5. La regression etait reelle —
    l'instrument l'affichait deux fois trop grande, et c'est le genre d'ecart qui fait chercher
    un defaut la ou il n'y en a pas.

    Donc DEUX niveaux de repli, qui ne se traitent pas pareil :
      - les variantes d'indexabilite d'une meme famille parlent des MEMES pages -> on prend le
        MAXIMUM, jamais la somme ;
      - les familles differentes regroupees sous un meme nom (`missing`, `too_short`, `too_long`
        -> « description ») parlent de pages DISTINCTES -> on somme leurs maximums.
    """
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    # famille repliee -> famille reelle (indexabilite otee) -> plus grand compte vu
    par_famille: dict[str, dict[str, int]] = {}
    for key, value in (data.get("issues") or {}).items():
        n = value.get("count") if isinstance(value, dict) else len(value or [])
        if not n:
            continue
        reelle = key.removesuffix("_not_indexable").removesuffix("_indexable")
        seau = par_famille.setdefault(base(key), {})
        seau[reelle] = max(seau.get(reelle, 0), int(n))
    return {nom: sum(v.values()) for nom, v in par_famille.items()}


def _alias_sollicite(path: str) -> bool:
    """Le crawl a-t-il eu l'occasion d'utiliser l'alias d'hote ?

    Le crawler ecrit `canonical_host_alias.pages_au_canonical_alias` dans son rapport des qu'un
    alias lui est passe. Un nombre non nul prouve que des pages nomment bien cet hote dans leur
    canonical — donc que l'alias a ete sollicite. Zero veut dire qu'il n'a rien eu a faire, et
    alors un « 0 » sur une famille a cible resolue ne prouve rien du tout.

    C'est le controle qui manquait : trois fois en deux jours l'alias a ete pose a moitie, et
    chaque fois le symptome etait un zero d'apparence parfaitement normale.
    """
    with open(path, encoding="utf-8") as fh:
        bloc = (json.load(fh).get("canonical_host_alias") or {})
    return int(bloc.get("pages_au_canonical_alias") or 0) > 0


def _pages_crawlees(path: str) -> int:
    with open(path, encoding="utf-8") as fh:
        return len(json.load(fh).get("pages") or [])


# En dessous de cette part des pages de la reference, la preview n'est pas comparable : on ne
# mesure plus le meme site. Deux tiers laissent passer la variation normale d'un crawl (une page
# lente, une limite atteinte) et arretent net un deploiement qui n'a rien servi.
_PART_MINIMALE = 0.66


def _preview_comparable(stack: str, ref_json: str, apres_json: str) -> bool:
    """La preview sert-elle vraiment le site, ou seulement des 404 ?

    Mesure du 15/09/2026, et c'est le defaut le plus grave trouve ce jour-la — dans l'INSTRUMENT,
    pas dans le correcteur. Le deploiement de next-app avait ECHOUE ; sa preview repondait 404 sur
    tout, `robots.txt` compris. Le crawl n'a ramene que 4 pages, la famille visee ne pouvait donc
    pas se declencher, son compteur valait zero — et ce controle a annonce « 1 -> 0 », c'est-a-dire
    REUSSITE, sur un site qui n'existait pas.

    L'absence d'une anomalie n'est pas sa correction. Un compteur a zero ne veut rien dire tant
    qu'on n'a pas etabli qu'il y avait quelque chose a compter.
    """
    avant, apres = _pages_crawlees(ref_json), _pages_crawlees(apres_json)
    if apres >= max(2, int(avant * _PART_MINIMALE)):
        return True
    print("  %-12s NON COMPARABLE : %d pages servies contre %d a la reference — deploiement "
          "echoue ou preview incomplete, aucun verdict possible" % (stack, apres, avant))
    return False


def main() -> int:
    prefixe, refs, audit = sys.argv[1], sys.argv[2], sys.argv[3]
    lignes = []
    for stack in STACKS:
        run_json = os.path.join(prefixe + stack, "gauntlet_run.json")
        ref_json = os.path.join(refs, stack, "report.json")
        if not (os.path.exists(run_json) and os.path.exists(ref_json)):
            print("  %-12s pas de passage" % stack)
            continue
        with open(run_json, encoding="utf-8") as fh:
            run = json.load(fh)
        # Le numero de PR n'est pas enregistre par le passage : on le retrouve par la BRANCHE,
        # ce qui evite d'ajouter un couplage entre le correcteur et ce controle.
        branche = run.get("branch") or ""
        pr = _pr_de_branche(stack, branche)
        if not pr:
            print("  %-12s aucune PR pour %s" % (stack, branche))
            continue
        url = "https://deploy-preview-%s--noyaru-stack-%s.netlify.app/" % (pr, stack)
        out = os.path.join(prefixe + stack, "apres")
        os.makedirs(out, exist_ok=True)
        # L'alias est OBLIGATOIRE ici, pas un confort : les canonical et hreflang du parcours
        # nomment l'hote de PRODUCTION — c'est voulu, les rendre relatifs desaccorderait og:url,
        # qui doit rester absolu. Sans cet alias, la cible n'est jamais parmi les pages crawlees
        # et cinq familles tombent a zero sans rien prouver.
        cmd = [sys.executable, audit, url, "--max-pages", "90", "--workers", "3",
               "--check-resources", "--canonical-host-alias",
               "noyaru-stack-%s.netlify.app" % stack, "--output-dir", out]
        with open(os.path.join(out, "crawl.log"), "w", encoding="utf-8") as fh:
            subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT, check=False)
        apres_json = os.path.join(out, "report.json")
        if not os.path.exists(apres_json):
            print("  %-12s preview non crawlable" % stack)
            continue
        if not _preview_comparable(stack, ref_json, apres_json):
            continue
        avant, apres = comptes(ref_json), comptes(apres_json)
        # Les familles muettes sortent des DEUX colonnes : les compter « en baisse » etait un faux
        # succes, les compter « inchangees » serait une fausse alerte. On ne sait pas, et c'est ce
        # qu'il faut dire.
        # Elles ne sont plus ecartees : l'alias d'hote les rend mesurables, donc on les juge.
        # La liste reste, et sert d'ALARME : si l'une d'elles tombe a zero pile-poil, c'est
        # peut-etre l'alias qui n'a pas pris, et non la correction qui a marche.
        # Le CONTROLE POSITIF : le crawl dit lui-meme si l'alias a ete sollicite. Sans lui, un
        # zero « corrige » et un zero « aveugle » sont indistinguables — et l'alias a ete pose a
        # moitie trois fois en deux jours, chaque fois avec ce meme zero d'apparence normale.
        _alias_a_servi = _alias_sollicite(apres_json)
        muettes = sorted(k for k in avant if k in _FAMILLES_A_CIBLE_RESOLUE
                         and apres.get(k, 0) == 0 and avant[k] > 0) if not _alias_a_servi else []
        if not _alias_a_servi:
            print("  %-12s ALIAS D'HOTE SANS EFFET : aucune page ne declare un canonical sur "
                  "l'hote alias — les familles a cible resolue ci-dessous ne prouvent rien"
                  % stack)
        mesurables = dict(avant)
        corrigees = sorted(k for k in mesurables if apres.get(k, 0) < mesurables[k])
        tenaces = sorted(k for k in mesurables if apres.get(k, 0) >= mesurables[k])
        lignes.append((stack, len(mesurables), len(corrigees), tenaces, muettes))
        print("  %-12s familles avant=%-3d en baisse=%-3d inchangees=%-3d a confirmer=%d"
              % (stack, len(mesurables), len(corrigees), len(tenaces), len(muettes)))
    print()
    for stack, _n, _c, _t, muettes in lignes:
        for key in muettes:
            print("    %-12s A CONFIRMER %s — famille a cible resolue tombee pile a zero : "
                  "verifier que l'alias d'hote a bien pris avant d'y croire" % (stack, key))
    print()
    for stack, _n, _c, tenaces, _m in lignes:
        for key in tenaces:
            print("    %-12s INCHANGEE %s" % (stack, key))
    return 0


if __name__ == "__main__":
    # Sans cette garde, `main()` s'executait a l'IMPORT et lisait `sys.argv` : le module etait
    # inutilisable depuis un test, et `comptes()` — qui rend les verdicts du banc — n'en avait
    # donc aucun. C'est l'instrument de mesure qui etait le moins mesure de tout le projet.
    raise SystemExit(main())
