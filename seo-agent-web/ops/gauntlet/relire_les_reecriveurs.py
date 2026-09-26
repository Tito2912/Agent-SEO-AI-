# -*- coding: utf-8 -*-
"""Un garde-fou ne doit toucher QUE ce que le patch a ecrit. Deux passages pour le verifier.

LA PROMESSE. Chaque garde-fou de la chaine dit la meme chose dans sa docstring : il ne corrige
que ce que CE patch a produit. C'est la seule chose qui l'autorise a reecrire le fichier d'un
client. Elle se verifie sans rien inventer, en deux passages de difficulte croissante.

PASSAGE 1 — LE PATCH N'A RIEN CHANGE. On donne `new == old`. Tout garde-fou qui modifie agit
alors sur le contenu du client : au mieux une correction facturee pour rien (le drapeau
`no_change` a ete retire de la boucle, donc une edition de garde-fou suffit a commiter), au
pire une corruption livree en pull request.

PASSAGE 2 — LE PATCH A CHANGE UNE SEULE LIGNE. C'est le passage qui mord. On fabrique un patch
realiste a partir du fichier lui-meme (un titre trop long, une description trop courte), puis
on verifie qu'AUCUNE LIGNE COMMUNE a l'ancien et au neuf n'a ete modifiee ni perdue. Les
insertions restent permises : plusieurs garde-fous rendent legitimement une balise que le patch
avait prise.

C'est exactement la forme du defaut du 14/09/2026 : `old_fm` etait un dictionnaire par nom de
champ, donc seule la DERNIERE ligne `title:` de l'ancien fichier etait retenue ; toutes les
precedentes se comparaient inegales et etaient coupees alors que le patch ne les avait jamais
ecrites.

POURQUOI LES FICHIERS REELS. Un cas invente mesure ce que j'imagine ; les 413 fichiers des neuf
depots mesurent ce qui existe, se construit et se sert. Aucun appel de modele : c'est gratuit.

CE QUE LA SONDE NE PROUVE PAS. Qu'un garde-fou ferait son travail quand il le faut — ca se
mesure par mutation, ailleurs. Ni qu'aucun fichier au monde ne le declenche : elle prouve que
les depots d'AUJOURD'HUI n'ont pas la forme qui declenche. Le defaut du 26/09/2026 etait latent
sur ces memes neuf depots, et bien reel.

SA FRONTIERE, MESUREE ET NON SUPPOSEE (26/09/2026). Une sonde verte ne vaut rien tant qu'on
n'a pas montre qu'elle sait rougir. On a donc sabote des garde-fous — en leur faisant ignorer
`old_content`, la forme exacte du defaut du 14/09/2026 — et regarde lesquels elle voit :

  VUS       `_enforce_length_ceilings` et `_align_og_url_with_added_canonical`. Ce sont les
            SEULS dont `old_content` change quelque chose sur un fichier valide : sans lui ils
            reecrivent le contenu du client sur 9 et 27 des 413 fichiers reels.
  PAS VUS   `_escape_quotes_in_written_values`, `_drop_duplicate_object_keys`,
            `_keep_head_meta_tags`, `_keep_social_object_keys`, `_repair_social_image_key`,
            `_forbid_https_downgrade`. Mesure faite sur les 413 fichiers : ils sont INERTES
            sur un fichier valide. Ils n'agissent que sur du contenu deja malforme — un
            delimiteur non echappe, une cle en double, une balise perdue, un https degrade —
            et un depot qui se construit n'en contient pas, par definition.

Ce n'est pas un trou, c'est une frontiere : ces six-la se testent sur des entrees MALFORMEES,
en tests unitaires (`test_quote_escaping_guard.py`, `test_duplicate_key_guard.py`,
`test_keep_head_meta_tags.py`, `test_social_image_key.py`), et ils y sont couverts. Les
chercher ici donnerait un vert qui ne veut rien dire.
"""
from __future__ import annotations

import base64
import collections
import os
import re
import sys

for _flux in (sys.stdout, sys.stderr):
    try:
        _flux.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

sys.path.insert(0, "seo-agent-web")
for line in open("seo-agent-web.env", encoding="utf-8", errors="replace"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        if k.strip() != "DATABASE_URL":
            os.environ.setdefault(k.strip(), v.strip())
os.environ["DATABASE_URL"] = "sqlite:///:memory:"
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "x" * 20)

from backend import app as m  # noqa: E402

TOKEN = os.environ["FIXTURE_TOKEN"]
OWNER, BRANCH = "pployeraffiliation-a11y", "main"
STACKS = ["static-html", "next-app", "next-pages", "astro", "nuxt", "gatsby",
          "sveltekit", "hugo", "jekyll"]
INTERESSANTES = (".ts", ".tsx", ".js", ".jsx", ".mjs", ".vue", ".svelte", ".astro",
                 ".html", ".htm", ".md", ".markdown", ".mdx")
IGNORES = ("node_modules/", "dist/", "build/", ".next/", ".nuxt/", ".output/", "public/build/")


def _langue_declaree(contenu: str) -> str:
    """La langue que le fichier declare lui-meme, ou 'fr'.

    `_keep_site_lang` compare a la langue du SITE. Lui en donner une autre mesurerait son
    travail normal, pas un faux positif : on lui donne celle qu'il voit deja, et toute
    reecriture devient une anomalie.
    """
    trouve = re.search(r'\blang\s*[=:]\s*["\']([A-Za-z-]+)', contenu)
    return trouve.group(1) if trouve else "fr"


REECRIVEURS = {
    "_enforce_length_ceilings": lambda n, o, p: m._enforce_length_ceilings(n, o),
    "_forbid_https_downgrade": lambda n, o, p: m._forbid_https_downgrade(n, o),
    "_escape_quotes_in_written_values": lambda n, o, p: m._escape_quotes_in_written_values(n, o),
    "_keep_length_above_floor": lambda n, o, p: m._keep_length_above_floor(n, o),
    "_drop_duplicate_object_keys": lambda n, o, p: m._drop_duplicate_object_keys(n, o),
    "_add_missing_object_commas": lambda n, o, p: m._add_missing_object_commas(n),
    "_keep_head_meta_tags": lambda n, o, p: m._keep_head_meta_tags(n, o),
    "_keep_social_object_keys": lambda n, o, p: m._keep_social_object_keys(n, o),
    "_repair_social_image_key": lambda n, o, p: m._repair_social_image_key(n, o),
    "_space_glued_front_matter": lambda n, o, p: m._space_glued_front_matter(n, o, set()),
    "_keep_site_lang": lambda n, o, p: m._keep_site_lang(n, o, _langue_declaree(o)),
    "_keep_canonical_master": lambda n, o, p: m._keep_canonical_master(n, ""),
    "_align_og_url_with_added_canonical":
        lambda n, o, p: m._align_og_url_with_added_canonical(n, o),
    "_complete_open_graph": lambda n, o, p: m._complete_open_graph(n, o, ""),
    "_completer_open_graph_objet": lambda n, o, p: m._completer_open_graph_objet(n, o, dict),
    "_requote_toml_apostrophes": lambda n, o, p: m._requote_toml_apostrophes(n, p),
    "_antislashs_de_trop": lambda n, o, p: m._antislashs_de_trop(n, p),
}

# Un titre de 92 caracteres portant une apostrophe francaise : au-dessus du plafond de 70, et
# de la forme qui a deja casse des litteraux. Une description de 21 caracteres : sous le
# plancher de 100. Les deux valeurs viennent des seuils du produit, pas de mon gout.
TITRE_LONG = ("Comment verifier l'ensemble des balises canoniques d'un site "
              "sans rien casser en chemin")
DESCRIPTION_COURTE = "Un resume trop court"

# Les ecritures d'un titre que ce projet rencontre sur les neuf idiomes.
FORMES_TITRE = (
    re.compile(r"(<title[^>]*>)([^<]{6,})(</title>)"),
    re.compile(r"(^\s*title\s*:\s*[\"'])([^\"'\n]{6,})([\"'])", re.M),
    re.compile(r"(^\s*title\s*=\s*[\"'])([^\"'\n]{6,})([\"'])", re.M),
)
FORMES_DESCRIPTION = (
    re.compile(r"(<meta\s+name=[\"']description[\"']\s+content=[\"'])([^\"'\n]{20,})([\"'])"),
    re.compile(r"(^\s*description\s*:\s*[\"'])([^\"'\n]{20,})([\"'])", re.M),
)


def patch_synthetique(contenu: str) -> list[tuple[str, str]]:
    """Des patchs realistes derives du fichier : on remplace UNE valeur, jamais deux.

    Remplacer la PREMIERE occurrence et pas toutes est deliberé : un fichier qui porte deux
    `title:` est precisement celui ou un garde-fou peut couper celui que le patch n'a pas
    ecrit.
    """
    patchs: list[tuple[str, str]] = []
    for forme in FORMES_TITRE:
        trouve = forme.search(contenu)
        if trouve:
            patchs.append(("titre trop long",
                           contenu[:trouve.start(2)] + TITRE_LONG + contenu[trouve.end(2):]))
            break
    for forme in FORMES_DESCRIPTION:
        trouve = forme.search(contenu)
        if trouve:
            patchs.append(("description trop courte",
                           contenu[:trouve.start(2)] + DESCRIPTION_COURTE
                           + contenu[trouve.end(2):]))
            break
    return patchs


def lignes_communes_perdues(ancien: str, neuf: str, sortie: str) -> list[str]:
    """Les lignes presentes dans l'ancien ET dans le neuf que le garde-fou a fait disparaitre.

    Une ligne identique des deux cotes n'a pas ete ecrite par ce patch : le garde-fou n'a
    aucun titre a y toucher. Les insertions restent permises — plusieurs rendent legitimement
    une balise que le patch avait prise.
    """
    compte_ancien = collections.Counter(ancien.splitlines())
    compte_neuf = collections.Counter(neuf.splitlines())
    compte_sortie = collections.Counter(sortie.splitlines())
    perdues = []
    for ligne, n in (compte_ancien & compte_neuf).items():
        if not ligne.strip():
            continue
        if compte_sortie[ligne] < n:
            perdues.append(ligne.strip()[:110])
    return perdues


def stacks_demandees() -> list[str]:
    """`--stack=astro` n'en relit qu'une. Utile pour VALIDER la sonde elle-meme.

    Une sonde qui ne peut pas echouer ne prouve rien : on casse volontairement un garde-fou et
    on verifie qu'elle le voit. Ce controle-la n'a pas besoin des neuf depots.
    """
    for arg in sys.argv[1:]:
        if arg.startswith("--stack="):
            voulues = [s.strip() for s in arg.split("=", 1)[1].split(",") if s.strip()]
            inconnues = [s for s in voulues if s not in STACKS]
            if inconnues:
                raise SystemExit("pile inconnue : %s" % ", ".join(inconnues))
            return voulues
    return list(STACKS)


def arbre(repo: str) -> list[str]:
    d = m._github_api_get(m._github_api_path("repos", OWNER, repo, "git", "trees", BRANCH),
                          token=TOKEN, params={"recursive": "1"}, timeout_s=30)
    return [n["path"] for n in (d.get("tree") or []) if n.get("type") == "blob"]


def lire(repo: str, chemin: str) -> str:
    fd = m._github_api_get(m._github_content_api_path(OWNER, repo, chemin),
                           token=TOKEN, params={"ref": BRANCH}, timeout_s=30)
    return base64.b64decode(str(fd.get("content") or "").replace("\n", "")).decode(
        "utf-8", errors="replace")


def main() -> None:
    fichiers = patches = 0
    anomalies: dict[str, list[str]] = {}
    exemples: dict[str, list[str]] = {}

    def noter(cle: str, ou: str, detail: list[str]) -> None:
        anomalies.setdefault(cle, []).append(ou)
        if cle not in exemples and detail:
            exemples[cle] = detail[:3]

    for stack in stacks_demandees():
        repo = "noyaru-stack-" + stack
        chemins = [c for c in arbre(repo)
                   if c.lower().endswith(INTERESSANTES)
                   and not any(c.startswith(g) or ("/" + g) in c for g in IGNORES)]
        for chemin in chemins:
            try:
                contenu = lire(repo, chemin)
            except Exception as exc:
                print("  %-52s LECTURE : %s" % (chemin, exc))
                continue
            fichiers += 1

            for nom, appel in REECRIVEURS.items():
                # Passage 1 : le patch n'a rien change.
                try:
                    sortie, _ = appel(contenu, contenu, chemin)
                except Exception as exc:
                    noter(nom + " [EXCEPTION sans patch]", "%s %s" % (stack, chemin), [str(exc)])
                    continue
                if sortie != contenu:
                    ecart = [a.strip()[:110] for a, b in
                             zip(contenu.splitlines(), sortie.splitlines()) if a != b]
                    noter(nom + " [touche un fichier non patche]",
                          "%s %s" % (stack, chemin), ecart)

            # Passage 2 : le patch a change une seule valeur.
            for quoi, neuf in patch_synthetique(contenu):
                patches += 1
                for nom, appel in REECRIVEURS.items():
                    try:
                        sortie, _ = appel(neuf, contenu, chemin)
                    except Exception as exc:
                        noter(nom + " [EXCEPTION avec patch]",
                              "%s %s (%s)" % (stack, chemin, quoi), [str(exc)])
                        continue
                    perdues = lignes_communes_perdues(contenu, neuf, sortie)
                    if perdues:
                        noter(nom + " [touche une ligne que le patch n'a pas ecrite]",
                              "%s %s (%s)" % (stack, chemin, quoi), perdues)
        print("%-12s relu" % stack)

    print("\n%d fichiers reels, %d patchs synthetiques, %d reecriveurs"
          % (fichiers, patches, len(REECRIVEURS)))
    if not anomalies:
        print("aucun ne sort de ce que le patch a ecrit")
        sys.exit(0)
    for cle, ou in sorted(anomalies.items(), key=lambda kv: -len(kv[1])):
        print("\n  %-64s %d cas" % (cle, len(ou)))
        for ligne in ou[:5]:
            print("      %s" % ligne)
        for ligne in exemples.get(cle, []):
            print("      ligne : %s" % ligne)
    sys.exit(1)


if __name__ == "__main__":
    main()
