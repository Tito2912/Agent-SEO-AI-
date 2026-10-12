"""Les champs de front matter qu'une page Jekyll REND vraiment.

Banc complet du 12/10/2026, jekyll, `missing_title` : le modele a ajoute `title:` au front matter
de la page — l'idiome Jekyll — et la page servie n'avait toujours aucun titre. Son gabarit
n'imprime que `page.raw_head` : le champ etait ecrit, et sans effet. Une pull request qui ne
change rien a la page servie, et que rien ne signalait.

Un vrai theme fait de meme des qu'il ecrit `<title>{{ site.title }}</title>` en dur. La seule
reponse qui ne devine pas : LIRE la chaine des gabarits — le layout de la page, ses propres
layouts parents, ses `include` — et relever les `page.<champ>` qu'elle cite. `{% seo %}`
(jekyll-seo-tag) rend le titre et la description.

Quand la chaine n'est pas entierement dans le depot — un theme installe en gem, un `include`
calcule — la reponse est None : on ne sait pas, et rien ne se refuse sur un doute.
"""

import re

import yaml

_FRONT_MATTER_RE = re.compile(r"\A---[ \t]*\r?\n(.*?)\r?\n---[ \t]*(?:\r?\n|\Z)", re.S)
_PAGE_CHAMP_RE = re.compile(r"\bpage\.([A-Za-z_][\w-]*)")
_INCLUDE_RE = re.compile(r"\{%-?\s*include\s+([^\s%]+)")
_SEO_RE = re.compile(r"\{%-?\s*seo\b")
_MAX_FICHIERS = 12


def front_matter(raw):
    """Le front matter d'une page sous forme de dict, ou None s'il n'y en a pas de lisible."""
    match = _FRONT_MATTER_RE.match(raw or "")
    if not match:
        return None
    try:
        data = yaml.safe_load(match.group(1))
    except yaml.YAMLError:
        return None
    return data if isinstance(data, dict) else None


def _unique(all_paths, suffix):
    found = [p for p in all_paths if p == suffix or p.endswith("/" + suffix)]
    return found[0] if len(found) == 1 else None


def champs_rendus(raw, all_paths, read):
    """Les champs `page.*` que la chaine de gabarits de cette page imprime, ou None si inconnu."""
    data = front_matter(raw)
    layout = data.get("layout") if data else None
    if not isinstance(layout, str) or not layout.strip():
        return None
    champs, vus, a_lire = set(), set(), [_unique(all_paths, "_layouts/%s.html" % layout.strip())]
    while a_lire:
        chemin = a_lire.pop()
        if chemin is None or len(vus) >= _MAX_FICHIERS:
            return None
        if chemin in vus:
            continue
        vus.add(chemin)
        contenu = read(chemin)
        if not isinstance(contenu, str):
            return None
        champs.update(_PAGE_CHAMP_RE.findall(contenu))
        if _SEO_RE.search(contenu):
            champs.update({"title", "description"})
        # Un include calcule (`{% include {{ x }} %}`) ne nomme aucun fichier du depot : il
        # tombe dans le cas de l'include introuvable, et la reponse est None.
        for cible in _INCLUDE_RE.findall(contenu):
            a_lire.append(_unique(all_paths, "_includes/" + cible))
        parent = (front_matter(contenu) or {}).get("layout")
        if isinstance(parent, str) and parent.strip():
            a_lire.append(_unique(all_paths, "_layouts/%s.html" % parent.strip()))
    return champs


def champs_sans_effet(new_content, old_content, rendus):
    """Les cles que le patch AJOUTE au front matter et que les gabarits n'impriment pas."""
    if rendus is None:
        return []
    avant, apres = front_matter(old_content), front_matter(new_content)
    if avant is None or apres is None:
        return []
    return sorted(cle for cle in apres if cle not in avant and cle not in rendus)


def consigne(rendus):
    """Ce que le modele doit savoir avant d'ecrire dans le front matter d'une page."""
    if rendus is None:
        return ""
    liste = ", ".join("page." + c for c in sorted(rendus)) or "aucun"
    return ("Les gabarits de cette page Jekyll n'impriment que ces champs de front matter : "
            + liste + ". Tout AUTRE champ ajoute (title, description...) serait SANS EFFET sur la "
            "page servie : ecris alors la balise la ou la page ecrit deja son <head>.")
