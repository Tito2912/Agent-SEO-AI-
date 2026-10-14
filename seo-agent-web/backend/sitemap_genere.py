"""Un sitemap ENGENDRE par du code : quand le correcteur peut-il en exclure les pages noindex ?

14/10/2026, easyshopbuilder.com. `app/sitemap.ts` liste toutes les pages de contenu, dont les
quatre accueils de blog que leur front matter met en `robots: "noindex, follow"`. Le plan
protege ne retire que des <loc> LITTERAUX : il refusait, et la seule erreur du site restait.
Le bon geste est une regle dans le generateur — filtrer sur le champ que la page porte DEJA —
et ce module dit quand ce geste se prouve, juge ce que le modele a ecrit, puis compare le
sitemap servi par l'apercu a celui du site.

Trois garde-fous, dans cet ordre :
1. AVANT le modele : un seul generateur, un champ noindex dans la source de chaque page
   signalee, et ce champ lu par le generateur ou un module qu'il importe ;
2. APRES le modele : `garde_filtre` — le filtre lit noindex ET ce champ, aucune URL ni motif
   d'URL en dur, une modification courte ;
3. APRES le deploiement : `ecart_apercu` — le sitemap de l'apercu est celui du site, moins
   exactement les pages noindex.
"""

from __future__ import annotations

import posixpath
import re
from collections import Counter

_FRONT_MATTER_RE = re.compile(r"\A---\s*\n(.*?)\n---\s*(?:\n|\Z)", re.S)
_CLE_RE = re.compile(r"^([A-Za-z_][\w-]*)\s*:\s*(.*)$")
_IMPORT_RE = re.compile(r"""(?:\bfrom\s+|\bimport\s+|\brequire\(\s*)["']([^"']+)["']""")
_EXTENSIONS = ("", ".ts", ".tsx", ".js", ".mjs", ".cjs", ".jsx",
               "/index.ts", "/index.tsx", "/index.js", "/index.mjs")


def champ_noindex(source: str) -> str:
    """Le champ qui porte le noindex dans la source d'une page, ou "" (absent ou ambigu)."""
    trouves: set[str] = set()
    m = _FRONT_MATTER_RE.match(source or "")
    if m:
        for ligne in m.group(1).splitlines():
            cle = _CLE_RE.match(ligne)
            if not cle:
                continue
            nom, valeur = cle.group(1), cle.group(2).strip().strip("\"'").lower()
            if "noindex" in valeur or (nom.lower() == "noindex" and valeur in {"true", "yes"}):
                trouves.add(nom)
    else:
        texte = source or ""
        if re.search(r"\brobots\s*:\s*\{[^}]*\bindex\s*:\s*false", texte) or re.search(
                r"""\brobots\s*:\s*["'][^"']*noindex""", texte, re.I):
            trouves.add("robots")
        if re.search(r"\bnoindex\s*:\s*true\b", texte):
            trouves.add("noindex")
    return trouves.pop() if len(trouves) == 1 else ""


def modules_importes(source: str, chemin: str, chemins: set[str]) -> list[str]:
    """Les fichiers du depot qu'importe `source` (relatifs ou alias `@/`), ceux qui existent."""
    sortie: list[str] = []
    dossier = posixpath.dirname(chemin)
    for spec in _IMPORT_RE.findall(source or ""):
        if spec.startswith("@/"):
            bases = [spec[2:], "src/" + spec[2:]]
        elif spec.startswith("./") or spec.startswith("../"):
            bases = [posixpath.normpath(posixpath.join(dossier, spec))]
        else:
            continue
        for base in bases:
            trouve = next((base + ext for ext in _EXTENSIONS if base + ext in chemins), "")
            if trouve and trouve not in sortie:
                sortie.append(trouve)
                break
    return sortie


def lit_le_champ(generateur: str, modules: dict[str, str], champ: str) -> bool:
    """Le generateur, ou un module qu'il importe, manipule-t-il deja ce champ ?"""
    motif = re.compile(r"\b%s\b" % re.escape(champ))
    return bool(champ) and any(motif.search(t or "") for t in [generateur, *modules.values()])


def _litteraux(ligne: str) -> list[str]:
    return [a or b or c for a, b, c in re.findall(r'"([^"\n]*)"|\'([^\'\n]*)\'|`([^`\n]*)`', ligne)]


def _chemin(url: str) -> str:
    s = re.sub(r"^[a-z]+://[^/]+", "", (url or "").strip(), flags=re.I).split("#", 1)[0].split("?", 1)[0]
    return (s.rstrip("/") or "/") if s else ""


def garde_filtre(ancien: str, nouveau: str, urls: list[str], champ: str) -> str:
    """"" si le generateur reecrit exclut les noindex PAR LE CHAMP, sinon la raison du refus."""
    restantes = Counter(ancien.splitlines())
    ajoutees: list[str] = []
    for ligne in nouveau.splitlines():
        if restantes[ligne] > 0:
            restantes[ligne] -= 1
        else:
            ajoutees.append(ligne)
    retirees = sum(restantes.values())
    if len(ajoutees) > 12 or retirees > 3:
        return "modification trop large pour un filtre (%d ligne(s) ajoutee(s), %d retiree(s))" % (
            len(ajoutees), retirees)
    if nouveau.lower().count("noindex") <= ancien.lower().count("noindex"):
        return "aucun filtre noindex ajoute"
    if not any(re.search(r"\b%s\b" % re.escape(champ), ligne) for ligne in ajoutees):
        return "le filtre ne lit pas le champ `%s`" % champ
    signales = {_chemin(u) for u in urls} | {u.strip() for u in urls}
    for ligne in ajoutees:
        for litteral in _litteraux(ligne):
            if litteral.strip() in signales or _chemin(litteral) in signales and litteral.startswith(("/", "http")):
                return "URL ecrite en dur : " + litteral[:80]
            if litteral.startswith("/") or "://" in litteral:
                return "motif d'URL ajoute : " + litteral[:80]
    return ""


def chemins_sitemap(xml: str) -> tuple[set[str], list[str]]:
    """(chemins des <url><loc>, URL des sitemaps enfants si c'est un index). Leve si illisible."""
    from defusedxml import ElementTree as ET

    racine = ET.fromstring(xml)
    nom = racine.tag.rsplit("}", 1)[-1]
    if nom == "sitemapindex":
        return set(), [(e.text or "").strip() for e in racine.findall("./{*}sitemap/{*}loc") if (e.text or "").strip()]
    if nom != "urlset":
        raise ValueError("ni urlset ni sitemapindex")
    return {_chemin(e.text or "") for e in racine.findall("./{*}url/{*}loc") if (e.text or "").strip()}, []


def ecart_apercu(vivants: set[str], apercu: set[str], urls: list[str]) -> str:
    """"" si l'apercu sert le sitemap du site moins EXACTEMENT les pages noindex."""
    retirer = {_chemin(u) for u in urls}
    encore = sorted(retirer & apercu)
    if encore:
        return "le sitemap de l'apercu liste encore : " + ", ".join(encore[:5])
    perdues = sorted((vivants - retirer) - apercu)
    if perdues:
        return "le sitemap de l'apercu a perdu %d page(s) indexable(s) : %s" % (
            len(perdues), ", ".join(perdues[:5]))
    return ""
