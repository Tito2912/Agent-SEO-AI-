"""Un texte qui se lit comme ecrit par une personne : les tics d'ecriture des modeles, mesures.

Demande du proprietaire, 02/11/2026 : « le contenu doit paraitre avoir ete fait par un humain ».

CE QUE CE MODULE FAIT, ET CE QU'IL NE FAIT PAS. Il repere les FORMULES ET LES HABITUDES qui
signalent un texte de modele a un lecteur — l'introduction « dans un monde en constante
evolution », le « il est important de noter », la conclusion qui resume ce qu'on vient de lire,
les phrases toutes de la meme longueur, les listes partout — et fait reecrire les passages
concernes. C'est un travail de QUALITE : ces tics rendent un texte creux, et un lecteur s'en va.

Il ne cherche PAS a tromper un detecteur d'IA, et ne promet pas d'y echapper : aucun caractere
invisible, aucune faute volontaire, aucune reformulation en boucle contre un outil de detection.
Les detecteurs se trompent (OpenAI a retire le sien en 2023 faute de precision), et Google ne
sanctionne pas l'IA en tant que telle mais le contenu sans utilite produit en masse. Decision
expliquee au proprietaire le 02/11/2026.

Les seuils sont des REPERES, pas des verites : ils se reglent ici, et le releve reel dira ou ils
tombent juste.
"""
from __future__ import annotations

import re
import statistics
from typing import Any

# Les formules que les modeles placent partout et que personne n'ecrit spontanement. Une
# expression par motif, insensible a la casse ; la liste se tient a ce qu'on a vu, elle ne
# pretend pas etre complete.
_TICS: dict[str, tuple[str, ...]] = {
    "fr": (
        r"dans un monde (?:en constante [ée]volution|o[uù] tout va (?:tr[eè]s )?vite|de plus en plus)",
        r"[àa] l['’][ée]re (?:du num[ée]rique|de l['’]intelligence artificielle)",
        r"il (?:est|convient) (?:important|essentiel|crucial|primordial) de (?:noter|souligner|rappeler|comprendre)",
        r"il convient de (?:noter|souligner)",
        r"force est de constater",
        r"(?:^|\n)\s*en (?:conclusion|r[ée]sum[ée]|somme)\s*,",
        r"dans cet article,? (?:nous (?:allons|verrons|explorerons)|vous (?:allez|d[ée]couvrirez))",
        r"plongeons(?: ensemble)?",
        r"sans plus attendre",
        r"vous l['’]aurez compris",
        r"n['’]h[ée]sitez pas [àa]",
        r"que vous soyez (?:un |une )?[^,.]{2,40} ou (?:un |une )?",
        r"(?:un|une) v[ée]ritable (?:atout|levier|game[- ]changer|r[ée]volution|alli[ée])",
        r"incontournable",
        r"(?:r[ée]volutionner|d[ée]cupler|booster) (?:votre|vos)",
        r"le saviez-vous",
        r"au c(?:œ|oe)ur de (?:votre|vos|la strat[ée]gie)",
        r"naviguer (?:dans|au sein de) (?:le|la|les|ce|cet|cette)",
        r"une (?:mine d['’]or|panoplie|pl[ée]thore) de",
    ),
    "en": (
        r"in today['’]s (?:fast-paced|digital|ever-changing|ever-evolving) (?:world|landscape|age)",
        r"in an (?:ever-changing|ever-evolving) (?:world|landscape)",
        r"it['’]s (?:important|essential|crucial|worth) (?:to note|noting|to remember|to understand)",
        r"\bdelv(?:e|es|ing) (?:into|deeper)",
        r"(?:^|\n)\s*in (?:conclusion|summary)\s*,",
        r"in this (?:article|guide|post),? we(?:['’]ll| will) (?:explore|dive|look|cover)",
        r"let['’]s dive in",
        r"\bdive (?:deep )?into\b",
        r"look no further",
        r"whether you['’]re an? [^,.]{2,40} or an? ",
        r"\b(?:game[- ]changer|tapestry|testament to|treasure trove|plethora of)\b",
        r"\bunlock(?:ing)? the (?:power|potential|secrets?)\b",
        r"\bnavigat(?:e|ing) the (?:complex(?:ities)?|world|landscape|realm)",
        r"\b(?:elevate|supercharge|revolutionize) your\b",
        r"\bin the realm of\b",
        r"\bseamless(?:ly)?\b",
        r"\bembark on\b",
    ),
}

# Les connecteurs dont les modeles ouvrent paragraphe apres paragraphe. PAS les mots courants :
# le premier essai en production (02/11/2026) signalait « Avec 2 %… Avec 5 %… Avec 8 % », « La… »,
# « Pour… » — un parallelisme voulu, que toute personne ecrit — et faisait reecrire sept
# paragraphes pour rien.
_CONNECTEURS: dict[str, tuple[str, ...]] = {
    "fr": ("de plus", "en outre", "par ailleurs", "ainsi", "en effet", "enfin", "ensuite",
           "toutefois", "cependant", "neanmoins", "néanmoins", "en somme", "d'autre part",
           "d’autre part", "par consequent", "par conséquent"),
    "en": ("moreover", "furthermore", "additionally", "in addition", "however", "ultimately",
           "overall", "consequently", "therefore", "notably"),
}

# Les reperes. Chacun dit ce qu'il mesure dans son nom.
SEUIL_TICS = 0                 # une formule de la liste suffit a faire reecrire son paragraphe
SEUIL_TIRETS_POUR_MILLE = 6    # tirets longs pour 1 000 mots
SEUIL_PART_DE_LISTES = 0.45    # part des lignes du corps qui sont des puces
SEUIL_VARIATION_PHRASES = 0.30 # coefficient de variation de la longueur des phrases
SEUIL_CONNECTEURS = 3          # paragraphes ouverts par un connecteur de la liste


def _sans_balisage(texte: str) -> str:
    texte = re.sub(r"!\[[^\]]*\]\([^)]*\)", " ", texte)
    texte = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", texte)
    return re.sub(r"[*_`#>]", " ", texte)


_TETE_RE = re.compile(r"\A(?:---|\+\+\+)\s*\n.*?\n(?:---|\+\+\+)\s*\n", re.S)


def paragraphes(markdown: str) -> list[str]:
    """Les paragraphes de PROSE du corps : ni tete (front matter), ni titres, ni puces, ni images,
    ni tableaux, ni code, ni `import`/`export` d'un fichier MDX."""
    out = []
    corps = _TETE_RE.sub("", str(markdown or "").replace("\r\n", "\n"))
    for bloc in re.split(r"\n\s*\n", corps):
        bloc = bloc.strip()
        if (not bloc or bloc.startswith(("#", "|", "!", "```", "<", "{", "import ", "export "))
                or re.match(r"^(?:[-*+]|\d+[.)])\s", bloc)):
            continue
        out.append(bloc)
    return out


def _phrases(texte: str) -> list[str]:
    return [p for p in re.split(r"(?<=[.!?…])\s+", _sans_balisage(texte)) if len(p.split()) >= 3]


def tics_du_paragraphe(paragraphe: str, langue: str) -> list[str]:
    """Les formules de modele qu'un paragraphe contient, telles qu'ecrites."""
    trouves = []
    for motif in _TICS.get(str(langue or "")[:2].lower(), ()):
        m = re.search(motif, paragraphe, flags=re.I)
        if m:
            trouves.append(m.group(0).strip())
    return trouves


def examiner(markdown: str, langue: str) -> dict[str, Any]:
    """Ce qu'un lecteur reconnaitrait comme un texte de modele, mesure.

    Rend {"a_reecrire": [index de paragraphe -> raisons], "global": [raisons], "mesures": {...}}.
    `a_reecrire` designe des paragraphes precis : une reecriture se fait a l'endroit du defaut,
    pas sur tout l'article (moins cher, et sans risque pour les liens et les visuels ailleurs).
    """
    corps = str(markdown or "")
    paras = paragraphes(corps)
    mots = len(_sans_balisage(corps).split()) or 1
    a_reecrire: dict[int, list[str]] = {}
    for i, p in enumerate(paras):
        tics = tics_du_paragraphe(p, langue)
        if len(tics) > SEUIL_TICS:
            a_reecrire.setdefault(i, []).append("formule toute faite : « %s »" % "», «".join(tics))
    longueurs = [len(ph.split()) for p in paras for ph in _phrases(p)]
    variation = (statistics.pstdev(longueurs) / statistics.mean(longueurs)) if len(longueurs) >= 6 else 1.0
    tirets = corps.count("—") * 1000 / mots
    lignes = [li for li in corps.splitlines() if li.strip() and not li.lstrip().startswith(("#", "!", "|"))]
    puces = sum(1 for li in lignes if re.match(r"^\s*(?:[-*+]|\d+[.)])\s", li))
    part_listes = puces / len(lignes) if lignes else 0.0
    connecteurs = _CONNECTEURS.get(str(langue or "")[:2].lower(), ())
    ouverts = [i for i, p in enumerate(paras)
               if any(re.match(r"%s\b" % re.escape(c), _sans_balisage(p).strip().lower()) for c in connecteurs)]
    globales: list[str] = []
    if variation < SEUIL_VARIATION_PHRASES:
        globales.append("phrases de longueur trop régulière (variation %.2f)" % variation)
    if tirets > SEUIL_TIRETS_POUR_MILLE:
        # Les tirets se corrigent la ou ils sont : chaque paragraphe qui en porte part en
        # reecriture (sous le plafond de paragraphes de l'appelant).
        for i, p in enumerate(paras):
            if "—" in p:
                a_reecrire.setdefault(i, []).append(
                    "tirets longs (—) : remplace-les par une virgule, un deux-points ou des parenthèses")
        globales.append("%.0f tirets longs pour 1 000 mots" % tirets)
    if part_listes > SEUIL_PART_DE_LISTES:
        globales.append("%.0f %% des lignes sont des puces" % (part_listes * 100))
    if len(ouverts) >= SEUIL_CONNECTEURS:
        for i in ouverts[1:]:
            a_reecrire.setdefault(i, []).append("paragraphe ouvert par un connecteur, comme d'autres")
    return {"a_reecrire": a_reecrire, "global": globales,
            "mesures": {"mots": mots, "variation_phrases": round(variation, 2),
                        "tirets_pour_mille": round(tirets, 1), "part_listes": round(part_listes, 2),
                        "tics": sum(len(tics_du_paragraphe(p, langue)) for p in paras)}}


def remplacer_les_paragraphes(markdown: str, anciens: "list[str]", nouveaux: "list[str]") -> tuple[str, int]:
    """Remplace chaque paragraphe par sa reecriture, seulement s'il est trouve UNE fois, tel quel.

    Une reecriture qui perd un lien, un marqueur de visuel ou un chiffre n'est pas posee : le
    defaut d'un tic vaut moins que la perte d'un lien verifie ou d'une donnee.
    """
    poses = 0
    for ancien, nouveau in zip(anciens, nouveaux):
        nouveau = str(nouveau or "").strip()
        if not nouveau or markdown.count(ancien) != 1:
            continue
        if _invariants(ancien) != _invariants(nouveau):
            continue
        markdown = markdown.replace(ancien, nouveau)
        poses += 1
    return markdown, poses


def _invariants(texte: str) -> tuple:
    """Ce qu'une reecriture de style ne doit pas toucher : liens, visuels et chiffres."""
    liens = sorted(re.findall(r"\]\(([^)\s]+)", texte))
    chiffres = sorted(re.findall(r"\d+(?:[.,\s  ]\d+)*", texte))
    return (liens, [re.sub(r"[\s  ]", "", c) for c in chiffres])


CONSIGNE_DE_NATUREL = (
    "\n\nECRITURE — le texte doit se lire comme celui d'une personne du metier, pas d'un modele :\n"
    "- entre directement dans le sujet : pas d'introduction generale sur l'epoque ou le monde, "
    "pas d'annonce du plan (« dans cet article, nous verrons ») ;\n"
    "- pas de conclusion qui resume ce qu'on vient de lire : finis sur un conseil concret, une "
    "limite ou une decision a prendre ;\n"
    "- varie la longueur des phrases ; une phrase courte apres une longue ;\n"
    "- des exemples precis et situes plutot que des generalites ; dis ce qui ne marche pas aussi ;\n"
    "- des listes seulement pour ce qui EST une liste (etapes, criteres), pas pour decouper la prose ;\n"
    "- peu de tirets longs ; jamais de titres en Majuscules A Chaque Mot en francais ;\n"
    "- bannis ces formules : « dans un monde en constante evolution », « il est important de "
    "noter », « en conclusion », « plongeons », « n'hesitez pas a », « un veritable atout », "
    "« incontournable », « in today's fast-paced world », « delve », « it's important to note », "
    "« let's dive in », « game-changer », « unlock the power »."
)


SYSTEME_REECRITURE = (
    "Tu reecris des paragraphes d'un article pour qu'ils se lisent comme ecrits par une personne "
    "du metier. Tu rends un objet JSON {\"paragraphes\": [\"...\", ...]} : un paragraphe reecrit "
    "par paragraphe recu, dans le meme ordre. Aucune explication."
)


def demande_de_reecriture(paras: "list[str]", raisons: "list[list[str]]", langue: str) -> str:
    return (
        "LANGUE : %s\n\nPour chaque paragraphe, garde le sens, les faits, CHAQUE chiffre et CHAQUE "
        "lien Markdown exactement ; change la tournure pour retirer le defaut signale. Ne rajoute "
        "ni idee ni promesse.\n\n%s" % (langue or "celle du texte", "\n\n".join(
            "PARAGRAPHE %d — defaut : %s\n-----\n%s\n-----" % (i + 1, " ; ".join(r), p)
            for i, (p, r) in enumerate(zip(paras, raisons))))
        + CONSIGNE_DE_NATUREL)
