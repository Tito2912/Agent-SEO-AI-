"""Les visuels d'un article : images du client, illustrations IA, graphiques, captures d'ecran.

Demande du proprietaire, 02/11/2026 : enrichir l'article de l'ecran Contenu, soit avec les
images du client, soit avec des visuels que Noyaru ajoute. Ses decisions, a ne pas re-litiger :

  * les visuels IA sont INCLUS dans l'article — au plus `MAX_VISUELS_IA` par article, aucun
    compteur de plus ;
  * un graphique ne porte QUE des chiffres ecrits dans l'article ou dans les donnees que le
    client a collees, et dit « exemple » quand le texte les presente comme illustratifs ;
  * une capture d'ecran seulement quand elle sert l'article ;
  * le mode « contenu seul » d'abord ; le mode GitHub ensuite.

CE MODULE NE PARLE A AUCUN FOURNISSEUR. Il prepare les fichiers, verifie ce que le modele
propose et dessine les graphiques ; les appels (modele de texte, modele d'image, navigateur)
restent dans `app.py`, ou chaque appel d'IA est releve.

LE DESSIN D'UN GRAPHIQUE EST FAIT ICI, JAMAIS PAR LE MODELE. Le modele rend des DONNEES (titre,
libelles, valeurs) ; une image ecrite par lui — un SVG, par exemple — serait du balisage venu
d'un modele et colle dans le site du client.
"""
from __future__ import annotations

import io
import math
import re
import zipfile
from functools import lru_cache
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont, ImageOps

MAX_IMPORTS = 6
MAX_VISUELS_IA = 4
IMPORT_MAX_OCTETS = 10 * 1024 * 1024
LARGEUR_MAX = 1600
# Un JPEG de 40 megapixels decode en ~120 Mo : au-dela, un seul envoi pese sur le conteneur web.
_PIXELS_MAX = 40_000_000
_FORMATS_ACCEPTES = {"JPEG", "PNG", "WEBP"}
_QUALITE_WEBP = 82

# Le marquage IPTC « produit par un algorithme entraine », lu notamment par Google pour signaler
# une image generee. Pose sur les seules illustrations IA : l'AI Act (article 50) demande que les
# contenus synthetiques soient marques de facon lisible par une machine, et ce marquage ne
# change rien a l'affichage.
XMP_IMAGE_IA = (
    b'<?xpacket begin="\xef\xbb\xbf" id="W5M0MpCehiHzreSzNTczkc9d"?>'
    b'<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF '
    b'xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"><rdf:Description rdf:about="" '
    b'xmlns:Iptc4xmpExt="http://iptc.org/std/Iptc4xmpExt/2008-02-29/">'
    b"<Iptc4xmpExt:DigitalSourceType>"
    b"http://cv.iptc.org/newscodes/digitalsourcetype/trainedAlgorithmicMedia"
    b"</Iptc4xmpExt:DigitalSourceType></rdf:Description></rdf:RDF></x:xmpmeta>"
    b'<?xpacket end="w"?>'
)


class ImageRefusee(ValueError):
    """Un fichier qu'on ne publiera pas, avec la raison a montrer a la personne."""


def preparer_image(octets: bytes, *, largeur_max: int = LARGEUR_MAX,
                   xmp: bytes = b"") -> tuple[bytes, int, int]:
    """Un fichier image pret a publier : (WebP, largeur, hauteur), ou `ImageRefusee`.

    TOUT FICHIER PASSE PAR ICI, celui du client comme celui du modele d'image : le format, la
    taille et les metadonnees d'un article ne dependent pas de la source de l'image.

    Les donnees EXIF sont retirees, pas recopiees : une photo de telephone porte souvent les
    coordonnees GPS de l'endroit ou elle a ete prise, et elle finirait en ligne avec elles.
    L'orientation qu'elles portaient est appliquee avant, sinon la photo sortirait couchee.
    """
    if not octets:
        raise ImageRefusee("fichier vide")
    if len(octets) > IMPORT_MAX_OCTETS:
        raise ImageRefusee("fichier trop lourd (10 Mo au plus)")
    try:
        with Image.open(io.BytesIO(octets)) as source:
            if source.format not in _FORMATS_ACCEPTES:
                raise ImageRefusee("format non pris en charge : JPEG, PNG ou WebP")
            if source.width * source.height > _PIXELS_MAX:
                raise ImageRefusee("image trop grande (40 mégapixels au plus)")
            if getattr(source, "n_frames", 1) > 1:
                raise ImageRefusee("image animée non prise en charge")
            icc = source.info.get("icc_profile")
            image = ImageOps.exif_transpose(source)
            transparente = "A" in image.getbands() or "transparency" in image.info
            image = image.convert("RGBA" if transparente else "RGB")
    except ImageRefusee:
        raise
    except Exception as e:
        raise ImageRefusee("fichier illisible : ce n'est pas une image") from e
    if image.width > largeur_max:
        hauteur = max(1, round(image.height * largeur_max / image.width))
        image = image.resize((largeur_max, hauteur), Image.Resampling.LANCZOS)
    sortie = io.BytesIO()
    options: dict[str, Any] = {"quality": _QUALITE_WEBP, "method": 4}
    if icc:
        options["icc_profile"] = icc
    if xmp:
        options["xmp"] = xmp
    image.save(sortie, "WEBP", **options)
    return sortie.getvalue(), image.width, image.height


def image_pour_le_modele(octets_webp: bytes, *, cote_max: int = 1024) -> bytes:
    """Une copie reduite, pour que le modele VOIE l'image sans payer ses 1600 pixels."""
    with Image.open(io.BytesIO(octets_webp)) as image:
        copie = image.convert("RGB")
    copie.thumbnail((cote_max, cote_max))
    sortie = io.BytesIO()
    copie.save(sortie, "WEBP", quality=75, method=4)
    return sortie.getvalue()


# ── Les marqueurs ────────────────────────────────────────────────────────────────────────────
#
# Le modele place chaque visuel DANS le texte, la ou il sert : `![texte alternatif](visuel:N)`.
# La syntaxe est celle d'une image Markdown, pour que le texte alternatif soit ecrit a cote du
# propos qu'il illustre, et dans la langue de la version. Le marqueur est remplace par le vrai
# fichier une fois le visuel produit — ou retire s'il n'a pas pu l'etre.

_MARQUEUR_RE = re.compile(r"!\[([^\]\n]*)\]\(visuel:(\d{1,2})\)")
_IMAGE_MD_RE = re.compile(r"!\[[^\]\n]*\]\(([^)\s]*)[^)]*\)")


def marqueurs(markdown: str) -> list[int]:
    """Les visuels places dans le texte, dans l'ordre de lecture, chacun une fois."""
    return list(dict.fromkeys(int(m.group(2)) for m in _MARQUEUR_RE.finditer(str(markdown or ""))))


def retirer_les_images_etrangeres(markdown: str) -> tuple[str, int]:
    """Retire toute image Markdown qui n'est pas un marqueur de visuel.

    Une adresse d'image ecrite par le modele est INVENTEE : il n'a aucune image a sa
    disposition. Elle afficherait une image cassee, ou celle d'un autre site.
    """
    retirees = [0]

    def _remplacer(m: re.Match) -> str:
        if m.group(1).startswith("visuel:"):
            return m.group(0)
        retirees[0] += 1
        return ""

    return _IMAGE_MD_RE.sub(_remplacer, str(markdown or "")), retirees[0]


def _alt_propre(alt: str) -> str:
    return re.sub(r"\s+", " ", str(alt or "").replace("[", "").replace("]", "")).strip()[:180]


def poser_les_visuels(markdown: str, fichiers: "dict[int, dict[str, Any]]", *,
                      dossier: str = "images") -> tuple[str, list[int]]:
    """Remplace les marqueurs par les fichiers produits. Rend (texte, visuels poses).

    Un marqueur sans fichier — visuel refuse, ou dont la production a echoue — est RETIRE :
    une image cassee dans un article livre est pire que pas d'image. Un visuel place deux fois
    ne l'est qu'a sa premiere place.
    """
    poses: list[int] = []

    def _remplacer(m: re.Match) -> str:
        n = int(m.group(2))
        fichier = fichiers.get(n)
        if fichier is None or n in poses:
            return ""
        poses.append(n)
        alt = _alt_propre(m.group(1)) or _alt_propre(fichier.get("alt", ""))
        return "![%s](%s/%s)" % (alt, dossier, fichier["fichier"])

    texte = _MARQUEUR_RE.sub(_remplacer, str(markdown or ""))
    # Un marqueur retire laisse sa ligne vide derriere lui.
    return re.sub(r"\n{3,}", "\n\n", texte), poses


def nom_de_visuel(slug: str, n: int, code: str = "") -> str:
    """`<slug>-<n>.webp` : un nom de fichier qui dit de quel article vient l'image."""
    base = re.sub(r"[^a-z0-9]+", "-", str(slug or "article").lower()).strip("-")[:60] or "article"
    suffixe = re.sub(r"[^a-z0-9]+", "", str(code or "").lower())
    return "%s-%d%s.webp" % (base, int(n), "-" + suffixe if suffixe else "")


# ── Les chiffres de l'article ────────────────────────────────────────────────────────────────
#
# LE GRAPHIQUE NE DIT RIEN QUE LE TEXTE NE DISE DEJA. Un chiffre qui n'apparait que dans
# l'image serait une donnee inventee presentee comme une mesure — et il se lirait parfaitement
# bien. Chaque valeur doit donc se retrouver dans l'article ou dans les donnees du client, a
# l'ecriture pres (« 1 200 », « 1,200 », « 1.200 », « 12,5 »).

_NOMBRE_RE = re.compile(
    r"(?<![\w.,])(\d{1,3}(?:[   .,]\d{3})+(?:[.,]\d+)?|\d+(?:[.,]\d+)?)(?!\w)")


def _lectures(jeton: str) -> set[float]:
    """Toutes les valeurs qu'une ecriture peut vouloir dire, dans les conventions du site."""
    t = jeton.replace(" ", " ").replace(" ", " ")
    if " " in t:
        return _lectures(t.replace(" ", ""))
    lectures: set[float] = set()
    if "," in t and "." in t:
        # « 1,234.5 » a l'anglaise, « 1.234,5 » a l'allemande : le dernier separateur est decimal.
        lectures.add(float(t.replace(".", "").replace(",", ".") if t.rfind(",") > t.rfind(".")
                           else t.replace(",", "")))
        return lectures
    for sep in (",", "."):
        if sep in t:
            parts = t.split(sep)
            if len(parts) == 2:
                lectures.add(float(t.replace(sep, ".")))
            if all(len(p) == 3 for p in parts[1:]):
                lectures.add(float(t.replace(sep, "")))
            return lectures
    lectures.add(float(t))
    return lectures


def nombres_du_texte(texte: str) -> set[float]:
    """Les valeurs ecrites dans un texte, toutes lectures confondues, au signe pres."""
    out: set[float] = set()
    for m in _NOMBRE_RE.finditer(str(texte or "")):
        try:
            out.update(round(abs(v), 6) for v in _lectures(m.group(1)))
        except ValueError:
            continue
    return out


_TYPES_DE_GRAPHIQUE = {"barres": "barres", "barre": "barres", "bar": "barres", "bars": "barres",
                       "colonnes": "barres", "courbe": "courbe", "ligne": "courbe",
                       "line": "courbe", "lignes": "courbe"}


def graphique_valide(spec: Any, *, textes: "list[str]") -> tuple["dict[str, Any] | None", str]:
    """Le graphique normalise, ou (None, raison). `textes` : l'article et les donnees du client."""
    if not isinstance(spec, dict):
        return None, "graphique illisible"
    sorte = _TYPES_DE_GRAPHIQUE.get(str(spec.get("type") or "barres").strip().lower())
    if sorte is None:
        return None, "type de graphique inconnu"
    titre = re.sub(r"\s+", " ", str(spec.get("titre") or "")).strip()[:90]
    libelles = [re.sub(r"\s+", " ", str(x)).strip()[:28] for x in (spec.get("libelles") or [])]
    if not titre or not 2 <= len(libelles) <= 12 or not all(libelles):
        return None, "graphique sans titre, ou sans 2 à 12 libellés"
    series_brutes = spec.get("series")
    if not isinstance(series_brutes, list) or not 1 <= len(series_brutes) <= 3:
        return None, "graphique sans 1 à 3 séries"
    series: list[dict[str, Any]] = []
    for s in series_brutes:
        valeurs = s.get("valeurs") if isinstance(s, dict) else None
        if (not isinstance(valeurs, list) or len(valeurs) != len(libelles)
                or not all(isinstance(v, (int, float)) and not isinstance(v, bool)
                           and math.isfinite(v) for v in valeurs)):
            return None, "valeurs du graphique illisibles"
        series.append({"nom": re.sub(r"\s+", " ", str(s.get("nom") or "")).strip()[:40],
                       "valeurs": [float(v) for v in valeurs]})
    connus: set[float] = set()
    for t in textes:
        connus |= nombres_du_texte(t)
    absentes = sorted({v for s in series for v in s["valeurs"] if round(abs(v), 6) not in connus})
    if absentes:
        return None, ("chiffres absents du texte (%s) : un graphique ne montre que des valeurs "
                      "que l'article écrit" % ", ".join(_format_brut(v) for v in absentes[:4]))
    source = re.sub(r"\s+", " ", str(spec.get("source") or "")).strip()[:120]
    # Sans source, le chiffre est un exemple : on le dit plutot que de le laisser passer pour
    # une mesure.
    exemple = bool(spec.get("exemple")) or not source
    return {"type": sorte, "titre": titre, "libelles": libelles, "series": series,
            "unite": re.sub(r"\s+", " ", str(spec.get("unite") or "")).strip()[:12],
            "source": "" if exemple else source, "exemple": exemple}, ""


def _format_brut(v: float) -> str:
    return str(int(v)) if float(v).is_integer() else str(v)


def textes_traduits(spec: "dict[str, Any]", traduction: Any) -> "dict[str, Any] | None":
    """Les textes d'un graphique dans une autre langue, ou None. Les VALEURS ne changent jamais.

    La traduction rend titre, libelles, noms de series, unite et source ; une liste de libelles
    de la mauvaise longueur ne se recolle pas a ses valeurs, donc elle est refusee.
    """
    if not isinstance(traduction, dict):
        return None
    libelles = [re.sub(r"\s+", " ", str(x)).strip()[:28] for x in (traduction.get("libelles") or [])]
    noms = [re.sub(r"\s+", " ", str(x)).strip()[:40] for x in (traduction.get("series") or [])]
    titre = re.sub(r"\s+", " ", str(traduction.get("titre") or "")).strip()[:90]
    if not titre or len(libelles) != len(spec["libelles"]) or not all(libelles):
        return None
    return {"titre": titre, "libelles": libelles,
            "noms": noms if len(noms) == len(spec["series"]) else [s["nom"] for s in spec["series"]],
            "unite": re.sub(r"\s+", " ", str(traduction.get("unite") or spec["unite"])).strip()[:12],
            "source": "" if spec["exemple"] else
            (re.sub(r"\s+", " ", str(traduction.get("source") or spec["source"])).strip()[:120])}


# ── Le dessin ────────────────────────────────────────────────────────────────────────────────

# La police Vera vient avec reportlab, deja une dependance : aucun fichier de plus a tenir, et
# elle couvre les accents de toutes les langues latines (é, œ, ß, ñ, €, guillemets).
_SEPARATEURS = {"fr": (" ", ","), "en": (",", "."), "de": (".", ","), "es": (".", ","),
                "it": (".", ","), "pt": (".", ","), "nl": (".", ",")}
_MENTION_EXEMPLE = {"fr": "Exemple chiffré", "en": "Illustrative figures", "de": "Beispielwerte",
                    "es": "Cifras de ejemplo", "it": "Cifre di esempio", "pt": "Valores de exemplo",
                    "nl": "Voorbeeldcijfers"}
_MENTION_SOURCE = {"fr": "Source : %s", "en": "Source: %s", "de": "Quelle: %s", "es": "Fuente: %s",
                   "it": "Fonte: %s", "pt": "Fonte: %s", "nl": "Bron: %s"}
_COULEURS = ("#2563eb", "#f59e0b", "#0f9d76")
_ENCRE, _GRIS, _GRILLE = "#1f2937", "#4b5563", "#e5e7eb"


@lru_cache(maxsize=16)
def _police(taille: int, gras: bool = False) -> Any:
    try:
        import reportlab
        dossier = Path(reportlab.__file__).resolve().parent / "fonts"
        return ImageFont.truetype(str(dossier / ("VeraBd.ttf" if gras else "Vera.ttf")), taille)
    except Exception:
        return ImageFont.load_default(size=taille)


def format_nombre(v: float, langue: str = "fr") -> str:
    """« 1 200 », « 1,200 » ou « 1.200 » ; « 12,5 » ou « 12.5 » — selon la langue de l'article."""
    milliers, decimale = _SEPARATEURS.get(str(langue or "").lower()[:2], _SEPARATEURS["en"])
    entier = float(v).is_integer()
    texte = "{:,.0f}".format(abs(v)) if entier else "{:,.2f}".format(abs(v)).rstrip("0").rstrip(".")
    texte = texte.replace(",", "\x00").replace(".", decimale).replace("\x00", milliers)
    return ("-" if v < 0 else "") + texte


def _echelle(bas: float, haut: float) -> tuple[float, float, float]:
    """Bornes et pas « ronds » (1, 2, 2,5 ou 5 fois une puissance de dix), environ cinq lignes."""
    bas, haut = min(0.0, bas), max(0.0, haut)
    if haut == bas:
        haut = bas + 1
    brut = (haut - bas) / 5
    puissance = 10 ** math.floor(math.log10(brut))
    pas = next(m * puissance for m in (1, 2, 2.5, 5, 10) if m * puissance >= brut)
    return math.floor(bas / pas) * pas, math.ceil(haut / pas) * pas, pas


def _ajuster(dessin: Any, texte: str, largeur: float, taille: int, *, gras: bool = False,
             mini: int = 16) -> tuple[str, Any]:
    """Le texte et la police qui tiennent dans `largeur` ; tronque avec « … » en dernier recours."""
    while taille > mini and dessin.textlength(texte, font=_police(taille, gras)) > largeur:
        taille -= 2
    police = _police(taille, gras)
    if dessin.textlength(texte, font=police) <= largeur:
        return texte, police
    while len(texte) > 1 and dessin.textlength(texte + "…", font=police) > largeur:
        texte = texte[:-1]
    return texte.rstrip() + "…", police


def dessiner_un_graphique(spec: "dict[str, Any]", *, langue: str = "fr",
                          textes: "dict[str, Any] | None" = None) -> bytes:
    """Le graphique en WebP, 1600 × 900. `textes` remplace les textes par leur traduction."""
    langue = str(langue or "").lower()[:2]
    t = textes or {"titre": spec["titre"], "libelles": spec["libelles"],
                   "noms": [s["nom"] for s in spec["series"]], "unite": spec["unite"],
                   "source": spec["source"]}
    largeur, hauteur = 1600, 900
    image = Image.new("RGB", (largeur, hauteur), "#ffffff")
    d = ImageDraw.Draw(image)
    titre, police_titre = _ajuster(d, t["titre"], largeur - 120, 46, gras=True)
    d.text((60, 44), titre, font=police_titre, fill=_ENCRE)

    series = spec["series"]
    haut_trace = 170
    if len(series) > 1:
        x = 60
        for i, nom in enumerate(t["noms"]):
            d.rectangle((x, 122, x + 26, 148), fill=_COULEURS[i])
            d.text((x + 38, 120), nom or "—", font=_police(26), fill=_GRIS)
            x += 38 + int(d.textlength(nom or "—", font=_police(26))) + 48
        haut_trace = 200

    valeurs = [v for s in series for v in s["valeurs"]]
    mini, maxi, pas = _echelle(min(valeurs), max(valeurs))
    unite = (" " + t["unite"]) if t["unite"] else ""
    graduations = [mini + i * pas for i in range(int(round((maxi - mini) / pas)) + 1)]
    police_axe = _police(24)
    marge_g = 60 + max(int(d.textlength(format_nombre(g, langue) + unite, font=police_axe))
                       for g in graduations) + 24
    gauche, droite, haut, bas = marge_g, largeur - 60, haut_trace, hauteur - 170

    def y_de(v: float) -> float:
        return bas - (v - mini) / (maxi - mini) * (bas - haut)

    for g in graduations:
        y = y_de(g)
        d.line((gauche, y, droite, y), fill=_GRILLE if g else "#9ca3af", width=2)
        texte = format_nombre(g, langue) + unite
        d.text((gauche - 18 - d.textlength(texte, font=police_axe), y - 14), texte,
               font=police_axe, fill=_GRIS)

    n = len(t["libelles"])
    groupe = (droite - gauche) / n
    etiquettes = n * len(series) <= 16
    police_val = _police(22, gras=True)
    for i, libelle in enumerate(t["libelles"]):
        centre = gauche + groupe * (i + 0.5)
        texte, police = _ajuster(d, libelle, groupe - 12, 26)
        d.text((centre - d.textlength(texte, font=police) / 2, bas + 18), texte, font=police,
               fill=_ENCRE)
    if spec["type"] == "barres":
        large = groupe * 0.72 / len(series)
        for j, s in enumerate(series):
            for i, v in enumerate(s["valeurs"]):
                x0 = gauche + groupe * i + groupe * 0.14 + large * j
                y0, y1 = sorted((y_de(v), y_de(0)))
                d.rectangle((x0, y0, x0 + large - 4, max(y1, y0 + 2)), fill=_COULEURS[j])
                if etiquettes:
                    texte = format_nombre(v, langue)
                    d.text((x0 + (large - 4) / 2 - d.textlength(texte, font=police_val) / 2,
                            (y0 - 32) if v >= 0 else (y1 + 6)), texte, font=police_val, fill=_ENCRE)
    else:
        for j, s in enumerate(series):
            points = [(gauche + groupe * (i + 0.5), y_de(v)) for i, v in enumerate(s["valeurs"])]
            d.line(points, fill=_COULEURS[j], width=6, joint="curve")
            for (x, y), v in zip(points, s["valeurs"]):
                d.ellipse((x - 9, y - 9, x + 9, y + 9), fill=_COULEURS[j])
                if etiquettes:
                    texte = format_nombre(v, langue)
                    d.text((x - d.textlength(texte, font=police_val) / 2, y - 42), texte,
                           font=police_val, fill=_ENCRE)

    mention = (_MENTION_EXEMPLE.get(langue, _MENTION_EXEMPLE["en"]) if spec["exemple"]
               else _MENTION_SOURCE.get(langue, _MENTION_SOURCE["en"]) % t["source"])
    mention, police_mention = _ajuster(d, mention, largeur - 120, 24)
    d.text((60, hauteur - 62), mention, font=police_mention, fill=_GRIS)
    sortie = io.BytesIO()
    image.save(sortie, "WEBP", lossless=True, method=4)
    return sortie.getvalue()


# ── La livraison ─────────────────────────────────────────────────────────────────────────────

def enrichir_les_images(html_texte: str, dimensions: "dict[str, tuple[int, int]]") -> str:
    """Pose largeur, hauteur et chargement differe sur les images de l'article.

    Sans dimensions, la page saute a chaque image qui arrive (CLS, une mesure des Core Web
    Vitals). La PREMIERE image n'est pas differee : elle est souvent visible au chargement.
    """
    premiere = [True]

    def _remplacer(m: re.Match) -> str:
        balise = m.group(0)
        src = re.search(r'src="images/([a-z0-9-]+\.webp)"', balise)
        if not src or src.group(1) not in dimensions:
            return balise
        largeur, hauteur = dimensions[src.group(1)]
        attributs = ' width="%d" height="%d"%s decoding="async"' % (
            largeur, hauteur, "" if premiere[0] else ' loading="lazy"')
        premiere[0] = False
        return balise.replace("<img", "<img" + attributs, 1)

    return re.sub(r"<img\b[^>]*>", _remplacer, str(html_texte or ""))


def nom_de_l_article(version: "dict[str, Any]") -> str:
    base = re.sub(r"[^a-z0-9-]+", "-", str(version.get("slug") or "article").lower()).strip("-")
    code = re.sub(r"[^a-z0-9]+", "", str(version.get("code") or "").lower())
    return (base or "article") + ("." + code if code else "")


def archive_de_l_article(versions: "list[dict[str, Any]]", images: "dict[str, bytes]") -> bytes:
    """Le .zip livre au client : chaque version en .md et .html, et le dossier `images/`.

    Les textes citent `images/<fichier>` : decompresse tel quel, l'apercu .html s'ouvre avec ses
    images, et le dossier se depose a cote de l'article dans le site.
    """
    sortie = io.BytesIO()
    with zipfile.ZipFile(sortie, "w", zipfile.ZIP_DEFLATED) as archive:
        for v in versions:
            nom = nom_de_l_article(v)
            archive.writestr(nom + ".md", str(v.get("markdown") or ""))
            archive.writestr(nom + ".html", str(v.get("html") or ""))
        for nom, octets in sorted(images.items()):
            # Un WebP est deja compresse : le recompresser couterait du temps pour rien.
            archive.writestr("images/" + nom, octets, compress_type=zipfile.ZIP_STORED)
    return sortie.getvalue()
