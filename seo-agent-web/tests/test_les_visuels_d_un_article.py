# -*- coding: utf-8 -*-
"""Les visuels d'un article : images du client, illustrations IA, graphiques, captures.

Demande du propriétaire, 02/11/2026, et ses décisions : visuels INCLUS dans l'article (au plus
4 ajoutés par Noyaru), illustrations IA réservées à Business (Pro passait sous 40 % de marge),
graphiques tirés des seuls chiffres de l'article ou des données du client, capture d'écran
seulement quand elle sert l'article, mode « contenu seul » d'abord.

Ce que ces tests défendent : aucune image cassée ni inventée dans un article livré ; aucun
chiffre qu'un graphique montrerait sans que le texte le dise ; aucune coordonnée GPS d'une photo
de client mise en ligne ; une image n'est lisible que depuis son projet ; et les visuels ne
coûtent aucun article de plus.
"""
from __future__ import annotations

import io
import re
import sys
import threading
import time
import uuid
import zipfile
from pathlib import Path

import pytest
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_une_page_neuve_part_en_brouillon as _brouillon  # noqa: E402
from test_un_article_sans_depot import CRAWL, S  # noqa: E402

app_module = _brouillon.app_module
Project = _brouillon.Project
customer = _brouillon.customer
plan = _brouillon.plan
m = app_module
visuels = app_module.visuels


def _png(largeur: int = 800, hauteur: int = 600, couleur: str = "#3366aa") -> bytes:
    sortie = io.BytesIO()
    Image.new("RGB", (largeur, hauteur), couleur).save(sortie, "PNG")
    return sortie.getvalue()


# ── Le fichier ────────────────────────────────────────────────────────────────────────────────

def test_une_photo_perd_ses_coordonnees_GPS_et_garde_son_orientation() -> None:
    """Une photo de téléphone porte souvent le lieu où elle a été prise ; elle finirait en ligne
    avec. L'orientation qu'elle portait est appliquée AVANT, sinon elle sortirait couchée."""
    exif = Image.Exif()
    exif[0x0112] = 6                              # « tourner de 90° pour l'afficher »
    exif[0x8825] = {2: (48.0, 51.0, 24.0)}        # latitude
    source = io.BytesIO()
    Image.new("RGB", (3000, 2000), "#88aacc").save(source, "JPEG", exif=exif.tobytes())
    webp, largeur, hauteur = visuels.preparer_image(source.getvalue())
    sortie = Image.open(io.BytesIO(webp))
    assert sortie.format == "WEBP" and not sortie.info.get("exif")
    assert (largeur, hauteur) == (1600, 2400) == sortie.size, "debout, et réduite à 1600 de large"


def test_une_illustration_IA_est_MARQUEE_comme_telle() -> None:
    """Le marquage IPTC « trainedAlgorithmicMedia » : lisible par une machine (AI Act, art. 50),
    sans rien changer à l'affichage. Les images du client ne le portent pas."""
    ia = Image.open(io.BytesIO(visuels.preparer_image(_png(), xmp=visuels.XMP_IMAGE_IA)[0]))
    client = Image.open(io.BytesIO(visuels.preparer_image(_png())[0]))
    assert b"trainedAlgorithmicMedia" in (ia.info.get("xmp") or b"")
    assert not client.info.get("xmp")


@pytest.mark.parametrize("octets, raison", [
    (b"", "vide"),
    (b"<html>pas une image</html>", "illisible"),
    (b"x" * (visuels.IMPORT_MAX_OCTETS + 1), "trop lourd"),
], ids=["vide", "illisible", "trop-lourd"])
def test_un_fichier_qui_n_est_pas_une_image_publiable_est_REFUSE(octets, raison) -> None:
    with pytest.raises(visuels.ImageRefusee, match=raison):
        visuels.preparer_image(octets)


def test_une_image_ANIMEE_ou_d_un_format_non_publie_est_refusee() -> None:
    gif = io.BytesIO()
    Image.new("RGB", (20, 20)).save(gif, "GIF", save_all=True,
                                     append_images=[Image.new("RGB", (20, 20), "red")])
    with pytest.raises(visuels.ImageRefusee, match="format"):
        visuels.preparer_image(gif.getvalue())


# ── Les marqueurs ────────────────────────────────────────────────────────────────────────────

def test_un_marqueur_sans_fichier_DISPARAIT_et_un_doublon_ne_se_pose_qu_une_fois() -> None:
    texte = ("## A\n\n![Le tableau](visuel:1)\n\n![Raté](visuel:2)\n\n![Encore](visuel:1)\n\n"
             "![](visuel:3)\n\nFin.\n")
    sortie, poses = visuels.poser_les_visuels(texte, {1: {"fichier": "a-1.webp"},
                                                      3: {"fichier": "a-3.webp", "alt": "Repli"}})
    assert poses == [1, 3]
    assert "![Le tableau](images/a-1.webp)" in sortie and "![Repli](images/a-3.webp)" in sortie
    assert "visuel:" not in sortie and "Raté" not in sortie and "Encore" not in sortie
    assert "\n\n\n" not in sortie


def test_une_image_a_ADRESSE_INVENTEE_par_le_modele_est_retiree() -> None:
    """Le modèle n'a aucune image à sa disposition : une adresse qu'il écrit est inventée."""
    texte, n = visuels.retirer_les_images_etrangeres(
        "![x](https://cdn.exemple.com/a.jpg)\n![y](/img/b.png)\n![z](visuel:2)\n[lien](/page)")
    assert n == 2 and texte == "\n\n![z](visuel:2)\n[lien](/page)"


def test_les_images_portent_leurs_DIMENSIONS_et_la_premiere_n_est_pas_differee() -> None:
    html = ('<p><img alt="a" src="images/x-1.webp" /></p><p><img alt="b" src="images/x-2.webp" />'
            '</p><img src="ailleurs.png">')
    sortie = visuels.enrichir_les_images(html, {"x-1.webp": (1600, 900), "x-2.webp": (800, 600)})
    premiere, seconde = re.findall(r"<img[^>]*>", sortie)[:2]
    assert 'width="1600" height="900"' in premiere and "lazy" not in premiere
    assert 'width="800" height="600"' in seconde and 'loading="lazy"' in seconde
    assert '<img src="ailleurs.png">' in sortie


# ── Les graphiques ────────────────────────────────────────────────────────────────────────────

TEXTE = "En janvier, 1 200 visites ; en février, 1,850 ; en mars 2.430, soit 12,5 % de plus."


def test_un_graphique_ne_montre_QUE_des_chiffres_ecrits_dans_le_texte() -> None:
    spec = {"titre": "Visites", "libelles": ["Jan", "Fév", "Mar"],
            "series": [{"nom": "Visites", "valeurs": [1200, 1850, 2430]}], "source": "GSC"}
    ok, refus = visuels.graphique_valide(spec, textes=[TEXTE])
    assert ok is not None and refus == "" and ok["source"] == "GSC" and not ok["exemple"]
    spec["series"][0]["valeurs"][2] = 2431
    ko, refus = visuels.graphique_valide(spec, textes=[TEXTE])
    assert ko is None and "2431" in refus


def test_les_DONNEES_du_client_justifient_un_chiffre() -> None:
    spec = {"titre": "Ventes", "libelles": ["A", "B"], "series": [{"nom": "", "valeurs": [37, 41]}]}
    assert visuels.graphique_valide(spec, textes=[TEXTE])[0] is None
    assert visuels.graphique_valide(spec, textes=[TEXTE, "produit;ventes\nA;37\nB;41"])[0] is not None


def test_un_graphique_SANS_SOURCE_se_presente_comme_un_exemple() -> None:
    spec = {"titre": "Visites", "libelles": ["Jan", "Fév"],
            "series": [{"nom": "Visites", "valeurs": [1200, 1850]}]}
    ok, _ = visuels.graphique_valide(spec, textes=[TEXTE])
    assert ok["exemple"] is True and ok["source"] == ""


def test_un_graphique_se_dessine_et_ses_textes_se_TRADUISENT_valeurs_comprises_jamais() -> None:
    spec, _ = visuels.graphique_valide(
        {"type": "courbe", "titre": "Visites", "libelles": ["Jan", "Fév"],
         "series": [{"nom": "Visites", "valeurs": [1200, 1850]}]}, textes=[TEXTE])
    image = Image.open(io.BytesIO(visuels.dessiner_un_graphique(spec, langue="fr")))
    assert image.format == "WEBP" and image.size == (1600, 900)
    traduits = visuels.textes_traduits(spec, {"titre": "Besuche", "libelles": ["Jan", "Feb"],
                                              "series": ["Besuche"], "valeurs": [1, 2]})
    assert traduits["titre"] == "Besuche" and "valeurs" not in traduits
    assert visuels.textes_traduits(spec, {"titre": "Besuche", "libelles": ["Jan"]}) is None


def test_les_nombres_suivent_la_LANGUE_de_l_article() -> None:
    assert visuels.format_nombre(1200, "fr") == "1 200" and visuels.format_nombre(12.5, "fr") == "12,5"
    assert visuels.format_nombre(1200, "en") == "1,200" and visuels.format_nombre(1200, "de") == "1.200"


# ── Ce que le modèle propose, vérifié ─────────────────────────────────────────────────────────

def _demande(**kw):
    base = {"imports": [{"legende": "tableau"}], "illustrations": 4, "graphiques": True,
            "captures": [{"chemin": "/fr/guides/dca-crypto", "titre": "DCA"}], "donnees": ""}
    base.update(kw)
    return base


def _valider(markdown, brut, **kw):
    demande = _demande(**kw)
    return m._valider_les_visuels(
        brut, markdown=markdown, demande=demande,
        capturables={p["chemin"]: p for p in demande["captures"]}, donnees=demande["donnees"])


def test_un_type_NON_COCHE_n_est_pas_produit() -> None:
    plan_v, notes = _valider("![a](visuel:1)", [{"id": 1, "type": "illustration",
                                                 "prompt": "a lever lifting a stone"}],
                             illustrations=0)
    assert plan_v == {} and any("non demandé" in n for n in notes)


def test_AU_PLUS_QUATRE_visuels_ajoutes_par_Noyaru() -> None:
    """Des types MÉLANGÉS : avec six illustrations, c'est le plafond du plan (4 aussi) qui
    arrêtait, et un plafond général décalé survivait (mutation du 02/11/2026)."""
    texte = "1 200 et 1 850\n" + "\n".join("![v](visuel:%d)" % i for i in range(1, 7))
    graphique = {"titre": "Visites", "libelles": ["Jan", "Fév"],
                 "series": [{"nom": "Visites", "valeurs": [1200, 1850]}]}
    brut = [{"id": 1, "type": "illustration", "prompt": "an editorial scene number one"},
            {"id": 2, "type": "capture", "page": "/fr/guides/dca-crypto/"},
            {"id": 3, "type": "graphique", "graphique": graphique},
            {"id": 4, "type": "capture", "page": "/fr/guides/dca-crypto/"},
            {"id": 5, "type": "graphique", "graphique": graphique},
            {"id": 6, "type": "illustration", "prompt": "an editorial scene number six"}]
    plan_v, notes = _valider(texte, brut)
    assert sorted(plan_v) == [1, 2, 3, 4], "les QUATRE premiers dans l'ordre de lecture"
    assert sum("au-delà de 4 visuels" in n for n in notes) == 2


def test_le_plafond_d_illustrations_est_celui_du_PLAN() -> None:
    texte = "![a](visuel:1)\n![b](visuel:2)"
    brut = [{"id": i, "type": "illustration", "prompt": "an editorial scene number %d" % i} for i in (1, 2)]
    plan_v, notes = _valider(texte, brut, illustrations=1)
    assert sorted(plan_v) == [1] and any("au-delà de 1 par article" in n for n in notes)


def test_une_CAPTURE_vise_une_page_de_la_liste_jamais_une_adresse_du_modele() -> None:
    texte = "![a](visuel:1)\n![b](visuel:2)"
    brut = [{"id": 1, "type": "capture", "page": "/fr/guides/dca-crypto/"},
            {"id": 2, "type": "capture", "page": "https://evil.example/admin"}]
    plan_v, notes = _valider(texte, brut)
    assert list(plan_v) == [1] and any("n'est pas une page proposée" in n for n in notes)


def test_un_chiffre_justifie_par_la_SEULE_legende_de_son_graphique_est_refuse() -> None:
    """On ne prouve pas un chiffre par le texte alternatif de son propre graphique."""
    texte = "Le trafic progresse.\n\n![Visites : 1 200 puis 1 850](visuel:1)"
    brut = [{"id": 1, "type": "graphique", "graphique": {
        "titre": "Visites", "libelles": ["Jan", "Fév"],
        "series": [{"nom": "Visites", "valeurs": [1200, 1850]}]}}]
    plan_v, notes = _valider(texte, brut)
    assert plan_v == {} and any("chiffres absents" in n for n in notes)


def test_une_image_du_client_NON_PLACEE_est_dite_et_reste_dans_l_archive() -> None:
    plan_v, notes = _valider("Rien.", [])
    assert plan_v == {} and any("votre image 1 n'a pas été placée" in n for n in notes)


def test_une_image_du_client_ne_se_place_qu_UNE_fois() -> None:
    plan_v, notes = _valider("![a](visuel:1)\n![b](visuel:2)",
                             [{"id": 1, "type": "import", "image": 1},
                              {"id": 2, "type": "import", "image": 1}])
    assert list(plan_v) == [1] and any("déjà placée" in n for n in notes)


# ── De bout en bout, par la route ─────────────────────────────────────────────────────────────

MD_FR = ("## Le levier\n\nEn janvier, 1 200 visites ; en février, 1 850.\n\n"
         "![Notre tableau de bord](visuel:1)\n\n![Un levier qui soulève une pierre](visuel:2)\n\n"
         "![Visites de janvier et février](visuel:3)\n\nVoir [DCA](/fr/guides/dca-crypto/).\n\n"
         "![La page DCA](visuel:4)\n")
VISUELS_FR = [
    {"id": 1, "type": "import", "image": 1},
    {"id": 2, "type": "illustration", "prompt": "A mechanical lever lifting a heavy stone"},
    {"id": 3, "type": "graphique", "graphique": {
        "type": "barres", "titre": "Visites", "libelles": ["Janvier", "Février"],
        "series": [{"nom": "Visites", "valeurs": [1200, 1850]}], "exemple": True}},
    {"id": 4, "type": "capture", "page": "/fr/guides/dca-crypto/"},
]
MD_DE = MD_FR.replace("## Le levier", "## Der Hebel").replace("/fr/guides/dca-crypto/",
                                                             "/de/guides/krypto-dca/")


@pytest.fixture()
def atelier(monkeypatch):
    """Le crawl, le modèle, le modèle d'image et le worker de captures — tous enregistrés."""
    vu: dict[str, list] = {"appels": [], "illustrations": [], "captures": []}
    monkeypatch.setattr(m, "_own_pages_for_project", lambda runs_dir, slug: (list(CRAWL), "20261102-0600"))

    def _ai(*, system, user_msg, **kw):
        langue = re.search(r"LANGUE DE L'ARTICLE : (\S+)", user_msg).group(1)
        vu["appels"].append({"langue": langue, "images": kw.get("images"), "demande": user_msg})
        if langue == "fr":
            return {"titre": "Le levier", "description": "d", "slug": "levier",
                    "markdown": MD_FR, "visuels": VISUELS_FR}
        return {"titre": "Der Hebel", "description": "d", "slug": "hebel", "markdown": MD_DE,
                "graphiques": {"3": {"titre": "Besuche", "libelles": ["Januar", "Februar"],
                                     "series": ["Besuche"]}}}

    def _image(*, prompt, taille="1536x1024"):
        vu["illustrations"].append(prompt)
        return _png(1536, 1024, "#aa6633")

    def _captures(urls, **kw):
        vu["captures"].append(list(urls))
        return {u: (visuels.preparer_image(_png(1280, 800))[0], 1280, 800) for u in urls}

    monkeypatch.setattr(m, "_correction_ai_json", _ai)
    monkeypatch.setattr(m, "_openai_generer_une_image", _image)
    monkeypatch.setattr(m, "_capturer_des_pages", _captures)
    return vu


def _csrf(client, slug) -> dict:
    client.get(f"/projects/{slug}")
    return {app_module._CSRF_HEADER_NAME: client.cookies.get(app_module._CSRF_COOKIE_NAME, "")}


def _importer(client, slug, octets=None, nom="photo.png"):
    return client.post(f"/api/projects/{slug}/content/images", headers=_csrf(client, slug),
                       files={"fichier": (nom, octets if octets is not None else _png(), "image/png")})


def _rediger(client, slug, **body):
    return client.post(f"/api/projects/{slug}/content/article", json=body, headers=_csrf(client, slug))


def test_un_article_et_ses_visuels_de_bout_en_bout(customer, plan, atelier) -> None:
    client, slug, _pid, _uid = customer
    plan["plan"] = "business"
    image = _importer(client, slug).json()
    assert image["ok"], image
    r = _rediger(client, slug, sujet="Le levier", section="/fr/guides", langues=["de"],
                 images=[{"id": image["id"], "legende": "notre tableau de bord"}],
                 illustrations=True, graphiques=True, captures=True)
    assert r.status_code == 200, r.text
    d = r.json()
    fr, de = d["versions"]

    # Le texte cite les vrais fichiers ; aucun marqueur ne reste.
    for nom in ("levier-1", "levier-2", "levier-3", "levier-4"):
        assert "](images/%s.webp)" % nom in fr["markdown"], nom
    assert "](images/levier-3-de.webp)" in de["markdown"], "le graphique allemand a ses libellés"
    assert "](images/levier-4-de.webp)" in de["markdown"], "la capture allemande montre la page allemande"
    assert "](images/levier-1.webp)" in de["markdown"] and "](images/levier-2.webp)" in de["markdown"]
    assert "visuel:" not in fr["markdown"] + de["markdown"]
    assert 'width="1600" height="900"' in fr["html"]

    # Le modèle a VU l'image du client — pour la version principale seulement.
    principal, traduction = atelier["appels"]
    assert principal["images"] and len(principal["images"]) == 1 and not traduction["images"]
    assert "notre tableau de bord" in principal["demande"]
    assert atelier["illustrations"][0].endswith("A mechanical lever lifting a heavy stone")
    assert atelier["captures"] == [[S + "/de/guides/krypto-dca/", S + "/fr/guides/dca-crypto/"]]

    # Inclus dans l'article : DEUX versions, deux articles, rien de plus.
    assert [x.get("amount") for x in plan["debits"]] == [2], plan["debits"]

    # L'archive : chaque version en .md et .html, et ses six images.
    archive = zipfile.ZipFile(io.BytesIO(client.get(d["zip"]).content))
    assert sorted(n for n in archive.namelist() if n.startswith("images/")) == [
        "images/levier-1.webp", "images/levier-2.webp", "images/levier-3-de.webp",
        "images/levier-3.webp", "images/levier-4-de.webp", "images/levier-4.webp"]
    assert {"levier.fr.md", "levier.fr.html", "hebel.de.md", "hebel.de.html"} <= set(archive.namelist())
    illustration = Image.open(io.BytesIO(archive.read("images/levier-2.webp")))
    assert b"trainedAlgorithmicMedia" in (illustration.info.get("xmp") or b"")

    # Relu depuis le journal : les mêmes images, sans rien coûter.
    lu = client.get(f"/api/projects/{slug}/content/article/{d['id']}").json()
    assert lu["versions"] == d["versions"] and lu["images"] == d["images"]


def test_les_illustrations_sont_REFUSEES_sous_Business_avant_tout_appel(customer, plan, atelier) -> None:
    client, slug, _pid, _uid = customer
    plan["plan"] = "pro"
    r = _rediger(client, slug, sujet="Le levier", section="/fr/guides", illustrations=True)
    assert r.status_code == 402 and "Business" in r.json()["error"], r.text
    assert atelier["appels"] == [] and plan["debits"] == []


def test_une_illustration_qui_ECHOUE_est_retiree_et_dite(customer, plan, atelier, monkeypatch) -> None:
    client, slug, _pid, _uid = customer
    plan["plan"] = "business"

    def _panne(**kw):
        raise RuntimeError("OpenAI images HTTP 500")

    monkeypatch.setattr(m, "_openai_generer_une_image", _panne)
    d = _rediger(client, slug, sujet="Le levier", section="/fr/guides", illustrations=True,
                 graphiques=True).json()
    fr = d["versions"][0]
    # Sans image importée, le visuel 1 tombe : l'illustration est la 1re retenue, le graphique
    # la 2e — les fichiers sont numérotés dans l'ordre de lecture de ce qui est retenu.
    assert "Un levier qui soulève" not in fr["markdown"] and "visuel:" not in fr["markdown"]
    assert any(n.startswith("illustration 1 : génération impossible") for n in fr["notes"]), fr["notes"]
    assert "![Visites de janvier et février](images/levier-2.webp)" in fr["markdown"], "le graphique, lui, est là"
    assert [x.get("amount") for x in plan["debits"]] == [1], "l'article est livré et débité"


def test_SANS_visuel_demande_le_modele_ne_recoit_ni_consigne_ni_image(customer, plan, atelier) -> None:
    client, slug, _pid, _uid = customer
    d = _rediger(client, slug, sujet="Le levier", section="/fr/guides").json()
    (appel,) = atelier["appels"]
    assert "VISUELS" not in appel["demande"] and not appel["images"]
    assert "visuel:" not in d["versions"][0]["markdown"] and d["images"] == []
    assert atelier["illustrations"] == [] and atelier["captures"] == []


def test_un_envoi_qui_n_est_pas_une_image_est_refuse_SANS_rien_ecrire(customer, plan) -> None:
    client, slug, _pid, _uid = customer
    r = _importer(client, slug, octets=b"<script>alert(1)</script>", nom="photo.png")
    assert r.status_code == 400 and "illisible" in r.json()["error"], r.text


def test_importer_une_image_demande_un_plan_qui_REDIGE(customer, plan) -> None:
    client, slug, _pid, _uid = customer
    plan["plan"] = "solo"
    assert _importer(client, slug).status_code == 402


def test_une_image_importee_ne_se_lit_que_depuis_SON_projet(customer, plan) -> None:
    client, slug, _pid, uid = customer
    image = _importer(client, slug).json()
    assert client.get(image["url"]).headers["content-type"] == "image/webp"
    autre = "autre-%s" % uuid.uuid4().hex[:8]
    with app_module.DB.session() as db:
        db.add(Project(owner_user_id=uid, slug=autre, site_name="autre", base_url="https://a.fr/",
                       settings={}))
        db.commit()
    assert client.get(f"/api/projects/{autre}/content/images/{image['id']}").status_code == 404


def test_une_image_d_article_n_est_servie_que_si_l_article_la_CITE(customer, plan, atelier) -> None:
    client, slug, _pid, _uid = customer
    image = _importer(client, slug).json()
    d = _rediger(client, slug, sujet="Le levier", section="/fr/guides", graphiques=True,
                 images=[{"id": image["id"], "legende": ""}]).json()
    base = f"/api/projects/{slug}/content/article/{d['id']}/images/"
    assert client.get(base + "levier-1.webp").status_code == 200
    for nom in ("levier-9.webp", "..%2F..%2Fsecret.webp", "levier-1.png"):
        assert client.get(base + nom).status_code == 404, nom


def test_une_image_importee_EXPIREE_refuse_l_article_avant_le_modele(customer, plan, atelier) -> None:
    client, slug, _pid, _uid = customer
    r = _rediger(client, slug, sujet="Le levier", section="/fr/guides",
                 images=[{"id": uuid.uuid4().hex, "legende": ""}])
    assert r.status_code == 400 and "importez-la à nouveau" in r.json()["error"]
    assert atelier["appels"] == [] and plan["debits"] == []


def test_l_ecran_montre_les_visuels_et_dit_ce_qu_il_faut_pour_les_ILLUSTRATIONS(customer, plan, monkeypatch) -> None:
    client, slug, _pid, _uid = customer
    monkeypatch.setattr(m, "_own_pages_for_project", lambda runs_dir, slug: (list(CRAWL), "ts"))
    page = client.get(f"/projects/{slug}/content").text
    assert 'id="c-v-fichiers"' in page and 'id="c-v-graphiques" checked' in page
    assert re.search(r'id="c-v-illustrations"\s+disabled', page), "Pro : visible mais inactive"
    assert "incluses à partir du plan Business" in page
    plan["plan"] = "business"
    page = client.get(f"/projects/{slug}/content").text
    assert not re.search(r'id="c-v-illustrations"\s+disabled', page)


# ── Les captures, par la file de travaux ─────────────────────────────────────────────────────

def _proprietaire() -> str:
    """Un travail sans propriétaire n'est pas écrit en base (clé étrangère) : il en faut un vrai."""
    m.DB.create_tables()
    with m.DB.session() as db:
        u = _brouillon.User(email="capture-%s@exemple.fr" % uuid.uuid4().hex[:8], password_hash="x",
                            is_admin=False)
        db.add(u)
        db.commit()
        db.refresh(u)
        return str(u.id)


def test_une_capture_passe_par_la_FILE_et_le_fil_qui_lui_est_reserve(monkeypatch) -> None:
    proprietaire = _proprietaire()
    monkeypatch.setattr(m, "_capturer_une_page", lambda url: _png(1280, 800))
    arret = threading.Event()

    def _fil() -> None:
        while not arret.is_set():
            jid = m._claim_next_job_id(worker_id="essai-captures", types=("capture",))
            if jid:
                m._execute_queued_job(jid)
            else:
                time.sleep(0.05)

    fil = threading.Thread(target=_fil, daemon=True)
    fil.start()
    try:
        out = m._capturer_des_pages(["https://site.fr/a/"], proprietaire=proprietaire, attente_s=20)
    finally:
        arret.set()
        fil.join(5)
    webp, largeur, hauteur = out["https://site.fr/a/"]
    assert (largeur, hauteur) == (1280, 800) and webp[:4] == b"RIFF"
    # Rendue, l'image quitte la table des travaux : elle n'a rien à y faire.
    (job,) = [j for j in m._list_jobs(limit=5, comptes=[proprietaire])]
    assert job.status == "done" and "image_b64" not in (job.result or {})
    assert not job.result.get("slug"), "sans projet : une capture n'est pas une analyse en cours"


def test_le_fil_des_captures_ne_prend_JAMAIS_un_crawl() -> None:
    proprietaire = _proprietaire()
    crawl = m.Job(id=str(uuid.uuid4()), status="queued", created_at=time.time() - 3600)
    crawl.result = {"type": "crawl", "slug": "x", "user_id": proprietaire}
    m._save_job(crawl)
    capture = m.Job(id=str(uuid.uuid4()), status="queued", created_at=time.time())
    capture.result = {"type": "capture", "url": "https://site.fr/", "user_id": proprietaire}
    m._save_job(capture)
    try:
        assert m._claim_next_job_id(worker_id="essai", types=("capture",)) == capture.id
        assert m._load_job(crawl.id).status == "queued"
    finally:
        for jid in (crawl.id, capture.id):
            job = m._load_job(jid)
            job.status = "canceled"
            m._save_job(job)


def test_une_capture_jamais_recue_est_ANNULEE_et_l_article_part_sans(monkeypatch) -> None:
    proprietaire = _proprietaire()
    out = m._capturer_des_pages(["https://site.fr/b/"], proprietaire=proprietaire, attente_s=0.2)
    assert out == {"https://site.fr/b/": None}
    (job,) = m._list_jobs(limit=5, comptes=[proprietaire])
    assert job.status == "canceled"


def test_la_page_des_ANALYSES_ne_montre_pas_les_captures(customer) -> None:
    client, _slug, _pid, uid = customer
    capture = m.Job(id=str(uuid.uuid4()), status="done", created_at=time.time())
    capture.result = {"type": "capture", "url": "https://site.fr/capture-cachee/", "user_id": uid}
    m._save_job(capture)
    page = client.get("/jobs").text
    assert capture.id not in page and "capture-cachee" not in page
