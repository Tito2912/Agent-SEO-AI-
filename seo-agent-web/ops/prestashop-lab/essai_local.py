"""Le connecteur PrestaShop contre une vraie boutique locale (premier essai : 8.2.8 bilingue, 02/11/2026, 25/25).

Préparer la boutique : voir README.md (PHP, MariaDB et PrestaShop portables, aucune installation
système). Puis :  PRESTASHOP_LAB=<dossier du laboratoire> python ops/prestashop-lab/essai_local.py
Seul le transport est redirigé : la boutique se présente au connecteur comme https://localhost ;
les requêtes partent en clair vers 127.0.0.1:8080. Ce script MODIFIE la boutique de laboratoire
(droits de la clé, produit 19 désactivé puis réactivé, valeurs SEO anglaises du produit 1 et de la
catégorie 3) : jamais contre une vraie boutique."""
import html
import http.client
import os
import re
import subprocess
import sys
import urllib.request
from pathlib import Path

RACINE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RACINE / "backend"))
import prestashop_cms as ps  # noqa: E402
from cms_commun import CmsError  # noqa: E402

ICI = Path(os.environ["PRESTASHOP_LAB"])
CLE = (ICI / "cle_ws.txt").read_text().strip()
MARIADB = ICI / "mariadb-11.4.13-winx64" / "bin" / "mariadb.exe"
LOCAL, PUBLIC = "http://localhost:8080", "https://localhost"


def sql(requete):
    return subprocess.run([str(MARIADB), "-h127.0.0.1", "-P3307", "-uroot", "prestashop", "-N", "-B", "-e", requete],
                          capture_output=True, text=True, encoding="utf-8", check=True).stdout.strip()


class _Reponse:
    def __init__(self, brute):
        self.brute, self.status = brute, brute.status

    def read(self, limite):
        return self.brute.read(limite).replace(LOCAL.encode(), PUBLIC.encode())

    def getheader(self, nom, defaut=""):
        return self.brute.getheader(nom, defaut)


class Local:
    def __init__(self, host, address):
        assert host == "localhost" and address == "127.0.0.1"
        self.c = http.client.HTTPConnection("127.0.0.1", 8080, timeout=180)

    def request(self, method, path, body=None, headers=None):
        self.c.request(method, path, body=body, headers={**headers, "Host": "localhost:8080"})

    def getresponse(self):
        return _Reponse(self.c.getresponse())

    def close(self):
        self.c.close()


ps.public_address = lambda host: "127.0.0.1"
ps._PinnedHTTPS = Local


class Projet:
    base_url = PUBLIC + "/"


def client(cle=CLE):
    return ps.PrestaShopClient(ps.identifiants({"site_url": PUBLIC + "/api/", "cle": cle}, Projet()))


def en_ligne(url):
    with urllib.request.urlopen(url.replace(PUBLIC, LOCAL), timeout=180) as reponse:
        texte = reponse.read().decode("utf-8", "replace")
    titre = re.search(r"<title>(.*?)</title>", texte, re.S)
    desc = re.search(r'<meta name="description" content="([^"]*)"', texte)
    return html.unescape(titre.group(1).strip()) if titre else None, html.unescape(desc.group(1)) if desc else ""


resultats = []


def constat(nom, ok, detail=""):
    resultats.append(ok)
    print(("OK    " if ok else "ÉCHEC ") + nom + (f" — {detail}" if detail else ""))


def refus(nom, action, code=None):
    try:
        action()
        constat(nom, False, "accepté")
    except CmsError as erreur:
        constat(nom, code is None or erreur.code == code, f"{erreur.code or '-'} : {erreur}"[:150])


c = client()
constat("connexion (version 8, droits, langues)", c.check() == "Atelier Presta")
refus("une clé fausse est refusée", lambda: client("FAUSSE" + "0" * 26).check(), "acces_refuse")
sql("DELETE p FROM ps_webservice_permission p JOIN ps_webservice_account a USING (id_webservice_account) "
    "WHERE a.description='Noyaru' AND p.resource='categories' AND p.method='PATCH'")
refus("une clé sans PATCH sur les catégories est refusée", lambda: client().check(), "droits")
sql("INSERT INTO ps_webservice_permission (resource, method, id_webservice_account) SELECT 'categories', 'PATCH', "
    "id_webservice_account FROM ps_webservice_account WHERE description='Noyaru'")
constat("langues lues", c.langues() == {"fr": 1, "en": 3}, str(c.langues()))

PRODUIT_FR = PUBLIC + "/fr/hommes/1-hummingbird-printed-t-shirt.html"
DECLINAISON = PUBLIC + "/fr/hommes/1-1-hummingbird-printed-t-shirt.html"
PRODUIT_EN = PUBLIC + "/en/men/1-hummingbird-printed-t-shirt.html"
CATEGORIE = PUBLIC + "/fr/3-vetements"
for url, attendu in ((PRODUIT_FR, "products:1:1"), (DECLINAISON, "products:1:1"), (CATEGORIE, "categories:3:1")):
    constat(f"lecture {url[len(PUBLIC):]}", c.read(url)["post_id"] == attendu)
lien_en = [u for u in re.findall(r'href="(http://localhost:8080/en/[^"#]+-hummingbird-printed-t-shirt\.html)',
                                 urllib.request.urlopen(LOCAL + "/en/", timeout=180).read().decode("utf-8", "replace"))]
if lien_en:
    PRODUIT_EN = lien_en[0].replace(LOCAL, PUBLIC)
    constat(f"lecture anglaise {PRODUIT_EN[len(PUBLIC):]}", c.read(PRODUIT_EN)["post_id"] == "products:1:3")
refus("une page CMS n'est pas modifiable", lambda: c.read(PUBLIC + "/fr/content/4-a-propos"))
refus("une adresse avec paramètres non plus", lambda: c.read(CATEGORIE + "?page=2"))
refus("une adresse inventée non plus", lambda: c.read(PUBLIC + "/fr/hommes/1-un-autre-produit.html"))
sql("UPDATE ps_product_shop SET active=0 WHERE id_product=19")
sql("UPDATE ps_product SET active=0 WHERE id_product=19")
lien19 = sql("SELECT link_rewrite FROM ps_product_lang WHERE id_product=19 AND id_lang=1")
refus("un produit désactivé non plus", lambda: c.read(PUBLIC + f"/fr/art/19-{lien19}.html"))
sql("UPDATE ps_product_shop SET active=1 WHERE id_product=19")
sql("UPDATE ps_product SET active=1 WHERE id_product=19")

for url in (PRODUIT_FR, CATEGORIE):
    page = c.read(url)
    titre, desc = en_ligne(url)
    constat(f"titre affiché {url[len(PUBLIC):]}", page["seo_title_rendered"] == titre,
            f"supposé {page['seo_title_rendered']!r}, en ligne {titre!r}")
    # Comme `cms_routes._same_text` : espaces normalisés (PrestaShop laisse une espace finale).
    constat(f"description affichée {url[len(PUBLIC):]}", page["meta_description_rendered"] == " ".join(desc.split()),
            f"supposée {page['meta_description_rendered'][:60]!r}, en ligne {desc[:60]!r}")


def anglais(table, cle, ident):
    return sql(f"SELECT IFNULL(meta_title,''), IFNULL(meta_description,'') FROM {table} WHERE {cle}={ident} AND id_lang=3")


sql("UPDATE ps_product_lang SET meta_title='EN title 1', meta_description='EN description 1' WHERE id_product=1 AND id_lang=3")
sql("UPDATE ps_category_lang SET meta_title='EN title 3', meta_description='EN description 3' WHERE id_category=3 AND id_lang=3")
en_produit, en_categorie = anglais("ps_product_lang", "id_product", 1), anglais("ps_category_lang", "id_category", 3)

avant = c.read(PRODUIT_FR)
ESSAI = "T-shirt imprimé colibri en coton pima — Atelier Presta"
ecrit = c.update(avant, {"seo_title": ESSAI})
constat("écriture du titre SEO d'un produit, relue", ecrit["seo_title"] == ESSAI and c.read(PRODUIT_FR)["seo_title"] == ESSAI)
constat("  l'anglais du produit est intact", anglais("ps_product_lang", "id_product", 1) == en_produit)
constat("  le site en ligne l'affiche", en_ligne(PRODUIT_FR)[0] == ESSAI, repr(en_ligne(PRODUIT_FR)[0]))
DESC = "Vêtements pour hommes et femmes : t-shirts, pulls & sweats imprimés, en coton bio."
cat = c.update(c.read(CATEGORIE), {"meta_description": DESC})
constat("écriture de la méta-description d'une catégorie", cat["meta_description"] == DESC and en_ligne(CATEGORIE)[1] == DESC,
        repr(en_ligne(CATEGORIE)[1]))
constat("  l'anglais de la catégorie est intact", anglais("ps_category_lang", "id_category", 3) == en_categorie)
refus("limite de 255 caractères tenue avant l'envoi", lambda: c.update(c.read(PRODUIT_FR), {"seo_title": "x" * 256}))
remis = c.update(c.read(PRODUIT_FR), {"seo_title": avant["seo_title"]})
remis_cat = c.update(c.read(CATEGORIE), {"meta_description": None})
constat("rétablissement (produit et catégorie)", remis["seo_title"] is None and remis_cat["meta_description"] is None)
constat("  le site en ligne revient au nom du produit", en_ligne(PRODUIT_FR)[0] == avant["seo_title_rendered"])
constat("  l'anglais est toujours intact", anglais("ps_product_lang", "id_product", 1) == en_produit
        and anglais("ps_category_lang", "id_category", 3) == en_categorie)
print(f"\n{sum(resultats)}/{len(resultats)} constats réussis")
sys.exit(0 if all(resultats) else 1)
