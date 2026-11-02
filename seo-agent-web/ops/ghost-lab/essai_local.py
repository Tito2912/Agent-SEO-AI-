"""Le connecteur Ghost contre un VRAI Ghost local (premier essai : Ghost 6.73, 02/11/2026, 17/17).

Installer Ghost (Node 22, aucun compte, base SQLite), dans un dossier vide hors du dépôt :
    npx ghost-cli@latest install local --no-start --no-prompt
    npx ghost-cli@latest start
puis :  python ops/ghost-lab/essai_local.py
1. Prépare le Ghost local : compte propriétaire, intégration personnalisée « Noyaru » (sa clé Admin
   API), un article et une page publiés.
2. Joue le connecteur `ghost_cms` tel quel. Seul le transport est redirigé : le site se présente au
   connecteur comme https://localhost (le connecteur refuse par principe une adresse privée et le
   HTTP) ; les requêtes partent en clair vers 127.0.0.1:2368 et les adresses des réponses sont
   réécrites dans l'autre sens. Jeton, chemins, en-têtes, corps et réponses sont ceux de Ghost.
"""
import http.client
import http.cookiejar
import json
import re
import sys
import html
import urllib.parse
import urllib.request
from pathlib import Path

RACINE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RACINE / "backend"))
import ghost_cms as gh  # noqa: E402
from cms_commun import CmsError  # noqa: E402

LOCAL = "http://localhost:2368"
PUBLIC = "https://localhost"
ORIGINE = {"Origin": LOCAL, "Content-Type": "application/json", "Accept-Version": "v5.0"}

# --- 1. Préparation (API de session de Ghost, comme l'écran d'installation) -------------------
jar = http.cookiejar.CookieJar()
navigateur = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))


def appel(methode, chemin, corps=None):
    requete = urllib.request.Request(LOCAL + "/ghost/api/admin/" + chemin, method=methode, headers=ORIGINE,
                                     data=json.dumps(corps).encode() if corps is not None else None)
    try:
        with navigateur.open(requete, timeout=60) as reponse:
            texte = reponse.read().decode()
            try:
                return reponse.status, json.loads(texte) if texte else {}
            except ValueError:
                return reponse.status, texte
    except urllib.error.HTTPError as erreur:
        return erreur.code, erreur.read().decode()[:300]


MOT = "Essai-Noyaru-2026-local!"
statut, etat = appel("GET", "authentication/setup/")
if statut == 200 and not etat["setup"][0]["status"]:
    print("installation :", appel("POST", "authentication/setup/", {"setup": [{
        "name": "Essai Noyaru", "email": "essai@noyaru.invalid", "password": MOT, "blogTitle": "Atelier Ghost"}]})[0])
print("session :", appel("POST", "session/", {"username": "essai@noyaru.invalid", "password": MOT})[0])

statut, integrations = appel("GET", "integrations/?include=api_keys&limit=all")
noyaru = next((i for i in integrations.get("integrations", []) if i["name"] == "Noyaru"), None)
if noyaru is None:
    statut, cree = appel("POST", "integrations/?include=api_keys", {"integrations": [{"name": "Noyaru"}]})
    noyaru = cree["integrations"][0]
admin = next(k for k in noyaru["api_keys"] if k["type"] == "admin")
# L'API rend la clé d'administration ENTIÈRE, « id:secret », comme l'écran de Ghost l'affiche.
CLE = admin["secret"]
print("intégration Noyaru, clé Admin API de forme valide :", bool(gh.CLE.fullmatch(CLE)))


def publie(genre, titre, extrait=None):
    statut, liste = appel("GET", f"{genre}/?" + urllib.parse.urlencode({"filter": f"title:'{titre}'", "limit": 1}))
    if isinstance(liste, dict) and liste.get(genre):
        return liste[genre][0]
    corps = {"title": titre, "status": "published", **({"custom_excerpt": extrait} if extrait else {})}
    statut, cree = appel("POST", f"{genre}/", {genre: [corps]})
    return cree[genre][0]


article = publie("posts", "Notre service de réparation", "Réparation de vélos à Lyon, en atelier.")
page_a_propos = publie("pages", "À propos de l’atelier")
print("article :", article["url"], "| page :", page_a_propos["url"])


# --- 2. Le connecteur, transport redirigé ------------------------------------------------------
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
        self.c = http.client.HTTPConnection("127.0.0.1", 2368, timeout=30)

    def request(self, method, path, body=None, headers=None):
        self.c.request(method, path, body=body, headers={**headers, "Host": "localhost:2368"})

    def getresponse(self):
        return _Reponse(self.c.getresponse())

    def close(self):
        self.c.close()


gh.public_address = lambda host: "127.0.0.1"
gh._PinnedHTTPS = Local


class Projet:
    base_url = PUBLIC + "/"


def public(url):
    return url.replace(LOCAL, PUBLIC)


client = gh.GhostClient(gh.identifiants({"site_url": PUBLIC + "/ghost/", "cle": CLE}, Projet()))
resultats = []


def constat(nom, ok, detail=""):
    resultats.append(ok)
    print(("OK    " if ok else "ÉCHEC ") + nom + (f" — {detail}" if detail else ""))


constat("connexion (site/ + lecture authentifiée)", client.check() == "Atelier Ghost")
try:
    gh.GhostClient(gh.identifiants({"site_url": PUBLIC, "cle": CLE[:-4] + "0000"}, Projet())).check()
    constat("une clé fausse est refusée", False)
except CmsError as erreur:
    constat("une clé fausse est refusée", erreur.code == "acces_refuse", erreur.code)


def en_ligne(url):
    with urllib.request.urlopen(url, timeout=30) as reponse:
        texte = reponse.read().decode("utf-8", "replace")
    titre = re.search(r"<title>(.*?)</title>", texte, re.S)
    desc = re.search(r'<meta name="description" content="([^"]*)"', texte)
    return html.unescape(titre.group(1).strip()) if titre else None, html.unescape(desc.group(1)) if desc else ""


for genre, publication in (("posts", article), ("pages", page_a_propos)):
    page = client.read(public(publication["url"]))
    constat(f"lecture d'un(e) {genre} par son adresse", page["post_id"] == f"{genre}:{publication['id']}")
    titre, desc = en_ligne(publication["url"])
    constat(f"  titre affiché sans titre SEO ({genre})", page["seo_title_rendered"] == titre,
            f"supposé {page['seo_title_rendered']!r}, en ligne {titre!r}")
    constat(f"  description affichée ({genre})", page["meta_description_rendered"] == desc,
            f"supposée {page['meta_description_rendered']!r}, en ligne {desc!r}")

url = public(article["url"])
avant = client.read(url)
ESSAI = "Réparation de vélos à Lyon — Atelier (essai Noyaru)"
ecrit = client.update(avant, {"seo_title": ESSAI})
constat("écriture du titre SEO, relue", ecrit["seo_title"] == ESSAI and client.read(url)["seo_title"] == ESSAI)
titre, _ = en_ligne(article["url"])
constat("  le site en ligne l'affiche", titre == ESSAI, repr(titre))
ecrit = client.update(client.read(url), {"meta_description": "Atelier de réparation de vélos à Lyon : diagnostic, entretien et pièces."})
_, desc = en_ligne(article["url"])
constat("écriture de la méta-description, en ligne", desc == ecrit["meta_description"], repr(desc))
# Ce qui protège vraiment : une modification faite dans Ghost après l'aperçu se VOIT à la relecture
# (le parcours compare les valeurs relues à celles de l'aperçu et s'arrête). Ghost, lui, ne refuse
# pas un updated_at périmé quand seuls les champs SEO changent (table posts_meta).
apercu = client.read(url)
statut, _ = appel("PUT", f"posts/{article['id']}/", {"posts": [{"meta_title": "Changé dans Ghost",
                                                               "updated_at": apercu["updated_at"]}]})
relu = client.read(url)
constat("une modification faite dans Ghost se voit à la relecture",
        statut == 200 and relu["seo_title"] == "Changé dans Ghost" and gh.values(relu) != gh.values(apercu))
statut, reponse = appel("PUT", f"posts/{article['id']}/", {"posts": [{"meta_title": "Sans updated_at"}]})
constat("Ghost exige updated_at", statut >= 400 and client.read(url)["seo_title"] == "Changé dans Ghost",
        f"HTTP {statut}")
try:
    client.update(client.read(url), {"seo_title": "x" * 301})
    constat("limite de 300 caractères tenue", False)
except CmsError as erreur:
    constat("limite de 300 caractères tenue avant l'envoi", not erreur.uncertain)
remis = client.update(client.read(url), {"seo_title": None, "meta_description": None})
constat("rétablissement (titre et description vidés)", remis["seo_title"] is None and remis["meta_description"] is None)
titre, _ = en_ligne(article["url"])
constat("  le site en ligne revient au titre de l'article", titre == avant["seo_title_rendered"], repr(titre))
try:
    client.read(PUBLIC + "/")
    constat("l'accueil n'est pas modifiable article par article", False)
except CmsError:
    constat("l'accueil n'est pas modifiable article par article", True)
print(f"\n{sum(resultats)}/{len(resultats)} constats réussis")
sys.exit(0 if all(resultats) else 1)
