# -*- coding: utf-8 -*-
"""Sur un projet d'equipe, le PLAN et le QUOTA sont ceux du proprietaire du projet.

Troisieme pierre des comptes d'equipe, et la seule qui touche a l'argent. La decision du
proprietaire, prise apres avoir vu les trois options : c'est LE PROJET QUI DECIDE. Un consultant
au forfait Gratuit qui travaille sur un projet Business obtient le modele et le quota Business,
debites sur l'agence ; hors projet (l'assistant, la page facturation) c'est son propre compte,
pour qu'un consultant qui est lui-meme client garde son abonnement sur ses propres projets.

POURQUOI CE N'ETAIT PAS UN SIMPLE REMPLACEMENT. Un crawl se facture en DEUX temps : on reserve
les pages au depart, on ajuste a l'arrivee. Rerouter la reservation sans rerouter l'ajustement
aurait laisse une charge definitive chez l'agence et une consommation negative chez le
consultant — un desequilibre qu'aucun ecran n'aurait montre. Tout ce qui suit le depart lit
`job.result["user_id"]` : le dossier ou le crawl s'ECRIT, la recherche du projet en base dans le
worker, la reservation, l'ajustement. C'est donc le JOB qui porte le compte payeur, et les
quatre suivent d'un coup.

CE QUE VERROUILLE LE DERNIER TEST DU FICHIER, et c'est la lecon d'une garde precedente de cette
session : il n'essaie pas de RECONNAITRE une forme correcte, il ENUMERE. Il parcourt l'arbre
syntaxique de `app.py`, releve chaque appel de facturation situe dans une route qui recoit un
`slug`, et exige que le compte vise soit resolu par le projet. Une route ajoutee demain qui
debiterait la personne connectee fera echouer ce test en nommant sa ligne.
"""

from __future__ import annotations

import ast
import os
import sys
import tempfile
import uuid
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-payeur-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as m  # noqa: E402
from backend import auth, billing  # noqa: E402
from backend.models import AccountMember, BillingSubscription, Project, User  # noqa: E402

APP_PY = Path(m.__file__).resolve()

# Les fonctions de `billing` qui decident d'un plan ou touchent a un quota. Toute autre est
# hors sujet ici.
FACTURATION = {
    "effective_plan_key", "remaining_quota", "usage_add", "usage_sum",
    "ensure_within_quota", "crawl_config_for_plan", "correction_config_for_plan",
}

# Les plafonds de plan ecrits dans `app.py` plutot que dans `billing`. Ils lisent le plan par
# un detour, mais ils decident de la meme chose : ce que le compte a le droit de faire. Les
# oublier aurait laisse un membre au forfait Gratuit devant une porte fermee sur un projet
# dont le proprietaire paie l'acces.
PLAFONDS_LOCAUX = {"_opp_has_access", "_competitor_has_access"}

# Les fonctions a `slug` que l'auditeur releve mais qui visent deja le bon compte par un autre
# chemin. Chacune est couverte par un test de COMPORTEMENT plus haut dans ce fichier : la
# dispense ne porte que sur la forme de l'appel, jamais sur ce qu'il fait.
EXCEPTIONS: dict[str, str] = {
    "_correction_charge":
        "meme resolution que la porte, volontairement par le meme chemin ; meme test",
    "_run_crawl_job":
        "tourne dans le worker, hors requete : son parametre `user_id` EST le compte payeur, "
        "pose par la route au depart ; teste par test_le_job_de_crawl_porte_le_compte_proprietaire",
}


def _utilisateur(prefixe: str, plan: str = "") -> str:
    m.DB.create_tables()
    with m.DB.session() as db:
        u = User(email="%s-%s@exemple.fr" % (prefixe, uuid.uuid4().hex[:8]),
                 password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(u)
        db.commit()
        db.refresh(u)
        uid = str(u.id)
    if plan:
        # L'abonnement est ecrit directement : `upsert_subscription` prend une charge utile
        # Stripe, et fabriquer une fausse charge utile ferait porter au test la forme d'une
        # API externe qu'il ne cherche pas a verifier.
        with m.DB.session() as db:
            db.add(BillingSubscription(
                user_id=uid, plan_key=plan, status="active",
                stripe_customer_id="cus_%s" % uuid.uuid4().hex[:10],
                stripe_subscription_id="sub_%s" % uuid.uuid4().hex[:10],
                stripe_price_id="price_%s" % uuid.uuid4().hex[:10]))
            db.commit()
        with m.DB.session() as db:
            obtenu = billing.effective_plan_key(db, user_id=uid)
        assert obtenu == plan, "le forfait pose n'est pas celui que lit l'application (%r)" % obtenu
    return uid


def _projet(owner_id: str, slug: str | None = None) -> str:
    s = slug or "site-%s" % uuid.uuid4().hex[:8]
    with m.DB.session() as db:
        db.add(Project(owner_user_id=owner_id, slug=s, site_name=s, base_url="https://%s.fr/" % s))
        db.commit()
    return s


def _rejoint(owner_id: str, member_id: str) -> None:
    with m.DB.session() as db:
        db.add(AccountMember(owner_user_id=owner_id, member_user_id=member_id))
        db.commit()


class _Personne:
    def __init__(self, uid: str, is_admin: bool = False) -> None:
        self.id = uid
        self.is_admin = is_admin


# --- qui paie -----------------------------------------------------------------------------

def test_le_payeur_est_le_PROPRIETAIRE_du_projet() -> None:
    patron, consultant = _utilisateur("agence"), _utilisateur("consultant")
    slug = _projet(patron)
    _rejoint(patron, consultant)
    assert m._compte_payeur(consultant, slug) == patron


def test_sur_SON_projet_le_consultant_paie_lui_meme() -> None:
    """La moitie de la decision qu'on oublierait sans ce test : il reste client chez lui."""
    patron, consultant = _utilisateur("agence"), _utilisateur("consultant")
    _rejoint(patron, consultant)
    sien = _projet(consultant)
    assert m._compte_payeur(consultant, sien) == consultant


def test_un_projet_hors_de_portee_ne_revele_pas_son_proprietaire() -> None:
    """Le repli ne doit pas servir de sonde : un slug devine renverrait sinon l'identifiant
    du compte qui le possede."""
    etranger = _utilisateur("etranger")
    slug = _projet(_utilisateur("ailleurs"))
    assert m._compte_payeur(etranger, slug) == etranger


# --- ce que ca change pour le moteur de correction ------------------------------------------

def test_le_consultant_gratuit_obtient_le_moteur_du_projet() -> None:
    """Sans ca le partage serait decoratif : un membre Gratuit ne pourrait rien corriger.

    Le plan Gratuit plafonne a deux fichiers ; le plan du projet doit l'emporter, sinon un
    consultant invite sur un compte Business travaillerait avec le moteur d'essai.
    """
    patron = _utilisateur("agence", plan="business")
    consultant = _utilisateur("consultant")
    slug = _projet(patron)
    _rejoint(patron, consultant)

    seul = m._plan_correction_cfg(_Personne(consultant))
    partage = m._plan_correction_cfg(_Personne(consultant), compte=m._compte_payeur(consultant, slug))
    assert seul["plan"] == "free"
    assert partage["plan"] == "business", partage["plan"]
    assert partage["max_files"] > seul["max_files"]


def test_la_porte_et_le_debit_visent_le_MEME_compte() -> None:
    """Le desequilibre qui ne se verrait sur aucun ecran.

    Autoriser sur le solde d'un compte puis debiter l'autre laisse le second filer en negatif
    sans qu'aucune porte ne se ferme. Les deux fonctions prennent donc le SLUG et resolvent le
    compte par le meme chemin, ce qui rend l'ecart impossible a produire depuis un appelant.
    """
    patron = _utilisateur("agence", plan="business")
    consultant = _utilisateur("consultant")
    slug = _projet(patron)
    _rejoint(patron, consultant)

    with m.DB.session() as db:
        avant_agence = billing.usage_sum(db, user_id=patron, metric="ai_corrections_month")
        avant_membre = billing.usage_sum(db, user_id=consultant, metric="ai_corrections_month")

    attendu = m._plan_correction_cfg(_Personne(consultant), compte=patron)
    ouvert, _msg, plafond, modele = m._correction_gate(_Personne(consultant), slug=slug)
    assert ouvert and plafond > 0, "le membre est refuse sur un projet Business"
    # Le plafond est plafonne par le SOLDE restant. Lire le plan chez l'agence mais le solde
    # chez le membre donnerait ici le quota Gratuit (deux) au lieu du plafond Business : c'est
    # exactement l'ecart que le test doit voir, et il ne se verrait sur aucun ecran.
    assert plafond == int(attendu["max_files"]), (
        "le plafond vaut %d ; le plan vient d'un compte et le solde d'un autre" % plafond)
    assert modele == attendu["model"]
    m._correction_charge(_Personne(consultant), 3, slug=slug)

    with m.DB.session() as db:
        apres_agence = billing.usage_sum(db, user_id=patron, metric="ai_corrections_month")
        apres_membre = billing.usage_sum(db, user_id=consultant, metric="ai_corrections_month")
    assert apres_agence - avant_agence == 3, "l'agence n'a pas ete debitee"
    assert apres_membre == avant_membre, "le membre a ete debite en plus"


def test_hors_projet_chacun_reste_sur_son_compte() -> None:
    """L'autre moitie de la decision : sans slug, rien ne bouge vers le compte hote."""
    patron = _utilisateur("agence", plan="business")
    consultant = _utilisateur("consultant")
    _rejoint(patron, consultant)
    with m.DB.session() as db:
        avant = billing.usage_sum(db, user_id=patron, metric="ai_corrections_month")
    m._correction_charge(_Personne(consultant), 2)
    with m.DB.session() as db:
        assert billing.usage_sum(db, user_id=patron, metric="ai_corrections_month") == avant
        assert billing.usage_sum(db, user_id=consultant, metric="ai_corrections_month") >= 2


# --- le crawl, facture en deux temps ----------------------------------------------------------

def test_le_job_de_crawl_porte_le_compte_proprietaire() -> None:
    """Une ligne qui decide de quatre choses, d'ou un test qui les verifie ensemble.

    Tout ce qui suit le depart d'un crawl lit `job.result["user_id"]` : le dossier ou le crawl
    s'ECRIT, la recherche du projet en base dans le worker, la reservation des pages, et son
    ajustement a l'arrivee. Si le job portait le membre, la reservation tomberait sur un compte
    et le remboursement sur l'autre — l'agence garderait une charge definitive et le consultant
    partirait en consommation negative, sans qu'aucun ecran ne le montre.

    `started_by` garde qui a clique : sans lui, l'information serait detruite par ce changement.
    """
    import re

    from fastapi.testclient import TestClient

    from backend.app import app

    patron = _utilisateur("agence", plan="business")
    consultant = _utilisateur("consultant")
    slug = _projet(patron)
    _rejoint(patron, consultant)

    client = TestClient(app)
    client.cookies.set(auth.SESSION_COOKIE_NAME,
                       auth.make_session_token(user_id=consultant,
                                               secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    page = client.get("/projects/%s" % slug)
    assert page.status_code == 200, page.status_code
    jeton = re.search(r'name="_csrf"\s+value="([^"]*)"', page.text)
    assert jeton, "pas de jeton CSRF sur la page projet"

    with m.DB.session() as db:
        avant_agence = billing.usage_sum(db, user_id=patron, metric="pages_crawled_month")
        avant_membre = billing.usage_sum(db, user_id=consultant, metric="pages_crawled_month")

    rep = client.post("/projects/%s/crawl" % slug, data={"_csrf": jeton.group(1)},
                      follow_redirects=False)
    assert rep.status_code in (200, 303), rep.status_code

    jobs = [j for j in m._list_jobs(limit=50)
            if isinstance(j.result, dict) and j.result.get("slug") == slug]
    assert jobs, "aucun job de crawl cree"
    resultat = jobs[0].result
    assert resultat.get("user_id") == patron, "le job est au nom du membre, pas du proprietaire"
    assert resultat.get("started_by") == consultant, "on ne sait plus qui a lance le crawl"

    with m.DB.session() as db:
        apres_agence = billing.usage_sum(db, user_id=patron, metric="pages_crawled_month")
        apres_membre = billing.usage_sum(db, user_id=consultant, metric="pages_crawled_month")
    assert apres_agence > avant_agence, "les pages reservees n'ont pas ete debitees a l'agence"
    assert apres_membre == avant_membre, "le membre a ete debite en plus"


# --- la garde qui enumere -------------------------------------------------------------------

def _appels_de_facturation_sans_exceptions() -> list[tuple[int, str, str, str]]:
    """Le meme relevé, dispenses COMPRISES — pour vérifier qu'elles servent encore."""
    return _appels_de_facturation_dans_les_routes_a_slug(dispenser=False)


def _appels_de_facturation_dans_les_routes_a_slug(
        *, dispenser: bool = True) -> list[tuple[int, str, str, str]]:
    """(ligne, route, fonction de billing, source de l'argument user_id) pour chaque appel."""
    texte = APP_PY.read_text(encoding="utf-8")
    arbre = ast.parse(texte)

    portees: list[tuple[int, int, str, bool]] = []
    for n in ast.walk(arbre):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
            a = n.args
            noms = [x.arg for x in list(a.posonlyargs) + list(a.args) + list(a.kwonlyargs)]
            portees.append((n.lineno, n.end_lineno or n.lineno, n.name, "slug" in noms))

    trouves: list[tuple[int, str, str, str]] = []
    for n in ast.walk(arbre):
        if not isinstance(n, ast.Call):
            continue
        f = n.func
        est_billing = (isinstance(f, ast.Attribute) and f.attr in FACTURATION
                       and isinstance(f.value, ast.Name) and f.value.id == "billing")
        est_plafond = isinstance(f, ast.Name) and f.id in PLAFONDS_LOCAUX
        if not (est_billing or est_plafond):
            continue
        appele = f.attr if est_billing else f.id
        englobantes = [p for p in portees if p[0] <= n.lineno <= p[1]]
        if not englobantes:
            continue
        _d, _f2, route, a_slug = max(englobantes, key=lambda p: p[0])
        if not a_slug or (dispenser and route in EXCEPTIONS):
            continue
        arg = None
        for kw in n.keywords:
            if kw.arg == "user_id":
                arg = kw.value
                break
        if arg is None:
            # `crawl_config_for_plan(effective_plan_key(...))` : l'appel externe ne porte pas
            # de user_id, l'interne si. On ne le compte pas deux fois.
            continue
        trouves.append((n.lineno, route, appele, ast.unparse(arg)))
    return trouves


def test_AUCUNE_route_projet_ne_facture_la_personne_connectee() -> None:
    """La garde ENUMERE au lieu de reconnaitre une forme.

    Une garde ecrite plus tot dans ce chantier cherchait un motif correct et laissait donc
    passer tout ce qui ne lui ressemblait pas. Celle-ci fait l'inverse : elle releve CHAQUE
    appel de facturation d'une route a `slug` et exige que le compte vienne du projet. Une
    route ajoutee demain qui debiterait la personne connectee fera echouer ce test en nommant
    sa ligne et son nom.
    """
    appels = _appels_de_facturation_dans_les_routes_a_slug()
    assert appels, "l'auditeur ne trouve plus rien : il ne mesure plus ce qu'il croit mesurer"

    acceptes = {"payeur", "compte"}
    fautifs = [(l, r, fn, src) for (l, r, fn, src) in appels
               if src not in acceptes and not src.startswith("_compte_payeur(")]
    assert not fautifs, "ces appels facturent la personne connectee sur un projet :\n" + "\n".join(
        "  app.py:%d  %s -> %s(user_id=%s)" % (l, r, fn, src) for l, r, fn, src in fautifs)


def test_aucune_dispense_ne_SURVIT_a_ce_qu_elle_dispensait() -> None:
    """Une dispense morte est pire qu'une dispense de trop : elle se lit comme une permission.

    `_correction_gate` figurait ici parce qu'il appelait `remaining_quota`. Le calcul est parti
    dans `_plafond_de_correction` le 19/09/2026 et la ligne serait restee — disant a qui la lit
    que la porte a le droit de facturer la mauvaise personne, alors qu'elle ne facture plus
    rien. Ce test fait tomber les dispenses en meme temps que leur raison d'etre.
    """
    vivantes = {r for (_l, r, _fn, _src) in _appels_de_facturation_sans_exceptions()}
    mortes = sorted(set(EXCEPTIONS) - vivantes)
    assert not mortes, (
        "ces fonctions n'appellent plus rien de facturable : retire leur dispense au lieu de "
        "la laisser se lire comme une permission :\n  " + "\n  ".join(mortes))
