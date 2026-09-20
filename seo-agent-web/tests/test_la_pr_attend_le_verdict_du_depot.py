# -*- coding: utf-8 -*-
"""Une correction n'est proposee au client qu'une fois que SON dépôt a dit que le build passe.

Seconde moitie du point 1 de l'audit. Le declencheur est un fait, pas une crainte : une pull
request de correction a deja atteint `headers()` dans un export statique et casse le build d'un
client.

POURQUOI PAS UN BUILD CHEZ NOUS, et c'est une mesure : un `npm ci` suivi d'un build Next.js
demande couramment un a deux gigaoctets, sur un worker de deux gigaoctets qui fait deja tourner
Chromium et n'a aucun disque persistant — donc aucun cache entre deux builds. On lit a la place
les verifications que le depot fait DEJA tourner. C'est gratuit, et c'est plus juste que tout ce
qu'on pourrait reproduire : c'est leur vraie chaine de build, avec leurs versions.

DEUX SOURCES, ET IL FAUT LES DEUX. Les « check runs » portent GitHub Actions et la plupart des
applications de CI ; les « statuses », plus anciens, portent encore les previews Netlify et
Vercel. N'en lire qu'une donnerait « aucune verification » sur des depots parfaitement equipes,
et on proposerait a la relecture une correction jamais testee.

LE CAS LE PLUS DELICAT EST CELUI OU IL SERAIT TENTANT DE MENTIR. Un depot sans CI ni preview ne
rendra jamais rien. Attendre indefiniment laisserait la correction en brouillon, invisible. On
la sort donc du brouillon, mais en disant qu'on n'a RIEN PU VERIFIER — pas « verifie ». Toute la
valeur de cette etape tient dans cette difference, et un test la tient.

CE QUE CETTE ETAPE RETIRE, et c'est le seul automatisme perdu : la fusion automatique ne part
plus au moment de la correction. Les conditions sont inchangees, mais elles decident desormais
d'un DROIT a fusionner, exerce apres le verdict. Sur un depot muet, la fusion est SUSPENDUE
plutot que tentee a l'aveugle — parce que fusionner sans savoir est exactement le geste qui
casse un site.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time
import uuid
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-verifpr-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as m  # noqa: E402
from backend import auth  # noqa: E402
from backend.models import IssueTask, Project, User  # noqa: E402


# --- la lecture des deux sources ---------------------------------------------------------------

def _repondre(monkeypatch, *, check_runs=None, statuses=None) -> None:
    """Remplace les deux lectures GitHub par ce que le depot est cense repondre."""
    def _faux_get(chemin, *, token, params=None, timeout_s=30.0):
        if chemin.endswith("/check-runs"):
            return {"check_runs": list(check_runs or [])}
        if chemin.endswith("/status"):
            return {"statuses": list(statuses or [])}
        raise AssertionError("appel inattendu : %s" % chemin)
    monkeypatch.setattr(m, "_github_api_get", _faux_get)


def test_les_check_runs_ET_les_statuses_sont_lus(monkeypatch) -> None:
    """N'en lire qu'un donnerait « aucune verification » sur un depot Netlify ou Vercel."""
    _repondre(monkeypatch,
              check_runs=[{"status": "completed", "conclusion": "success", "name": "build"}],
              statuses=[{"state": "success", "context": "netlify/deploy-preview"}])
    v = m._verifications_du_commit(owner="a", repo="b", sha="c" * 40, token="t")
    assert v["reussis"] == 2, v
    assert v["total"] == 2 and not v["echoues"]


def test_une_preview_NETLIFY_seule_suffit_a_compter(monkeypatch) -> None:
    """Le cas qui serait passe pour « depot muet » si on ne lisait que les check runs."""
    _repondre(monkeypatch, check_runs=[], statuses=[{"state": "failure", "context": "netlify/build"}])
    v = m._verifications_du_commit(owner="a", repo="b", sha="c" * 40, token="t")
    assert v["echoues"] == ["netlify/build"], v


def test_une_execution_ANNULEE_ne_compte_ni_pour_ni_contre(monkeypatch) -> None:
    """Une CI annulee ne dit rien du code : la traiter comme un echec bloquerait a tort."""
    _repondre(monkeypatch,
              check_runs=[{"status": "completed", "conclusion": "cancelled", "name": "build"}])
    v = m._verifications_du_commit(owner="a", repo="b", sha="c" * 40, token="t")
    assert v == {"total": 0, "en_cours": 0, "echoues": [], "reussis": 0}


# --- la decision -------------------------------------------------------------------------------

@pytest.mark.parametrize("verifs, age, attendu", [
    ({"en_cours": 2, "echoues": [], "reussis": 0}, 60, "attendre"),
    ({"en_cours": 0, "echoues": ["build"], "reussis": 1}, 60, "refuser"),
    ({"en_cours": 0, "echoues": [], "reussis": 3}, 60, "promouvoir"),
    ({"en_cours": 0, "echoues": [], "reussis": 0}, 5, "attendre"),
    ({"en_cours": 0, "echoues": [], "reussis": 0}, 99_999, "inconnu"),
    ({"en_cours": 1, "echoues": [], "reussis": 0}, 99_999, "inconnu"),
])
def test_la_decision(verifs, age, attendu) -> None:
    decision, raison = m._decider_de_la_pr(verifs, age_s=age)
    assert decision == attendu, (decision, raison)
    assert raison, "une decision sans raison est illisible dans un journal"


def test_un_seul_echec_l_emporte_sur_dix_succes() -> None:
    """Le bord qui compte : un build rouge parmi des verts reste un build rouge."""
    decision, _r = m._decider_de_la_pr(
        {"en_cours": 3, "echoues": ["build"], "reussis": 10}, age_s=60)
    assert decision == "refuser"


def test_AUCUNE_verification_n_est_jamais_dit_VERIFIE() -> None:
    """La distinction qui porte toute la valeur de l'etape.

    Un depot muet ne prouve rien. Sortir la correction du brouillon est le bon geste — sinon
    elle resterait invisible — mais l'annoncer comme verifiee serait un mensonge, et c'est
    exactement le mensonge qu'un raccourci produirait.
    """
    decision, raison = m._decider_de_la_pr(
        {"en_cours": 0, "echoues": [], "reussis": 0}, age_s=99_999)
    assert decision == "inconnu"
    assert "ucune vérification" in raison
    assert "érifié" not in raison, "la raison laisse croire a une verification : %r" % raison


# --- la reprise, bout en bout -------------------------------------------------------------------

def _projet_avec_tache(*, note: dict, depot: str = "agence/site") -> tuple[str, str]:
    m.DB.create_tables()
    with m.DB.session() as db:
        u = User(email="proprio-%s@exemple.fr" % uuid.uuid4().hex[:8],
                 password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(u)
        db.commit()
        db.refresh(u)
        uid = str(u.id)
        p = Project(owner_user_id=uid, slug="site-%s" % uuid.uuid4().hex[:8],
                    site_name="site", base_url="https://site.fr/",
                    settings={"github_repo": depot, "github_branch": "main"})
        db.add(p)
        db.commit()
        db.refresh(p)
        t = IssueTask(project_id=str(p.id), user_id=uid, issue_key="k", issue_label="l",
                      url="https://site.fr/", status="in_progress", severity="notice",
                      note=json.dumps(note, ensure_ascii=False))
        db.add(t)
        db.commit()
        db.refresh(t)
        return uid, str(t.id)


def _note_en_attente(*, fusion_auto: bool = False, age_s: float = 600.0) -> dict:
    return {
        "pr_url": "https://github.com/agence/site/pull/7", "pr_number": 7,
        "pr_title": "fix(seo): titre",
        "verification": {"etat": "en_attente", "sha": "d" * 40, "node_id": "PR_node",
                         "ouvert_le": time.time() - age_s, "fusion_auto": fusion_auto,
                         "raison": ""},
    }


def _verif_relue(task_id: str) -> dict:
    with m.DB.session() as db:
        t = db.get(IssueTask, task_id)
        return json.loads(t.note)["verification"], t.status


def _brancher(monkeypatch, *, check_runs=None, statuses=None, ouverte=True) -> dict:
    """Branche GitHub en entier et enregistre ce que le code aurait FAIT."""
    fait: dict = {"prete": 0, "fusions": 0, "commentaires": []}
    _repondre(monkeypatch, check_runs=check_runs, statuses=statuses)
    monkeypatch.setattr(m, "_effective_user_connection_value",
                        lambda **kw: ("ghp_jeton", "user"))
    monkeypatch.setattr(m, "_github_pr_is_open", lambda *a, **k: ouverte)
    monkeypatch.setattr(m, "_pr_marquer_prete",
                        lambda **kw: fait.__setitem__("prete", fait["prete"] + 1))
    monkeypatch.setattr(m, "_github_api_put",
                        lambda *a, **k: fait.__setitem__("fusions", fait["fusions"] + 1))
    monkeypatch.setattr(m, "_github_api_post",
                        lambda *a, **k: fait["commentaires"].append(k.get("json_body", {})))
    return fait


def test_un_build_VERT_sort_la_pr_du_brouillon(monkeypatch) -> None:
    _uid, tid = _projet_avec_tache(note=_note_en_attente())
    fait = _brancher(monkeypatch,
                     check_runs=[{"status": "completed", "conclusion": "success", "name": "build"}])
    with m.DB.session() as db:
        assert m._reprendre_une_verification(db.get(IssueTask, tid)) == "verifiee"
    verif, _statut = _verif_relue(tid)
    assert verif["etat"] == "verifiee"
    assert fait["prete"] == 1, "la pull request est restee en brouillon"
    assert fait["fusions"] == 0, "fusion sans droit de fusion"


def test_un_build_ROUGE_laisse_la_pr_en_brouillon_et_le_DIT(monkeypatch) -> None:
    """Le cas pour lequel toute l'etape existe.

    La PR reste en brouillon plutot que d'etre refermee : le brouillon dit « ne merge pas ca »
    sans detruire le diff, que le client peut vouloir lire pour comprendre ce qui etait tente.
    """
    _uid, tid = _projet_avec_tache(note=_note_en_attente(fusion_auto=True))
    fait = _brancher(monkeypatch,
                     check_runs=[{"status": "completed", "conclusion": "failure", "name": "build"}])
    with m.DB.session() as db:
        assert m._reprendre_une_verification(db.get(IssueTask, tid)) == "echouee"
    verif, statut = _verif_relue(tid)
    assert verif["etat"] == "echouee" and "build" in verif["raison"]
    assert statut == "blocked"
    assert fait["prete"] == 0, "une correction au build rouge a ete proposee a la relecture"
    assert fait["fusions"] == 0, "une correction au build rouge a ete FUSIONNEE"
    assert fait["commentaires"], "rien n'explique l'echec sur la pull request"


def test_un_build_VERT_avec_droit_de_fusion_FUSIONNE(monkeypatch) -> None:
    _uid, tid = _projet_avec_tache(note=_note_en_attente(fusion_auto=True))
    fait = _brancher(monkeypatch,
                     check_runs=[{"status": "completed", "conclusion": "success", "name": "build"}])
    with m.DB.session() as db:
        assert m._reprendre_une_verification(db.get(IssueTask, tid)) == "fusionnee"
    _verif, statut = _verif_relue(tid)
    assert fait["fusions"] == 1
    assert statut == "done"


def test_un_depot_MUET_ne_fusionne_PAS_tout_seul(monkeypatch) -> None:
    """Le seul automatisme que cette etape retire, et le test qui le fige.

    Avant, une reparation mecanique en mode auto partait sans rien savoir. Desormais, faute de
    verdict, elle attend une relecture : la correction est proposee, pas appliquee.
    """
    _uid, tid = _projet_avec_tache(note=_note_en_attente(fusion_auto=True, age_s=99_999))
    fait = _brancher(monkeypatch, check_runs=[], statuses=[])
    with m.DB.session() as db:
        assert m._reprendre_une_verification(db.get(IssueTask, tid)) == "non_verifiable"
    verif, _statut = _verif_relue(tid)
    assert verif["etat"] == "non_verifiable"
    assert "suspendue" in verif["raison"], verif["raison"]
    assert fait["prete"] == 1, "la correction est restee invisible"
    assert fait["fusions"] == 0, "fusion sur un depot qui n'a rien verifie"


def test_une_verification_EN_COURS_est_simplement_reprise_plus_tard(monkeypatch) -> None:
    _uid, tid = _projet_avec_tache(note=_note_en_attente(age_s=30))
    fait = _brancher(monkeypatch, check_runs=[{"status": "in_progress", "name": "build"}])
    with m.DB.session() as db:
        assert m._reprendre_une_verification(db.get(IssueTask, tid)) == "attendre"
    verif, _s = _verif_relue(tid)
    assert verif["etat"] == "en_attente", "une verification en cours a ete tranchee"
    assert fait["prete"] == 0 and fait["fusions"] == 0


def test_une_pr_FERMEE_a_la_main_n_est_pas_ranimee(monkeypatch) -> None:
    _uid, tid = _projet_avec_tache(note=_note_en_attente(fusion_auto=True))
    fait = _brancher(monkeypatch, ouverte=False,
                     check_runs=[{"status": "completed", "conclusion": "success", "name": "b"}])
    with m.DB.session() as db:
        assert m._reprendre_une_verification(db.get(IssueTask, tid)) == "fermee"
    verif, _s = _verif_relue(tid)
    assert verif["etat"] == "abandonnee"
    assert fait["prete"] == 0 and fait["fusions"] == 0


def test_sans_JETON_on_ne_touche_a_rien(monkeypatch) -> None:
    """Un jeton revoque entre l'ouverture et le balayage ne doit rien casser."""
    _uid, tid = _projet_avec_tache(note=_note_en_attente())
    _brancher(monkeypatch, check_runs=[])
    monkeypatch.setattr(m, "_effective_user_connection_value", lambda **kw: ("", "none"))
    with m.DB.session() as db:
        assert m._reprendre_une_verification(db.get(IssueTask, tid)) == "sans_jeton"
    verif, _s = _verif_relue(tid)
    assert verif["etat"] == "en_attente", "l'attente a ete tranchee sans pouvoir rien lire"


# --- le balayage et son cron ---------------------------------------------------------------------

def test_le_balayage_ne_reprend_que_ce_qui_ATTEND(monkeypatch) -> None:
    _uid, attend = _projet_avec_tache(note=_note_en_attente())
    note_finie = _note_en_attente()
    note_finie["verification"]["etat"] = "verifiee"
    _uid2, finie = _projet_avec_tache(note=note_finie)

    _brancher(monkeypatch,
              check_runs=[{"status": "completed", "conclusion": "success", "name": "build"}])
    m._balayer_verifications_pr()

    assert _verif_relue(attend)[0]["etat"] == "verifiee"
    assert _verif_relue(finie)[0]["etat"] == "verifiee", "une tache deja close a ete retouchee"


def test_le_cron_REFUSE_sans_le_secret() -> None:
    from fastapi.testclient import TestClient

    from backend.app import app

    client = TestClient(app)
    assert client.post("/cron/verify-pull-requests").status_code == 401
    assert client.post("/cron/verify-pull-requests",
                       headers={"Authorization": "Bearer mauvais"}).status_code == 401
    ok = client.post("/cron/verify-pull-requests",
                     headers={"Authorization": "Bearer %s" % os.environ["CRON_SECRET"]})
    assert ok.status_code == 200, ok.text


def test_le_cron_AUTOPILOTE_balaie_aussi(monkeypatch) -> None:
    """Le filet de securite : sans lui, oublier une ligne d'ordonnanceur arreterait le correcteur.

    Les corrections attendent le balayage pour sortir du brouillon. Si l'entree
    `/cron/verify-pull-requests` n'est pas configuree, elles y resteraient pour toujours. Le
    balayage tourne donc aussi depuis l'autopilote : moins souvent, mais il tourne.
    """
    import ast as _ast

    source = Path(m.__file__).read_text(encoding="utf-8")
    arbre = _ast.parse(source)
    dedans = False
    for n in _ast.walk(arbre):
        if isinstance(n, (_ast.FunctionDef, _ast.AsyncFunctionDef)) and n.name == "cron_autopilot":
            dedans = any(isinstance(c, _ast.Call)
                         and _ast.unparse(c.func) == "_balayer_verifications_pr"
                         for c in _ast.walk(n))
    assert dedans, (
        "le cron autopilote ne balaie plus les vérifications : oublier l'entrée "
        "`/cron/verify-pull-requests` figerait toutes les corrections en brouillon")


# --- ce que les routes posent -------------------------------------------------------------------

def test_les_CINQ_routes_ouvrent_en_brouillon_et_laissent_une_trace() -> None:
    """La garde ENUMERE : une sixieme route qui ouvrirait sans brouillon serait nommee ici.

    La cinquieme est arrivee le 20/09/2026 : `api_content_draft`, qui CREE une page au lieu
    d'en corriger une. Le compte est un fil declencheur, pas une limite — il force a venir
    verifier que la nouvelle passe bien par le brouillon et la trace, ce que ce test fait
    juste en dessous. Le monter sans lire les deux assertions suivantes viderait la garde.
    """
    import ast as _ast

    source = Path(m.__file__).read_text(encoding="utf-8")
    arbre = _ast.parse(source)
    appels = [n for n in _ast.walk(arbre)
              if isinstance(n, _ast.Call) and _ast.unparse(n.func) == "_ouvrir_pull_request"]
    assert len(appels) == 5, "%d routes ouvrent une PR (attendu 5)" % len(appels)
    sans_brouillon = [n.lineno for n in appels
                      if not any(k.arg == "draft" and getattr(k.value, "value", None) is True
                                 for k in n.keywords)]
    assert not sans_brouillon, (
        "ces routes ouvrent une pull request sans brouillon, donc sans vérification possible : %s"
        % ", ".join("app.py:%d" % l for l in sans_brouillon))

    traces = [n for n in _ast.walk(arbre)
              if isinstance(n, _ast.Call) and _ast.unparse(n.func) == "_bloc_verification"]
    assert len(traces) == 5, (
        "%d traces de vérification pour 5 pull requests : une route ouvrira un brouillon que "
        "personne ne viendra jamais sortir" % len(traces))


def test_plus_AUCUNE_fusion_synchrone_dans_les_routes() -> None:
    """Le geste retire doit rester retire.

    Une fusion posee a nouveau dans une route s'executerait avant tout verdict — exactement ce
    que cette etape supprime — et aucun test de comportement ne la verrait, puisque le chemin
    du cron continuerait de fonctionner a cote.
    """
    import ast as _ast

    source = Path(m.__file__).read_text(encoding="utf-8")
    arbre = _ast.parse(source)
    portees = [(n.lineno, n.end_lineno or n.lineno, n.name)
               for n in _ast.walk(arbre)
               if isinstance(n, (_ast.FunctionDef, _ast.AsyncFunctionDef))]
    fautifs = []
    for n in _ast.walk(arbre):
        if not (isinstance(n, _ast.Call) and _ast.unparse(n.func) == "_github_api_put"):
            continue
        if not n.args:
            continue
        chemin = _ast.unparse(n.args[0])
        if "'merge'" not in chemin:
            continue
        englobantes = [p for p in portees if p[0] <= n.lineno <= p[1]]
        nom = max(englobantes, key=lambda p: p[0])[2] if englobantes else "<module>"
        if nom == "_reprendre_une_verification":
            continue
        fautifs.append("app.py:%d (%s)" % (n.lineno, nom))
    assert not fautifs, (
        "ces fusions partent sans attendre le verdict du dépôt :\n  " + "\n  ".join(fautifs))


def test_toute_route_CRON_est_dans_les_DEUX_listes() -> None:
    """La garde qui aurait attrape deux defauts, dont un deja en place.

    Une route `/cron` traverse deux barrieres. Le middleware de session redirige vers la
    connexion tout chemin absent de sa liste blanche ; le middleware CSRF repond 403 a tout
    POST absent de la sienne. Les deux listes sont ecrites a des endroits differents du
    fichier, et rien ne les reliait.

    Le cout de l'oubli est le meme dans les deux sens et il est invisible : l'ordonnanceur
    appelle, recoit 303 ou 403, et personne ne regarde. Le travail cesse simplement de se
    faire.

    Ce test a trouve deux choses. La premiere en l'ecrivant : la nouvelle route de verification
    manquait a la liste CSRF, donc le balayage n'aurait jamais tourne en production. La seconde
    etait DEJA LA : `/cron/refresh-competitors` etait exemptee de CSRF sans etre dans la liste
    blanche de session — exemptee d'une barriere, bloquee par l'autre.
    """
    import ast as _ast

    source = Path(m.__file__).read_text(encoding="utf-8")
    arbre = _ast.parse(source)

    routes_cron = set()
    for n in _ast.walk(arbre):
        if not isinstance(n, (_ast.FunctionDef, _ast.AsyncFunctionDef)):
            continue
        for deco in n.decorator_list:
            if not isinstance(deco, _ast.Call) or not deco.args:
                continue
            premier = deco.args[0]
            if isinstance(premier, _ast.Constant) and str(premier.value).startswith("/cron/"):
                routes_cron.add(str(premier.value))
    assert routes_cron, "aucune route /cron trouvée : le test ne mesure plus rien"

    hors_csrf = sorted(routes_cron - set(m._CSRF_EXEMPT_PATHS))
    assert not hors_csrf, (
        "ces routes /cron ne sont pas exemptées de CSRF : un POST de l'ordonnanceur "
        "recevra 403 sans que personne ne le voie : %s" % ", ".join(hors_csrf))

    # La liste blanche du middleware de session est un littéral dans sa fonction : on la relit
    # là où elle est écrite plutôt que de la recopier ici.
    blanche: set[str] = set()
    for n in _ast.walk(arbre):
        if isinstance(n, (_ast.FunctionDef, _ast.AsyncFunctionDef)) and n.name == "session_auth_middleware":
            for c in _ast.walk(n):
                if isinstance(c, _ast.Set):
                    blanche |= {e.value for e in c.elts
                                if isinstance(e, _ast.Constant) and isinstance(e.value, str)}
    assert blanche, "liste blanche du middleware introuvable : le test ne mesure plus rien"

    hors_blanche = sorted(routes_cron - blanche)
    assert not hors_blanche, (
        "ces routes /cron sont derrière l'authentification : l'ordonnanceur sera redirigé "
        "vers la page de connexion : %s" % ", ".join(hors_blanche))


def test_une_pull_request_ne_porte_qu_UNE_trace_de_verification() -> None:
    """Une trace creee DANS une boucle veut dire plusieurs reprises de la meme PR.

    La correction en lot ouvre UNE pull request qui ferme plusieurs anomalies, et enregistre une
    tache par anomalie. Poser la trace de verification sur chacune ferait reprendre la meme PR
    autant de fois par le balayage — donc tenter de la sortir du brouillon et de la fusionner
    plusieurs fois.

    Le test ne cherche pas un motif correct : il verifie une propriete de structure — aucune
    trace n'est fabriquee a l'interieur d'une boucle. C'est vrai aujourd'hui pour les cinq
    routes, et ca le restera pour la sixieme.
    """
    import ast as _ast

    arbre = _ast.parse(Path(m.__file__).read_text(encoding="utf-8"))
    dans_une_boucle = []
    for n in _ast.walk(arbre):
        if not isinstance(n, (_ast.For, _ast.AsyncFor, _ast.While)):
            continue
        for c in _ast.walk(n):
            if isinstance(c, _ast.Call) and _ast.unparse(c.func) == "_bloc_verification":
                dans_une_boucle.append(c.lineno)
    assert not dans_une_boucle, (
        "une trace de vérification est fabriquée dans une boucle : la même pull request serait "
        "reprise plusieurs fois par le balayage — app.py:%s"
        % ", ".join(str(l) for l in sorted(set(dans_une_boucle))))


def test_la_porte_envoie_REELLEMENT_le_drapeau_brouillon(monkeypatch) -> None:
    """Passer `draft=True` a la porte ne prouve rien si la porte ne le transmet pas.

    Une mutation l'a montre : desactiver le drapeau A L'INTERIEUR de la fonction laissait les
    cinq appelants intacts, et les gardes qui les enumerent restaient vertes. La verification
    aurait alors porte sur des pull requests deja ouvertes a la relecture du client.
    """
    envoye: dict = {}
    monkeypatch.setattr(m, "_github_api_post",
                        lambda chemin, *, token, json_body, **kw: envoye.update(json_body) or {})
    m._ouvrir_pull_request(owner="a", repo="b", token="t", title="T", body="B",
                           head="h", base="main", draft=True)
    assert envoye.get("draft") is True, "le brouillon n'est pas transmis à GitHub : %r" % envoye

    envoye.clear()
    m._ouvrir_pull_request(owner="a", repo="b", token="t", title="T", body="B",
                           head="h", base="main")
    assert "draft" not in envoye, "une PR ordinaire part en brouillon sans qu'on l'ait demandé"


def test_le_balayage_ne_va_PAS_CHERCHER_les_taches_deja_closes(monkeypatch) -> None:
    """Le filtre en base, et pourquoi il vaut un test a lui.

    La tache close est de toute facon ignoree une fois chargee — c'est la double protection qui
    avait laisse survivre une mutation. Mais la table des taches ne fait que grandir : la
    balayer entierement toutes les deux minutes est la difference entre un cron qui reste
    gratuit et un cron qui ralentit tous les mois. Le resultat du balayage rend la difference
    OBSERVABLE : une tache close ne doit meme pas y apparaitre.
    """
    note_finie = _note_en_attente()
    note_finie["verification"]["etat"] = "verifiee"
    _uid, _finie = _projet_avec_tache(note=note_finie)
    _uid2, _attend = _projet_avec_tache(note=_note_en_attente())

    _brancher(monkeypatch,
              check_runs=[{"status": "completed", "conclusion": "success", "name": "build"}])
    resultats = m._balayer_verifications_pr()
    assert "sans_objet" not in resultats, (
        "le balayage charge des tâches déjà tranchées : %r" % resultats)
    assert resultats.get("verifiee", 0) >= 1, resultats


def test_les_CINQ_routes_annoncent_que_la_verification_est_EN_COURS() -> None:
    """Une interface qui dit « Pull Request créée » sur un brouillon ment au client.

    Avant ce changement, « PR créée » et « Correction mergée » etaient exacts. Ils ne le sont
    plus : la correction part en brouillon et la fusion attend le verdict. Le drapeau
    `verification` dans la reponse est ce qui permet a l'ecran de le dire — sans lui, le
    message redeviendrait faux sans qu'aucun test ne bronche, parce que rien de casse ne se
    produirait.
    """
    import ast as _ast

    source = Path(m.__file__).read_text(encoding="utf-8")
    arbre = _ast.parse(source)
    portees = [(n.lineno, n.end_lineno or n.lineno, n.name)
               for n in _ast.walk(arbre)
               if isinstance(n, (_ast.FunctionDef, _ast.AsyncFunctionDef))]
    routes_pr = set()
    for n in _ast.walk(arbre):
        if isinstance(n, _ast.Call) and _ast.unparse(n.func) == "_ouvrir_pull_request":
            englobantes = [p for p in portees if p[0] <= n.lineno <= p[1]]
            if englobantes:
                routes_pr.add(max(englobantes, key=lambda p: p[0])[2])
    assert len(routes_pr) == 5, routes_pr

    muettes = []
    for debut, fin, nom in portees:
        if nom not in routes_pr:
            continue
        corps = "\n".join(source.splitlines()[debut - 1:fin])
        if '"verification": "en_attente"' not in corps:
            muettes.append(nom)
    assert not muettes, (
        "ces routes n'annoncent pas que la vérification est en cours, donc l'écran dira "
        "« Pull Request créée » sur un brouillon : %s" % ", ".join(sorted(muettes)))


def test_l_ecran_DISTINGUE_le_brouillon_de_la_pr_prete() -> None:
    """Le drapeau ne sert a rien si l'ecran ne le lit pas."""
    page = (Path(m.__file__).resolve().parents[1] / "templates" / "corrections.html").read_text(
        encoding="utf-8")
    assert "function statutCorrection" in page, "l'écran n'a plus de message dédié au brouillon"
    assert "d.verification" in page, "l'écran ne lit pas l'état de vérification"
    assert "brouillon" in page, "le mot « brouillon » a disparu du message"


# --- le balayage tourne SANS ordonnanceur externe -----------------------------------------

def test_le_balayage_est_LANCE_au_demarrage_du_service() -> None:
    """Sans ce demarrage, rien ne sort jamais du brouillon.

    Le balayage a d'abord ete confie a un workflow GitHub planifie. Mesure le 19/09/2026 : une
    cadence de cinq minutes n'avait produit AUCUNE execution apres vingt minutes — GitHub
    deprioritise les cadences courtes, et le documente. Pour une tache quotidienne c'est sans
    consequence ; pour un client qui attend sa correction, c'est le mauvais outil.

    Le produit avait deja le bon mecanisme : un fil demon periodique dans le service web, comme
    `_start_retention`. Ce test verifie que le demarrage l'appelle — un fil qu'on oublie de
    lancer ne se signale par rien.
    """
    import ast as _ast

    arbre = _ast.parse(Path(m.__file__).read_text(encoding="utf-8"))
    for n in _ast.walk(arbre):
        if isinstance(n, (_ast.FunctionDef, _ast.AsyncFunctionDef)) and n.name == "_startup":
            appels = {_ast.unparse(c.func) for c in _ast.walk(n) if isinstance(c, _ast.Call)}
            assert "_start_verification_pr" in appels, (
                "le démarrage ne lance pas le balayage : les corrections resteraient en "
                "brouillon indéfiniment")
            return
    raise AssertionError("`_startup` introuvable : le test ne mesure plus rien")


def test_une_ERREUR_ne_tue_pas_la_boucle(monkeypatch) -> None:
    """La protection que `_retention_loop` n'a pas, et qui compte ici.

    Dans `_retention_loop`, une exception d'un nettoyage arrete le fil pour de bon, en silence,
    jusqu'au redemarrage. Le balayage tourne toutes les deux minutes et appelle un service
    externe : GitHub sera indisponible tot ou tard. Si la premiere panne arretait le fil, une
    coupure de trente secondes chez GitHub gelerait toutes les corrections jusqu'au prochain
    deploiement — et personne ne verrait pourquoi.
    """
    passages = {"n": 0}

    def _capricieux(**kwargs):
        passages["n"] += 1
        if passages["n"] == 1:
            raise RuntimeError("GitHub indisponible")
        m._WORKER_STOP.set()          # on arrete la boucle au second passage
        return {"verifiee": 1}

    monkeypatch.setattr(m, "_balayer_verifications_pr", _capricieux)
    monkeypatch.setattr(m, "_pr_verif_interval_s", lambda: 30)
    monkeypatch.setattr(m._WORKER_STOP, "wait", lambda _s: None)
    try:
        m._boucle_verification_pr()
    finally:
        m._WORKER_STOP.clear()
    assert passages["n"] == 2, (
        "la boucle s'est arrêtée à la première erreur : une coupure passagère de GitHub "
        "gèlerait toutes les corrections jusqu'au prochain déploiement")


def test_la_cadence_est_BORNEE_des_deux_cotes(monkeypatch) -> None:
    """Une cadence mal reglee est soit un martelage de l'API GitHub, soit un gel."""
    monkeypatch.delenv("PR_VERIFY_EVERY_SECONDS", raising=False)
    assert m._pr_verif_interval_s() == 120
    monkeypatch.setenv("PR_VERIFY_EVERY_SECONDS", "1")
    assert m._pr_verif_interval_s() == 30, "une cadence d'une seconde martèlerait l'API GitHub"
    monkeypatch.setenv("PR_VERIFY_EVERY_SECONDS", "999999")
    assert m._pr_verif_interval_s() == 3600
    monkeypatch.setenv("PR_VERIFY_EVERY_SECONDS", "pas-un-nombre")
    assert m._pr_verif_interval_s() == 120, "une valeur illisible doit retomber sur le défaut"


def test_le_fil_n_est_lance_QU_UNE_FOIS(monkeypatch) -> None:
    """Deux fils balaieraient les memes taches et tenteraient deux fois chaque fusion."""
    lances = {"n": 0}

    class _FauxFil:
        def __init__(self, **kwargs) -> None:
            lances["n"] += 1

        def start(self) -> None:
            pass

    monkeypatch.setattr(m.threading, "Thread", _FauxFil)
    monkeypatch.setattr(m, "_PR_VERIF_STARTED", False)
    m._start_verification_pr()
    m._start_verification_pr()
    m._start_verification_pr()
    assert lances["n"] == 1, "%d fils lancés" % lances["n"]
