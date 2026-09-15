# -*- coding: utf-8 -*-
"""L'acces des robots d'IA : la section qu'Ahrefs a ajoutee entre juin et septembre 2026.

Releve le 15/09/2026 sur un export du compte client : Ahrefs suit desormais 178 familles au lieu
de 173, et l'ecart tient a une seule section — AI Discoverability. Sur ses sept lignes, une
seule etait deja detectee chez nous (`pages_to_submit_to_indexnow`) et cinq manquaient.

Trois sont ajoutees ici, celles qui se lisent dans `robots.txt` — un fichier que le crawl
recupere DEJA, donc sans une requete de plus.

CE QUI COMPTE DANS CETTE FAMILLE, ce n'est pas la detection mais la DISTINCTION :

  RECHERCHE    l'agent va chercher une page pour repondre a quelqu'un, et cite sa source.
               Le bloquer coute des visites. C'est une question de referencement.
  ENTRAINEMENT l'agent collecte un corpus. Le bloquer est une decision de propriete
               intellectuelle, parfaitement legitime, et sans effet sur le referencement.

Les confondre ferait conseiller a un proprietaire d'ouvrir ses contenus a l'entrainement « pour
le SEO ». C'est pourquoi les deux listes sont nommees, et pourquoi ces tests verifient surtout
qu'elles ne se melangent pas.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

AUDIT = (Path(__file__).resolve().parents[2]
         / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py")


def _audit():
    if "seo_audit_mod" not in sys.modules:
        spec = importlib.util.spec_from_file_location("seo_audit_mod", AUDIT)
        mod = importlib.util.module_from_spec(spec)
        sys.modules["seo_audit_mod"] = mod
        spec.loader.exec_module(mod)
    return sys.modules["seo_audit_mod"]


def _robots(texte: str):
    m = _audit()
    return m._parse_robots(texte) if hasattr(m, "_parse_robots") else None


def test_les_deux_listes_ne_se_recouvrent_pas():
    """Un agent ne peut pas etre a la fois « recherche » et « entrainement » : le conseil differe."""
    m = _audit()
    assert set(m.AI_SEARCH_BOTS) & set(m.AI_TRAINING_BOTS) == set()


def test_les_listes_ne_sont_pas_vides():
    m = _audit()
    assert len(m.AI_SEARCH_BOTS) >= 4
    assert len(m.AI_TRAINING_BOTS) >= 5


def test_les_agents_de_recherche_connus_sont_couverts():
    """Ceux qui envoient reellement des visites aujourd'hui."""
    m = _audit()
    for agent in ("OAI-SearchBot", "PerplexityBot", "ChatGPT-User"):
        assert agent in m.AI_SEARCH_BOTS


def test_les_agents_d_entrainement_connus_sont_couverts():
    m = _audit()
    for agent in ("GPTBot", "CCBot", "Google-Extended"):
        assert agent in m.AI_TRAINING_BOTS


def test_les_trois_familles_sont_montrees_au_client():
    from backend import audit_dashboard as dash
    for cle in ("indexable_page_blocked_from_all_ai_search_bots",
                "indexable_page_blocked_from_some_ai_search_bots",
                "inconsistent_ai_training_bot_policy"):
        assert cle in dash.ISSUE_CATALOG
        assert cle not in dash.NON_ISSUE_KEYS


def test_la_politique_d_entrainement_est_une_REMARQUE_pas_un_defaut():
    """Fermer l'entrainement est un choix legitime : le classer en avertissement pousserait a
    « corriger » une decision du proprietaire."""
    from backend import audit_dashboard as dash
    assert dash.ISSUE_CATALOG["inconsistent_ai_training_bot_policy"].severity == "notice"
    assert dash.ISSUE_CATALOG[
        "indexable_page_blocked_from_all_ai_search_bots"].severity == "warning"


def test_aucune_de_ces_familles_n_est_declaree_corrigeable_automatiquement():
    """Editer le robots.txt d'un client pour ouvrir des robots d'IA est une decision qui lui
    appartient. On la lui EXPLIQUE, on ne la prend pas a sa place."""
    import os
    os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
    os.environ.setdefault("SEO_AGENT_SECRET_KEY", "x" * 20)
    from backend import app as m
    for cle in ("indexable_page_blocked_from_all_ai_search_bots",
                "inconsistent_ai_training_bot_policy"):
        assert not m._github_issue_auto_fixable(cle)


# ── Le comportement, sur de vrais robots.txt ─────────────────────────────────────────────────

BASE = "https://exemple.fr"
CLES = ("indexable_page_blocked_from_all_ai_search_bots",
        "indexable_page_blocked_from_some_ai_search_bots",
        "inconsistent_ai_training_bot_policy")


def _page(url: str, indexable: bool = True):
    m = _audit()
    p = m.PageData(url=url)
    p.final_url = url
    p.status_code = 200
    p.content_type = "text/html"
    if not indexable:
        p.meta_robots = "noindex"
    return p


def _comptes(robots_txt: str, pages=None) -> tuple[int, int, int]:
    m = _audit()
    issues = m._score_issues(pages or [_page(BASE + "/"), _page(BASE + "/a-propos")],
                             base_url=BASE, robots=m._parse_robots_rules(robots_txt))
    return tuple(int((issues.get(k) or {}).get("count") or 0) for k in CLES)


def _tout_bloquer(agents) -> str:
    return "User-agent: *\nAllow: /\n" + "".join(
        "\nUser-agent: %s\nDisallow: /\n" % a for a in agents)


def test_un_robots_sans_regle_IA_ne_leve_rien():
    """Le cas de l'immense majorite des sites : aucune fausse alerte."""
    assert _comptes("User-agent: *\nAllow: /\n") == (0, 0, 0)


def test_un_seul_bot_de_recherche_bloque_donne_CERTAINS():
    tous, certains, _ = _comptes(
        "User-agent: *\nAllow: /\n\nUser-agent: PerplexityBot\nDisallow: /\n")
    assert (tous, certains) == (0, 2)


def test_tous_les_bots_de_recherche_bloques_donnent_TOUS():
    """Et pas les deux a la fois : une page ne se compte qu'une fois."""
    m = _audit()
    tous, certains, _ = _comptes(_tout_bloquer(m.AI_SEARCH_BOTS))
    assert (tous, certains) == (2, 0)


def test_un_bot_d_entrainement_bloque_parmi_d_autres_est_INCOHERENT():
    assert _comptes("User-agent: *\nAllow: /\n\nUser-agent: GPTBot\nDisallow: /\n")[2] == 1


def test_bloquer_TOUT_l_entrainement_n_est_PAS_incoherent():
    """Fermer l'entrainement en entier est une position tenable. La signaler reviendrait a
    reprocher au proprietaire une decision qu'il a prise expres."""
    m = _audit()
    assert _comptes(_tout_bloquer(m.AI_TRAINING_BOTS))[2] == 0


def test_une_page_NON_indexable_n_est_pas_comptee():
    """La famille dit « page INDEXABLE bloquee » : une page en noindex n'attendait deja rien
    des moteurs."""
    m = _audit()
    assert _comptes(_tout_bloquer(m.AI_SEARCH_BOTS), [_page(BASE + "/", indexable=False)])[0] == 0


def test_bloquer_l_entrainement_ne_declenche_aucune_famille_de_RECHERCHE():
    """La preuve que les deux listes ne se contaminent pas."""
    m = _audit()
    tous, certains, incoherent = _comptes(_tout_bloquer(m.AI_TRAINING_BOTS[:3]))
    assert (tous, certains) == (0, 0)
    assert incoherent == 1


# ── Les deux dernieres familles de la section : lenteur et ressemblance ──────────────────────

def test_une_page_lente_est_vue_du_point_de_vue_du_ROBOT():
    """`elapsed_ms` mesure jusqu'au HTML complet, sans rendu JS — ce qu'aucun de ces agents
    n'execute. C'est ce qui distingue cette famille de `slow_page`, qui parlait d'un visiteur
    muni d'un navigateur."""
    m = _audit()
    lente, rapide = _page(BASE + "/lente"), _page(BASE + "/rapide")
    lente.elapsed_ms = m._AI_CRAWLER_SLOW_MS + 1
    rapide.elapsed_ms = m._AI_CRAWLER_SLOW_MS - 1
    issues = m._score_issues([lente, rapide], base_url=BASE)
    bloc = issues["slow_server_response_for_ai_crawlers"]
    assert bloc["count"] == 1
    assert bloc["examples"] == [BASE + "/lente"]


def test_une_page_sans_mesure_de_temps_n_est_pas_accusee():
    m = _audit()
    sans = _page(BASE + "/inconnue")
    sans.elapsed_ms = None
    assert m._score_issues([sans], base_url=BASE)["slow_server_response_for_ai_crawlers"]["count"] == 0


def _esquisse(m, texte: str):
    p = m.PageHTMLExtractor()
    p.feed("<html><body><main><p>" + texte + "</p></main></body></html>")
    return p.get_content_sketch()


def test_deux_textes_identiques_ont_la_meme_empreinte():
    m = _audit()
    texte = " ".join("mot%d" % i for i in range(120))
    assert _esquisse(m, texte) == _esquisse(m, texte)
    assert _esquisse(m, texte), "un texte assez long doit produire une empreinte"


def test_deux_textes_differents_ont_des_empreintes_differentes():
    m = _audit()
    a = _esquisse(m, " ".join("alpha%d" % i for i in range(120)))
    b = _esquisse(m, " ".join("beta%d" % i for i in range(120)))
    assert a and b and a != b
    commun = len(set(a) & set(b)) / float(len(set(a) | set(b)))
    assert commun < m._SKETCH_SIMILARITY


def test_un_texte_trop_court_n_a_pas_d_empreinte():
    """Sous cinq mots il n'existe aucune suite a hacher : on ne compare pas ce qu'on n'a pas."""
    m = _audit()
    assert _esquisse(m, "trois mots seulement") == ()


def test_deux_pages_quasi_identiques_se_ressemblent_au_dessus_du_seuil():
    """Le cas que la famille vise : meme plan, quelques mots changes."""
    m = _audit()
    base = " ".join("phrase%d" % i for i in range(200))
    variante = base.replace("phrase7 ", "autre7 ").replace("phrase42 ", "autre42 ")
    a, b = set(_esquisse(m, base)), set(_esquisse(m, variante))
    assert len(a & b) / float(len(a | b)) >= m._SKETCH_SIMILARITY


def test_les_seuils_sont_nommes_donc_revisables():
    """Trois conventions assumees — taille des suites, taille de l'esquisse, seuil de
    ressemblance. Nommees pour pouvoir etre revues sur des donnees reelles plutot que
    disseminees dans le code."""
    m = _audit()
    assert m._SKETCH_SHINGLE == 5 and m._SKETCH_SIZE == 64
    assert 0 < m._SKETCH_SIMILARITY <= 1
    assert m._AI_CRAWLER_SLOW_MS > 0


def test_la_ressemblance_ne_pretend_PAS_reconnaitre_une_IA():
    """Le libelle montre au client ne doit rien affirmer que le produit ne mesure pas : nous
    n'avons aucun classifieur de texte genere."""
    from backend import audit_dashboard as dash
    libelle = dash.ISSUE_CATALOG["similar_ai_generated_content"].label.lower()
    assert "ia" not in libelle.split() and "générés" not in libelle
