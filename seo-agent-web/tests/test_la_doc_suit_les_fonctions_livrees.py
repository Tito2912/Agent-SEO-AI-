# -*- coding: utf-8 -*-
"""Une fonctionnalité livrée sans sa page de documentation rend la documentation fausse.

La règle existe depuis le 03/09/2026 et elle est écrite dans ce projet : *en ajoutant ou
changeant une fonction, mettre à jour la page `content/docs/` correspondante*. Elle a tenu tant
qu'un humain s'en souvenait, puis elle a cédé deux fois de suite :

    comptes d'équipe       livrés le 19/09 — aucune page
    rédaction de contenu   livrée le 20/09 — aucune page, aucun jeton de quota

Et personne ne l'a vu, parce qu'une doc incomplète ne casse rien : elle se contente d'être
muette sur ce qui vient de sortir. Elle est pourtant publique et indexée.

CE FICHIER N'EST PAS UNE LISTE DE PAGES À COCHER. Il énumère ce que le PRODUIT expose — les
écrans que la navigation propose, les métriques que la facturation compte — et exige que la
documentation en parle. Une liste écrite à la main vieillirait de la même façon que la doc.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

os.environ.setdefault("SEO_AGENT_DATA_DIR", tempfile.mkdtemp(prefix="seo-agent-doc-"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", tempfile.mkdtemp(prefix="seo-agent-doc-runs-"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402
from backend import billing  # noqa: E402
from backend import content_library as cl  # noqa: E402

DOCS = WEB_ROOT / "content" / "docs"


def _tout_le_texte() -> str:
    """Le corps brut de toutes les pages de documentation, en une seule chaîne."""
    return "\n".join(f.read_text(encoding="utf-8") for f in sorted(DOCS.glob("*.md"))).lower()


# ── chaque compteur facturé doit être expliqué quelque part ──────────────────────────────────

def test_chaque_metrique_de_quota_a_son_jeton() -> None:
    """Sans jeton, la seule façon de documenter un plafond est de l'écrire en dur — et une doc
    qui écrit un chiffre en dur ment au premier réglage de `PLAN_CONFIG_JSON`.

    C'est ce qui est arrivé au quota d'articles : livré sans jeton, donc impossible à
    documenter honnêtement, donc pas documenté du tout.
    """
    jetons = set(m._content_tokens())
    limites = set((billing.plan_catalog().get("business") or {}).get("limits") or {})
    # La correspondance métrique -> préfixe de jeton, telle que `_content_tokens` la pose.
    attendus = {
        "ai_corrections_month": "corrections",
        "ai_articles_month": "articles",
        "assistant_messages_month": "assistant",
        "pages_crawled_month": "pages",
        "projects": "projects",
        "members": "members",
    }
    manquants = [metrique for metrique, prefixe in attendus.items()
                 if metrique in limites and f"{prefixe}_business" not in jetons]
    assert not manquants, (
        "ces compteurs sont facturés mais n'ont aucun jeton, donc aucune page ne peut les "
        "citer sans mentir : %s" % manquants)


def test_le_tableau_des_plans_cite_CHAQUE_jeton_de_plafond() -> None:
    """Un jeton qui existe mais que personne n'affiche ne documente rien."""
    tableau = (DOCS / "plans-et-quotas.md").read_text(encoding="utf-8")
    absents = [p for p in ("corrections", "articles", "members", "assistant", "pages", "projects")
               if "{{%s_business}}" % p not in tableau]
    assert not absents, "le tableau des plans ne montre pas ces plafonds : %s" % absents


# ── chaque écran du produit doit être documenté ──────────────────────────────────────────────

def test_chaque_ECRAN_de_projet_est_documente() -> None:
    """La navigation d'un projet est la liste de ce que le client peut faire. Un écran qui n'y
    renvoie à aucune page de doc est une fonctionnalité qu'il découvre seul."""
    mise_en_page = (WEB_ROOT / "templates" / "layout.html").read_text(encoding="utf-8")
    texte = _tout_le_texte()
    # (fragment d'URL de l'écran, mot que la doc doit employer pour en parler)
    ecrans = [
        ("/settings/crawl", "crawl"),
        ("/performance", "performance"),
        ("/keywords/", "mots-clés"),
        ("/competitors", "concurrent"),
        ("/content", "rédig"),
        ("/backlinks", "backlink"),
        ("/issues", "anomalie"),
        ("/corrections", "correction"),
    ]
    presents = [(url, mot) for url, mot in ecrans
                if ("href=\"/projects/{{ project.slug }}%s" % url) in mise_en_page]
    # LE TEMOIN. Sans lui, un motif de `href` qui ne correspond plus a rien rendrait ce test
    # vert en ne mesurant AUCUN ecran — la forme de garde creuse que ce projet traque partout.
    assert len(presents) >= 6, (
        "l'enumeration ne reconnait plus les entrees du menu : elle n'en voit que %d, "
        "donc elle ne mesure plus rien" % len(presents))
    muets = [url for url, mot in presents if mot.lower() not in texte]
    assert not muets, "ces écrans existent dans le menu et ne sont documentés nulle part : %s" % muets


def test_les_deux_fonctions_livrees_en_septembre_ont_leur_page() -> None:
    """Le témoin daté. Ces deux-là sont la raison d'être de ce fichier ; si l'une disparaît,
    c'est que la règle a cédé une troisième fois."""
    slugs = {p["slug"] for p in cl.docs_pages()}
    assert "comptes-d-equipe" in slugs, "les comptes d'équipe ne sont plus documentés"
    assert "rediger-du-contenu" in slugs, "la rédaction de contenu n'est plus documentée"


def test_la_page_contenu_parle_de_ce_qui_peut_MAL_se_passer() -> None:
    """Une page qui ne décrit que le chemin heureux est une brochure. Le client doit savoir
    d'avance ce que l'agent refuse, et pourquoi une page peut rester orpheline."""
    page = (DOCS / "rediger-du-contenu.md").read_text(encoding="utf-8").lower()
    for notion in ("orpheline", "brouillon", "refus", "canonical", "quota"):
        assert notion in page, "la page ne parle pas de %r" % notion


def test_la_page_equipe_dit_QUI_PAIE_et_avec_quel_jeton() -> None:
    """Les deux décisions que personne ne devine : le projet décide du plan, et un membre
    pousse sur GitHub avec le jeton de l'hôte."""
    page = (DOCS / "comptes-d-equipe.md").read_text(encoding="utf-8").lower()
    assert "débité sur" in page or "débitée sur" in page, "la page ne dit pas qui paie"
    assert "jeton github" in page, "la page ne dit pas avec quel compte un membre pousse"
