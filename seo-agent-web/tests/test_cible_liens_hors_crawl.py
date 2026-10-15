# -*- coding: utf-8 -*-
"""Un fichier PROPRE a une page que le crawl n'a pas signalee ne se corrige pas.

PR #1 de cityscootlab, 15/10/2026. « Page -> lien vers redirection » signale QUATRE pages (un lien
`/de/#compare`, `/fr/#deals`…). La preuve est l'URL qui redirige, `https://cityscootlab.com/de/`,
dont l'aiguille de recherche est `/de/` — presente dans TOUS les .mdx allemands, puisque chacun
lie `/de/about`, `/de/blog`… Le correcteur a donc modifie quatre fichiers de pages saines, dont
le `href: "/de/"` d'en-tete passe par `<Link>` de Next qui ote deja le slash, puis annonce seize
« Non traites » qui « resteront signales au prochain crawl ». Aucun ne l'etait. Le plafond de 40
fichiers a ete consomme pour rien, et la PR disait son perimetre « borne par le crawl ».

Un fichier qui est la source propre d'une AUTRE page ne rend que cette page-la : il ne peut pas
porter l'anomalie d'une page signalee. Un fichier PARTAGE, lui, si — un lien de menu vit dans un
composant — et il reste cible.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-cible-liens-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402
from backend import repo_index  # noqa: E402

CLE = "page_has_links_to_redirect_indexable"
B = "https://site.test"
# La forme du depot de cityscootlab : Next en export statique, une route attrape-tout par langue,
# le contenu en .mdx, un composant d'en-tete partage.
CHEMINS = [
    "package.json", "next.config.mjs",
    "app/(de)/layout.tsx", "app/(de)/de/page.tsx", "app/(de)/de/[...slug]/page.tsx",
    "content/de/index.mdx", "content/de/guide.mdx", "content/de/about.mdx",
    "components/Header.tsx", "lib/site.ts",
]
# Ce que la fouille du depot renvoie pour l'aiguille `/de/` : la page saine, le composant, la page
# signalee — dans un ordre quelconque.
TROUVES = ["content/de/about.mdx", "components/Header.tsx", "content/de/guide.mdx"]


def _cibles(signalees: list[str], *, complete: bool = True, ecartes: list[str] | None = None,
            max_files: int = 40) -> list[str]:
    return m._resolve_issue_targets(
        all_paths=CHEMINS, index=repo_index.build_repo_index(CHEMINS), issue_key=CLE,
        issue_label="Page -> lien vers redirection", impacted_urls=signalees, located=list(TROUVES),
        max_files=max_files, evidence=[f"{B}/de/"], wants_page_targeting=True, allow_ai=False,
        ecartes=ecartes, impacted_complete=complete)


def test_la_carte_lit_ce_depot_comme_celui_de_cityscootlab() -> None:
    """Precondition : sans elle, les tests suivants ne prouveraient rien."""
    index = repo_index.build_repo_index(CHEMINS)
    assert repo_index.route_files(index, f"{B}/de/guide") == ["content/de/guide.mdx"]
    assert repo_index.route_files(index, f"{B}/de/about") == ["content/de/about.mdx"]
    assert not repo_index.is_shared_path(index, "components/Header.tsx")


def test_le_fichier_d_une_page_saine_n_est_ni_cible_ni_annonce() -> None:
    ecartes: list[str] = []
    cibles = _cibles([f"{B}/de/guide"], ecartes=ecartes)

    assert cibles[0] == "content/de/guide.mdx"
    assert "content/de/about.mdx" not in cibles
    assert "content/de/about.mdx" not in ecartes


def test_un_composant_trouve_par_la_preuve_reste_cible() -> None:
    """Un lien de menu vit dans un composant, une fois pour toutes les pages."""
    assert "components/Header.tsx" in _cibles([f"{B}/de/guide"])


def test_la_page_signalee_reste_cible_meme_trouvee_en_dernier() -> None:
    cibles = _cibles([f"{B}/de/guide", f"{B}/de/about"])

    assert {"content/de/guide.mdx", "content/de/about.mdx"} <= set(cibles)


def test_une_liste_tronquee_garde_les_fichiers_des_autres_pages() -> None:
    """Le rapport ne garde que 200 pages par famille : « une autre page » peut etre une page
    signalee qu'il n'a pas gardee. Ses fichiers ne doivent pas disparaitre sans un mot."""
    assert "content/de/about.mdx" in _cibles([f"{B}/de/guide"], complete=False)


def test_une_page_que_la_carte_ne_lit_pas_garde_les_fichiers_des_autres() -> None:
    """La page inconnue de la carte a sa source quelque part : peut-etre dans un fichier que la
    carte attribue a une autre URL. Dans le doute, l'ancien comportement."""
    assert "content/de/about.mdx" in _cibles([f"{B}/de/guide", f"{B}/de/inconnue"])


def test_la_liste_complete_se_lit_dans_le_rapport() -> None:
    entiere = {CLE: {"count": 2, "examples": [f"{B}/a", f"{B}/b"]}}
    tronquee = {CLE: {"count": 250, "examples": [f"{B}/p{i}" for i in range(200)]}}

    assert m._pages_signalees_completes(entiere, CLE) is True
    assert m._pages_signalees_completes(tronquee, CLE) is False
    assert m._pages_signalees_completes({}, CLE) is True


def test_la_preparation_transmet_la_completude() -> None:
    tronquee = {CLE: {"count": 250, "examples": [f"{B}/p{i}" for i in range(200)]}}
    prep = m._prepare_issue_fix(issue_key=CLE, issues=tronquee, impacted=[f"{B}/p0"], all_paths=CHEMINS,
                                site_name="site.test", owner="o", repo_name="r", branch="main", token="t")

    assert prep["impacted_complete"] is False


def test_l_execution_transmet_la_completude_au_patcheur(monkeypatch) -> None:
    recu: dict = {}
    monkeypatch.setattr(m, "_deep_patch_issue_files", lambda **kw: recu.update(kw) or ([], [], [], []))
    prep = {"refusal": None, "loop_paths": [], "evidence": [], "extra_hint": "", "link_rewriter": None,
            "rewriter_ai_fallback": False, "rewriter_is_ai": False, "impacted_complete": False}
    m._apply_prepared_issue_fix(owner="o", repo_name="r", branch="main", token="t", fix_branch="fix",
                                all_paths=CHEMINS, issue_key=CLE, issue_label="L", impacted=[f"{B}/de/guide"],
                                site_name="site.test", file_state={}, max_files=8, prep=prep, pages=[],
                                index=None)

    assert recu["impacted_complete"] is False


def test_le_patcheur_transmet_la_completude_au_selecteur(monkeypatch) -> None:
    recu: dict = {}
    monkeypatch.setattr(m, "_resolve_issue_targets", lambda **kw: recu.update(kw) or [])
    m._deep_patch_issue_files(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="fix", all_paths=CHEMINS,
        issue_key=CLE, issue_label="L", impacted_urls=[f"{B}/de/guide"], site_name="s",
        file_state={}, max_files=8, ecartes=[], allow_ai_targeting=False, impacted_complete=False,
        link_rewriter=lambda raw: (raw, 0), rewriter_ai_fallback=False)

    assert recu["impacted_complete"] is False
