# -*- coding: utf-8 -*-
"""Un sitemap ENGENDRE par du code : le correcteur y pose un filtre noindex, et le prouve.

14/10/2026, easyshopbuilder.com. `app/sitemap.ts` liste toutes les pages de contenu, dont les
quatre accueils de blog que leur front matter met en `robots: "noindex, follow"`. Le plan
protege ne retire que des <loc> litteraux : il refusait, et la seule erreur du site restait.

Trois garde-fous, et ces tests les tiennent un par un :
1. AVANT le modele (`_plan_filtre_sitemap`) : un seul fichier de route, le noindex de chaque
   page declare dans un champ de SA source, ce champ deja lu par le generateur ;
2. APRES le modele (`garde_filtre`) : le filtre lit noindex ET ce champ, aucune URL ni motif
   d'URL, une modification courte ;
3. APRES le deploiement (`_controler_sitemap_apercu`) : l'apercu sert le sitemap du site moins
   EXACTEMENT les pages noindex — sinon la PR reste en brouillon et le dit.
"""
from __future__ import annotations

import pytest

from backend import app as m
from backend import sitemap_genere as sg
from backend.models import IssueTask
from tests.test_la_pr_attend_le_verdict_du_depot import (_brancher, _note_en_attente, _projet_avec_tache,
                                                         _verif_relue)

S = "https://site.fr"
URLS = [S + "/blog", S + "/de/blog"]
SITEMAP_TS = """import type { MetadataRoute } from 'next';
import { getAllDocMetas } from '@/lib/content';
import { SITE } from '@/lib/site';

export default async function sitemap(): Promise<MetadataRoute.Sitemap> {
  const metas = await getAllDocMetas();
  return [
    ...metas.map((m) => ({
      url: new URL(m.canonical ?? m.routePath, SITE.baseUrl).toString(),
    })),
  ];
}
"""
BON = SITEMAP_TS.replace("    ...metas.map((m) => ({",
                         "    ...metas\n      .filter((m) => !/noindex/i.test(m.robots ?? ''))\n      .map((m) => ({")
CONTENT_TS = "export async function getAllDocMetas() {\n  return docs.map((fm) => ({ robots: fm.robots, routePath: fm.path }));\n}\n"
BLOG = '---\ntitle: "Blog"\nrobots: "noindex, follow"\n---\n\n# Blog\n'
ARTICLE = '---\ntitle: "Article"\n---\n\nTexte.\n'
DEPOT = {
    "package.json": "{}", "next.config.mjs": "", "app/sitemap.ts": SITEMAP_TS, "lib/content.ts": CONTENT_TS,
    "lib/site.ts": "export const SITE = { baseUrl: 'https://site.fr' };",
    "app/(fr)/[...slug]/page.tsx": "", "app/de/[...slug]/page.tsx": "",
    "content/blog.mdx": BLOG, "content/de/blog.mdx": BLOG, "content/about.mdx": ARTICLE,
}


# --- 1. avant le modele ---------------------------------------------------------------------

@pytest.mark.parametrize("source, champ", [
    (BLOG, "robots"),
    ('---\ntitle: "x"\nnoindex: true\n---\n', "noindex"),
    ("export const metadata = { robots: { index: false, follow: true } };", "robots"),
    (ARTICLE, ""),
    ('---\nrobots: "noindex"\nnoindex: true\n---\n', ""),   # deux champs : ambigu
])
def test_le_champ_qui_porte_le_noindex(source, champ) -> None:
    assert sg.champ_noindex(source) == champ


def test_les_modules_importes_se_resolvent_alias_et_relatifs() -> None:
    chemins = {"lib/content.ts", "lib/site.ts", "app/util/index.ts"}
    src = SITEMAP_TS + "import x from './util';\nimport y from 'react';\nimport z from '@/lib/absent';\n"
    assert sg.modules_importes(src, "app/sitemap.ts", chemins) == ["lib/content.ts", "lib/site.ts", "app/util/index.ts"]


def _plan(monkeypatch, depot: dict[str, str], generateur: str = "app/sitemap.ts"):
    monkeypatch.setattr(m, "_lire_texte_du_depot", lambda o, r, b, t, chemin: depot.get(chemin))
    return m._plan_filtre_sitemap(generateur=generateur, urls=URLS, all_paths=list(depot),
                                  owner="o", repo_name="r", branch="main", token="t")


def test_le_cas_easyshopbuilder_donne_un_plan(monkeypatch) -> None:
    plan = _plan(monkeypatch, DEPOT)

    assert plan["targets_override"] == ["app/sitemap.ts"]
    assert plan["sitemap_noindex_urls"] is None          # la piste generale, pas le plan litteral
    assert plan["sitemap_filtre"] == {"champ": "robots", "urls": URLS, "generateur": "app/sitemap.ts"}
    assert "`lib/content.ts` transmet deja" in plan["extra_hint"]


@pytest.mark.parametrize("modif, motif", [
    ({"lib/content.ts": "export async function getAllDocMetas() { return docs; }"}, "ne lit pas le champ"),
    ({"content/de/blog.mdx": ARTICLE}, "ne declare pas son noindex"),
    ({"content/de/blog.mdx": '---\nnoindex: true\n---\n'}, "champs differents"),
    ({"app/sitemap.ts": None}, "illisible"),
])
def test_sans_preuve_on_s_abstient(monkeypatch, modif, motif) -> None:
    assert motif in _plan(monkeypatch, {**DEPOT, **modif})


def test_un_paquet_n_a_pas_de_fichier_ou_poser_le_filtre(monkeypatch) -> None:
    assert "sans fichier de route" in _plan(monkeypatch, DEPOT, generateur="@astrojs/sitemap")


def test_une_route_qui_n_est_pas_un_fichier_sitemap_reste_hors_champ(monkeypatch) -> None:
    # SvelteKit rend son sitemap par `+server.ts` : present dans le depot, mais pas la forme
    # dont on sait lire la construction. Abstention plutot que devinette.
    route = "src/routes/sitemap.xml/+server.ts"
    assert "sans fichier de route" in _plan(monkeypatch, {**DEPOT, route: SITEMAP_TS}, generateur=route)


def test_sans_page_prouvee_rien_a_exclure(monkeypatch) -> None:
    monkeypatch.setattr(m, "_lire_texte_du_depot", lambda *a: pytest.fail("rien a lire"))
    assert "Aucune page noindex" in m._plan_filtre_sitemap(
        generateur="app/sitemap.ts", urls=[], all_paths=list(DEPOT), owner="o", repo_name="r",
        branch="main", token="t")


def _pages() -> list[dict]:
    return [{"url": u, "final_url": u, "status_code": 200, "content_type": "text/html",
             "meta_robots": "noindex, follow", "meta_robots_tag_count": 1} for u in URLS]


def test_la_preparation_passe_par_le_plan_quand_aucun_xml_n_est_ecrit(monkeypatch) -> None:
    monkeypatch.setattr(m, "_lire_texte_du_depot", lambda o, r, b, t, chemin: DEPOT.get(chemin))
    prep = m._prepare_issue_fix(issue_key="sitemap_noindex_page", issues={"sitemap_noindex_page": {"count": 2}},
                                impacted=URLS, all_paths=list(DEPOT), site_name="site.fr", owner="o",
                                repo_name="r", branch="main", token="t", pages=_pages())

    assert not prep["refusal"] and prep["sitemap_filtre"]["champ"] == "robots"


def test_un_xml_ecrit_a_la_main_a_cote_reste_ambigu(monkeypatch) -> None:
    monkeypatch.setattr(m, "_lire_texte_du_depot", lambda *a: pytest.fail("rien a lire"))
    prep = m._prepare_issue_fix(issue_key="sitemap_noindex_page", issues={"sitemap_noindex_page": {"count": 2}},
                                impacted=URLS, all_paths=[*DEPOT, "public/sitemap.xml"], site_name="site.fr",
                                owner="o", repo_name="r", branch="main", token="t", pages=_pages())

    assert "engendre, absent ou ambigu" in prep["refusal"]


# --- 2. apres le modele ---------------------------------------------------------------------

def test_le_bon_filtre_passe() -> None:
    assert sg.garde_filtre(SITEMAP_TS, BON, URLS, "robots") == ""


@pytest.mark.parametrize("remplacement, motif", [
    (".filter((m) => !m.routePath.endsWith('/blog') && !/noindex/.test(m.robots))", "URL ecrite en dur"),
    (".filter((m) => !m.routePath.startsWith('/de/') && !/noindex/.test(m.robots))", "motif d'URL"),
    (".filter((m) => m.routePath !== 'x')", "aucun filtre noindex"),
    (".filter((m) => !/noindex/.test(m.flags))", "ne lit pas le champ"),
])
def test_un_mauvais_filtre_est_refuse(remplacement, motif) -> None:
    mauvais = SITEMAP_TS.replace("    ...metas.map((m) => ({", "    ...metas%s.map((m) => ({" % remplacement)
    assert motif in sg.garde_filtre(SITEMAP_TS, mauvais, URLS, "robots")


def test_une_reecriture_large_est_refusee() -> None:
    large = BON + "\n".join("// ligne %d" % i for i in range(13))
    assert "trop large" in sg.garde_filtre(SITEMAP_TS, large, URLS, "robots")


def _corriger(monkeypatch, reponse: str) -> tuple[list[str], list[str]]:
    monkeypatch.setattr(m, "_openai_generate_file_patch", lambda **kw: {"patched_content": reponse})
    monkeypatch.setattr(m, "_github_api_put",
                        lambda chemin, **k: {"content": {"sha": "n"}, "commit": {"sha": "a", "html_url": ""}})
    patched, skipped, _t, _ai = m._deep_patch_issue_files(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="fix", all_paths=list(DEPOT),
        issue_key="sitemap_noindex_page", issue_label="Sitemap noindex", impacted_urls=URLS,
        site_name="site.fr", file_state={"app/sitemap.ts": {"sha": "v", "content": SITEMAP_TS}},
        max_files=2, targets_override=["app/sitemap.ts"], allow_ai_targeting=False,
        sitemap_filtre={"champ": "robots", "urls": URLS, "generateur": "app/sitemap.ts"})
    return patched, skipped


def test_la_boucle_commite_le_bon_filtre(monkeypatch) -> None:
    assert _corriger(monkeypatch, BON)[0] == ["app/sitemap.ts"]


def test_la_boucle_refuse_un_filtre_par_url(monkeypatch) -> None:
    mauvais = SITEMAP_TS.replace("    ...metas.map((m) => ({",
                                 "    ...metas.filter((m) => !['/blog', '/de/blog'].includes(m.routePath)).map((m) => ({")
    patched, skipped = _corriger(monkeypatch, mauvais)
    assert patched == [] and skipped == ["app/sitemap.ts"]


def test_les_deux_routes_posent_le_controle_sur_la_trace_de_verification() -> None:
    """Sans lui, la sortie de brouillon ne regarderait que le build — qui passe tres bien avec un
    filtre qui retire trop ou rien."""
    import inspect
    for route in (m.api_github_bulk_fix, m.api_issue_deep_fix):
        src = inspect.getsource(route)
        assert "_controle_sitemap_de(_prep" in src, route.__name__
        assert '"controle_sitemap":' in src, route.__name__


def test_le_controle_ne_part_que_si_le_generateur_a_ete_commite() -> None:
    prep = {"sitemap_filtre": {"champ": "robots", "urls": URLS, "generateur": "app/sitemap.ts"}}
    assert m._controle_sitemap_de(prep, ["app/sitemap.ts"]) == {"urls": URLS}
    assert m._controle_sitemap_de(prep, ["content/x.mdx"]) is None
    assert m._controle_sitemap_de({}, ["app/sitemap.ts"]) is None
    assert m._bloc_verification({"head": {"sha": "s"}}, fusion_auto=False,
                                controle_sitemap={"urls": URLS})["controle_sitemap"] == {"urls": URLS}


# --- 3. apres le deploiement ----------------------------------------------------------------

URLSET = ('<?xml version="1.0"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
          + "".join("<url><loc>https://site.fr%s</loc></url>" % p for p in ("/", "/about", "/blog", "/de/blog")) + "</urlset>")
INDEX = ('<?xml version="1.0"?><sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
         "<sitemap><loc>https://site.fr/sitemap-0.xml</loc></sitemap></sitemapindex>")


def test_le_sitemap_se_lit_urlset_et_index() -> None:
    assert sg.chemins_sitemap(URLSET) == ({"/", "/about", "/blog", "/de/blog"}, [])
    assert sg.chemins_sitemap(INDEX) == (set(), ["https://site.fr/sitemap-0.xml"])
    with pytest.raises(Exception):
        sg.chemins_sitemap("<html></html>")


@pytest.mark.parametrize("apercu, motif", [
    ({"/", "/about"}, ""),
    ({"/", "/about", "/new"}, ""),                      # une page ajoutee ailleurs ne bloque pas
    ({"/", "/about", "/blog"}, "liste encore : /blog"),
    ({"/"}, "perdu 1 page(s) indexable(s) : /about"),
])
def test_l_ecart_entre_le_site_et_l_apercu(apercu, motif) -> None:
    ecart = sg.ecart_apercu({"/", "/about", "/blog", "/de/blog"}, apercu, URLS)
    assert (motif in ecart) if motif else ecart == ""


def test_seuls_les_apercus_des_hebergeurs_connus_sont_lus(monkeypatch) -> None:
    def faux_get(chemin, **kw):
        if chemin.endswith("/status"):
            return {"statuses": [
                {"state": "success", "target_url": "https://deploy-preview-2--site.netlify.app"},
                {"state": "success", "target_url": "http://169.254.169.254/latest"},
                {"state": "success", "target_url": "https://app.netlify.com/sites/x/deploys/1"},
                {"state": "pending", "target_url": "https://deploy-preview-3--site.netlify.app"}]}
        if chemin.endswith("/deployments"):
            return [{"id": 9}]
        return [{"state": "success", "environment_url": "https://site-git-fix.vercel.app"}]
    monkeypatch.setattr(m, "_github_api_get", faux_get)

    assert m._apercus_du_commit(owner="o", repo="r", sha="s", token="t") == [
        "https://deploy-preview-2--site.netlify.app", "https://site-git-fix.vercel.app"]


@pytest.mark.parametrize("apercus, servis, attendu", [
    ([], {}, "attendre"),
    (["https://p.netlify.app"], {"https://site.fr": {"/", "/about", "/blog"}, "https://p.netlify.app": {"/", "/about"}}, "ok"),
    (["https://p.netlify.app"], {"https://site.fr": {"/", "/about", "/blog"}, "https://p.netlify.app": {"/", "/blog"}}, "echec"),
    (["https://p.netlify.app"], {"https://site.fr": {"/"}}, "illisible"),
])
def test_le_controle_de_l_apercu(monkeypatch, apercus, servis, attendu) -> None:
    monkeypatch.setattr(m, "_apercus_du_commit", lambda **kw: apercus)
    monkeypatch.setattr(m, "_chemins_du_sitemap_servi", lambda origine: servis.get(origine))

    etat, _raison = m._controler_sitemap_apercu({"urls": URLS}, owner="o", repo="r", sha="s",
                                                token="t", base_url="https://site.fr/")
    assert etat == attendu


def _reprendre(monkeypatch, etat_controle: str, *, fusion_auto: bool = False):
    note = _note_en_attente(fusion_auto=fusion_auto)
    note["verification"]["controle_sitemap"] = {"urls": URLS}
    _uid, tid = _projet_avec_tache(note=note)
    fait = _brancher(monkeypatch, check_runs=[{"status": "completed", "conclusion": "success", "name": "build"}])
    monkeypatch.setattr(m, "_controler_sitemap_apercu",
                        lambda controle, **kw: (etat_controle, "motif-" + etat_controle))
    with m.DB.session() as db:
        decision = m._reprendre_une_verification(db.get(IssueTask, tid))
    verif, _statut = _verif_relue(tid)
    return decision, verif, fait


def test_un_apercu_qui_dement_le_filtre_laisse_la_pr_en_brouillon(monkeypatch) -> None:
    decision, verif, fait = _reprendre(monkeypatch, "echec", fusion_auto=True)

    assert decision == "echouee" and "motif-echec" in verif["raison"]
    assert fait["prete"] == 0 and fait["fusions"] == 0 and fait["commentaires"]


def test_un_apercu_qui_confirme_sort_la_pr_et_le_dit(monkeypatch) -> None:
    decision, verif, fait = _reprendre(monkeypatch, "ok")

    assert decision == "verifiee" and "motif-ok" in verif["raison"] and fait["prete"] == 1


def test_un_apercu_illisible_n_est_jamais_verifie_ni_fusionne(monkeypatch) -> None:
    decision, verif, fait = _reprendre(monkeypatch, "illisible", fusion_auto=True)

    assert decision == "non_verifiable" and fait["fusions"] == 0


def test_sans_apercu_on_attend(monkeypatch) -> None:
    decision, _verif, fait = _reprendre(monkeypatch, "attendre")

    assert decision == "attendre" and fait["prete"] == 0
