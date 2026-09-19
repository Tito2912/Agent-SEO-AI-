# -*- coding: utf-8 -*-
"""Un bloc `openGraph` que CE run ajoute doit etre complet — quel qu'en soit l'auteur.

MESURE DU 19/09/2026, PULL REQUEST REELLE SUR oryvalo.com. Le modele a pose sur
`app/contact/page.tsx` :

    openGraph: { url: 'https://oryvalo.com/contact', images: [{ ... }] },

Ni `type` ni `siteName`. Les metadonnees Next sont fusionnees SUPERFICIELLEMENT : ce bloc
REMPLACE celui de la mise en page, donc la page perdait `og:type` et `og:site_name`. Une
anomalie troquee contre `open_graph_tags_incomplete`, l'apercu de partage avec — exactement le
dommage que la question du proprietaire sur la PR #12 avait evite le matin meme.

DEUX GARDES EXISTAIENT ET AUCUNE N'A RIEN VU. La lecon n'est pas qu'il en manquait une.

- `_complete_open_graph` enonce ce risque MOT POUR MOT dans sa docstring (« UNE ANOMALIE
  TROQUEE CONTRE UNE AUTRE »). Il ne lit que `<meta property="og:...">`. Le meme defaut, dans
  l'idiome JS/TS, lui est invisible. **Une garde juste peut surveiller le mauvais endroit.**
- `_inserer_og_complet` s'abstient « quand un openGraph existe deja ». Cette abstention a ete
  ecrite pour un bloc DU CLIENT, delibere, dans lequel on refuse de s'inserer. Elle s'est
  appliquee a un bloc que le modele venait d'ecrire trois lignes plus tot dans le MEME run.
  **Une abstention qui protege le travail du client protege aussi l'erreur du modele tant
  qu'elle ne sait pas les distinguer.** D'ou le test sur `old_content` : seul ce qui n'etait
  pas la avant est du ressort de ce run.

POURQUOI RETIRER PLUTOT QUE LAISSER INCOMPLET, contrairement a l'idiome HTML. En HTML les
balises sont independantes : une de plus est un gain meme si les cinq n'y sont pas. Ici le
bloc REMPLACE celui de la mise en page, donc un bloc incomplet DETRUIT des balises existantes.
Le retirer rend la page a son anomalie d'origine, qui est la moins grave des deux.

SECOND DEFAUT DE LA MEME PULL REQUEST : le fichier entier a change de fins de ligne, 16 CRLF
avant, 18 LF apres. Dix-sept lignes de diff pour un ajout d'une ligne. Un diff illisible est un
diff qu'on approuve sans le lire, ce qui annule la revue humaine sur laquelle repose tout le
reste du dispositif.
"""

from __future__ import annotations

import base64
import os
import sys
import tempfile
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-og-objet-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402

# La mise en page d'oryvalo, recopiee.
LAYOUT = """export const metadata: Metadata = {
  openGraph: {
    type: "website",
    title: "Oryvalo",
    url: getSiteUrl(),
    siteName: "Oryvalo",
    images: [{ url: "/opengraph-image", width: 1200, height: 630, alt: "Oryvalo" }],
  },
};
"""

# `app/contact/page.tsx` tel qu'il etait AVANT la correction.
AVANT = """import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Contact',
  alternates: { canonical: '/contact' },
};
"""

# Ce que le modele a reellement ecrit, au caractere pres.
APRES_MODELE = AVANT.replace(
    "  alternates: { canonical: '/contact' },",
    "  alternates: { canonical: '/contact' },\n"
    "  openGraph: { url: 'https://oryvalo.com/contact',"
    " images: [{ url: \"/opengraph-image\", width: 1200, height: 630, alt: \"Oryvalo\" }] },")


def _depuis_le_layout():
    return m._og_a_reporter_depuis_layout(LAYOUT)


# --- le cas reel ---------------------------------------------------------------------------

def test_le_bloc_du_modele_est_COMPLETE_au_lieu_d_etre_livre_mutile() -> None:
    """La reproduction exacte de la pull request du 19/09/2026."""
    sortie, notes = m._completer_open_graph_objet(APRES_MODELE, AVANT, _depuis_le_layout)
    assert any("complete" in n for n in notes), notes
    for attendu in ('type: "website"', 'siteName: "Oryvalo"'):
        assert attendu in sortie, "%r manque :\n%s" % (attendu, sortie)
    assert sortie.count("openGraph") == 1


def test_le_resultat_reste_un_fichier_equilibre() -> None:
    sortie, _n = m._completer_open_graph_objet(APRES_MODELE, AVANT, _depuis_le_layout)
    assert sortie.count("{") == sortie.count("}")
    assert sortie.count("[") == sortie.count("]")


def test_la_mise_en_forme_du_bloc_est_CLONEE_pas_imposee() -> None:
    """Un bloc sur une ligne reste sur une ligne ; reformater produirait un diff que le
    formateur du client reecrirait au commit suivant."""
    sortie, _n = m._completer_open_graph_objet(APRES_MODELE, AVANT, _depuis_le_layout)
    ligne = [l for l in sortie.splitlines() if "openGraph" in l]
    assert len(ligne) == 1, sortie
    assert ligne[0].rstrip().endswith("},"), ligne[0]


def test_un_bloc_INDENTE_recoit_des_lignes_indentees() -> None:
    multi = AVANT.replace(
        "  alternates: { canonical: '/contact' },",
        "  alternates: { canonical: '/contact' },\n"
        "  openGraph: {\n    url: '/contact',\n"
        "    images: [{ url: \"/x\" }],\n  },")
    sortie, _n = m._completer_open_graph_objet(multi, AVANT, _depuis_le_layout)
    assert '\n    type: "website",' in sortie, sortie


# --- og:url reprend le LITTERAL du canonical -------------------------------------------------

def test_l_url_en_dur_du_modele_devient_le_litteral_du_canonical() -> None:
    """MESURE DU 19/09/2026 sur une reproduction Next.js, les trois configurations :

        avec metadataBase, litteral relatif   canonical et og:url -> https://exemple.fr/x
        avec metadataBase, litteral absolu    canonical et og:url -> https://exemple.fr/x
        SANS metadataBase, litteral relatif   canonical et og:url -> /x

    Next resout les DEUX de la meme facon ou n'en resout AUCUN. Deux litteraux identiques
    rendent donc deux valeurs identiques PARTOUT — l'anomalie est fermee par construction.
    L'URL en dur, elle, n'est juste que tant que la base du site ne bouge pas : elle diverge
    sur un apercu (`NEXT_PUBLIC_SITE_URL` different) et definitivement a un changement de
    domaine.
    """
    sortie, notes = m._completer_open_graph_objet(APRES_MODELE, AVANT, _depuis_le_layout)
    assert "url: '/contact'" in sortie, sortie
    assert "https://oryvalo.com/contact" not in sortie, sortie
    assert any("canonical" in n for n in notes), notes


def test_ce_n_est_PAS_une_preference_pour_le_relatif() -> None:
    """Un canonical absolu donne un og:url absolu. La regle est « le MEME litteral », pas
    « toujours relatif » — confondre les deux casserait les sites qui ecrivent en absolu."""
    absolu = AVANT.replace("canonical: '/contact'", "canonical: 'https://oryvalo.com/contact'")
    pose = absolu.replace(
        "  alternates: { canonical: 'https://oryvalo.com/contact' },",
        "  alternates: { canonical: 'https://oryvalo.com/contact' },\n"
        "  openGraph: { url: '/autre',"
        " images: [{ url: \"/opengraph-image\" }] },")
    sortie, _n = m._completer_open_graph_objet(pose, absolu, _depuis_le_layout)
    assert "url: 'https://oryvalo.com/contact'" in sortie, sortie


def test_le_guillemet_du_canonical_est_repris_avec_la_valeur() -> None:
    """Imposer nos guillemets produirait un diff que le formateur du client reecrirait."""
    dq = AVANT.replace("canonical: '/contact'", 'canonical: "/contact"')
    pose = dq.replace(
        '  alternates: { canonical: "/contact" },',
        '  alternates: { canonical: "/contact" },\n'
        "  openGraph: { url: '/faux', images: [{ url: \"/x\" }] },")
    sortie, _n = m._completer_open_graph_objet(pose, dq, _depuis_le_layout)
    assert 'url: "/contact"' in sortie, sortie


@pytest.mark.parametrize("nom, page", [
    ("plusieurs pages dans un fichier", AVANT.replace(
        "  alternates: { canonical: '/contact' },",
        "  alternates: { canonical: '/contact' },\n  alternates: { canonical: '/x' },")),
    ("valeur assemblee", AVANT.replace("'/contact'", '"${BASE}/contact"')),
    ("aucun canonical", AVANT.replace("  alternates: { canonical: '/contact' },\n", "")),
])
def test_sans_canonical_LISIBLE_l_url_du_modele_est_laissee(nom, page) -> None:
    """Memes abstentions que le reste de la famille : on ne devine pas une valeur."""
    pose = page.replace(
        "  title: 'Contact',",
        "  title: 'Contact',\n  openGraph: { url: 'https://oryvalo.com/contact',"
        " images: [{ url: \"/x\" }], type: \"website\" },")
    sortie, _n = m._completer_open_graph_objet(pose, page, _depuis_le_layout)
    assert "url: 'https://oryvalo.com/contact'" in sortie, "%s :\n%s" % (nom, sortie)


def test_une_url_DEJA_conforme_ne_produit_aucun_changement() -> None:
    deja = AVANT.replace(
        "  alternates: { canonical: '/contact' },",
        "  alternates: { canonical: '/contact' },\n"
        "  openGraph: { url: '/contact', type: \"website\","
        " siteName: \"Oryvalo\","
        " images: [{ url: \"/opengraph-image\", width: 1200, height: 630, alt: \"Oryvalo\" }] },")
    sortie, notes = m._completer_open_graph_objet(deja, AVANT, _depuis_le_layout)
    assert sortie == deja and not notes, notes


# --- un seul scanner de litteral d'objet ------------------------------------------------------

def test_les_bornes_et_la_valeur_viennent_du_MEME_scanner() -> None:
    """Lire une valeur et la remplacer posent la meme question. Deux fonctions qui y repondent
    chacune de leur cote finissent par diverger — trois defauts de cette forme aujourd'hui."""
    bloc = '{ type: "website", images: [{ url: "/x", alt: "a, b" }], url: \'/p\' }'
    spans = m._spans_des_cles_objet(bloc)
    assert set(spans) == {"type", "images", "url"}
    for cle, (d, f) in spans.items():
        assert bloc[d:f].strip() == m._valeur_de_cle_objet(bloc, cle), cle


def test_remplacer_la_DERNIERE_cle_ne_mange_pas_l_espace_avant_l_accolade() -> None:
    """Une mutation a survecu ici : mes blocs de test avaient tous `url` au MILIEU.

    La valeur du dernier element va jusqu'a l'accolade fermante, espace compris. Sans rognage
    a droite, la remplacer avale cet espace et rend `url: '/contact'}` — le formateur du
    client le reecrirait au commit suivant, et on aurait produit du bruit dans SON historique
    pour rien.
    """
    # Bloc DEJA complet : rien ne sera ajouté, donc `url` reste le dernier élément et seule
    # sa valeur change. C'est la seule façon d'isoler ce qu'on mesure ici.
    pose = AVANT.replace(
        "  alternates: { canonical: '/contact' },",
        "  alternates: { canonical: '/contact' },\n"
        "  openGraph: { type: \"website\", siteName: \"Oryvalo\","
        " images: [{ url: \"/opengraph-image\", width: 1200, height: 630, alt: \"Oryvalo\" }],"
        " url: '/faux' },")
    sortie, _n = m._completer_open_graph_objet(pose, AVANT, _depuis_le_layout)
    assert "url: '/contact' }" in sortie, "l'espace avant `}` a disparu :\n%s" % sortie


def test_une_virgule_DANS_UNE_CHAINE_ne_coupe_pas_une_cle() -> None:
    """`alt: "a, b"` : une virgule entre guillemets n'est pas un separateur de propriete."""
    bloc = '{ url: \'/p\', images: [{ alt: "a, b" }] }'
    assert set(m._spans_des_cles_objet(bloc)) == {"url", "images"}


# --- ce qu'on REFUSE de livrer ---------------------------------------------------------------

def test_sans_type_ni_images_disponibles_le_bloc_est_RETIRE() -> None:
    """Le bloc detruirait des balises sans pouvoir les remplacer : la page repart avec son
    anomalie d'origine, qui est la moins grave des deux."""
    pauvre = lambda: {"siteName": '"Oryvalo"'}  # noqa: E731
    nu = AVANT.replace(
        "  alternates: { canonical: '/contact' },",
        "  alternates: { canonical: '/contact' },\n  openGraph: { url: '/contact' },")
    sortie, notes = m._completer_open_graph_objet(nu, AVANT, pauvre)
    assert "openGraph" not in sortie, sortie
    assert notes and "retire" in notes[0], notes
    assert sortie == AVANT, "le retrait doit rendre le fichier à son état d'avant :\n%s" % sortie


def test_un_openGraph_DEJA_PRESENT_avant_le_run_n_est_pas_touche() -> None:
    """C'est un choix du client. L'abstention est juste — c'est de l'avoir appliquée au bloc
    que le modèle venait d'écrire qui était faux."""
    avec = AVANT.replace(
        "  alternates: { canonical: '/contact' },",
        "  alternates: { canonical: '/contact' },\n  openGraph: { url: '/contact' },")
    sortie, notes = m._completer_open_graph_objet(avec, avec, _depuis_le_layout)
    assert sortie == avec and not notes


def test_sans_openGraph_dans_la_mise_en_page_on_ne_touche_a_rien() -> None:
    """Aucun bloc n'est masque, donc rien n'est detruit : le cas revient a l'idiome HTML."""
    sortie, notes = m._completer_open_graph_objet(APRES_MODELE, AVANT, lambda: {})
    assert sortie == APRES_MODELE and not notes


def test_un_bloc_DEJA_complet_ne_bouge_pas() -> None:
    complet = AVANT.replace(
        "  alternates: { canonical: '/contact' },",
        "  alternates: { canonical: '/contact' },\n"
        "  openGraph: { type: \"website\", siteName: \"O\", images: [{ url: \"/x\" }],"
        " url: '/contact' },")
    sortie, notes = m._completer_open_graph_objet(complet, AVANT, _depuis_le_layout)
    assert sortie == complet and not notes


def test_une_cle_IMBRIQUEE_ne_compte_pas_comme_presente() -> None:
    """`images: [{ url: ... }]` contient `url`, mais au second niveau. Confondre les deux
    ferait croire un bloc complet alors qu'il lui manque une cle de premier niveau."""
    bloc = '{ images: [{ url: "/x", type: "image/png" }] }'
    assert m._cles_du_bloc_objet(bloc) == {"images"}


# --- les fins de ligne ------------------------------------------------------------------------

@pytest.mark.parametrize("nom, original, attendu_crlf", [
    ("fichier CRLF", "a\r\nb\r\nc\r\n", True),
    ("fichier LF", "a\nb\nc\n", False),
    ("fichier mixte à dominante CRLF (le cas d'oryvalo : 16 contre 1)",
     "a\r\nb\r\nc\r\nd\r\ne\n", True),
    ("fichier mixte à dominante LF", "a\nb\nc\nd\ne\r\n", False),
])
def test_le_fichier_garde_SES_fins_de_ligne(nom, original, attendu_crlf) -> None:
    sortie = m._respecter_les_fins_de_ligne(original, "x\ny\nz\n")
    assert (("\r\n" in sortie) is attendu_crlf), "%s : %r" % (nom, sortie)
    assert sortie.replace("\r\n", "\n") == "x\ny\nz\n", "le contenu a changé : %r" % sortie


def test_un_fichier_sans_saut_de_ligne_est_rendu_tel_quel() -> None:
    assert m._respecter_les_fins_de_ligne("une ligne", "autre\nligne") == "autre\nligne"


def test_convertir_ne_perd_ni_n_ajoute_de_ligne() -> None:
    """Une conversion qui doublerait les CR produirait un fichier a moitie binaire."""
    sortie = m._respecter_les_fins_de_ligne("a\r\nb\r\n", "1\r\n2\n3\r\n")
    assert sortie == "1\r\n2\r\n3\r\n", repr(sortie)


# --- la BOUCLE, parce que deux fonctions justes reliees par rien ne corrigent rien -----------

def test_la_boucle_commite_un_bloc_complet_ET_les_bonnes_fins_de_ligne(monkeypatch) -> None:
    """Le test qui aurait attrape la pull request du 19/09/2026.

    Il fait tourner `_deep_patch_issue_files` avec un reecriveur qui se comporte comme le
    modele ce jour-la — il pose un openGraph incomplet — et lit ce qui part reellement chez
    GitHub. Des tests unitaires verts pendant qu'une correction abime un fichier, ce projet en
    a deja fait l'experience deux fois aujourd'hui.
    """
    commits: dict[str, str] = {}

    monkeypatch.setattr(
        m, "_github_api_get",
        lambda chemin, **k: {"content": base64.b64encode(LAYOUT.encode()).decode()})

    def _faux_put(chemin, *, token, json_body):
        commits[chemin] = base64.b64decode(json_body["content"]).decode("utf-8")
        return {"content": {"sha": "n"}, "commit": {"sha": "a", "html_url": ""}}

    monkeypatch.setattr(m, "_github_api_put", _faux_put)

    # Le fichier du client est en CRLF, comme celui d'oryvalo.
    avant_crlf = AVANT.replace("\n", "\r\n")

    def _comme_le_modele(brut: str) -> tuple[str, int]:
        return APRES_MODELE, 1

    patched, _skipped, _targets, _ai = m._deep_patch_issue_files(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="fix",
        all_paths=["app/contact/page.tsx", "app/layout.tsx"],
        issue_key="open_graph_url_not_matching_canonical", issue_label="OG",
        impacted_urls=["https://oryvalo.com/contact"], site_name="oryvalo",
        file_state={"app/contact/page.tsx": {"sha": "v", "content": avant_crlf}},
        max_files=4, link_rewriter=_comme_le_modele, rewriter_ai_fallback=False,
        targets_override=["app/contact/page.tsx"], allow_ai_targeting=False)

    assert patched == ["app/contact/page.tsx"], patched
    assert len(commits) == 1, commits.keys()
    ecrit = next(iter(commits.values()))
    assert 'type: "website"' in ecrit, "le bloc parti chez GitHub est incomplet :\n%s" % ecrit
    assert 'siteName: "Oryvalo"' in ecrit, ecrit
    assert "url: '/contact'" in ecrit and "https://oryvalo.com" not in ecrit, (
        "l'URL en dur du modèle est partie telle quelle :\n%s" % ecrit)
    assert "\r\n" in ecrit and ecrit.count("\n") == ecrit.count("\r\n"), (
        "les fins de ligne du client n'ont pas été respectées : %r" % ecrit[:80])
