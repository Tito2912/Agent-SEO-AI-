# -*- coding: utf-8 -*-
"""Un lien de sommaire écrit par le modèle mène à un titre de la page, ou il n'est pas là.

MESURE DU 27/09/2026, première page multilingue en production (PR #7 de prosperfactory.com) :
5 liens de sommaire morts sur 40, contre 0 sur toutes les pages écrites à la main du site.

    « Axe 1 — Classes d’actifs »  le modèle écrit `classes-d-actifs`, le site RETIRE
                                  l'apostrophe et produit `classes-dactifs` ;
    « Schritt 1 — Asset‑Klassen»  tiret insécable U+2011, même histoire ;
    « Rééquilibrer »              le modèle écrit `#rebalancer`.

Build vert, page qui se lit bien, lien qui ne mène nulle part.

Les fixtures reprennent les formes mesurées : la sœur d'un dossier ne DÉPARTAGE souvent pas
les règles (ses titres n'ont ni apostrophe ni tiret spécial), et il faut une voisine pour
trancher.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")

from backend import app as m  # noqa: E402


def _page(liens: list[str], titres: list[str]) -> str:
    tete = "---\ntitle: \"x\"\njumpLinks:\n" + "".join(
        '  - href: "#%s"\n    label: "%s"\n' % (a, a) for a in liens) + "---\n\n"
    return tete + "".join("## %s\n\nTexte.\n\n" % t for t in titres)


# Ne départage rien : « supprime » et « remplace » donnent les mêmes identifiants.
SOEUR = _page(["pourquoi", "frais-et-retraits"], ["Pourquoi", "Frais et retraits"])
# Départage les trois : l'apostrophe est RETIRÉE (contre « remplace »), l'accent et le tiret
# cadratin aussi (contre github-slugger, qui garde `é` et double le tiret). Forme mesurée sur
# `debuter-investissement`, la page du site client qui tranchait.
VOISINE = _page(["etape-1-les-bases-dun-plan"], ["Étape 1 — Les bases d’un plan"])
# Ne départage que github-slugger : « supprime » et « remplace » restent ex aequo.
SANS_APOSTROPHE = _page(["etape-2-frais"], ["Étape 2 — Frais"])

NEUVE = _page(["pourquoi-diversifier", "axe-1-classes-d-actifs", "rebalancer"],
              ["Pourquoi diversifier", "Axe 1 — Classes d’actifs", "Rééquilibrer"])


def _ancres(contenu: str) -> list[str]:
    return m._titres_et_ancres(contenu)[1]


def test_une_ancre_ecrite_dans_une_AUTRE_regle_est_remise_dans_celle_du_site() -> None:
    out, notes = m._ancres_du_sommaire(NEUVE, exemples=[SOEUR], plus_d_exemples=lambda: [VOISINE])
    assert "axe-1-classes-dactifs" in _ancres(out), out
    assert "axe-1-classes-d-actifs" not in _ancres(out)
    assert any("remise" in n for n in notes), notes


def test_une_ancre_qu_AUCUN_titre_n_explique_est_retiree_avec_son_libelle() -> None:
    out, notes = m._ancres_du_sommaire(NEUVE, exemples=[SOEUR], plus_d_exemples=lambda: [VOISINE])
    assert "rebalancer" not in out, out
    assert _ancres(out) == ["pourquoi-diversifier", "axe-1-classes-dactifs"], out
    assert m._front_matter_parse_error(out, "a.mdx") == ""
    assert "## Rééquilibrer" in out, "le TITRE du corps ne se retire pas, seul le lien"


def test_une_ancre_JUSTE_n_est_jamais_touchee() -> None:
    out, _notes = m._ancres_du_sommaire(NEUVE, exemples=[SOEUR], plus_d_exemples=lambda: [VOISINE])
    assert "pourquoi-diversifier" in _ancres(out)


def test_a_EGALITE_seules_les_regles_de_tete_votent() -> None:
    """Régression de la PR #7 : github-slugger fait `schritt-0--ziel` de « Schritt 0 — Ziel ».
    Battue par la mesure, elle ne doit pas faire retirer une ancre que les deux règles de
    tête acceptent — ma première version en retirait 8 justes sur 10."""
    neuve = _page(["schritt-0-ziel", "schritt-1-asset-klassen"],
                  ["Schritt 0 — Ziel", "Schritt 1 — Asset‑Klassen"])
    out, notes = m._ancres_du_sommaire(neuve, exemples=[SOEUR],
                                       plus_d_exemples=lambda: [SANS_APOSTROPHE])
    assert _ancres(out) == ["schritt-0-ziel"], out
    # Et aucune note ne la cite : une ancre juste « remise » sur elle-meme laisserait croire au
    # relecteur que le modele s'etait trompe.
    assert not any("schritt-0-ziel" in n for n in notes), notes


def test_sans_regle_mesurable_l_ancre_AMBIGUE_est_retiree_pas_devinee() -> None:
    """Le tiret insécable : `asset-klassen` ou `assetklassen` selon la règle, et aucune page
    ne dit laquelle. Retirer se voit ; deviner faux ne se voit pas."""
    neuve = _page(["schritt-1-asset-klassen", "schritt-0-ziel"],
                  ["Schritt 0 — Ziel", "Schritt 1 — Asset‑Klassen"])
    out, notes = m._ancres_du_sommaire(neuve, exemples=[SOEUR],
                                       plus_d_exemples=lambda: [SANS_APOSTROPHE])
    # L'entree retiree est la PREMIERE : celle qui la suit doit survivre intacte, libelle
    # compris — un retrait qui deborde emporterait une ancre juste.
    assert _ancres(out) == ["schritt-0-ziel"], out
    assert 'label: "schritt-0-ziel"' in out, out
    assert "ne se mesure pas" in notes[0], notes


def test_la_soeur_qui_TRANCHE_suffit_sans_lire_d_autres_pages() -> None:
    lues: list[int] = []
    out, _notes = m._ancres_du_sommaire(
        NEUVE, exemples=[VOISINE], plus_d_exemples=lambda: (lues.append(1), [])[1])
    assert lues == [], "des pages ont été lues alors que la sœur départageait déjà"
    assert "axe-1-classes-dactifs" in _ancres(out)


def test_une_page_SANS_ancre_ne_coute_aucune_lecture() -> None:
    lues: list[int] = []
    page = "---\ntitle: \"x\"\n---\n\n## Un titre\n"
    out, notes = m._ancres_du_sommaire(
        page, exemples=[SOEUR], plus_d_exemples=lambda: (lues.append(1), [])[1])
    assert (out, notes, lues) == (page, [], [])


def test_une_ancre_TRANSLITTEREE_a_l_allemande_est_remise_pas_retiree() -> None:
    """Mesure du 28/09/2026 : `verstaerkt` pour « verstärkt », `haeufige` pour « Häufige ».
    Le site décompose et retire l'accent (`verstarkt`) ; deux entrées justes étaient perdues."""
    neuve = _page(["warum-der-hebel-alles-verstaerkt", "haeufige-fehler"],
                  ["Warum der Hebel alles verstärkt", "Häufige Fehler"])
    out, notes = m._ancres_du_sommaire(neuve, exemples=[VOISINE])
    assert _ancres(out) == ["warum-der-hebel-alles-verstarkt", "haufige-fehler"], out
    assert all("remise" in n for n in notes), notes


def test_A_EGALITE_un_titre_dont_les_regles_s_accordent_est_quand_meme_rattrape() -> None:
    """Mesure du 28/09/2026 (PR #9) : les pages allemandes ne départagent pas « supprime » et
    « remplace ». Faute de règle UNIQUE, `#beispiel-fuer-eine-einfache-aufteilung` était
    retirée — alors que les deux règles écrivent le MÊME identifiant pour ce titre."""
    neuve = _page(["beispiel-fuer-eine-einfache-aufteilung", "schritt-1-asset-klassen"],
                  ["Beispiel für eine einfache Aufteilung", "Schritt 1 — Asset‑Klassen"])
    out, notes = m._ancres_du_sommaire(neuve, exemples=[SOEUR],
                                       plus_d_exemples=lambda: [SANS_APOSTROPHE])
    # Le premier est su (les deux regles s'accordent) ; le second ne l'est pas (tiret
    # insecable : `assetklassen` contre `asset-klassen`) et reste retire.
    assert _ancres(out) == ["beispiel-fur-eine-einfache-aufteilung"], out
    assert "remise" in notes[0] and "retiré" in notes[1], notes


def test_le_ESZETT_translittere_est_remis_sur_l_identifiant_du_site() -> None:
    """« Größe » : le modèle écrit `groesse`, le site `groe` — il RETIRE le ß, que la
    décomposition des accents ne touche pas. Des digrammes ne pouvaient pas le relier."""
    neuve = _page(["nach-sektor-und-groesse"], ["Nach Sektor und Größe"])
    out, notes = m._ancres_du_sommaire(neuve, exemples=[VOISINE])
    assert _ancres(out) == ["nach-sektor-und-groe"], out
    assert "remise" in notes[0], notes


def test_la_translitteration_SUISSE_ne_change_que_le_eszett() -> None:
    """L'usage suisse écrit `strasse` mais garde l'umlaut, que le slug retire ensuite :
    « Straße über » -> `strasse-uber`. La translittération allemande donnerait `ueber`."""
    neuve = _page(["strasse-uber-alles"], ["Straße über alles"])
    out, _notes = m._ancres_du_sommaire(neuve, exemples=[VOISINE])
    assert _ancres(out) == ["strae-uber-alles"], out


def test_la_translitteration_ne_devine_pas_entre_DEUX_titres() -> None:
    """« Größe » (allemand) et « Grösse » (suisse) se translittèrent TOUS DEUX en `groesse`,
    mais le site leur donne deux identifiants — `groe` et `grosse`. L'ancre `groesse` ne dit
    pas lequel elle vise : on retire, on ne choisit pas.

    (Deux fixtures fausses avant celle-ci : « Bär »/« Bar » donnent le MÊME identifiant sur ce
    site ; `aue`/`au` ne se reliaient plus du tout une fois la translittération appliquée au
    titre. Chacune faisait passer le test sans rien mesurer.)"""
    neuve = _page(["groesse"], ["Größe", "Grösse"])
    out, notes = m._ancres_du_sommaire(neuve, exemples=[VOISINE])
    assert _ancres(out) == [], out
    assert "retiré" in notes[0], notes


def test_la_comparaison_est_SYMETRIQUE_quand_le_titre_porte_aussi_un_vrai_ue() -> None:
    """« Häufige Fragen der Bauern » : le site écrit `haufige-fragen-der-bauern`, le modèle
    `haeufige-fragen-der-bauern`. Ne retirer les digrammes que de l'ancre laisserait le `ue` de
    `bauern` d'un seul côté, et l'entrée juste serait perdue."""
    neuve = _page(["haeufige-fragen-der-bauern"], ["Häufige Fragen der Bauern"])
    out, _notes = m._ancres_du_sommaire(neuve, exemples=[VOISINE])
    assert _ancres(out) == ["haufige-fragen-der-bauern"], out


def test_un_ue_AUTHENTIQUE_n_est_pas_pris_pour_un_umlaut() -> None:
    """« Frauen » : le site écrit `frauen`, et le modèle aussi. Rien à reprendre."""
    neuve = _page(["frauen-und-geld"], ["Frauen und Geld"])
    out, notes = m._ancres_du_sommaire(neuve, exemples=[VOISINE])
    assert (_ancres(out), notes) == (["frauen-und-geld"], [])


def test_les_ancres_MORTES_d_une_page_existante_n_eliminent_pas_la_vraie_regle() -> None:
    """Une page écrite à la main peut porter son propre sommaire cassé : exiger qu'une règle
    explique TOUTES les ancres éliminerait celle du site."""
    cassee = _page(["disclosure", "tarifs"], ["Avertissement", "Les tarifs d’ouverture"])
    assert m._regles_des_ancres([VOISINE, cassee]) == ["supprime"]


def test_un_lien_de_CORPS_mort_garde_son_texte() -> None:
    page = "---\ntitle: \"x\"\n---\n\nVoir [la suite](#nulle-part).\n\n## Autre chose\n"
    out, _notes = m._ancres_du_sommaire(page, exemples=[VOISINE])
    assert "Voir la suite." in out, out


def _preparer(monkeypatch, fichiers: dict[str, str], lus: list[str]) -> dict:
    monkeypatch.setattr(m, "_correction_ai_json", lambda **kw: {"contenu": NEUVE})

    def _lire(c):
        lus.append(c)
        return (fichiers[c], "sha") if c in fichiers else None

    return m._preparer_la_page(
        lire_fichier=_lire,
        all_paths=["package.json", "next.config.mjs", "app/(site)/[...slug]/page.tsx",
                   "app/(site)/fr/[...slug]/page.tsx", *fichiers],
        sujet="Diversifier", route="/fr/guides/diversifier", base_url="https://site.fr",
        site_name="site.fr", slug="s")


def test_les_exemples_viennent_du_MEME_gabarit(monkeypatch) -> None:
    """Un autre dossier peut suivre une autre fabrique d'identifiants (un blog, un gabarit
    tiers) : ses pages ne votent pas pour les guides. Ici trois pages du blog « prouvent »
    github-slugger, et l'emporteraient si on les lisait."""
    github = _page(["étape-1--les-bases"], ["Étape 1 — Les bases"])
    fichiers = {"content/fr/guides/a.mdx": SOEUR, "content/fr/guides/b.mdx": VOISINE,
                "content/fr/guides.mdx": "---\ntitle: \"G\"\n---\n\n- [A](/fr/guides/a/)\n",
                **{"content/fr/blog/%s.mdx" % n: github for n in ("x", "y", "z")}}
    lus: list[str] = []
    plan = _preparer(monkeypatch, fichiers, lus)
    assert "axe-1-classes-dactifs" in plan["contenu"], plan["contenu"]
    assert not any("/blog/" in c for c in lus), lus


def test_le_MEME_gabarit_dans_une_AUTRE_langue_tranche_quand_la_sienne_ne_peut_pas(
        monkeypatch) -> None:
    """Mesure du 28/09/2026 sur le site client : les guides allemands n'ont ni apostrophe ni
    tiret spécial, ils ne départagent jamais les règles — et « Größe » donne `groe` avec
    l'une, `gro-e` avec l'autre. Les guides français, même gabarit, tranchent. Un blog dans une
    autre langue diffère par DEUX segments : il ne vote pas, même s'il « prouve » autre chose."""
    neuve = _page(["nach-groesse"], ["Nach Größe"])
    github_slugger = _page(["étape-1--les-bases"], ["Étape 1 — Les bases"])
    fichiers = {"content/de/guides/a.mdx": SOEUR,
                "content/de/guides.mdx": "---\ntitle: \"G\"\n---\n\n- [A](/de/guides/a/)\n",
                "content/fr/guides/x.mdx": VOISINE,
                **{"content/en/blog/%s.mdx" % n: github_slugger for n in ("p", "q", "r")}}
    monkeypatch.setattr(m, "_correction_ai_json", lambda **kw: {"contenu": neuve})
    lus: list[str] = []

    def _lire(c):
        lus.append(c)
        return (fichiers[c], "sha") if c in fichiers else None

    plan = m._preparer_la_page(
        lire_fichier=_lire,
        all_paths=["package.json", "next.config.mjs", "app/(site)/[...slug]/page.tsx",
                   "app/(site)/de/[...slug]/page.tsx", "app/(site)/fr/[...slug]/page.tsx",
                   *fichiers],
        sujet="x", route="/de/guides/neu", base_url="https://site.fr", site_name="s", slug="s")
    assert plan["ok"], plan
    assert '"#nach-groe"' in plan["contenu"], plan["contenu"]
    assert "content/fr/guides/x.mdx" in lus
    assert not any("/blog/" in c for c in lus), lus


def test_les_exemples_sont_BORNES_et_de_la_meme_extension(monkeypatch) -> None:
    """Une section de mille pages ne doit pas coûter mille lectures GitHub."""
    fichiers = {"content/fr/guides/a.mdx": SOEUR,
                "content/fr/guides.mdx": "---\ntitle: \"G\"\n---\n\n- [A](/fr/guides/a/)\n",
                "content/fr/guides/donnees.json": "{}",
                **{"content/fr/guides/p%02d.mdx" % i: SOEUR for i in range(20)}}
    lus: list[str] = []
    _preparer(monkeypatch, fichiers, lus)
    exemples = [c for c in lus if c.startswith("content/fr/guides/p")]
    assert len(exemples) == 8, lus
    assert "content/fr/guides/donnees.json" not in lus


def test_le_garde_fou_est_BRANCHE_sur_la_redaction(monkeypatch) -> None:
    """Sans ce test, retirer l'appel dans `_preparer_la_page` ne casserait rien."""
    fichiers = {"content/fr/guides/a.mdx": SOEUR, "content/fr/guides/b.mdx": VOISINE,
                "content/fr/guides.mdx": "---\ntitle: \"G\"\n---\n\n- [A](/fr/guides/a/)\n"}
    monkeypatch.setattr(m, "_correction_ai_json", lambda **kw: {"contenu": NEUVE})
    plan = m._preparer_la_page(
        lire_fichier=lambda c: (fichiers[c], "sha") if c in fichiers else None,
        all_paths=["package.json", "next.config.mjs", "app/(site)/[...slug]/page.tsx",
                   "app/(site)/fr/[...slug]/page.tsx", *fichiers],
        sujet="Diversifier", route="/fr/guides/diversifier", base_url="https://site.fr",
        site_name="site.fr", slug="s")
    assert plan["ok"], plan
    assert "rebalancer" not in plan["contenu"] and "axe-1-classes-dactifs" in plan["contenu"]
    assert any("sommaire" in n for n in plan["notes_redaction"]), plan["notes_redaction"]
