# -*- coding: utf-8 -*-
"""Un antislash posé dans du texte de balisage s'affiche chez le visiteur.

MESURE DU 20/09/2026, banc des neuf idiomes, page next-app. Le modèle a écrit **neuf**
antislashs dans le même fichier :

    SIX justes    `title: 'Comment … d\\'un site'` — une chaîne JavaScript à guillemets
                  simples, où l'échappement est obligatoire.
    TROIS faux    `<p>… d\\'un site</p>` — du texte JSX, où les enfants d'un élément ne sont
                  PAS une chaîne littérale. L'antislash s'affiche, tel quel, sur la page.

Mêmes deux caractères, contexte opposé. On ne peut donc pas les enlever tous — et c'est
exactement pourquoi ce défaut survit à tout : le fichier compile, le build est VERT, et seule
la page rendue le montre. Troisième fois de ce chantier qu'un contrôle de compilation laisse
passer une faute de SENS.

POURQUOI PAS UN SIMPLE SUIVI DE GUILLEMETS, qui ferait dix lignes : dans du JSX, une apostrophe
de prose (`l'accueil`) est indiscernable d'une ouverture de chaîne. Un scanner qui ne suivrait
que les guillemets se désynchroniserait à la première phrase française et lirait tout le reste
du fichier à l'envers. On suit donc les BALISES, et les guillemets seulement à l'intérieur
d'une balise, là où ils délimitent vraiment une valeur d'attribut.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-antislash-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402

CHEMIN = "app/gauntlet/ma-page/page.tsx"

# La forme réelle de la page que le banc a produite, réduite : six échappements justes dans les
# chaînes de `metadata`, deux faux dans le corps.
REEL = """export const metadata = {
  title: 'Comment verifier les balises canoniques d\\'un site',
  description: 'Ce que fait la verification d\\'un site.',
  openGraph: {
    title: 'Comment verifier les balises canoniques d\\'un site',
    description: 'Ce que fait la verification d\\'un site.',
  },
  twitter: {
    title: 'Comment verifier les balises canoniques d\\'un site',
    description: 'Ce que fait la verification d\\'un site.',
  },
};

export default function Page() {
  return (
    <main>
      <h1>Verifier les balises canoniques</h1>
      <p>Cette page explique la verification d\\'un site.</p>
      <p>Retour a l\\'accueil.</p>
    </main>
  );
}
"""


def _sortie(source: str, chemin: str = CHEMIN) -> str:
    return m._antislashs_de_trop(source, chemin)[0]


# ── le défaut mesuré, dans le fichier qui l'a porté ──────────────────────────────────────────

def test_les_DEUX_antislashs_de_texte_sont_retires() -> None:
    sortie, notes = m._antislashs_de_trop(REEL, CHEMIN)
    assert "<p>Cette page explique la verification d'un site.</p>" in sortie, sortie
    assert "<p>Retour a l'accueil.</p>" in sortie, sortie
    assert notes and "2 antislash" in notes[0], notes


def test_les_SIX_antislashs_de_chaine_sont_GARDES() -> None:
    """La moitié qu'un remède trop large casserait. Dans `'…'`, l'échappement est obligatoire :
    l'enlever ferme la chaîne au milieu d'une phrase et le fichier ne compile plus."""
    sortie = _sortie(REEL)
    assert sortie.count("\\'") == 6, sortie.count("\\'")
    assert "title: 'Comment verifier les balises canoniques d\\'un site'" in sortie


def test_le_fichier_repare_SE_RELIT() -> None:
    """Le contrôle qui existait déjà doit toujours accepter ce qu'on vient de modifier."""
    assert m._refus_de_format(CHEMIN, _sortie(REEL)) is None


def test_une_page_SANS_antislash_ne_produit_aucun_diff() -> None:
    propre = _sortie(REEL)
    assert m._antislashs_de_trop(propre, CHEMIN) == (propre, [])


# ── le piège qu'un suivi de guillemets ne passerait pas ──────────────────────────────────────

def test_une_apostrophe_de_PROSE_ne_desynchronise_pas_la_lecture() -> None:
    """LE test qui justifie de suivre les balises plutôt que les guillemets.

    `l'accueil` en clair dans du texte JSX ouvrirait une chaîne pour un scanner naïf, qui
    lirait alors tout le reste du fichier comme une chaîne — et laisserait passer l'antislash
    fautif qui suit.
    """
    source = ("<main>\n  <p>Retour a l'accueil, sans antislash.</p>\n"
              "  <p>Et ici la verification d\\'un site.</p>\n</main>\n")
    sortie, notes = m._antislashs_de_trop(source, CHEMIN)
    assert "verification d'un site" in sortie, sortie
    assert "Retour a l'accueil, sans antislash." in sortie
    assert len(notes) == 1, notes


# ── ce qu'on ne touche pas ────────────────────────────────────────────────────────────────────

def test_le_corps_d_un_SCRIPT_est_du_code_pas_de_la_prose() -> None:
    """Il est entre deux balises sans être du texte. Un `\\'` y est un échappement légitime, et
    le retirer casserait la chaîne."""
    source = ("<html><body>\n<script>\nvar s = 'aujourd\\'hui';\n</script>\n"
              "<p>Texte d\\'ici.</p>\n</body></html>\n")
    sortie, notes = m._antislashs_de_trop(source, "page.html")
    assert "var s = 'aujourd\\'hui';" in sortie, sortie
    assert "<p>Texte d'ici.</p>" in sortie, sortie
    assert len(notes) == 1, notes


def test_une_EXPRESSION_entre_accolades_est_laissee_tranquille() -> None:
    """`{maVariable}` est du JavaScript, pas de la prose."""
    source = "<main>\n  <p>{ligne.replace('a\\'b', '')}</p>\n</main>\n"
    assert _sortie(source) == source


def test_une_valeur_d_ATTRIBUT_est_laissee_tranquille() -> None:
    """Dans une balise, les guillemets délimitent vraiment une chaîne.

    LA FIXTURE PORTE UN `>` DANS UN ATTRIBUT, et ce n'est pas de la décoration : sans le suivi
    des guillemets à l'intérieur d'une balise, celle-ci se terminerait à ce `>`-là, la fin de
    la balise passerait pour du texte, et l'échappement de l'attribut voisin serait retiré.
    Ma première version n'avait pas ce `>` — la mutation qui désactive le suivi y a SURVÉCU,
    faute de conséquence observable.
    """
    source = '<main>\n  <a href="/x" title="a > b" alt="l\\\'accueil">Aller</a>\n</main>\n'
    assert _sortie(source) == source


def test_le_code_AVANT_la_premiere_balise_est_laisse_tranquille() -> None:
    """Le front matter JavaScript d'Astro, entre `---`, et tout ce qui précède le balisage.
    Une région doit être bornée des DEUX côtés par des balises pour être du texte."""
    source = ("---\nconst titre = 'Page d\\'accueil';\n---\n"
              "<main>\n  <h1>Bonjour</h1>\n</main>\n")
    assert _sortie(source, "src/pages/x.astro") == source


def test_le_code_APRES_la_derniere_balise_est_laisse_tranquille() -> None:
    source = "<main>\n  <h1>Bonjour</h1>\n</main>\nconst fin = 'c\\'est tout';\n"
    assert _sortie(source) == source


def test_un_fichier_qui_n_est_PAS_du_balisage_est_intact() -> None:
    """Hors du balisage, un antislash peut vouloir dire autre chose — et surtout : on ne l'a
    pas mesuré là. Ce projet s'abstient partout où il n'a pas regardé.

    LA FIXTURE CONTIENT DU BALISAGE EXPRÈS. Ma première version n'en avait aucun, donc le
    scanner ne trouvait rien à faire de toute façon, et la mutation qui supprime le contrôle
    d'extension y a SURVÉCU. Ici l'extension est la SEULE chose qui protège ce fichier — le
    témoin en fin de test le montre.
    """
    source = ('+++\ntitle = "Guide"\n+++\n\n'
              "<figure>\n  <figcaption>Le guide d\\'achat</figcaption>\n</figure>\n")
    assert "d\\'achat" in source, "la fixture ne pose plus le piège"
    assert _sortie(source, "content/blog/x.md") == source
    assert _sortie(source, "content/blog/x.toml") == source
    assert "d'achat" in _sortie(source, "src/pages/x.astro"), "le témoin ne mesure plus rien"


# ── bout en bout ─────────────────────────────────────────────────────────────────────────────

def test_le_REDACTEUR_applique_la_garde_lui_meme(monkeypatch) -> None:
    """Comme le garde-fou d'adresse : dans `rediger_une_page`, pas chez l'appelant."""
    monkeypatch.setattr(m, "_correction_ai_json", lambda **kw: {"contenu": REEL})
    notes: list[str] = []
    contenu, refus = m.rediger_une_page(
        sujet="Ma page", chemin=CHEMIN, soeur_chemin="app/gauntlet/soeur/page.tsx",
        soeur_contenu=REEL, url_de_la_page="https://site.fr/gauntlet/ma-page", notes=notes)
    assert refus == "", refus
    assert "<p>Cette page explique la verification d'un site.</p>" in contenu, contenu
    assert contenu.count("\\'") == 6, contenu.count("\\'")
    assert any("antislash" in n for n in notes), notes


# ── le code PRIS EN SANDWICH entre deux blocs de balisage ────────────────────────────────────
# Mesure du 26/09/2026. Les tests ci-dessus couvrent le code AVANT la première balise et APRÈS
# la dernière. Entre deux, personne n'avait regardé : `_spans_texte_balise` ne répond qu'à
# « entre quelles balises », et du code placé entre `</main>` et `<footer>` y répond « texte ».
# Un `BS'` légitime y était retiré — le littéral JavaScript se ferme au milieu d'une phrase et
# le build du client casse. Aucun des 360 fichiers réels des neuf dépôts ne le déclenche (58
# portent pourtant un échappement), donc c'était un défaut LATENT : réel, atteignable, et que
# rien n'aurait signalé avant un déploiement rouge.

def test_du_code_ENTRE_deux_blocs_de_balisage_est_laisse_tranquille() -> None:
    """Ni avant la première balise, ni après la dernière : ENTRE les deux.

    La région ne contient aucune accolade, donc la garde qui épargnait les expressions ne la
    protégeait pas. C'est la profondeur d'éléments qui tranche : après `</main>` plus rien
    n'est ouvert, donc ce qui suit est du code.
    """
    source = ("<main><p>texte</p></main>\n"
              "const titre = 'l\\'agent lit';\n"
              "<footer>fin</footer>\n")
    assert "\\'" in source, "la fixture ne pose plus le piege"
    assert _sortie(source) == source


def test_un_fichier_qui_fabrique_du_balisage_en_chaines_est_intact() -> None:
    """Construire du HTML par concaténation est courant, et chaque chaîne porte des balises.

    Le scanner voit `<p>` … `</p>` et appelle « texte » tout ce qu'il y a entre — y compris la
    chaîne voisine, qui n'a rien à voir.
    """
    source = ("const ouvre = '<p>un</p>';\n"
              "const titre = 'l\\'agent';\n"
              "const ferme = '<p>deux</p>';\n")
    assert "\\'" in source, "la fixture ne pose plus le piege"
    assert _sortie(source, "src/lib/html.js") == source


def test_une_expression_reste_epargnee_sans_la_garde_des_accolades() -> None:
    """La garde « la région contient une accolade » devient inutile une fois la prose définie
    par la profondeur : `_spans_de_prose` coupe déjà ses régions à chaque accolade.

    On le vérifie ici plutôt que de garder une garde dont plus rien ne prouve l'effet.
    """
    source = "<main>\n  <p>{ligne.replace('a\\'b', '')}</p>\n</main>\n"
    assert "\\'" in source, "la fixture ne pose plus le piege"
    assert _sortie(source) == source
    assert all("{" not in source[d:f] and "}" not in source[d:f]
               for d, f in m._spans_de_prose(source)), "une region de prose porte une accolade"


def test_un_antislash_devant_autre_chose_qu_un_guillemet_est_du_CONTENU() -> None:
    """Un chemin Windows dans du texte s'affiche tel quel, et c'est voulu.

    On ne retire un antislash que devant un guillemet, parce que la seule chose mesuree est un
    modele qui echappe par reflexe une apostrophe francaise la ou il ne faut pas. Devant une
    lettre, l'antislash est ce que l'auteur a ecrit : le retirer changerait le texte que le
    visiteur lit, ce qui est exactement le tort qu'on repare ici, en sens inverse.
    """
    source = "<main>\n  <p>Le dossier C:\\dossier\\projet contient tout.</p>\n</main>\n"
    assert source.count("\\") == 2, "la fixture ne pose plus le piege"
    assert _sortie(source) == source
