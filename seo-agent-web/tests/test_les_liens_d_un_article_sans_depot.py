# -*- coding: utf-8 -*-
"""Un article rédigé depuis le crawl : TOUTES les formes de lien interne sont contrôlées.

Relecture du 26/11/2026 de la PR #96 : en mode crawl, seuls les liens `[texte](adresse)` simples
étaient lus. Un lien par référence, un `<a href>`, un texte entre crochets imbriqués ou une image
cliquable vers une page inexistante passaient sans retrait ni note.

C'est le convertisseur des articles qui dit ce qui est un lien : un exemple écrit dans un bloc de
code n'en est pas un, et rien ne le réécrit.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")

from backend import app as m  # noqa: E402

S = "https://site.fr"
ROUTES = {c: {"url": S + c} for c in ("/", "/guide", "/de/leitfaden")}
TRADUCTIONS = {("/guide", "de"): "/de/leitfaden"}


def _traduire(lien: str, langue: str) -> str:
    return TRADUCTIONS.get((m.repo_index.norm_route(lien), langue), "")


def _reparer(*contenus: str) -> list[dict]:
    plans = [{"langue": langue, "contenu": contenu, "notes_redaction": []}
             for langue, contenu in zip(("fr", "de"), contenus)]
    m._reparer_les_liens(plans, routes=ROUTES, traduire=_traduire, routes_du_crawl=True)
    return plans


@pytest.mark.parametrize("contenu, attendu", [
    ("Voir [le détail][d].\n\n[d]: /mort\n", "Voir le détail.\n\n"),
    ("Voir [le détail] [d].\n\n[D]: /mort \"Titre\"\n", "Voir le détail.\n\n"),
    ("Voir [Détail][].\n\n[détail]: /mort\n", "Voir Détail.\n\n"),
    ("Voir [Détail].\n\n[détail]: /mort\n", "Voir Détail.\n\n"),
    ("Voir [le détail][d].\n\n[d]:\n    /mort\n", "Voir le détail.\n\n"),
    ("Voir [le détail][d].\n\n[d]: /mort\n    \"Titre\"\n", "Voir le détail.\n\n"),
    ("Voir [le détail][d].\n\n[d]: https://www.site.fr/mort?x=1\n", "Voir le détail.\n\n"),
    ('Voir <a href="/mort">le détail</a>.', "Voir le détail."),
    ("Voir <a class='x' href='/mort'><strong>le détail</strong></a>.", "Voir le détail."),
    ("Voir <A HREF=/mort>le détail</A>.", "Voir le détail."),
    ('Voir <a href="https://site.fr/mort">le détail</a>.', "Voir le détail."),
    ("Voir [le [vrai] détail](/mort).", "Voir le [vrai] détail."),
    ("Voir [le\ndétail](/mort).", "Voir le\ndétail."),
    ("Voir [le détail](/mort_(ancien)).", "Voir le détail."),
    ("[![Schéma](visuel:1)](/mort)", "![Schéma](visuel:1)"),
], ids=["reference", "espace-casse-titre", "repliee", "raccourcie", "adresse-ligne-suivante",
        "titre-ligne-suivante", "reference-absolue", "html", "html-attributs", "html-majuscules",
        "html-absolu", "crochets-imbriques", "texte-sur-deux-lignes", "parentheses",
        "image-cliquable"])
def test_un_lien_mort_est_retire_quelle_que_soit_sa_forme(contenu: str, attendu: str) -> None:
    plan = _reparer(contenu)[0]
    assert plan["contenu"] == attendu
    assert len(plan["notes_redaction"]) == 1 and "retiré" in plan["notes_redaction"][0]
    assert "mort" not in m._article_en_html(plan["contenu"])


@pytest.mark.parametrize("contenu", [
    "Voir [le guide][g].\n\n[g]: /guide\n",
    "Voir [guide].\n\n[guide]: /guide#partie \"Titre\"\n",
    'Voir <a href="/guide">le guide</a>.',
    "[source][s], [courriel](mailto:a@site.fr), [ancre](#partie).\n\n[s]: https://autre.fr/mort\n",
    '<a href="https://autre.fr/mort">source</a> et <a href="//autre.fr/mort">autre</a>',
    "[inutilisée]: /mort\n\nTexte.",
    "Note[^1].\n\n[^1]: /mort",
], ids=["reference", "raccourcie", "html", "externes", "html-externes", "definition-inutilisee",
        "note-de-bas-de-page"])
def test_un_lien_vivant_ou_externe_n_est_pas_touche(contenu: str) -> None:
    plan = _reparer(contenu)[0]
    assert plan["contenu"] == contenu and plan["notes_redaction"] == []


@pytest.mark.parametrize("contenu", [
    '```html\n<a href="/mort">exemple</a>\n```\n',
    "```markdown\n[exemple](/mort)\n[autre][a]\n\n[a]: /mort\n```\n",
    'Écris `<a href="/mort">x</a>` ou `[x](/mort)` dans ta page.',
    '    <a href="/mort">exemple indenté</a>\n',
], ids=["bloc-html", "bloc-markdown", "code-en-ligne", "code-indente"])
def test_un_exemple_dans_du_code_reste_tel_quel(contenu: str) -> None:
    plan = _reparer(contenu)[0]
    assert plan["contenu"] == contenu and plan["notes_redaction"] == []


def test_le_code_reste_quand_le_meme_lien_est_retire_du_texte() -> None:
    code = "```\n[exemple](/mort)\n```\n"
    plan = _reparer("Voir [ceci](/mort).\n\n" + code)[0]
    assert plan["contenu"] == "Voir ceci.\n\n" + code


@pytest.mark.parametrize("traduit, attendu", [
    ("Siehe [Leitfaden][l].\n\n[l]: /de/erfunden\n",
     "Siehe [Leitfaden][l].\n\n[l]: /de/leitfaden\n"),
    ('Siehe <a href="/de/erfunden">Leitfaden</a>.', 'Siehe <a href="/de/leitfaden">Leitfaden</a>.'),
    ("Siehe [Leitfaden](/de/erfunden).", "Siehe [Leitfaden](/de/leitfaden)."),
], ids=["reference", "html", "en-ligne"])
def test_la_traduction_se_repare_par_la_principale_quelle_que_soit_la_forme(
    traduit: str, attendu: str,
) -> None:
    """La position se compte dans l'ordre de lecture, définitions non comprises."""
    principal = "Voir [le guide][g].\n\n[g]: /guide\n"
    fr, de = _reparer(principal, traduit)
    assert fr["contenu"] == principal
    assert de["contenu"] == attendu
    assert "remis à `/de/leitfaden`" in de["notes_redaction"][0]


def test_plusieurs_formes_vers_la_meme_adresse_font_une_seule_note() -> None:
    plan = _reparer('[a](/mort), [b][r] et <a href="/mort">c</a>.\n\n[r]: /mort\n')[0]
    assert plan["contenu"] == "a, b et c.\n\n"
    assert len(plan["notes_redaction"]) == 1


def test_un_lien_que_la_reparation_ne_sait_pas_lire_est_signale() -> None:
    """Le filet : ce que le convertisseur rend comme lien mort ne passe jamais sans un mot."""
    contenu = "Voir [ceci](/mort_(a_(b)))."
    plan = _reparer(contenu)[0]
    assert plan["contenu"] == contenu
    assert len(plan["notes_redaction"]) == 1
    assert "MORT, À CORRIGER À LA MAIN" in plan["notes_redaction"][0]


@pytest.mark.parametrize("contenu", [
    "Remplace [ton adresse](https://[votre-site].fr/page).",
    '<a href="http://[">cassé</a> et [autre][a].\n\n[a]: http://[domaine]/x\n',
], ids=["en-ligne", "html-et-reference"])
def test_une_adresse_illisible_ne_fait_pas_tomber_la_redaction(contenu: str) -> None:
    plan = _reparer(contenu)[0]
    assert plan["contenu"] == contenu and plan["notes_redaction"] == []


def test_un_crochet_echappe_ne_deplace_pas_le_retrait() -> None:
    plan = _reparer("Voir [le tableau \[ ouvert](/mort).")[0]
    assert plan["contenu"] == "Voir le tableau \[ ouvert."
