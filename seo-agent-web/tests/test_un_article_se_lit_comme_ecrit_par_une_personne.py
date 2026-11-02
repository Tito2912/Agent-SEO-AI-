# -*- coding: utf-8 -*-
"""Un article se lit comme écrit par une personne du métier — pas par un modèle.

Demande du propriétaire, 02/11/2026. Ce que ces tests défendent : les formules toutes faites des
modèles sont repérées et leurs paragraphes réécrits ; une réécriture de STYLE ne perd jamais un
lien, un visuel ou un chiffre ; et rien ici ne cherche à tromper un détecteur — aucun caractère
invisible, aucune faute volontaire.
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")

from backend import naturel  # noqa: E402

ROBOT = (
    "## Pourquoi le levier compte\n\n"
    "Dans un monde en constante évolution, le levier est un véritable atout pour les investisseurs.\n\n"
    "Le levier multiplie les gains. Il multiplie aussi les pertes. Un compte de 1 000 € à levier 5 "
    "expose 5 000 €, et une baisse de 20 % efface tout le dépôt avant même que la plupart des "
    "plateformes ne vous préviennent.\n\n"
    "![Le levier](visuel:1)\n\n"
    "- point un\n- point deux\n\n"
    "En conclusion, il est important de noter que le levier est [risqué](/fr/guides/risques/).\n"
)
HUMAIN = (
    "## Ce que coûte une erreur\n\n"
    "Un compte de 1 000 € à levier 5 expose 5 000 €. Une baisse de 20 % suffit. Le dépôt est "
    "perdu avant la plupart des alertes, et c'est la partie que les tutoriels taisent souvent.\n\n"
    "J'ai vu des débutants doubler leur mise en une semaine. Rarement deux.\n"
)


def test_les_FORMULES_de_modele_designent_leur_paragraphe() -> None:
    examen = naturel.examiner(ROBOT, "fr")
    paras = naturel.paragraphes(ROBOT)
    designes = {paras[i]: r for i, r in examen["a_reecrire"].items()}
    assert any("Dans un monde en constante évolution" in p for p in designes)
    assert any(p.startswith("En conclusion") for p in designes)
    assert not any(p.startswith("Le levier multiplie") for p in designes), "le bon paragraphe reste"
    assert examen["mesures"]["tics"] >= 4


def test_un_texte_de_personne_ne_designe_RIEN() -> None:
    examen = naturel.examiner(HUMAIN, "fr")
    assert examen["a_reecrire"] == {} and examen["global"] == []


def test_les_tics_ANGLAIS_se_reperent_dans_un_article_anglais() -> None:
    texte = "In today's fast-paced world, let's dive in.\n\nIt's important to note that fees add up.\n"
    examen = naturel.examiner(texte, "en")
    assert len(examen["a_reecrire"]) == 2


def test_titres_puces_images_ne_sont_pas_de_la_PROSE() -> None:
    paras = naturel.paragraphes(ROBOT)
    assert not any(p.startswith(("#", "!", "- ")) for p in paras)


def test_la_TETE_d_un_fichier_du_depot_n_est_pas_de_la_prose() -> None:
    """Mode GitHub : un .md porte sa tête ; une ligne vide dans le YAML ne doit pas en faire un
    paragraphe à réécrire."""
    fichier = ("---\ntitle: \"Le levier\"\n\ndescription: \"Dans un monde en constante évolution\"\n"
               "---\n\nimport X from './x'\n\nDans un monde en constante évolution, le levier aide.\n")
    assert naturel.paragraphes(fichier) == ["Dans un monde en constante évolution, le levier aide."]


def test_des_phrases_toutes_de_la_MEME_longueur_se_signalent() -> None:
    monotone = "\n\n".join("Le levier change le risque du compte de chaque client." for _ in range(8))
    assert any("trop régulière" in r for r in naturel.examiner(monotone, "fr")["global"])


def test_trop_de_PUCES_et_de_TIRETS_longs_se_signalent() -> None:
    texte = "Intro — avec — trois — tirets — ici.\n\n" + "\n".join("- point %d" % i for i in range(10))
    globales = naturel.examiner(texte, "fr")["global"]
    assert any("tirets longs" in r for r in globales) and any("puces" in r for r in globales)


def test_des_paragraphes_ouverts_par_des_CONNECTEURS_en_serie_se_signalent() -> None:
    texte = ("De plus, le réglage compte beaucoup ici.\n\nEn outre, la fréquence joue un rôle.\n\n"
             "Le budget, lui, reste fixe chaque mois.\n\nPar ailleurs, les frais pèsent lourd.\n")
    examen = naturel.examiner(texte, "fr")
    assert sorted(examen["a_reecrire"]) == [1, 3], "le premier connecteur garde sa place"


def test_un_PARALLELISME_voulu_n_est_pas_un_tic() -> None:
    """Premier essai en production (02/11/2026) : « Avec 2 %… Avec 5 %… Avec 8 % », « Pour… »
    étaient signalés et sept paragraphes réécrits pour rien — une personne écrit ainsi."""
    texte = "\n\n".join("Avec **%d %%** de rendement, le capital atteint une autre valeur finale." % t
                        for t in (2, 5, 8, 10)) + "\n\nPour comparer, voir le guide.\n\nPour finir, un mot.\n"
    assert naturel.examiner(texte, "fr")["a_reecrire"] == {}


def test_des_TIRETS_longs_trop_denses_font_reecrire_leurs_paragraphes() -> None:
    texte = ("La question n'est pas là — c'est de savoir quoi en attendre.\n\n"
             "Les frais comptent, sans tiret ici.\n\nUn scénario — rare — reste possible.\n")
    examen = naturel.examiner(texte, "fr")
    assert sorted(examen["a_reecrire"]) == [0, 2]
    assert all("tirets longs" in r[0] for r in examen["a_reecrire"].values())
    assert naturel.examiner("Un seul — tiret dans un long texte. " + "Mot " * 400, "fr")["a_reecrire"] == {}


def test_une_reecriture_qui_perd_un_LIEN_ou_un_CHIFFRE_n_est_pas_posee() -> None:
    ancien = "En conclusion, il est important de noter que le levier est [risqué](/fr/guides/risques/)."
    texte = ROBOT
    garde, n = naturel.remplacer_les_paragraphes(texte, [ancien], ["Le levier reste risqué."])
    assert n == 0 and garde == texte
    pose, n = naturel.remplacer_les_paragraphes(
        texte, [ancien], ["Gardez en tête que le levier est [risqué](/fr/guides/risques/)."])
    assert n == 1 and "Gardez en tête" in pose and ancien not in pose
    ancien2 = naturel.paragraphes(ROBOT)[1]
    _t, n = naturel.remplacer_les_paragraphes(texte, [ancien2], [ancien2.replace("20 %", "25 %")])
    assert n == 0, "un chiffre changé n'est pas une réécriture de style"


def test_la_reecriture_ne_vise_que_les_paragraphes_DESIGNES_avec_leur_raison() -> None:
    demande = naturel.demande_de_reecriture(["Dans un monde en constante évolution, x."],
                                            [["formule toute faite"]], "fr")
    assert "PARAGRAPHE 1 — defaut : formule toute faite" in demande
    assert "CHAQUE chiffre et CHAQUE lien" in demande


# ── Dans la rédaction ─────────────────────────────────────────────────────────────────────────

def _article_avec_relecture(monkeypatch, reecriture):
    """Rédige un article « contenu seul » dont le modèle place une formule toute faite."""
    from backend import app as m
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from test_un_article_sans_depot import PAGES

    appels: list[dict] = []

    def _ai(*, system, user_msg, **kw):
        appels.append({"system": system, "user": user_msg})
        if system == naturel.SYSTEME_REECRITURE:
            return {"paragraphes": reecriture}
        # Un lien que le crawl connaît : un lien mort serait retiré AVANT la relecture.
        return {"titre": "Le levier", "description": "d", "slug": "levier",
                "markdown": ROBOT.replace("/fr/guides/risques/", "/fr/guides/dca-crypto/")}

    monkeypatch.setattr(m, "_correction_ai_json", _ai)
    out = m._preparer_des_articles(pages=PAGES, sujet="Le levier", section="/fr/guides",
                                   site_name="site.fr")
    return out["versions"][0], appels


def test_la_redaction_REECRIT_les_paragraphes_fautifs_et_le_dit(monkeypatch) -> None:
    version, appels = _article_avec_relecture(monkeypatch, [
        "Le levier attire les investisseurs pressés, et c'est souvent là que tout se joue.",
        "Retenez une chose : le levier reste [risqué](/fr/guides/dca-crypto/)."])
    assert naturel.CONSIGNE_DE_NATUREL in appels[0]["user"], "la consigne part avec la demande"
    (relecture,) = [a for a in appels if a["system"] == naturel.SYSTEME_REECRITURE]
    assert "Dans un monde en constante évolution" in relecture["user"]
    assert "Le levier multiplie les gains" not in relecture["user"], "seuls les paragraphes fautifs partent"
    assert "Dans un monde" not in version["contenu"] and "En conclusion" not in version["contenu"]
    assert "Retenez une chose" in version["contenu"]
    assert any(n.startswith("naturel : 2 paragraphe(s) réécrit(s)") for n in version["notes_redaction"])


def test_une_reecriture_qui_PERD_un_lien_est_refusee_et_le_reste_est_dit(monkeypatch) -> None:
    version, _appels = _article_avec_relecture(monkeypatch, [
        "Le levier attire les investisseurs pressés.", "Le levier reste risqué."])
    assert "[risqué](/fr/guides/dca-crypto/)" in version["contenu"], "le lien vérifié reste"
    assert any(n.startswith("naturel, à relire : 1 paragraphe(s)") for n in version["notes_redaction"])


def test_une_relecture_EN_PANNE_ne_fait_pas_tomber_l_article(monkeypatch) -> None:
    from backend import app as m
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from test_un_article_sans_depot import PAGES

    def _ai(*, system, user_msg, **kw):
        if system == naturel.SYSTEME_REECRITURE:
            raise RuntimeError("modèle indisponible")
        return {"titre": "Le levier", "description": "d", "slug": "levier", "markdown": ROBOT}

    monkeypatch.setattr(m, "_correction_ai_json", _ai)
    out = m._preparer_des_articles(pages=PAGES, sujet="Le levier", section="/fr/guides")
    assert out["ok"] and "Dans un monde" in out["versions"][0]["contenu"]


def test_sous_le_plafond_les_FORMULES_passent_avant_les_tirets(monkeypatch) -> None:
    """Dix paragraphes à tirets et une formule en dernier : la formule part quand même."""
    from backend import app as m
    vus: list[str] = []
    monkeypatch.setattr(m, "_correction_ai_json", lambda **kw: vus.append(kw["user_msg"]) or {})
    texte = "\n\n".join("Paragraphe %d — avec un tiret — et du texte." % i for i in range(10))
    texte += "\n\nDans un monde en constante évolution, la fin arrive.\n"
    m._rendre_naturel({"contenu": texte, "notes_redaction": []}, langue="fr")
    (demande,) = vus
    assert "Dans un monde en constante évolution" in demande
    assert demande.count("PARAGRAPHE ") == m._NATUREL_MAX_PARAGRAPHES


def test_une_page_du_depot_hors_Markdown_n_est_pas_relue(monkeypatch) -> None:
    """Dans du JSX, un paragraphe est entouré de code : le remplacer par du texte n'est pas sûr."""
    from backend import app as m
    appels: list = []
    monkeypatch.setattr(m, "_correction_ai_json", lambda **kw: appels.append(kw) or {})
    plan = {"fichier": "app/blog/levier/page.tsx", "langue": "fr", "notes_redaction": [],
            "contenu": "Dans un monde en constante évolution, le levier aide.\n"}
    m._rendre_naturels([plan])
    assert appels == [] and plan["notes_redaction"] == []
    plan["fichier"] = "content/blog/levier.md"
    m._rendre_naturels([plan])
    assert len(appels) == 1


def test_RIEN_ici_ne_cherche_a_tromper_un_detecteur() -> None:
    """Ni caractère invisible, ni homoglyphe, ni faute volontaire : la consigne parle de style."""
    source = Path(naturel.__file__).read_text(encoding="utf-8")
    assert not re.search(r"\\u200[b-d]|\\u2060|\\ufeff|homoglyph|faute volontaire a introduire", source)
    assert "detecteur" not in naturel.CONSIGNE_DE_NATUREL.lower()
