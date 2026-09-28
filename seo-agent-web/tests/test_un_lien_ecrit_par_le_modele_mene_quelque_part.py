# -*- coding: utf-8 -*-
"""Un lien interne écrit par le modèle mène à une page du dépôt, ou il n'est plus là.

MESURE DU 28/09/2026, PR #10 de prosperfactory.com : la consigne « les liens internes sont
ceux de la page montrée, dans SA langue » n'a pas tenu. Le modèle n'a pas recopié le lien
français `/fr/guides/dca-crypto/` — il l'a TRADUIT, en inventant `/de/guides/dca-krypto/` et
`/es/guides/dca-crypto/`. Les vrais slugs sont `krypto-dca` et `dca-cripto`. Build vert.
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

ROUTES = {"/fr/guides/dca-crypto": ["a"], "/fr/guides/debuter": ["b"],
          "/de/guides/krypto-dca": ["c"], "/de/guides/anfangen": ["d"],
          "/fr/guides": ["i"], "/de/guides": ["j"]}
TRADUCTIONS = {("/fr/guides/dca-crypto/", "de"): "/de/guides/krypto-dca",
               ("/fr/guides/debuter/", "de"): "/de/guides/anfangen"}


def _traduire(lien: str, langue: str) -> str:
    return TRADUCTIONS.get((lien, langue), "")


def _page(liens: list[str], corps: str = "") -> str:
    return ("---\ntitle: \"x\"\ninternalLinks:\n"
            + "".join('  - href: "%s"\n    anchor: "a"\n' % l for l in liens)
            + "---\n\n" + corps)


def _plans(fr: list[str], de: list[str], corps_de: str = "") -> list[dict]:
    return [{"langue": "fr", "contenu": _page(fr), "notes_redaction": []},
            {"langue": "de", "contenu": _page(de, corps_de), "notes_redaction": []}]


def test_un_lien_TRADUIT_au_hasard_est_remis_sur_la_vraie_traduction() -> None:
    plans = _plans(["/fr/guides/dca-crypto/", "/fr/guides/debuter/"],
                   ["/de/guides/dca-krypto/", "/de/guides/anfangen/"])
    m._reparer_les_liens(plans, routes=ROUTES, traduire=_traduire)
    assert m._liens_internes(plans[1]["contenu"]) == ["/de/guides/krypto-dca/", "/de/guides/anfangen/"]
    assert "remis à `/de/guides/krypto-dca/`" in plans[1]["notes_redaction"][0]
    assert plans[0]["notes_redaction"] == [], "la version principale n'avait rien de faux"


def test_sans_correspondance_de_RANG_le_lien_mort_est_retire_pas_devine() -> None:
    """Deux listes de longueurs différentes : le rang ne désigne plus rien."""
    plans = _plans(["/fr/guides/dca-crypto/", "/fr/guides/debuter/"], ["/de/guides/dca-krypto/"])
    m._reparer_les_liens(plans, routes=ROUTES, traduire=_traduire)
    assert m._liens_internes(plans[1]["contenu"]) == []
    assert "retiré" in plans[1]["notes_redaction"][0]
    assert m._front_matter_parse_error(plans[1]["contenu"], "a.mdx") == ""


def test_un_lien_mort_de_la_page_PRINCIPALE_est_retire() -> None:
    plans = _plans(["/fr/guides/invente/", "/fr/guides/debuter/"], [])[:1]
    m._reparer_les_liens(plans, routes=ROUTES, traduire=_traduire)
    assert m._liens_internes(plans[0]["contenu"]) == ["/fr/guides/debuter/"]


def test_une_section_INCONNUE_ou_un_FICHIER_ne_se_jugent_pas() -> None:
    """`/fr/tags/…` peut être une page engendrée que la carte des routes ignore ; `.png` est un
    fichier. Les retirer ferait perdre des liens justes."""
    liens = ["/fr/tags/levier/", "/fr/guides/schema.png", "/fr/guides/debuter/"]
    plans = _plans(liens, [])[:1]
    m._reparer_les_liens(plans, routes=ROUTES, traduire=_traduire)
    assert m._liens_internes(plans[0]["contenu"]) == liens
    assert plans[0]["notes_redaction"] == []


def test_une_cle_SEULE_n_est_pas_retiree_et_sa_voisine_non_plus() -> None:
    """`buttonHref` sous `cta:` n'est pas une entrée de liste : la retirer casserait le schéma.
    Et la liste JUSTE au-dessus ne doit pas perdre sa dernière entrée à sa place."""
    contenu = ('---\ntitle: "x"\njumpLinks:\n  - href: "#a"\n    label: "A"\n'
               'cta:\n  buttonHref: "/fr/guides/invente/"\n---\n\nTexte.\n')
    plans = [{"langue": "fr", "contenu": contenu, "notes_redaction": []}]
    m._reparer_les_liens(plans, routes=ROUTES, traduire=_traduire)
    assert plans[0]["contenu"] == contenu
    assert "À CORRIGER À LA MAIN" in plans[0]["notes_redaction"][0]


def test_un_lien_de_CORPS_mort_garde_son_texte() -> None:
    plans = _plans([], [], "Voir [ce guide](/de/guides/invente/).\n")[1:]
    m._reparer_les_liens(plans, routes=ROUTES, traduire=_traduire)
    assert "Voir ce guide." in plans[0]["contenu"]


def test_un_lien_cite_DEUX_fois_est_repare_une_fois() -> None:
    plans = _plans(["/fr/guides/dca-crypto/", "/fr/guides/dca-crypto/"],
                   ["/de/guides/dca-krypto/", "/de/guides/dca-krypto/"])
    m._reparer_les_liens(plans, routes=ROUTES, traduire=_traduire)
    assert m._liens_internes(plans[1]["contenu"]) == ["/de/guides/krypto-dca/"] * 2
    assert len(plans[1]["notes_redaction"]) == 1, plans[1]["notes_redaction"]
