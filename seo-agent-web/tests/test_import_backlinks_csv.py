# -*- coding: utf-8 -*-
"""L'import de backlinks recoit un FICHIER DU CLIENT : la seule entree vraiment hostile du produit.

Zone mesuree sans couverture le 18/09/2026 : 142 occurrences dans app.py, un modele, quatre
ecrans, trois taches planifiees — et aucun test dedie. Ce fichier couvre le maillon d'entree,
celui ou un tableur inconnu rencontre le produit.

CE QUI EST DEJA BORNE, et qu'il ne faut donc pas re-tester ici : la route lit au plus 10 Mo
(`file.read(_CSV_MAX_BYTES + 1)`, lecture bornee et non « lire puis mesurer »), refuse un type de
contenu etranger, et refuse le fichier vide. Le risque memoire est tenu en amont.

CE QUI COMPTE ICI EST LA JUSTESSE, pas la robustesse : un import qui reussit A MOITIE est pire
qu'un import qui echoue. Les backlinks d'un concurrent verses dans le projet d'un client, une
ancre dont les accents sont perdus, un export Excel refuse avec un motif qui accuse les mauvaises
colonnes — dans les trois cas le produit repond « ok » ou « votre fichier est mauvais », et dans
les trois cas c'est faux.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-backlinks-csv-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402

CLIENT = "client.fr"
ACCENTS = "crème brûlée"


# --- les formes que les tableurs produisent vraiment ----------------------------------------

@pytest.mark.parametrize("nom,octets", [
    ("Ahrefs, en-tetes anglais, virgule",
     b"Referring page URL,Linked page URL,Anchor\n"
     b"https://blog.fr/a,https://client.fr/x,mon ancre\n"),
    ("Excel francais, point-virgule",
     "URL de provenance;URL cible;Texte d ancrage\n"
     "https://blog.fr/a;https://client.fr/x;ancre\n".encode("utf-8")),
    ("UTF-8 avec BOM",
     "﻿Source URL,Target URL\nhttps://b.fr/a,https://client.fr/x\n".encode("utf-8")),
    ("tabulation",
     b"Source URL\tTarget URL\nhttps://b.fr/a\thttps://client.fr/x\n"),
])
def test_les_exports_courants_sont_reconnus(nom: str, octets: bytes) -> None:
    kind, rows = m._parse_backlinks_csv(octets, target_host=CLIENT)
    assert kind == "backlinks", nom
    assert len(rows) == 1, (nom, rows)
    assert rows[0]["target_url"].endswith("/x"), (nom, rows)


@pytest.mark.parametrize("kind_attendu,octets", [
    ("domains", b"Domain,Backlinks\nexemple.com,42\nwww.autre.fr,7\n"),
    ("pages", b"Page,Backlinks\nhttps://client.fr/x,12\n"),
    ("anchors", b"Anchor,Backlinks\nvoir ici,9\n"),
])
def test_les_trois_autres_formes_d_export_sont_distinguees(kind_attendu: str, octets: bytes) -> None:
    """Un export « par domaine » verse dans les backlinks donnerait des liens inventes."""
    kind, rows = m._parse_backlinks_csv(octets, target_host=CLIENT)
    assert kind == kind_attendu, (kind, rows)
    assert rows


# --- la garde qui protege les donnees du client ----------------------------------------------

def test_les_liens_vers_un_AUTRE_domaine_sont_ecartes() -> None:
    """La garde la plus importante du parseur.

    Un export Ahrefs couvre souvent plusieurs sites d'un meme compte. Verser les backlinks d'un
    concurrent dans le projet d'un client fausserait tout ce qui s'appuie dessus, sans qu'aucune
    erreur n'apparaisse — le produit dirait simplement « import reussi ».
    """
    melange = (b"Referring page URL,Linked page URL\n"
               b"https://blog.fr/a,https://client.fr/page\n"
               b"https://blog.fr/b,https://concurrent.fr/page\n"
               b"https://blog.fr/c,https://www.client.fr/autre\n")
    _kind, rows = m._parse_backlinks_csv(melange, target_host=CLIENT)
    cibles = [r["target_url"] for r in rows]
    assert len(rows) == 2, cibles
    assert not any("concurrent.fr" in u for u in cibles), cibles
    assert any("www.client.fr" in u for u in cibles), (
        "le www du client a ete pris pour un autre domaine")


def test_sans_domaine_cible_on_importe_tout() -> None:
    """Le filtre ne doit pas s'appliquer quand l'appelant ne sait pas quel est le site."""
    melange = (b"Referring page URL,Linked page URL\n"
               b"https://blog.fr/a,https://client.fr/page\n"
               b"https://blog.fr/b,https://concurrent.fr/page\n")
    _kind, rows = m._parse_backlinks_csv(melange, target_host=None)
    assert len(rows) == 2


# --- l'encodage, la ou le produit mentait ----------------------------------------------------

@pytest.mark.parametrize("encodage", ["utf-8", "utf-8-sig", "cp1252", "utf-16"])
def test_les_accents_survivent_a_l_encodage_du_tableur(encodage: str) -> None:
    """UTF-16 est le « Texte Unicode » d'Excel, et il echouait — pour une raison qui n'a rien a
    voir avec ce que le message d'erreur disait.

    `latin-1` ne leve JAMAIS d'UnicodeDecodeError : tout octet y est valide. Un UTF-16 y tombait
    donc et ressortait avec un octet nul entre chaque lettre ; le client lisait « CSV non reconnu
    (colonnes: ␀S␀o␀u␀r␀c␀e… ) », un motif qui accuse ses en-tetes au lieu de son encodage.
    """
    texte = ("Source URL,Target URL,Anchor\n"
             "https://b.fr/a,https://client.fr/x,%s\n" % ACCENTS)
    octets = texte.encode(encodage)
    kind, rows = m._parse_backlinks_csv(octets, target_host=CLIENT)
    assert kind == "backlinks", encodage
    assert rows and rows[0].get("anchor") == ACCENTS, (encodage, rows)


def test_le_caractere_de_remplacement_ne_doit_jamais_atteindre_les_donnees() -> None:
    """Un import « reussi » qui stocke des losanges noirs est un import rate qui se tait."""
    octets = ("Source URL,Target URL,Anchor\nhttps://b.fr/a,https://client.fr/x,%s\n"
              % ACCENTS).encode("utf-16")
    _kind, rows = m._parse_backlinks_csv(octets, target_host=CLIENT)
    assert "�" not in rows[0].get("anchor", ""), rows


# --- le refus, quand il est justifie ----------------------------------------------------------

def test_un_csv_vraiment_inconnu_est_refuse_en_nommant_ses_colonnes() -> None:
    """Le refus doit aider : le client ne peut corriger que ce qu'on lui montre."""
    with pytest.raises(ValueError) as erreur:
        m._parse_backlinks_csv(b"Colonne A,Colonne B\n1,2\n", target_host=None)
    message = str(erreur.value)
    assert "Colonne A" in message and "Colonne B" in message, message


def test_une_ligne_sans_source_ou_sans_cible_est_ignoree_sans_tout_faire_echouer() -> None:
    """Un export reel contient des lignes vides ; elles ne doivent ni entrer ni tout annuler."""
    troue = (b"Referring page URL,Linked page URL\n"
             b"https://blog.fr/a,https://client.fr/page\n"
             b",https://client.fr/orphelin\n"
             b"https://blog.fr/c,\n")
    _kind, rows = m._parse_backlinks_csv(troue, target_host=CLIENT)
    assert len(rows) == 1, rows
