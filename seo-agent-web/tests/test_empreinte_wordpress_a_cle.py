"""Une empreinte de connexion WordPress stockée en base ne permet pas d'éprouver un mot de passe hors ligne.

L'empreinte servait déjà à refuser un aperçu préparé avec une autre connexion ; elle était un
SHA-256 nu des identifiants, mot de passe compris. Elle est maintenant calculée avec la clé du
serveur. Les opérations enregistrées avant restent restaurables, et leur empreinte est remplacée
à la première écriture.
"""
from __future__ import annotations

import hashlib
import json

from backend import app as m, cms_routes, wordpress_cms as wp
from backend.models import CmsChange, Project
from test_cms_integrations import action, cms as cms, connect, journey as journey, preview, row, writes

IDENTIFIANTS = {"site_url": "https://atelier.example", "username": "editor", "password": "mot-de-passe"}


def _identifiants(data):
    with m.DB.session() as db:
        project = db.get(Project, data["project_id"])
    return cms_routes.credentials(project)


def _sha256_nu(c):
    return hashlib.sha256(json.dumps(c, sort_keys=True).encode()).hexdigest()


def test_l_empreinte_depend_de_la_cle_du_serveur():
    a, b = wp.fingerprint(IDENTIFIANTS, b"cle-a"), wp.fingerprint(IDENTIFIANTS, b"cle-b")
    assert a != b and _sha256_nu(IDENTIFIANTS) not in {a, b, a.removeprefix("hmac:")}
    assert wp.same_connection(IDENTIFIANTS, a, b"cle-a")
    assert not wp.same_connection(IDENTIFIANTS, a, b"cle-b")
    assert not wp.same_connection({**IDENTIFIANTS, "password": "autre"}, a, b"cle-a")


def test_l_apercu_enregistre_l_empreinte_a_cle(cms):
    connect(cms)
    stockee = row(preview(cms)).data["connection"]
    c = _identifiants(cms)
    assert stockee == wp.fingerprint(c, cms_routes.fingerprint_key())
    assert stockee != _sha256_nu(c)


def test_une_ancienne_empreinte_reste_utilisable_puis_est_remplacee(cms):
    connect(cms)
    change = preview(cms)
    c = _identifiants(cms)
    with m.DB.session() as db:
        ligne = db.get(CmsChange, change)
        ligne.data = {**ligne.data, "connection": _sha256_nu(c)}
        db.commit()
    reponse = action(cms, change, "apply")
    assert "err=" not in reponse.headers["location"]
    assert len(writes(cms)) == 1 and row(change).state == "applied"
    assert row(change).data["connection"] == wp.fingerprint(c, cms_routes.fingerprint_key())
