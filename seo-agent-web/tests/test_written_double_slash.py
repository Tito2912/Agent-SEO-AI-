# -*- coding: utf-8 -*-
"""La double barre ne survit qu'AVANT la normalisation.

`_normalize_url` collapse `/{2,}` en `/`, et c'est voulu : sans cela le crawler visiterait deux
fois la meme page. Mais ce collapse effacait la seule trace de l'anomalie, si bien que
`double_slash_in_url` ne pouvait se declencher sur AUCUN site — alors qu'Ahrefs la rapporte, et
que le correcteur porte un `_rewrite_double_slash` qui n'avait jamais de quoi s'executer.

La note qui declarait cette famille « indeclenchable » avait donc raison sur le fait et tort sur
la conclusion : la trace existait, une ligne plus haut.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
_SPEC = importlib.util.spec_from_file_location(
    "seo_audit_double_slash",
    REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py",
)
assert _SPEC and _SPEC.loader
seo_audit = importlib.util.module_from_spec(_SPEC)
sys.modules["seo_audit_double_slash"] = seo_audit
_SPEC.loader.exec_module(seo_audit)

BASE = "https://exemple.test/gauntlet/double-slash"


def test_le_cas_mesure_sur_le_parcours():
    """Un lien protocole-relatif dont le chemin porte la double barre."""
    trouve = seo_audit._written_double_slash("//exemple.test//a-propos", BASE)
    assert trouve == "https://exemple.test//a-propos"


def test_une_double_barre_au_milieu_du_chemin_est_reconnue():
    """Attention au piege : `//a-propos` n'est PAS un chemin relatif double, c'est une URL
    protocole-relative dont `urljoin` fait un HOTE. Le vrai cas est la double barre interne."""
    assert seo_audit._written_double_slash("/blog//article", BASE) == \
        "https://exemple.test/blog//article"


def test_une_url_saine_ne_renvoie_rien():
    assert seo_audit._written_double_slash("/a-propos", BASE) == ""
    assert seo_audit._written_double_slash("https://exemple.test/a-propos", BASE) == ""


def test_le_double_slash_du_protocole_n_est_pas_une_anomalie():
    """`//hote/page` est protocole-relatif : `urljoin` le resout, le chemin reste propre."""
    assert seo_audit._written_double_slash("//exemple.test/a-propos", BASE) == ""


def test_les_schemas_non_web_sont_ignores():
    for lien in ("mailto:x@y.z", "tel:+33100000000", "javascript:void(0)", "#ancre", ""):
        assert seo_audit._written_double_slash(lien, BASE) == "", lien


def test_la_normalisation_efface_bien_la_trace():
    """Le fait qui rend la capture necessaire — mesure, pas supposition."""
    normalisee = seo_audit._normalize_url("//exemple.test//a-propos", base=BASE)
    assert normalisee == "https://exemple.test/a-propos"
    assert "//a-propos" not in normalisee
