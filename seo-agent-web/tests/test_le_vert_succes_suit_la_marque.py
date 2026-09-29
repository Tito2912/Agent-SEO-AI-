# -*- coding: utf-8 -*-
"""Le vert « succès » suit la marque, et passe par ses variables.

28/09/2026 : l'interface a pris le vert du logo Noyaru (`--accent`), mais le vert « succès »
des badges restait émeraude — #10B981 / #34D399, écrit EN DUR à une quarantaine d'endroits dans
une douzaine de gabarits. Changer une couleur écrite quarante fois, c'est en oublier une.
Tout passe désormais par `--ok`, `--ok-2` (texte) et `--ok-rgb` (fonds translucides).
"""
from __future__ import annotations

import re
from pathlib import Path

WEB = Path(__file__).resolve().parents[1]
EMERAUDE = re.compile(r"#34d399|#10b981|#4ade80|#22c55e|rgba\(\s*16\s*,\s*185\s*,\s*129", re.I)


def test_plus_aucun_vert_succes_en_dur() -> None:
    fichiers = sorted((WEB / "templates").glob("*.html")) + [WEB / "static" / "styles.css"]
    fautifs = ["%s:%d" % (f.name, n) for f in fichiers
               for n, l in enumerate(f.read_text(encoding="utf-8").splitlines(), 1) if EMERAUDE.search(l)]
    assert not fautifs, fautifs


def test_les_variables_existent_et_sont_de_la_famille_de_la_marque() -> None:
    css = (WEB / "static" / "styles.css").read_text(encoding="utf-8")
    valeurs = dict(re.findall(r"(--ok(?:-2|-rgb)?):\s*([^;]+);", css))
    assert set(valeurs) == {"--ok", "--ok-2", "--ok-rgb"}, valeurs
    r, g, b = (int(x) for x in valeurs["--ok-rgb"].split(","))
    # Un vert sourd comme le logo : le vert domine, sans la saturation de l'emeraude (b > r).
    assert g > r and g > b and abs(b - r) < 60, (r, g, b)
