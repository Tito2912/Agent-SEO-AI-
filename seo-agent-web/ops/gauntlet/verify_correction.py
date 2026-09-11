# -*- coding: utf-8 -*-
"""L'anomalie a-t-elle DISPARU ? La seule question que le banc ne posait pas.

Le verdict `ok` de `run.py` signifie « un fichier a ete patche », pas « la famille ne se
declenche plus ». Mesure du 11/09/2026 : `missing_alt_text` etait comptee reussie sur les NEUF
stacks alors que le modele modifiait six pages sans image pendant que l'image fautive restait
intacte. Rien ne l'a signale.

Ce script recrawle la PREVIEW de chaque PR de correction et confronte, famille par famille, le
compte d'avant a celui d'apres.

    python verifier_apres_correction.py <prefixe_des_passages> <dossier_des_references>

Piege connu et assume : Netlify sert toute preview en `noindex`, donc les familles a variante
d'indexabilite se deplacent d'une variante a l'autre. On les REGROUPE avant de comparer, ce qui
rend la comparaison honnete sans avoir a fermer les yeux sur elles.
"""

import json
import os
import subprocess
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    "..", "..", "..", "..", ".."))
AUDIT = None
STACKS = ["static-html", "astro", "hugo", "jekyll", "gatsby", "nuxt",
          "sveltekit", "next-pages", "next-app"]


# Familles que le CRAWLER replie l'une dans l'autre selon l'indexabilite. Les comparer separement
# produit une fausse alerte des que l'indexabilite change entre les deux crawls — et elle change
# toujours, puisque Netlify sert toute preview en `noindex`. Mesure du 11/09/2026 : sur hugo,
# l'index du parcours (sans description) passait de `meta_description_too_short_indexable` a
# `missing_meta_description`, et le controle criait a la regression sur une page qu'aucune
# correction n'avait touchee.
_REPLIS = {
    "missing_meta_description": "description",
    "meta_description_too_short": "description",
    "meta_description_too_long": "description",
}


def base(key: str) -> str:
    nu = key.removesuffix("_not_indexable").removesuffix("_indexable")
    return _REPLIS.get(nu, nu)


def _pr_de_branche(stack: str, branche: str) -> str:
    import urllib.request
    tok = open(os.path.expanduser("~/.noyaru-token"), encoding="utf-8").read().strip()
    url = ("https://api.github.com/repos/pployeraffiliation-a11y/noyaru-stack-%s/pulls"
           "?head=pployeraffiliation-a11y:%s&state=all" % (stack, branche))
    req = urllib.request.Request(url)
    req.add_header("Authorization", "Bearer " + tok)
    req.add_header("Accept", "application/vnd.github+json")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            rows = json.load(resp)
        return str(rows[0]["number"]) if rows else ""
    except Exception:
        return ""


def comptes(path: str) -> dict[str, int]:
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    out: dict[str, int] = {}
    for key, value in (data.get("issues") or {}).items():
        n = value.get("count") if isinstance(value, dict) else len(value or [])
        if n:
            out[base(key)] = out.get(base(key), 0) + int(n)
    return out


def main() -> int:
    prefixe, refs, audit = sys.argv[1], sys.argv[2], sys.argv[3]
    lignes = []
    for stack in STACKS:
        run_json = os.path.join(prefixe + stack, "gauntlet_run.json")
        ref_json = os.path.join(refs, stack, "report.json")
        if not (os.path.exists(run_json) and os.path.exists(ref_json)):
            print("  %-12s pas de passage" % stack)
            continue
        with open(run_json, encoding="utf-8") as fh:
            run = json.load(fh)
        # Le numero de PR n'est pas enregistre par le passage : on le retrouve par la BRANCHE,
        # ce qui evite d'ajouter un couplage entre le correcteur et ce controle.
        branche = run.get("branch") or ""
        pr = _pr_de_branche(stack, branche)
        if not pr:
            print("  %-12s aucune PR pour %s" % (stack, branche))
            continue
        url = "https://deploy-preview-%s--noyaru-stack-%s.netlify.app/" % (pr, stack)
        out = os.path.join(prefixe + stack, "apres")
        os.makedirs(out, exist_ok=True)
        cmd = [sys.executable, audit, url, "--max-pages", "90", "--workers", "3",
               "--check-resources", "--output-dir", out]
        with open(os.path.join(out, "crawl.log"), "w", encoding="utf-8") as fh:
            subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT, check=False)
        apres_json = os.path.join(out, "report.json")
        if not os.path.exists(apres_json):
            print("  %-12s preview non crawlable" % stack)
            continue
        avant, apres = comptes(ref_json), comptes(apres_json)
        corrigees = sorted(k for k in avant if apres.get(k, 0) < avant[k])
        tenaces = sorted(k for k in avant if apres.get(k, 0) >= avant[k])
        lignes.append((stack, len(avant), len(corrigees), tenaces))
        print("  %-12s familles avant=%-3d en baisse=%-3d inchangees=%d"
              % (stack, len(avant), len(corrigees), len(tenaces)))
    print()
    for stack, _n, _c, tenaces in lignes:
        for key in tenaces:
            print("    %-12s INCHANGEE %s" % (stack, key))
    return 0


raise SystemExit(main())
