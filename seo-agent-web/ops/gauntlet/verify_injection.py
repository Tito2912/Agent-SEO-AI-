# -*- coding: utf-8 -*-
"""L'injection a-t-elle produit ce qu'elle annonce ? Crawler les neuf sites et confronter.

C'est l'etape qui doit passer AVANT la moindre correction. Une famille injectee mais non
detectee, ce n'est pas un defaut du correcteur : c'est une fixture qui ne dit pas ce qu'elle
croit dire, et corriger par-dessus reviendrait a payer 265 appels de modele pour mesurer un
mensonge. L'erreur inverse compte autant — une famille QU'ON N'A PAS injectee et qui se
declenche quand meme signale une anomalie parasite, donc une page qui n'en porte plus une seule.

    python ops/gauntlet/verify_injection.py --workdir <dir>              # crawle les 9
    python ops/gauntlet/verify_injection.py --workdir <dir> --stack nuxt # une seule
    python ops/gauntlet/verify_injection.py --workdir <dir> --report     # relit sans recrawler

Le `--max-pages` est le MEME pour tous et doit rester le meme entre ce crawl et celui de
verification d'apres-correction : un plafond different deplace les comptes tout seul et se lit
comme une regression (mesure sur prosperfactory, 50 contre 73 pages).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.gauntlet.catalogue import CATALOGUE  # noqa: E402
from ops.gauntlet.emit_stacks import SITES, skipped  # noqa: E402

AUDIT = ROOT.parent / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py"

# Ces familles-la se declenchent sur un SITE, pas sur une page du parcours : elles decrivent le
# graphe interne et le sitemap du site entier, que les 31 pages injectees modifient forcement.
# Les compter comme « parasites » ferait crier au loup a chaque passage.
SITE_WIDE = {
    "internal_pages", "orphan_page_indexable", "orphan_page_not_indexable",
    "page_has_only_one_dofollow_incoming_internal_link",
    "page_has_only_one_dofollow_incoming_internal_link_links",
    "page_has_only_one_dofollow_incoming_internal_link_indexable",
    "page_has_only_one_dofollow_incoming_internal_link_not_indexable",
    "canonical_url_has_no_incoming_internal_links", "sitemap_non_canonical_page",
    "sitemap_noindex_page", "noindex_page", "noindex_follow_page",
    "redirect_3xx", "http_to_https_redirect", "missing_canonical",
    "meta_description_too_short", "meta_description_too_short_indexable",
    "meta_description_too_short_not_indexable",
}


# Deux familles que le CRAWLER ne peut pas rapporter, quelle que soit la page qu'on lui donne.
# Mesure le 09/09/2026 sur le banc deploye, en crawlant les 73 pages avec `--check-resources`.
# Les laisser dans les attendues ferait echouer chaque verification pour une raison qui n'a rien
# a voir avec l'injection.
NOT_TRIGGERABLE: dict[str, str] = {
    # `double_slash_in_url` et `twitter_card_missing` etaient ici. Les deux raisons etaient
    # fausses, mesure du 12/09/2026 :
    #   - la double barre survivait bien quelque part — dans la reference TELLE QU'ELLE EST
    #     ECRITE, avant `_normalize_url`. Le crawler la releve desormais (`double_slash_refs`) ;
    #   - le controle twitter a DEUX branches, et la note n'en lisait qu'une. Une balise twitter
    #     sans `twitter:card` suffit, Open Graph intact.
    # Ne rien mettre ici sans avoir mesure que la famille est impossible, pas seulement absente.
    "canonical_from_http_to_https":
        "il faut une page SERVIE en http, et l'hote n'en sert aucune : mesure du 12/09/2026, "
        "`http://…/gauntlet/missing-h1` renvoie 301 vers https. Une page redirigee n'entre pas "
        "dans `ok_html_pages`, donc `page_scheme == 'http'` ne peut jamais etre vrai. C'est une "
        "propriete de l'hebergeur, pas du parcours — la famille redeviendra exercable le jour "
        "ou le banc servira une fixture en http.",
}


def expected_families(stack: str) -> set[str]:
    """Ce que le catalogue promet pour cette stack, refus deduits."""
    out: set[str] = set()
    for spec in CATALOGUE:
        if skipped(stack, spec):
            continue
        for name in spec.family.split("+"):
            name = name.strip()
            if name and name not in NOT_TRIGGERABLE:
                out.add(name)
    return out


def crawl(stack: str, workdir: Path, max_pages: int, workers: int) -> Path:
    out = workdir / stack
    out.mkdir(parents=True, exist_ok=True)
    # `--check-resources` est OBLIGATOIRE depuis que le parcours porte des pages a ressources.
    # Sans lui, tout le bloc `_score_resource_issues` est saute et 26 cles n'existent meme pas
    # dans le rapport — les familles d'alt et de redirection d'assets seraient declarees
    # manquantes alors que c'est le crawl qui ne les a pas cherchees.
    cmd = [sys.executable, str(AUDIT), SITES[stack] + "/",
           "--max-pages", str(max_pages), "--workers", str(workers), "--check-resources",
           "--output-dir", str(out)]
    log = out / "crawl.log"
    with open(log, "w", encoding="utf-8") as fh:
        subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT, check=False)
    return out / "report.json"


def detected(report: Path) -> dict[str, int]:
    data = json.loads(report.read_text(encoding="utf-8"))
    issues = data.get("issues") or {}
    out: dict[str, int] = {}
    for key, value in issues.items():
        count = value.get("count") if isinstance(value, dict) else len(value or [])
        if count:
            out[key] = count
    return out


def _urls_of(block: object) -> list[str]:
    """Les URL portees par un bloc d'anomalie, quelle que soit la cle qui les range."""
    if isinstance(block, list):
        return [str(u) for u in block if isinstance(u, str)]
    if not isinstance(block, dict):
        return []
    for key in ("urls", "items", "pages", "examples", "sample"):
        val = block.get(key)
        if isinstance(val, list):
            return [str(u) if isinstance(u, str) else str((u or {}).get("url") or "")
                    for u in val]
    return []


def outside_gauntlet(report: Path, family: str) -> bool:
    """La famille ne touche-t-elle QUE des pages hors du parcours ?

    Les fixtures portent leurs propres defauts, anterieurs au banc : la page `a-propos` d'Astro
    declare une description de 268 caracteres, deliberement, pour la famille des longueurs. Les
    compter comme « parasites » du parcours ferait crier au loup a chaque passage et noierait un
    vrai parasite le jour ou il apparaitra.
    """
    data = json.loads(report.read_text(encoding="utf-8"))
    urls = _urls_of((data.get("issues") or {}).get(family))
    return bool(urls) and not any("/gauntlet" in u for u in urls)


def _slug(url: str) -> str:
    """Le slug de parcours porte par une URL, quelle que soit la convention de la stack."""
    path = str(url or "").split("?", 1)[0].split("#", 1)[0].rstrip("/")
    return path.rsplit("/", 1)[-1].removesuffix(".html")


def crawled_slugs(report: Path) -> set[str]:
    data = json.loads(report.read_text(encoding="utf-8"))
    return {_slug(p.get("url") or "") for p in (data.get("pages") or [])
            if "/gauntlet" in str(p.get("url") or "")}


def carriers(stack: str) -> dict[str, set[str]]:
    """Quelle page du parcours est censee PORTER chaque famille."""
    out: dict[str, set[str]] = {}
    for spec in CATALOGUE:
        if skipped(stack, spec):
            continue
        for name in spec.family.split("+"):
            name = name.strip()
            if name and name not in NOT_TRIGGERABLE:
                out.setdefault(name, set()).add(spec.slug)
    return out


def compare(stack: str, report: Path) -> dict:
    want = expected_families(stack)
    got = detected(report)
    # Une famille peut se declencher sous sa variante d'indexabilite (`_indexable`,
    # `_not_indexable`) : `missing_h1` compte comme trouvee si `missing_h1_indexable` l'est.
    def seen(name: str) -> bool:
        return any(k == name or k.startswith(name + "_") for k in got)

    # La page qui porte la famille a-t-elle seulement ete SERVIE ? Sans cette question, une
    # famille se declarait detectee parce que la cle existait, sans regarder d'ou. Mesure du
    # 11/09/2026 : les quatre pages de ressources n'etaient pas deployees, et `image_redirects`
    # comptait quand meme — sa seule preuve etait la redirection http->https de `og.png`, portee
    # par la page `mixed-image`, qui n'a rien a voir. L'instrument confirmait une injection qui
    # n'avait pas eu lieu ; six familles sur sept etaient fausses.
    porte = carriers(stack)
    servies = crawled_slugs(report)
    attendues_pages = {s.slug for s in CATALOGUE if not skipped(stack, s)}
    absentes = sorted(attendues_pages - servies)

    missing = sorted(n for n in want
                     if not seen(n) or not (porte.get(n, set()) & servies))
    base = {n.replace("_indexable", "").replace("_not_indexable", "") for n in got}
    extra = sorted(k for k in got
                   if k not in SITE_WIDE
                   and not any(k == n or k.startswith(n + "_") for n in want)
                   and k.replace("_indexable", "").replace("_not_indexable", "") not in want
                   and k not in base - want)
    hors = [k for k in extra if outside_gauntlet(report, k)]
    extra = [k for k in extra if k not in hors]
    return {"stack": stack, "attendues": len(want), "detectees": len(want) - len(missing),
            "manquantes": missing, "parasites": extra, "hors_parcours": hors,
            "pages_absentes": absentes}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--stack", action="append", default=None)
    ap.add_argument("--max-pages", type=int, default=80)
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--report", action="store_true",
                    help="relire les rapports deja ecrits sans recrawler")
    args = ap.parse_args()

    workdir = Path(args.workdir)
    stacks = args.stack or list(SITES)
    rows = []
    for stack in stacks:
        report = workdir / stack / "report.json"
        if not args.report:
            report = crawl(stack, workdir, args.max_pages, args.workers)
        if not report.exists():
            print(f"{stack:<12} PAS DE RAPPORT ({report})")
            continue
        rows.append(compare(stack, report))

    print(f"\n{'stack':<12} {'attendues':>9} {'detectees':>9}  manquantes / parasites")
    ok = True
    for r in rows:
        print(f"{r['stack']:<12} {r['attendues']:>9} {r['detectees']:>9}  "
              f"{len(r['manquantes'])} / {len(r['parasites'])}")
        for slug in r.get("pages_absentes", []):
            print(f"             PAGE ABSENTE (non servie) {slug}")
        for name in r["manquantes"]:
            print(f"             MANQUE   {name}")
        for name in r["parasites"]:
            print(f"             PARASITE {name}")
        for name in r.get("hors_parcours", []):
            print(f"             (hors parcours, defaut propre a la fixture) {name}")
        ok = (ok and not r["manquantes"] and not r["parasites"]
              and not r.get("pages_absentes"))
    (workdir / "injection.json").write_text(
        json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print("\nInjection conforme." if ok else
          "\nInjection NON conforme — corriger les fixtures avant toute correction.")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
