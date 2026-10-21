"""Inventory route declarations and template interactions without starting the application.

Run from any directory: python seo-agent-web/ops/ux_inventory.py
The generated inventory is evidence of code coverage, not browser verification or an
authorization audit. Dynamic JavaScript controls still need a manual review.
"""
from __future__ import annotations

import argparse
import ast
import subprocess
from datetime import datetime, timezone
import re
from pathlib import Path

WEB = Path(__file__).resolve().parents[1]
REPO = WEB.parent
DESTINATION = WEB / "docs" / "UX_SCREEN_INVENTORY_CURRENT.md"
HISTORY = WEB / "docs" / "UX_SCREEN_INVENTORY.md"


def cell(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ").strip()


def revision() -> str:
    try:
        head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, capture_output=True, text=True, check=True).stdout.strip()
        inputs = [str((WEB / name).relative_to(REPO)) for name in ("backend", "templates", "static", "content", "ops")]
        dirty = subprocess.run(["git", "status", "--porcelain", "--untracked-files=all", "--", *inputs],
                               cwd=REPO, capture_output=True, text=True, check=True).stdout.strip()
        return head + (" (modifications locales du code)" if dirty else "")
    except (OSError, subprocess.CalledProcessError):
        return "révision Git non disponible"


def inventory() -> str:
    rows = []
    framework_routes = []
    for source in sorted((WEB / "backend").glob("*.py")):
        tree = ast.parse(source.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "FastAPI":
                options = {kw.arg: ast.literal_eval(kw.value) for kw in node.keywords
                           if kw.arg in {"docs_url", "openapi_url", "swagger_ui_oauth2_redirect_url"}}
                for name in ("docs_url", "openapi_url"):
                    if options.get(name):
                        framework_routes.append((options[name], name, source.relative_to(WEB), node.lineno))
                if options.get("docs_url"):
                    redirect = options.get("swagger_ui_oauth2_redirect_url", "/docs/oauth2-redirect")
                    if redirect:
                        framework_routes.append((redirect, "swagger_ui_oauth2_redirect_url (défaut FastAPI)",
                                                 source.relative_to(WEB), node.lineno))
        for function in ast.walk(tree):
            if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            templates = {
                node.value for node in ast.walk(function)
                if isinstance(node, ast.Constant) and isinstance(node.value, str)
                and node.value.endswith(".html") and (WEB / "templates" / node.value).is_file()
            }
            # The legal helper explicitly chooses the existing English sibling at runtime.
            if any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "_legal_page"
                   for node in ast.walk(function)):
                templates.update(name.replace(".html", "_en.html") for name in list(templates)
                                 if (WEB / "templates" / name.replace(".html", "_en.html")).is_file())
            templates = sorted(templates)
            for decorator in function.decorator_list:
                if not (isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute)
                        and isinstance(decorator.func.value, ast.Name)
                        and decorator.func.value.id in {"app", "router"}
                        and decorator.func.attr in {"get", "post", "put", "patch", "delete", "api_route"}):
                    continue
                route = ast.literal_eval(decorator.args[0])
                methods = next((ast.literal_eval(kw.value) for kw in decorator.keywords if kw.arg == "methods"),
                               [decorator.func.attr.upper()])
                rows.append((route, ", ".join(methods), ", ".join(templates) or "—",
                             source.relative_to(WEB), function.lineno, function.name))

    templates = sorted((WEB / "templates").glob("*.html"))
    rendered = {name for _, _, names, _, _, _ in rows for name in names.split(", ") if name != "—"}
    parts = ["# Inventaire courant des écrans et interactions Noyaru", "",
             f"Généré par `python seo-agent-web/ops/ux_inventory.py`. Révision du code : `{revision()}`. "
             f"Date UTC : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')}. "
             "Le [relevé initial et ses compléments](UX_SCREEN_INVENTORY.md) sont conservés.",
             "", f"{len(rows)} déclarations de routes ; {len(rendered)} templates repérés dans les gestionnaires de routes ; "
             f"{len(templates)} templates HTML au total (inclusions et anciens templates compris). "
             f"{len(framework_routes)} routes automatiques FastAPI complètent les déclarations, "
             f"soit {len(rows) + len(framework_routes)} déclarations et routes automatiques, hors montage statique. Les alias de langue du middleware ne sont pas des déclarations supplémentaires.", "",
             "Les verbes sont ceux déclarés dans le code. Une route peut servir plusieurs états ou rediriger. "
             "Les droits sont déterminés par les middlewares et les contrôles de projet/compte, pas par cette extraction. "
             "Une référence littérale ou une variante du résolveur juridique n’atteste pas son exécution. "
             "Les rendus entièrement dynamiques peuvent ne pas être attribués à une route par cette extraction. "
             "Voir [le bilan transversal](UX_CROSS_JOURNEY_LOT16.md) pour les vérifications et leurs limites.", "",
             "## Routes, écrans, actions et sorties", "",
             "Les API, exports, callbacks et actions POST sont inclus pour rendre visibles les étapes intermédiaires. "
             "Le montage des fichiers statiques est distingué des écrans. Les routes automatiques du framework "
             "sont recensées séparément ci-dessous.", "",
             "| Route | Méthode | Template(s) | Source et fonction |",
             "| --- | --- | --- | --- |"]
    for path, methods, names, source, line, function in sorted(rows):
        parts.append(f"| `{cell(path)}` | {methods} | {cell(names)} | "
                     f"[{cell(source)}:{line}](../{source.as_posix()}#L{line}) · `{function}` |")
    parts += ["", "## Routes automatiques de FastAPI", "",
              "Ces chemins sont extraits de la configuration FastAPI. Le rapprochement avec le registre "
              "effectivement chargé est consigné dans les preuves du bilan transversal, pas déduit de ce relevé. "
              "Ils ne passent pas par un template du dépôt. "
              "L’explorateur et son schéma exigent une session via le middleware ; le callback Swagger "
              "reste sous le préfixe public `/docs`. Aucun accès administrateur supplémentaire n’est "
              "déclaré pour l’explorateur. Le chemin de retour OAuth2 ci-dessous est le défaut "
              "de la version FastAPI utilisée par cette application.", "",
              "| Route | Méthodes | Configuration source |", "| --- | --- | --- |"]
    for path, option, source, line in framework_routes:
        parts.append(f"| `{cell(path)}` | GET, HEAD | `{option}` · "
                     f"[{source}:{line}](../{source.as_posix()}#L{line}) |")
    parts += ["", "## Fenêtres, formulaires et panneaux par template", "",
              "Extraction statique des balises `form`, `dialog`, `details` et des régions `role=dialog`. "
              "Les expressions Jinja sont conservées. Les boutons pilotés par JavaScript sont examinés dans le bilan ; "
              "cette liste n’atteste ni l’ouverture d’une fenêtre ni la soumission d’un formulaire.", ""]
    for source in templates:
        contents = source.read_text(encoding="utf-8")
        direct = source.name in rendered
        parts += [f"### {source.name}", "",
                  "Référence repérée dans un gestionnaire de route ou sa variante juridique." if direct else "Inclusion, gabarit partagé, rendu dynamique ou ancien template sans référence littérale de route.", ""]
        interactions = list(re.finditer(r"<(?:form|dialog|details)\b[^>]*>|<[^>]+role=[\"']dialog[\"'][^>]*>", contents, re.S))
        if not interactions:
            parts += ["Aucune balise de formulaire, fenêtre ou panneau repliable.", ""]
            continue
        for match in interactions:
            line = contents.count("\n", 0, match.start()) + 1
            tag = re.sub(r"\s+", " ", match.group()).replace("`", "'")
            parts.append(f"- Ligne {line} : `{tag}`")
        parts.append("")
    parts += ["## Variantes des articles publics", "",
              "Une ligne par contenu versionné. La preuve navigateur de chaque variante est indiquée dans l’audit, "
              "pas déduite de sa présence dans ce tableau.", "",
              "| URL | Contenu source |", "| --- | --- |"]
    for directory, prefix in (("docs", "/docs"), ("blog", "/ressources-seo")):
        for source in sorted((WEB / "content" / directory).rglob("*.md")):
            relative = source.relative_to(WEB).as_posix()
            language = source.parent.name if source.parent != WEB / "content" / directory else "fr"
            localized = prefix if language == "fr" else "/" + language + prefix
            parts.append(f"| `{localized}/{source.stem}` | [{source.name}](../{relative}) |")
    parts.append("")
    return "\n".join(parts)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DESTINATION, help="Fichier courant à générer ; le relevé historique est préservé.")
    args = parser.parse_args()
    if args.output.resolve() == HISTORY.resolve():
        parser.error("Le relevé historique est conservé ; choisissez un autre fichier --output.")
    args.output.write_text(inventory(), encoding="utf-8")
    try:
        print(args.output.relative_to(REPO))
    except ValueError:
        print(args.output)
