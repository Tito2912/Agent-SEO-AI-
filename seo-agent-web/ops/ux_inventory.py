"""Inventory route declarations and template interactions without starting the application.

Run from any directory: python seo-agent-web/ops/ux_inventory.py
The generated inventory is evidence of code coverage, not browser verification or an
authorization audit. Dynamic JavaScript controls still need a manual review.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

WEB = Path(__file__).resolve().parents[1]
REPO = WEB.parent
DESTINATION = WEB / "docs" / "UX_SCREEN_INVENTORY.md"


def cell(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ").strip()


def inventory() -> str:
    rows = []
    for source in sorted((WEB / "backend").glob("*.py")):
        tree = ast.parse(source.read_text(encoding="utf-8"))
        for function in ast.walk(tree):
            if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            templates = sorted({
                node.args[0].value for node in ast.walk(function)
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "TemplateResponse" and node.args
                and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str)
            })
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
    parts = ["# Inventaire des écrans et interactions Noyaru", "",
             "Généré par `python seo-agent-web/ops/ux_inventory.py`. Base de l’audit : `44f575c` (main, 19 octobre 2026).",
             "", f"{len(rows)} déclarations de routes ; {len(rendered)} templates directement référencés par les routes ; "
             f"{len(templates)} templates HTML au total (inclusions et anciens templates compris).", "",
             "Les verbes sont ceux déclarés dans le code. Une route peut servir plusieurs états ou rediriger. "
             "Les droits sont déterminés par les middlewares et les contrôles de projet/compte, pas par cette extraction. "
             "Voir [l’audit](UX_AUDIT_2026-10-19.md) pour les accès, constats et preuves navigateur.", "",
             "## Routes, écrans, actions et sorties", "",
             "Les API, exports, callbacks et actions POST sont inclus pour rendre visibles les étapes intermédiaires. "
             "Les fichiers statiques montés et les routes implicites du framework ne sont pas des écrans.", "",
             "| Route | Méthode | Template(s) | Source et fonction |",
             "| --- | --- | --- | --- |"]
    for path, methods, names, source, line, function in sorted(rows):
        parts.append(f"| `{cell(path)}` | {methods} | {cell(names)} | "
                     f"[{cell(source)}:{line}](../{source.as_posix()}#L{line}) · `{function}` |")
    parts += ["", "## Fenêtres, formulaires et panneaux par template", "",
              "Extraction statique des balises `form`, `dialog`, `details` et des régions `role=dialog`. "
              "Les expressions Jinja sont conservées. Les boutons pilotés par JavaScript sont examinés dans le bilan ; "
              "cette liste n’atteste ni l’ouverture d’une fenêtre ni la soumission d’un formulaire.", ""]
    for source in templates:
        contents = source.read_text(encoding="utf-8")
        direct = source.name in rendered
        parts += [f"### {source.name}", "",
                  "Template de route." if direct else "Inclusion, gabarit partagé ou ancien template sans référence directe de route.", ""]
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
        for source in sorted((WEB / "content" / directory).glob("*.md")):
            relative = source.relative_to(WEB).as_posix()
            parts.append(f"| `{prefix}/{source.stem}` | [{source.name}](../{relative}) |")
    parts.append("")
    return "\n".join(parts)


if __name__ == "__main__":
    DESTINATION.write_text(inventory(), encoding="utf-8")
    print(DESTINATION.relative_to(REPO))
