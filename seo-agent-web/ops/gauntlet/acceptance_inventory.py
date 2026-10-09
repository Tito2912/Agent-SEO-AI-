"""Read-only inventory of claimed families and named validation-record references.

A name in a record is not proof of a successful repair. This inventory deliberately
does not promote mentions, skipped runs or failing runs to client readiness.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile

WEB = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(WEB))


def family(key: str) -> str:
    return key.removesuffix("_not_indexable").removesuffix("_indexable")


def names(value) -> set[str]:
    if isinstance(value, str):
        return {family(value)}
    if isinstance(value, dict):
        return {family(key) for key in value} | {name for child in value.values() for name in names(child)}
    if isinstance(value, list):
        return {name for child in value for name in names(child)}
    return set()


def inventory(backend, records: dict[str, dict]) -> dict:
    catalog = backend.dash.ISSUE_CATALOG
    claimed = {family(key) for key in catalog if backend._github_issue_auto_fixable(key)}
    references = {name: names(record) for name, record in records.items()}
    rows = []
    for key in sorted(claimed):
        variants = sorted(k for k in catalog if family(k) == key)
        visible = [k for k in variants if backend._github_issue_auto_fixable(k)
                   and k not in backend.dash.NON_ISSUE_KEYS and not backend.dash.is_delta_issue_key(k)]
        mentions = sorted(name for name, keys in references.items() if key in keys)
        rows.append({"family": key, "catalog_keys": variants, "visible_offered_keys": visible,
                     "named_validation_records": mentions, "all_occurrences_or_stacks_certified": False})
    return {"catalog_key_count": len(catalog), "claimed_family_count": len(rows),
            "visible_offered_family_count": sum(bool(row["visible_offered_keys"]) for row in rows),
            "record_mentions_are_not_success_verdicts": True, "client_readiness_certified": False,
            "families_without_named_record": [row["family"] for row in rows if not row["named_validation_records"]],
            "families": rows}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Use a new output file; no existing report is overwritten.")
    with tempfile.TemporaryDirectory(prefix="seo-acceptance-inventory-") as scratch:
        os.environ.update({"DATABASE_URL": "sqlite:///" + (Path(scratch) / "isolated.db").as_posix(),
            "SEO_AGENT_DATA_DIR": str(Path(scratch) / "data"), "SEO_AGENT_RUNS_DIR": str(Path(scratch) / "runs"),
            "SEO_AGENT_DISABLE_WORKER": "true", "SEO_AGENT_SECRET_KEY": "fixture-only-inventory-secret",
            "SEO_CORRECTION_AI_PROVIDER": "none", "ANTHROPIC_API_KEY": "", "OPENAI_API_KEY": ""})
        from backend import app

        files = sorted(Path(__file__).parent.glob("validation-*.json"))
        result = inventory(app, {path.name: json.loads(path.read_text(encoding="utf-8")) for path in files})
        result["record_sha256"] = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: result[k] for k in ("catalog_key_count", "claimed_family_count",
        "visible_offered_family_count", "families_without_named_record", "client_readiness_certified")}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
