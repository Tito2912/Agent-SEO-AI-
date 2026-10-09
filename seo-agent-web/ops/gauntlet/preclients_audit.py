"""Read-only capability matrix. Named records and policy flags are not repair certification."""

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
from ops.gauntlet.acceptance_inventory import family, inventory  # noqa: E402


def audit(backend, records: dict[str, dict]) -> dict:
    result = inventory(backend, records)
    rows = []
    for key in sorted(backend.dash.ISSUE_CATALOG):
        controls = backend._issue_correction_controls(key)
        if (set(controls) != {"deep_fix", "url_fix"} or any(type(v) is not bool for v in controls.values())
                or controls["deep_fix"] != backend._github_issue_auto_fixable(key)
                or controls["url_fix"] and not controls["deep_fix"]):
            raise ValueError("Inconsistent individual correction policy: " + key)
        rows.append({"key": key, "family": family(key), **controls,
            "manual_only": not controls["deep_fix"], "all_occurrences_or_stacks_certified": False})
    result.update(status="capability_inventory_not_release_certification", individual_policy=rows,
        manual_catalog_key_count=sum(row["manual_only"] for row in rows),
        deep_only_families=sorted({row["family"] for row in rows if row["deep_fix"] and not row["url_fix"]}),
        preview_capable_families=sorted({row["family"] for row in rows if row["url_fix"]}),
        individual_api_requests_exercised_by_this_inventory=False,
        real_provider_or_github_requests=0, deployed_agent_checked=False,
        remaining_release_gates=["staging_startup_scheduler_and_capacity", "receipt_retention_and_operator_recovery",
            "test_mode_onboarding_github_permissions_and_payment_lifecycle", "customer_stack_allowlist_and_supervised_pilots"],
        release_checklist="ops/gauntlet/PRECLIENTS.md")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Use a new output file; no existing report is overwritten.")
    with tempfile.TemporaryDirectory(prefix="seo-preclients-audit-") as scratch:
        work = Path(scratch)
        os.environ.update(DATABASE_URL="sqlite:///" + (work / "isolated.db").as_posix(),
            SEO_AGENT_DATA_DIR=str(work / "data"), SEO_AGENT_RUNS_DIR=str(work / "runs"),
            SEO_AGENT_DISABLE_WORKER="true", SEO_AGENT_SECRET_KEY="fixture-only-inventory-secret",
            SEO_CORRECTION_AI_PROVIDER="none", ANTHROPIC_API_KEY="", OPENAI_API_KEY="")
        from backend import app

        paths = sorted(Path(__file__).parent.glob("validation-*.json"))
        records = {path.name: json.loads(path.read_text(encoding="utf-8")) for path in paths}
        result = audit(app, records)
        result["record_sha256"] = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: result[k] for k in ("status", "catalog_key_count", "claimed_family_count",
        "manual_catalog_key_count", "client_readiness_certified")}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
