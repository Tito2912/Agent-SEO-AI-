#!/usr/bin/env python3
"""Inspect pending correction receipts; explicitly abandon unpublished, unbilled work."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def parser():
    cli = argparse.ArgumentParser(description=__doc__)
    commands = cli.add_subparsers(dest="command", required=True)
    listing = commands.add_parser("list")
    listing.add_argument("--payer")
    inspection = commands.add_parser("inspect")
    inspection.add_argument("operation_id")
    abandonment = commands.add_parser("abandon")
    abandonment.add_argument("operation_id")
    abandonment.add_argument("--operator", required=True)
    abandonment.add_argument("--reason", required=True)
    abandonment.add_argument("--retain-branch", action="store_true")
    return cli


def main(argv=None):
    args = parser().parse_args(argv)
    # Import the service's configuration/credential helpers, never start its workers.
    os.environ["SEO_AGENT_DISABLE_WORKER"] = "true"
    from backend import app, correction_reconciliation as doctor
    from backend.correction_guard import CorrectionBusy, CorrectionStoreUnavailable

    def reader(payer):
        token, source = app._effective_user_connection_value(user_id=payer, key="GITHUB_TOKEN")
        if not token or source != "user":
            raise CorrectionStoreUnavailable()
        return doctor.GitHubReader(token)

    try:
        if args.command == "list":
            result = doctor.list_pending(app.DB, payer=args.payer)
        else:
            result = doctor.reconcile(app.DB, args.operation_id, reader,
                                      abandon=args.command == "abandon",
                                      retain_branch=getattr(args, "retain_branch", False),
                                      operator=getattr(args, "operator", ""), reason=getattr(args, "reason", ""))
        print(json.dumps(result, ensure_ascii=True, indent=2))
        return 0
    except doctor.ReconciliationRefused as exc:
        print(json.dumps({"ok": False, "refused": str(exc)}))
        return 2
    except CorrectionBusy:
        print(json.dumps({"ok": False, "error": "correction_in_progress"}))
        return 3
    except CorrectionStoreUnavailable:
        print(json.dumps({"ok": False, "error": "verification_unavailable"}))
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
