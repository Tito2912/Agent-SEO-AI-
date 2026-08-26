#!/usr/bin/env python3
"""Say exactly why an email did not leave the building.

Signup answers "l'email de vérification n'a pas pu partir" and nothing else, because the
handler catches every exception and turns it into one message for the visitor. Finding the
cause meant a signup round-trip per attempt — each one leaving another unverified account
behind — followed by a hunt through logs for a line that may sit downstream of the failure.

Run this on the service instead:

    cd /app/seo-agent-web && python ops/mail_doctor.py destinataire@exemple.fr

Lives in `ops/` rather than `tools/` on purpose: .gitignore excludes `tools/` as "dev-only
notes/tools (keep local, not in SaaS repo)", and this one has to ship, because the only place
worth running it is the service that cannot send the mail.

It prints the resolved configuration (never the password), which transport that configuration
selects, and then actually sends — reporting the full traceback rather than a summary, since
the useful part of an SMTP or API refusal is usually the server's own wording.
"""

from __future__ import annotations

import sys
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend import app as seo_app  # noqa: E402


def _redacted(cfg: dict) -> dict:
    out = {}
    for key, value in cfg.items():
        if key == "password":
            out[key] = f"<{len(str(value or ''))} caractères>" if value else "<vide>"
        else:
            out[key] = value
    return out


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__)
        return 2
    to_addr = argv[1].strip()

    cfg = seo_app._smtp_config()
    if not cfg:
        print("SMTP non configuré: SMTP_HOST ou l'expéditeur est absent.")
        print("Conséquence: _email_verification_enabled() est False, donc l'inscription")
        print("ne tente aucun envoi et ne signale aucune erreur — elle passe simplement.")
        return 1

    print("--- configuration résolue ---")
    for key, value in sorted(_redacted(cfg).items()):
        print(f"  {key:<12} {value}")

    sendgrid_key = seo_app._sendgrid_api_key_from_smtp_cfg(cfg)
    transport = "SendGrid HTTP API" if sendgrid_key else f"SMTP {cfg['host']}:{cfg['port']}"
    print(f"\n--- transport retenu: {transport} ---")
    if not sendgrid_key and str(cfg.get("host") or "").lower() == "smtp.sendgrid.net":
        # The HTTP API exists precisely because PaaS hosts block outbound SMTP ports; falling
        # back to SMTP on a SendGrid host is almost always a misconfigured username.
        print("  ATTENTION: hôte SendGrid mais l'API HTTP n'est pas retenue.")
        print("  SMTP_USERNAME doit valoir exactement 'apikey' et SMTP_PASSWORD être la clé.")

    print(f"\n--- envoi vers {to_addr} ---")
    try:
        seo_app._send_email(
            to_addr=to_addr,
            subject="Test Noyaru — mail_doctor",
            body="Si tu lis ceci, l'envoi fonctionne.",
        )
    except Exception:
        print("ÉCHEC. Trace complète:\n")
        traceback.print_exc()
        return 1

    print("ENVOI ACCEPTÉ par le transport.")
    print("Note: accepté n'est pas délivré. Vérifie la réception, et les spams.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
