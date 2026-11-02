"""Private operator console: inspect by default, scoped writes only with --apply."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select  # noqa: E402
from backend import app as m, recipe_access  # noqa: E402
from backend.models import ProjectRecipeAccess, User  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['inspect', 'grant', 'revoke'])
    parser.add_argument('--email', required=True)
    parser.add_argument('--site', default='')
    parser.add_argument('--hours', type=int, default=24)
    parser.add_argument('--reason', default='')
    parser.add_argument('--grant-id', default='')
    parser.add_argument('--confirm-email', default='')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    email = m._normalize_email(args.email)
    with m.DB.session() as db:
        target = db.scalar(select(User).where(User.email == email))
        if target is None or target.is_admin:
            raise SystemExit('Compte client introuvable.')
        row = db.get(ProjectRecipeAccess, target.id)
        result = {'user_id': target.id, 'email': email, 'paid_plan': m.billing.effective_plan_key(db, user_id=target.id),
                  'active': recipe_access.active(row), 'grant_id': row.grant_id if row else None,
                  'site': row.base_url if row else None,
                  'expires_at': recipe_access.utc(row.expires_at).isoformat() if row else None,
                  'applied': False, 'stripe_modified': False}
        if args.action == 'grant':
            site = recipe_access.canonical(args.site)
            if not 1 <= args.hours <= 48 or not args.reason.strip() or len(args.reason) > 320:
                raise SystemExit('Précisez le motif et une durée de 1 à 48 heures.')
            if m._validate_public_crawl_target(site):
                raise SystemExit('Le site doit être une origine publique HTTPS accessible.')
            result['requested'] = {'site': site, 'hours': args.hours, 'reason': args.reason}
        if args.apply:
            if args.action == 'inspect' or m._normalize_email(args.confirm_email) != email:
                raise SystemExit('Confirmez exactement le compte cible avec --confirm-email.')
            owner = m._system_settings_owner_email()
            actor = db.scalar(select(User).where(User.email == owner)) if owner else None
            if actor is None or not m._user_can_access_system_settings(actor):
                raise SystemExit('Propriétaire système configuré et présent requis.')
            if args.action == 'grant':
                row = recipe_access.grant(db, actor=actor, user_id=target.id, base_url=site,
                                          hours=args.hours, reason=args.reason, source='operator_console')
            else:
                row = recipe_access.revoke(db, actor=actor, user_id=target.id, grant_id=args.grant_id,
                                           source='operator_console')
            db.commit()
            result.update(applied=True, active=recipe_access.active(row), grant_id=row.grant_id, site=row.base_url,
                          expires_at=recipe_access.utc(row.expires_at).isoformat())
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
