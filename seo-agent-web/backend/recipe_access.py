"""An expiring permission for one demonstration origin, separate from paid plans."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from sqlalchemy import select, update

try:
    from .models import AuditLog, Project, ProjectRecipeAccess, User
except ImportError:
    from models import AuditLog, Project, ProjectRecipeAccess, User


def utc(value):
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


def canonical(value):
    """A single HTTPS origin: no path, wildcard, credentials or nonstandard port."""
    try:
        url = urlsplit(str(value or '').strip())
        if (len(str(value or '')) > 2048 or url.scheme.lower() != 'https' or not url.hostname or url.username is not None
                or url.password is not None or url.port not in (None, 443)
                or url.path not in ('', '/') or url.query or url.fragment
                or any(c.isspace() for c in value) or '*' in url.hostname):
            raise ValueError()
        return 'https://' + url.hostname.lower() + '/'
    except (ValueError, TypeError):
        raise ValueError('Renseignez l’origine HTTPS du site de démonstration, sans chemin ni identifiant.') from None


def active(row, now=None):
    return bool(row and row.revoked_at is None and utc(row.expires_at) > utc(now or datetime.now(timezone.utc)))


def permits(db, user_id, base_url, *, now=None):
    row = db.get(ProjectRecipeAccess, str(user_id))
    if not active(row, now):
        return False
    try:
        return canonical(base_url) == row.base_url
    except ValueError:
        return False


def offer(db, user_id):
    """A pending addition, for the real form; already-created projects need no offer."""
    row = db.get(ProjectRecipeAccess, str(user_id))
    if not active(row):
        return None
    for base in db.scalars(select(Project.base_url).where(Project.owner_user_id == str(user_id))):
        try:
            if canonical(base) == row.base_url:
                return None
        except ValueError:
            continue
    return {'base_url': row.base_url,
            'expires_label': utc(row.expires_at).astimezone(ZoneInfo('Europe/Paris')).strftime('%Y-%m-%d %H:%M')}


def _owner(actor):
    if __package__:
        from . import app
    else:
        import app
    if not app._user_can_access_system_settings(actor):
        raise PermissionError('system_owner_required')


def _audit(db, actor, user_id, action, row, source):
    db.add(AuditLog(actor_user_id=actor.id, actor_email=actor.email, action=action, status='ok',
                    target_type='user', target_id=str(user_id), meta={
                        'grant_id': row.grant_id, 'base_url': row.base_url,
                        'expires_at': utc(row.expires_at).isoformat(), 'reason': row.reason,
                        'scope': 'one_demo_project', 'stripe_modified': False, 'source': source}))


def grant(db, *, actor, user_id, base_url, hours, reason, now=None, source='web'):
    """The caller commits the permission and its audit together."""
    _owner(actor)
    base_url = canonical(base_url)
    reason = ' '.join(str(reason or '').split())
    if type(hours) is not int or not 1 <= hours <= 48:
        raise ValueError('Choisissez une durée de 1 à 48 heures.')
    if not reason or len(reason) > 320 or '\0' in reason:
        raise ValueError('Précisez le motif de recette en 320 caractères maximum.')
    target = db.get(User, str(user_id))
    if target is None or target.is_admin:
        raise ValueError('Compte client introuvable dans ce périmètre.')
    now = now or datetime.now(timezone.utc)
    row = db.get(ProjectRecipeAccess, str(user_id))
    if row is None:
        row = ProjectRecipeAccess(user_id=str(user_id))
        db.add(row)
    row.grant_id = str(uuid.uuid4())
    row.base_url, row.reason = base_url, reason
    row.granted_by_user_id, row.granted_at = actor.id, now
    row.expires_at, row.revoked_at = now + timedelta(hours=hours), None
    _audit(db, actor, user_id, 'recipe_access.grant', row, source)
    db.flush()
    return row


def revoke(db, *, actor, user_id, grant_id, now=None, source='web'):
    _owner(actor)
    row = db.get(ProjectRecipeAccess, str(user_id))
    if row is None or row.grant_id != grant_id:
        raise ValueError('Cet accès a changé. Rechargez la page avant de le retirer.')
    if row.revoked_at is None:
        changed = db.execute(update(ProjectRecipeAccess).where(
            ProjectRecipeAccess.user_id == str(user_id), ProjectRecipeAccess.grant_id == grant_id,
            ProjectRecipeAccess.revoked_at.is_(None)).values(revoked_at=now or datetime.now(timezone.utc)),
            execution_options={'synchronize_session': False})
        if changed.rowcount != 1:
            raise ValueError('Cet accès a changé. Rechargez la page avant de le retirer.')
        db.refresh(row)
        _audit(db, actor, user_id, 'recipe_access.revoke', row, source)
        db.flush()
    return row
