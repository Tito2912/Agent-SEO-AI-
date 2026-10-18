"""Opt-in signup attribution from registered links; no visitor storage or third-party pixels."""
from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timezone
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

try:
    from .models import AccountActivation, AcquisitionCampaign
except ImportError:
    from models import AccountActivation, AcquisitionCampaign

log = logging.getLogger('seo_agent.attribution')
MEDIUMS = {'cpc': 'Publicité sur moteur', 'paid_social': 'Publicité sur réseau social',
           'social': 'Publication sur réseau social', 'email': 'Email', 'referral': 'Partenaire'}
KEYS = ('utm_source', 'utm_medium', 'utm_campaign')


def label(value, maximum):
    if not isinstance(value, str) or len(value) > maximum:
        return ''
    value = value.lower()
    return value if re.fullmatch(r'[a-z0-9][a-z0-9_-]*', value) else ''


def tags(query):
    if hasattr(query, 'getlist') and any(len(query.getlist(key)) != 1 for key in KEYS):
        return {}
    source, medium, name = (label(query.get(key, ''), maximum) for key, maximum in zip(KEYS, (40, 32, 64)))
    return dict(zip(KEYS, (source, medium, name))) if source and name and medium in MEDIUMS else {}


def carry(request, path):
    """Only the three bounded labels travel in public links, without browser storage."""
    if getattr(request.state, 'user', None):
        return path
    return append_tags(path, tags(request.query_params))


def append_tags(path, campaign_tags):
    parts = urlsplit(path)
    if not campaign_tags or parts.scheme or parts.netloc or not parts.path.startswith('/'):
        return path
    query = [(key, value) for key, value in parse_qsl(parts.query) if key not in KEYS]
    query.extend(campaign_tags.items())
    return urlunsplit(('', '', parts.path, urlencode(query), parts.fragment))


def campaign_tags(campaign):
    return dict(zip(KEYS, (campaign.source, campaign.medium, campaign.name))) if campaign else {}


def find(database, *, query=None, identifier=''):
    try:
        with database.session() as db:
            if identifier:
                if str(uuid.UUID(identifier)) != identifier:
                    return None
                return db.get(AcquisitionCampaign, identifier)
            values = tags(query or {})
            if not values:
                return None
            return db.scalar(select(AcquisitionCampaign).where(
                AcquisitionCampaign.source == values['utm_source'], AcquisitionCampaign.medium == values['utm_medium'],
                AcquisitionCampaign.name == values['utm_campaign']))
    except Exception as error:
        log.warning('Signup attribution unavailable (%s)', type(error).__name__)
        return None


def prepare_signup(user, campaign, consent):
    if campaign and consent == 'yes':
        user._acquisition_campaign_id = campaign.id
        user._acquisition_consented_at = datetime.now(timezone.utc)


def signup_fields(connection, user):
    identifier = getattr(user, '_acquisition_campaign_id', None)
    consented = getattr(user, '_acquisition_consented_at', None)
    if identifier and isinstance(consented, datetime) and connection.scalar(
            select(AcquisitionCampaign.id).where(AcquisitionCampaign.id == identifier)):
        return {'acquisition_campaign_id': identifier, 'acquisition_consented_at': datetime.now(timezone.utc)}
    return {}


def create(database, source, medium, name):
    source, medium, name = label(source, 40), label(medium, 32), label(name, 64)
    if not source or not name or medium not in MEDIUMS:
        raise ValueError('Utilisez des lettres sans accent, des chiffres, des tirets ou des underscores : source de 40 caractères maximum et campagne de 64 caractères maximum. Choisissez un support proposé.')
    with database.session() as db:
        row = db.scalar(select(AcquisitionCampaign).where(AcquisitionCampaign.source == source,
                        AcquisitionCampaign.medium == medium, AcquisitionCampaign.name == name))
        if row:
            return row
        row = AcquisitionCampaign(source=source, medium=medium, name=name)
        db.add(row)
        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            row = db.scalar(select(AcquisitionCampaign).where(AcquisitionCampaign.source == source,
                            AcquisitionCampaign.medium == medium, AcquisitionCampaign.name == name))
            if row is None:
                raise
        return row


def links(database, base_url):
    with database.session() as db:
        rows = db.scalars(select(AcquisitionCampaign).order_by(AcquisitionCampaign.created_at.desc(), AcquisitionCampaign.id)).all()
    return [{'source': row.source, 'medium': row.medium, 'name': row.name,
             'url': base_url.rstrip('/') + append_tags('/', campaign_tags(row))} for row in rows]


def own_status(database, user_id):
    try:
        with database.session() as db:
            row = db.get(AccountActivation, str(user_id))
            accepted = bool(row and row.acquisition_campaign_id and row.acquisition_consented_at)
        return {'available': True, 'accepted': accepted}
    except Exception:
        return {'available': False, 'accepted': False}


def withdraw(database, user_id):
    with database.session() as db:
        db.execute(update(AccountActivation).where(AccountActivation.user_id == str(user_id)).values(
            acquisition_campaign_id=None, acquisition_consented_at=None))
        db.commit()
