"""CMS approval evidence survives deletion; copies and five-year purge scope."""
from datetime import datetime, timezone
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import NAMESPACE_URL, uuid5

from alembic import op
import sqlalchemy as sa

revision = "20261119_0034"
down_revision = "20261117_0033"
branch_labels = None
depends_on = None

TABLE = "cms_publication_consents"
NAMING = {"fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s"}
ADDED = {"owner_user_id": sa.String(36), "actor_email": sa.String(320),
         "site_url": sa.String(2048), "snapshot_source": sa.String(24),
         "expires_at": sa.DateTime(timezone=True)}


def _url(value):
    # Frozen migration helper: do not import a future mutable application module.
    try:
        value = str(value or "")
        p = urlsplit(value)
        if (p.scheme not in {"http", "https"} or not p.hostname or p.username or p.password
                or "\\" in value or any(ord(c) < 32 for c in value)):
            return None
        _ = p.port
        query = urlencode([(k, v) for k, v in parse_qsl(p.query)
                           if (k in {"id_cms", "id_lang", "p", "page_id"} and v.isdigit()) or (k == "controller" and v == "cms")])
        clean = urlunsplit((p.scheme, p.netloc, p.path, query, ""))
        return clean if len(clean) <= 2048 else None
    except ValueError:
        return None


def _destination(config):
    out = {k: str(config[k]) for k in ("blog_id", "collection_id", "id_shop", "id_lang", "id_cms_category")
           if config.get(k) is not None}
    mapping = config.get("field_mapping")
    if isinstance(mapping, dict):
        out["field_mapping"] = {k: str(mapping[k]) for k in ("title", "slug", "body", "excerpt") if k in mapping}
    return out


def _fk_name(fk):
    return fk["name"] or f"fk_{TABLE}_{fk['constrained_columns'][0]}_{fk['referred_table']}"


def _expiration(stamp):
    try:
        return stamp.replace(year=stamp.year + 5)
    except ValueError:
        return stamp.replace(year=stamp.year + 5, day=28)


def _backfill(bind):
    receipts = sa.Table(TABLE, sa.MetaData(), autoload_with=bind)
    projects = sa.table("projects", sa.column("id"), sa.column("owner_user_id"), sa.column("base_url"))
    users = sa.table("users", sa.column("id"), sa.column("email"))
    publications = sa.table("cms_publications", sa.column("id"), sa.column("remote_id"), sa.column("remote_url"))
    def context(project_id, actor_id):
        project = bind.execute(sa.select(projects).where(projects.c.id == project_id)).mappings().first()
        actor = bind.execute(sa.select(users).where(users.c.id == actor_id)).mappings().first()
        owner = (bind.execute(sa.select(users.c.id).where(users.c.id == project["owner_user_id"])).scalar()
                 if project else None)
        return {"owner_user_id": owner, "actor_email": actor["email"] if actor else None,
                "site_url": _url(project["base_url"]) if project else None}
    for receipt in bind.execute(sa.select(receipts).where(receipts.c.snapshot_source == "legacy")).mappings():
        copies = context(receipt["project_id"], receipt["actor_id"])
        raw = receipt["targets"] if isinstance(receipt["targets"], dict) else {}
        targets = {key: raw.get(key) for key in ("platform", "language", "slug", "remote_id") if key in raw}
        targets["remote_url"] = _url(raw.get("remote_url"))
        targets["destination"] = _destination(raw.get("destination") if isinstance(raw.get("destination"), dict) else {})
        current = bind.execute(sa.select(publications).where(
            publications.c.id == receipt["publication_id"])).mappings().first()
        if current and current["remote_id"]:
            targets["result"] = {"remote_id": current["remote_id"], "url": _url(current["remote_url"]),
                                 "source": "legacy_current_state", "observed_at": datetime.now(timezone.utc).isoformat()}
        bind.execute(receipts.update().where(receipts.c.id == receipt["id"]).values(
            **copies, targets=targets, snapshot_source="backfilled"))
    # Earlier media approvals lived only in ephemeral transfer rows. Preserve what is
    # actually known; their old digest describes an intent, not the raw asset bytes.
    media = sa.Table("cms_media_transfers", sa.MetaData(), autoload_with=bind)
    for transfer in bind.execute(sa.select(media).where(media.c.consent_digest.is_not(None))).mappings():
        if transfer["created_at"] is None:
            continue  # Do not fabricate an approval date missing from a legacy transfer.
        ident = str(uuid5(NAMESPACE_URL, "noyaru:cms-media:" + transfer["id"]))
        if bind.execute(sa.select(receipts.c.id).where(receipts.c.id == ident)).scalar():
            continue
        targets = {"platform": transfer["platform"], "media_key": transfer["media_key"],
                   "legacy_digest_kind": "intent"}
        if transfer["remote_id"] or transfer["remote_url"]:
            targets["result"] = {"remote_id": transfer["remote_id"], "url": _url(transfer["remote_url"]),
                                 "source": "legacy_current_state", "observed_at": datetime.now(timezone.utc).isoformat()}
        bind.execute(receipts.insert().values(id=ident, project_id=transfer["project_id"],
            publication_id=None, actor_id=transfer["approved_by"],
            **context(transfer["project_id"], transfer["approved_by"]),
            snapshot_source="backfilled", operation="media_upload", content_digest=transfer["consent_digest"],
            targets=targets, approved_at=transfer["created_at"], expires_at=_expiration(transfer["created_at"])))
    for row in bind.execute(sa.select(receipts.c.id, receipts.c.approved_at).where(receipts.c.expires_at.is_(None))):
        bind.execute(receipts.update().where(receipts.c.id == row.id).values(expires_at=_expiration(row.approved_at)))


def upgrade():
    bind = op.get_bind()
    schema = sa.inspect(bind)
    columns = {c["name"]: c for c in schema.get_columns(TABLE)}
    fks = schema.get_foreign_keys(TABLE)
    with op.batch_alter_table(TABLE, naming_convention=NAMING) as batch:
        for name, kind in ADDED.items():
            if name not in columns:
                batch.add_column(sa.Column(name, kind, nullable=name != "snapshot_source",
                    server_default="legacy" if name == "snapshot_source" else None))
        if not any(fk["constrained_columns"] == ["owner_user_id"] for fk in fks):
            batch.create_foreign_key(f"fk_{TABLE}_owner_user_id_users", "users", ["owner_user_id"], ["id"], ondelete="SET NULL")
        for name, parent in (("project_id", "projects"), ("publication_id", "cms_publications")):
            if not columns[name]["nullable"]:
                batch.alter_column(name, existing_type=sa.String(36), nullable=True)
            current = [fk for fk in fks if fk["constrained_columns"] == [name]]
            if len(current) != 1 or current[0].get("options", {}).get("ondelete", "").upper() != "SET NULL":
                for fk in current:
                    batch.drop_constraint(_fk_name(fk), type_="foreignkey")
                batch.create_foreign_key(f"fk_{TABLE}_{name}_{parent}", parent, [name], ["id"], ondelete="SET NULL")
    indexes = {index["name"] for index in sa.inspect(bind).get_indexes(TABLE)}
    for name in ("owner_user_id", "expires_at"):
        index = f"ix_{TABLE}_{name}"
        if index not in indexes:
            op.create_index(index, TABLE, [name])
    _backfill(bind)


def downgrade():
    bind = op.get_bind()
    table = sa.table(TABLE, sa.column("id"))
    if bind.execute(sa.select(sa.func.count()).select_from(table)).scalar():
        raise RuntimeError("Descente refusee : elle retirerait des preuves CMS conservees. Aucune preuve n'a ete effacee.")
    schema = sa.inspect(bind)
    fks = schema.get_foreign_keys(TABLE)
    indexes = {index["name"] for index in schema.get_indexes(TABLE)}
    for name in ("owner_user_id", "expires_at"):
        if f"ix_{TABLE}_{name}" in indexes:
            op.drop_index(f"ix_{TABLE}_{name}", table_name=TABLE)
    with op.batch_alter_table(TABLE, naming_convention=NAMING) as batch:
        for fk in fks:
            if fk["constrained_columns"] in (["project_id"], ["publication_id"], ["owner_user_id"]):
                batch.drop_constraint(_fk_name(fk), type_="foreignkey")
        for name, parent in (("project_id", "projects"), ("publication_id", "cms_publications")):
            batch.alter_column(name, existing_type=sa.String(36), nullable=False)
            batch.create_foreign_key(f"fk_{TABLE}_{name}_{parent}", parent, [name], ["id"], ondelete="CASCADE")
        for name in reversed(ADDED):
            batch.drop_column(name)
