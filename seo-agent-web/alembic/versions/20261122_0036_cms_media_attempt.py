"""Keep the latest media attempt separate from its retained approval receipts."""

import sqlalchemy as sa
from alembic import op

revision = "20261122_0036"
down_revision = "20261119_0035"
branch_labels = None
depends_on = None


def upgrade():
    columns = sa.inspect(op.get_bind()).get_columns("cms_media_transfers")
    if not any(column["name"] == "attempt_id" for column in columns):
        op.add_column("cms_media_transfers", sa.Column("attempt_id", sa.String(36), nullable=True))


def downgrade():
    transfers = sa.table("cms_media_transfers", sa.column("attempt_id", sa.String(36)))
    active = op.get_bind().scalar(sa.select(transfers.c.attempt_id).where(
        transfers.c.attempt_id.is_not(None)).limit(1))
    if active is not None:
        raise RuntimeError("Descente refusee : des identités de tentatives de transfert sont conservées.")
    op.drop_column("cms_media_transfers", "attempt_id")
