"""Scope student admission numbers to a school.

Revision ID: 3c6d2a1e5b47
Revises: f066f1e37b59
Create Date: 2025-11-16 20:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


def _drop_index_if_exists(conn, table_name: str, index_name: str) -> None:
    sql = sa.text(
        "SELECT COUNT(*) AS cnt FROM information_schema.statistics "
        "WHERE table_schema=DATABASE() AND table_name=:table AND index_name=:idx"
    )
    result = conn.execute(sql, {"table": table_name, "idx": index_name}).scalar()
    if result and int(result) > 0:
        conn.execute(sa.text(f"ALTER TABLE `{table_name}` DROP INDEX `{index_name}`"))


# revision identifiers, used by Alembic.
revision = "3c6d2a1e5b47"
down_revision = "f066f1e37b59"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    for idx in ("uq_students_admission_no", "admission_no"):
        _drop_index_if_exists(bind, "students", idx)
    op.create_unique_constraint(
        "uq_students_school_admission",
        "students",
        ["school_id", "admission_no"],
    )


def downgrade():
    op.drop_constraint("uq_students_school_admission", "students", type_="unique")
    op.create_unique_constraint(
        "uq_students_admission_no",
        "students",
        ["admission_no"],
    )
