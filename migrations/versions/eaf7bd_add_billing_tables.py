"""add billing tables

Revision ID: 584f4cfb9c03
Revises: f066f1e37b59
Create Date: 2025-01-01 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "584f4cfb9c03"
down_revision = "f066f1e37b59"
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        'license_requests',
        sa.Column('id', sa.String(length=36), primary_key=True),
        sa.Column('user_name', sa.String(length=255), nullable=False),
        sa.Column('user_email', sa.String(length=255), nullable=False),
        sa.Column('receipt_filename', sa.String(length=512), nullable=False),
        sa.Column('amount', sa.Numeric(12, 2), nullable=False),
        sa.Column('message', sa.Text(), nullable=True),
        sa.Column('status', sa.String(length=20), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('verify_token', sa.String(length=36), nullable=True),
        sa.Column('verify_expires', sa.DateTime(timezone=True), nullable=True),
        sa.Column('admin_note', sa.Text(), nullable=True),
        sa.Column('admin_verified_by', sa.String(length=255), nullable=True),
    )
    op.create_index('ix_license_requests_verify_token', 'license_requests', ['verify_token'], unique=True)
    op.create_index('ix_license_requests_user_email', 'license_requests', ['user_email'])

    op.create_table(
        'license_keys',
        sa.Column('id', sa.String(length=36), primary_key=True),
        sa.Column('request_id', sa.String(length=36), sa.ForeignKey('license_requests.id', ondelete='SET NULL'), nullable=True),
        sa.Column('user_email', sa.String(length=255), nullable=False),
        sa.Column('license_key', sa.String(length=200), nullable=False),
        sa.Column('issued_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('expires_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('signature', sa.String(length=128), nullable=False),
        sa.Column('active', sa.Boolean(), nullable=False, server_default=sa.text('0')),
    )
    op.create_index('ix_license_keys_user_email', 'license_keys', ['user_email'])
    op.create_index('ux_license_keys_license_key', 'license_keys', ['license_key'], unique=True)


def downgrade():
    op.drop_index('ux_license_keys_license_key', table_name='license_keys')
    op.drop_index('ix_license_keys_user_email', table_name='license_keys')
    op.drop_table('license_keys')
    op.drop_index('ix_license_requests_user_email', table_name='license_requests')
    op.drop_index('ix_license_requests_verify_token', table_name='license_requests')
    op.drop_table('license_requests')
