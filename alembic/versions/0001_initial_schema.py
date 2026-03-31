"""initial_schema

Revision ID: 0001
Revises:
Create Date: 2026-03-30

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # firms
    # ------------------------------------------------------------------
    op.create_table(
        "firms",
        sa.Column("crd_number", sa.Integer(), nullable=False),
        sa.Column("sec_number", sa.Text(), nullable=True),
        sa.Column("legal_name", sa.Text(), nullable=False),
        sa.Column("business_name", sa.Text(), nullable=True),
        sa.Column("registration_status", sa.Text(), nullable=True),
        sa.Column("firm_type", sa.Text(), nullable=True),
        sa.Column("aum_total", sa.BigInteger(), nullable=True),
        sa.Column("aum_discretionary", sa.BigInteger(), nullable=True),
        sa.Column("aum_non_discretionary", sa.BigInteger(), nullable=True),
        sa.Column("num_accounts", sa.Integer(), nullable=True),
        sa.Column("num_employees", sa.Integer(), nullable=True),
        sa.Column("main_street1", sa.Text(), nullable=True),
        sa.Column("main_street2", sa.Text(), nullable=True),
        sa.Column("main_city", sa.Text(), nullable=True),
        sa.Column("main_state", sa.Text(), nullable=True),
        sa.Column("main_zip", sa.Text(), nullable=True),
        sa.Column("main_country", sa.Text(), nullable=True),
        sa.Column("phone", sa.Text(), nullable=True),
        sa.Column("website", sa.Text(), nullable=True),
        sa.Column("fiscal_year_end", sa.Text(), nullable=True),
        sa.Column("org_type", sa.Text(), nullable=True),
        sa.Column("raw_adv", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("last_filing_date", sa.Date(), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=True,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("crd_number"),
    )
    op.create_index(
        "idx_firms_legal_name",
        "firms",
        [sa.text("to_tsvector('english', legal_name)")],
        postgresql_using="gin",
    )
    op.create_index(
        "idx_firms_business_name",
        "firms",
        [sa.text("to_tsvector('english', coalesce(business_name, ''))")],
        postgresql_using="gin",
    )
    op.create_index("idx_firms_state", "firms", ["main_state"])
    op.create_index("idx_firms_status", "firms", ["registration_status"])

    # ------------------------------------------------------------------
    # firm_aum_history
    # ------------------------------------------------------------------
    op.create_table(
        "firm_aum_history",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("crd_number", sa.Integer(), nullable=False),
        sa.Column("filing_date", sa.Date(), nullable=False),
        sa.Column("aum_total", sa.BigInteger(), nullable=True),
        sa.Column("aum_discretionary", sa.BigInteger(), nullable=True),
        sa.Column("aum_non_discretionary", sa.BigInteger(), nullable=True),
        sa.Column("num_accounts", sa.Integer(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(["crd_number"], ["firms.crd_number"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("crd_number", "filing_date", "source", name="uq_aum_history"),
    )
    op.create_index("idx_aum_history_crd", "firm_aum_history", ["crd_number"])
    op.create_index("idx_aum_history_date", "firm_aum_history", ["filing_date"])

    # ------------------------------------------------------------------
    # firm_snapshots
    # ------------------------------------------------------------------
    op.create_table(
        "firm_snapshots",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("crd_number", sa.Integer(), nullable=False),
        sa.Column("snapshot_hash", sa.Text(), nullable=False),
        sa.Column("raw_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "synced_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["crd_number"], ["firms.crd_number"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "idx_snapshots_crd", "firm_snapshots", ["crd_number", "synced_at"]
    )

    # ------------------------------------------------------------------
    # firm_changes
    # ------------------------------------------------------------------
    op.create_table(
        "firm_changes",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("crd_number", sa.Integer(), nullable=False),
        sa.Column("field_path", sa.Text(), nullable=False),
        sa.Column("old_value", sa.Text(), nullable=True),
        sa.Column("new_value", sa.Text(), nullable=True),
        sa.Column(
            "detected_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.Column("snapshot_from", sa.BigInteger(), nullable=True),
        sa.Column("snapshot_to", sa.BigInteger(), nullable=True),
        sa.ForeignKeyConstraint(["crd_number"], ["firms.crd_number"]),
        sa.ForeignKeyConstraint(["snapshot_from"], ["firm_snapshots.id"]),
        sa.ForeignKeyConstraint(["snapshot_to"], ["firm_snapshots.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "idx_changes_crd", "firm_changes", ["crd_number", "detected_at"]
    )
    op.create_index("idx_changes_field", "firm_changes", ["field_path"])

    # ------------------------------------------------------------------
    # adv_brochures
    # ------------------------------------------------------------------
    op.create_table(
        "adv_brochures",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("crd_number", sa.Integer(), nullable=False),
        sa.Column("brochure_version_id", sa.Integer(), nullable=False),
        sa.Column("brochure_name", sa.Text(), nullable=True),
        sa.Column("date_submitted", sa.Date(), nullable=True),
        sa.Column("source_month", sa.Text(), nullable=True),
        sa.Column("file_path", sa.Text(), nullable=False),
        sa.Column("file_size_bytes", sa.Integer(), nullable=True),
        sa.Column(
            "downloaded_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(["crd_number"], ["firms.crd_number"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("brochure_version_id"),
    )
    op.create_index(
        "idx_brochures_crd", "adv_brochures", ["crd_number", "date_submitted"]
    )

    # ------------------------------------------------------------------
    # platform_definitions
    # ------------------------------------------------------------------
    op.create_table(
        "platform_definitions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )

    # ------------------------------------------------------------------
    # firm_platforms
    # ------------------------------------------------------------------
    op.create_table(
        "firm_platforms",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("crd_number", sa.Integer(), nullable=False),
        sa.Column("platform_id", sa.Integer(), nullable=False),
        sa.Column(
            "tagged_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=True,
        ),
        sa.Column("tagged_by", sa.Text(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["crd_number"], ["firms.crd_number"]),
        sa.ForeignKeyConstraint(["platform_id"], ["platform_definitions.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("crd_number", "platform_id", name="uq_firm_platform"),
    )
    op.create_index("idx_firm_platforms_crd", "firm_platforms", ["crd_number"])
    op.create_index("idx_firm_platforms_platform", "firm_platforms", ["platform_id"])

    # ------------------------------------------------------------------
    # custom_property_definitions
    # ------------------------------------------------------------------
    op.create_table(
        "custom_property_definitions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=False),
        sa.Column("field_type", sa.Text(), nullable=False),
        sa.Column("options", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )

    # ------------------------------------------------------------------
    # firm_custom_properties
    # ------------------------------------------------------------------
    op.create_table(
        "firm_custom_properties",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("crd_number", sa.Integer(), nullable=False),
        sa.Column("definition_id", sa.Integer(), nullable=False),
        sa.Column("value", sa.Text(), nullable=True),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(["crd_number"], ["firms.crd_number"]),
        sa.ForeignKeyConstraint(
            ["definition_id"], ["custom_property_definitions.id"]
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "crd_number", "definition_id", name="uq_firm_custom_property"
        ),
    )

    # ------------------------------------------------------------------
    # alert_rules
    # ------------------------------------------------------------------
    op.create_table(
        "alert_rules",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("rule_type", sa.Text(), nullable=False),
        sa.Column(
            "platform_ids",
            postgresql.ARRAY(sa.Integer()),
            nullable=True,
        ),
        sa.Column(
            "crd_numbers",
            postgresql.ARRAY(sa.Integer()),
            nullable=True,
        ),
        sa.Column("threshold_pct", sa.Numeric(5, 2), nullable=True),
        sa.Column("field_path", sa.Text(), nullable=True),
        sa.Column(
            "delivery", sa.Text(), nullable=False, server_default="in_app"
        ),
        sa.Column("delivery_target", sa.Text(), nullable=True),
        sa.Column(
            "active", sa.Boolean(), nullable=False, server_default=sa.text("true")
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    # ------------------------------------------------------------------
    # alert_events
    # ------------------------------------------------------------------
    op.create_table(
        "alert_events",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("rule_id", sa.Integer(), nullable=False),
        sa.Column("crd_number", sa.Integer(), nullable=False),
        sa.Column("firm_name", sa.Text(), nullable=True),
        sa.Column("rule_type", sa.Text(), nullable=False),
        sa.Column("field_path", sa.Text(), nullable=True),
        sa.Column("old_value", sa.Text(), nullable=True),
        sa.Column("new_value", sa.Text(), nullable=True),
        sa.Column("platform_name", sa.Text(), nullable=True),
        sa.Column(
            "fired_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.Column("delivered_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "delivery_status", sa.Text(), nullable=True, server_default="pending"
        ),
        sa.ForeignKeyConstraint(["rule_id"], ["alert_rules.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "idx_alert_events_rule", "alert_events", ["rule_id", "fired_at"]
    )
    op.create_index(
        "idx_alert_events_crd", "alert_events", ["crd_number", "fired_at"]
    )

    # ------------------------------------------------------------------
    # sync_jobs
    # ------------------------------------------------------------------
    op.create_table(
        "sync_jobs",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("job_type", sa.Text(), nullable=False),
        sa.Column(
            "status", sa.Text(), nullable=False, server_default="pending"
        ),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("firms_processed", sa.Integer(), nullable=True, server_default="0"),
        sa.Column("firms_updated", sa.Integer(), nullable=True, server_default="0"),
        sa.Column(
            "changes_detected", sa.Integer(), nullable=True, server_default="0"
        ),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("completed_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    # ------------------------------------------------------------------
    # export_jobs
    # ------------------------------------------------------------------
    op.create_table(
        "export_jobs",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("format", sa.Text(), nullable=False),
        sa.Column(
            "filter_criteria",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "crd_list", postgresql.ARRAY(sa.Integer()), nullable=True
        ),
        sa.Column(
            "field_selection",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "status", sa.Text(), nullable=False, server_default="pending"
        ),
        sa.Column("file_path", sa.Text(), nullable=True),
        sa.Column("row_count", sa.Integer(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=True,
        ),
        sa.Column("completed_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("expires_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    # ------------------------------------------------------------------
    # api_keys
    # ------------------------------------------------------------------
    op.create_table(
        "api_keys",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("key_hash", sa.Text(), nullable=False),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column(
            "active", sa.Boolean(), nullable=False, server_default=sa.text("true")
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=True,
        ),
        sa.Column("last_used_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("key_hash"),
    )


def downgrade() -> None:
    op.drop_table("api_keys")
    op.drop_table("export_jobs")
    op.drop_table("sync_jobs")
    op.drop_index("idx_alert_events_crd", table_name="alert_events")
    op.drop_index("idx_alert_events_rule", table_name="alert_events")
    op.drop_table("alert_events")
    op.drop_table("alert_rules")
    op.drop_table("firm_custom_properties")
    op.drop_table("custom_property_definitions")
    op.drop_index("idx_firm_platforms_platform", table_name="firm_platforms")
    op.drop_index("idx_firm_platforms_crd", table_name="firm_platforms")
    op.drop_table("firm_platforms")
    op.drop_table("platform_definitions")
    op.drop_index("idx_brochures_crd", table_name="adv_brochures")
    op.drop_table("adv_brochures")
    op.drop_index("idx_changes_field", table_name="firm_changes")
    op.drop_index("idx_changes_crd", table_name="firm_changes")
    op.drop_table("firm_changes")
    op.drop_index("idx_snapshots_crd", table_name="firm_snapshots")
    op.drop_table("firm_snapshots")
    op.drop_index("idx_aum_history_date", table_name="firm_aum_history")
    op.drop_index("idx_aum_history_crd", table_name="firm_aum_history")
    op.drop_table("firm_aum_history")
    op.drop_index("idx_firms_status", table_name="firms")
    op.drop_index("idx_firms_state", table_name="firms")
    op.drop_index("idx_firms_business_name", table_name="firms")
    op.drop_index("idx_firms_legal_name", table_name="firms")
    op.drop_table("firms")
