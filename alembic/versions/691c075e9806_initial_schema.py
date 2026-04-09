"""initial schema

Revision ID: 691c075e9806
Revises:
Create Date: 2026-04-02

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "691c075e9806"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "ingest_runs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("dataset", sa.String(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("run_mode", sa.String(), nullable=False),
        sa.Column("start_period", sa.String(), nullable=True),
        sa.Column("end_period", sa.String(), nullable=True),
        sa.Column("row_count_raw", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("row_count_skipped_raw", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("row_count_normalized", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("row_count_inserted", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("row_count_updated", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("quality_issue_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("s3_archive_key", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "raw_retail_rows",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("dataset", sa.String(), nullable=False),
        sa.Column("period", sa.String(), nullable=False),
        sa.Column("state_id", sa.String(), nullable=False),
        sa.Column("sector_id", sa.String(), nullable=False),
        sa.Column("source_hash", sa.String(), nullable=False),
        sa.Column("row_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["ingest_runs.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("dataset", "source_hash", name="uq_raw_retail_rows_dataset_hash"),
    )
    op.create_index("ix_raw_retail_rows_run_id", "raw_retail_rows", ["run_id"])
    op.create_index(
        "ix_raw_retail_rows_period_state_sector",
        "raw_retail_rows",
        ["period", "state_id", "sector_id"],
    )

    op.create_table(
        "retail_metrics",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("raw_row_id", sa.Integer(), nullable=False),
        sa.Column("dataset", sa.String(), nullable=False),
        sa.Column("period", sa.Date(), nullable=False),
        sa.Column("state_id", sa.String(), nullable=False),
        sa.Column("sector_id", sa.String(), nullable=False),
        sa.Column("price_cents_per_kwh", sa.Numeric(), nullable=True),
        sa.Column("sales_mwh", sa.Numeric(), nullable=True),
        sa.Column("revenue_thousand_usd", sa.Numeric(), nullable=True),
        sa.Column("customers_count", sa.Numeric(), nullable=True),
        sa.Column("source_hash", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["raw_row_id"], ["raw_retail_rows.id"]),
        sa.ForeignKeyConstraint(["run_id"], ["ingest_runs.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("dataset", "period", "state_id", "sector_id", name="uq_retail_metrics_key"),
    )
    op.create_index(
        "ix_retail_metrics_period_state_sector",
        "retail_metrics",
        ["period", "state_id", "sector_id"],
    )

    op.create_table(
        "quality_issues",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("raw_row_id", sa.Integer(), nullable=True),
        sa.Column("metric_id", sa.Integer(), nullable=True),
        sa.Column("issue_type", sa.String(), nullable=False),
        sa.Column("severity", sa.String(), nullable=False),
        sa.Column("issue_message", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["metric_id"], ["retail_metrics.id"]),
        sa.ForeignKeyConstraint(["raw_row_id"], ["raw_retail_rows.id"]),
        sa.ForeignKeyConstraint(["run_id"], ["ingest_runs.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_quality_issues_run_id", "quality_issues", ["run_id"])

    op.create_table(
        "state_month_summary",
        sa.Column("period", sa.Date(), nullable=False),
        sa.Column("state_id", sa.String(), nullable=False),
        sa.Column("avg_price_cents_per_kwh", sa.Numeric(), nullable=True),
        sa.Column("total_sales_mwh", sa.Numeric(), nullable=True),
        sa.Column("total_revenue_thousand_usd", sa.Numeric(), nullable=True),
        sa.Column("total_customers_count", sa.Numeric(), nullable=True),
        sa.Column("refreshed_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("period", "state_id"),
    )

    op.create_table(
        "sector_month_summary",
        sa.Column("period", sa.Date(), nullable=False),
        sa.Column("sector_id", sa.String(), nullable=False),
        sa.Column("avg_price_cents_per_kwh", sa.Numeric(), nullable=True),
        sa.Column("total_sales_mwh", sa.Numeric(), nullable=True),
        sa.Column("total_revenue_thousand_usd", sa.Numeric(), nullable=True),
        sa.Column("total_customers_count", sa.Numeric(), nullable=True),
        sa.Column("refreshed_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("period", "sector_id"),
    )


def downgrade() -> None:
    op.drop_table("sector_month_summary")
    op.drop_table("state_month_summary")
    op.drop_index("ix_quality_issues_run_id", table_name="quality_issues")
    op.drop_table("quality_issues")
    op.drop_index("ix_retail_metrics_period_state_sector", table_name="retail_metrics")
    op.drop_table("retail_metrics")
    op.drop_index("ix_raw_retail_rows_period_state_sector", table_name="raw_retail_rows")
    op.drop_index("ix_raw_retail_rows_run_id", table_name="raw_retail_rows")
    op.drop_table("raw_retail_rows")
    op.drop_table("ingest_runs")
