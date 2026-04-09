from sqlalchemy import (
    Column, Integer, String, Text, DateTime, Date, Numeric,
    ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from app.database import Base


class IngestRun(Base):
    __tablename__ = "ingest_runs"

    id = Column(Integer, primary_key=True)
    dataset = Column(String, nullable=False)
    started_at = Column(DateTime, nullable=False)
    completed_at = Column(DateTime, nullable=True)
    status = Column(String, nullable=False)  # running, success, failed
    run_mode = Column(String, nullable=False)  # latest, backfill
    start_period = Column(String, nullable=True)
    end_period = Column(String, nullable=True)
    row_count_raw = Column(Integer, nullable=False, default=0, server_default="0")
    row_count_skipped_raw = Column(Integer, nullable=False, default=0, server_default="0")
    row_count_normalized = Column(Integer, nullable=False, default=0, server_default="0")
    row_count_inserted = Column(Integer, nullable=False, default=0, server_default="0")
    row_count_updated = Column(Integer, nullable=False, default=0, server_default="0")
    quality_issue_count = Column(Integer, nullable=False, default=0, server_default="0")
    error_message = Column(Text, nullable=True)
    s3_archive_key = Column(String, nullable=True)


class RawRetailRow(Base):
    __tablename__ = "raw_retail_rows"

    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, ForeignKey("ingest_runs.id"), nullable=False)
    dataset = Column(String, nullable=False)
    period = Column(String, nullable=False)
    state_id = Column(String, nullable=False)
    sector_id = Column(String, nullable=False)
    source_hash = Column(String, nullable=False)
    row_json = Column(JSONB, nullable=False)
    created_at = Column(DateTime, nullable=False)

    __table_args__ = (
        UniqueConstraint("dataset", "source_hash", name="uq_raw_retail_rows_dataset_hash"),
        Index("ix_raw_retail_rows_run_id", "run_id"),
        Index("ix_raw_retail_rows_period_state_sector", "period", "state_id", "sector_id"),
    )


class RetailMetric(Base):
    __tablename__ = "retail_metrics"

    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, ForeignKey("ingest_runs.id"), nullable=False)
    raw_row_id = Column(Integer, ForeignKey("raw_retail_rows.id"), nullable=False)
    dataset = Column(String, nullable=False)
    period = Column(Date, nullable=False)
    state_id = Column(String, nullable=False)
    sector_id = Column(String, nullable=False)
    price_cents_per_kwh = Column(Numeric, nullable=True)
    sales_mwh = Column(Numeric, nullable=True)
    revenue_thousand_usd = Column(Numeric, nullable=True)
    customers_count = Column(Numeric, nullable=True)
    source_hash = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("dataset", "period", "state_id", "sector_id", name="uq_retail_metrics_key"),
        Index("ix_retail_metrics_period_state_sector", "period", "state_id", "sector_id"),
    )


class QualityIssue(Base):
    __tablename__ = "quality_issues"

    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, ForeignKey("ingest_runs.id"), nullable=False)
    raw_row_id = Column(Integer, ForeignKey("raw_retail_rows.id"), nullable=True)
    metric_id = Column(Integer, ForeignKey("retail_metrics.id"), nullable=True)
    issue_type = Column(String, nullable=False)
    severity = Column(String, nullable=False)  # warning, error
    issue_message = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False)

    __table_args__ = (
        Index("ix_quality_issues_run_id", "run_id"),
    )


class StateMonthSummary(Base):
    __tablename__ = "state_month_summary"

    period = Column(Date, primary_key=True)
    state_id = Column(String, primary_key=True)
    avg_price_cents_per_kwh = Column(Numeric, nullable=True)
    total_sales_mwh = Column(Numeric, nullable=True)
    total_revenue_thousand_usd = Column(Numeric, nullable=True)
    total_customers_count = Column(Numeric, nullable=True)
    refreshed_at = Column(DateTime, nullable=False)


class SectorMonthSummary(Base):
    __tablename__ = "sector_month_summary"

    period = Column(Date, primary_key=True)
    sector_id = Column(String, primary_key=True)
    avg_price_cents_per_kwh = Column(Numeric, nullable=True)
    total_sales_mwh = Column(Numeric, nullable=True)
    total_revenue_thousand_usd = Column(Numeric, nullable=True)
    total_customers_count = Column(Numeric, nullable=True)
    refreshed_at = Column(DateTime, nullable=False)
