from datetime import datetime, date
import pytest
from sqlalchemy.exc import IntegrityError
from app.models import (
    IngestRun,
    RawRetailRow,
    RetailMetric,
    QualityIssue,
    StateMonthSummary,
    SectorMonthSummary,
)


def make_run(db, status="success", run_mode="backfill"):
    run = IngestRun(
        dataset="electricity/retail-sales",
        started_at=datetime(2024, 1, 1),
        status=status,
        run_mode=run_mode,
        start_period="2024-01",
        end_period="2024-01",
    )
    db.add(run)
    db.flush()
    return run


def make_raw_row(db, run_id, source_hash="abc123"):
    row = RawRetailRow(
        run_id=run_id,
        dataset="electricity/retail-sales",
        period="2024-01",
        state_id="CA",
        sector_id="RES",
        source_hash=source_hash,
        row_json={"period": "2024-01", "stateid": "CA"},
        created_at=datetime(2024, 1, 1),
    )
    db.add(row)
    db.flush()
    return row


def make_metric(db, run_id, raw_row_id, period=date(2024, 1, 1), state_id="CA", sector_id="RES"):
    metric = RetailMetric(
        run_id=run_id,
        raw_row_id=raw_row_id,
        dataset="electricity/retail-sales",
        period=period,
        state_id=state_id,
        sector_id=sector_id,
        price_cents_per_kwh=31.52,
        source_hash="abc123",
        created_at=datetime(2024, 1, 1),
    )
    db.add(metric)
    db.flush()
    return metric


def test_ingest_run_insert(db):
    run = make_run(db)
    db.commit()
    assert run.id is not None
    assert run.dataset == "electricity/retail-sales"
    assert run.status == "success"


def test_raw_retail_row_insert(db):
    run = make_run(db)
    row = make_raw_row(db, run.id)
    db.commit()
    assert row.id is not None
    assert row.run_id == run.id
    assert row.row_json["stateid"] == "CA"


def test_raw_retail_row_unique_constraint(db):
    run = make_run(db)
    make_raw_row(db, run.id, source_hash="dup_hash")
    db.flush()
    dup = RawRetailRow(
        run_id=run.id,
        dataset="electricity/retail-sales",
        period="2024-01",
        state_id="TX",
        sector_id="RES",
        source_hash="dup_hash",
        row_json={},
        created_at=datetime(2024, 1, 1),
    )
    db.add(dup)
    with pytest.raises(IntegrityError):
        db.flush()


def test_retail_metric_insert(db):
    run = make_run(db)
    row = make_raw_row(db, run.id)
    metric = make_metric(db, run.id, row.id)
    db.commit()
    assert metric.id is not None
    assert metric.period == date(2024, 1, 1)


def test_retail_metric_unique_constraint(db):
    run = make_run(db)
    row = make_raw_row(db, run.id, source_hash="h1")
    row2 = make_raw_row(db, run.id, source_hash="h2")
    make_metric(db, run.id, row.id)
    db.flush()
    dup = RetailMetric(
        run_id=run.id,
        raw_row_id=row2.id,
        dataset="electricity/retail-sales",
        period=date(2024, 1, 1),
        state_id="CA",
        sector_id="RES",
        source_hash="h2",
        created_at=datetime(2024, 1, 1),
    )
    db.add(dup)
    with pytest.raises(IntegrityError):
        db.flush()


def test_quality_issue_nullable_foreign_keys(db):
    run = make_run(db)
    issue = QualityIssue(
        run_id=run.id,
        raw_row_id=None,
        metric_id=None,
        issue_type="missing_field",
        severity="warning",
        issue_message="period is missing",
        created_at=datetime(2024, 1, 1),
    )
    db.add(issue)
    db.commit()
    assert issue.id is not None
    assert issue.raw_row_id is None
    assert issue.metric_id is None


def test_state_month_summary_primary_key(db):
    s = StateMonthSummary(
        period=date(2024, 1, 1),
        state_id="CA",
        avg_price_cents_per_kwh=31.5,
        total_sales_mwh=10000,
        total_revenue_thousand_usd=500000,
        total_customers_count=5000000,
        refreshed_at=datetime(2024, 1, 2),
    )
    db.add(s)
    db.commit()
    assert s.period == date(2024, 1, 1)
    assert s.state_id == "CA"


def test_state_month_summary_pk_uniqueness(db):
    db.add(StateMonthSummary(
        period=date(2024, 1, 1),
        state_id="TX",
        refreshed_at=datetime(2024, 1, 2),
    ))
    db.flush()
    db.add(StateMonthSummary(
        period=date(2024, 1, 1),
        state_id="TX",
        refreshed_at=datetime(2024, 1, 2),
    ))
    with pytest.raises(IntegrityError):
        db.flush()


def test_sector_month_summary_primary_key(db):
    s = SectorMonthSummary(
        period=date(2024, 1, 1),
        sector_id="RES",
        avg_price_cents_per_kwh=29.0,
        refreshed_at=datetime(2024, 1, 2),
    )
    db.add(s)
    db.commit()
    assert s.sector_id == "RES"
