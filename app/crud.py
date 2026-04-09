from datetime import date

from sqlalchemy import desc

from app.models import (
    IngestRun, QualityIssue, RetailMetric,
    SectorMonthSummary, StateMonthSummary,
)


def _parse_period(period_str: str | None) -> date | None:
    if period_str is None:
        return None
    year, month = period_str.split("-")
    return date(int(year), int(month), 1)


def _run_dict(run: IngestRun) -> dict:
    return {
        "run_id": run.id,
        "status": run.status,
        "run_mode": run.run_mode,
        "dataset": run.dataset,
        "start_period": run.start_period,
        "end_period": run.end_period,
        "row_count_raw": run.row_count_raw,
        "row_count_skipped_raw": run.row_count_skipped_raw,
        "row_count_normalized": run.row_count_normalized,
        "row_count_inserted": run.row_count_inserted,
        "row_count_updated": run.row_count_updated,
        "quality_issue_count": run.quality_issue_count,
        "s3_archive_key": run.s3_archive_key,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "completed_at": run.completed_at.isoformat() if run.completed_at else None,
        "error_message": run.error_message,
    }


def _metric_dict(m: RetailMetric) -> dict:
    return {
        "id": m.id,
        "dataset": m.dataset,
        "period": m.period.isoformat() if m.period else None,
        "state_id": m.state_id,
        "sector_id": m.sector_id,
        "price_cents_per_kwh": m.price_cents_per_kwh,
        "sales_mwh": m.sales_mwh,
        "revenue_thousand_usd": m.revenue_thousand_usd,
        "customers_count": m.customers_count,
        "source_hash": m.source_hash,
    }


def _issue_dict(i: QualityIssue) -> dict:
    return {
        "id": i.id,
        "run_id": i.run_id,
        "raw_row_id": i.raw_row_id,
        "issue_type": i.issue_type,
        "severity": i.severity,
        "issue_message": i.issue_message,
        "created_at": i.created_at.isoformat() if i.created_at else None,
    }


def get_ingest_run(db, run_id: int) -> dict | None:
    run = db.query(IngestRun).filter(IngestRun.id == run_id).first()
    return _run_dict(run) if run else None


def list_ingest_runs(db, limit: int = 20) -> list[dict]:
    runs = db.query(IngestRun).order_by(desc(IngestRun.id)).limit(limit).all()
    return [_run_dict(r) for r in runs]


def get_retail_metric(db, metric_id: int) -> dict | None:
    m = db.query(RetailMetric).filter(RetailMetric.id == metric_id).first()
    return _metric_dict(m) if m else None


def list_retail_metrics(
    db,
    state_id: str | None = None,
    sector_id: str | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    q = db.query(RetailMetric)
    if state_id:
        q = q.filter(RetailMetric.state_id == state_id)
    if sector_id:
        q = q.filter(RetailMetric.sector_id == sector_id)
    if start_period:
        q = q.filter(RetailMetric.period >= _parse_period(start_period))
    if end_period:
        q = q.filter(RetailMetric.period <= _parse_period(end_period))
    rows = q.order_by(RetailMetric.period, RetailMetric.state_id).offset(offset).limit(limit).all()
    return [_metric_dict(r) for r in rows]


def get_quality_issue(db, issue_id: int) -> dict | None:
    i = db.query(QualityIssue).filter(QualityIssue.id == issue_id).first()
    return _issue_dict(i) if i else None


def list_quality_issues(db, run_id: int | None = None, limit: int = 100) -> list[dict]:
    q = db.query(QualityIssue)
    if run_id is not None:
        q = q.filter(QualityIssue.run_id == run_id)
    rows = q.order_by(QualityIssue.id).limit(limit).all()
    return [_issue_dict(r) for r in rows]


def list_state_summary(
    db,
    period: str | None = None,
    state_id: str | None = None,
    limit: int = 100,
) -> list[dict]:
    q = db.query(StateMonthSummary)
    if period:
        q = q.filter(StateMonthSummary.period == _parse_period(period))
    if state_id:
        q = q.filter(StateMonthSummary.state_id == state_id)
    rows = q.order_by(StateMonthSummary.period, StateMonthSummary.state_id).limit(limit).all()
    return [
        {
            "period": r.period.isoformat(),
            "state_id": r.state_id,
            "avg_price_cents_per_kwh": r.avg_price_cents_per_kwh,
            "total_sales_mwh": r.total_sales_mwh,
            "total_revenue_thousand_usd": r.total_revenue_thousand_usd,
            "total_customers_count": r.total_customers_count,
            "refreshed_at": r.refreshed_at.isoformat() if r.refreshed_at else None,
        }
        for r in rows
    ]


def list_sector_summary(
    db,
    period: str | None = None,
    sector_id: str | None = None,
    limit: int = 100,
) -> list[dict]:
    q = db.query(SectorMonthSummary)
    if period:
        q = q.filter(SectorMonthSummary.period == _parse_period(period))
    if sector_id:
        q = q.filter(SectorMonthSummary.sector_id == sector_id)
    rows = q.order_by(SectorMonthSummary.period, SectorMonthSummary.sector_id).limit(limit).all()
    return [
        {
            "period": r.period.isoformat(),
            "sector_id": r.sector_id,
            "avg_price_cents_per_kwh": r.avg_price_cents_per_kwh,
            "total_sales_mwh": r.total_sales_mwh,
            "total_revenue_thousand_usd": r.total_revenue_thousand_usd,
            "total_customers_count": r.total_customers_count,
            "refreshed_at": r.refreshed_at.isoformat() if r.refreshed_at else None,
        }
        for r in rows
    ]


def get_top_states(db, period: str, metric: str, limit: int = 10) -> list[dict]:
    period_date = _parse_period(period)
    col = getattr(StateMonthSummary, metric)
    rows = (
        db.query(StateMonthSummary)
        .filter(StateMonthSummary.period == period_date, col.isnot(None))
        .order_by(col.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "period": r.period.isoformat(),
            "state_id": r.state_id,
            "metric_value": getattr(r, metric),
            "rank": i + 1,
        }
        for i, r in enumerate(rows)
    ]
