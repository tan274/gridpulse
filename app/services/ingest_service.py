import hashlib
import json
from datetime import datetime, timedelta

from app.config import settings
from app.models import IngestRun, QualityIssue, RawRetailRow, RetailMetric
from app.services import analytics, eia_client, normalizer, quality, s3_archive

DATASET = "electricity/retail-sales"


def compute_source_hash(row: dict) -> str:
    stable = json.dumps(row, sort_keys=True)
    return hashlib.sha256(stable.encode()).hexdigest()


def resolve_latest_period() -> str:
    """Return the most recent fully completed month as YYYY-MM string."""
    today = datetime.utcnow().date()
    first_of_current_month = today.replace(day=1)
    last_month = first_of_current_month - timedelta(days=1)
    return last_month.strftime("%Y-%m")


def resolve_period_range(
    mode: str,
    start_period: str | None,
    end_period: str | None,
) -> tuple[str, str]:
    if mode == "latest":
        period = resolve_latest_period()
        return period, period
    elif mode == "backfill":
        if not start_period or not end_period:
            raise ValueError("backfill mode requires start_period and end_period")
        return start_period, end_period
    else:
        raise ValueError(f"Invalid mode '{mode}'. Must be 'latest' or 'backfill'.")


def run_ingestion(
    db,
    mode: str = "backfill",
    start_period: str | None = None,
    end_period: str | None = None,
    state_ids: list[str] | None = None,
    sector_ids: list[str] | None = None,
) -> dict:
    """
    Orchestrate a full ingestion run.
    Resolve period range from mode -> create ingest_runs row -> fetch -> archive
    -> persist raw rows -> normalize -> persist canonical rows -> persist quality issues
    -> build quality report -> refresh summaries -> update run status.
    Return a summary dict.
    """
    effective_start, effective_end = resolve_period_range(mode, start_period, end_period)

    run = IngestRun(
        dataset=DATASET,
        started_at=datetime.utcnow(),
        status="running",
        run_mode=mode,
        start_period=effective_start,
        end_period=effective_end,
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    try:
        rows = eia_client.fetch_retail_sales(
            api_key=settings.eia_api_key,
            start_period=effective_start,
            end_period=effective_end,
            state_ids=state_ids,
            sector_ids=sector_ids,
        )
        archive_key = s3_archive.upload_run_archive(run.id, rows)
    except Exception as e:
        run.status = "failed"
        run.error_message = str(e)
        run.completed_at = datetime.utcnow()
        db.commit()
        return {
            "run_id": run.id,
            "status": "failed",
            "run_mode": run.run_mode,
            "dataset": run.dataset,
            "start_period": run.start_period,
            "end_period": run.end_period,
            "error_message": str(e),
        }

    run.s3_archive_key = archive_key
    db.commit()

    try:
        existing_hashes = {
            h for (h,) in db.query(RawRetailRow.source_hash)
            .filter(RawRetailRow.dataset == DATASET)
            .all()
        }

        row_count_raw = 0
        row_count_skipped_raw = 0
        now = datetime.utcnow()
        new_raw_rows = []

        for row in rows:
            source_hash = compute_source_hash(row)
            if source_hash in existing_hashes:
                row_count_skipped_raw += 1
                continue
            raw_row_obj = RawRetailRow(
                run_id=run.id,
                dataset=DATASET,
                period=row.get("period", ""),
                state_id=row.get("stateid", ""),
                sector_id=row.get("sectorid", ""),
                source_hash=source_hash,
                row_json=row,
                created_at=now,
            )
            db.add(raw_row_obj)
            new_raw_rows.append(raw_row_obj)
            existing_hashes.add(source_hash)
            row_count_raw += 1

        db.flush()

        pending_issues = []  # list of (issue_dict, raw_row_id or None)

        if not rows:
            pending_issues.append((
                {
                    "issue_type": "no_data_returned",
                    "severity": "warning",
                    "issue_message": "No rows returned by upstream API for the requested period range",
                },
                None,
            ))

        row_count_normalized = 0
        row_count_inserted = 0
        row_count_updated = 0
        normalized_dicts = []

        for raw_row in new_raw_rows:
            normalized, row_issues = normalizer.normalize_retail_row(raw_row.row_json)
            for issue in row_issues:
                pending_issues.append((issue, raw_row.id))
            if normalized is None:
                continue

            existing_metric = db.query(RetailMetric).filter(
                RetailMetric.dataset == DATASET,
                RetailMetric.period == normalized["period"],
                RetailMetric.state_id == normalized["state_id"],
                RetailMetric.sector_id == normalized["sector_id"],
            ).first()

            if existing_metric:
                existing_metric.price_cents_per_kwh = normalized["price_cents_per_kwh"]
                existing_metric.sales_mwh = normalized["sales_mwh"]
                existing_metric.revenue_thousand_usd = normalized["revenue_thousand_usd"]
                existing_metric.customers_count = normalized["customers_count"]
                existing_metric.source_hash = raw_row.source_hash
                existing_metric.run_id = run.id
                existing_metric.raw_row_id = raw_row.id
                row_count_updated += 1
            else:
                db.add(RetailMetric(
                    run_id=run.id,
                    raw_row_id=raw_row.id,
                    dataset=DATASET,
                    period=normalized["period"],
                    state_id=normalized["state_id"],
                    sector_id=normalized["sector_id"],
                    price_cents_per_kwh=normalized["price_cents_per_kwh"],
                    sales_mwh=normalized["sales_mwh"],
                    revenue_thousand_usd=normalized["revenue_thousand_usd"],
                    customers_count=normalized["customers_count"],
                    source_hash=raw_row.source_hash,
                    created_at=now,
                ))
                row_count_inserted += 1

            normalized_dicts.append(normalized)
            row_count_normalized += 1

        for issue in quality.detect_quality_issues(normalized_dicts):
            pending_issues.append((issue, None))

        for issue_dict, raw_row_id in pending_issues:
            db.add(QualityIssue(
                run_id=run.id,
                raw_row_id=raw_row_id,
                metric_id=None,
                issue_type=issue_dict["issue_type"],
                severity=issue_dict["severity"],
                issue_message=issue_dict["issue_message"],
                created_at=now,
            ))

        db.flush()

        run.row_count_raw = row_count_raw
        run.row_count_skipped_raw = row_count_skipped_raw
        run.row_count_normalized = row_count_normalized
        run.row_count_inserted = row_count_inserted
        run.row_count_updated = row_count_updated
        run.quality_issue_count = db.query(QualityIssue).filter(QualityIssue.run_id == run.id).count()

        analytics.refresh_state_month_summary(db)
        analytics.refresh_sector_month_summary(db)

        run.status = "success"
        run.completed_at = datetime.utcnow()
        db.commit()

    except Exception as e:
        db.rollback()
        run.status = "failed"
        run.error_message = str(e)
        run.completed_at = datetime.utcnow()
        db.commit()
        return {
            "run_id": run.id,
            "status": "failed",
            "run_mode": run.run_mode,
            "dataset": run.dataset,
            "start_period": run.start_period,
            "end_period": run.end_period,
            "error_message": str(e),
        }

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
    }
