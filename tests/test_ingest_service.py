import json
import pytest
from datetime import date
from decimal import Decimal
from pathlib import Path

from app.models import IngestRun, QualityIssue, RawRetailRow, RetailMetric, StateMonthSummary, SectorMonthSummary
from app.services.ingest_service import run_ingestion, resolve_latest_period, resolve_period_range

FIXTURES = Path(__file__).parent / "fixtures"

SAMPLE_ROWS = json.loads((FIXTURES / "eia_retail_sales_sample.json").read_text())["response"]["data"]
FAKE_ARCHIVE_KEY = "archives/electricity-retail-sales/1.json"


def patch_dependencies(mocker, rows=None, archive_key=FAKE_ARCHIVE_KEY):
    mocker.patch(
        "app.services.ingest_service.eia_client.fetch_retail_sales",
        return_value=rows if rows is not None else SAMPLE_ROWS,
    )
    mocker.patch(
        "app.services.ingest_service.s3_archive.upload_run_archive",
        return_value=archive_key,
    )


# --- Period resolution tests ---

def test_latest_mode_march_15_resolves_to_february(mocker):
    mock_dt = mocker.patch("app.services.ingest_service.datetime")
    mock_dt.utcnow.return_value.date.return_value = date(2024, 3, 15)
    assert resolve_latest_period() == "2024-02"


def test_latest_mode_april_1_resolves_to_march(mocker):
    mock_dt = mocker.patch("app.services.ingest_service.datetime")
    mock_dt.utcnow.return_value.date.return_value = date(2024, 4, 1)
    assert resolve_latest_period() == "2024-03"


def test_invalid_mode_raises(db):
    with pytest.raises(ValueError, match="Invalid mode"):
        resolve_period_range("weekly", None, None)


def test_backfill_missing_periods_raises(db):
    with pytest.raises(ValueError, match="requires start_period and end_period"):
        resolve_period_range("backfill", None, None)


# --- Ingestion run tests ---

def test_run_row_created(db, mocker):
    patch_dependencies(mocker)
    result = run_ingestion(db, mode="backfill", start_period="2024-01", end_period="2024-01")

    run = db.query(IngestRun).filter(IngestRun.id == result["run_id"]).one()
    assert run.status == "success"
    assert run.dataset == "electricity/retail-sales"
    assert run.run_mode == "backfill"
    assert run.start_period == "2024-01"
    assert run.end_period == "2024-01"


def test_raw_rows_persisted(db, mocker):
    patch_dependencies(mocker)
    result = run_ingestion(db, mode="backfill", start_period="2024-01", end_period="2024-01")

    count = db.query(RawRetailRow).filter(RawRetailRow.run_id == result["run_id"]).count()
    assert count == len(SAMPLE_ROWS)
    assert result["row_count_raw"] == len(SAMPLE_ROWS)
    assert result["row_count_skipped_raw"] == 0


def test_duplicate_raw_row_skipped(db, mocker):
    patch_dependencies(mocker)

    result1 = run_ingestion(db, mode="backfill", start_period="2024-01", end_period="2024-01")
    assert result1["row_count_raw"] == len(SAMPLE_ROWS)

    result2 = run_ingestion(db, mode="backfill", start_period="2024-01", end_period="2024-01")
    assert result2["row_count_raw"] == 0
    assert result2["row_count_skipped_raw"] == len(SAMPLE_ROWS)
    assert result2["status"] == "success"


def test_s3_uploader_called_once(db, mocker):
    mocker.patch(
        "app.services.ingest_service.eia_client.fetch_retail_sales",
        return_value=SAMPLE_ROWS,
    )
    mock_upload = mocker.patch(
        "app.services.ingest_service.s3_archive.upload_run_archive",
        return_value=FAKE_ARCHIVE_KEY,
    )

    run_ingestion(db, mode="backfill", start_period="2024-01", end_period="2024-01")
    mock_upload.assert_called_once()


def test_failed_s3_upload_marks_run_failed(db, mocker):
    mocker.patch(
        "app.services.ingest_service.eia_client.fetch_retail_sales",
        return_value=SAMPLE_ROWS,
    )
    mocker.patch(
        "app.services.ingest_service.s3_archive.upload_run_archive",
        side_effect=RuntimeError("S3 connection error"),
    )

    result = run_ingestion(db, mode="backfill", start_period="2024-01", end_period="2024-01")

    assert result["status"] == "failed"
    run = db.query(IngestRun).filter(IngestRun.id == result["run_id"]).one()
    assert run.status == "failed"
    assert "S3" in run.error_message


# --- Phase 4: Normalization tests ---

def test_canonical_row_written_after_raw_stage(db, mocker):
    patch_dependencies(mocker)
    result = run_ingestion(db, mode="backfill", start_period="2024-01", end_period="2024-01")

    count = db.query(RetailMetric).filter(RetailMetric.run_id == result["run_id"]).count()
    assert count == len(SAMPLE_ROWS)
    assert result["row_count_normalized"] == len(SAMPLE_ROWS)
    assert result["row_count_inserted"] == len(SAMPLE_ROWS)
    assert result["row_count_updated"] == 0


def test_existing_canonical_key_updates_not_duplicates(db, mocker):
    row_v1 = [{"period": "2024-01", "stateid": "CA", "sectorid": "RES",
                "price": "29.62", "sales": "100", "revenue": "200", "customers": "300"}]
    row_v2 = [{"period": "2024-01", "stateid": "CA", "sectorid": "RES",
                "price": "31.52", "sales": "100", "revenue": "200", "customers": "300"}]

    patch_dependencies(mocker, rows=row_v1)
    run_ingestion(db, mode="backfill", start_period="2024-01", end_period="2024-01")

    patch_dependencies(mocker, rows=row_v2)
    result2 = run_ingestion(db, mode="backfill", start_period="2024-01", end_period="2024-01")

    total = db.query(RetailMetric).count()
    assert total == 1
    assert result2["row_count_inserted"] == 0
    assert result2["row_count_updated"] == 1


def test_upsert_price_is_updated_value(db, mocker):
    row_v1 = [{"period": "2024-01", "stateid": "CA", "sectorid": "RES",
                "price": "29.62", "sales": "100", "revenue": "200", "customers": "300"}]
    row_v2 = [{"period": "2024-01", "stateid": "CA", "sectorid": "RES",
                "price": "31.52", "sales": "100", "revenue": "200", "customers": "300"}]

    patch_dependencies(mocker, rows=row_v1)
    run_ingestion(db, mode="backfill", start_period="2024-01", end_period="2024-01")

    patch_dependencies(mocker, rows=row_v2)
    run_ingestion(db, mode="backfill", start_period="2024-01", end_period="2024-01")

    metric = db.query(RetailMetric).one()
    assert metric.price_cents_per_kwh == Decimal("31.52")


# --- Phase 5: Quality issue tests ---

def test_zero_rows_records_no_data_returned_issue(db, mocker):
    patch_dependencies(mocker, rows=[])

    result = run_ingestion(db, mode="backfill", start_period="2024-01", end_period="2024-01")

    issue = db.query(QualityIssue).filter(QualityIssue.run_id == result["run_id"]).one()
    assert issue.issue_type == "no_data_returned"
    assert issue.severity == "warning"


def test_zero_rows_still_marks_run_success(db, mocker):
    patch_dependencies(mocker, rows=[])

    result = run_ingestion(db, mode="backfill", start_period="2024-01", end_period="2024-01")

    assert result["status"] == "success"
    run = db.query(IngestRun).filter(IngestRun.id == result["run_id"]).one()
    assert run.status == "success"


def test_failed_fetch_marks_run_failed(db, mocker):
    mocker.patch(
        "app.services.ingest_service.eia_client.fetch_retail_sales",
        side_effect=RuntimeError("EIA API returned status 500"),
    )
    mocker.patch(
        "app.services.ingest_service.s3_archive.upload_run_archive",
        return_value=FAKE_ARCHIVE_KEY,
    )

    result = run_ingestion(db, mode="backfill", start_period="2024-01", end_period="2024-01")

    assert result["status"] == "failed"
    run = db.query(IngestRun).filter(IngestRun.id == result["run_id"]).one()
    assert run.status == "failed"
    assert "500" in run.error_message


# --- Phase 7: Full pipeline tests ---

def test_post_fetch_failure_marks_run_failed(db, mocker):
    patch_dependencies(mocker)
    mocker.patch(
        "app.services.ingest_service.analytics.refresh_state_month_summary",
        side_effect=RuntimeError("summary refresh exploded"),
    )

    result = run_ingestion(db, mode="backfill", start_period="2024-01", end_period="2024-01")

    assert result["status"] == "failed"
    assert "summary refresh exploded" in result["error_message"]
    run = db.query(IngestRun).filter(IngestRun.id == result["run_id"]).one()
    assert run.status == "failed"
    assert run.completed_at is not None


def test_summary_tables_refreshed_after_successful_run(db, mocker):
    patch_dependencies(mocker)
    run_ingestion(db, mode="backfill", start_period="2024-01", end_period="2024-01")

    assert db.query(StateMonthSummary).count() > 0
    assert db.query(SectorMonthSummary).count() > 0


def test_failed_normalization_still_records_run_and_issues(db, mocker):
    bad_row = [{"stateid": "CA", "sectorid": "RES",
                "price": "10", "sales": "100", "revenue": "200", "customers": "300"}]  # missing period
    patch_dependencies(mocker, rows=bad_row)

    result = run_ingestion(db, mode="backfill", start_period="2024-01", end_period="2024-01")

    assert result["status"] == "success"
    assert result["row_count_normalized"] == 0
    issues = db.query(QualityIssue).filter(QualityIssue.run_id == result["run_id"]).all()
    assert len(issues) >= 1
    assert any(i.issue_type == "missing_field" for i in issues)


def test_response_summary_dict_contains_expected_counts(db, mocker):
    patch_dependencies(mocker)
    result = run_ingestion(db, mode="backfill", start_period="2024-01", end_period="2024-01")

    expected_keys = [
        "run_id", "status", "run_mode", "dataset", "start_period", "end_period",
        "row_count_raw", "row_count_skipped_raw", "row_count_normalized",
        "row_count_inserted", "row_count_updated", "quality_issue_count", "s3_archive_key",
    ]
    for key in expected_keys:
        assert key in result

    assert result["row_count_raw"] == len(SAMPLE_ROWS)
    assert result["row_count_normalized"] == len(SAMPLE_ROWS)
    assert result["row_count_inserted"] == len(SAMPLE_ROWS)
    assert result["row_count_updated"] == 0
    assert result["row_count_skipped_raw"] == 0
