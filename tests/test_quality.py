from datetime import date, datetime
from decimal import Decimal

import pytest

from app.models import IngestRun, QualityIssue
from app.services.quality import detect_quality_issues


def normalized_row(**overrides):
    row = {
        "dataset": "electricity/retail-sales",
        "period": date(2024, 1, 1),
        "state_id": "CA",
        "sector_id": "RES",
        "price_cents_per_kwh": Decimal("31.52"),
        "sales_mwh": Decimal("21345.21"),
        "revenue_thousand_usd": Decimal("672813.19"),
        "customers_count": Decimal("13654000"),
    }
    row.update(overrides)
    return row


def test_detect_duplicate_canonical_key():
    rows = [
        normalized_row(),
        normalized_row(),  # same key
    ]
    issues = detect_quality_issues(rows)

    assert len(issues) == 1
    assert issues[0]["issue_type"] == "duplicate_row"
    assert issues[0]["severity"] == "warning"


def test_no_issues_for_clean_rows():
    rows = [
        normalized_row(state_id="CA"),
        normalized_row(state_id="TX"),
    ]
    issues = detect_quality_issues(rows)
    assert issues == []


def test_detect_all_metrics_null():
    rows = [normalized_row(
        price_cents_per_kwh=None,
        sales_mwh=None,
        revenue_thousand_usd=None,
        customers_count=None,
    )]
    issues = detect_quality_issues(rows)

    assert any(i["issue_type"] == "missing_field" for i in issues)


def test_detect_negative_values():
    rows = [normalized_row(price_cents_per_kwh=Decimal("-5.00"))]
    issues = detect_quality_issues(rows)

    assert len(issues) == 1
    assert issues[0]["issue_type"] == "negative_value"
    assert issues[0]["severity"] == "warning"
    assert "price_cents_per_kwh" in issues[0]["issue_message"]


def test_quality_issue_persistence(db):
    run = IngestRun(
        dataset="electricity/retail-sales",
        started_at=datetime(2024, 1, 1),
        status="success",
        run_mode="backfill",
        start_period="2024-01",
        end_period="2024-01",
    )
    db.add(run)
    db.flush()

    issue = QualityIssue(
        run_id=run.id,
        raw_row_id=None,
        metric_id=None,
        issue_type="missing_field",
        severity="warning",
        issue_message="Test issue",
        created_at=datetime(2024, 1, 1),
    )
    db.add(issue)
    db.commit()

    saved = db.query(QualityIssue).filter(QualityIssue.run_id == run.id).one()
    assert saved.issue_type == "missing_field"
    assert saved.severity == "warning"
    assert saved.raw_row_id is None
    assert saved.metric_id is None
