from datetime import datetime

import pytest

from app.models import IngestRun, QualityIssue
from app.services.quality import build_quality_report


def make_run(db, status="success", start="2024-01", end="2024-01"):
    run = IngestRun(
        dataset="electricity/retail-sales",
        started_at=datetime(2024, 1, 1),
        completed_at=datetime(2024, 1, 1, 1),
        status=status,
        run_mode="backfill",
        start_period=start,
        end_period=end,
        row_count_raw=4,
        row_count_skipped_raw=0,
        row_count_normalized=4,
        row_count_inserted=4,
        row_count_updated=0,
    )
    db.add(run)
    db.flush()
    return run


def make_issue(db, run_id, issue_type="missing_field", severity="warning"):
    issue = QualityIssue(
        run_id=run_id,
        raw_row_id=None,
        metric_id=None,
        issue_type=issue_type,
        severity=severity,
        issue_message="test issue",
        created_at=datetime(2024, 1, 1),
    )
    db.add(issue)
    db.flush()
    return issue


def test_quality_report_aggregates_correctly(db):
    run = make_run(db)
    make_issue(db, run.id, issue_type="missing_field", severity="warning")
    make_issue(db, run.id, issue_type="negative_value", severity="warning")
    make_issue(db, run.id, issue_type="duplicate_row", severity="warning")
    db.commit()

    report = build_quality_report(db, run_id=run.id)

    assert report["run_id"] == run.id
    assert report["status"] == "success"
    assert report["issue_count_total"] == 3
    assert report["issues_by_severity"] == {"warning": 3}
    assert report["issues_by_type"]["missing_field"] == 1
    assert report["issues_by_type"]["negative_value"] == 1
    assert report["issues_by_type"]["duplicate_row"] == 1
    assert report["row_count_raw"] == 4
    assert report["row_count_normalized"] == 4


def test_latest_run_default(db):
    run1 = make_run(db)
    make_issue(db, run1.id, issue_type="missing_field")
    run2 = make_run(db)
    db.commit()

    report = build_quality_report(db)

    assert report["run_id"] == run2.id
    assert report["issue_count_total"] == 0


def test_empty_report_when_no_runs(db):
    report = build_quality_report(db)

    assert report["run_id"] is None
    assert report["status"] is None
    assert report["issue_count_total"] == 0
    assert report["issues_by_severity"] == {}
    assert report["issues_by_type"] == {}
