from app.models import IngestRun, QualityIssue


def detect_quality_issues(normalized_rows: list[dict]) -> list[dict]:
    """Run post-normalization quality checks and return issue dicts."""
    issues = []
    seen_keys = set()

    for row in normalized_rows:
        key = (row["dataset"], row["period"], row["state_id"], row["sector_id"])
        if key in seen_keys:
            issues.append({
                "issue_type": "duplicate_row",
                "severity": "warning",
                "issue_message": (
                    f"Duplicate canonical key in batch: "
                    f"{row['state_id']}/{row['sector_id']}/{row['period']}"
                ),
            })
        seen_keys.add(key)

        metrics = [
            row.get("price_cents_per_kwh"),
            row.get("sales_mwh"),
            row.get("revenue_thousand_usd"),
            row.get("customers_count"),
        ]
        if all(v is None for v in metrics):
            issues.append({
                "issue_type": "missing_field",
                "severity": "warning",
                "issue_message": "All metric fields are null after normalization",
            })

        for field in ["price_cents_per_kwh", "sales_mwh", "revenue_thousand_usd", "customers_count"]:
            val = row.get(field)
            if val is not None and val < 0:
                issues.append({
                    "issue_type": "negative_value",
                    "severity": "warning",
                    "issue_message": f"Negative value for {field}: {val}",
                })

        for field in ["state_id", "sector_id"]:
            if not row.get(field):
                issues.append({
                    "issue_type": "missing_field",
                    "severity": "error",
                    "issue_message": f"Required dimension '{field}' is null after normalization",
                })

    return issues


def build_quality_report(db, run_id: int | None = None) -> dict:
    """
    Return an aggregated quality report for one run or the latest run.

    If run_id is None, use the run with the highest id (latest by insertion order).
    Do NOT filter for only 'success' runs — a failed run should still be reportable.

    Read issue counts from the quality_issues table by aggregating rows WHERE run_id = X.
    Do NOT read from ingest_runs.quality_issue_count — that column may be stale.
    The quality_issues table is the source of truth for issue counts.
    """
    if run_id is None:
        run = db.query(IngestRun).order_by(IngestRun.id.desc()).first()
        if run is None:
            return {
                "run_id": None,
                "status": None,
                "row_count_raw": 0,
                "row_count_normalized": 0,
                "row_count_inserted": 0,
                "row_count_updated": 0,
                "row_count_skipped_raw": 0,
                "issue_count_total": 0,
                "issues_by_severity": {},
                "issues_by_type": {},
            }
        run_id = run.id
    else:
        run = db.query(IngestRun).filter(IngestRun.id == run_id).first()
        if run is None:
            return {
                "run_id": run_id,
                "status": None,
                "row_count_raw": 0,
                "row_count_normalized": 0,
                "row_count_inserted": 0,
                "row_count_updated": 0,
                "row_count_skipped_raw": 0,
                "issue_count_total": 0,
                "issues_by_severity": {},
                "issues_by_type": {},
            }

    issues = db.query(QualityIssue).filter(QualityIssue.run_id == run_id).all()

    by_severity = {}
    by_type = {}
    for issue in issues:
        by_severity[issue.severity] = by_severity.get(issue.severity, 0) + 1
        by_type[issue.issue_type] = by_type.get(issue.issue_type, 0) + 1

    return {
        "run_id": run_id,
        "status": run.status,
        "row_count_raw": run.row_count_raw,
        "row_count_normalized": run.row_count_normalized,
        "row_count_inserted": run.row_count_inserted,
        "row_count_updated": run.row_count_updated,
        "row_count_skipped_raw": run.row_count_skipped_raw,
        "issue_count_total": len(issues),
        "issues_by_severity": by_severity,
        "issues_by_type": by_type,
    }
