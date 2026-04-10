from datetime import datetime
from decimal import Decimal, InvalidOperation

from app.constants import DATASET


def _parse_metric(value) -> tuple[Decimal | None, bool]:
    """Returns (parsed_value, failed). failed=True means value was present but unparseable."""
    if value is None:
        return None, False
    try:
        return Decimal(str(value)), False
    except (InvalidOperation, ValueError):
        return None, True


def normalize_retail_row(raw_row: dict) -> tuple[dict | None, list[dict]]:
    """
    Convert one raw EIA row into a canonical metric dict.
    Return (normalized_dict, issues).
    If row cannot be normalized, return (None, issues).
    """
    issues = []

    period_str = raw_row.get("period")
    if not period_str:
        issues.append({
            "issue_type": "missing_field",
            "severity": "error",
            "issue_message": "Missing required field 'period'",
        })
        return None, issues

    state_id = raw_row.get("stateid")
    if not state_id:
        issues.append({
            "issue_type": "missing_field",
            "severity": "error",
            "issue_message": "Missing required field 'stateid'",
        })
        return None, issues

    sector_id = raw_row.get("sectorid")
    if not sector_id:
        issues.append({
            "issue_type": "missing_field",
            "severity": "error",
            "issue_message": "Missing required field 'sectorid'",
        })
        return None, issues

    try:
        period = datetime.strptime(period_str, "%Y-%m").date()
    except ValueError:
        issues.append({
            "issue_type": "invalid_period",
            "severity": "error",
            "issue_message": f"Could not parse period '{period_str}'",
        })
        return None, issues

    price, price_failed = _parse_metric(raw_row.get("price"))
    sales, sales_failed = _parse_metric(raw_row.get("sales"))
    revenue, revenue_failed = _parse_metric(raw_row.get("revenue"))
    customers, customers_failed = _parse_metric(raw_row.get("customers"))

    for raw_key, failed in [
        ("price", price_failed),
        ("sales", sales_failed),
        ("revenue", revenue_failed),
        ("customers", customers_failed),
    ]:
        if failed:
            issues.append({
                "issue_type": "invalid_numeric_value",
                "severity": "warning",
                "issue_message": f"Unparseable value for '{raw_key}': {raw_row.get(raw_key)!r}",
            })

    if all(v is None for v in [price, sales, revenue, customers]):
        issues.append({
            "issue_type": "missing_field",
            "severity": "warning",
            "issue_message": "All metric fields are null",
        })
        return None, issues

    return {
        "dataset": DATASET,
        "period": period,
        "state_id": state_id,
        "sector_id": sector_id,
        "price_cents_per_kwh": price,
        "sales_mwh": sales,
        "revenue_thousand_usd": revenue,
        "customers_count": customers,
    }, issues
