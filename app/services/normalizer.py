from datetime import datetime
from decimal import Decimal, InvalidOperation

DATASET = "electricity/retail-sales"


def _parse_metric(value) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


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

    price = _parse_metric(raw_row.get("price"))
    sales = _parse_metric(raw_row.get("sales"))
    revenue = _parse_metric(raw_row.get("revenue"))
    customers = _parse_metric(raw_row.get("customers"))

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
