from datetime import date
from decimal import Decimal

from app.services.normalizer import normalize_retail_row


def valid_row(**overrides):
    row = {
        "period": "2024-01",
        "stateid": "CA",
        "sectorid": "RES",
        "price": "31.52",
        "sales": "21345.21",
        "revenue": "672813.19",
        "customers": "13654000",
    }
    row.update(overrides)
    return row


def test_valid_row_normalization():
    normalized, issues = normalize_retail_row(valid_row())

    assert issues == []
    assert normalized is not None
    assert normalized["period"] == date(2024, 1, 1)
    assert normalized["state_id"] == "CA"
    assert normalized["sector_id"] == "RES"
    assert normalized["price_cents_per_kwh"] == Decimal("31.52")
    assert normalized["sales_mwh"] == Decimal("21345.21")
    assert normalized["revenue_thousand_usd"] == Decimal("672813.19")
    assert normalized["customers_count"] == Decimal("13654000")
    assert normalized["dataset"] == "electricity/retail-sales"


def test_invalid_period_returns_issue():
    normalized, issues = normalize_retail_row(valid_row(period="2024-99"))

    assert normalized is None
    assert len(issues) == 1
    assert issues[0]["issue_type"] == "invalid_period"
    assert issues[0]["severity"] == "error"
    assert "2024-99" in issues[0]["issue_message"]


def test_missing_period_returns_issue():
    row = valid_row()
    del row["period"]
    normalized, issues = normalize_retail_row(row)

    assert normalized is None
    assert issues[0]["issue_type"] == "missing_field"
    assert "period" in issues[0]["issue_message"]


def test_missing_state_returns_issue():
    row = valid_row()
    del row["stateid"]
    normalized, issues = normalize_retail_row(row)

    assert normalized is None
    assert issues[0]["issue_type"] == "missing_field"
    assert "stateid" in issues[0]["issue_message"]


def test_all_metrics_null_returns_issue():
    normalized, issues = normalize_retail_row(valid_row(
        price=None, sales=None, revenue=None, customers=None
    ))

    assert normalized is None
    assert len(issues) == 1
    assert issues[0]["issue_type"] == "missing_field"
    assert issues[0]["severity"] == "warning"


def test_partial_metrics_still_normalize():
    normalized, issues = normalize_retail_row(valid_row(
        sales=None, revenue=None, customers=None
    ))

    assert issues == []
    assert normalized is not None
    assert normalized["price_cents_per_kwh"] == Decimal("31.52")
    assert normalized["sales_mwh"] is None
    assert normalized["revenue_thousand_usd"] is None
    assert normalized["customers_count"] is None


def test_invalid_numeric_emits_issue():
    """A non-null unparseable value must emit invalid_numeric_value, not silently become None."""
    normalized, issues = normalize_retail_row(valid_row(price="N/A"))

    assert len(issues) == 1
    assert issues[0]["issue_type"] == "invalid_numeric_value"
    assert issues[0]["severity"] == "warning"
    assert "price" in issues[0]["issue_message"]
    assert "N/A" in issues[0]["issue_message"]
    # Row still normalizes — other fields are valid
    assert normalized is not None
    assert normalized["price_cents_per_kwh"] is None


def test_multiple_invalid_numerics_each_emit_issue():
    normalized, issues = normalize_retail_row(valid_row(price="N/A", sales="--"))

    invalid_issues = [i for i in issues if i["issue_type"] == "invalid_numeric_value"]
    assert len(invalid_issues) == 2
    fields = {i["issue_message"] for i in invalid_issues}
    assert any("price" in f for f in fields)
    assert any("sales" in f for f in fields)
