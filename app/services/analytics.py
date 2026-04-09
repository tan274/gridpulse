from datetime import date, datetime

from sqlalchemy import func

from app.models import RetailMetric, SectorMonthSummary, StateMonthSummary


def refresh_state_month_summary(db) -> int:
    """Rebuild state_month_summary from retail_metrics. Return row count written."""
    now = datetime.utcnow()

    rows = (
        db.query(
            RetailMetric.period,
            RetailMetric.state_id,
            func.avg(RetailMetric.price_cents_per_kwh).label("avg_price"),
            func.sum(RetailMetric.sales_mwh).label("total_sales"),
            func.sum(RetailMetric.revenue_thousand_usd).label("total_revenue"),
            func.sum(RetailMetric.customers_count).label("total_customers"),
        )
        .group_by(RetailMetric.period, RetailMetric.state_id)
        .all()
    )

    db.query(StateMonthSummary).delete()

    for row in rows:
        db.add(StateMonthSummary(
            period=row.period,
            state_id=row.state_id,
            avg_price_cents_per_kwh=row.avg_price,
            total_sales_mwh=row.total_sales,
            total_revenue_thousand_usd=row.total_revenue,
            total_customers_count=row.total_customers,
            refreshed_at=now,
        ))

    db.flush()
    return len(rows)


def refresh_sector_month_summary(db) -> int:
    """Rebuild sector_month_summary from retail_metrics. Return row count written."""
    now = datetime.utcnow()

    rows = (
        db.query(
            RetailMetric.period,
            RetailMetric.sector_id,
            func.avg(RetailMetric.price_cents_per_kwh).label("avg_price"),
            func.sum(RetailMetric.sales_mwh).label("total_sales"),
            func.sum(RetailMetric.revenue_thousand_usd).label("total_revenue"),
            func.sum(RetailMetric.customers_count).label("total_customers"),
        )
        .group_by(RetailMetric.period, RetailMetric.sector_id)
        .all()
    )

    db.query(SectorMonthSummary).delete()

    for row in rows:
        db.add(SectorMonthSummary(
            period=row.period,
            sector_id=row.sector_id,
            avg_price_cents_per_kwh=row.avg_price,
            total_sales_mwh=row.total_sales,
            total_revenue_thousand_usd=row.total_revenue,
            total_customers_count=row.total_customers,
            refreshed_at=now,
        ))

    db.flush()
    return len(rows)


def get_price_movers(db, end_period: str, limit: int = 10) -> list[dict]:
    """
    Return top states ranked by absolute price change (RES sector only).

    Compares avg(price_cents_per_kwh) at end_period vs exactly 12 months prior.
    States missing either comparison period are excluded.
    `end_period` is a YYYY-MM string (e.g. "2024-12").
    """
    year, month = end_period.split("-")
    end_date = date(int(year), int(month), 1)
    start_date = end_date.replace(year=end_date.year - 1)

    end_prices = {
        row.state_id: row.price
        for row in db.query(
            RetailMetric.state_id,
            func.avg(RetailMetric.price_cents_per_kwh).label("price"),
        )
        .filter(RetailMetric.sector_id == "RES", RetailMetric.period == end_date)
        .group_by(RetailMetric.state_id)
        .all()
    }

    start_prices = {
        row.state_id: row.price
        for row in db.query(
            RetailMetric.state_id,
            func.avg(RetailMetric.price_cents_per_kwh).label("price"),
        )
        .filter(RetailMetric.sector_id == "RES", RetailMetric.period == start_date)
        .group_by(RetailMetric.state_id)
        .all()
    }

    results = []
    for state_id, end_avg in end_prices.items():
        if state_id not in start_prices or end_avg is None:
            continue
        start_avg = start_prices[state_id]
        if start_avg is None:
            continue
        abs_change = end_avg - start_avg
        pct_change = (abs_change / start_avg * 100) if start_avg != 0 else None
        results.append({
            "state_id": state_id,
            "start_period": start_date.isoformat(),
            "end_period": end_date.isoformat(),
            "start_avg_price_cents_per_kwh": start_avg,
            "end_avg_price_cents_per_kwh": end_avg,
            "absolute_change": abs_change,
            "percent_change": pct_change,
        })

    results.sort(key=lambda r: r["absolute_change"], reverse=True)
    results = results[:limit]

    for i, r in enumerate(results):
        r["rank"] = i + 1

    return results
