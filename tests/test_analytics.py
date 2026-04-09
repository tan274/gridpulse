from datetime import date, datetime
from decimal import Decimal

import pytest

from app.models import IngestRun, RawRetailRow, RetailMetric, StateMonthSummary, SectorMonthSummary
from app.services.analytics import (
    get_price_movers,
    refresh_sector_month_summary,
    refresh_state_month_summary,
)

DATASET = "electricity/retail-sales"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_run(db):
    run = IngestRun(
        dataset=DATASET,
        started_at=datetime.utcnow(),
        status="success",
        run_mode="backfill",
        start_period="2024-01",
        end_period="2024-01",
    )
    db.add(run)
    db.flush()
    return run


def _make_raw_row(db, run_id, *, period="2024-01", state_id="CA", sector_id="RES", source_hash=None):
    source_hash = source_hash or f"{period}-{state_id}-{sector_id}"
    row = RawRetailRow(
        run_id=run_id,
        dataset=DATASET,
        period=period,
        state_id=state_id,
        sector_id=sector_id,
        source_hash=source_hash,
        row_json={},
        created_at=datetime.utcnow(),
    )
    db.add(row)
    db.flush()
    return row


def _make_metric(db, run_id, raw_row_id, *, period, state_id, sector_id,
                 price=None, sales=None, revenue=None, customers=None, source_hash=None):
    source_hash = source_hash or f"metric-{period}-{state_id}-{sector_id}"
    m = RetailMetric(
        run_id=run_id,
        raw_row_id=raw_row_id,
        dataset=DATASET,
        period=period,
        state_id=state_id,
        sector_id=sector_id,
        price_cents_per_kwh=Decimal(str(price)) if price is not None else None,
        sales_mwh=Decimal(str(sales)) if sales is not None else None,
        revenue_thousand_usd=Decimal(str(revenue)) if revenue is not None else None,
        customers_count=Decimal(str(customers)) if customers is not None else None,
        source_hash=source_hash,
        created_at=datetime.utcnow(),
    )
    db.add(m)
    db.flush()
    return m


# ---------------------------------------------------------------------------
# State summary tests
# ---------------------------------------------------------------------------

def test_state_summary_rows_generated(db):
    run = _make_run(db)
    raw = _make_raw_row(db, run.id)
    _make_metric(db, run.id, raw.id, period=date(2024, 1, 1), state_id="CA", sector_id="RES", price=10, sales=100, revenue=500, customers=50)
    _make_metric(db, run.id, raw.id, period=date(2024, 1, 1), state_id="TX", sector_id="RES", price=8, sales=200, revenue=600, customers=80)

    count = refresh_state_month_summary(db)

    assert count == 2
    summaries = db.query(StateMonthSummary).order_by(StateMonthSummary.state_id).all()
    assert len(summaries) == 2
    state_ids = {s.state_id for s in summaries}
    assert state_ids == {"CA", "TX"}


def test_sector_summary_rows_generated(db):
    run = _make_run(db)
    raw = _make_raw_row(db, run.id)
    _make_metric(db, run.id, raw.id, period=date(2024, 1, 1), state_id="CA", sector_id="RES", price=10, sales=100, revenue=500, customers=50)
    _make_metric(db, run.id, raw.id, period=date(2024, 1, 1), state_id="CA", sector_id="COM", price=12, sales=150, revenue=700, customers=30)

    count = refresh_sector_month_summary(db)

    assert count == 2
    summaries = db.query(SectorMonthSummary).order_by(SectorMonthSummary.sector_id).all()
    assert len(summaries) == 2
    sector_ids = {s.sector_id for s in summaries}
    assert sector_ids == {"COM", "RES"}


def test_state_summary_average_price(db):
    """avg_price_cents_per_kwh averages across all sectors for a (period, state)."""
    run = _make_run(db)
    raw = _make_raw_row(db, run.id)
    _make_metric(db, run.id, raw.id, period=date(2024, 1, 1), state_id="CA", sector_id="RES", price=10)
    _make_metric(db, run.id, raw.id, period=date(2024, 1, 1), state_id="CA", sector_id="COM", price=20)

    refresh_state_month_summary(db)

    summary = db.query(StateMonthSummary).filter_by(state_id="CA", period=date(2024, 1, 1)).one()
    assert summary.avg_price_cents_per_kwh == Decimal("15")


def test_state_summary_total_sales(db):
    run = _make_run(db)
    raw = _make_raw_row(db, run.id)
    _make_metric(db, run.id, raw.id, period=date(2024, 1, 1), state_id="CA", sector_id="RES", sales=100)
    _make_metric(db, run.id, raw.id, period=date(2024, 1, 1), state_id="CA", sector_id="COM", sales=250)

    refresh_state_month_summary(db)

    summary = db.query(StateMonthSummary).filter_by(state_id="CA", period=date(2024, 1, 1)).one()
    assert summary.total_sales_mwh == Decimal("350")


def test_state_summary_total_revenue(db):
    run = _make_run(db)
    raw = _make_raw_row(db, run.id)
    _make_metric(db, run.id, raw.id, period=date(2024, 1, 1), state_id="CA", sector_id="RES", revenue=400)
    _make_metric(db, run.id, raw.id, period=date(2024, 1, 1), state_id="CA", sector_id="COM", revenue=600)

    refresh_state_month_summary(db)

    summary = db.query(StateMonthSummary).filter_by(state_id="CA", period=date(2024, 1, 1)).one()
    assert summary.total_revenue_thousand_usd == Decimal("1000")


def test_state_summary_refresh_replaces_stale(db):
    run = _make_run(db)
    raw = _make_raw_row(db, run.id)
    _make_metric(db, run.id, raw.id, period=date(2024, 1, 1), state_id="CA", sector_id="RES", price=10, sales=100, revenue=500, customers=50)

    refresh_state_month_summary(db)
    first = db.query(StateMonthSummary).filter_by(state_id="CA").one()
    assert first.total_sales_mwh == Decimal("100")

    # Update the underlying metric
    metric = db.query(RetailMetric).filter_by(state_id="CA").one()
    metric.sales_mwh = Decimal("999")
    db.flush()

    refresh_state_month_summary(db)
    db.expire_all()

    refreshed = db.query(StateMonthSummary).filter_by(state_id="CA").one()
    assert refreshed.total_sales_mwh == Decimal("999")
    assert db.query(StateMonthSummary).count() == 1


# ---------------------------------------------------------------------------
# Price movers tests
# ---------------------------------------------------------------------------

def _seed_price_movers(db, *, state_id, end_period, end_price, prior_price, sector_id="RES"):
    """Insert RES metrics for a state at end_period and the prior year."""
    run = _make_run(db)
    raw_end = _make_raw_row(db, run.id, period=end_period.strftime("%Y-%m"), state_id=state_id, sector_id=sector_id, source_hash=f"end-{state_id}-{sector_id}-{end_period}")
    prior_period = end_period.replace(year=end_period.year - 1)
    raw_prior = _make_raw_row(db, run.id, period=prior_period.strftime("%Y-%m"), state_id=state_id, sector_id=sector_id, source_hash=f"prior-{state_id}-{sector_id}-{prior_period}")
    _make_metric(db, run.id, raw_end.id, period=end_period, state_id=state_id, sector_id=sector_id, price=end_price, source_hash=f"m-end-{state_id}-{sector_id}-{end_period}")
    _make_metric(db, run.id, raw_prior.id, period=prior_period, state_id=state_id, sector_id=sector_id, price=prior_price, source_hash=f"m-prior-{state_id}-{sector_id}-{prior_period}")
    db.commit()


def test_price_movers_ranks_by_absolute_change_descending(db):
    end = date(2024, 1, 1)
    _seed_price_movers(db, state_id="CA", end_period=end, end_price=15, prior_price=10)   # +5
    _seed_price_movers(db, state_id="TX", end_period=end, end_price=12, prior_price=10)   # +2
    _seed_price_movers(db, state_id="NY", end_period=end, end_price=18, prior_price=10)   # +8

    movers = get_price_movers(db, end_period="2024-01")

    assert [m["state_id"] for m in movers] == ["NY", "CA", "TX"]
    assert movers[0]["rank"] == 1
    assert movers[1]["rank"] == 2
    assert movers[2]["rank"] == 3


def test_price_movers_excludes_states_missing_either_period(db):
    end = date(2024, 1, 1)
    prior = date(2023, 1, 1)
    run = _make_run(db)

    # CA: has both periods → included
    raw_ca_end = _make_raw_row(db, run.id, period="2024-01", state_id="CA", source_hash="ca-end")
    raw_ca_prior = _make_raw_row(db, run.id, period="2023-01", state_id="CA", source_hash="ca-prior")
    _make_metric(db, run.id, raw_ca_end.id, period=end, state_id="CA", sector_id="RES", price=12, source_hash="m-ca-end")
    _make_metric(db, run.id, raw_ca_prior.id, period=prior, state_id="CA", sector_id="RES", price=10, source_hash="m-ca-prior")

    # TX: only end period → excluded
    raw_tx = _make_raw_row(db, run.id, period="2024-01", state_id="TX", source_hash="tx-end")
    _make_metric(db, run.id, raw_tx.id, period=end, state_id="TX", sector_id="RES", price=15, source_hash="m-tx-end")

    # NY: only prior period → excluded
    raw_ny = _make_raw_row(db, run.id, period="2023-01", state_id="NY", source_hash="ny-prior")
    _make_metric(db, run.id, raw_ny.id, period=prior, state_id="NY", sector_id="RES", price=9, source_hash="m-ny-prior")

    db.commit()
    movers = get_price_movers(db, end_period="2024-01")

    assert len(movers) == 1
    assert movers[0]["state_id"] == "CA"


def test_price_movers_empty_when_end_period_missing(db):
    movers = get_price_movers(db, end_period="2024-01")
    assert movers == []


def test_price_movers_respects_limit(db):
    end = date(2024, 1, 1)
    for i, state in enumerate(["CA", "TX", "NY", "FL", "WA"]):
        _seed_price_movers(db, state_id=state, end_period=end, end_price=10 + i, prior_price=5)

    movers = get_price_movers(db, end_period="2024-01", limit=3)
    assert len(movers) == 3
    assert [m["state_id"] for m in movers] == ["WA", "FL", "NY"]


def test_price_movers_percent_change(db):
    end = date(2024, 1, 1)
    _seed_price_movers(db, state_id="CA", end_period=end, end_price=12, prior_price=10)

    movers = get_price_movers(db, end_period="2024-01")

    assert len(movers) == 1
    assert movers[0]["start_period"] == "2023-01-01"
    assert movers[0]["end_period"] == "2024-01-01"
    assert movers[0]["start_avg_price_cents_per_kwh"] == Decimal("10")
    assert movers[0]["end_avg_price_cents_per_kwh"] == Decimal("12")
    assert movers[0]["absolute_change"] == Decimal("2")
    assert movers[0]["percent_change"] == Decimal("20")


def test_price_movers_only_uses_res_sector(db):
    """COM sector rows must not affect price movers — only RES counts."""
    end = date(2024, 1, 1)

    # CA: RES with small change
    _seed_price_movers(db, state_id="CA", end_period=end, end_price=11, prior_price=10, sector_id="RES")

    # CA: COM with huge change — must not affect result
    _seed_price_movers(db, state_id="CA", end_period=end, end_price=100, prior_price=1, sector_id="COM")

    movers = get_price_movers(db, end_period="2024-01")

    assert len(movers) == 1
    assert movers[0]["state_id"] == "CA"
    assert movers[0]["absolute_change"] == Decimal("1")
