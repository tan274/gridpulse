from fastapi import APIRouter, Depends, HTTPException

from app import crud
from app.database import get_db
from app.routers.utils import validate_period
from app.services import analytics

VALID_METRICS = {"avg_price_cents_per_kwh", "total_sales_mwh", "total_revenue_thousand_usd"}

router = APIRouter()


@router.get("/state-summary")
def state_summary(
    period: str | None = None,
    state_id: str | None = None,
    limit: int = 100,
    db=Depends(get_db),
):
    if period is not None:
        validate_period(period)
    return crud.list_state_summary(db, period=period, state_id=state_id, limit=limit)


@router.get("/sector-summary")
def sector_summary(
    period: str | None = None,
    sector_id: str | None = None,
    limit: int = 100,
    db=Depends(get_db),
):
    if period is not None:
        validate_period(period)
    return crud.list_sector_summary(db, period=period, sector_id=sector_id, limit=limit)


@router.get("/top-states")
def top_states(
    period: str,
    metric: str,
    limit: int = 10,
    db=Depends(get_db),
):
    validate_period(period)
    if metric not in VALID_METRICS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid metric '{metric}'. Must be one of: {sorted(VALID_METRICS)}",
        )
    return crud.get_top_states(db, period=period, metric=metric, limit=limit)


@router.get("/price-movers")
def price_movers(
    end_period: str,
    limit: int = 10,
    db=Depends(get_db),
):
    validate_period(end_period)
    return analytics.get_price_movers(db, end_period=end_period, limit=limit)
