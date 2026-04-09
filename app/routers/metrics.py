from fastapi import APIRouter, Depends, HTTPException

from app import crud
from app.database import get_db
from app.routers.utils import validate_period

router = APIRouter()


@router.get("")
def list_metrics(
    state_id: str | None = None,
    sector_id: str | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    limit: int = 100,
    offset: int = 0,
    db=Depends(get_db),
):
    if start_period is not None:
        validate_period(start_period)
    if end_period is not None:
        validate_period(end_period)
    return crud.list_retail_metrics(
        db,
        state_id=state_id,
        sector_id=sector_id,
        start_period=start_period,
        end_period=end_period,
        limit=limit,
        offset=offset,
    )


@router.get("/{metric_id}")
def get_metric(metric_id: int, db=Depends(get_db)):
    metric = crud.get_retail_metric(db, metric_id)
    if metric is None:
        raise HTTPException(status_code=404, detail="Metric not found")
    return metric
