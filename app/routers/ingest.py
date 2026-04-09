from fastapi import APIRouter, Depends, HTTPException

from app import crud
from app.database import get_db
from app.schemas import IngestRunRequest
from app.services import ingest_service

router = APIRouter()


@router.post("/run")
def trigger_ingest_run(body: IngestRunRequest, db=Depends(get_db)):
    return ingest_service.run_ingestion(
        db,
        mode=body.mode,
        start_period=body.start_period,
        end_period=body.end_period,
        state_ids=body.state_ids,
        sector_ids=body.sector_ids,
    )


@router.get("/runs")
def list_runs(limit: int = 20, db=Depends(get_db)):
    limit = min(limit, 100)
    return crud.list_ingest_runs(db, limit=limit)


@router.get("/runs/{run_id}")
def get_run(run_id: int, db=Depends(get_db)):
    run = crud.get_ingest_run(db, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return run
