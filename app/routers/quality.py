from fastapi import APIRouter, Depends, HTTPException

from app import crud
from app.database import get_db
from app.services.quality import build_quality_report

router = APIRouter()


@router.get("/issues")
def list_issues(run_id: int | None = None, limit: int = 100, db=Depends(get_db)):
    return crud.list_quality_issues(db, run_id=run_id, limit=limit)


@router.get("/issues/{issue_id}")
def get_issue(issue_id: int, db=Depends(get_db)):
    issue = crud.get_quality_issue(db, issue_id)
    if issue is None:
        raise HTTPException(status_code=404, detail="Issue not found")
    return issue


@router.get("/report")
def quality_report(run_id: int | None = None, db=Depends(get_db)):
    return build_quality_report(db, run_id=run_id)
