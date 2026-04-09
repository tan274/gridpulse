"""
Usage:
  python scripts/run_ingest.py --mode latest
  python scripts/run_ingest.py --mode backfill --start 2024-01 --end 2024-12
  python scripts/run_ingest.py --mode backfill --start 2024-01 --end 2024-12 --states CA TX --sectors RES COM
"""
import argparse
from app.database import SessionLocal
from app.services.ingest_service import run_ingestion


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["latest", "backfill"], default="backfill")
    parser.add_argument("--start", default=None)
    parser.add_argument("--end", default=None)
    parser.add_argument("--states", nargs="*", default=None)
    parser.add_argument("--sectors", nargs="*", default=None)
    args = parser.parse_args()

    db = SessionLocal()
    try:
        result = run_ingestion(
            db=db,
            mode=args.mode,
            start_period=args.start,
            end_period=args.end,
            state_ids=args.states,
            sector_ids=args.sectors,
        )
        print(result)
    finally:
        db.close()


if __name__ == "__main__":
    main()
