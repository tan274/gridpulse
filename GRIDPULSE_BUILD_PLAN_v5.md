# GridPulse — Full Build Plan

> **Reference document for Codex / Claude Code.**
> Read this before writing code.
> Complete each phase in order.
> Every phase has a test gate.
> Do not proceed until the current phase passes.
>
> **Project goal:** build a small but strong data-engineering-style backend that ingests public U.S. electricity retail-sales data from the EIA API, supports latest-month loads and explicit backfills, stores raw and normalized records in PostgreSQL, computes summary tables, exposes analytics through FastAPI, surfaces visible data-quality reports, archives raw payloads to S3, and deploys to AWS with CI/CD.

---

## 1. What This Project Is

GridPulse is a **data ingestion + analytics API** for public U.S. electricity retail-sales data.

It does six things:

1. Pulls monthly electricity retail-sales data from the official EIA API
2. Supports both latest-month ingestion and explicit historical backfills
3. Stores raw API rows and metadata for auditability
4. Normalizes those rows into a clean relational schema
5. Runs quality checks, builds a visible quality report, and refreshes summary tables
6. Serves analytics through a FastAPI API

This is **not** an energy research project. It is an engineering project that uses a clean public time-series dataset.

### The exact dataset in scope
Use only this EIA subject area in v1:

- **Electricity → retail-sales**

For v1, ingest these metrics when available:

- `price`
- `sales`
- `revenue`
- `customers`

Use these dimensions:

- `period` (monthly)
- `stateid`
- `sectorid`

---

## 2. Scope Boundaries

These boundaries are intentional. They keep the project finishable and make the code easier for AI coding tools to follow.

### In scope

- Python backend only
- FastAPI API
- PostgreSQL database
- Alembic migrations
- Docker + docker-compose for local development
- EIA API ingestion for one dataset only: `electricity/retail-sales`
- Summary tables refreshed after ingestion
- Latest-month ingestion and explicit historical backfill
- S3 archival of raw API payloads by ingestion run
- Visible quality report endpoint
- One polished analytics endpoint: price movers over trailing 12 months
- AWS EC2 deployment
- GitHub Actions for tests and deploy

### Out of scope

- No frontend
- No auth in v1
- No Spark
- No Airflow
- No Kafka / SQS / Celery
- No streaming
- No multiple datasets beyond `electricity/retail-sales`
- No asynchronous FastAPI endpoints unless clearly necessary
- No LLM features

### Design principles

- Prefer simple, explicit code over clever abstractions
- Prefer one clear path over multiple options
- Prefer plain service functions over heavy framework magic
- Prefer deterministic tests over live-network tests when possible
- Every table and endpoint must have a clear reason to exist

---

## 3. Chosen Architecture

### High-level flow

```text
EIA API
  -> decide run mode (latest or backfill)
  -> fetch monthly retail-sales rows
  -> persist raw payload + run metadata
  -> archive raw JSON payloads to S3      <- happens here, before normalization
  -> normalize rows into canonical facts table
  -> run quality checks
  -> build quality report summary
  -> refresh summary tables
  -> serve analytics through FastAPI
```

### Deployment architecture

Use this exact deployment model in v1:

- **EC2**: one Ubuntu instance
- **Docker Compose**: app container + postgres container
- **S3**: raw ingestion payload archive
- **GitHub Actions**: test on push, deploy to EC2 on main

---

## 4. Stack

| Layer | Technology |
|---|---|
| API | FastAPI |
| ASGI server | Uvicorn |
| HTTP client | requests |
| Database | PostgreSQL |
| ORM | SQLAlchemy 2.x |
| Migrations | Alembic |
| Validation | Pydantic v2 |
| Local dev | Docker + docker-compose |
| Tests | pytest |
| Cloud storage | AWS S3 |
| AWS compute | EC2 |
| CI/CD | GitHub Actions |

Use `requests` (not `httpx`) in v1 — simpler to monkeypatch in tests, no async complexity needed.

---

## 5. Project Folder Structure

Create this exact structure before writing logic.

```text
gridpulse/
├── app/
│   ├── __init__.py
│   ├── main.py                    # FastAPI app — wire routers here
│   ├── config.py                  # pydantic-settings BaseSettings
│   ├── database.py                # engine, session, Base, get_db
│   ├── models.py                  # SQLAlchemy ORM models
│   ├── schemas.py                 # Pydantic request/response schemas
│   ├── crud.py                    # raw DB reads/writes (no business logic)
│   ├── dependencies.py            # get_db and any reusable FastAPI Depends()
│   ├── services/
│   │   ├── __init__.py
│   │   ├── eia_client.py          # EIA API fetch logic
│   │   ├── ingest_service.py      # orchestrates ingestion run
│   │   ├── normalizer.py          # raw row -> canonical metric
│   │   ├── quality.py             # quality issue detection and reporting
│   │   ├── analytics.py           # summary table refresh and price movers
│   │   └── s3_archive.py          # upload raw payload archive to S3
│   └── routers/
│       ├── __init__.py
│       ├── health.py
│       ├── ingest.py
│       ├── metrics.py
│       ├── analytics.py
│       └── quality.py
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   ├── fixtures/
│   │   ├── eia_retail_sales_sample.json
│   │   └── eia_retail_sales_bad_rows.json
│   ├── test_models.py
│   ├── test_eia_client.py
│   ├── test_normalizer.py
│   ├── test_ingest_service.py
│   ├── test_quality.py
│   ├── test_analytics.py
│   ├── test_reporting.py
│   ├── test_api.py
│   └── test_health.py
├── alembic/
│   └── versions/
├── scripts/
│   ├── run_ingest.py              # local manual ingestion script (latest or backfill)
│   └── seed_dev_data.py           # optional local bootstrap
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── .gitignore
├── pytest.ini
├── requirements.txt
├── README.md
└── .github/
    └── workflows/
        └── ci.yml                     # contains both test and deploy jobs
```

### What belongs in each file

- `crud.py` — raw database reads and writes only: inserts, selects, upserts. No business logic.
- `services/` — business logic. May call `crud.py` functions or write directly via the db session.
- `routers/` — thin handlers only: validate input, call one service function, return response. No direct DB access.
- `dependencies.py` — re-exports `get_db` and any shared `Depends()` callables used across routers.

### `scripts/run_ingest.py` spec

This script allows manual ingestion runs from the command line without going through the API.

```python
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
```

---

## 6. Core Data Model

The database model is the backbone of the project. Do not improvise.

### Table 1: `ingest_runs`
Represents one ingestion attempt.

| Column | Type | Notes |
|---|---|---|
| id | Integer | primary key |
| dataset | String | always `electricity/retail-sales` in v1 |
| started_at | DateTime | required |
| completed_at | DateTime | nullable |
| status | String | `running`, `success`, `failed` |
| run_mode | String | `latest` or `backfill` |
| start_period | String | e.g. `2024-01` |
| end_period | String | e.g. `2024-12` |
| row_count_raw | Integer | default 0 |
| row_count_skipped_raw | Integer | default 0 |
| row_count_normalized | Integer | default 0 |
| row_count_inserted | Integer | default 0 |
| row_count_updated | Integer | default 0 |
| quality_issue_count | Integer | default 0 |
| error_message | Text | nullable |
| s3_archive_key | String | nullable |

### Table 2: `raw_retail_rows`
Stores raw EIA rows plus a stable dedupe hash.

| Column | Type | Notes |
|---|---|---|
| id | Integer | primary key |
| run_id | Integer | FK -> ingest_runs |
| dataset | String | required |
| period | String | required |
| state_id | String | required |
| sector_id | String | required |
| source_hash | String | required |
| row_json | JSONB | full raw row |
| created_at | DateTime | required |

**Constraint:** unique on `(dataset, source_hash)`

### Table 3: `retail_metrics`
Canonical normalized fact table.

| Column | Type | Notes |
|---|---|---|
| id | Integer | primary key |
| run_id | Integer | FK -> ingest_runs |
| raw_row_id | Integer | FK -> raw_retail_rows |
| dataset | String | required |
| period | Date | required, use first day of month |
| state_id | String | required |
| sector_id | String | required |
| price_cents_per_kwh | Numeric | nullable |
| sales_mwh | Numeric | nullable |
| revenue_thousand_usd | Numeric | nullable |
| customers_count | Numeric | nullable |
| source_hash | String | required — update on upsert to reflect latest raw row |
| created_at | DateTime | required — set on insert only |
| updated_at | DateTime | required — set on insert AND updated on every upsert |

**Constraint:** unique on `(dataset, period, state_id, sector_id)`

**`updated_at` implementation:** use `server_default=func.now()` and `onupdate=func.now()` in SQLAlchemy so it auto-updates on every write. Do not manage this manually.

**`source_hash` on upsert:** when an existing canonical row is updated, replace `source_hash` with the new raw row's hash so the canonical row always traces back to its most recent source.

### Table 4: `quality_issues`
Problems found during normalization or post-normalization validation.

| Column | Type | Notes |
|---|---|---|
| id | Integer | primary key |
| run_id | Integer | FK -> ingest_runs |
| raw_row_id | Integer | FK -> raw_retail_rows, nullable |
| metric_id | Integer | FK -> retail_metrics, nullable |
| issue_type | String | e.g. `missing_field`, `invalid_period`, `duplicate_row` |
| severity | String | `warning` or `error` |
| issue_message | Text | required |
| created_at | DateTime | required |

### Table 5: `state_month_summary`
Aggregate table refreshed after each successful ingestion.

| Column | Type | Notes |
|---|---|---|
| period | Date | PK part 1 |
| state_id | String | PK part 2 |
| avg_price_cents_per_kwh | Numeric | nullable |
| total_sales_mwh | Numeric | nullable |
| total_revenue_thousand_usd | Numeric | nullable |
| total_customers_count | Numeric | nullable |
| refreshed_at | DateTime | required |

**Primary key:** `(period, state_id)`

### Table 6: `sector_month_summary`
Aggregate table refreshed after each successful ingestion.

| Column | Type | Notes |
|---|---|---|
| period | Date | PK part 1 |
| sector_id | String | PK part 2 |
| avg_price_cents_per_kwh | Numeric | nullable |
| total_sales_mwh | Numeric | nullable |
| total_revenue_thousand_usd | Numeric | nullable |
| total_customers_count | Numeric | nullable |
| refreshed_at | DateTime | required |

**Primary key:** `(period, sector_id)`

---

## 7. Canonical Rules

These rules remove ambiguity and make testing straightforward.

### Period format rules for API layer

This project uses two period representations. They must be consistent across all endpoints.

- **Query parameters** on all endpoints use `YYYY-MM` string format (e.g. `"2024-01"`)
- **Response fields** use `YYYY-MM-DD` date string format (e.g. `"2024-01-01"`, always the first of the month)
- Pydantic schemas must validate and convert at the boundary
- Do not mix formats within the same endpoint or across endpoints

This applies to every endpoint including `price-movers`. The `end_period` query param is `YYYY-MM`, not `YYYY-MM-DD`.

### Period handling
- EIA periods arrive as strings like `2024-01`
- Normalize to Python `date(2024, 1, 1)` in the canonical table
- Keep original raw string in `raw_retail_rows.row_json`

### Dataset handling
- `dataset` is always the literal string: `electricity/retail-sales`
- Do not generalize beyond this in v1

### State handling
- Store EIA state codes exactly as received, e.g. `CA`, `TX`, `US`
- No custom remapping in v1

### Sector handling
Store EIA `sectorid` exactly as received.

Expected values commonly include things like:
- `RES`
- `COM`
- `IND`
- `TRA`
- `OTH`
- `ALL`

Do not invent a separate sector lookup table in v1.

### Dedupe rules
- Raw-row dedupe key = `sha256` of stable JSON with sorted keys
- Canonical dedupe key = unique constraint on `(dataset, period, state_id, sector_id)`
- If canonical row already exists, update it instead of inserting duplicate
- A backfill run must be idempotent: rerunning the same period range should not create duplicate raw or canonical rows

### Quality issue rules
A quality issue should be recorded when:
- required dimensions are missing
- `period` cannot be parsed
- all metric columns are null
- duplicate raw rows are encountered in the same run
- duplicate canonical key conflict occurs
- a requested period has no rows returned by the upstream API

**`no_data_returned` behavior:** if the upstream API returns zero rows for a requested period range, create one `no_data_returned` quality issue of severity `warning` and mark the run status as `success`. An empty response is not a pipeline failure — it is valid data absence. The run should complete and be queryable.

### Run mode rules
- `latest` mode means: ingest exactly one month, the most recent fully completed month in UTC
- `backfill` mode means: ingest an explicit inclusive monthly range from `start_period` to `end_period`
- `latest` mode must not require the caller to provide `start_period` or `end_period`
- `backfill` mode must require both `start_period` and `end_period`
- `backfill` mode is the default in local/manual scripts because it is deterministic for testing

**`latest` mode date resolution — implement exactly this logic:**

```python
from datetime import datetime, timedelta

def resolve_latest_period() -> str:
    """Return the most recent fully completed month as YYYY-MM string."""
    today = datetime.utcnow().date()
    first_of_current_month = today.replace(day=1)
    last_month = first_of_current_month - timedelta(days=1)
    return last_month.strftime("%Y-%m")
```

Do not use `month - 1` arithmetic — it breaks in January (produces month 0).
In tests, mock `datetime.utcnow` using `pytest-mock` `mocker.patch` to make the result deterministic.

### S3 archive rules
- archive one JSON file per ingestion run
- archive after raw rows are fetched but before normalization begins
- S3 key pattern:
  `archives/electricity-retail-sales/{run_id}.json`

---

## 8. API Surface

### Health
| Method | Path | Response |
|---|---|---|
| GET | `/health` | `{"status": "ok"}` |

### Ingestion
| Method | Path | Purpose |
|---|---|---|
| POST | `/api/ingest/run` | create a new ingestion run and process a date range |
| GET | `/api/ingest/runs` | list recent ingestion runs |
| GET | `/api/ingest/runs/{run_id}` | inspect one ingestion run |

### Raw / canonical metrics
| Method | Path | Purpose |
|---|---|---|
| GET | `/api/metrics` | list normalized rows with filters |
| GET | `/api/metrics/{metric_id}` | get one normalized row |

### Analytics
| Method | Path | Purpose |
|---|---|---|
| GET | `/api/analytics/state-summary` | state-level aggregate rows |
| GET | `/api/analytics/sector-summary` | sector-level aggregate rows |
| GET | `/api/analytics/top-states` | top-N states by one metric for a single period |
| GET | `/api/analytics/price-movers` | trailing-12-month residential price change by state |

### Quality
| Method | Path | Purpose |
|---|---|---|
| GET | `/api/quality/issues` | list quality issues |
| GET | `/api/quality/issues/{issue_id}` | get one quality issue |
| GET | `/api/quality/report` | aggregated quality report for one run or latest run |

---

## 9. Example Request / Response Contracts

These exact contracts reduce ambiguity for AI coding tools.

### POST `/api/ingest/run`

**Request body**

```json
{
  "mode": "backfill",
  "start_period": "2024-01",
  "end_period": "2024-12",
  "state_ids": ["CA", "TX", "NY"],
  "sector_ids": ["RES", "COM"]
}
```

**Response**

```json
{
  "run_id": 7,
  "status": "success",
  "run_mode": "backfill",
  "dataset": "electricity/retail-sales",
  "start_period": "2024-01",
  "end_period": "2024-12",
  "row_count_raw": 72,
  "row_count_skipped_raw": 0,
  "row_count_normalized": 72,
  "row_count_inserted": 60,
  "row_count_updated": 12,
  "quality_issue_count": 0,
  "s3_archive_key": "archives/electricity-retail-sales/7.json"
}
```

### GET `/api/metrics?state_id=CA&sector_id=RES&start_period=2024-01&end_period=2024-12`

**Response item**

```json
{
  "id": 101,
  "dataset": "electricity/retail-sales",
  "period": "2024-01-01",
  "state_id": "CA",
  "sector_id": "RES",
  "price_cents_per_kwh": 31.52,
  "sales_mwh": 21345.21,
  "revenue_thousand_usd": 672813.19,
  "customers_count": 13654000,
  "source_hash": "..."
}
```

### GET `/api/analytics/top-states?period=2024-12&metric=avg_price_cents_per_kwh&limit=5`

**Response**

```json
[
  {
    "period": "2024-12-01",
    "state_id": "HI",
    "metric_value": 41.22,
    "rank": 1
  }
]
```

### GET `/api/analytics/price-movers?end_period=2024-12&limit=5`

**Response**

```json
[
  {
    "state_id": "CA",
    "start_period": "2023-12-01",
    "end_period": "2024-12-01",
    "start_avg_price_cents_per_kwh": 28.10,
    "end_avg_price_cents_per_kwh": 31.52,
    "absolute_change": 3.42,
    "percent_change": 12.17,
    "rank": 1
  }
]
```

---

## 10. Service Function Contracts

Implement these functions exactly. Keep them small.

### `app/services/eia_client.py`

```python
def fetch_retail_sales(
    api_key: str,
    start_period: str,
    end_period: str,
    state_ids: list[str] | None = None,
    sector_ids: list[str] | None = None,
) -> list[dict]:
    """Fetch EIA retail-sales rows and return a flat list of raw row dicts."""
```

Rules:
- use `requests.get`
- handle pagination if needed
- raise a descriptive exception on non-200 response
- do not write to the database here

### `app/services/normalizer.py`

```python
def normalize_retail_row(raw_row: dict) -> tuple[dict | None, list[dict]]:
    """
    Convert one raw EIA row into a canonical metric dict.
    Return (normalized_dict, issues).
    If row cannot be normalized, return (None, issues).
    """
```

Issue dict format:

```python
{
    "issue_type": "invalid_period",
    "severity": "error",
    "issue_message": "Could not parse period '2024-99'"
}
```

### `app/services/quality.py`

```python
def detect_quality_issues(normalized_rows: list[dict]) -> list[dict]:
    """Run post-normalization quality checks and return issue dicts."""

def build_quality_report(db, run_id: int | None = None) -> dict:
    """
    Return an aggregated quality report for one run or the latest run.

    If run_id is None, use the run with the highest id (latest by insertion order).
    Do NOT filter for only 'success' runs — a failed run should still be reportable.

    Read issue counts from the quality_issues table by aggregating rows WHERE run_id = X.
    Do NOT read from ingest_runs.quality_issue_count — that column may be stale.
    The quality_issues table is the source of truth for issue counts.
    """
```

### `app/services/analytics.py`

```python
def refresh_state_month_summary(db) -> int:
    """Rebuild state_month_summary from retail_metrics. Return row count written."""

def refresh_sector_month_summary(db) -> int:
    """Rebuild sector_month_summary from retail_metrics. Return row count written."""

def get_price_movers(db, end_period: str, limit: int = 10) -> list[dict]:
    """
    Return states ranked by trailing-12-month residential price change.

    IMPORTANT: Query retail_metrics directly with sector_id = 'RES'.
    Do NOT use state_month_summary — that table aggregates across all sectors
    and has no sector_id column, so it cannot filter for residential only.

    `end_period` arrives as a YYYY-MM string (e.g. "2024-12").
    Convert to date: end_date = date(int(year), int(month), 1).
    Compute start_date = end_date.replace(year=end_date.year - 1).
    This works correctly for all months including January because all periods are first-of-month.
    Do NOT use end_date.month - 12 arithmetic — it produces month 0 in January.
    Do NOT use dateutil — it is not in requirements.

    Logic:
    - For each state, compute avg(price_cents_per_kwh) at end_period and start_period
      from retail_metrics WHERE sector_id = 'RES'
    - Only include states where both periods exist
    - Compute absolute_change = end_avg - start_avg
    - Compute percent_change = (absolute_change / start_avg) * 100
    - Rank by absolute_change descending
    - Return up to `limit` results
    """
```

### `app/services/s3_archive.py`

```python
def upload_run_archive(run_id: int, payload: list[dict]) -> str:
    """Upload raw payload JSON to S3 and return the object key."""
```

### `app/services/ingest_service.py`

```python
def run_ingestion(
    db,
    mode: str = "backfill",
    start_period: str | None = None,
    end_period: str | None = None,
    state_ids: list[str] | None = None,
    sector_ids: list[str] | None = None,
) -> dict:
    """
    Orchestrate a full ingestion run.
    Resolve period range from mode -> create ingest_runs row -> fetch -> archive
    -> persist raw rows -> normalize -> persist canonical rows -> persist quality issues
    -> build quality report -> refresh summaries -> update run status.
    Return a summary dict.
    """
```

---

## 11. Phase-by-Phase Build

---

# Phase 0 — Environment Setup and Skeleton

## Objective
A runnable project skeleton with working FastAPI health check and Dockerized PostgreSQL.

## Tasks

1. Create repo: `gridpulse`
2. Create exact folder structure from Section 5
3. Add `requirements.txt`
4. Add `.env.example`
5. Add `.gitignore`
6. Create `docker-compose.yml` with the following structure:

    ```yaml
    services:
      db:
        image: postgres:15
        environment:
          POSTGRES_USER: postgres
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: gridpulse
        ports:
          - "5432:5432"
        volumes:
          - db_data:/var/lib/postgresql/data

      app:
        build: .
        env_file: .env
        ports:
          - "8000:8000"
        depends_on:
          - db

    volumes:
      db_data:
    ```

    The named volume `db_data` is required. Without it, `docker compose down` destroys the database.

7. Create `app/main.py` with `/health`
8. Create `app/config.py` to load:
   - `DATABASE_URL`
   - `TEST_DATABASE_URL`
   - `EIA_API_KEY`
   - `AWS_REGION`
   - `S3_BUCKET_NAME`
   - `AWS_ACCESS_KEY_ID` (local dev only — EC2 uses instance role)
   - `AWS_SECRET_ACCESS_KEY` (local dev only — EC2 uses instance role)

   Use `pydantic-settings` `BaseSettings` for config loading:

   ```python
   from pydantic_settings import BaseSettings

   class Settings(BaseSettings):
       database_url: str
       test_database_url: str = "postgresql://postgres:postgres@localhost:5432/gridpulse_test"
       eia_api_key: str
       aws_region: str = "us-east-1"
       s3_bucket_name: str
       aws_access_key_id: str = ""
       aws_secret_access_key: str = ""

       class Config:
           env_file = ".env"

   settings = Settings()
   ```

9. Create `app/database.py` with engine, session, `Base`, `get_db`

10. Create `tests/conftest.py` with the test database session and FastAPI dependency override:

    ```python
    import pytest
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.database import Base, get_db
    from app.main import app
    from fastapi.testclient import TestClient
    import os

    TEST_DATABASE_URL = os.getenv(
        "TEST_DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/gridpulse_test"
    )

    engine = create_engine(TEST_DATABASE_URL)
    TestingSessionLocal = sessionmaker(bind=engine)


    @pytest.fixture(scope="function")
    def db():
        Base.metadata.create_all(bind=engine)
        session = TestingSessionLocal()
        try:
            yield session
        finally:
            session.rollback()
            session.close()
            Base.metadata.drop_all(bind=engine)


    @pytest.fixture(scope="function")
    def client(db):
        def override_get_db():
            yield db
        app.dependency_overrides[get_db] = override_get_db
        yield TestClient(app)
        app.dependency_overrides.clear()
    ```

    The `db` fixture is used by service-level tests. The `client` fixture is used by API-level tests. Both create and drop all tables per test function for isolation.

11. Create `.env.example`:

    ```
    DATABASE_URL=postgresql://postgres:postgres@localhost:5432/gridpulse
    TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/gridpulse_test
    EIA_API_KEY=your_eia_api_key_here
    AWS_REGION=us-east-1
    S3_BUCKET_NAME=your-bucket-name
    AWS_ACCESS_KEY_ID=your_key_here        # local dev only
    AWS_SECRET_ACCESS_KEY=your_secret_here # local dev only
    ```

    Get a free EIA API key at: https://www.eia.gov/opendata/register.php

12. Create `pytest.ini` at project root:

    ```ini
    [pytest]
    testpaths = tests
    ```

    Without this, pytest may fail to resolve imports when run from the project root.

## Required dependencies

Add these to `requirements.txt`:

```text
fastapi
uvicorn
sqlalchemy
alembic
psycopg2-binary
pydantic>=2.0
pydantic-settings
requests
boto3
pytest
httpx
pytest-mock
```

Note: `python-dotenv` is NOT needed — `pydantic-settings` loads `.env` files natively via `env_file = ".env"`.

## EIA API base URL

```
https://api.eia.gov/v2/electricity/retail-sales/data/
```

Use this URL in `eia_client.py`. The API key is passed as a query param: `api_key=<EIA_API_KEY>`.

## Phase 0 checklist

- [ ] `uvicorn app.main:app --reload` starts without errors
- [ ] `GET /health` returns `{"status": "ok"}`
- [ ] `docker compose up db` starts PostgreSQL successfully
- [ ] app can connect to the dev database
- [ ] `.env.example` exists
- [ ] `.env` is gitignored


---

# Phase 1 — Database Models and Migrations

## Objective
All six tables exist in PostgreSQL with correct constraints and indexes.

## Tasks

1. Implement ORM models for:
   - `IngestRun`
   - `RawRetailRow`
   - `RetailMetric`
   - `QualityIssue`
   - `StateMonthSummary`
   - `SectorMonthSummary`
2. Add indexes:
   - `raw_retail_rows(run_id)`
   - `raw_retail_rows(period, state_id, sector_id)`
   - `retail_metrics(period, state_id, sector_id)`
   - `quality_issues(run_id)`
3. Configure Alembic:
   - run `alembic init alembic` if not already initialized
   - open `alembic/env.py` and make these changes:

     ```python
     # Add near the top of env.py
     import sys
     import os
     sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

     from app.database import Base
     from app import models  # noqa: F401 — must import so all models register with Base

     # Set target_metadata so autogenerate sees the models
     target_metadata = Base.metadata
     ```

   - in `alembic.ini`, leave `sqlalchemy.url` blank — it will be set dynamically
   - in `env.py`, set the URL dynamically in both `run_migrations_offline()` and `run_migrations_online()`:

     ```python
     from app.config import settings
     url = settings.database_url
     ```

4. Generate initial migration:
   ```
   alembic revision --autogenerate -m "initial schema"
   ```
5. Apply migration to dev DB:
   ```
   alembic upgrade head
   ```

## Tests

Create `tests/test_models.py` with tests for:

- model insertion
- unique constraint on raw rows
- unique constraint on canonical rows
- nullable behavior for `quality_issues.raw_row_id` and `metric_id`
- primary keys for summary tables

## Phase 1 checklist

- [ ] `alembic upgrade head` succeeds
- [ ] all tables exist
- [ ] unique constraints behave correctly
- [ ] model tests pass


---

# Phase 2 — EIA Client

## Objective
Fetch retail-sales rows from the EIA API and return a flat Python list without touching the DB.

## Tasks

1. Implement `fetch_retail_sales()` in `eia_client.py`
2. Build query params for:
   - `data[]=price`
   - `data[]=sales`
   - `data[]=revenue`
   - `data[]=customers`
   - `frequency=monthly`
   - `start`
   - `end`
   - `facets[stateid][]`
   - `facets[sectorid][]`
   - sorting by `period`
3. Handle pagination if total rows exceed one page
4. Raise clean exceptions on:
   - network failure
   - non-200 response
   - malformed JSON
   - missing `response.data`

## Important rule
**Do not hit the live EIA API in unit tests.**
Use JSON fixtures and monkeypatch `requests.get`.

## Tests

Create `tests/test_eia_client.py` with fixture-based tests for:

- successful fetch returns list of rows
- pagination combines rows correctly
- missing `response.data` raises error
- 500 response raises descriptive error
- request timeout raises descriptive error

## Phase 2 checklist

- [ ] client returns correct rows from fixture
- [ ] pagination logic works
- [ ] all client tests pass without network access


---

# Phase 3 — Raw Ingestion Persistence

## Objective
A manual ingestion run can create an `ingest_runs` row, resolve a valid period range, fetch raw rows, archive payload to S3 mock, and persist deduped `raw_retail_rows`.

## Tasks

1. Implement the beginning of `run_ingestion()`:
   - validate `mode`
   - resolve effective `start_period` and `end_period`
   - create `ingest_runs` row with `status=running`
   - fetch rows via `eia_client`
   - upload payload archive via `s3_archive`
   - persist raw rows with `source_hash`
2. Implement a `resolve_period_range()` helper:
   - `latest` mode -> most recent fully completed month in UTC
   - `backfill` mode -> use explicit inclusive range
3. Implement `source_hash` helper:
   - stable JSON dump with sorted keys
   - SHA-256 hash
4. Skip duplicate raw rows based on `(dataset, source_hash)` unique constraint
5. Update `row_count_raw` and `row_count_skipped_raw`

## Important rule
In tests, **mock S3**. Do not hit AWS.

## Tests

Create `tests/test_ingest_service.py` cases for raw stage only:

- run row created
- `latest` mode resolves to one valid month — mock `datetime.utcnow` using `mocker.patch("app.services.ingest_service.datetime")` so the result is deterministic regardless of when the test runs
- `latest` mode on March 15 2024 resolves to `"2024-02"` (previous completed month)
- `latest` mode on April 1 2024 resolves to `"2024-03"` (not the current month)
- invalid mode raises clean error
- raw rows persisted
- duplicate raw row skipped cleanly
- S3 uploader called once per run
- failed fetch marks run as `failed`

## Phase 3 checklist

- [ ] raw rows persist correctly
- [ ] latest/backfill mode resolution works
- [ ] duplicate raw rows do not crash ingestion
- [ ] failed fetch updates run status properly
- [ ] S3 upload is mocked in tests


---

# Phase 4 — Normalization

## Objective
Raw EIA rows convert into canonical `retail_metrics` rows using simple explicit logic.

## IMPORTANT
Do not rewrite `run_ingestion()`. Only add the normalization and upsert logic this phase requires. All behavior from Phase 3 must continue to pass its tests.

## Tasks

1. Implement `normalize_retail_row(raw_row)`
2. Parse and validate:
   - `period`
   - `stateid`
   - `sectorid`
   - metric fields if present
3. Convert period string like `2024-01` to `date(2024, 1, 1)`
4. Return issue dicts for invalid rows
5. Upsert canonical rows into `retail_metrics`

## Rules

- do not discard issues silently
- if required dimension is missing, return `None` plus issues
- if one metric column is missing but others are valid, still normalize the row
- if all metric columns are missing, emit quality issue and skip canonical write

## Tests

Create `tests/test_normalizer.py` for:

- valid row normalization
- invalid period handling
- missing state handling
- all-metrics-null handling
- partial metrics still normalize

Extend `tests/test_ingest_service.py` for:

- canonical row written after raw stage
- existing canonical key updates instead of duplicating
- when same canonical key is ingested twice with different price values, the updated price is correct (not the original value)

## Phase 4 checklist

- [ ] valid rows normalize correctly
- [ ] invalid rows produce issues
- [ ] canonical upsert works
- [ ] normalization tests pass


---

# Phase 5 — Quality Checks and Reporting

## Objective
Post-normalization checks create `quality_issues` rows for suspicious data conditions, and the project can expose a visible aggregated quality report.

## IMPORTANT
Do not rewrite `run_ingestion()`. Only add the quality check and reporting steps this phase requires. All behavior from Phases 3 and 4 must continue to pass their tests.

## Tasks

1. Implement `detect_quality_issues(normalized_rows)`
2. Add simple rules only:
   - duplicate canonical keys in one batch
   - all metrics null
   - negative metric values where not expected
   - null required dimension after normalization
3. If the upstream fetch returns zero rows for a requested period range, create a deterministic `no_data_returned` quality issue in `run_ingestion()`
4. Persist quality issues linked to run and raw row / metric where possible
5. Implement `build_quality_report(db, run_id=None)` to summarize:
   - run id
   - run status
   - raw rows
   - normalized rows
   - inserted rows
   - updated rows
   - skipped raw rows
   - issue counts by severity
   - issue counts by type

## Important rule
Do not overcomplicate with anomaly detection in v1.
No statistical outlier logic.
Use only deterministic checks.

## Tests

Create `tests/test_quality.py` for:

- duplicate key issue detection
- null metrics issue detection
- negative values issue detection
- issue persistence and severity values

Extend `tests/test_ingest_service.py` for:

- zero-row upstream response records a `no_data_returned` quality issue with severity `warning`
- zero-row upstream response still marks run as `success` (not `failed`)

Create `tests/test_reporting.py` for:

- quality report aggregates counts correctly
- latest-run default works
- empty report case returns clean response

## Phase 5 checklist

- [ ] deterministic issue rules work
- [ ] issue rows are persisted
- [ ] quality report is correct and readable
- [ ] quality tests pass


---

# Phase 6 — Summary Tables

## Objective
Refresh both summary tables from `retail_metrics` after a successful ingestion run. These summaries will support both plain aggregate endpoints and the trailing-12-month price-movers endpoint.

## Tasks

1. Implement `refresh_state_month_summary(db)`
2. Implement `refresh_sector_month_summary(db)`
3. Implement `get_price_movers(db, end_period, limit=10)`
   - query `retail_metrics` directly with `WHERE sector_id = 'RES'`
   - do NOT use `state_month_summary` — that table has no sector_id column
   - compute avg(price_cents_per_kwh) per state at end_period and exactly 12 months earlier
   - require both periods to exist for a state before including it
   - return absolute and percent change plus rank
4. Use explicit SQLAlchemy delete + insert strategy for summary refresh (truncate, recompute, insert). Do not use materialized views or incremental refresh in v1.

## Tests

Create `tests/test_analytics.py` for:

- state summary rows generated correctly
- sector summary rows generated correctly
- average price calculation
- total sales calculation
- total revenue calculation
- refresh replaces stale values
- price movers ranks states correctly by absolute change descending
- states missing either comparison month are excluded from price movers
- price movers returns empty list when end_period does not exist in retail_metrics
- price movers respects the `limit` parameter
- price movers percent_change calculation is correct
- price movers only uses sector_id = 'RES' rows — other sectors must not affect the result

## Phase 6 checklist

- [ ] summary tables populate correctly
- [ ] price movers logic is correct and deterministic
- [ ] refresh logic is deterministic
- [ ] analytics tests pass


---

# Phase 7 — Full Ingestion Orchestrator

## Objective
`run_ingestion()` performs the full end-to-end pipeline correctly.

## IMPORTANT
Do not rewrite `run_ingestion()` from scratch. Complete it by wiring together everything built in Phases 3–6. All prior phase tests must continue to pass. Only add what is missing to connect the full pipeline.

## Tasks

Complete `run_ingestion()` so it does this in order:

1. create ingest run row
2. fetch raw rows
3. archive raw payload to S3
4. persist raw rows
5. normalize rows
6. upsert canonical rows
7. persist quality issues
8. build and persist/report quality counts on the run row
9. refresh summary tables
10. update run status to `success`
11. return summary dict

On failure:

- update run status to `failed`
- store `error_message`
- re-raise only if needed for internal debugging, not from API route

## Tests

Expand `tests/test_ingest_service.py` for full pipeline:

- successful run writes all expected rows
- backfill rerun updates existing canonical rows without duplicating
- failed S3 upload fails run cleanly
- failed normalization still records run and issues
- summary refresh called after canonical writes
- response summary dict contains expected counts

## Phase 7 checklist

- [ ] end-to-end pipeline works with fixtures
- [ ] success and failure cases handled cleanly
- [ ] full ingest service tests pass


---

# Phase 8 — FastAPI Routes

## Objective
API routes expose health, ingestion, metrics, analytics, and quality endpoints.

## Tasks

1. Create routers:
   - `health.py`
   - `ingest.py`
   - `metrics.py`
   - `analytics.py`
   - `quality.py`
2. Implement Pydantic request/response schemas
3. Wire routers into `app/main.py`
4. Keep route handlers thin:
   - validate input
   - call service or CRUD function
   - return response

## Important route rules

### `POST /api/ingest/run`
- synchronous in v1
- support `mode=latest` and `mode=backfill`
- return final run summary after pipeline completes
- acceptable because dataset scope is small

**Request schema validation — required:**
The Pydantic request schema must use a `@model_validator` to enforce that `backfill` mode requires both `start_period` and `end_period`. Without this, the API will accept an invalid backfill request and crash inside the service layer instead of returning a clean 422.

```python
from pydantic import BaseModel, model_validator

class IngestRunRequest(BaseModel):
    mode: str
    start_period: str | None = None
    end_period: str | None = None
    state_ids: list[str] | None = None
    sector_ids: list[str] | None = None

    @model_validator(mode="after")
    def check_backfill_periods(self):
        if self.mode == "backfill":
            if not self.start_period or not self.end_period:
                raise ValueError("backfill mode requires both start_period and end_period")
        return self
```

### `GET /api/metrics`
Support these filters:
- `state_id`
- `sector_id`
- `start_period`
- `end_period`
- `limit`
- `offset`

### `GET /api/analytics/top-states`
Support only these metric names in v1:
- `avg_price_cents_per_kwh`
- `total_sales_mwh`
- `total_revenue_thousand_usd`

Reject anything else with 400.

### `GET /api/ingest/runs`
- default `limit=20`, most recent runs first (ORDER BY id DESC)
- support optional `limit` query param, max 100

### `GET /api/analytics/price-movers`
Rules:
- use residential sector only (`RES`) in v1
- `end_period` query param is `YYYY-MM` string — convert to date inside the service function
- compare `end_period` vs exactly 12 months earlier
- require both periods to exist for a state before ranking it
- rank by absolute price change descending
- return both absolute and percent change

Note: `/api/analytics/trends` is out of scope for v1. Do not implement it.


## Tests

Create `tests/test_api.py` covering:

- `/health`
- `POST /api/ingest/run` with `mode=backfill` and valid period range
- `POST /api/ingest/run` with `mode=latest` (no period params required)
- `POST /api/ingest/run` with `mode=backfill` missing `start_period` returns 422
- `POST /api/ingest/run` with `mode=backfill` missing `end_period` returns 422
- `POST /api/ingest/run` with invalid `mode` returns 422
- `GET /api/ingest/runs`
- `GET /api/metrics`
- `GET /api/analytics/state-summary`
- `GET /api/analytics/top-states` with valid metric name
- `GET /api/analytics/top-states` with invalid metric name returns 400
- `GET /api/analytics/price-movers`
- `GET /api/quality/issues`
- `GET /api/quality/report`
- `GET /api/ingest/runs/{run_id}` with nonexistent id returns 404
- `GET /api/metrics/{metric_id}` with nonexistent id returns 404

Use dependency overrides to inject test DB.
Mock EIA client and S3 uploader in API tests.

## Phase 8 checklist

- [ ] all routes work
- [ ] invalid query params return clean 400
- [ ] API tests pass without live network


---

# Phase 9 — Local Dockerized Run

## Objective
The entire application runs locally via Docker Compose.

## Tasks

1. Create `Dockerfile` with exactly this content:

   ```dockerfile
   FROM python:3.11-slim

   WORKDIR /app

   COPY requirements.txt .
   RUN pip install --no-cache-dir -r requirements.txt

   COPY . .

   CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
   ```

2. The `docker-compose.yml` was already created in Phase 0. Do not rewrite it.
   Verify it has both `app` and `db` services and the named `db_data` volume.

3. Add a DB readiness check to the `db` service in `docker-compose.yml`.
   `depends_on: db` only waits for the container to start, not for Postgres to accept connections.
   Add a healthcheck to the db service:

   ```yaml
   db:
     image: postgres:15
     environment:
       POSTGRES_USER: postgres
       POSTGRES_PASSWORD: postgres
       POSTGRES_DB: gridpulse
     ports:
       - "5432:5432"
     volumes:
       - db_data:/var/lib/postgresql/data
     healthcheck:
       test: ["CMD-SHELL", "pg_isready -U postgres"]
       interval: 5s
       timeout: 5s
       retries: 5
   ```

   And update the `app` service `depends_on` to wait for healthy:

   ```yaml
   app:
     build: .
     env_file: .env
     ports:
       - "8000:8000"
     depends_on:
       db:
         condition: service_healthy
   ```

4. Run migrations manually before starting the app the first time:
   ```
   docker compose run --rm app alembic upgrade head
   ```
5. Ensure env vars from `.env` flow correctly into the app container via `env_file: .env`

## Tests

Manual verification:

- [ ] `docker compose up --build` works
- [ ] API starts and connects to DB
- [ ] `/health` responds
- [ ] one manual latest-mode ingest run succeeds using live EIA API
- [ ] one manual backfill run succeeds using live EIA API
- [ ] metrics, price-movers, and quality-report endpoints return rows


---

# Phase 10 — README and Demo Readiness

## Objective
The repo is understandable and demoable.

## Tasks

1. Write README with:
   - what GridPulse does
   - architecture diagram in prose
   - local setup commands
   - environment variables
   - endpoint overview
   - deployment overview
2. Add a short demo flow:
   - run local app
   - trigger a latest-mode ingest
   - trigger a short backfill
   - query analytics and quality report
3. Add sample curl commands

## Checklist

- [ ] README is accurate
- [ ] no claimed feature is missing
- [ ] demo steps work exactly as written


---

# Phase 11 — AWS Deployment

## Objective
Deploy the app to EC2 with Docker Compose and S3 archive support.

## Deployment design

Use one Ubuntu EC2 instance.

On the instance:
- install Docker and Docker Compose plugin
- clone repo
- create `.env`
- run postgres and app via Compose
- expose app on port 80 or 8000 behind Nginx if desired

### Recommended v1 deployment path
Keep it simple:
- open port 80 or 8000 in security group for testing
- if using Nginx, proxy 80 -> app container
- do not introduce HTTPS in v1 unless you already know how to do it cleanly

### S3 access
Use an **EC2 instance role** with least-privilege access to the archive bucket.
Do not hardcode AWS credentials in the deployed app.

## Tasks

1. Launch EC2 Ubuntu instance
2. Install Docker + Compose plugin + Git
3. Clone repo
4. Configure env vars
5. Attach IAM instance role for S3 archive bucket access
6. Run `docker compose up -d --build`
7. Verify `/health`
8. Verify one live ingestion run
9. Verify JSON archive landed in S3

## Checklist

- [ ] EC2 app is reachable
- [ ] DB persists across restarts
- [ ] live latest-mode ingest succeeds in deployed environment
- [ ] one short backfill succeeds in deployed environment
- [ ] archive JSON appears in S3


---

# Phase 12 — GitHub Actions CI/CD

## Objective
Every push to main runs tests. Deploy happens only if tests pass.

## Workflow

Both CI and deploy live in a single file so `deploy` can declare `needs: test`. Create `.github/workflows/ci.yml`:

```yaml
name: CI and Deploy

on:
  push:
    branches: [main]
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest

    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_USER: postgres
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: gridpulse_test
        ports:
          - 5432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5

    env:
      DATABASE_URL: postgresql://postgres:postgres@localhost:5432/gridpulse_test
      TEST_DATABASE_URL: postgresql://postgres:postgres@localhost:5432/gridpulse_test
      EIA_API_KEY: dummy
      AWS_REGION: us-east-1
      S3_BUCKET_NAME: dummy-bucket
      AWS_ACCESS_KEY_ID: dummy
      AWS_SECRET_ACCESS_KEY: dummy

    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -r requirements.txt
      - run: alembic upgrade head
      - run: pytest

  deploy:
    runs-on: ubuntu-latest
    needs: test
    if: github.ref == 'refs/heads/main' && github.event_name == 'push'

    steps:
      - uses: actions/checkout@v4

      - name: Deploy to EC2
        uses: appleboy/ssh-action@v1.0.3
        with:
          host: ${{ secrets.EC2_HOST }}
          username: ubuntu
          key: ${{ secrets.EC2_SSH_KEY }}
          script: |
            cd ~/gridpulse
            git pull origin main
            docker compose build
            docker compose run --rm app alembic upgrade head
            docker compose up -d
            sleep 5
            curl -f http://localhost:8000/health || exit 1
```

**Key points:**
- `needs: test` ensures deploy only runs after CI passes
- `if: github.ref == 'refs/heads/main'` ensures deploy only runs on pushes to main, not on PRs
- In CI the database host is `localhost`, not `db` — do not copy the `db` hostname from docker-compose

**Required GitHub secrets:**
- `EC2_HOST` — public IP or DNS of your EC2 instance
- `EC2_SSH_KEY` — contents of your EC2 private key (PEM file)

Delete `.github/workflows/deploy.yml` if it was created separately — consolidate into `ci.yml`.

## Important rule
CI must use mocks / fixtures.
It must not require live EIA API or live S3.

## Tests

Deployment checklist:

- [ ] CI goes green on feature branch or PR
- [ ] deploy job in `ci.yml` has `needs: test` — deploy only runs if tests pass
- [ ] deployed app stays healthy after restart
- [ ] a visible code change reaches EC2 successfully


---


## 17. What to Tell Codex / Claude Code

Use instructions like this:

> Read GRIDPULSE_BUILD_PLAN.md. We are on Phase X only. Do not work ahead. Implement only the files, functions, and tests for this phase. Keep the code explicit and simple. Do not add features outside the plan. After writing code, run the exact test file(s) for this phase and fix failing tests before stopping.

And for specific phases:

> Do not introduce async complexity.
> Do not add generic abstractions.
> Do not change schema or endpoint names unless the plan explicitly says so.
> Prefer plain functions over base classes or strategy patterns.

---

## 18. Final Sanity Rules

Before you call the project finished, all of these must be true:

- [ ] every endpoint in the README actually exists
- [ ] every table in the schema has a migration
- [ ] every phase test passes
- [ ] no feature depends on hidden manual setup
- [ ] no duplicate canonical rows exist for the same `(dataset, period, state_id, sector_id)`
- [ ] one live latest-mode ingestion run works from the real EIA API
- [ ] one live short backfill works from the real EIA API
- [ ] one deployment works on EC2
- [ ] one archive file lands in S3
- [ ] `/api/quality/report` returns a meaningful summary
- [ ] `/api/analytics/price-movers` returns ranked rows
- [ ] the project can be explained simply in under 2 minutes

