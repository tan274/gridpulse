# GridPulse

GridPulse is a data pipeline that fetches U.S. retail electricity sales data from the EIA API, archives raw payloads to S3, normalizes the data into PostgreSQL, runs quality checks, and exposes the results through a FastAPI REST API.

---

## What it does

- Fetches monthly retail electricity data (prices, sales, revenue by state and sector) from the EIA `electricity/retail-sales` dataset
- Supports two ingestion modes: **latest** (most recent fully completed month) and **backfill** (a specified date range)
- Archives raw API responses as JSON to S3 before any transformation
- Normalizes rows into a canonical facts table, deduplicating by `(dataset, period, state_id, sector_id)`
- Runs quality checks after each ingestion and stores flagged issues
- Refreshes state-month and sector-month summary tables after each run
- Exposes analytics and quality data via a REST API

---

## Architecture

```
EIA API
  -> resolve run mode (latest or backfill)
  -> create ingest run record in DB (to get a run ID)
  -> fetch monthly retail-sales rows from EIA
  -> archive raw JSON to S3 (keyed by run ID)
  -> persist raw rows to DB (skip duplicates by source hash)
  -> normalize rows into canonical facts table (upsert by dataset/period/state/sector)
  -> run quality checks
  -> refresh state-month and sector-month summary tables
  -> mark run complete
  -> serve results via FastAPI
```

The app runs as a Docker container alongside a PostgreSQL container. S3 is used only for archiving raw payloads. There is no frontend, no auth, and no background task queue.

---

## Local Setup

### Prerequisites

- Docker with Compose plugin
- An EIA API key (free at [eia.gov](https://www.eia.gov/opendata/))
- An AWS S3 bucket for raw payload archival

### 1. Create a `.env` file

```env
EIA_API_KEY=your_eia_api_key_here
S3_BUCKET_NAME=your-s3-bucket-name
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_access_key_id
AWS_SECRET_ACCESS_KEY=your_secret_access_key
```

> `DATABASE_URL` is already set in `docker-compose.yml` for the app container (`postgresql://postgres:postgres@db:5432/gridpulse`). You do not need it in `.env`.

### 2. Start the services

```bash
docker compose up --build -d
```

This starts `db` (PostgreSQL) and `app` (FastAPI on port 8000). The app container waits for the database healthcheck to pass before starting.

### 3. Run migrations

```bash
docker compose run --rm app alembic upgrade head
```

### 4. Verify

```bash
curl http://localhost:8000/health
# {"status": "ok"}
```

---

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | Set by docker-compose | `postgresql://postgres:postgres@db:5432/gridpulse` | PostgreSQL connection string — overridden in docker-compose.yml |
| `EIA_API_KEY` | Yes | — | EIA Open Data API key |
| `S3_BUCKET_NAME` | Yes | — | S3 bucket for raw payload archives |
| `AWS_REGION` | No | `us-east-1` | AWS region for S3 |
| `AWS_ACCESS_KEY_ID` | No | `""` | AWS credentials (omit if using an instance role) |
| `AWS_SECRET_ACCESS_KEY` | No | `""` | AWS credentials (omit if using an instance role) |
| `TEST_DATABASE_URL` | No | `postgresql://postgres:postgres@localhost:5432/gridpulse_test` | Used by pytest only |

---

## Endpoint Overview

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Health check |
| POST | `/api/ingest/run` | Trigger an ingestion run |
| GET | `/api/ingest/runs` | List all ingestion runs |
| GET | `/api/ingest/runs/{run_id}` | Get a single ingestion run |
| GET | `/api/metrics` | List canonical fact rows |
| GET | `/api/metrics/{metric_id}` | Get a single canonical fact row |
| GET | `/api/analytics/state-summary` | State-month summary aggregates |
| GET | `/api/analytics/sector-summary` | Sector-month summary aggregates |
| GET | `/api/analytics/top-states` | Top states by a chosen metric: `avg_price_cents_per_kwh`, `total_sales_mwh`, or `total_revenue_thousand_usd` (requires `period` and `metric` params) |
| GET | `/api/analytics/price-movers` | States with largest price changes over trailing 12 months (requires `end_period` param) |
| GET | `/api/quality/issues` | List quality issues |
| GET | `/api/quality/issues/{issue_id}` | Get a single quality issue |
| GET | `/api/quality/report` | Quality summary report |

---

## Demo Flow

With the app running locally:

### Run a latest-mode ingestion

Fetches the most recent fully completed calendar month.

```bash
curl -X POST http://localhost:8000/api/ingest/run \
  -H "Content-Type: application/json" \
  -d '{"mode": "latest"}'
```

### Run a backfill

```bash
curl -X POST http://localhost:8000/api/ingest/run \
  -H "Content-Type: application/json" \
  -d '{"mode": "backfill", "start_period": "2025-01", "end_period": "2025-03"}'
```

### Check ingestion runs

```bash
curl http://localhost:8000/api/ingest/runs
```

### Query state-month summaries

```bash
curl http://localhost:8000/api/analytics/state-summary
```

### Top states by average price for a given month

```bash
curl "http://localhost:8000/api/analytics/top-states?period=2025-03&metric=avg_price_cents_per_kwh"
```

### Query price movers

```bash
curl "http://localhost:8000/api/analytics/price-movers?end_period=2025-03"
```

### Check the quality report

```bash
curl http://localhost:8000/api/quality/report
```

---

## Deployment Overview

The intended deployment target is a single EC2 instance running Docker Compose.

1. Launch an Ubuntu EC2 instance
2. Install Docker and the Compose plugin
3. Clone this repo
4. Create a `.env` file with your credentials (see Environment Variables above)
5. Attach an IAM instance role with least-privilege access to your S3 bucket (omit `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` when using an instance role)
6. Run `docker compose up -d --build`
7. Run `docker compose run --rm app alembic upgrade head`
8. Verify with `curl http://localhost:8000/health`

The `db_data` Docker volume persists the database across container restarts.
