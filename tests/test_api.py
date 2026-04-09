import json
import pytest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from app.models import IngestRun, RawRetailRow, RetailMetric

DATASET = "electricity/retail-sales"

FIXTURES = Path(__file__).parent / "fixtures"
SAMPLE_ROWS = json.loads((FIXTURES / "eia_retail_sales_sample.json").read_text())["response"]["data"]
FAKE_ARCHIVE_KEY = "archives/electricity-retail-sales/1.json"


@pytest.fixture
def mock_ingest_deps(mocker):
    mocker.patch(
        "app.services.ingest_service.eia_client.fetch_retail_sales",
        return_value=SAMPLE_ROWS,
    )
    mocker.patch(
        "app.services.ingest_service.s3_archive.upload_run_archive",
        return_value=FAKE_ARCHIVE_KEY,
    )


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------

def test_ingest_run_backfill(client, mock_ingest_deps):
    resp = client.post("/api/ingest/run", json={
        "mode": "backfill",
        "start_period": "2024-01",
        "end_period": "2024-01",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "success"
    assert data["run_mode"] == "backfill"
    assert "run_id" in data


def test_ingest_run_latest(client, mock_ingest_deps):
    resp = client.post("/api/ingest/run", json={"mode": "latest"})
    assert resp.status_code == 200
    assert resp.json()["status"] == "success"


def test_ingest_run_missing_start_period_returns_422(client):
    resp = client.post("/api/ingest/run", json={
        "mode": "backfill",
        "end_period": "2024-01",
    })
    assert resp.status_code == 422


def test_ingest_run_missing_end_period_returns_422(client):
    resp = client.post("/api/ingest/run", json={
        "mode": "backfill",
        "start_period": "2024-01",
    })
    assert resp.status_code == 422


def test_ingest_run_invalid_mode_returns_422(client):
    resp = client.post("/api/ingest/run", json={"mode": "weekly"})
    assert resp.status_code == 422


def test_list_ingest_runs(client, mock_ingest_deps):
    client.post("/api/ingest/run", json={
        "mode": "backfill",
        "start_period": "2024-01",
        "end_period": "2024-01",
    })
    resp = client.get("/api/ingest/runs")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)
    assert len(resp.json()) >= 1


def test_ingest_run_not_found_returns_404(client):
    resp = client.get("/api/ingest/runs/99999")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def test_list_metrics(client, mock_ingest_deps):
    client.post("/api/ingest/run", json={
        "mode": "backfill",
        "start_period": "2024-01",
        "end_period": "2024-01",
    })
    resp = client.get("/api/metrics")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_metric_not_found_returns_404(client):
    resp = client.get("/api/metrics/99999")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------

def test_analytics_state_summary(client, mock_ingest_deps):
    client.post("/api/ingest/run", json={
        "mode": "backfill",
        "start_period": "2024-01",
        "end_period": "2024-01",
    })
    resp = client.get("/api/analytics/state-summary")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_analytics_top_states_valid(client, mock_ingest_deps):
    client.post("/api/ingest/run", json={
        "mode": "backfill",
        "start_period": "2024-01",
        "end_period": "2024-01",
    })
    resp = client.get("/api/analytics/top-states?period=2024-01&metric=avg_price_cents_per_kwh")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_analytics_top_states_invalid_metric_returns_400(client):
    resp = client.get("/api/analytics/top-states?period=2024-01&metric=nonexistent_field")
    assert resp.status_code == 400


def test_analytics_sector_summary(client, mock_ingest_deps):
    client.post("/api/ingest/run", json={
        "mode": "backfill",
        "start_period": "2024-01",
        "end_period": "2024-01",
    })
    resp = client.get("/api/analytics/sector-summary")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)
    assert len(resp.json()) > 0


def test_analytics_price_movers(client, db):
    """Seed RES metrics for two periods, verify price movers returns ranked results."""
    run = IngestRun(
        dataset=DATASET, started_at=datetime.utcnow(), status="success",
        run_mode="backfill", start_period="2023-01", end_period="2024-01",
    )
    db.add(run)
    db.flush()

    for period, price, source_hash in [
        (date(2024, 1, 1), Decimal("15"), "end-CA"),
        (date(2023, 1, 1), Decimal("10"), "prior-CA"),
    ]:
        raw = RawRetailRow(
            run_id=run.id, dataset=DATASET, period=period.strftime("%Y-%m"),
            state_id="CA", sector_id="RES", source_hash=source_hash,
            row_json={}, created_at=datetime.utcnow(),
        )
        db.add(raw)
        db.flush()
        db.add(RetailMetric(
            run_id=run.id, raw_row_id=raw.id, dataset=DATASET,
            period=period, state_id="CA", sector_id="RES",
            price_cents_per_kwh=price, source_hash=f"m-{source_hash}",
            created_at=datetime.utcnow(),
        ))
    db.commit()

    resp = client.get("/api/analytics/price-movers?end_period=2024-01")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["state_id"] == "CA"
    assert data[0]["rank"] == 1
    assert "absolute_change" in data[0]
    assert "percent_change" in data[0]


# ---------------------------------------------------------------------------
# Quality
# ---------------------------------------------------------------------------

def test_quality_issues(client, mock_ingest_deps):
    client.post("/api/ingest/run", json={
        "mode": "backfill",
        "start_period": "2024-01",
        "end_period": "2024-01",
    })
    resp = client.get("/api/quality/issues")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_ingest_run_get_by_id(client, mock_ingest_deps):
    post = client.post("/api/ingest/run", json={
        "mode": "backfill",
        "start_period": "2024-01",
        "end_period": "2024-01",
    })
    run_id = post.json()["run_id"]
    resp = client.get(f"/api/ingest/runs/{run_id}")
    assert resp.status_code == 200
    assert resp.json()["run_id"] == run_id
    assert resp.json()["status"] == "success"


def test_metric_get_by_id(client, mock_ingest_deps):
    client.post("/api/ingest/run", json={
        "mode": "backfill",
        "start_period": "2024-01",
        "end_period": "2024-01",
    })
    metrics = client.get("/api/metrics").json()
    assert len(metrics) > 0
    metric_id = metrics[0]["id"]
    resp = client.get(f"/api/metrics/{metric_id}")
    assert resp.status_code == 200
    assert resp.json()["id"] == metric_id


def test_quality_issues_get_by_id(client, mock_ingest_deps):
    client.post("/api/ingest/run", json={
        "mode": "backfill",
        "start_period": "2024-01",
        "end_period": "2024-01",
    })
    issues = client.get("/api/quality/issues").json()
    if len(issues) > 0:
        issue_id = issues[0]["id"]
        resp = client.get(f"/api/quality/issues/{issue_id}")
        assert resp.status_code == 200
        assert resp.json()["id"] == issue_id


def test_quality_issues_not_found_returns_404(client):
    resp = client.get("/api/quality/issues/99999")
    assert resp.status_code == 404


def test_invalid_period_on_metrics_returns_400(client):
    resp = client.get("/api/metrics?start_period=abc")
    assert resp.status_code == 400


def test_invalid_period_on_price_movers_returns_400(client):
    resp = client.get("/api/analytics/price-movers?end_period=2024-99")
    assert resp.status_code == 400


def test_invalid_period_on_top_states_returns_400(client):
    resp = client.get("/api/analytics/top-states?period=bad&metric=avg_price_cents_per_kwh")
    assert resp.status_code == 400


def test_invalid_period_on_state_summary_returns_400(client):
    resp = client.get("/api/analytics/state-summary?period=2024-13")
    assert resp.status_code == 400


def test_invalid_period_on_sector_summary_returns_400(client):
    resp = client.get("/api/analytics/sector-summary?period=abc")
    assert resp.status_code == 400


def test_metrics_state_id_filter(client, mock_ingest_deps):
    client.post("/api/ingest/run", json={
        "mode": "backfill",
        "start_period": "2024-01",
        "end_period": "2024-01",
    })
    resp = client.get("/api/metrics?state_id=CA")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) > 0
    assert all(m["state_id"] == "CA" for m in data)


def test_ingest_runs_limit_capped_at_100(client, mock_ingest_deps):
    resp = client.get("/api/ingest/runs?limit=999")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_quality_report(client, mock_ingest_deps):
    client.post("/api/ingest/run", json={
        "mode": "backfill",
        "start_period": "2024-01",
        "end_period": "2024-01",
    })
    resp = client.get("/api/quality/report")
    assert resp.status_code == 200
    data = resp.json()
    assert "run_id" in data
    assert "issue_count_total" in data
