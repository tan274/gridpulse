from fastapi import FastAPI
from app.routers import health, ingest, metrics, analytics, quality

app = FastAPI(title="GridPulse")

app.include_router(health.router)
app.include_router(ingest.router, prefix="/api/ingest")
app.include_router(metrics.router, prefix="/api/metrics")
app.include_router(analytics.router, prefix="/api/analytics")
app.include_router(quality.router, prefix="/api/quality")
