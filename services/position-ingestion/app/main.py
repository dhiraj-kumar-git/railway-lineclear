"""Position Ingestion: accepts GPS position reports from trains."""

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel, Field

from app.position_publisher import PositionPublisher
from app.position_store import PositionStore
from app.train_registry import TrainRegistryClient, TrainRegistryUnavailable


class PositionReport(BaseModel):
    train_number: str = Field(pattern=r"^\d{5}$", examples=["12951"])
    section: str = Field(pattern=r"^[A-Z][A-Z0-9]{0,7}$", examples=["S7"])
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    speed_kmph: float = Field(ge=0, le=200)
    recorded_at: datetime | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.registry = TrainRegistryClient()
    app.state.publisher = PositionPublisher()
    app.state.store = PositionStore()
    yield
    app.state.publisher.close()
    app.state.registry.close()


app = FastAPI(title="Position Ingestion", lifespan=lifespan)
Instrumentator(excluded_handlers=["/metrics", "/healthz", "/readyz"]).instrument(app).expose(
    app, include_in_schema=False
)


@app.get("/healthz")
def healthz() -> dict:
    """Liveness: the process is up."""
    return {"status": "ok"}


@app.get("/readyz")
def readyz() -> dict:
    """Readiness: Kafka answers and the topic exists."""
    if not app.state.publisher.ready():
        raise HTTPException(status_code=503, detail="Kafka topic train.positions is not reachable")
    return {"status": "ready"}


@app.post("/api/positions", status_code=202)
def report_position(report: PositionReport) -> dict:
    try:
        known = app.state.registry.train_exists(report.train_number)
    except TrainRegistryUnavailable:
        raise HTTPException(status_code=503, detail="Train Registry is unavailable; try again")
    if not known:
        raise HTTPException(status_code=422, detail=f"Train {report.train_number} is not in the Train Registry")

    recorded_at = report.recorded_at or datetime.now(timezone.utc)
    if recorded_at.tzinfo is None:
        recorded_at = recorded_at.replace(tzinfo=timezone.utc)
    recorded_at = recorded_at.astimezone(timezone.utc)

    position = {
        "event_id": str(uuid.uuid4()),
        "train_number": report.train_number,
        "section": report.section,
        "latitude": report.latitude,
        "longitude": report.longitude,
        "speed_kmph": report.speed_kmph,
        "recorded_at": recorded_at.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "recorded_at_ms": int(recorded_at.timestamp() * 1000),
    }
    app.state.publisher.publish(position)
    stored = app.state.store.save_latest(position)
    return {"event_id": position["event_id"], "stored_as_latest": stored}


@app.get("/api/positions/{train_number}")
def latest_position(train_number: str) -> dict:
    position = app.state.store.latest(train_number)
    if position is None:
        raise HTTPException(status_code=404, detail=f"No position known for train {train_number}")
    return position