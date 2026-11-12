"""Incident Manager: track incidents over REST, and the incident's side of two-phase commit over gRPC."""

import os
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import Depends, FastAPI, HTTPException
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel, Field

from app.db import apply_schema, create_pool
from app.events import IncidentEvents
from app.grpc_server import GrpcServer
from app.incidents import IncidentBusy, IncidentNotFound, IncidentStore
from app.participant import IncidentParticipant
from app.recovery import InDoubtRecovery
from app.staff import require_staff
from app.telemetry import configure_logging


class IncidentReport(BaseModel):
    kind: Literal["rail_fracture", "signal_failure", "track_obstruction", "overhead_wire_fault"]
    section: str = Field(pattern=r"^S([1-9]|10)$", examples=["S7"])
    train_number: str | None = Field(default=None, pattern=r"^\d{5}$", examples=["12951"])
    description: str = Field(default="", max_length=500)


class Resolution(BaseModel):
    note: str = Field(default="Line restored", max_length=500)


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    pool = create_pool()
    pool.open(wait=True, timeout=30)
    apply_schema(pool)
    app.state.store = IncidentStore(pool)
    app.state.events = IncidentEvents()
    participant = IncidentParticipant(pool)
    app.state.grpc = GrpcServer(participant, app.state.events)
    app.state.grpc.start()

    # Settle anything left PREPARED by a previous run, by asking the coordinator's log
    def committed(txn_id: str, result: tuple) -> None:
        change = result[1]
        if change is not None:  # sent only after the database change is committed, as for a Commit call
            app.state.events.status_changed(change["incident_id"], change["status"], change["note"], txn_id)

    recovery = None
    if os.environ.get("COORDINATOR_TARGET"):
        recovery = InDoubtRecovery(participant, os.environ["COORDINATOR_TARGET"], on_commit=committed)
        recovery.start()
    yield
    if recovery is not None:
        recovery.stop()
    app.state.grpc.stop()
    app.state.events.close()
    pool.close()


app = FastAPI(title="Incident Manager", lifespan=lifespan)
# Request rate, errors and duration per endpoint; the same /metrics also carries the gRPC counters
Instrumentator(excluded_handlers=["/metrics", "/healthz", "/readyz"]).instrument(app).expose(
    app, include_in_schema=False
)


@app.get("/healthz")
def healthz() -> dict:
    """Liveness: the process is up."""
    return {"status": "ok"}


@app.get("/readyz")
def readyz() -> dict:
    """Readiness: the database answers, and Kafka answers with the topic present."""
    app.state.store.ping()
    if not app.state.events.ready():
        raise HTTPException(status_code=503, detail="Kafka topic incident.events is not reachable")
    return {"status": "ready"}


@app.post("/api/incidents", status_code=201)
def report_incident(report: IncidentReport) -> dict:
    incident = app.state.store.report(report.kind, report.section, report.train_number, report.description)
    app.state.events.incident_reported(incident)  # after the database commit
    return incident


@app.get("/api/incidents")
def list_incidents(status: Literal["REPORTED", "MITIGATING", "RESOLVED"] | None = None) -> list[dict]:
    return app.state.store.recent(status)


@app.get("/api/incidents/{incident_id}")
def get_incident(incident_id: str) -> dict:
    try:
        return app.state.store.get(incident_id)
    except IncidentNotFound:
        raise HTTPException(status_code=404, detail=f"Incident {incident_id} not found")


@app.post("/api/incidents/{incident_id}/resolve", dependencies=[Depends(require_staff)])
def resolve_incident(incident_id: str, resolution: Resolution) -> dict:
    try:
        incident = app.state.store.resolve(incident_id, resolution.note)
    except IncidentNotFound:
        raise HTTPException(status_code=404, detail=f"Incident {incident_id} not found")
    except IncidentBusy as error:
        raise HTTPException(status_code=409, detail=str(error))
    app.state.events.status_changed(incident_id, "RESOLVED", resolution.note)
    return incident