"""Schedule Manager: campaigns to be the coordinator; the leader turns reported incidents into diversions."""

import os
import socket
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from prometheus_fastapi_instrumentator import Instrumentator

from app.consumer import IncidentConsumer
from app.coordinator import Coordinator
from app.db import apply_schema, create_pool
from app.diversions import DiversionLog
from app.events import CoordinationEvents
from app.grpc_server import GrpcServer
from app.leadership import Leadership
from app.participants import ParticipantClient
from app.telemetry import configure_logging
from app.train_registry import TrainRegistryClient


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    pool = create_pool()
    pool.open(wait=True, timeout=30)
    apply_schema(pool)

    candidate_id = os.environ.get("CANDIDATE_ID") or socket.gethostname()  # the pod name in Kubernetes
    leadership = Leadership(os.environ["LEADER_ELECTION_TARGET"], os.environ.get("ELECTION", "schedule-coordinator"),
                            candidate_id)
    participants = [
        ParticipantClient("track-availability", os.environ["TRACK_AVAILABILITY_TARGET"]),
        ParticipantClient("incident-manager", os.environ["INCIDENT_MANAGER_TARGET"]),
    ]
    registry = TrainRegistryClient()
    events = CoordinationEvents()
    coordinator = Coordinator(pool, leadership, participants, registry, events,
                              int(os.environ.get("DIVERSION_DELAY_MINUTES", "25")))
    consumer = IncidentConsumer(leadership, coordinator)
    app.state.leadership = leadership
    app.state.log = DiversionLog(pool)
    grpc_server = GrpcServer(app.state.log)

    grpc_server.start()
    leadership.start()
    consumer.start()
    yield
    consumer.stop()
    leadership.stop()
    grpc_server.stop()
    coordinator.close()
    events.close()
    registry.close()
    for client in participants:
        client.close()
    pool.close()


app = FastAPI(title="Schedule Manager", lifespan=lifespan)
# Request rate, errors and duration per endpoint; the same /metrics also carries the coordinator's numbers
Instrumentator(excluded_handlers=["/metrics", "/healthz", "/readyz"]).instrument(app).expose(
    app, include_in_schema=False
)


@app.get("/healthz")
def healthz() -> dict:
    """Liveness: the process is up."""
    return {"status": "ok"}


@app.get("/readyz")
def readyz() -> dict:
    """Readiness: the database answers. Followers are ready too: every replica serves the REST API."""
    app.state.log.ping()
    return {"status": "ready"}


@app.get("/api/schedule/coordinator")
def coordinator_status() -> dict:
    """Which replica answered, and whether it is the coordinator."""
    return app.state.leadership.status()


@app.get("/api/schedule/diversions")
def recent_diversions() -> list[dict]:
    return app.state.log.recent()


@app.get("/api/schedule/diversions/{txn_id}")
def get_diversion(txn_id: str) -> dict:
    txn = app.state.log.get(txn_id)
    if txn is None:
        raise HTTPException(status_code=404, detail=f"Transaction {txn_id} not found")
    return txn


@app.get("/api/schedule/trains/{train_number}")
def revised_times(train_number: str) -> dict:
    """The train's revised arrival times from its latest committed diversion."""
    txn = app.state.log.latest_revision(train_number)
    if txn is None:
        raise HTTPException(status_code=404, detail=f"No committed diversion for train {train_number}")
    return txn