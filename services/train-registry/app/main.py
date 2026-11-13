"""Train Registry: trains, their routes and stops."""

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from prometheus_fastapi_instrumentator import Instrumentator
from app.db import apply_schema, create_pool
from app.telemetry import configure_logging, logger

pool = create_pool()


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    pool.open(wait=True, timeout=30)
    apply_schema(pool)
    yield
    pool.close()


app = FastAPI(title="Train Registry", lifespan=lifespan)

Instrumentator(excluded_handlers=["/metrics", "/healthz", "/readyz"]).instrument(app).expose(
    app, include_in_schema=False
)


@app.middleware("http")
async def log_diversion_lookups(request: Request, call_next):
    """Schedule Manager sends X-Txn-Id when it reads a route for a diversion; log those lookups."""
    response = await call_next(request)
    txn_id = request.headers.get("x-txn-id")
    if txn_id:
        logger.info(
            "Route looked up for a diversion",
            extra={"txn_id": txn_id, "path": request.url.path, "status": response.status_code},
        )
    return response


@app.get("/healthz")
def healthz() -> dict:
    """Liveness: the process is up."""
    return {"status": "ok"}


@app.get("/readyz")
def readyz() -> dict:
    """Readiness: the database answers."""
    with pool.connection() as conn:
        conn.execute("SELECT 1")
    return {"status": "ready"}


@app.get("/api/trains")
def list_trains() -> list[dict]:
    with pool.connection() as conn:
        return conn.execute(
            "SELECT number, name, category, origin, destination FROM trains ORDER BY number"
        ).fetchall()


@app.get("/api/trains/{number}")
def get_train(number: str) -> dict:
    with pool.connection() as conn:
        train = conn.execute(
            "SELECT number, name, category, origin, destination FROM trains WHERE number = %s",
            (number,),
        ).fetchone()
        if train is None:
            raise HTTPException(status_code=404, detail=f"Train {number} not found")
        train["stops"] = conn.execute(
            """
            SELECT r.stop_seq, r.station_code, s.name AS station_name,
                   r.arrives, r.departs, r.journey_day
            FROM route_stops r
            JOIN stations s ON s.code = r.station_code
            WHERE r.train_number = %s
            ORDER BY r.stop_seq
            """,
            (number,),
        ).fetchall()
    return train