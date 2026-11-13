"""Operations Analytics reader: Flink's results to DynamoDB, with Prometheus metrics."""

import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response
from prometheus_client import make_asgi_app

from .consumer import ResultsReader
from .metrics import LatestResults
from .results_store import ResultsStore


class JsonLogFormatter(logging.Formatter):
    """One JSON object per line, so Loki can filter on any field."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {"level": record.levelname, "logger": record.name, "message": record.getMessage()}
        for field in ("metric", "section"):
            if hasattr(record, field):
                entry[field] = getattr(record, field)
        if record.exc_info:
            entry["error"] = self.formatException(record.exc_info)
        return json.dumps(entry)


handler = logging.StreamHandler()
handler.setFormatter(JsonLogFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.store = ResultsStore()
    app.state.reader = ResultsReader(app.state.store, LatestResults())
    app.state.reader.start()
    yield
    app.state.reader.close()


app = FastAPI(title="Operations Analytics", lifespan=lifespan)
app.mount("/metrics", make_asgi_app())


@app.get("/healthz")
def healthz(response: Response) -> dict:
    """Alive while the reading thread runs; if it stopped, Kubernetes restarts the pod."""
    if not app.state.reader.alive():
        response.status_code = 503
        return {"status": "reader stopped"}
    return {"status": "ok"}


@app.get("/readyz")
def readyz(response: Response) -> dict:
    if not app.state.reader.kafka_ready():
        response.status_code = 503
        return {"status": "kafka unavailable"}
    if not app.state.store.ready():
        response.status_code = 503
        return {"status": "results table unavailable"}
    return {"status": "ready"}
